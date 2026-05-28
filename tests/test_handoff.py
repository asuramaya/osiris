from __future__ import annotations

import uuid
from pathlib import Path

import pytest
import redis.asyncio as aioredis
from src.actions.core import Actions
from src.orchestrator.budgets import BudgetLedger
from src.orchestrator.cascade import CascadeContext, run_cascade
from src.orchestrator.challenges import Challenge, ChallengeDetected, ChallengeKind
from src.orchestrator.handoff import HandoffError, abandon, open_handoff, post_back, suspend, tray
from src.orchestrator.manifests import Manifest, load_manifests
from src.orchestrator.ratelimit import RateLimiter
from src.parsers.base import InputObject

HELPERS_DIR = Path(__file__).parent.parent / "helpers"
TELEGRAM = load_manifests(HELPERS_DIR)["telegram_channel_profile"]


def _ctx(actions: Actions, redis: aioredis.Redis, connectors: dict | None = None) -> CascadeContext:
    return CascadeContext(
        actions=actions,
        limiter=RateLimiter(redis),
        ledger=BudgetLedger(actions.pool, redis),
        manifests={"telegram_channel_profile": TELEGRAM},
        connectors=connectors or {},
    )


async def _make_case(actions: Actions, budgets: dict) -> uuid.UUID:
    cid = await actions.pool.fetchval(
        "INSERT INTO cases (name, owner, budgets) VALUES ('c','analyst:test',$1) RETURNING id",
        budgets,
    )
    await actions.pool.execute(
        "INSERT INTO triggers (on_event, match, helper_id, enabled) "
        "VALUES ('object_created', $1, 'telegram_channel_profile', true)",
        {"type": "TelegramChannel"},
    )
    return uuid.UUID(str(cid))


async def test_gated_cascade_suspends_to_tray(
    actions: Actions, redis_client: aioredis.Redis
) -> None:
    case_id = await _make_case(actions, {"max_human_handoffs": 5})
    await actions.create_or_find_object(
        "TelegramChannel", "dprk_news", "analyst:test", case_id
    )
    await run_cascade(_ctx(actions, redis_client))

    # the run parked awaiting a human; nothing was fetched server-side
    run = await actions.pool.fetchrow(
        "SELECT status, tier FROM helper_runs WHERE helper_id='telegram_channel_profile'"
    )
    assert run["status"] == "awaiting_human"
    assert run["tier"] == "gated"

    items = await tray(actions, case_id=case_id)
    assert len(items) == 1
    assert items[0]["url"] == "https://t.me/s/dprk_news"  # template rendered
    assert items[0]["challenge_kind"] is None              # gated, not a challenge


async def test_full_resume_runs_parser_and_finishes(
    actions: Actions, redis_client: aioredis.Redis, monkeypatch, tmp_path
) -> None:
    monkeypatch.setenv("OSIRIS_ARTIFACT_DIR", str(tmp_path))
    case_id = await _make_case(actions, {"max_human_handoffs": 5})
    chan = await actions.create_or_find_object(
        "TelegramChannel", "dprk_news", "analyst:test", case_id
    )
    await run_cascade(_ctx(actions, redis_client))
    handoff_id = (await tray(actions, case_id=case_id))[0]["id"]

    await open_handoff(actions, handoff_id)
    assert await actions.pool.fetchval(
        "SELECT status FROM helper_runs WHERE id=(SELECT helper_run_id FROM handoffs WHERE id=$1)",
        handoff_id,
    ) == "in_browser"

    # analyst posts back what they scraped in their real browser session
    counts = await post_back(
        actions, TELEGRAM, handoff_id,
        {"title": "DPRK News", "subscribers": 12345, "description": "state media mirror"},
    )
    assert counts["properties"] >= 3

    # run finished, handoff resolved, properties asserted on the channel
    assert await actions.pool.fetchval(
        "SELECT status FROM helper_runs WHERE id=(SELECT helper_run_id FROM handoffs WHERE id=$1)",
        handoff_id,
    ) == "done"
    assert await actions.pool.fetchval(
        "SELECT resolved_at IS NOT NULL FROM handoffs WHERE id=$1", handoff_id
    ) is True
    title = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name='title'", chan
    )
    assert title == "DPRK News"
    # the scrape was recorded as ObservedData
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='ObservedData' "
        "AND canonical='tg-snapshot:dprk_news'"
    ) == 1
    # tray is now empty
    assert await tray(actions, case_id=case_id) == []


async def test_abandon_releases_claim(
    actions: Actions, redis_client: aioredis.Redis
) -> None:
    case_id = await _make_case(actions, {"max_human_handoffs": 5})
    await actions.create_or_find_object("TelegramChannel", "ch", "analyst:test", case_id)
    await run_cascade(_ctx(actions, redis_client))
    handoff_id = (await tray(actions, case_id=case_id))[0]["id"]

    await abandon(actions, handoff_id)
    assert await actions.pool.fetchval(
        "SELECT status FROM helper_runs WHERE id=(SELECT helper_run_id FROM handoffs WHERE id=$1)",
        handoff_id,
    ) == "abandoned"
    assert await tray(actions, case_id=case_id) == []
    with pytest.raises(HandoffError):  # can't re-open a resolved handoff
        await open_handoff(actions, handoff_id)


async def test_handoff_budget_exhaustion(
    actions: Actions, redis_client: aioredis.Redis
) -> None:
    case_id = await _make_case(actions, {"max_human_handoffs": 1})
    await actions.create_or_find_object("TelegramChannel", "a", "analyst:test", case_id)
    await actions.create_or_find_object("TelegramChannel", "b", "analyst:test", case_id)
    await run_cascade(_ctx(actions, redis_client))
    # only one handoff fit in the budget; the other channel was blocked
    assert len(await tray(actions, case_id=case_id)) == 1
    assert int(await redis_client.get(f"budget:{case_id}:handoffs")) == 0


async def test_tray_ordered_by_priority(
    actions: Actions, redis_client: aioredis.Redis
) -> None:
    case_id = await _make_case(actions, {"max_human_handoffs": 9})
    ledger = BudgetLedger(actions.pool, redis_client)
    # a seed (hop 0) and a deeper object (hop 2); closer-to-seed should sort first
    near = await actions.create_or_find_object("TelegramChannel", "near", "a", case_id)
    far = await actions.pool.fetchval(
        "INSERT INTO objects (type, canonical) VALUES ('TelegramChannel','far') RETURNING id"
    )
    await actions.pool.execute(
        "INSERT INTO case_objects (case_id, object_id, hop_distance) VALUES ($1,$2,2)",
        case_id, far,
    )
    await suspend(actions, ledger, TELEGRAM, far, case_id, url="u-far", challenge_kind=None)
    await suspend(actions, ledger, TELEGRAM, near, case_id, url="u-near", challenge_kind=None)

    items = await tray(actions, case_id=case_id)
    assert [i["url"] for i in items] == ["u-near", "u-far"]  # hop0 before hop2


async def test_challenge_mid_fetch_suspends(
    actions: Actions, redis_client: aioredis.Redis
) -> None:
    """An open-tier helper whose connector hits a wall suspends to a handoff
    (we never solve/evade) — proven without a gated manifest."""
    open_manifest = Manifest.model_validate(
        {
            "id": "telegram_channel_profile",  # reuse the parser; pretend open tier
            "name": "x",
            "consumes": {"type": "TelegramChannel"},
            "parser": "telegram_channel_profile",
            "origin": "t.me",
            "tier": "open",
            "rate": {"per_origin_rps": 1e9, "per_origin_concurrent": 1e9},
        }
    )

    async def walled(_: InputObject) -> dict:
        raise ChallengeDetected(
            Challenge(ChallengeKind.CLOUDFLARE, "interstitial"), url="https://t.me/s/x"
        )

    case_id = await _make_case(actions, {"max_human_handoffs": 5, "rate_credits": 5})
    obj = await actions.create_or_find_object("TelegramChannel", "x", "analyst:test", case_id)
    ctx = CascadeContext(
        actions=actions,
        limiter=RateLimiter(redis_client),
        ledger=BudgetLedger(actions.pool, redis_client),
        manifests={"telegram_channel_profile": open_manifest},
        connectors={"telegram_channel_profile": walled},
    )
    from src.orchestrator.cascade import dispatch

    outcome = await dispatch(ctx, open_manifest, obj, case_id)
    assert outcome == "suspended"
    item = (await tray(actions, case_id=case_id))[0]
    assert item["challenge_kind"] == "cloudflare"
    # the rate credit was refunded (a handoff credit was spent instead)
    assert int(await redis_client.get(f"budget:{case_id}:rate")) == 5
