from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import redis.asyncio as aioredis
from src.actions.core import Actions
from src.orchestrator.budgets import BudgetLedger
from src.orchestrator.cascade import CascadeContext
from src.orchestrator.hygiene import archive_stale_patterns, promote_campaigns
from src.orchestrator.manifests import load_manifests
from src.orchestrator.ratelimit import RateLimiter
from src.orchestrator.windows import tick
from src.parsers.base import InputObject

HELPERS = Path(__file__).parent.parent / "helpers"
TGSTAT = load_manifests(HELPERS)["tgstat_channel_behavior"]
NOW = datetime(2026, 5, 28, 12, 0, tzinfo=UTC)


def _conn_factory(confidences: dict[str, float] | None = None):
    async def connector(inp: InputObject, start: datetime, end: datetime) -> dict:
        key = start.date().isoformat()
        return {
            "window_start": key,
            "post_count": 10,
            "forward_count": 2,
            "campaign_confidence": (confidences or {}).get(key, 0.4),
        }
    return connector


def _ctx(actions: Actions, redis: aioredis.Redis) -> CascadeContext:
    return CascadeContext(
        actions=actions,
        limiter=RateLimiter(redis),
        ledger=BudgetLedger(actions.pool, redis),
        manifests={"tgstat_channel_behavior": TGSTAT},
        connectors={},
    )


async def _case(actions: Actions, budgets: dict) -> uuid.UUID:
    cid = await actions.pool.fetchval(
        "INSERT INTO cases (name, owner, budgets) VALUES ('w','analyst:test',$1) RETURNING id",
        budgets,
    )
    return uuid.UUID(str(cid))


async def test_windowed_append_evidence_supersede_judgment(
    actions: Actions, redis_client: aioredis.Redis, monkeypatch, tmp_path
) -> None:
    monkeypatch.setenv("OSIRIS_ARTIFACT_DIR", str(tmp_path))
    cid = await _case(actions, {"rate_credits": 100})
    await actions.create_or_find_object("TelegramChannel", "dprk_news", "analyst:test", cid)

    # confidence rises across the last two days, so the rolling judgment should
    # end at the latest window's value
    confidences = {"2026-05-27": 0.6, "2026-05-28": 0.8}
    result = await tick(
        _ctx(actions, redis_client),
        {"tgstat_channel_behavior": _conn_factory(confidences)},
        now=NOW,
    )
    # ~30 daily windows backfilled (lookback 30d)
    assert 29 <= result["runs"] <= 31

    # APPEND: one ObservedData object per window
    obs = await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='ObservedData' "
        "AND canonical LIKE 'tgstat-window:dprk_news:%'"
    )
    assert obs == result["runs"]

    # SUPERSEDE: a single Campaign, current behavior_confidence = latest window
    campaign = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='campaign:tgstat:dprk_news'"
    )
    current = await actions.pool.fetch(
        "SELECT value #>> '{}' AS v FROM current_assertions "
        "WHERE object_id=$1 AND name='behavior_confidence'",
        campaign,
    )
    assert [r["v"] for r in current] == ["0.8"]  # only the latest assessment is current


async def test_window_tick_is_idempotent(
    actions: Actions, redis_client: aioredis.Redis, monkeypatch, tmp_path
) -> None:
    monkeypatch.setenv("OSIRIS_ARTIFACT_DIR", str(tmp_path))
    cid = await _case(actions, {"rate_credits": 100})
    await actions.create_or_find_object("TelegramChannel", "ch", "analyst:test", cid)
    conns = {"tgstat_channel_behavior": _conn_factory()}

    first = await tick(_ctx(actions, redis_client), conns, now=NOW)
    # same `now` -> no new buckets are due
    second = await tick(_ctx(actions, redis_client), conns, now=NOW)
    assert second["runs"] == 0
    # one slide later -> exactly one new window
    third = await tick(_ctx(actions, redis_client), conns, now=NOW + timedelta(days=1))
    assert third["runs"] == 1
    assert first["runs"] >= 29


async def test_budget_only_termination(
    actions: Actions, redis_client: aioredis.Redis, monkeypatch, tmp_path
) -> None:
    monkeypatch.setenv("OSIRIS_ARTIFACT_DIR", str(tmp_path))
    cid = await _case(actions, {"rate_credits": 5})  # only 5 windows affordable
    await actions.create_or_find_object("TelegramChannel", "ch", "analyst:test", cid)

    result = await tick(
        _ctx(actions, redis_client), {"tgstat_channel_behavior": _conn_factory()}, now=NOW
    )
    assert result["runs"] == 5
    assert result["budget_blocked"] == 1
    assert int(await redis_client.get(f"budget:{cid}:rate")) == 0


async def test_archived_case_is_skipped(
    actions: Actions, redis_client: aioredis.Redis, monkeypatch, tmp_path
) -> None:
    monkeypatch.setenv("OSIRIS_ARTIFACT_DIR", str(tmp_path))
    cid = await _case(actions, {"rate_credits": 100})
    await actions.create_or_find_object("TelegramChannel", "ch", "analyst:test", cid)
    await actions.pool.execute("UPDATE cases SET archived_at=now() WHERE id=$1", cid)

    result = await tick(
        _ctx(actions, redis_client), {"tgstat_channel_behavior": _conn_factory()}, now=NOW
    )
    assert result["runs"] == 0


async def test_hygiene_promote_and_archive(
    actions: Actions, redis_client: aioredis.Redis, monkeypatch, tmp_path
) -> None:
    monkeypatch.setenv("OSIRIS_ARTIFACT_DIR", str(tmp_path))
    cid = await _case(actions, {"rate_credits": 100})
    await actions.create_or_find_object("TelegramChannel", "ch", "analyst:test", cid)
    await tick(_ctx(actions, redis_client), {"tgstat_channel_behavior": _conn_factory()}, now=NOW)

    # the Campaign has ~30 ObservedData edges -> promotes past the threshold
    promoted = await promote_campaigns(actions, min_observed=3)
    assert promoted == 1
    assert await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions c JOIN objects o ON o.id=c.object_id "
        "WHERE o.canonical='campaign:tgstat:ch' AND c.name='lifecycle'"
    ) == "published"
    # idempotent: already published, not re-promoted
    assert await promote_campaigns(actions, min_observed=3) == 0

    # newest observation is from NOW; with a 1-day max age (relative to far future) it's stale
    archived = await archive_stale_patterns(
        actions, now=NOW + timedelta(days=400), max_age=timedelta(days=1)
    )
    assert archived == 1
    assert await actions.pool.fetchval(
        "SELECT status FROM objects WHERE canonical='campaign:tgstat:ch'"
    ) == "archived"
