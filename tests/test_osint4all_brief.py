from __future__ import annotations

import uuid
from datetime import UTC, datetime

import redis.asyncio as aioredis
from src.actions.core import Actions
from src.connectors.osint4all import import_startme, suggest_manifests
from src.dissemination.brief import build_case_brief
from src.orchestrator.budgets import BudgetLedger
from src.orchestrator.cascade import CascadeContext, run_cascade
from src.orchestrator.manifests import project_triggers
from src.orchestrator.ratelimit import RateLimiter

NOW = datetime(2026, 5, 28, tzinfo=UTC)


def test_suggest_manifests_generated() -> None:
    ms = suggest_manifests()
    assert all(m.tier == "suggest" for m in ms.values())
    email = [m for m in ms.values() if m.consumes.type == "Email"]
    assert email and all(m.template and m.template.url for m in email)


def test_import_startme_board() -> None:
    board = {"groups": [
        {"title": "Email tools",
         "bookmarks": [{"title": "X", "url": "https://x/{object.canonical}"}]},
        {"title": "Random", "bookmarks": [{"title": "Y", "url": "https://y"}]},  # unmapped->skip
    ]}
    ms = import_startme(board)
    assert any(m.consumes.type == "Email" for m in ms.values())
    assert all(m.consumes.type != "Random" for m in ms.values())


async def test_osint4all_suggest_fills_tray_on_expand(
    actions: Actions, redis_client: aioredis.Redis
) -> None:
    """Paste an email -> Expand -> osint4all sources land in the handoff tray."""
    manifests = suggest_manifests()
    await project_triggers(actions.pool, manifests)
    cid = uuid.UUID(str(await actions.pool.fetchval(
        "INSERT INTO cases (name, owner, budgets) VALUES ('o','a',$1) RETURNING id",
        {"max_human_handoffs": 50, "rate_credits": 50},
    )))
    await actions.create_or_find_object("Email", "kim@dprk.example", "a", cid)

    ctx = CascadeContext(
        actions=actions, limiter=RateLimiter(redis_client),
        ledger=BudgetLedger(actions.pool, redis_client),
        manifests=manifests, connectors={},
    )
    await run_cascade(ctx)
    # the Email suggest sources are now awaiting the analyst, with rendered URLs
    rows = await actions.pool.fetch(
        "SELECT url FROM handoffs h JOIN helper_runs r ON r.id=h.helper_run_id "
        "WHERE r.status='awaiting_human'"
    )
    assert len(rows) >= 2
    assert any("kim@dprk.example" in (r["url"] or "") for r in rows)


async def test_pdf_brief(actions: Actions) -> None:
    cid = uuid.UUID(str(await actions.pool.fetchval(
        "INSERT INTO cases (name, owner) VALUES ('Lazarus brief','a') RETURNING id"
    )))
    laz = await actions.create_or_find_object("IntrusionSet", "is--laz", "a", cid)
    await actions.assert_property(laz, "name", "Lazarus Group", "a", NOW, 1.0, case_id=cid)
    await actions.assert_property(laz, "external_id", "G0032", "a", NOW, 1.0, case_id=cid)

    pdf = await build_case_brief(actions.pool, cid, generated_at=NOW)
    assert pdf[:5] == b"%PDF-"   # a real PDF
    assert len(pdf) > 1500
