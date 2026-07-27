"""The declarative action-binding write leg (ruling c5b184cd, thread d56e7073/#44) — a
composition row carries `{"_action": {...}}`, the generic renderer turns it into a button,
one click POSTs to /act, which dispatches through the closed ACTION_VERBS registry. Grounded
in the same write-safety /threads/triage and /desk/settle already established: authority
from the route (never the request body), a closed allowlist, the real verb keeps its own
guards.
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime

import httpx
import pytest_asyncio
from src.actions.core import Actions
from src.api.actions import ACTION_VERBS
from src.api.app import create_app
from src.orchestrator.capture import open_thread
from src.orchestrator.compositions import LIVE_DESK, save_composition

NOW = datetime(2026, 7, 27, tzinfo=UTC)


@pytest_asyncio.fixture
async def client(actions: Actions) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(actions.pool)
    app.state.pool = actions.pool
    app.state.manifests = {}
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


# --- ACTION_VERBS registry, direct (no route) ------------------------------------------

async def test_resolve_thread_action_resolves_a_real_thread(actions: Actions) -> None:
    tid = await open_thread(actions, "a debt", owner="operator", source="agent:me")
    out = await ACTION_VERBS["resolve_thread"](actions.pool, {"ref": str(tid)[:8]})
    assert out["ok"] is True
    status = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", tid)
    assert status == "resolved"


async def test_resolve_thread_action_hardcodes_the_operator_attribution(
    actions: Actions,
) -> None:
    """The load-bearing property: the caller's args can NEVER set who's acting."""
    tid = await open_thread(actions, "a debt", owner="operator", source="agent:me")
    await ACTION_VERBS["resolve_thread"](
        actions.pool, {"ref": str(tid)[:8], "source": "someone-else", "because": "done"})
    because = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='resolved_because' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1",
        tid)
    assert because == "done"  # the args this adapter DOES read still work...
    row = await actions.pool.fetchrow(
        "SELECT source_id FROM current_assertions WHERE object_id=$1 AND name='status' "
        "ORDER BY confidence DESC, observed_at DESC LIMIT 1", tid)
    assert row["source_id"] == "analyst:operator"  # ...but "source" in args never wins


async def test_settle_action_acks_a_real_message(actions: Actions) -> None:
    """Settled through the SAME desk_decisions Function that surfaced it in the first place
    — the honest end-to-end check, not a guess at ack_messages' own storage column."""
    from src.orchestrator.compositions import _fn_desk_decisions
    from src.orchestrator.mailbox import send_message

    sent = await send_message(actions.pool, from_agent="agent:x", from_project="osiris",
                              to_project="operator", body="needs your call",
                              desk_kind="decision")
    before = await _fn_desk_decisions(actions.pool, None, {})
    assert any(d["id"] == str(sent["id"]) for d in before)

    out = await ACTION_VERBS["settle"](actions.pool, {"ids": [sent["id"]]})
    assert out["settled"] == 1

    after = await _fn_desk_decisions(actions.pool, None, {})
    assert not any(d["id"] == str(sent["id"]) for d in after)


async def test_resolve_thread_action_refuses_cleanly_on_a_missing_ref(
    actions: Actions,
) -> None:
    out = await ACTION_VERBS["resolve_thread"](actions.pool, {})
    assert "error" in out


# --- POST /act, through the real ASGI route ---------------------------------------------

async def test_act_route_dispatches_a_known_action(
    actions: Actions, client: httpx.AsyncClient,
) -> None:
    tid = await open_thread(actions, "a debt via the route", owner="operator",
                            source="agent:me")
    r = await client.post("/act", json={"action": "resolve_thread",
                                        "args": {"ref": str(tid)[:8]}})
    assert r.status_code == 200 and r.json()["ok"] is True


async def test_act_route_refuses_an_unknown_action(
    client: httpx.AsyncClient,
) -> None:
    r = await client.post("/act", json={"action": "delete_everything", "args": {}})
    assert r.status_code == 200  # refuses honestly in the body, never a 500
    assert "error" in r.json() and "unknown action" in r.json()["error"]


# --- /live-desk, end to end: composition -> generic renderer -> a real button -----------

async def test_live_desk_page_renders_real_buttons(
    actions: Actions, client: httpx.AsyncClient,
) -> None:
    await save_composition(actions.pool, "live-desk", LIVE_DESK)
    await open_thread(actions, "operator must pick a direction", owner="operator",
                      source="agent:me")

    r = await client.get("/live-desk")
    assert r.status_code == 200
    assert "operator must pick a direction" in r.text
    assert 'data-action="resolve_thread"' in r.text
    assert "your clicks write" in r.text  # actions=True armed the page, honestly labeled

    partial = await client.get("/live-desk?partial=1")
    assert "<!doctype" not in partial.text.lower()
