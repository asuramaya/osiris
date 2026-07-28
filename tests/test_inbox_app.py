"""THE INBOX's wiring — ONE live-route test (test_api.py's own precedent: pure unit tests
can't catch whether the route is actually wired to the real pool/registry). Everything
else about the Inbox is covered by test_inbox_blocks.py (builders) and
test_inbox_catalog.py (rendering)."""
from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest_asyncio
from fastapi import FastAPI
from src.actions.core import Actions
from src.api.inbox.app import router


@pytest_asyncio.fixture
async def client(actions: Actions) -> AsyncIterator[httpx.AsyncClient]:
    app = FastAPI()
    app.state.pool = actions.pool
    app.include_router(router)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def test_inbox_shell_route_renders_a_live_empty_desk(client: httpx.AsyncClient,
                                                           actions: Actions) -> None:
    from src.orchestrator.compositions import LIVE_DESK, save_composition

    await save_composition(actions.pool, "live-desk", LIVE_DESK)

    r = await client.get("/")
    assert r.status_code == 200
    assert "<!doctype html>" in r.text.lower()
    assert "Inbox clear." in r.text


async def test_inbox_action_route_dispatches_through_the_real_registry(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    from src.orchestrator.capture import open_thread

    tid = await open_thread(actions, "an owed obligation", owner="operator",
                            source="agent:me")
    short = str(tid)[:8]

    r = await client.post(f"/inbox/{short}/resolve_thread")
    assert r.status_code == 200
    assert r.json()["ok"] is True

    status = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", tid)
    assert status == "resolved"


async def test_inbox_action_route_refuses_an_unknown_action(
    client: httpx.AsyncClient,
) -> None:
    r = await client.post("/inbox/anything00/not-a-real-action")
    assert r.status_code == 200
    assert "unknown action" in r.json()["error"]


async def test_inbox_stream_route_returns_a_datastar_response() -> None:
    """A STRUCTURAL check, not a live SSE round-trip: driving the actual infinite
    generator through httpx's ASGITransport hangs waiting on Starlette's disconnect-
    listener task (confirmed live, no fix found — and no precedent anywhere in this
    suite for testing an SSE route that way; /console/stream isn't tested live either).
    is_disconnected() returning True immediately means the generator's body never runs
    (no DB call, no side effect) — this only proves the route is wired to build the right
    RESPONSE TYPE. Content correctness is test_inbox_blocks.py/test_inbox_catalog.py's job."""
    from datastar_py.fastapi import DatastarResponse
    from src.api.inbox.app import inbox_stream

    class _FakeRequest:
        async def is_disconnected(self) -> bool:
            return True

    resp = await inbox_stream(_FakeRequest())  # type: ignore[arg-type]
    assert isinstance(resp, DatastarResponse)
