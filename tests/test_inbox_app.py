"""THE INBOX's wiring — ONE live-route test (test_api.py's own precedent: pure unit tests
can't catch whether the route is actually wired to the real pool/registry). Everything
else about the Inbox is covered by test_inbox_blocks.py (builders) and
test_inbox_catalog.py (rendering).

A circular import lived here since the inbox cutover: src.api.inbox.app imported
get_pool straight from src.api.app, while src.api.app (at module scope, via
`app = create_app()`) lazily imports src.api.inbox.app's own router. Importing
src.api.inbox.app FIRST, in a process where src.api.app has never been imported,
re-entered src.api.app's module execution, reached create_app(), and tried to import
src.api.inbox.app back while it was still mid-import (only partially initialized,
`router` not yet defined) -- raising an ImportError. The in-process import above (line
13, historically) never caught it, because by the time this test file's collection ran,
some OTHER test module had already imported src.api.app first, leaving it fully
initialized in sys.modules. Fixed by moving get_pool into its own dependency-free module
(src.api.deps) that both sides import instead of each other; test_inbox_app_imports_
standalone_in_a_fresh_process below is what would have caught the regression, since it
runs in a subprocess with neither module preloaded, mirroring the collect-this-file-
alone repro (`pytest tests/test_inbox_app.py` on its own)."""
from __future__ import annotations

import subprocess
import sys
from collections.abc import AsyncIterator

import httpx
import pytest_asyncio
from fastapi import FastAPI
from src.actions.core import Actions
from src.api.inbox.app import router


def test_inbox_app_imports_standalone_in_a_fresh_process() -> None:
    """`import src.api.inbox.app` with nothing preloaded, in a real subprocess so no
    module already sits in sys.modules from an earlier import in this same test run --
    the exact shape that let the circular import above hide behind import order."""
    result = subprocess.run(
        [sys.executable, "-c", "import src.api.inbox.app"],
        capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr


@pytest_asyncio.fixture
async def client(actions: Actions) -> AsyncIterator[httpx.AsyncClient]:
    app = FastAPI()
    app.state.pool = actions.pool
    app.include_router(router)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def test_inbox_shell_route_redirects_to_the_console(client: httpx.AsyncClient) -> None:
    """The standalone Inbox shell (ruling 0b3dd431) is dead — :8011's front door is /ui,
    the operator's own consolidation (2026-09-10); GET / just redirects there now."""
    r = await client.get("/", follow_redirects=False)
    assert r.status_code in (302, 307)
    assert r.headers["location"] == "/ui/"


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
