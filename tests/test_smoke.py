"""smoke — the deploy-time liveness check (ruling 2ee43411, task #63, threads bb763977 and
1849d800). The core probes are fully testable (a real pool, a real ASGI-served chrome app);
the CLI's own network glue (scripts/osiris_smoke.py's MCP round-trip, the operator brief) is
live-environment-dependent by nature and untested here, same precedent as
scripts/osiris_preflight.py leaving its own collectors untested while `evaluate()` — the pure
judgment layer, `_fails_from`'s own sibling here — is thoroughly covered.
"""
from __future__ import annotations

import httpx
import pytest_asyncio
from src.actions.core import Actions
from src.api.app import create_app
from src.orchestrator.smoke import CHROME_ROUTES, smoke, smoke_chrome, smoke_pool


@pytest_asyncio.fixture
async def client(actions: Actions) -> httpx.AsyncClient:
    app = create_app(actions.pool)
    app.state.pool = actions.pool
    app.state.manifests = {}
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


class _RaisingPool:
    """A stand-in that fails exactly where smoke_pool touches a real pool — no live broken
    Postgres connection needed to prove the error path reports honestly."""

    async def fetchval(self, *args: object, **kwargs: object) -> None:
        raise ConnectionError("simulated: pool bound to a dead event loop")


# --- smoke_chrome: every named route, against the real ASGI app, no compositions seeded ----

async def test_smoke_chrome_walks_every_named_route(client: httpx.AsyncClient) -> None:
    """Every route named in thread bb763977, on a BLANK DB — /roadmap, /canon, /live-desk all
    degrade to an honest 'no composition' line rather than crashing (their own docstrings say
    so); the smoke walk only cares that the surface answered, not what it said."""
    out = await smoke_chrome(client)
    assert set(out) == set(CHROME_ROUTES)
    assert all(v == "ok" for v in out.values()), out


async def test_smoke_chrome_names_a_real_failure(client: httpx.AsyncClient) -> None:
    out = await smoke_chrome(client, routes=("/desk", "/not-a-real-route-at-all"))
    assert out["/desk"] == "ok"
    assert out["/not-a-real-route-at-all"] == "http 404"


# --- smoke_pool: one real query, honest on failure ------------------------------------------

async def test_smoke_pool_is_ok_on_a_real_pool(actions: Actions) -> None:
    assert await smoke_pool(actions.pool) == "ok"


async def test_smoke_pool_reports_the_real_error_not_a_crash() -> None:
    assert (await smoke_pool(_RaisingPool())).startswith("error: ")


# --- smoke(): the composed report -----------------------------------------------------------

async def test_smoke_is_ok_when_both_probes_are_green(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    res = await smoke(client, actions.pool)
    assert res["ok"] is True
    assert res["db"] == "ok"
    assert all(v == "ok" for v in res["chrome"].values())


async def test_smoke_is_not_ok_when_the_pool_fails(client: httpx.AsyncClient) -> None:
    res = await smoke(client, _RaisingPool())
    assert res["ok"] is False
    assert res["db"].startswith("error: ")
    assert all(v == "ok" for v in res["chrome"].values())  # chrome's own verdict stays honest


# --- the MCP tool wrapper: srv._pool swap, mirroring test_describe.py's own pattern --------

async def test_the_mcp_tool_wrapper_delegates_to_smoke(actions: Actions) -> None:
    """Proves the ACTUAL MCP verb wires `_pool_get()` correctly (db="ok" with the swapped
    real test pool) and hits every named chrome route via a real httpx client pointed at
    `settings.osiris_console_base_url` — deliberately NOT asserting chrome's own up/down
    verdict, since whether anything is actually listening on that port is real environment
    state outside this test's control (this box happens to run a live console); only that
    the wrapper composes and reports honestly, never crashes, regardless."""
    from src import mcp_server as srv

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.smoke()
    finally:
        srv._pool = saved_pool
    assert out["db"] == "ok"
    assert set(out["chrome"]) == set(CHROME_ROUTES)
    assert out["ok"] == (out["db"] == "ok" and all(v == "ok" for v in out["chrome"].values()))
