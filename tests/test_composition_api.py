"""The composer's REST surface (P4/P5) — the human channel the shell drives, mirroring
the MCP authoring tools. Proves the generic renderer's contract: a composition runs over
REST and returns a Result whose `kind` (objects / values / rows / data) is what the JS
`renderResult` dispatches on. The console pages are now render modes of this one surface.
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest_asyncio
from src.actions.core import Actions
from src.api.app import create_app

NOW = datetime(2026, 6, 28, tzinfo=UTC)


@pytest_asyncio.fixture
async def client(actions: Actions) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(actions.pool)
    app.state.pool = actions.pool
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def _orgs(actions: Actions) -> None:
    for cik, sector in [("1", "ai"), ("2", "ai"), ("3", "bio")]:
        o = await actions.create_or_find_object("Organization", f"cik:{cik}", "edgar")
        await actions.assert_property(o, "name", f"Co {cik}", "edgar", NOW, 0.85)
        await actions.assert_property(o, "sector", sector, "edgar", NOW, 0.85)


async def test_save_list_run_objects(client: httpx.AsyncClient, actions: Actions) -> None:
    await _orgs(actions)
    # save a lens (a select op) via the composer's REST channel
    r = await client.post("/compositions", json={
        "name": "all-orgs", "spec": {"op": "select", "object_type": "Organization"}})
    assert r.status_code == 200
    # it appears in the list alongside its kind (the rail reads this)
    names = {c["name"]: c for c in (await client.get("/compositions")).json()}
    assert names["all-orgs"]["kind"] == "lens"
    # running it returns an OBJECTS result (the board render mode)
    res = (await client.post("/compositions/all-orgs/run", json={})).json()
    assert res["kind"] == "objects" and res["count"] == 3
    assert all("label" in it and "type" in it for it in res["items"])


async def test_run_rows_aggregate(client: httpx.AsyncClient, actions: Actions) -> None:
    await _orgs(actions)
    await client.post("/compositions", json={"name": "by-sector", "spec": {
        "op": "aggregate", "group_by": ["sector"], "metric": {"type": "count"},
        "from": {"op": "select", "object_type": "Organization"}}})
    res = (await client.post("/compositions/by-sector/run", json={})).json()
    assert res["kind"] == "rows"  # the table render mode
    by = {row["group"]["sector"]: row["metric"] for row in res["items"]}
    assert by == {"ai": 2, "bio": 1}


async def test_run_function_without_subject_returns_error(client: httpx.AsyncClient) -> None:
    """A Function composition needs a subject; the endpoint reports it (no 500) so the
    shell can prompt 'focus a subject first' instead of breaking."""
    await client.post("/compositions", json={
        "name": "ties", "spec": {"op": "function", "name": "coinvest"}})
    res = (await client.post("/compositions/ties/run", json={})).json()
    assert "error" in res and "subject" in res["error"]


async def test_watch_appears_as_a_composition(client: httpx.AsyncClient) -> None:
    """A watch saved via /subscriptions is the SAME primitive — it shows up in
    /compositions as kind='watch', so the composer rail lists lenses and watches together."""
    await client.post("/subscriptions", json={"name": "sec watch", "criteria": {
        "object_type": "Organization", "where": []}})
    comps = (await client.get("/compositions")).json()
    watch = next(c for c in comps if c["name"] == "sec watch")
    assert watch["kind"] == "watch"
    assert watch["spec"]["op"] == "select"  # a watch's spec is a runnable select


async def test_console_pages_are_render_mode_stubs() -> None:
    """object.html and watch.html are CUT — thin redirect stubs into the composer."""
    ui = Path(__file__).resolve().parent.parent / "src" / "ui" / "static"
    for page, marker in [("object.html", "?id="), ("watch.html", "?run=")]:
        text = (ui / page).read_text()
        assert "location.replace" in text and marker in text
    # the shared library + its styles exist (the renderer is a real library)
    assert (ui / "osiris.js").is_file() and (ui / "osiris.css").is_file()
