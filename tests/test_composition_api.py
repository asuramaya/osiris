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
    # running it returns an OBJECTS result, each item carrying label/type/canonical AND its
    # compact properties — the Table view (W1) reads `props` for its columns (sector here)
    res = (await client.post("/compositions/all-orgs/run", json={})).json()
    assert res["kind"] == "objects" and res["count"] == 3
    assert all({"label", "type", "canonical", "props"} <= set(it) for it in res["items"])
    assert {it["props"]["sector"] for it in res["items"]} == {"ai", "bio"}


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


async def test_watermark_endpoint_returns_the_four_markers_and_moves_on_a_write(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    """ruling cf9286b2's whole poll target — osiris.js fetches this, never a composition,
    to decide whether to re-run one. Full behavioral coverage of graph_watermark lives in
    test_watermark.py; this just proves the REST route (the thing the browser actually
    calls) wires through to it correctly."""
    before = (await client.get("/watermark")).json()
    assert before == {"audit_log": None, "fleet_messages": None, "agent_mounts": None,
                      "agent_wakes": None}
    await actions.create_or_find_object("Thread", "thread:wmapi1", "test")
    after = (await client.get("/watermark")).json()
    assert after["audit_log"] is not None and after["audit_log"] != before["audit_log"]


async def test_compositions_list_surfaces_refresh_secs_over_rest(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    """The sidebar's own source of truth for whether/how often to poll a lens — read once
    when GET /compositions loads, per composition, not re-fetched on every run."""
    from src.orchestrator.compositions import save_composition

    await save_composition(actions.pool, "wm-rest", {"op": "select"}, refresh_secs=12)
    comps = {c["name"]: c for c in (await client.get("/compositions")).json()}
    assert comps["wm-rest"]["refresh_secs"] == 12


async def test_related_pivot_returns_a_result_set(
    client: httpx.AsyncClient, actions: Actions
) -> None:
    """W3: a relationship group opens as a SET. /related?type=&direction= returns the
    typed neighbours as enriched object items (table-ready) — the pivot behind 'open as set'."""
    dev = await actions.create_or_find_object("Person", "dev:x@y.z", "git")
    await actions.assert_property(dev, "name", "Dev X", "git", NOW, 0.85)
    for sha in ["c1", "c2", "c3"]:
        c = await actions.create_or_find_object("Commit", f"commit:{sha}", "git")
        await actions.assert_property(c, "subject", f"did {sha}", "git", NOW, 0.85)
        await actions.create_link(c, dev, "authored_by", "git", NOW, 0.85)  # commit -> dev (in)
    res = (await client.get(f"/objects/{dev}/related",
                            params={"type": "authored_by", "direction": "in"})).json()
    assert res["kind"] == "objects" and res["count"] == 3
    assert {it["canonical"] for it in res["items"]} == {"commit:c1", "commit:c2", "commit:c3"}
    assert all("subject" in it["props"] for it in res["items"])  # table columns available
    # the other direction is empty (the dev didn't author anything outbound)
    out = (await client.get(f"/objects/{dev}/related", params={"direction": "out"})).json()
    assert out["count"] == 0


async def test_run_spec_is_ephemeral_and_echoes_spec(
    client: httpx.AsyncClient, actions: Actions
) -> None:
    """W4: the inline composer runs an EPHEMERAL working spec (no save) and echoes it back
    so the lineage breadcrumb + chips can re-render. Adding a where filters in place."""
    await _orgs(actions)  # 2 ai + 1 bio
    base = {"op": "select", "object_type": "Organization"}
    res = (await client.post("/compositions/run-spec", json={"spec": base})).json()
    assert res["count"] == 3 and res["spec"] == base  # spec echoed for the breadcrumb
    # add a filter chip → re-run the working spec → fewer
    filtered = {"op": "select", "object_type": "Organization",
                "where": [{"property": "sector", "op": "eq", "value": "ai"}]}
    res2 = (await client.post("/compositions/run-spec", json={"spec": filtered})).json()
    assert res2["count"] == 2  # only the ai orgs
    # nothing was saved (ephemeral) — the working spec never hit the compositions table
    assert "(working" in res2["composition"] or res2["composition"] == "(spec)"
    assert not any(c["spec"] == filtered for c in (await client.get("/compositions")).json())


async def test_rooms_scope_artifacts_not_the_graph(
    client: httpx.AsyncClient, actions: Actions
) -> None:
    """W2: a Room scopes the WORK (cases + compositions) to a stance, but never the graph.
    Switching rooms re-scopes /compositions and /cases; the objects stay global."""
    eng = (await client.post("/rooms", json={"name": "engineer"})).json()["id"]
    jour = (await client.post("/rooms", json={"name": "journalist"})).json()["id"]
    # a composition + a case in each stance
    await client.post("/compositions", json={
        "name": "commits", "spec": {"op": "select", "object_type": "Commit"}, "room_id": eng})
    await client.post("/compositions", json={
        "name": "screen", "spec": {"op": "function", "name": "screen_network"}, "room_id": jour})
    await client.post("/cases", json={"name": "self-track", "room_id": eng})
    # a GLOBAL object exists regardless of room
    await actions.create_or_find_object("Commit", "commit:z", "git")

    # /compositions scopes to the stance
    eng_comps = {c["name"] for c in (await client.get(f"/compositions?room={eng}")).json()}
    assert eng_comps == {"commits"}  # not the journalist's
    jour_comps = {c["name"] for c in (await client.get(f"/compositions?room={jour}")).json()}
    assert jour_comps == {"screen"}
    # the All view (no room) sees both
    assert {"commits", "screen"} <= {c["name"] for c in (await client.get("/compositions")).json()}

    # /cases scopes too; /rooms carries the counts
    assert [c["name"] for c in (await client.get(f"/cases?room={eng}")).json()] == ["self-track"]
    assert (await client.get(f"/cases?room={jour}")).json() == []
    rooms = {r["name"]: r for r in (await client.get("/rooms")).json()}
    assert rooms["engineer"]["compositions"] == 1 and rooms["engineer"]["cases"] == 1

    # the GRAPH is never room-scoped: a global search sees the object from any stance
    found = (await client.get("/objects", params={"q": "commit:z"})).json()
    assert any(o["canonical"] == "commit:z" for o in found)


async def test_claude_authors_a_room_from_a_sentence(actions: Actions) -> None:
    """W5: the FDE move — create_room + save_composition(room=) is all Claude needs to mint
    a stance from a sentence ('set up a compliance desk'). resolve_room takes name or id."""
    from src.orchestrator.compositions import (
        create_room,
        list_compositions,
        list_rooms,
        resolve_room,
        save_composition,
    )
    rid = await create_room(actions.pool, "compliance")
    assert await resolve_room(actions.pool, "compliance") == rid  # by name
    assert await resolve_room(actions.pool, str(rid)) == rid       # by id
    assert await resolve_room(actions.pool, None) is None          # the All scope
    # stock the desk, scoped to the room
    await save_composition(actions.pool, "new sanctioned wallets",
                           {"op": "select", "object_type": "CryptoAddress"}, "watch", room_id=rid)
    scoped = [c["name"] for c in await list_compositions(actions.pool, rid)]
    assert scoped == ["new sanctioned wallets"]
    assert "compliance" in {r["name"] for r in await list_rooms(actions.pool)}


async def test_console_pages_are_render_mode_stubs() -> None:
    """object.html and watch.html are CUT — thin redirect stubs into the composer."""
    ui = Path(__file__).resolve().parent.parent / "src" / "ui" / "static"
    for page, marker in [("object.html", "?id="), ("watch.html", "?run=")]:
        text = (ui / page).read_text()
        assert "location.replace" in text and marker in text
    # the shared library + its styles exist (the renderer is a real library)
    assert (ui / "osiris.js").is_file() and (ui / "osiris.css").is_file()
