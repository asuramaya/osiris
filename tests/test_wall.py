"""THE ONE WALL LAW (ruling 923c380f): the graded wall as a composition, the console's
triage verbs, and the shelf metadata — the 919-raw-rows briefing is dead."""
from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime

import httpx
import pytest_asyncio
from src.actions.core import Actions
from src.api.app import create_app
from src.orchestrator.capture import open_thread
from src.orchestrator.compositions import (
    list_compositions,
    run_composition,
    seed_default_compositions,
)

NOW = datetime(2026, 7, 11, tzinfo=UTC)


@pytest_asyncio.fixture
async def client(actions: Actions) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(actions.pool)
    app.state.pool = actions.pool
    app.state.manifests = {}
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def _project_with_threads(actions: Actions) -> None:
    proj = await actions.create_or_find_object("SoftwareProject", "repo:walltest", "session")
    await actions.assert_property(proj, "name", "walltest", "session", NOW, 0.9)
    await open_thread(actions, "a duty someone owes", repo="walltest",
                      kind="obligation", source="agent:me")
    await open_thread(actions, "an operator blocker", repo="walltest",
                      kind="obligation", owner="operator", source="agent:me")
    # a miner echo: DERIVED-only, old — off the wall, into the pile
    t = await actions.create_or_find_object("Thread", "thread:echo-old", "session-miner")
    await actions.assert_property(t, "summary", "an ancient mined commitment",
                                  "session-miner", NOW, 0.4, evidence_class="derived")
    await actions.assert_property(t, "status", "open", "session-miner", NOW, 0.4,
                                  evidence_class="derived")
    await actions.create_link(
        t, await actions.create_or_find_object("SoftwareProject", "repo:walltest", "session"),
        "in_repo", "session-miner", NOW, 0.4, evidence_class="derived")
    await actions.pool.execute(
        "UPDATE objects SET created_at = now() - interval '30 days' WHERE id=$1", t)


async def test_the_wall_lens_grades_a_project(actions: Actions) -> None:
    """Project-scoped: obligations ride (operator-owned last), the old echo collapses into
    a counted pile — the same law orient enforces, now as a composition."""
    await _project_with_threads(actions)
    await seed_default_compositions(actions.pool)
    proj = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='repo:walltest'")
    out = (await run_composition(actions.pool, "the-wall", proj))["items"]
    walls = [w["summary"] for w in out["wall"]]
    # the seeded lens is OPERATOR-EYED (args me=['operator']): his blockers are his own
    # moves and ride first; recency breaks the tie with the unowned duty
    assert set(walls) == {"an operator blocker", "a duty someone owes"}
    assert walls[0] == "an operator blocker"
    assert out["echo_pile"]["count"] == 1
    assert "ancient mined commitment" not in walls


async def test_the_fleet_briefing_shows_counts_never_the_scroll(actions: Actions) -> None:
    """Subject-less (the console's default briefing): a per-project rollup + top
    obligations — the 919-row raw select is gone from the briefing composition."""
    await _project_with_threads(actions)
    await seed_default_compositions(actions.pool)
    out = (await run_composition(actions.pool, "briefing"))["items"]
    wall = next(iter(out.values()))  # first section = the wall
    assert wall["totals"]["open"] == 3 and wall["totals"]["obligations"] == 2
    assert wall["totals"]["pile"] == 1
    proj_row = next(p for p in wall["projects"] if p["project"] == "repo:walltest")
    assert proj_row["pile"] == 1 and proj_row["obligations"] == 2
    tops = [t["summary"] for t in wall["top_of_wall"]]
    assert "a duty someone owes" in tops
    # never a raw thread scroll: the section is a dict of counts, not 900 rows
    assert "wall" not in out or isinstance(wall, dict)


async def test_triage_route_writes_as_the_operator(
        actions: Actions, client: httpx.AsyncClient) -> None:
    """The console's triage verbs (the operator's ruling): resolve closes through the
    Actions waist signed analyst:operator; reclassify adopts an echo as owed work; a
    bogus ref reports a miss instead of failing the batch."""
    await _project_with_threads(actions)
    echo_id = str(await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='thread:echo-old'"))
    r = await client.post("/threads/triage", json={
        "ids": [echo_id, "zzzz-nothing"], "verb": "resolve",
        "because": "operator triage: done weeks ago"})
    body = r.json()
    assert body["acted"] == 1 and body["missed"] == 1
    src = await actions.pool.fetchval(
        "SELECT a.source_id FROM assertions a WHERE a.object_id=$1::uuid "
        "AND a.name='status' AND a.value #>> '{}' = 'resolved' "
        "ORDER BY a.created_at DESC LIMIT 1", echo_id)
    assert src == "analyst:operator"
    # reclassify lane: adopt a thread as an obligation without touching status
    duty = await actions.pool.fetchval(
        "SELECT o.id::text FROM objects o JOIN current_assertions a ON a.object_id=o.id "
        "WHERE a.name='summary' AND a.value #>> '{}' = 'a duty someone owes'")
    r2 = await client.post("/threads/triage", json={
        "ids": [duty[:8]], "verb": "question", "because": "not work after all"})
    assert r2.json()["acted"] == 1
    # bad verb refused
    r3 = await client.post("/threads/triage", json={"ids": [duty], "verb": "delete"})
    assert "error" in r3.json()


async def test_the_shelf_metadata_reaches_the_list(actions: Actions) -> None:
    """Compositions say what they are: section + description ride /compositions, and the
    seeder stamps known names — the 19-flat-chips sidebar has what it needs to group."""
    await seed_default_compositions(actions.pool)
    comps = {c["name"]: c for c in await list_compositions(actions.pool)}
    assert comps["briefing"]["section"] == "arrive"
    assert comps["the-wall"]["section"] == "wall"
    assert "GENUINELY unresolved" in comps["the-wall"]["description"]
    assert comps["graph-lint"]["section"] == "engine"
    assert comps["co-investment-ties"]["section"] == "casework"
