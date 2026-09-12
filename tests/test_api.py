from __future__ import annotations

import json
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest_asyncio
from src.actions.core import Actions
from src.api.app import create_app
from src.ontology.ingest import ingest_bundle

DPRK_BUNDLE = Path(__file__).parent / "fixtures" / "dprk_attack_bundle.json"
HELPERS = Path(__file__).parent.parent / "helpers"
LAZARUS = "intrusion-set--c93fccb1-e8e8-42cf-ae33-2ad1d183913a"
APPLEJEUS = "malware--6a0ef5d4-fc7c-4dda-85d7-592e4dbdc5d9"


@pytest_asyncio.fixture
async def client(actions: Actions) -> AsyncIterator[httpx.AsyncClient]:
    from src.orchestrator.manifests import load_manifests

    app = create_app(actions.pool)
    app.state.pool = actions.pool
    app.state.manifests = load_manifests(HELPERS)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def _seed(actions: Actions) -> uuid.UUID:
    cid = await actions.pool.fetchval(
        "INSERT INTO cases (name, owner) VALUES ('api','analyst:test') RETURNING id"
    )
    await ingest_bundle(actions, json.loads(DPRK_BUNDLE.read_text()), case_id=uuid.UUID(str(cid)))
    return uuid.UUID(str(cid))


async def test_health(client: httpx.AsyncClient) -> None:
    r = await client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


async def test_membrane_route_is_retired(client: httpx.AsyncClient) -> None:
    """THE INBOX (task #71, ruling 0b3dd431) replaced /membrane as :8011's front door —
    locking in the retirement as intentional, not an accidental regression. src/api/
    membrane.py itself is gone now too (task #92's residual, thread 0aa9debf7c04) — its
    three still-live names (_CSS/_age/_e) folded into chrome.py, which was already their
    only caller."""
    r = await client.get("/membrane")
    assert r.status_code == 404


async def test_inbox_route_wires_the_real_app_live(client: httpx.AsyncClient) -> None:
    """The one live-route test for THE INBOX at the real create_app() level (pure
    builder/render coverage lives in test_inbox_blocks.py/test_inbox_catalog.py/
    test_inbox_app.py) — this is the wiring check those can't cover on their own: the
    router is actually include_router()'d (GET / redirects to /ui, the operator's own
    front-door consolidation, 2026-09-10), and the static mount still serves the
    vendored assets, from the SAME app real deploys boot."""
    r = await client.get("/", follow_redirects=False)
    assert r.status_code in (302, 307)
    assert r.headers["location"] == "/ui/"
    css = await client.get("/static/app.css")
    assert css.status_code == 200
    js = await client.get("/static/datastar.js")
    assert js.status_code == 200


async def test_list_and_get_object(client: httpx.AsyncClient, actions: Actions) -> None:
    await _seed(actions)
    r = await client.get("/objects", params={"type": "IntrusionSet"})
    assert r.status_code == 200
    canonicals = {o["canonical"] for o in r.json()}
    assert LAZARUS in canonicals

    oid = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical=$1", LAZARUS)
    r = await client.get(f"/objects/{oid}")
    assert r.status_code == 200
    body = r.json()
    assert body["type"] == "IntrusionSet"
    names = {p["value"] for p in body["properties"] if p["name"] == "name"}
    assert "Lazarus Group" in names


async def test_get_object_resolves_name_via_the_full_chain(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    """Task #97 workstream 3 (client half): osiris.js's objectDetail rendered the
    inspector's title from a raw `name`-property scan — a Practice (statement/
    failure_prevented/surface, no name) showed its canonical hash there even though
    every list view of the SAME object already resolved it correctly. GET /objects/{id}
    now carries a top-level `name` via resolve_label, same as /objects and the dossier."""
    p = await actions.create_or_find_object("Practice", "practice:gettest", "test")
    await actions.assert_property(p, "statement", "measure it yourself, not from memory",
                                  "test", datetime.now(UTC), 0.9)
    r = await client.get(f"/objects/{p}")
    assert r.status_code == 200
    assert r.json()["name"] == "measure it yourself, not from memory"


async def test_objects_search_is_word_order_proof(
    client: httpx.AsyncClient, actions: Actions
) -> None:
    from src.orchestrator.capture import record_decision

    # summary carries some tokens, rationale others — so a query can span both properties
    d = await record_decision(
        actions, "the atomic claim uses a partial unique index",
        rationale="idempotent retry-safe dedup on active statuses",
    )

    async def ids(q: str) -> set[str]:
        r = await client.get("/objects", params={"q": q})
        assert r.status_code == 200
        return {o["id"] for o in r.json()}

    assert str(d) in await ids("claim atomic")        # reordered, both in summary
    assert str(d) in await ids("idempotent claim")    # cross-property AND (rationale + summary)
    assert str(d) in await ids("index atomic dedup")  # non-adjacent, both properties
    assert str(d) not in await ids("claim nonexistenttoken")  # every token must hit
    assert str(d) in await ids("idempotent")          # single-token behaviour unchanged


async def test_objects_list_resolves_labels_via_the_full_chain_and_disambiguates(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    """Task #97 workstream 3: /objects' own `name` used to be a raw SQL COALESCE
    (name/title/summary/subject/canonical only) — a Practice fell straight to its
    canonical hash. Now resolve_label + disambiguate_labels, same as every other
    consumer, with a `display_label` field alongside."""
    p = await actions.create_or_find_object("Practice", "practice:apitest", "test")
    await actions.assert_property(p, "statement", "an api-level practice test", "test",
                                  datetime.now(UTC), 0.9)
    r = await client.get("/objects", params={"type": "Practice"})
    assert r.status_code == 200
    row = next(o for o in r.json() if o["canonical"] == "practice:apitest")
    assert row["name"] == "an api-level practice test"
    assert row["display_label"]


async def test_object_edge_counts_batches_by_id(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    """GRAPH VISUALIZER wave A item 7 (thread 8839): Browse's own edge-count badge, as a
    batched lookup separate from the (already complex) /objects listing query."""
    hub = await actions.create_or_find_object("Thread", "thread:gv-item7-hub", "test")
    leaf1 = await actions.create_or_find_object("Agent", "agent:gv-item7-leaf1", "test")
    leaf2 = await actions.create_or_find_object("Agent", "agent:gv-item7-leaf2", "test")
    lonely = await actions.create_or_find_object("Thread", "thread:gv-item7-lonely", "test")
    await actions.create_link(hub, leaf1, "closed_by", "test", datetime.now(UTC), 1.0)
    await actions.create_link(hub, leaf2, "closed_by", "test", datetime.now(UTC), 1.0)

    r = await client.get("/objects/edge_counts", params={"ids": f"{hub},{lonely},not-a-uuid,"})
    body = r.json()
    assert body[str(hub)] == 2
    assert str(lonely) not in body  # zero-edge objects carry no row -- absence IS zero
    assert body == {str(hub): 2}


async def test_object_edge_counts_is_empty_for_no_ids(client: httpx.AsyncClient) -> None:
    r = await client.get("/objects/edge_counts", params={"ids": ""})
    assert r.json() == {}


async def test_object_viewport_returns_only_nodes_inside_the_bounding_box(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    """GRAPH VISUALIZER wave B item 2 (thread 8839): the full-view renderer's own pull,
    read straight off graph_x/graph_y (wave B item 1's positions)."""
    inside = await actions.create_or_find_object("Thread", "thread:vp-inside", "test")
    outside = await actions.create_or_find_object("Thread", "thread:vp-outside", "test")
    await actions.assert_property(inside, "graph_x", 10.0, "test", datetime.now(UTC), 1.0)
    await actions.assert_property(inside, "graph_y", 10.0, "test", datetime.now(UTC), 1.0)
    await actions.assert_property(outside, "graph_x", 9000.0, "test", datetime.now(UTC), 1.0)
    await actions.assert_property(outside, "graph_y", 9000.0, "test", datetime.now(UTC), 1.0)

    r = await client.get("/objects/viewport",
                         params={"minx": 0, "maxx": 100, "miny": 0, "maxy": 100})
    ids = {n["id"] for n in r.json()["nodes"]}
    assert str(inside) in ids
    assert str(outside) not in ids


async def test_object_viewport_edges_only_among_the_returned_nodes(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    a = await actions.create_or_find_object("Thread", "thread:vp-a", "test")
    b = await actions.create_or_find_object("Thread", "thread:vp-b", "test")
    c = await actions.create_or_find_object("Thread", "thread:vp-c", "test")
    for oid, x in ((a, 5.0), (b, 6.0)):
        await actions.assert_property(oid, "graph_x", x, "test", datetime.now(UTC), 1.0)
        await actions.assert_property(oid, "graph_y", 5.0, "test", datetime.now(UTC), 1.0)
    await actions.assert_property(c, "graph_x", 9000.0, "test", datetime.now(UTC), 1.0)
    await actions.assert_property(c, "graph_y", 9000.0, "test", datetime.now(UTC), 1.0)
    await actions.create_link(a, b, "cites", "test", datetime.now(UTC), 1.0)
    await actions.create_link(a, c, "cites", "test", datetime.now(UTC), 1.0)

    r = await client.get("/objects/viewport",
                         params={"minx": 0, "maxx": 100, "miny": 0, "maxy": 100})
    body = r.json()
    edge_types = [(e["source"], e["target"]) for e in body["edges"]]
    assert (str(a), str(b)) in edge_types
    assert not any(str(c) in pair for pair in edge_types)


async def test_object_viewport_exclude_skips_ids_the_caller_already_holds(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    a = await actions.create_or_find_object("Thread", "thread:vp-excl-a", "test")
    b = await actions.create_or_find_object("Thread", "thread:vp-excl-b", "test")
    for oid in (a, b):
        await actions.assert_property(oid, "graph_x", 1.0, "test", datetime.now(UTC), 1.0)
        await actions.assert_property(oid, "graph_y", 1.0, "test", datetime.now(UTC), 1.0)

    r = await client.get("/objects/viewport", params={
        "minx": 0, "maxx": 100, "miny": 0, "maxy": 100, "exclude": str(a)})
    ids = {n["id"] for n in r.json()["nodes"]}
    assert str(a) not in ids
    assert str(b) in ids


async def test_graph_supernodes_counts_members_per_project(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    """GRAPH VISUALIZER wave B item 3 (thread 8839): LOD level 0 -- one supernode per
    project, sized by count. A project MEMBER always carries its own in_repo edge, so it
    can never itself read as "orphan" (zero live links, graph_lint/triage's own bucket
    definition) -- a project supernode's own orphans reads structurally 0; the real
    zero-link population lands entirely in the unfiled bucket (tested separately below)."""
    proj = await actions.create_or_find_object("SoftwareProject", "repo:gv-lod-proj", "test")
    connected = await actions.create_or_find_object("Thread", "thread:gv-lod-connected", "test")
    member = await actions.create_or_find_object("Thread", "thread:gv-lod-member", "test")
    other = await actions.create_or_find_object("Thread", "thread:gv-lod-other", "test")
    await actions.create_link(connected, proj, "in_repo", "test", datetime.now(UTC), 1.0)
    await actions.create_link(member, proj, "in_repo", "test", datetime.now(UTC), 1.0)
    await actions.create_link(connected, other, "cites", "test", datetime.now(UTC), 1.0)

    r = await client.get("/graph/supernodes")
    body = r.json()
    row = next(s for s in body["supernodes"] if s["label"] == "repo:gv-lod-proj")
    assert row["count"] == 2
    assert row["orphans"] == 0


async def test_graph_supernodes_shows_the_projects_name_not_its_raw_canonical(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    """Thoth dispatch 9563/9769: the retired Atlas showed `project_canonical` verbatim
    (repo:foo) — a named project now resolves through the SAME resolve_label chain
    /objects and the projects composition already use. Unnamed stays canonical (the test
    above, unchanged) — this is the ONLY new behavior."""
    proj = await actions.create_or_find_object("SoftwareProject", "repo:gv-lod-named", "test")
    await actions.assert_property(proj, "name", "gv-lod-named-display", "test",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")
    member = await actions.create_or_find_object("Thread", "thread:gv-lod-named-member", "test")
    await actions.create_link(member, proj, "in_repo", "test", datetime.now(UTC), 1.0)

    r = await client.get("/graph/supernodes")
    body = r.json()
    row = next(s for s in body["supernodes"] if s["id"] == str(proj))
    assert row["label"] == "gv-lod-named-display"


async def test_graph_supernodes_unfiled_orphans_are_the_true_zero_link_objects(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    """The real "zero links at all" population (graph_lint/triage's exact orphan
    definition) can only ever land in `unfiled` -- project/cluster membership itself
    requires a link, so a genuinely edgeless object is never IN a project to begin with."""
    await actions.create_or_find_object("Thread", "thread:gv-lod-true-orphan", "test")
    linked_but_unfiled = await actions.create_or_find_object(
        "Thread", "thread:gv-lod-linked-unfiled", "test")
    other = await actions.create_or_find_object("Thread", "thread:gv-lod-unfiled-peer", "test")
    await actions.create_link(linked_but_unfiled, other, "cites", "test", datetime.now(UTC), 1.0)

    r = await client.get("/graph/supernodes")
    unfiled = r.json()["unfiled"]
    assert unfiled["count"] >= 3          # both unfiled objects, at least
    assert unfiled["orphans"] >= 1        # at least the truly-zero-link one


async def test_graph_supernodes_weighs_inter_project_edges(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    proj_a = await actions.create_or_find_object("SoftwareProject", "repo:gv-lod-a", "test")
    proj_b = await actions.create_or_find_object("SoftwareProject", "repo:gv-lod-b", "test")
    obj_a = await actions.create_or_find_object("Thread", "thread:gv-lod-ia", "test")
    obj_b = await actions.create_or_find_object("Thread", "thread:gv-lod-ib", "test")
    await actions.create_link(obj_a, proj_a, "in_repo", "test", datetime.now(UTC), 1.0)
    await actions.create_link(obj_b, proj_b, "in_repo", "test", datetime.now(UTC), 1.0)
    await actions.create_link(obj_a, obj_b, "cites", "test", datetime.now(UTC), 1.0)

    r = await client.get("/graph/supernodes")
    edges = r.json()["project_edges"]
    pair = {str(proj_a), str(proj_b)}
    match = next(e for e in edges if {e["source"], e["target"]} == pair)
    assert match["weight"] == 1


async def test_graph_supernodes_unfiled_bucket_counts_projectless_objects(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    await actions.create_or_find_object("Thread", "thread:gv-lod-unfiled", "test")
    r = await client.get("/graph/supernodes")
    assert r.json()["unfiled"]["count"] >= 1


async def test_graph_supernodes_unfiled_is_positioned_and_id_matches_the_sentinel(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    """Thoth DM 9019: unfiled carries its own `id` (the sentinel /graph/clusters?project=
    reads back), sized and placed by the SAME heartbeat-stored positions as a project
    supernode — the centroid of whichever of its members the heartbeat already placed,
    never a bespoke layout of its own."""
    positioned = await actions.create_or_find_object("Thread", "thread:gv-unfiled-pos", "test")
    await actions.assert_property(positioned, "graph_x", 12.0, "test", datetime.now(UTC), 1.0)
    await actions.assert_property(positioned, "graph_y", -4.0, "test", datetime.now(UTC), 1.0)

    r = await client.get("/graph/supernodes")
    unfiled = r.json()["unfiled"]
    assert unfiled["id"] == "unfiled"
    assert unfiled["x"] is not None and unfiled["y"] is not None


async def test_graph_supernodes_unfiled_abstained_matches_the_census_predicate(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    """`abstained` mirrors compositions.orphan_census's own live-`derivation_abstained_*`
    predicate exactly (Khnum's stale-abstention catch, DM 8855): an acknowledged
    disconnection counts, an unexamined one doesn't, and a RESOLVED abstention (a later
    successful mint superseded it) never masquerades as a live one. Measured as a DELTA
    against the endpoint's own before/after reading — the test DB already carries other
    fixtures' orphans, so only the shift this test itself causes is a safe assertion."""
    before = (await client.get("/graph/supernodes")).json()["unfiled"]

    acknowledged = await actions.create_or_find_object(
        "Thread", "thread:gv-unfiled-abstained", "test")
    await actions.assert_property(
        acknowledged, "derivation_abstained_in_repo",
        {"link_type": "in_repo", "candidate_count": 0, "reason": "no candidate found",
         "candidates": []},
        "test", datetime.now(UTC), 0.6, evidence_class="derived")
    await actions.create_or_find_object("Thread", "thread:gv-unfiled-unexamined", "test")
    resolved = await actions.create_or_find_object(
        "Thread", "thread:gv-unfiled-resolved-abstention", "test")
    await actions.assert_property(
        resolved, "derivation_abstained_in_repo",
        {"link_type": "in_repo", "resolved": True, "resolved_to": "repo:whatever"},
        "test", datetime.now(UTC), 0.6, evidence_class="derived")

    after = (await client.get("/graph/supernodes")).json()["unfiled"]
    assert after["orphans"] - before["orphans"] == 3   # none of the three carries a real link
    assert after["abstained"] - before["abstained"] == 1  # only the acknowledged one


async def test_graph_clusters_groups_by_type_inside_one_project(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    proj = await actions.create_or_find_object("SoftwareProject", "repo:gv-clusters", "test")
    t1 = await actions.create_or_find_object("Thread", "thread:gv-cl-1", "test")
    t2 = await actions.create_or_find_object("Thread", "thread:gv-cl-2", "test")
    c1 = await actions.create_or_find_object("Commit", "commit:gv-cl-1", "test")
    for oid in (t1, t2, c1):
        await actions.create_link(oid, proj, "in_repo", "test", datetime.now(UTC), 1.0)

    r = await client.get("/graph/clusters", params={"project": "repo:gv-clusters"})
    clusters = {c["type"]: c["count"] for c in r.json()["clusters"]}
    assert clusters.get("Thread") == 2
    assert clusters.get("Commit") == 1


async def test_graph_clusters_resolves_a_bare_project_name(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    proj = await actions.create_or_find_object("SoftwareProject", "repo:gv-bare", "test")
    t1 = await actions.create_or_find_object("Thread", "thread:gv-bare-1", "test")
    await actions.create_link(t1, proj, "in_repo", "test", datetime.now(UTC), 1.0)

    r = await client.get("/graph/clusters", params={"project": "gv-bare"})
    clusters = {c["type"]: c["count"] for c in r.json()["clusters"]}
    assert clusters.get("Thread") == 1


async def test_graph_clusters_unfiled_sentinel_groups_the_last_resort_bucket_by_type(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    """The Atlas's unfiled supernode (Thoth DM 9019) drills into its own members by type
    the SAME way a real project's clusters do — `project="unfiled"` (graph_supernodes's
    own `unfiled.id`) is the sentinel, never a real project's canonical, so it never
    collides with an actual repo named "unfiled"."""
    await actions.create_or_find_object("Thread", "thread:gv-unfiled-cl-1", "test")
    await actions.create_or_find_object("Thread", "thread:gv-unfiled-cl-2", "test")
    await actions.create_or_find_object("Commit", "commit:gv-unfiled-cl-1", "test")
    # a filed member must never leak into the unfiled clusters
    proj = await actions.create_or_find_object("SoftwareProject", "repo:gv-unfiled-cl", "test")
    filed = await actions.create_or_find_object("Thread", "thread:gv-unfiled-cl-filed", "test")
    await actions.create_link(filed, proj, "in_repo", "test", datetime.now(UTC), 1.0)

    r = await client.get("/graph/clusters", params={"project": "unfiled"})
    clusters = {c["type"]: c["count"] for c in r.json()["clusters"]}
    assert clusters.get("Thread") == 2
    assert clusters.get("Commit") == 1


async def test_object_graph(client: httpx.AsyncClient, actions: Actions) -> None:
    await _seed(actions)
    oid = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical=$1", LAZARUS)
    r = await client.get(f"/objects/{oid}/graph", params={"hops": 1})
    g = r.json()
    types = {n["type"] for n in g["nodes"]}
    assert "IntrusionSet" in types and "Malware" in types
    assert any(e["type"] == "uses" for e in g["edges"])


async def test_object_graph_paints_agent_nodes_with_live_idle_dead_state(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    """GRAPH VISUALIZER wave A item 4 (thread 8839): an Agent node's own graph-endpoint row
    carries `agent_state`, resolved from agent_mounts.last_seen against the same LIVE_SECS
    window (900s) seats.py's own occupancy read uses, plus an idle tier (seen in the last
    day). A non-Agent node never carries the field at all -- it isn't a claim this endpoint
    can make about a Thread or a Commit."""
    from src.orchestrator.mounts import save_mount

    live_id = await actions.create_or_find_object("Agent", "agent:gv-live", "test")
    idle_id = await actions.create_or_find_object("Agent", "agent:gv-idle", "test")
    dead_id = await actions.create_or_find_object("Agent", "agent:gv-dead", "test")
    thread_id = await actions.create_or_find_object("Thread", "thread:gv-item4", "test")
    await actions.create_link(thread_id, live_id, "closed_by", "test", datetime.now(UTC), 1.0)
    await actions.create_link(thread_id, idle_id, "closed_by", "test", datetime.now(UTC), 1.0)
    await actions.create_link(thread_id, dead_id, "closed_by", "test", datetime.now(UTC), 1.0)

    await save_mount(actions.pool, job_dir="/test/gv-live", agent_id="agent:gv-live",
                     project="osiris", cwd="/test", model=None, session_key=None)
    await save_mount(actions.pool, job_dir="/test/gv-idle", agent_id="agent:gv-idle",
                     project="osiris", cwd="/test", model=None, session_key=None)
    await actions.pool.execute(
        "UPDATE agent_mounts SET last_seen = now() - interval '6 hours' WHERE agent_id=$1",
        "agent:gv-idle")
    await save_mount(actions.pool, job_dir="/test/gv-dead", agent_id="agent:gv-dead",
                     project="osiris", cwd="/test", model=None, session_key=None)
    await actions.pool.execute(
        "UPDATE agent_mounts SET last_seen = now() - interval '3 days' WHERE agent_id=$1",
        "agent:gv-dead")

    r = await client.get(f"/objects/{thread_id}/graph", params={"hops": 1})
    g = r.json()
    by_id = {n["id"]: n for n in g["nodes"]}
    assert by_id[str(live_id)]["agent_state"] == "live"
    assert by_id[str(idle_id)]["agent_state"] == "idle"
    assert by_id[str(dead_id)]["agent_state"] == "dead"
    assert "agent_state" not in by_id[str(thread_id)]


async def test_available_helpers_from_manifest_registry(
    client: httpx.AsyncClient, actions: Actions
) -> None:
    await _seed(actions)
    applejeus = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical=$1", APPLEJEUS)
    r = await client.get(f"/objects/{applejeus}/helpers")
    assert "threatfox_malware_iocs" in {h["id"] for h in r.json()}


async def test_object_404(client: httpx.AsyncClient) -> None:
    r = await client.get(f"/objects/{uuid.uuid4()}")
    assert r.status_code == 404


async def test_list_cases_with_counts(client: httpx.AsyncClient, actions: Actions) -> None:
    cid = await _seed(actions)
    cases = (await client.get("/cases")).json()
    mine = next(c for c in cases if c["id"] == str(cid))
    assert mine["name"] == "api"
    assert mine["object_count"] >= 11  # the ingested ATT&CK objects


async def test_projects_index(client: httpx.AsyncClient, actions: Actions) -> None:
    """#93: the project index — name never leaks the `repo:` canonical prefix (operator's
    own ruling), object_count/object_counts_by_type reflect real linked objects, and the
    status dimension is never collapsed (each row carries its own real status)."""
    from src.orchestrator.capture import link_repo

    now = datetime.now(UTC)
    d = await actions.create_or_find_object("Decision", "decision:projidxtest", "test")
    await link_repo(actions, d, "projidxtest", now)
    r = await client.get("/projects")
    assert r.status_code == 200
    rows = r.json()
    mine = next(row for row in rows if row["canonical"] == "repo:projidxtest")
    assert mine["name"] == "projidxtest"  # no "repo:" prefix leaked
    assert mine["status"] == "active"
    assert mine["object_count"] >= 1
    assert mine["object_counts_by_type"].get("Decision", 0) >= 1
    assert "bucket" in mine


async def test_projects_index_object_count_excludes_archived_and_retired(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    from src.orchestrator.capture import link_repo

    now = datetime.now(UTC)
    d = await actions.create_or_find_object("Decision", "decision:projidxarchived", "test")
    await link_repo(actions, d, "projidxarchivedtest", now)
    await actions.pool.execute("UPDATE objects SET status='archived' WHERE id=$1", d)
    r = await client.get("/projects")
    mine = next(row for row in r.json() if row["canonical"] == "repo:projidxarchivedtest")
    assert mine["object_count"] == 0
    assert "Decision" not in mine["object_counts_by_type"]


async def test_projects_index_unnamed_project_strips_repo_prefix(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    """Console honesty fix (2026-09-07): a SoftwareProject minted with no `name`/`title`/
    etc. property resolves through resolve_label's canonical tier — `/projects` must say
    so (`unnamed: true`), and `name` must not leak the internal `repo:` scheme prefix, only
    the bare id, per the operator's own ruling that `repo:` never reaches the UI."""
    await actions.create_or_find_object("SoftwareProject", "repo:bareidtest", "test")
    r = await client.get("/projects")
    mine = next(row for row in r.json() if row["canonical"] == "repo:bareidtest")
    assert mine["unnamed"] is True
    assert mine["name"] == "bareidtest"


async def test_projects_index_named_project_is_not_unnamed(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    """The converse of the fix above: a project with a real `name` assertion (link_repo
    always stamps one on a fresh mint) resolves through the `chain` tier, never `canonical`
    — `unnamed` must read False and `name` must be the real chosen name, not a bare id."""
    from src.orchestrator.capture import link_repo

    now = datetime.now(UTC)
    d = await actions.create_or_find_object("Decision", "decision:namedprojtest", "test")
    await link_repo(actions, d, "namedprojtest", now)
    r = await client.get("/projects")
    mine = next(row for row in r.json() if row["canonical"] == "repo:namedprojtest")
    assert mine["unnamed"] is False
    assert mine["name"] == "namedprojtest"


async def test_projects_index_nests_worktrees_under_their_parent(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    """Thread 922d920c/55992ca9: a Worktree is never a project of its own (Sekhmet's own
    Worktree/worktree_of shape) — /projects must nest it under its parent's own row, never
    list it as a flat sibling that could misread as a project (the exact ballgem-wt-*
    misfiling shape this whole mechanism exists to stop)."""
    now = datetime.now(UTC)
    proj = await actions.create_or_find_object("SoftwareProject", "repo:wtparenttest", "test")
    await actions.assert_property(proj, "name", "wtparenttest", "test", now, 0.9,
                                  evidence_class="self_declared")
    wt = await actions.create_or_find_object("Worktree", "worktree:wtparenttest-feature",
                                             "test")
    await actions.assert_property(wt, "branch", "khnum-feature", "test", now, 0.9,
                                  evidence_class="self_declared")
    await actions.create_link(wt, proj, "worktree_of", "test", now, 0.9,
                              evidence_class="self_declared")

    r = await client.get("/projects")
    body = r.json()
    mine = next(row for row in body if row["canonical"] == "repo:wtparenttest")
    assert mine["worktrees"] == [
        {"canonical": "worktree:wtparenttest-feature", "name": "wtparenttest-feature",
         "branch": "khnum-feature"},
    ]
    assert not any(row["canonical"] == "worktree:wtparenttest-feature"
                  for row in body)  # never a flat sibling row


async def test_projects_index_ordinary_project_has_no_worktrees(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    await actions.create_or_find_object("SoftwareProject", "repo:nowtreetest", "test")
    r = await client.get("/projects")
    mine = next(row for row in r.json() if row["canonical"] == "repo:nowtreetest")
    assert mine["worktrees"] == []


async def test_objects_scoped_to_case(client: httpx.AsyncClient, actions: Actions) -> None:
    cid = await _seed(actions)
    # an object in a *different* case must not appear when scoping to this one
    other = uuid.UUID(
        str(await actions.pool.fetchval(
            "INSERT INTO cases (name, owner) VALUES ('other','x') RETURNING id"
        ))
    )
    await actions.create_or_find_object("Domain", "elsewhere.test", "x", other)
    scoped = (await client.get(f"/objects?case_id={cid}")).json()
    canonicals = {o["canonical"] for o in scoped}
    assert LAZARUS in canonicals
    assert "elsewhere.test" not in canonicals


async def test_objects_scoped_to_multiple_projects(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    """#196 (Thoth msg 5600): `project` used to bind as a bare `str`, so a multi-repo scope
    pill selection (console.js sends `&project=a&project=b` for a multi-select) silently
    filtered on whichever value FastAPI happened to bind last — the scope pill's own
    "server-side, not client-side after a capped fetch" gap. Two repos selected together
    must return objects from BOTH, not just one."""
    from src.orchestrator.capture import open_thread

    ta = await open_thread(actions, "in project alpha only", repo="apitest-alpha")
    tb = await open_thread(actions, "in project beta only", repo="apitest-beta")
    r = await client.get("/objects", params={
        "type": "Thread", "project": ["apitest-alpha", "apitest-beta"],
    })
    assert r.status_code == 200
    ids = {o["id"] for o in r.json()}
    assert str(ta) in ids
    assert str(tb) in ids


async def test_object_counts_endpoint(client: httpx.AsyncClient, actions: Actions) -> None:
    """#196: /objects/counts is the TRUE per-type count over the live scope — the browse
    surface's own stat chrome must never infer counts from a capped /objects fetch (silently
    wrong the instant a type/scope exceeds the cap)."""
    await _seed(actions)
    r = await client.get("/objects/counts")
    assert r.status_code == 200
    body = r.json()
    assert body["by_type"].get("IntrusionSet", 0) >= 1
    assert body["total"] >= sum(body["by_type"].values()) - 1  # sum IS the total, not approx
    assert body["total"] == sum(body["by_type"].values())


async def test_object_counts_scoped_to_project(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    from src.orchestrator.capture import open_thread

    await open_thread(actions, "counts-scoped alpha thread", repo="apitest-counts-alpha")
    r_all = await client.get("/objects/counts")
    r_scoped = await client.get(
        "/objects/counts", params={"project": "apitest-counts-alpha"})
    assert r_scoped.json()["by_type"].get("Thread", 0) >= 1
    assert r_scoped.json()["total"] <= r_all.json()["total"]


async def test_objects_keyset_pagination_walks_strictly_older_and_never_repeats(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    """#196: keyset paging is ADDITIVE — omitting before_created_at/before_id must reproduce
    the exact prior (uncursored) response, and a cursor built from the last row of one page
    must walk strictly older, with zero overlap between pages (the OFFSET failure mode this
    replaces: re-sorting mid-walk can duplicate or skip rows)."""
    from src.orchestrator.capture import link_repo

    now = datetime.now(UTC)
    for i in range(5):
        d = await actions.create_or_find_object(
            "Domain", f"keyset-{i}.apitest", "test")
        await link_repo(actions, d, "apitest-keyset", now)

    page1 = (await client.get("/objects", params={
        "type": "Domain", "project": "apitest-keyset", "limit": 2,
    })).json()
    assert len(page1) == 2
    last = page1[-1]
    page2 = (await client.get("/objects", params={
        "type": "Domain", "project": "apitest-keyset", "limit": 2,
        "before_created_at": last["created_at"], "before_id": last["id"],
    })).json()
    assert len(page2) == 2
    ids1 = {o["id"] for o in page1}
    ids2 = {o["id"] for o in page2}
    assert ids1.isdisjoint(ids2)
    for o in page2:
        assert o["created_at"] <= last["created_at"]


async def test_snapshot_time_travel(client: httpx.AsyncClient, actions: Actions) -> None:
    cid = uuid.UUID(
        str(
            await actions.pool.fetchval(
                "INSERT INTO cases (name, owner) VALUES ('snap','analyst:test') RETURNING id"
            )
        )
    )
    a = await actions.create_or_find_object("Domain", "early.test", "analyst:test", cid)
    cutoff = await actions.pool.fetchval("SELECT created_at FROM objects WHERE id=$1", a)
    await actions.create_or_find_object("Domain", "late.test", "analyst:test", cid)

    r = await client.get(f"/cases/{cid}/snapshot", params={"at": cutoff.isoformat()})
    canonicals = {o["canonical"] for o in r.json()["objects"]}
    assert "early.test" in canonicals
    assert "late.test" not in canonicals  # created after the cutoff


async def test_merge_candidates_endpoint(client: httpx.AsyncClient, actions: Actions) -> None:
    from src.ontology.resolution import find_person_merge_candidates

    cid = uuid.UUID(
        str(
            await actions.pool.fetchval(
                "INSERT INTO cases (name, owner) VALUES ('mc','analyst:test') RETURNING id"
            )
        )
    )
    for canon in ("pa", "pb"):
        oid = await actions.create_or_find_object("Person", canon, "analyst:test", cid)
        await actions.assert_property(
            oid, "email", "shared@x.com", "analyst:test", datetime.now(UTC), 0.9, case_id=cid
        )
    await find_person_merge_candidates(actions.pool)
    r = await client.get("/merge-candidates")
    assert len(r.json()) == 1


async def test_lifespan_seeds_the_type_catalog_on_boot(
    pg_dsn: str, redis_url: str, actions: Actions
) -> None:
    """Task #97 workstream 1 shipped `seed_catalog()` but nothing called it on a real
    boot (Thoth msg 2131, caught live: 0f139b9 deployed with the catalog machinery
    correct and production sitting at zero Type objects). This proves the FIX, not
    just the unit — the real ASGI lifespan protocol, driven exactly like uvicorn
    drives it, against a DB where Type rows have deliberately been wiped.

    The expected count is schema.py's own tuple lengths, not a hardcoded guess — the
    manifest is the source of truth and this test breaks loudly the day it drifts."""
    import os

    from src.ontology import schema

    n_expect_object = len(schema._OBJECT_TYPES)
    n_expect_link = len(schema._LINK_TYPES)

    # unwind exactly what the earlier (actions fixture's own) seed_catalog call wrote
    # for the Type rows this test is about to delete — every table it inserts into
    # (src/actions/core.py: assert_property -> outbox/object_events, create_or_find_object
    # -> object_events), mirroring the FK-reachability discipline conftest.py's own
    # comment documents for the shared fixture.
    await actions.pool.execute(
        "DELETE FROM outbox o2 USING objects o "
        "WHERE o2.object_id = o.id AND o.type = 'Type'"
    )
    await actions.pool.execute(
        "DELETE FROM assertions a USING objects o "
        "WHERE a.object_id = o.id AND o.type = 'Type'"
    )
    await actions.pool.execute(
        "DELETE FROM object_events e USING objects o "
        "WHERE e.object_id = o.id AND o.type = 'Type'"
    )
    await actions.pool.execute("DELETE FROM objects WHERE type = 'Type'")
    n_before = await actions.pool.fetchval("SELECT count(*) FROM objects WHERE type = 'Type'")
    assert n_before == 0

    os.environ["DATABASE_URL"] = pg_dsn
    os.environ["REDIS_URL"] = redis_url
    app = create_app()  # own pool: exercises the exact lifespan a real boot runs
    async with app.router.lifespan_context(app):
        n_object = await app.state.pool.fetchval(
            "SELECT count(*) FROM objects o JOIN assertions a ON a.object_id = o.id "
            "WHERE o.type = 'Type' AND a.name = 'kind' AND a.value = '\"object\"'"
        )
        n_link = await app.state.pool.fetchval(
            "SELECT count(*) FROM objects o JOIN assertions a ON a.object_id = o.id "
            "WHERE o.type = 'Type' AND a.name = 'kind' AND a.value = '\"link\"'"
        )
    assert n_object == n_expect_object
    assert n_link == n_expect_link

    # idempotent on a second boot (the real restart case) — no duplicates, no error
    app2 = create_app()
    async with app2.router.lifespan_context(app2):
        n2 = await app2.state.pool.fetchval("SELECT count(*) FROM objects WHERE type = 'Type'")
    assert n2 == n_expect_object + n_expect_link


async def test_object_card_title_uses_resolve_label_not_name_only(actions: Actions) -> None:
    """Task #97 workstream 3: _object_card (the watch/subscription card-preview
    endpoint) used to check ONLY the `name` property for its title — a Practice
    (statement/failure_prevented/surface, no name) rendered its raw canonical hash.
    Now shares the same resolve_label every other consumer does."""
    from src.api.app import _object_card

    p = await actions.create_or_find_object("Practice", "practice:cardtest", "test")
    await actions.assert_property(p, "statement", "measure it yourself", "test",
                                  datetime.now(UTC), 0.9)
    card = await _object_card(actions.pool, p)
    assert card is not None
    assert card["title"] == "measure it yourself"
    # the statement still appears in the generic properties list too (unchanged
    # behavior for any field besides `name`, which alone gets excluded)
    assert any(pr["name"] == "statement" for pr in card["properties"])


# THE READ-ONLY PANE (Thoth dispatch 9378, lane B piece 2, thread 9d2aaf4d): /pane/{agent_id}/
# stream tails a live seat's own transcript over SSE, no writes. Both tests below hit only the
# EARLY-RETURN refusal branches, which return before the route's own
# `while not await request.is_disconnected()` loop is ever reached — the same structural limit
# test_inbox_app.py's own SSE test names ("driving the actual infinite generator through
# httpx's ASGITransport hangs... no precedent anywhere in this suite for testing an SSE route
# that way"). The polling loop itself reuses sessions.py's own _read_chunk/distill, already
# covered by test_sessions.py; this file's job is the route's OWN new resolution logic.


async def test_pane_live_lists_only_live_seated_agents(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    from src.orchestrator.mounts import save_mount

    # live + seated (a real object behind the canonical) — must appear
    await actions.create_or_find_object("Agent", "agent:paneseated1", "test")
    await save_mount(actions.pool, job_dir="/tmp/jobs/paneseated1", agent_id="agent:paneseated1",
                     project="p", cwd="/tmp/paneseated1", model=None, session_key=None)

    r = await client.get("/pane/live")
    assert r.status_code == 200
    rows = r.json()
    assert any(row["agent_id"] == "agent:paneseated1" for row in rows)
    for row in rows:
        assert set(row) == {"agent_id", "seat", "project"}


# THE REPLY DOOR (Thoth dispatch 9378, lane B piece 3): POST /pane/{agent_id}/reply — a
# one-shot turn against the seat's OWN already-running session via ProcessAdapter.reply(),
# never a new spawn. Carries the SAME may_spend gate piece 1 gave every other hand-birth
# path (a new billed call site with no gate would have been a fifth instance of that bug).


async def test_pane_reply_refuses_honestly_when_no_live_mount_exists(
    client: httpx.AsyncClient,
) -> None:
    r = await client.post("/pane/agent:nobody-here/reply", json={"prompt": "hello"})
    assert r.status_code == 200
    assert "no live mount" in r.json()["error"]


async def test_pane_reply_refuses_an_over_budget_turn(
    client: httpx.AsyncClient, actions: Actions, monkeypatch: Any,
) -> None:
    from src.config.settings import Settings
    from src.ingest.providers import Usage
    from src.ingest.usage import record_usage
    from src.orchestrator.mounts import save_mount

    await save_mount(actions.pool, job_dir="/tmp/jobs/panereply-budget",
                     agent_id="agent:panereply-budget", project="p",
                     cwd="/tmp/panereply-budget", model=None, session_key=None)
    for _ in range(12):
        await record_usage(actions.pool, purpose="wake", usage=Usage(
            model="claude-haiku-4-5-20251001", input_tokens=1, output_tokens=1,
            cache_read_tokens=0, cache_creation_tokens=0, cost_usd=1.00))
    monkeypatch.setattr(
        "src.config.settings.get_settings",
        lambda: Settings(osiris_daily_usd=10.0, osiris_extract_provider="anthropic",
                         anthropic_api_key="k"))

    async def _boom(**kw: Any) -> Any:
        raise AssertionError("a refused reply must never reach the adapter")

    monkeypatch.setattr(
        "src.orchestrator.harness_process.resolve_process_adapter",
        lambda *a, **k: SimpleNamespace(list_sessions=_boom, reply=_boom))

    r = await client.post("/pane/agent:panereply-budget/reply", json={"prompt": "hello"})
    assert r.status_code == 200
    assert "ceiling" in r.json()["error"].lower()


async def test_pane_reply_resolves_the_resume_session_by_cwd_and_calls_the_real_adapter(
    client: httpx.AsyncClient, actions: Actions, monkeypatch: Any,
) -> None:
    from src.orchestrator.mounts import save_mount

    await save_mount(actions.pool, job_dir="/tmp/jobs/panereply-happy",
                     agent_id="agent:panereply-happy", project="p",
                     cwd="/tmp/panereply-happy", model=None, session_key=None)

    calls: dict[str, Any] = {}

    async def _list_sessions(*, cwd: str | None = None, **k: Any) -> list[dict[str, Any]]:
        return [{"sessionId": "sess-wrong", "cwd": "/tmp/somewhere-else"},
                {"sessionId": "sess-right", "cwd": cwd}]

    async def _reply(**kw: Any) -> dict[str, Any]:
        calls.update(kw)
        return {"replied": True}

    monkeypatch.setattr(
        "src.orchestrator.harness_process.resolve_process_adapter",
        lambda *a, **k: SimpleNamespace(list_sessions=_list_sessions, reply=_reply))

    r = await client.post("/pane/agent:panereply-happy/reply", json={"prompt": "hello"})
    assert r.status_code == 200
    assert r.json() == {"replied": True}
    assert calls["repo"] == "/tmp/panereply-happy"
    assert calls["prompt"] == "hello"
    assert calls["job_dir"] == "/tmp/jobs/panereply-happy"
    assert calls["resume_session"] == "sess-right"


async def test_pane_stream_refuses_honestly_when_no_live_mount_exists(
    client: httpx.AsyncClient,
) -> None:
    r = await client.get("/pane/agent:nobody-here/stream")
    assert r.status_code == 200
    assert "no live mount" in r.text
    assert "agent:nobody-here" in r.text


async def test_pane_stream_refuses_honestly_when_no_transcript_resolves(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    from src.orchestrator.mounts import save_mount

    await save_mount(actions.pool, job_dir="/tmp/jobs/no-such-transcript-anywhere",
                     agent_id="agent:paneless01", project="p",
                     cwd="/tmp/no-such-cwd-anywhere-either", model=None, session_key=None)

    r = await client.get("/pane/agent:paneless01/stream")
    assert r.status_code == 200
    assert "no transcript found" in r.text
    assert "agent:paneless01" in r.text


# --- backup-settings (Wave 21, thread f04cce36 piece 3b) --------------------

async def test_backup_settings_route_get_starts_empty(client: httpx.AsyncClient) -> None:
    r = await client.get("/backup-settings")
    assert r.status_code == 200
    assert r.json()["vault_path"] is None


async def test_backup_settings_route_writes_as_the_operator(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    """The console is the operator's own surface (6c18709f) — every write here is
    analyst:operator, an operator actor by construction, needing no ruling citation."""
    r = await client.post("/backup-settings", json={
        "because": "switching to the NAS mount", "vault_path": "/mnt/nas/osiris-vault"})
    assert r.status_code == 200
    body = r.json()
    assert body["vault_path"] == "/mnt/nas/osiris-vault"
    # a later partial write leaves vault_path untouched
    r2 = await client.post("/backup-settings", json={
        "because": "rescheduling dumps",
        "timer_schedules": {"osiris-backup.timer": "*-*-* 00,12:00:00"}})
    body2 = r2.json()
    assert body2["vault_path"] == "/mnt/nas/osiris-vault"
    assert body2["timer_schedules"] == {"osiris-backup.timer": "*-*-* 00,12:00:00"}
    read_back = (await client.get("/backup-settings")).json()
    body2.pop("because")  # write's own receipt-only field, absent from a plain read
    assert read_back == body2


async def test_backup_settings_route_rejects_a_bad_field_without_writing(
    client: httpx.AsyncClient,
) -> None:
    r = await client.post("/backup-settings", json={
        "because": "testing", "timer_schedules": {"not-a-real.timer": "daily"}})
    assert "error" in r.json()
    assert (await client.get("/backup-settings")).json()["timer_schedules"] == {}


# --- settings (THE SETTINGS MENU, thread f4498ab304e4 piece 1) --------------

async def test_settings_route_list_shows_every_registered_knob(
    client: httpx.AsyncClient,
) -> None:
    r = await client.get("/settings")
    assert r.status_code == 200
    keys = {s["key"] for s in r.json()["settings"]}
    assert "daemon.pit_watch.enabled" in keys


async def test_settings_route_writes_as_the_operator(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    """The console is the operator's own surface (6c18709f) — every write here is
    analyst:operator, an operator actor by construction, needing no ruling citation."""
    r = await client.post("/settings", json={
        "key": "daemon.pit_watch.enabled", "value": True, "because": "watching tonight"})
    assert r.status_code == 200
    body = r.json()
    assert body["value"] is True
    listed = (await client.get("/settings")).json()["settings"]
    row = next(s for s in listed if s["key"] == "daemon.pit_watch.enabled")
    assert row["value"] is True


async def test_settings_route_rejects_a_bad_value_without_writing(
    client: httpx.AsyncClient,
) -> None:
    r = await client.post("/settings", json={
        "key": "daemon.pit_watch.enabled", "value": "not-a-bool", "because": "testing"})
    assert "error" in r.json()
    listed = (await client.get("/settings")).json()["settings"]
    row = next(s for s in listed if s["key"] == "daemon.pit_watch.enabled")
    assert row["value"] is False  # untouched
