"""The entity dossier: a federated entity's identity + named relationship network."""
from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest_asyncio
import redis.asyncio as aioredis
from src.actions.core import Actions
from src.api.app import create_app
from src.ingest.opensanctions import ingest_ftm
from src.orchestrator.dossier import entity_dossier
from src.orchestrator.manifests import load_manifests

HELPERS = Path(__file__).parent.parent / "helpers"


@pytest_asyncio.fixture
async def client(
    actions: Actions, redis_client: aioredis.Redis
) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(actions.pool)
    app.state.pool = actions.pool
    app.state.manifests = load_manifests(HELPERS)
    app.state.connectors = {}
    app.state.redis = redis_client
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c

_FTM = [
    {"id": "P1", "schema": "Person", "properties": {
        "name": ["Kim Jong Un"], "country": ["kp"],
        "birthDate": ["1984-01-08"], "topics": ["sanction"]}},
    {"id": "O1", "schema": "Company", "properties": {"name": ["Bureau 39"], "country": ["kp"]}},
    {"id": "R1", "schema": "Directorship", "properties": {
        "director": ["P1"], "organization": ["O1"]}},
    {"id": "P2", "schema": "Person", "properties": {"name": ["Relative X"]}},
    {"id": "R2", "schema": "Family", "properties": {"person": ["P1"], "relative": ["P2"]}},
]


async def test_dossier_renders_identity_and_named_network(actions: Actions) -> None:
    await ingest_ftm(actions, _FTM)
    p1 = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='P1'")

    d = await entity_dossier(actions.pool, p1)

    assert d["type"] == "Person"
    assert d["name"] == "Kim Jong Un"

    # identity properties (task #170's neighbour: `name` now JOINS this set, `tag` stays
    # excluded on its own separate grounds — see dossier.py's own comment)
    prop_names = {p["name"] for p in d["properties"]}
    assert {"country", "birthDate", "topics", "name"} <= prop_names
    assert "tag" not in prop_names
    country = next(p for p in d["properties"] if p["name"] == "country")
    assert country["values"][0]["value"] == "kp"
    assert country["values"][0]["evidence_class"] == "authoritative_api"

    # the relationship network, each endpoint NAMED (the void->named payoff)
    network = {(r["direction"], r["type"], r["neighbor"]["name"]) for r in d["relationships"]}
    assert ("out", "directs", "Bureau 39") in network
    assert ("out", "family", "Relative X") in network
    assert all(r["evidence_class"] == "authoritative_api" for r in d["relationships"])
    org_edge = next(r for r in d["relationships"] if r["type"] == "directs")
    assert org_edge["neighbor"]["type"] == "Organization"


async def test_dossier_missing_object_is_empty(actions: Actions) -> None:
    assert await entity_dossier(actions.pool, uuid.uuid4()) == {}


async def test_dossier_splits_lifecycle_status_from_semantic_status(
    actions: Actions,
) -> None:
    """THE FIX (thread 6212d9f5, Thoth DM 2746, "the graph knows and the display lies"):
    the object's own LIFECYCLE (objects.status: active/merged/archived) and a Thread's
    semantic status ASSERTION (open/resolved/retracted) are two different concepts that
    used to share one top-level "status" key — the lifecycle value always won, silently.
    They must now render as two distinctly-named fields."""
    from datetime import UTC, datetime

    obj = await actions.create_or_find_object("Thread", "thread:dosssplit", "test")
    await actions.assert_property(obj, "status", "resolved", "agent:alice",
                                  datetime.now(UTC), 0.9)
    d = await entity_dossier(actions.pool, obj)
    assert d["object_status"] == "active"   # the objects-table lifecycle, unambiguous now
    assert d["status"] == "resolved"        # the semantic status, no longer shadowed by it


async def test_dossier_status_resolves_the_winner_not_the_first_or_last_write(
    actions: Actions,
) -> None:
    """THE ACCEPTANCE SHAPE (b318a9d3's own live specimen): THREE sources, none
    superseding another — open (oldest), resolved (middle), resolved (newest, highest
    confidence tied with the others) — winning_props' own ordering (confidence DESC, then
    observed_at DESC) must pick the newest 'resolved', not whichever row the DB happened
    to return first."""
    from datetime import UTC, datetime, timedelta

    obj = await actions.create_or_find_object("Thread", "thread:dosswinner", "test")
    t0 = datetime.now(UTC)
    await actions.assert_property(obj, "status", "open", "agent:first", t0, 0.9)
    await actions.assert_property(obj, "status", "resolved", "agent:second",
                                  t0 + timedelta(days=1), 0.9)
    await actions.assert_property(obj, "status", "resolved", "agent:third",
                                  t0 + timedelta(days=2), 0.9)
    d = await entity_dossier(actions.pool, obj)
    assert d["status"] == "resolved"
    # the full disagreement is STILL visible in properties (#102: mark, never resolve away)
    status_prop = next(p for p in d["properties"] if p["name"] == "status")
    assert status_prop["agreement"] == "contradicting"
    assert {v["value"] for v in status_prop["values"]} == {"open", "resolved"}


async def test_dossier_status_is_none_for_a_type_with_no_status_assertion(
    actions: Actions,
) -> None:
    """Most object types (Person, Company, ...) never carry a `status` PROPERTY at all —
    the new top-level `status` must stay honestly None rather than inventing one, while
    `object_status` still answers the lifecycle question it always did."""
    await ingest_ftm(actions, _FTM)
    p1 = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='P1'")
    d = await entity_dossier(actions.pool, p1)
    assert d["status"] is None
    assert d["object_status"] == "active"


async def test_dossier_resolves_own_name_and_neighbor_name_via_the_full_chain(
    actions: Actions,
) -> None:
    """Task #97 workstream 3: both the entity's own name and a neighbor's name used to
    check ONLY the `name` property — a Thread (summary, no name) or a Practice
    (statement, no name) rendered its raw canonical hash here, even in a dossier for
    an object the graph/table views already labelled correctly."""
    from datetime import UTC, datetime

    thread = await actions.create_or_find_object("Thread", "thread:dossiertest", "test")
    await actions.assert_property(thread, "summary", "a thread with no name property",
                                  "test", datetime.now(UTC), 0.9)
    practice = await actions.create_or_find_object("Practice", "practice:dossiertest", "test")
    await actions.assert_property(practice, "statement", "a practice with no name property",
                                  "test", datetime.now(UTC), 0.9)
    await actions.create_link(thread, practice, "linked_to", "test", datetime.now(UTC), 0.9)

    d = await entity_dossier(actions.pool, thread)
    assert d["name"] == "a thread with no name property"
    nbr = next(r for r in d["relationships"] if r["type"] == "linked_to")
    assert nbr["neighbor"]["name"] == "a practice with no name property"


async def test_dossier_already_surfaced_both_sides_of_a_contradiction_before_marking(
    actions: Actions,
) -> None:
    """VERIFICATION for task #102 (Thoth's DM 2279), done BEFORE building the mark: does
    dossier show BOTH contradicting assertions, or silently take the first?
    `current_assertions` (alembic 0001/0005) excludes ONLY assertions someone explicitly
    `supersedes`d — two DIFFERENT sources asserting DIFFERENT values on the same property,
    neither superseding the other, BOTH stay current by design. This proves
    entity_dossier's properties query (no LIMIT, no ORDER-BY-confidence-then-take-one)
    already read that whole multi-source set rather than silently collapsing to a winner —
    NOT an instrument defect of the valid_until family Imhotep fixed at 4610cb2. #102's
    actual gap, closed by the `agreement` field below, was that nothing NAMED whether the
    values it already returns agree or genuinely contradict."""
    from datetime import UTC, datetime

    obj = await actions.create_or_find_object("Thread", "thread:dosscontra", "test")
    now = datetime.now(UTC)
    await actions.assert_property(obj, "status", "open", "agent:alice", now, 0.9)
    await actions.assert_property(obj, "status", "resolved", "agent:bob", now, 0.9)

    d = await entity_dossier(actions.pool, obj)
    status = next(p for p in d["properties"] if p["name"] == "status")
    values = {v["value"] for v in status["values"]}
    sources = {v["source"] for v in status["values"]}
    # both genuinely contradicting values are present — not silently collapsed to one
    assert values == {"open", "resolved"}
    assert sources == {"agent:alice", "agent:bob"}
    assert len(status["values"]) == 2


async def test_dossier_marks_a_single_source_property_as_single(actions: Actions) -> None:
    from datetime import UTC, datetime

    obj = await actions.create_or_find_object("Thread", "thread:dosssingle", "test")
    await actions.assert_property(obj, "status", "open", "agent:alice",
                                  datetime.now(UTC), 0.9)
    d = await entity_dossier(actions.pool, obj)
    status = next(p for p in d["properties"] if p["name"] == "status")
    assert status["agreement"] == "single"
    assert len(status["values"]) == 1


async def test_dossier_marks_two_sources_with_the_same_value_as_agreeing(
    actions: Actions,
) -> None:
    """SAME tag, SAME data — the operator's rule names this ONE referent corroborated by
    two sources, never a conflict, and MUST render distinctly from a genuine contradiction
    (same tag, DIFFERENT data)."""
    from datetime import UTC, datetime

    obj = await actions.create_or_find_object("Thread", "thread:dossagree", "test")
    now = datetime.now(UTC)
    await actions.assert_property(obj, "status", "open", "agent:alice", now, 0.9)
    await actions.assert_property(obj, "status", "open", "agent:bob", now, 0.9)
    d = await entity_dossier(actions.pool, obj)
    status = next(p for p in d["properties"] if p["name"] == "status")
    assert status["agreement"] == "agreeing"
    assert len(status["values"]) == 2
    assert {v["value"] for v in status["values"]} == {"open"}


async def test_dossier_marks_two_sources_with_different_values_as_contradicting(
    actions: Actions,
) -> None:
    """The actual #102 payoff: MARKED, never resolved — no value dropped, ranked, or
    picked as a winner; both remain, now with the epistemic state named."""
    from datetime import UTC, datetime

    obj = await actions.create_or_find_object("Thread", "thread:dosscontra2", "test")
    now = datetime.now(UTC)
    await actions.assert_property(obj, "status", "open", "agent:alice", now, 0.9)
    await actions.assert_property(obj, "status", "resolved", "agent:bob", now, 0.9)
    d = await entity_dossier(actions.pool, obj)
    status = next(p for p in d["properties"] if p["name"] == "status")
    assert status["agreement"] == "contradicting"
    assert {v["value"] for v in status["values"]} == {"open", "resolved"}


async def test_dossier_marks_name_disagreement_without_the_display_field_picking_a_winner(
    actions: Actions,
) -> None:
    """THE NAME-PROPERTY GAP (Thoth msg 4292, Sekhmet's find, decision 7960db40): `name`
    used to be silently excluded from the agreement view — a UI-dedup call made before
    #102 existed, inherited by #102's own query without anyone revisiting it. `name` now
    joins the SAME vocabulary as every other property (single/agreeing/contradicting), and
    the top-level `name` field (resolve_label's own silent winner-pick, task #97/ruling
    52daab71) must keep answering a DIFFERENT question — "what to display" — never leaking
    into or being replaced by the agreement mark. This is repo:bytebye's/repo:tony's own
    live shape in miniature: two different sources, two different spellings, neither
    superseding the other."""
    from datetime import UTC, datetime

    obj = await actions.create_or_find_object("Domain", "domain:namegap", "test")
    now = datetime.now(UTC)
    await actions.assert_property(obj, "name", "ByeByte", "agent:first", now, 0.9)
    await actions.assert_property(obj, "name", "byebyte", "agent:second", now, 0.9)

    d = await entity_dossier(actions.pool, obj)
    name_prop = next(p for p in d["properties"] if p["name"] == "name")
    assert name_prop["agreement"] == "contradicting"
    assert {v["value"] for v in name_prop["values"]} == {"ByeByte", "byebyte"}
    # the top-level display field is still a single, resolved string — never a list, never
    # the agreement mark itself; it answers "what to show", not "do sources disagree"
    assert d["name"] in {"ByeByte", "byebyte"}
    assert isinstance(d["name"], str)


async def test_dossier_tag_stays_excluded_from_the_agreement_view(actions: Actions) -> None:
    """`tag` keeps its own, separate, still-correct exclusion (dossier.py's own comment):
    additive/multi-valued by design, no winner or disagreement concept applies to it the
    way it does to a single-fact property like `name` or `status`."""
    obj = await actions.create_or_find_object("Domain", "domain:tagstays", "test")
    await actions.tag_object(obj, "flagged", "session", "agent:alice")
    await actions.tag_object(obj, "reviewed", "session", "agent:bob")

    d = await entity_dossier(actions.pool, obj)
    prop_names = {p["name"] for p in d["properties"]}
    assert "tag" not in prop_names


async def test_dossier_relationships_filter_invalidated_links(actions: Actions) -> None:
    """Task #114 (thread 7b258b5f, found by Thoth closing #99): a link healed by
    invalidate_link (valid_until stamped, never deleted) used to render identically to a
    live one — the exact shape that produced a false-urgent finding published as fact
    (a managed_by edge read as active a full day after it was invalidated). A still-live
    link on the SAME (type, direction) must keep showing — this proves the filter is on
    validity, not a blanket drop of the edge type."""
    from datetime import UTC, datetime, timedelta

    NOW = datetime.now(UTC)
    a = await actions.create_or_find_object("Thread", "thread:dossvalid-a", "test")
    dead_nbr = await actions.create_or_find_object("Thread", "thread:dossvalid-dead", "test")
    live_nbr = await actions.create_or_find_object("Thread", "thread:dossvalid-live", "test")
    await actions.create_link(a, dead_nbr, "linked_to", "test", NOW, 0.9)
    await actions.create_link(a, live_nbr, "linked_to", "test", NOW, 0.9)
    await actions.invalidate_link(a, dead_nbr, "linked_to", "test", NOW - timedelta(days=1))

    d = await entity_dossier(actions.pool, a)
    neighbor_ids = {r["neighbor"]["id"] for r in d["relationships"]}
    assert str(dead_nbr) not in neighbor_ids, "an invalidated link still rendered as active"
    assert str(live_nbr) in neighbor_ids, "the filter dropped a genuinely live link too"


async def test_dossier_resolves_a_fleet_handle_to_the_real_agent_not_a_sidechain(
    actions: Actions,
) -> None:
    """Task #114 (thread 05a72d2c0af0, found by Seshat XIII): dossier("sekhmet") returned
    "sekhmet I.1" — a harness sidechain artifact whose own label merely CONTAINED the
    handle — ahead of the real agent, reachable only by following that artifact's own
    spawned_by edge. resolve_ref now tries agents.resolve_seat (mail's own battle-tested
    handle resolver, which explicitly excludes spawned_by visitors) before falling
    through to the generic name-substring legs that have no concept of "visitor" at all."""
    from datetime import UTC, datetime

    from src import mcp_server as srv

    NOW = datetime.now(UTC)
    real = await actions.create_or_find_object("Agent", "agent:handletest99", "test")
    await actions.assert_property(real, "handle", "handletest99", "test", NOW, 0.95)
    # the decoy: a harness sidechain artifact whose OWN name merely contains the handle as
    # a substring — shorter than any real name resolve_ref's ILIKE leg would otherwise favor
    decoy = await actions.create_or_find_object("Thread", "thread:handletest99decoy", "test")
    await actions.assert_property(decoy, "name", "handletest99 I.1", "test", NOW, 0.95)

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.dossier("handletest99")
    finally:
        srv._pool = saved_pool
    assert out["id"] == str(real), "resolved to the sidechain artifact, not the real agent"


async def test_the_mcp_dossier_tool_resolves_a_short_id(actions: Actions) -> None:
    """task #64 (ruling ad19a779): every id a composition ROW hands out (a table/Function
    row's own 8-char "id" column) must feed straight back into dossier(), not just
    recall(). Before the resolve_ref fix, this returned {"error": "no object ..."} — proven
    directly against the real MCP tool (srv._pool swap, mirrors test_describe.py's own
    pattern), not just the lower-level resolve_ref function."""
    from src import mcp_server as srv

    await ingest_ftm(actions, _FTM)
    p1 = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='P1'")
    short = str(p1)[:8]

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.dossier(short)
    finally:
        srv._pool = saved_pool
    assert out["name"] == "Kim Jong Un"


async def test_dossier_endpoint(client: httpx.AsyncClient, actions: Actions) -> None:
    await ingest_ftm(actions, _FTM)
    p1 = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='P1'")

    r = await client.get(f"/objects/{p1}/dossier")
    assert r.status_code == 200
    assert r.json()["name"] == "Kim Jong Un"

    missing = await client.get(f"/objects/{uuid.uuid4()}/dossier")
    assert missing.status_code == 404
