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

    # identity properties (name/tag excluded; multi-source set preserved)
    prop_names = {p["name"] for p in d["properties"]}
    assert {"country", "birthDate", "topics"} <= prop_names
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
