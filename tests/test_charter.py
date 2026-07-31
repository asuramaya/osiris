"""THE CHARTER — a house is what a seat RULES, not where it sits (Phase 1 §4.1, `dd47c1da`).

set_charter mints `governs` links (Agent → SoftwareProject); a repo dropped from a later call
is healed by a COMPENSATING EVENT (`links.valid_until`), never deleted — these tests prove the
mint, the read-back, and the heal.
"""
from __future__ import annotations

from src.actions.core import Actions
from src.orchestrator.charter import charter_of, set_charter


async def _agent(actions: Actions, canonical: str) -> None:
    await actions.create_or_find_object("Agent", canonical, canonical)


async def test_set_charter_mints_governs_links(actions: Actions) -> None:
    await _agent(actions, "agent:steward")
    out = await set_charter(actions, "agent:steward", ["osiris", "bytebye"])
    assert out["charter"] == ["bytebye", "osiris"]  # sorted
    assert out["added"] == ["bytebye", "osiris"]
    assert out["removed"] == []
    assert await charter_of(actions.pool, "agent:steward") == ["bytebye", "osiris"]
    # real graph edges, not a property — governs, Agent -> SoftwareProject, active
    links = await actions.pool.fetch(
        "SELECT p.canonical, l.valid_until FROM links l "
        "JOIN objects a ON a.id=l.from_id AND a.canonical='agent:steward' "
        "JOIN objects p ON p.id=l.to_id WHERE l.type='governs' ORDER BY p.canonical")
    assert [r["canonical"] for r in links] == ["repo:bytebye", "repo:osiris"]
    assert all(r["valid_until"] is None for r in links)


async def test_set_charter_is_idempotent(actions: Actions) -> None:
    await _agent(actions, "agent:steward2")
    await set_charter(actions, "agent:steward2", ["a", "b"])
    again = await set_charter(actions, "agent:steward2", ["a", "b"])
    assert again == {"agent": "agent:steward2", "charter": ["a", "b"], "added": [], "removed": []}
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links l JOIN objects a ON a.id=l.from_id "
        "WHERE a.canonical='agent:steward2' AND l.type='governs'")
    assert n == 2  # no duplicate mint on a no-op call


async def test_set_charter_removal_heals_by_compensating_event(actions: Actions) -> None:
    """A repo dropped from the charter is healed (valid_until stamped), never deleted — the
    row survives, readable, exactly like every other retraction in this graph."""
    await _agent(actions, "agent:steward3")
    await set_charter(actions, "agent:steward3", ["osiris", "sibling-two", "bytebye"])
    amend = await set_charter(actions, "agent:steward3", ["osiris", "bytebye"])
    assert amend["added"] == []
    assert amend["removed"] == ["sibling-two"]
    assert await charter_of(actions.pool, "agent:steward3") == ["bytebye", "osiris"]
    # the dropped repo's link STILL EXISTS — healed, not deleted
    row = await actions.pool.fetchrow(
        "SELECT l.valid_until FROM links l "
        "JOIN objects a ON a.id=l.from_id AND a.canonical='agent:steward3' "
        "JOIN objects p ON p.id=l.to_id AND p.canonical='repo:sibling-two' "
        "WHERE l.type='governs'")
    assert row is not None and row["valid_until"] is not None
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links l JOIN objects a ON a.id=l.from_id "
        "WHERE a.canonical='agent:steward3' AND l.type='governs'")
    assert n == 3  # nothing deleted — all three rows remain


async def test_set_charter_readd_after_removal_mints_a_fresh_link(actions: Actions) -> None:
    """A repo that left the charter and returns gets a NEW governs link (a fresh grant), not a
    resurrection of the healed one — the old row's heal stays true forever."""
    await _agent(actions, "agent:steward4")
    await set_charter(actions, "agent:steward4", ["osiris"])
    await set_charter(actions, "agent:steward4", [])  # drop everything
    assert await charter_of(actions.pool, "agent:steward4") == []
    back = await set_charter(actions, "agent:steward4", ["osiris"])
    assert back["added"] == ["osiris"] and back["removed"] == []
    assert await charter_of(actions.pool, "agent:steward4") == ["osiris"]
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links l JOIN objects a ON a.id=l.from_id "
        "WHERE a.canonical='agent:steward4' AND l.type='governs'")
    assert n == 2  # the healed grant + the fresh one, both on the record


async def test_charter_of_is_empty_for_an_unchartered_seat(actions: Actions) -> None:
    await _agent(actions, "agent:plain")
    assert await charter_of(actions.pool, "agent:plain") == []


def test_schema_declares_governs() -> None:
    from src.ontology.schema import LINK_TYPES

    assert "governs" in LINK_TYPES
    assert LINK_TYPES["governs"].domain == ("Agent",)
    assert LINK_TYPES["governs"].range == ("SoftwareProject",)


# ═══ _lineage_charter — Lane C, decision 1913683e: charter_of is agent-EXACT, so a governs
# link declared by an earlier generation reads back EMPTY for its own successor (proven live
# on Imhotep's own seat). _lineage_charter walks the whole lineage; these tests pin both the
# fix and the over-match risk Thoth named it against. ═══


async def test_lineage_charter_reads_forward_from_an_ancestor_generation(
    actions: Actions,
) -> None:
    """The exact bug: a charter declared by generation 1 must still read back for
    generation 2 — a DIFFERENT Agent object, no governs link of its own."""
    from src.orchestrator.offices import _lineage_charter

    await _agent(actions, "agent:linchart")
    await set_charter(actions, "agent:linchart", ["osiris"])
    heir = "agent:linchart-ii"
    await _agent(actions, heir)
    assert await charter_of(actions.pool, heir) == []          # the OLD bug, still true
    assert await _lineage_charter(actions.pool, heir) == ["osiris"]  # the fix


async def test_lineage_charter_negative_control_reports_none_for_an_unrelated_lineage(
    actions: Actions,
) -> None:
    """A lineage that never declared a charter reports none — even in a graph where OTHER
    lineages have real charters on record."""
    from src.orchestrator.offices import _lineage_charter

    await _agent(actions, "agent:haschart")
    await set_charter(actions, "agent:haschart", ["osiris"])
    await _agent(actions, "agent:nochart")
    assert await _lineage_charter(actions.pool, "agent:nochart") == []


async def test_lineage_charter_does_not_over_match_a_similarly_prefixed_root(
    actions: Actions,
) -> None:
    """THE OVER-MATCH RISK, NAMED AND KILLED (Thoth's gate, msg 2393): the LIKE-prefix walk
    (`base || '-%'`) must require the hyphen boundary — 'agent:ab' governing something must
    never leak into 'agent:abc's lineage just because 'ab' is a literal string prefix of
    'abc'. Only a genuine `-ii`/`-iii`/... generation suffix counts as the same lineage."""
    from src.orchestrator.offices import _lineage_charter

    await _agent(actions, "agent:ab")
    await set_charter(actions, "agent:ab", ["osiris"])
    await _agent(actions, "agent:abc")  # a DIFFERENT root, not a generation of "agent:ab"
    assert await _lineage_charter(actions.pool, "agent:abc") == []
    # and the true lineage is unaffected by the near-miss existing alongside it
    assert await _lineage_charter(actions.pool, "agent:ab") == ["osiris"]


async def test_charter_tool_reads_the_lineage_not_just_the_exact_generation(
    actions: Actions,
) -> None:
    """Integration: the charter() MCP tool's no-args read path, through a real succession."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    await _agent(actions, "agent:chtool1")
    await set_charter(actions, "agent:chtool1", ["osiris", "bytebye"])
    heir = "agent:chtool1-ii"
    await _agent(actions, heir)

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id=heir, session="chtool1", project="chtoolproj", model=None, cwd=None)
    try:
        out = await srv.charter(ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert out == {"agent": heir, "charter": ["bytebye", "osiris"]}


async def test_orient_tool_surfaces_the_lineage_charter(actions: Actions) -> None:
    """Integration: orient()'s own charter line — the exact live case Lane C proved (a
    seat's CLAUDE.md says 'You govern: X' but orient() showed nothing) — through a real
    successor identity."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    await _agent(actions, "agent:orichart1")
    await set_charter(actions, "agent:orichart1", ["osiris"])
    heir = "agent:orichart1-ii"
    await _agent(actions, heir)

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id=heir, session="orichart1", project="orichartproj", model=None, cwd=None)
    try:
        out = await srv.orient(ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert out.get("charter") == ["osiris"]
