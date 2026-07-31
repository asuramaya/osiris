"""THE CHARTER — a house is what a seat RULES, not where it sits (Phase 1 §4.1, `dd47c1da`).

RE-KEYED ONTO THE SEAT (operator ruling 1db1ff41): set_charter/charter_of mint and read
`governs` links (Seat → SoftwareProject), never Agent → SoftwareProject — these tests prove
the mint, the read-back, the heal, AND the two things the old agent-keyed model got wrong:
a successor couldn't heal an ancestor's grant (dissolved here, one seat/one link/no
generations), and a self-declared string alone could mint a fake repo (atlas's garbled
charter) — set_charter now refuses any name the graph has no independent evidence for.
"""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator.charter import charter_of, migrate_charter_to_seat, set_charter

NOW = datetime.now(UTC)


async def _seat(actions: Actions, handle: str) -> str:
    """A fresh, unheld Seat for this handle."""
    from src.orchestrator.seats import ensure_seat

    out = await ensure_seat(actions, house="osiris", handle=handle, source="test")
    return str(out["seat_id"])


async def _seated(actions: Actions, agent_id: str, handle: str) -> str:
    """An Agent bound to a fresh Seat via `holds` — returns the seat_id."""
    from src.orchestrator.seats import bind_holder

    seat_id = await _seat(actions, handle)
    await bind_holder(actions, seat_id=seat_id, agent_id=agent_id)
    return seat_id


async def _agent(actions: Actions, canonical: str) -> None:
    await actions.create_or_find_object("Agent", canonical, canonical)


async def _repo(actions: Actions, name: str) -> None:
    """Pre-mint a SoftwareProject — simulates 'the graph already has independent evidence
    this repo is real' (a prior git ingest, in production; a bare mint here in tests)."""
    await actions.create_or_find_object("SoftwareProject", f"repo:{name}", "test")


async def test_set_charter_mints_governs_links(actions: Actions) -> None:
    seat_id = await _seated(actions, "agent:steward", "Steward")
    await _repo(actions, "osiris")
    await _repo(actions, "bytebye")
    out = await set_charter(actions, seat_id, ["osiris", "bytebye"], actor="agent:steward")
    assert out["charter"] == ["bytebye", "osiris"]  # sorted
    assert out["added"] == ["bytebye", "osiris"]
    assert out["removed"] == []
    assert await charter_of(actions.pool, seat_id) == ["bytebye", "osiris"]
    # real graph edges, not a property — governs, Seat -> SoftwareProject, active
    links = await actions.pool.fetch(
        "SELECT p.canonical, l.valid_until FROM links l "
        "JOIN objects s ON s.id=l.from_id AND s.canonical=$1 "
        "JOIN objects p ON p.id=l.to_id WHERE l.type='governs' ORDER BY p.canonical",
        seat_id)
    assert [r["canonical"] for r in links] == ["repo:bytebye", "repo:osiris"]
    assert all(r["valid_until"] is None for r in links)


async def test_set_charter_is_idempotent(actions: Actions) -> None:
    seat_id = await _seated(actions, "agent:steward2", "Steward2")
    await _repo(actions, "a")
    await _repo(actions, "b")
    await set_charter(actions, seat_id, ["a", "b"], actor="agent:steward2")
    again = await set_charter(actions, seat_id, ["a", "b"], actor="agent:steward2")
    assert again == {"seat": seat_id, "charter": ["a", "b"], "added": [], "removed": []}
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links l JOIN objects s ON s.id=l.from_id "
        "WHERE s.canonical=$1 AND l.type='governs'", seat_id)
    assert n == 2  # no duplicate mint on a no-op call


async def test_set_charter_removal_heals_by_compensating_event(actions: Actions) -> None:
    """A repo dropped from the charter is healed (valid_until stamped), never deleted — the
    row survives, readable, exactly like every other retraction in this graph."""
    seat_id = await _seated(actions, "agent:steward3", "Steward3")
    for r in ("osiris", "sibling-two", "bytebye"):
        await _repo(actions, r)
    await set_charter(actions, seat_id, ["osiris", "sibling-two", "bytebye"],
                      actor="agent:steward3")
    amend = await set_charter(actions, seat_id, ["osiris", "bytebye"], actor="agent:steward3")
    assert amend["added"] == []
    assert amend["removed"] == ["sibling-two"]
    assert await charter_of(actions.pool, seat_id) == ["bytebye", "osiris"]
    # the dropped repo's link STILL EXISTS — healed, not deleted
    row = await actions.pool.fetchrow(
        "SELECT l.valid_until FROM links l "
        "JOIN objects s ON s.id=l.from_id AND s.canonical=$1 "
        "JOIN objects p ON p.id=l.to_id AND p.canonical='repo:sibling-two' "
        "WHERE l.type='governs'", seat_id)
    assert row is not None and row["valid_until"] is not None
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links l JOIN objects s ON s.id=l.from_id "
        "WHERE s.canonical=$1 AND l.type='governs'", seat_id)
    assert n == 3  # nothing deleted — all three rows remain


async def test_set_charter_readd_after_removal_mints_a_fresh_link(actions: Actions) -> None:
    """A repo that left the charter and returns gets a NEW governs link (a fresh grant), not a
    resurrection of the healed one — the old row's heal stays true forever."""
    seat_id = await _seated(actions, "agent:steward4", "Steward4")
    await _repo(actions, "osiris")
    await set_charter(actions, seat_id, ["osiris"], actor="agent:steward4")
    await set_charter(actions, seat_id, [], actor="agent:steward4")  # drop everything
    assert await charter_of(actions.pool, seat_id) == []
    back = await set_charter(actions, seat_id, ["osiris"], actor="agent:steward4")
    assert back["added"] == ["osiris"] and back["removed"] == []
    assert await charter_of(actions.pool, seat_id) == ["osiris"]
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links l JOIN objects s ON s.id=l.from_id "
        "WHERE s.canonical=$1 AND l.type='governs'", seat_id)
    assert n == 2  # the healed grant + the fresh one, both on the record


async def test_charter_of_is_empty_for_an_unchartered_seat(actions: Actions) -> None:
    seat_id = await _seat(actions, "Plain")
    assert await charter_of(actions.pool, seat_id) == []


def test_schema_declares_governs() -> None:
    from src.ontology.schema import LINK_TYPES

    assert "governs" in LINK_TYPES
    assert LINK_TYPES["governs"].domain == ("Seat",)
    assert LINK_TYPES["governs"].range == ("SoftwareProject",)


# ═══ repo-name validation (Thoth's design constraint, msg 2402): a charter is SELF_DECLARED
# evidence and must never be the first and only witness that a repo exists — set_charter
# refuses any name the graph has no independent evidence for, rather than minting it. ═══


async def test_set_charter_refuses_an_unknown_repo_name(actions: Actions) -> None:
    seat_id = await _seated(actions, "agent:validate1", "Validate1")
    out = await set_charter(actions, seat_id, ["Us"], actor="agent:validate1")
    assert out["added"] == [] and out["charter"] == []
    assert out["rejected"] == [{"repo": "Us", "error": out["rejected"][0]["error"]}]
    assert await charter_of(actions.pool, seat_id) == []
    # never minted as a side effect of the refusal
    assert await actions.pool.fetchval(
        "SELECT 1 FROM objects WHERE canonical='repo:Us'") is None


async def test_set_charter_partial_apply_when_one_name_is_unknown(actions: Actions) -> None:
    """One bad item never sinks the whole batch — the same discipline settle() runs."""
    seat_id = await _seated(actions, "agent:validate2", "Validate2")
    await _repo(actions, "osiris")
    out = await set_charter(actions, seat_id, ["osiris", "vector"], actor="agent:validate2")
    assert out["added"] == ["osiris"]
    assert out["charter"] == ["osiris"]
    assert [r["repo"] for r in out["rejected"]] == ["vector"]
    assert await charter_of(actions.pool, seat_id) == ["osiris"]


async def test_set_charter_a_near_miss_caps_variant_is_not_conflated(actions: Actions) -> None:
    """RAMstein/ramstein, ByeByte/byebyte (Thoth's named cases): exact match only, no fuzzy
    resolution — a caps-variant of a real repo is a DIFFERENT, unknown string."""
    seat_id = await _seated(actions, "agent:validate3", "Validate3")
    await _repo(actions, "bytebyte")
    out = await set_charter(actions, seat_id, ["ByteByte"], actor="agent:validate3")
    assert out["added"] == [] and out["rejected"][0]["repo"] == "ByteByte"


async def test_set_charter_refuses_for_an_unknown_seat(actions: Actions) -> None:
    out = await set_charter(actions, "seat:doesnotexist", ["osiris"], actor="agent:x")
    assert "error" in out
    assert await actions.pool.fetchval(
        "SELECT 1 FROM objects WHERE canonical='repo:osiris'") is None


# ═══ succession dissolves invalidate_link's exact-from_id limitation (Thoth's explicit gate,
# msg 2402): one seat, one link, no generations — a successor re-declaring now heals the SAME
# row an ancestor generation minted, not a duplicate the ancestor's row survives alongside. ═══


async def test_set_charter_a_successor_heals_the_link_its_ancestor_declared(
    actions: Actions,
) -> None:
    from src.orchestrator.seats import bind_holder

    seat_id = await _seated(actions, "agent:succ", "Succ")
    for r in ("osiris", "bytebye"):
        await _repo(actions, r)
    await set_charter(actions, seat_id, ["osiris", "bytebye"], actor="agent:succ")
    link_row = await actions.pool.fetchrow(
        "SELECT l.id FROM links l JOIN objects p ON p.id=l.to_id "
        "AND p.canonical='repo:bytebye' WHERE l.type='governs'")
    # a SUCCESSOR takes the same seat — the ancestor's holds link heals, a new one binds
    await bind_holder(actions, seat_id=seat_id, agent_id="agent:succ-ii")
    dropped = await set_charter(actions, seat_id, ["osiris"], actor="agent:succ-ii")
    assert dropped["removed"] == ["bytebye"]
    healed_row = await actions.pool.fetchrow(
        "SELECT l.id, l.valid_until FROM links l JOIN objects p ON p.id=l.to_id "
        "AND p.canonical='repo:bytebye' WHERE l.type='governs'")
    assert healed_row["id"] == link_row["id"]  # the SAME row, not a second one left dangling
    assert healed_row["valid_until"] is not None
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links l JOIN objects s ON s.id=l.from_id "
        "WHERE s.canonical=$1 AND l.type='governs'", seat_id)
    assert n == 2  # osiris (still active) + the one healed bytebye row — no duplicate


# ═══ migrate_charter_to_seat — the one-time migration off the old Agent-keyed model ═══


async def test_migrate_charter_to_seat_dry_run_writes_nothing(actions: Actions) -> None:
    await _agent(actions, "agent:mig1")
    await _repo(actions, "osiris")
    seat_id = await _seated(actions, "agent:mig1", "Mig1")
    # simulate a LEGACY Agent-origin governs link (what set_charter used to mint)
    a_oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='agent:mig1'")
    p_oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='repo:osiris'")
    await actions.create_link(a_oid, p_oid, "governs", "agent:mig1", NOW, 0.9)
    out = await migrate_charter_to_seat(actions, dry_run=True, only_seats={seat_id})
    assert out["applied"] is False
    assert out["plan"] == [{"seat_id": seat_id, "repos": ["osiris"],
                            "from_agents": ["agent:mig1"]}]
    assert await charter_of(actions.pool, seat_id) == []  # nothing written yet
    still_active = await actions.pool.fetchval(
        "SELECT valid_until FROM links WHERE from_id=$1 AND to_id=$2 AND type='governs'",
        a_oid, p_oid)
    assert still_active is None  # the old link is untouched


async def test_migrate_charter_to_seat_moves_a_single_agents_charter(actions: Actions) -> None:
    await _agent(actions, "agent:mig2")
    await _repo(actions, "osiris")
    seat_id = await _seated(actions, "agent:mig2", "Mig2")
    a_oid = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='agent:mig2'")
    p_oid = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='repo:osiris'")
    await actions.create_link(a_oid, p_oid, "governs", "agent:mig2", NOW, 0.9)
    out = await migrate_charter_to_seat(actions, dry_run=False, only_seats={seat_id})
    assert out["applied"] is True
    assert out["seats_migrated"] == 1
    assert await charter_of(actions.pool, seat_id) == ["osiris"]
    healed = await actions.pool.fetchval(
        "SELECT valid_until FROM links WHERE from_id=$1 AND to_id=$2 AND type='governs'",
        a_oid, p_oid)
    assert healed is not None  # the OLD Agent-origin link is healed, never deleted
    still_there = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND to_id=$2 AND type='governs'",
        a_oid, p_oid)
    assert still_there == 1  # the row survives


async def test_migrate_charter_to_seat_unions_across_generations(actions: Actions) -> None:
    """The exact accumulation bug this ruling closes: generation I declared osiris+bytebye,
    generation II (a re-declaration the old model couldn't heal) declared osiris+sibling —
    migration must union what the LINEAGE ever actively declared, not just one generation."""
    from src.orchestrator.seats import bind_holder

    await _agent(actions, "agent:mig3")
    await _agent(actions, "agent:mig3-ii")
    for r in ("osiris", "bytebye", "sibling"):
        await _repo(actions, r)
    seat_id = await _seated(actions, "agent:mig3", "Mig3")
    await bind_holder(actions, seat_id=seat_id, agent_id="agent:mig3-ii")
    a1 = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='agent:mig3'")
    a2 = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='agent:mig3-ii'")
    osiris = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='repo:osiris'")
    bytebye = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='repo:bytebye'")
    sibling = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='repo:sibling'")
    await actions.create_link(a1, osiris, "governs", "agent:mig3", NOW, 0.9)
    await actions.create_link(a1, bytebye, "governs", "agent:mig3", NOW, 0.9)
    await actions.create_link(a2, osiris, "governs", "agent:mig3-ii", NOW, 0.9)
    await actions.create_link(a2, sibling, "governs", "agent:mig3-ii", NOW, 0.9)
    out = await migrate_charter_to_seat(actions, dry_run=False, only_seats={seat_id})
    assert out["seats_migrated"] == 1
    assert await charter_of(actions.pool, seat_id) == ["bytebye", "osiris", "sibling"]
    for oid, pid in ((a1, osiris), (a1, bytebye), (a2, osiris), (a2, sibling)):
        healed = await actions.pool.fetchval(
            "SELECT valid_until FROM links WHERE from_id=$1 AND to_id=$2 AND type='governs'",
            oid, pid)
        assert healed is not None


async def test_migrate_charter_to_seat_reports_and_skips_an_unseated_agent(
    actions: Actions,
) -> None:
    """An agent whose lineage holds NO seat (never attached/claimed) is reported, never
    guessed — its link is left untouched, a named residual."""
    await _agent(actions, "agent:mig4")
    await _repo(actions, "osiris")
    a_oid = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='agent:mig4'")
    p_oid = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='repo:osiris'")
    await actions.create_link(a_oid, p_oid, "governs", "agent:mig4", NOW, 0.9)
    out = await migrate_charter_to_seat(actions, dry_run=True)
    hit = [u for u in out["unresolved"] if u["agent_id"] == "agent:mig4"]
    assert hit == [{"agent_id": "agent:mig4", "repo": "osiris",
                    "note": hit[0]["note"]}]
    assert not any(p["from_agents"] == ["agent:mig4"] for p in out["plan"])


async def test_migrate_charter_to_seat_negative_control_undeclared_seat_stays_empty(
    actions: Actions,
) -> None:
    """A seat that never declared anything reports none afterward — a migration run must
    never invent a charter for a seat that was never party to any legacy governs link."""
    seat_id = await _seated(actions, "agent:mig5", "Mig5")
    await migrate_charter_to_seat(actions, dry_run=False, only_seats={seat_id})
    assert await charter_of(actions.pool, seat_id) == []


async def test_migrate_charter_to_seat_does_not_cross_contaminate_lineages(
    actions: Actions,
) -> None:
    """No charter lands on the wrong seat: two independent lineages, each with its own
    legacy-declared repo, must migrate to two DISTINCT, non-overlapping charters."""
    await _agent(actions, "agent:mig6a")
    await _agent(actions, "agent:mig6b")
    await _repo(actions, "reponly-a")
    await _repo(actions, "reponly-b")
    seat_a = await _seated(actions, "agent:mig6a", "Mig6a")
    seat_b = await _seated(actions, "agent:mig6b", "Mig6b")
    a_oid = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='agent:mig6a'")
    b_oid = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='agent:mig6b'")
    pa = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='repo:reponly-a'")
    pb = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='repo:reponly-b'")
    await actions.create_link(a_oid, pa, "governs", "agent:mig6a", NOW, 0.9)
    await actions.create_link(b_oid, pb, "governs", "agent:mig6b", NOW, 0.9)
    await migrate_charter_to_seat(actions, dry_run=False, only_seats={seat_a, seat_b})
    assert await charter_of(actions.pool, seat_a) == ["reponly-a"]
    assert await charter_of(actions.pool, seat_b) == ["reponly-b"]


async def test_migrate_charter_to_seat_is_idempotent_on_a_second_run(actions: Actions) -> None:
    await _agent(actions, "agent:mig7")
    await _repo(actions, "osiris")
    seat_id = await _seated(actions, "agent:mig7", "Mig7")
    a_oid = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='agent:mig7'")
    p_oid = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='repo:osiris'")
    await actions.create_link(a_oid, p_oid, "governs", "agent:mig7", NOW, 0.9)
    await migrate_charter_to_seat(actions, dry_run=False, only_seats={seat_id})
    again = await migrate_charter_to_seat(actions, dry_run=False, only_seats={seat_id})
    assert again["plan"] == []  # no active Agent-origin governs links left to find
    assert again["seats_migrated"] == 0
    assert await charter_of(actions.pool, seat_id) == ["osiris"]  # unchanged


# ═══ mcp_server.py integration — orient()/charter() resolve the caller's SEAT first ═══


class _Ctx:
    class request_context:  # noqa: N801
        request = None
        session = object()


async def test_charter_tool_a_stranger_declares_its_own_charter_unaided(
    actions: Actions,
) -> None:
    """THE ACCEPTANCE BAR (operator, ruling 1db1ff41): a seat arriving fresh, with no batch
    and no operator, must declare and be right on its FIRST call. A bare attach (bind_holder,
    the same primitive the daemon's birth ceremony runs) is enough — no claim_name, no prior
    charter, no lineage history."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    await _repo(actions, "osiris")
    seat_id = await _seated(actions, "agent:stranger1", "Stranger1")
    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:stranger1", session="s1", project="strangerproj", model=None, cwd=None)
    try:
        out = await srv.charter(repos=["osiris"], ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert out["charter"] == ["osiris"] and out["added"] == ["osiris"]
    assert await charter_of(actions.pool, seat_id) == ["osiris"]


async def test_charter_tool_refuses_an_unseated_identity(actions: Actions) -> None:
    """An identity that holds no seat at all is refused, plainly — never silently keyed on
    the bare Agent id, which is the exact bug ruling 1db1ff41 closes."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    await _agent(actions, "agent:unseated1")
    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:unseated1", session="u1", project="unseatedproj", model=None, cwd=None)
    try:
        out = await srv.charter(ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert "error" in out


async def test_charter_tool_reads_through_a_re_seated_successor(actions: Actions) -> None:
    """Integration: a DIFFERENT Agent generation bound to the SAME seat sees the SAME
    charter — succession transparency now falls out of seat-keying for free, no lineage
    walk needed at the mcp_server layer at all."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity
    from src.orchestrator.seats import bind_holder

    await _repo(actions, "osiris")
    await _repo(actions, "bytebye")
    seat_id = await _seated(actions, "agent:chtool1", "Chtool1")
    await set_charter(actions, seat_id, ["osiris", "bytebye"], actor="agent:chtool1")
    heir = "agent:chtool1-ii"
    await _agent(actions, heir)
    await bind_holder(actions, seat_id=seat_id, agent_id=heir)

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
    assert out == {"agent": heir, "seat": seat_id, "charter": ["bytebye", "osiris"]}


async def test_orient_tool_surfaces_the_seats_charter(actions: Actions) -> None:
    """Integration: orient()'s own charter line — the exact live case Lane C proved (a
    seat's CLAUDE.md says 'You govern: X' but orient() showed nothing) — now resolved via
    held_seat instead of a lineage-string walk."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity
    from src.orchestrator.seats import bind_holder

    await _repo(actions, "osiris")
    seat_id = await _seated(actions, "agent:orichart1", "Orichart1")
    await set_charter(actions, seat_id, ["osiris"], actor="agent:orichart1")
    heir = "agent:orichart1-ii"
    await _agent(actions, heir)
    await bind_holder(actions, seat_id=seat_id, agent_id=heir)

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


# ═══ charter_for — the manager-invoked sibling (thread 2446): a seat may declare its own
# charter, its manager may declare for it, the operator is every seat's ultimate manager. ═══


async def test_charter_for_the_manager_declares_successfully(actions: Actions) -> None:
    from src.orchestrator.charter import charter_for
    from src.orchestrator.seats import attach_seat

    await _repo(actions, "osiris")
    manager_seat = await _seated(actions, "agent:manager1", "Manager1")
    worker_seat = await _seat(actions, "Worker1")
    await attach_seat(actions, worker_seat, manager_seat, evidence="org chart", actor="test")
    out = await charter_for(actions, worker_seat, ["osiris"], because="onboarding",
                            actor="agent:manager1")
    assert out["charter"] == ["osiris"] and out["because"] == "onboarding"
    assert out["declared_by"] == "agent:manager1"
    assert await charter_of(actions.pool, worker_seat) == ["osiris"]


async def test_charter_for_refuses_a_non_manager_non_operator(actions: Actions) -> None:
    from src.orchestrator.charter import charter_for
    from src.orchestrator.seats import attach_seat

    await _repo(actions, "osiris")
    manager_seat = await _seated(actions, "agent:manager2", "Manager2")
    worker_seat = await _seat(actions, "Worker2")
    await attach_seat(actions, worker_seat, manager_seat, evidence="org chart", actor="test")
    await _seated(actions, "agent:stranger2", "Stranger2")  # a seat, but not the manager

    out = await charter_for(actions, worker_seat, ["osiris"], because="unauthorized try",
                            actor="agent:stranger2")
    assert "not authorized" in out["error"]
    assert manager_seat in out["error"] and "agent:stranger2" in out["error"]
    assert await charter_of(actions.pool, worker_seat) == []


async def test_charter_for_refuses_an_unmanaged_seat_from_a_non_manager(
    actions: Actions,
) -> None:
    """No manager on record must be named plainly, not left implicit."""
    from src.orchestrator.charter import charter_for

    await _repo(actions, "osiris")
    worker_seat = await _seat(actions, "Worker3")
    await _seated(actions, "agent:stranger3", "Stranger3")
    out = await charter_for(actions, worker_seat, ["osiris"], because="try anyway",
                            actor="agent:stranger3")
    assert "no manager on record" in out["error"]


async def test_charter_for_succeeds_for_an_operator_actor(actions: Actions) -> None:
    """One of seats._OPERATOR_ACTORS — authorized regardless of managed_by (the operator
    is every seat's ultimate manager)."""
    from src.orchestrator.charter import charter_for

    await _repo(actions, "osiris")
    worker_seat = await _seat(actions, "Worker4")  # no manager at all
    out = await charter_for(actions, worker_seat, ["osiris"], because="operator backfill",
                            actor="operator")
    assert out["charter"] == ["osiris"]
    assert await charter_of(actions.pool, worker_seat) == ["osiris"]


async def test_charter_for_refuses_blank_because(actions: Actions) -> None:
    from src.orchestrator.charter import charter_for

    await _repo(actions, "osiris")
    worker_seat = await _seat(actions, "Worker5")
    out = await charter_for(actions, worker_seat, ["osiris"], because="  ", actor="operator")
    assert "because is required" in out["error"]
    assert await charter_of(actions.pool, worker_seat) == []


async def test_charter_for_never_touches_a_legacy_agent_origin_governs_edge(
    actions: Actions,
) -> None:
    """Seshat's blocker 2, carried into the design (thread 2446): a seat with pre-existing
    Agent-origin governs links (the un-migrated shape) must see charter_for act ONLY on
    the Seat-origin side — the legacy row stays exactly as it was, neither healed nor
    duplicated, because set_charter's own SQL can never match it at all."""
    from src.orchestrator.charter import charter_for

    await _agent(actions, "agent:legacyholder")
    await _repo(actions, "legacyrepo")
    await _repo(actions, "newrepo")
    worker_seat = await _seated(actions, "agent:legacyholder", "LegacyWorker")
    a_oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='agent:legacyholder'")
    legacy_repo = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='repo:legacyrepo'")
    await actions.create_link(a_oid, legacy_repo, "governs", "agent:legacyholder", NOW, 0.9)

    out = await charter_for(actions, worker_seat, ["newrepo"], because="fresh declaration",
                            actor="operator")
    assert out["charter"] == ["newrepo"]
    legacy_still_active = await actions.pool.fetchval(
        "SELECT valid_until FROM links WHERE from_id=$1 AND to_id=$2 AND type='governs'",
        a_oid, legacy_repo)
    assert legacy_still_active is None  # untouched — never healed
    assert await charter_of(actions.pool, worker_seat) == ["newrepo"]  # legacy invisible here


async def test_charter_for_tool_the_manager_declares_through_the_wrapper(
    actions: Actions,
) -> None:
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity
    from src.orchestrator.seats import attach_seat

    await _repo(actions, "osiris")
    manager_seat = await _seated(actions, "agent:mgrtool1", "Mgrtool1")
    worker_seat = await _seat(actions, "Workertool1")
    await attach_seat(actions, worker_seat, manager_seat, evidence="org chart", actor="test")

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:mgrtool1", session="mgrtool1", project="mgrtoolproj", model=None,
        cwd=None)
    try:
        out = await srv.charter_for(seat_id=worker_seat, repos=["osiris"],
                                    because="onboarding via tool", ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert out["charter"] == ["osiris"] and out["declared_by"] == "agent:mgrtool1"


async def test_charter_for_tool_refuses_a_stranger_through_the_wrapper(
    actions: Actions,
) -> None:
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity
    from src.orchestrator.seats import attach_seat

    await _repo(actions, "osiris")
    manager_seat = await _seated(actions, "agent:mgrtool2", "Mgrtool2")
    worker_seat = await _seat(actions, "Workertool2")
    await attach_seat(actions, worker_seat, manager_seat, evidence="org chart", actor="test")
    await _seated(actions, "agent:strangertool2", "Strangertool2")

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:strangertool2", session="strangertool2", project="strangertoolproj",
        model=None, cwd=None)
    try:
        out = await srv.charter_for(seat_id=worker_seat, repos=["osiris"],
                                    because="unauthorized try", ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert "not authorized" in out["error"]
    assert await charter_of(actions.pool, worker_seat) == []


def test_charter_tool_stays_self_declaration_only() -> None:
    """charter() must never widen to accept a target — that would break the STRANGER
    acceptance bar this whole ruling was built on (ruling 1db1ff41)."""
    import inspect

    from src import mcp_server as srv

    params = set(inspect.signature(srv.charter).parameters)
    assert params == {"repos", "ctx"}
