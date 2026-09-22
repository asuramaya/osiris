"""PROVENANCE PIECE 1, READ-SET STAMPING AT THE DOOR (thread da545039f2ba, ruling
bb3e4422 "provenance by channel, not by text") — the durable read-set log, the
possible_upstream edges minted from it at write time, and credence.py's second,
orthogonal independence leg (distinct_upstreams) that reads them.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from src.actions.core import Actions
from src.orchestrator.credence import distinct_upstream_count, upstream_sets
from src.orchestrator.dossier import entity_dossier
from src.orchestrator.provenance import (
    message_object_id,
    stamp_possible_upstream,
    stamp_read,
    unread_message_ids,
)
from src.parsers.base import EvidenceClass

NOW = datetime(2026, 9, 14, tzinfo=UTC)
_EC = EvidenceClass.DIRECT_OBSERVATION.value


async def test_stamp_read_inserts_a_row(actions: Actions) -> None:
    o = await actions.create_or_find_object("Reference", "ref:pv-demo-1", "test")
    await stamp_read(actions.pool, agent_id="agent:reader", door="dossier", object_id=o)
    row = await actions.pool.fetchrow(
        "SELECT agent_id, door, object_id FROM session_reads WHERE object_id=$1", o)
    assert row is not None
    assert row["agent_id"] == "agent:reader" and row["door"] == "dossier"


async def test_stamp_read_none_object_id_is_a_noop(actions: Actions) -> None:
    await stamp_read(actions.pool, agent_id="agent:reader", door="dossier", object_id=None)
    n = await actions.pool.fetchval("SELECT count(*) FROM session_reads")
    assert n == 0


async def test_message_object_id_resolves_the_existing_message_object(actions: Actions) -> None:
    # the SAME mint mailbox.py's own send() path already does — this door only RESOLVES.
    msg_oid = await actions.create_or_find_object("Message", "message:4242", "agent:sender")
    resolved = await message_object_id(actions.pool, 4242, "agent:sender")
    assert resolved == msg_oid


async def test_message_object_id_returns_none_for_operator_sender(actions: Actions) -> None:
    # NEVER an edge to the operator's own words — refused at the door, even when a
    # Message object genuinely exists for this id.
    await actions.create_or_find_object("Message", "message:99", "operator")
    assert await message_object_id(actions.pool, 99, "operator") is None


async def test_message_object_id_returns_none_when_no_message_object_exists(
    actions: Actions,
) -> None:
    # the mail write's own graph edges are best-effort (mailbox.py's own guarded try);
    # a message whose Message object never landed is skipped, never a fresh mint from
    # the read side (the same existence-checked law mailbox.py holds itself to).
    assert await message_object_id(actions.pool, 55555, "agent:sender") is None


async def test_unread_message_ids_flags_an_id_never_read_through_inbox(
    actions: Actions,
) -> None:
    """THE FIRST-BREATH READ LAW (thread afd27e1a, Thoth mail 13003): a message this
    agent has never returned through a real inbox() call (inbox-lease or inbox-peek)
    comes back as unread — the exact check `inbox(ack=...)` gates on."""
    from src.orchestrator.mailbox import send_message

    out = await send_message(actions.pool, from_agent="agent:sender", from_project="osiris",
                             to_agent="agent:reader3", body="an ask")
    msg_id = int(out["id"])
    assert await unread_message_ids(actions.pool, [msg_id], agent_id="agent:reader3") == [
        msg_id]


async def test_unread_message_ids_clears_once_a_real_inbox_read_is_stamped(
    actions: Actions,
) -> None:
    """A door OTHER than inbox-lease/inbox-peek (e.g. a search hit surfacing the same
    Message object) does NOT satisfy the law — only a genuine inbox() call, peek or
    lease, does."""
    from src.orchestrator.mailbox import send_message

    out = await send_message(actions.pool, from_agent="agent:sender", from_project="osiris",
                             to_agent="agent:reader4", body="an ask")
    msg_id = int(out["id"])
    msg_oid = await message_object_id(actions.pool, msg_id, "agent:sender")
    assert msg_oid is not None

    await stamp_read(actions.pool, agent_id="agent:reader4", door="search", object_id=msg_oid)
    assert await unread_message_ids(actions.pool, [msg_id], agent_id="agent:reader4") == [
        msg_id]

    await stamp_read(actions.pool, agent_id="agent:reader4", door="inbox-peek",
                     object_id=msg_oid)
    assert await unread_message_ids(actions.pool, [msg_id], agent_id="agent:reader4") == []


async def test_unread_message_ids_fails_open_with_no_message_object(actions: Actions) -> None:
    # an id fleet_messages has never heard of (or whose Message object never landed)
    # cannot be checked against session_reads at all — this law gates what it can
    # actually observe, it never invents evidence it doesn't have.
    assert await unread_message_ids(actions.pool, [999999], agent_id="agent:reader5") == []


async def test_stamp_possible_upstream_only_reaches_reads_before_the_write(
    actions: Actions,
) -> None:
    o = await actions.create_or_find_object("SoftwareProject", "repo:pv-demo-2", "test")
    upstream_before = await actions.create_or_find_object("Reference", "ref:pv-before", "test")
    upstream_after = await actions.create_or_find_object("Reference", "ref:pv-after", "test")
    write_at = NOW
    await stamp_read(actions.pool, agent_id="agent:writer", door="dossier",
                     object_id=upstream_before, read_at=write_at - timedelta(minutes=5))
    await stamp_read(actions.pool, agent_id="agent:writer", door="dossier",
                     object_id=upstream_after, read_at=write_at + timedelta(minutes=5))
    minted = await stamp_possible_upstream(
        actions, written_object_id=o, source_id="agent:writer", observed_at=write_at)
    assert minted == 1
    rows = await actions.pool.fetch(
        "SELECT to_id, properties FROM links WHERE from_id=$1 AND type='possible_upstream'", o)
    assert len(rows) == 1 and rows[0]["to_id"] == upstream_before
    assert rows[0]["properties"]["door"] == "dossier"


async def test_stamp_possible_upstream_an_empty_read_set_writes_exactly_as_today(
    actions: Actions,
) -> None:
    o = await actions.create_or_find_object("SoftwareProject", "repo:pv-demo-3", "test")
    minted = await stamp_possible_upstream(
        actions, written_object_id=o, source_id="agent:blank-slate", observed_at=NOW)
    assert minted == 0
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND type='possible_upstream'", o)
    assert n == 0


async def test_stamp_possible_upstream_skips_a_self_read(actions: Actions) -> None:
    o = await actions.create_or_find_object("SoftwareProject", "repo:pv-demo-4", "test")
    await stamp_read(actions.pool, agent_id="agent:writer", door="dossier",
                     object_id=o, read_at=NOW - timedelta(minutes=5))
    minted = await stamp_possible_upstream(
        actions, written_object_id=o, source_id="agent:writer", observed_at=NOW)
    # a session that read the very object it now writes is not upstream of itself
    assert minted == 0


async def test_stamp_possible_upstream_never_duplicates_an_existing_edge(actions: Actions) -> None:
    o = await actions.create_or_find_object("SoftwareProject", "repo:pv-demo-5", "test")
    up = await actions.create_or_find_object("Reference", "ref:pv-demo-5-up", "test")
    await stamp_read(actions.pool, agent_id="agent:writer", door="dossier",
                     object_id=up, read_at=NOW - timedelta(minutes=5))
    first = await stamp_possible_upstream(
        actions, written_object_id=o, source_id="agent:writer", observed_at=NOW)
    second = await stamp_possible_upstream(
        actions, written_object_id=o, source_id="agent:writer",
        observed_at=NOW + timedelta(minutes=1))
    assert first == 1 and second == 0
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND type='possible_upstream'", o)
    assert n == 1


# --- credence.py's second independence leg ----------------------------------------

def test_distinct_upstream_count_collapses_two_sources_sharing_an_upstream() -> None:
    ups = {"agent:a": frozenset({"msg:1"}), "agent:b": frozenset({"msg:1"})}
    assert distinct_upstream_count(ups) == 1


def test_distinct_upstream_count_keeps_unrelated_sources_apart() -> None:
    ups = {"agent:a": frozenset({"msg:1"}), "agent:b": frozenset({"msg:2"})}
    assert distinct_upstream_count(ups) == 2


def test_distinct_upstream_count_never_collapses_a_source_with_no_recorded_upstream() -> None:
    # the safe default: nothing to withhold on, so it counts as its own witness.
    ups = {"agent:a": frozenset(), "agent:b": frozenset()}
    assert distinct_upstream_count(ups) == 2


def test_distinct_upstream_count_a_looker_is_never_collapsed() -> None:
    # THE REBUTTAL SIGNAL CARRIES OVER (Thoth's own acceptance spec): a source that
    # performed its own observation act is not deflated by sharing an upstream read —
    # it may have verified independently, same law resolve_credence's own rebuttal
    # rule already applies to the spawned_by clamp.
    ups = {"agent:a": frozenset({"msg:1"}), "agent:b": frozenset({"msg:1"})}
    assert distinct_upstream_count(ups, looked={"agent:a": True}) == 2


async def test_upstream_sets_over_the_graph(actions: Actions) -> None:
    o = await actions.create_or_find_object("SoftwareProject", "repo:pv-demo-6", "test")
    up = await actions.create_or_find_object("Reference", "ref:pv-demo-6-up", "test")
    await actions.create_link(o, up, "possible_upstream", "agent:a", NOW, 1.0,
                              properties={"door": "dossier", "read_at": NOW.isoformat()})
    got = await upstream_sets(actions, o, ["agent:a", "agent:b"])
    assert got["agent:a"] == frozenset({str(up)})
    assert got["agent:b"] == frozenset()


# --- the star acceptance test (Thoth's own spec) ------------------------------------

async def test_two_seats_reading_the_same_message_then_asserting_resolve_to_one_witness(
    actions: Actions,
) -> None:
    """Two agent sources with NO spawned_by relation (so the OLD independence oracle
    would call them fully independent) both read message X, then assert the SAME fact —
    the dossier's distinct_upstreams collapses them to one witness even though
    `agreement` (the raw value-count) already read 'agreeing' on its own."""
    o = await actions.create_or_find_object("SoftwareProject", "repo:pv-demo-7", "test")
    await actions.create_or_find_object("Agent", "agent:seat-a", "test")
    await actions.create_or_find_object("Agent", "agent:seat-b", "test")
    msg_x = await actions.create_or_find_object("Message", "message:777", "agent:sender")
    for src in ("agent:seat-a", "agent:seat-b"):
        await actions.create_link(o, msg_x, "possible_upstream", src, NOW, 1.0,
                                  properties={"door": "inbox-lease", "read_at": NOW.isoformat()})
    await actions.assert_property(o, "status", "green", "agent:seat-a", NOW, 0.9,
                                  evidence_class=_EC)
    await actions.assert_property(o, "status", "green", "agent:seat-b", NOW, 0.9,
                                  evidence_class=_EC)
    dossier = await entity_dossier(actions.pool, o)
    status = next(p for p in dossier["properties"] if p["name"] == "status")
    assert status["agreement"] == "agreeing"
    assert status["distinct_upstreams"] == 1


async def test_a_seat_that_read_x_and_observed_is_not_deflated(actions: Actions) -> None:
    o = await actions.create_or_find_object("SoftwareProject", "repo:pv-demo-8", "test")
    a = await actions.create_or_find_object("Agent", "agent:seat-c", "test")
    await actions.create_or_find_object("Agent", "agent:seat-d", "test")
    msg_x = await actions.create_or_find_object("Message", "message:778", "agent:sender")
    for src in ("agent:seat-c", "agent:seat-d"):
        await actions.create_link(o, msg_x, "possible_upstream", src, NOW, 1.0,
                                  properties={"door": "inbox-lease", "read_at": NOW.isoformat()})
    await actions.assert_property(a, "backed_by_observation", True, "test", NOW, 0.9,
                                  evidence_class=_EC)
    await actions.assert_property(o, "status", "green", "agent:seat-c", NOW, 0.9,
                                  evidence_class=_EC)
    await actions.assert_property(o, "status", "green", "agent:seat-d", NOW, 0.9,
                                  evidence_class=_EC)
    dossier = await entity_dossier(actions.pool, o)
    status = next(p for p in dossier["properties"] if p["name"] == "status")
    assert status["distinct_upstreams"] == 2  # seat-c's own observation act is never deflated


async def test_a_mined_fact_and_an_agents_restatement_collapse_to_one_witness(
    actions: Actions,
) -> None:
    """FACT-SCOPED FOLLOW-UP (Thoth's own mail 10405): piece 2's mined facts are
    sourced to the literal "session-miner" constant, never agent:-prefixed
    (ingest/sessions.py emit_yield, ruling ceae1604) — the ORIGINAL cut of this
    metric filtered to agent:-prefixed sources only, so a mined fact could never
    collapse with an agent's own restatement of the same upstream read even when
    their possible_upstream edges genuinely agreed. That gap is closed: distinct_
    upstreams now walks EVERY source on the object, regardless of source id."""
    o = await actions.create_or_find_object("SoftwareProject", "repo:pv-demo-9", "test")
    msg_x = await actions.create_or_find_object("Message", "message:779", "agent:sender")
    for src in ("session-miner", "agent:seat-e"):
        await actions.create_link(o, msg_x, "possible_upstream", src, NOW, 1.0,
                                  properties={"door": "session-miner:tool_result:message"
                                              if src == "session-miner" else "inbox-lease",
                                              "read_at": NOW.isoformat()})
    await actions.assert_property(o, "status", "green", "session-miner", NOW, 0.6,
                                  evidence_class=EvidenceClass.DERIVED.value)
    await actions.assert_property(o, "status", "green", "agent:seat-e", NOW, 0.9,
                                  evidence_class=_EC)
    dossier = await entity_dossier(actions.pool, o)
    status = next(p for p in dossier["properties"] if p["name"] == "status")
    assert status["distinct_upstreams"] == 1


# --- end-to-end through the MCP door -------------------------------------------------

class _Ctx:
    class request_context:  # noqa: N801
        request = None
        session = object()


async def _mounted(agent_id: str) -> _Ctx:
    import src.mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    ctx = _Ctx()
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id=agent_id, session=agent_id, project=None, model=None, cwd=None)
    return ctx


@pytest.fixture
async def _pool(actions: Actions):
    import src.mcp_server as srv

    saved = srv._pool
    srv._pool = actions.pool
    yield actions.pool
    srv._pool = saved


async def test_dossier_door_stamps_a_read_set_entry(actions: Actions, _pool) -> None:
    from src.mcp_server import dossier as dossier_tool

    o = await actions.create_or_find_object("Reference", "ref:pv-door-1", "test")
    ctx = await _mounted("agent:door-reader")
    await dossier_tool(object_ref="ref:pv-door-1", ctx=ctx)
    row = await actions.pool.fetchrow(
        "SELECT agent_id, door FROM session_reads WHERE object_id=$1", o)
    assert row is not None
    assert row["agent_id"] == "agent:door-reader" and row["door"] == "dossier"


async def test_record_decision_mints_possible_upstream_to_what_it_read(
    actions: Actions, _pool,
) -> None:
    from src.mcp_server import dossier as dossier_tool
    from src.mcp_server import record_decision as record_decision_tool

    up = await actions.create_or_find_object("Reference", "ref:pv-door-2", "test")
    ctx = await _mounted("agent:door-writer")
    await dossier_tool(object_ref="ref:pv-door-2", ctx=ctx)  # the prior read
    out = await record_decision_tool(summary="a decision that read ref:pv-door-2 first", ctx=ctx)
    did = out["id"]
    rows = await actions.pool.fetch(
        "SELECT to_id FROM links WHERE from_id=$1 AND type='possible_upstream'", did)
    assert str(up) in {str(r["to_id"]) for r in rows}


async def test_inbox_door_never_stamps_a_read_to_the_operators_own_words(
    actions: Actions, _pool, tmp_path: Path,
) -> None:
    from src.mcp_server import inbox as inbox_tool
    from src.orchestrator import mounts
    from src.orchestrator.mailbox import send_message

    await mounts.save_mount(actions.pool, job_dir=str(tmp_path / "jobs" / "door3"),
                            agent_id="agent:door-reader-2", project="pv-door-3",
                            cwd="/repo/pv-door-3", model=None, session_key=None)
    op_sent = await send_message(actions.pool, from_agent="operator", from_project="operator",
                                 to_project="pv-door-3", body="an operator broadcast",
                                 grade="fyi")
    peer_sent = await send_message(actions.pool, from_agent="agent:door-peer",
                                   from_project="pv-door-3", to_project="pv-door-3",
                                   body="an ordinary peer broadcast", grade="fyi")
    op_msg_oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1", f"message:{op_sent['id']}")
    peer_msg_oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1", f"message:{peer_sent['id']}")
    ctx = await _mounted("agent:door-reader-2")
    await inbox_tool(project="pv-door-3", peek=True, ctx=ctx)
    n_op = await actions.pool.fetchval(
        "SELECT count(*) FROM session_reads WHERE object_id=$1", op_msg_oid)
    n_peer = await actions.pool.fetchval(
        "SELECT count(*) FROM session_reads WHERE object_id=$1", peer_msg_oid)
    assert n_op == 0     # never an edge to the operator's own words
    assert n_peer == 1   # an ordinary peer message in the SAME inbox call IS stamped
