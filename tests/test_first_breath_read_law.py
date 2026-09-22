"""THE FIRST-BREATH READ LAW (thread afd27e1a, Thoth mail 13003): an heir must never
settle mail it has not read in full. `inbox(ack=[ids])` refuses an id this agent's own
generation has never returned through a real inbox() call (session_reads, provenance
piece 1) — the wake prompt's own headline preview (`_mail_envelope` in trigger.py) is
not an inbox() call and stamps nothing, so acking straight off it is refused, not
silently accepted.
"""
from __future__ import annotations

from datetime import UTC, datetime

import pytest
from src.actions.core import Actions
from src.orchestrator.agents import AgentIdentity, claim_name, mint_heir
from src.orchestrator.mailbox import send_message
from src.parsers.base import EvidenceClass


class _Ctx:
    class request_context:  # noqa: N801
        request = None
        session = object()


async def _mount(agent_id: str, project: str | None = "osiris") -> _Ctx:
    import src.mcp_server as srv

    ctx = _Ctx()
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id=agent_id, session=agent_id, project=project, model=None, cwd=None)
    return ctx


@pytest.fixture
async def _pool(actions: Actions):
    import src.mcp_server as srv

    saved = srv._pool
    srv._pool = actions.pool
    yield actions.pool
    srv._pool = saved


async def test_heir_acking_straight_off_the_wake_preview_is_refused(
    actions: Actions, _pool,
) -> None:
    """The exact specimen Thoth asked for: a heir minted with an ask DM in flight —
    never itself having called inbox() — must be refused when it tries to ack the id
    straight off the wake prompt's own headline preview. Reading it in full first
    (peek or lease) then allows the ack."""
    from src.mcp_server import inbox as inbox_tool

    anc = await actions.create_or_find_object("Agent", "agent:fbreadtest", "test")
    await actions.assert_property(anc, "project", "osiris", "test", datetime.now(UTC), 0.9,
                                  evidence_class=EvidenceClass.SELF_DECLARED.value)
    await claim_name(actions, "agent:fbreadtest", "Firstbreadholder", source="test")

    out = await send_message(actions.pool, from_agent="agent:sender", from_project="osiris",
                             to_agent="agent:fbreadtest", body="an ask still in flight",
                             grade="ask")
    msg_id = int(out["id"])

    # the mint: a seam (compaction/succession) lands WHILE the ask is still unread —
    # the heir inherits the estate, but never the ancestor's own read-set (session_reads
    # is keyed on the exact generation that called inbox(), never carried forward).
    heir, _heir_oid = await mint_heir(actions, "agent:fbreadtest", anc,
                                      because="compaction", succession=None)

    ctx = await _mount(heir)
    # ACKS STRAIGHT FROM THE PREVIEW — no PRIOR inbox() call by this generation at
    # all. peek=True here only avoids leasing the message as a side effect of this
    # very call (a lease would make it undeliverable to the very next call for the
    # rest of the lease window, muddying the "reads in full -> allowed" step below);
    # it changes nothing about the read-law check itself, which is keyed off calls
    # BEFORE this one, never this one's own concurrent read.
    refused = await inbox_tool(peek=True, ack=[msg_id], ctx=ctx)
    assert refused["settled"] == []
    assert msg_id in refused["skipped"]
    assert "not yet read in full" in refused["skipped"][msg_id]
    # never actually settled — redelivers exactly as an unread message should
    assert await actions.pool.fetchval(
        "SELECT read_at FROM message_recipients WHERE message_id=$1 AND agent_id=$2",
        msg_id, heir) is None

    # the refusal's OWN read already stamped session_reads (peek or lease, both do) —
    # so a plain retry, one call later, now finds the id already read and settles it.
    allowed = await inbox_tool(ack=[msg_id], ctx=ctx)
    assert allowed["settled"] == [msg_id]
    assert "skipped" not in allowed
    assert "skipped" not in allowed
    assert await actions.pool.fetchval(
        "SELECT read_at FROM message_recipients WHERE message_id=$1 AND agent_id=$2",
        msg_id, heir) is not None


async def test_a_genuine_lease_before_ack_still_works_the_ordinary_way(
    actions: Actions, _pool,
) -> None:
    """The ordinary, already-documented workflow — lease (peek=False, the default) in
    one call, settle by acking in a LATER call — must keep working unchanged: the
    earlier lease already stamped session_reads, so the later ack's own check finds
    it satisfied."""
    from src.mcp_server import inbox as inbox_tool

    await claim_name(actions, "agent:fbreadtest2", "Firstbreadholder2", source="test")
    out = await send_message(actions.pool, from_agent="agent:sender", from_project="osiris",
                             to_agent="agent:fbreadtest2", body="an ordinary ask",
                             grade="ask")
    msg_id = int(out["id"])
    ctx = await _mount("agent:fbreadtest2")

    leased = await inbox_tool(ctx=ctx)  # peek=False (default) — a real lease
    assert [m["id"] for m in leased["messages"]] == [msg_id]

    settled = await inbox_tool(ack=[msg_id], ctx=ctx)  # a later, separate call
    assert settled["settled"] == [msg_id]
    assert "skipped" not in settled
