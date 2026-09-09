"""Wave 13 item 1 (thread 9d1d41c8's own audit, operator's word 2026-09-09): the
compensating-write closer for the zero-recipient DM backlog — never a delete, a marker row
per orphaned DM, idempotent on the tracking thread's own ref."""
from __future__ import annotations

from src.actions.core import Actions
from src.orchestrator.mailbox import close_zero_recipient_dm_backlog, zero_recipient_dm_rows


async def _orphan_dm(actions: Actions, *, to_agent: str, body: str = "nobody home") -> int:
    row = await actions.pool.fetchrow(
        "INSERT INTO fleet_messages (from_agent, to_agent, body) "
        "VALUES ('agent:sender', $1, $2) RETURNING id", to_agent, body)
    return int(row["id"])


async def test_closes_every_zero_recipient_dm_with_a_marker_row(actions: Actions) -> None:
    a = await _orphan_dm(actions, to_agent="agent:lost-a")
    b = await _orphan_dm(actions, to_agent="agent:lost-b")

    before = await zero_recipient_dm_rows(actions.pool)
    assert {r["id"] for r in before} == {a, b}

    out = await close_zero_recipient_dm_backlog(actions.pool, thread_ref="thread:abc123")
    assert out["before"] == 2
    assert out["after"] == 0
    assert set(out["closed_message_ids"]) == {a, b}
    assert out["marker"] == "system:undeliverable-superseded-by-thread:abc123"

    after = await zero_recipient_dm_rows(actions.pool)
    assert after == []


async def test_never_deletes_the_original_fleet_message(actions: Actions) -> None:
    msg_id = await _orphan_dm(actions, to_agent="agent:lost-c", body="the original text")
    await close_zero_recipient_dm_backlog(actions.pool, thread_ref="thread:xyz789")
    row = await actions.pool.fetchrow(
        "SELECT body FROM fleet_messages WHERE id=$1", msg_id)
    assert row is not None
    assert row["body"] == "the original text"


async def test_the_marker_row_never_claims_the_real_addressee_read_it(
    actions: Actions,
) -> None:
    msg_id = await _orphan_dm(actions, to_agent="agent:the-real-addressee")
    await close_zero_recipient_dm_backlog(actions.pool, thread_ref="thread:mrk001")
    row = await actions.pool.fetchrow(
        "SELECT agent_id FROM message_recipients WHERE message_id=$1", msg_id)
    assert row is not None
    assert row["agent_id"] != "agent:the-real-addressee"
    assert row["agent_id"] == "system:undeliverable-superseded-by-thread:mrk001"


async def test_is_idempotent_on_the_same_thread_ref(actions: Actions) -> None:
    await _orphan_dm(actions, to_agent="agent:lost-d")
    first = await close_zero_recipient_dm_backlog(actions.pool, thread_ref="thread:same111")
    assert first["before"] == 1 and first["after"] == 0
    second = await close_zero_recipient_dm_backlog(actions.pool, thread_ref="thread:same111")
    assert second["before"] == 0 and second["after"] == 0
    assert second["closed_message_ids"] == []


async def test_never_touches_a_project_broadcast(actions: Actions) -> None:
    """A broadcast (to_agent IS NULL) is not a DM — every agent in the project is its own
    implicit recipient, so it must never be swept into the backlog closure."""
    await actions.pool.execute(
        "INSERT INTO fleet_messages (from_agent, to_project, body) "
        "VALUES ('agent:sender', 'widget', 'group chat')")
    out = await close_zero_recipient_dm_backlog(actions.pool, thread_ref="thread:bcast1")
    assert out["before"] == 0 and out["closed_message_ids"] == []
    broadcast_count = await actions.pool.fetchval(
        "SELECT count(*) FROM message_recipients WHERE agent_id LIKE 'system:undeliverable%'")
    assert broadcast_count == 0


async def test_never_touches_a_dm_with_a_real_recipient_row(actions: Actions) -> None:
    msg_id = await _orphan_dm(actions, to_agent="agent:delivered")
    await actions.pool.execute(
        "INSERT INTO message_recipients (message_id, agent_id, delivered_at) "
        "VALUES ($1, 'agent:delivered', now())", msg_id)
    out = await close_zero_recipient_dm_backlog(actions.pool, thread_ref="thread:already1")
    assert out["before"] == 0 and out["closed_message_ids"] == []
    rows = await actions.pool.fetch(
        "SELECT agent_id FROM message_recipients WHERE message_id=$1", msg_id)
    assert [r["agent_id"] for r in rows] == ["agent:delivered"]
