"""The fleet mailbox — directed agent-to-agent coordination, pull semantics."""
from __future__ import annotations

from src.actions.core import Actions
from src.orchestrator.mailbox import read_inbox, send_message, unread_count


async def test_send_then_read_marks_read(actions: Actions) -> None:
    p = actions.pool
    mid = await send_message(p, from_agent="agent:aaa", from_project="decepticons",
                             to_project="heinrich", body="the Toeplitz counterfactual holds")
    assert mid > 0
    assert await unread_count(p, "heinrich") == 1
    box = await read_inbox(p, "heinrich")
    assert len(box) == 1
    assert box[0]["from"] == "agent:aaa" and "Toeplitz" in box[0]["body"]
    assert box[0]["from_project"] == "decepticons"
    assert await unread_count(p, "heinrich") == 0     # reading consumed it
    assert await read_inbox(p, "heinrich") == []      # gone from the inbox


async def test_peek_leaves_the_message_unread(actions: Actions) -> None:
    p = actions.pool
    await send_message(p, from_agent="agent:x", from_project="a", to_project="b", body="hi")
    peeked = await read_inbox(p, "b", mark_read=False)
    assert len(peeked) == 1
    assert await unread_count(p, "b") == 1            # a peek doesn't consume


async def test_inbox_is_scoped_to_the_recipient_project(actions: Actions) -> None:
    p = actions.pool
    await send_message(p, from_agent="agent:x", from_project="a", to_project="heinrich", body="yo")
    assert await unread_count(p, "decepticons") == 0  # not addressed here
    assert await read_inbox(p, "decepticons") == []
    assert await unread_count(p, "heinrich") == 1
    assert await unread_count(p, "repo:heinrich") == 1  # the repo: prefix is normalized
