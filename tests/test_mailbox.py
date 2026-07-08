"""The fleet mailbox — directed agent-to-agent coordination, AT-LEAST-ONCE (decision 56f6a0d6).

The old mailbox consumed a message at read time, BEFORE the response provably reached the
agent — a server bounce mid-flight silently swallowed mail. Now reading LEASES; settling is a
separate act (reply / explicit ack / never → redelivery). These tests drive the lease cycle,
the dedup that makes client retries safe, the reply lane (auto-route + thread + reply-as-ack),
and the ack scoping that stops one project settling another's mail.
"""
from __future__ import annotations

import pytest
from src.actions.core import Actions
from src.orchestrator.mailbox import (
    OPERATOR_ADDR,
    ack_messages,
    read_inbox,
    send_message,
    unread_count,
)


async def test_reading_leases_rather_than_consumes(actions: Actions) -> None:
    p = actions.pool
    res = await send_message(p, from_agent="agent:aaa", from_project="decepticons",
                             to_project="heinrich", body="the Toeplitz counterfactual holds")
    assert res["id"] > 0 and res["dedup"] is False
    assert await unread_count(p, "heinrich") == 1
    box = await read_inbox(p, "heinrich")
    assert len(box) == 1
    assert box[0]["from"] == "agent:aaa" and "Toeplitz" in box[0]["body"]
    assert box[0]["from_project"] == "decepticons"
    # LEASED, not consumed: hidden while the lease is live…
    assert await unread_count(p, "heinrich") == 0
    assert await read_inbox(p, "heinrich") == []
    # …but with the lease expired (lease_secs=0) it REDELIVERS, flagged — never silently lost.
    assert await unread_count(p, "heinrich", lease_secs=0) == 1
    again = await read_inbox(p, "heinrich", lease_secs=0)
    assert len(again) == 1 and again[0]["redelivered"] is True


async def test_ack_settles_for_good(actions: Actions) -> None:
    p = actions.pool
    res = await send_message(p, from_agent="agent:x", from_project="a",
                             to_project="b", body="handle me")
    (msg,) = await read_inbox(p, "b")
    assert await ack_messages(p, "b", [msg["id"]]) == 1
    # settled: even a zero lease cannot resurrect it
    assert await unread_count(p, "b", lease_secs=0) == 0
    assert await read_inbox(p, "b", lease_secs=0) == []
    assert await ack_messages(p, "b", [res["id"]]) == 0  # idempotent — no double settle


async def test_ack_is_scoped_to_the_recipient(actions: Actions) -> None:
    p = actions.pool
    res = await send_message(p, from_agent="agent:x", from_project="a",
                             to_project="b", body="b's mail")
    assert await ack_messages(p, "c", [res["id"]]) == 0  # another project can't settle it
    assert await unread_count(p, "b") == 1


async def test_peek_neither_leases_nor_settles(actions: Actions) -> None:
    p = actions.pool
    await send_message(p, from_agent="agent:x", from_project="a", to_project="b", body="hi")
    peeked = await read_inbox(p, "b", mark_read=False)
    assert len(peeked) == 1
    assert await unread_count(p, "b") == 1  # untouched — no lease was taken


async def test_send_dedups_a_client_retry(actions: Actions) -> None:
    # the retry-after-severed-response case: an identical (sender, recipient, body) inside the
    # window returns the EXISTING id instead of double-posting
    p = actions.pool
    first = await send_message(p, from_agent="agent:x", from_project="a",
                               to_project="b", body="exactly once")
    retry = await send_message(p, from_agent="agent:x", from_project="a",
                               to_project="b", body="exactly once")
    assert retry["dedup"] is True and retry["id"] == first["id"]
    assert await unread_count(p, "b") == 1
    # outside the window (or a different body) it's a genuinely new message
    other = await send_message(p, from_agent="agent:x", from_project="a",
                               to_project="b", body="exactly once, again")
    assert other["dedup"] is False and other["id"] != first["id"]


async def test_reply_routes_back_threads_and_acks(actions: Actions) -> None:
    p = actions.pool
    ask = await send_message(p, from_agent="agent:asker", from_project="decepticons",
                             to_project="heinrich", body="what does your witness say?")
    (q,) = await read_inbox(p, "heinrich")
    # the reply names no `to` — it routes to the ASKER's project and joins the thread
    reply = await send_message(p, from_agent="agent:heinrich", from_project="heinrich",
                               body="digit-exact agreement", reply_to=q["id"])
    assert reply["to"] == "decepticons"
    assert reply["thread_id"] == ask["id"]
    # replying IS the ack: the question is settled even with the lease expired
    assert await unread_count(p, "heinrich", lease_secs=0) == 0
    # the asker receives the reply carrying the thread
    (ans,) = await read_inbox(p, "decepticons")
    assert ans["thread"] == ask["id"] and ans["reply_to"] == q["id"]
    # a second hop stays in the SAME thread (thread root is stable, not the previous message)
    hop2 = await send_message(p, from_agent="agent:asker", from_project="decepticons",
                              body="and the control?", reply_to=ans["id"])
    assert hop2["to"] == "heinrich" and hop2["thread_id"] == ask["id"]


async def test_reply_does_not_ack_someone_elses_mail(actions: Actions) -> None:
    # replying to a message addressed to ANOTHER project must not settle it
    p = actions.pool
    await send_message(p, from_agent="agent:x", from_project="a", to_project="b", body="for b")
    (m,) = await read_inbox(p, "b", mark_read=False)
    await send_message(p, from_agent="agent:c", from_project="c",
                       body="butting in", reply_to=m["id"])
    assert await unread_count(p, "b") == 1  # still b's to settle


async def test_reply_to_unknown_message_is_an_error(actions: Actions) -> None:
    with pytest.raises(ValueError, match="does not exist"):
        await send_message(actions.pool, from_agent="agent:x", from_project="a",
                           body="into the void", reply_to=999999)
    with pytest.raises(ValueError, match="no recipient"):
        await send_message(actions.pool, from_agent="agent:x", from_project="a", body="lost")


async def test_inbox_is_scoped_and_normalized(actions: Actions) -> None:
    p = actions.pool
    await send_message(p, from_agent="agent:x", from_project="a",
                       to_project="heinrich", body="yo")
    assert await unread_count(p, "decepticons") == 0  # not addressed here
    assert await read_inbox(p, "decepticons") == []
    assert await unread_count(p, "heinrich") == 1
    assert await unread_count(p, "repo:heinrich") == 1  # the repo: prefix is normalized


async def test_operator_is_an_ordinary_address(actions: Actions) -> None:
    # the human's desk is just a project string — no repo, no wake, read like any inbox
    p = actions.pool
    await send_message(p, from_agent="agent:x", from_project="decepticons",
                       to_project=OPERATOR_ADDR, body="finding: emergence confirmed")
    assert await unread_count(p, OPERATOR_ADDR) == 1
    (m,) = await read_inbox(p, OPERATOR_ADDR, mark_read=False)
    assert m["from_project"] == "decepticons"


async def test_lease_is_visible_to_the_owner(actions: Actions) -> None:
    """msg-78 lesson: a twin leasing the owner's mail must be VISIBLE — 'mail 0' with a held
    lease is not 'nothing happening'. The lease records its holder; in_flight names it."""
    from src.orchestrator.mailbox import in_flight

    p = actions.pool
    await send_message(p, from_agent="agent:x", from_project="decepticons",
                       to_project="heinrich", body="context brief before you MRI")
    # a twin leases it, stamped as the lessee
    await read_inbox(p, "heinrich", lessee="agent:dcfa2136")
    (f,) = await in_flight(p, "heinrich")
    assert f["leased_by"] == "agent:dcfa2136"
    assert f["held_for_secs"] >= 0
    # settled → no longer in flight
    await send_message(p, from_agent="agent:dcfa2136", from_project="heinrich",
                       body="brief received", reply_to=f["id"])
    assert await in_flight(p, "heinrich") == []
