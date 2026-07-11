"""The fleet mailbox — group-chat + DM, per-recipient AT-LEAST-ONCE (thread 6fa9791d).

Reading LEASES per reader (a message_recipients row); settling is separate (reply / ack /
never → redelivery). A BROADCAST (to_project) is the group chat — every agent in the project
sees it and settles independently; a DM (to_agent) reaches one agent. These tests drive the
per-reader lease cycle, the group visibility (two agents both see a broadcast), the DM lane
(only the addressee, reply routes back privately), dedup, and the reply-as-ack.
"""
from __future__ import annotations

import pytest
from src.actions.core import Actions
from src.orchestrator.mailbox import (
    OPERATOR_ADDR,
    ack_messages,
    in_flight,
    read_inbox,
    send_message,
    unread_count,
)

R = "agent:reader"  # a representative reader for single-agent-project tests


async def test_reading_leases_rather_than_consumes(actions: Actions) -> None:
    p = actions.pool
    res = await send_message(p, from_agent="agent:aaa", from_project="decepticons",
                             to_project="heinrich", body="the Toeplitz counterfactual holds")
    assert res["id"] > 0 and res["dedup"] is False
    assert await unread_count(p, "heinrich", reader_agent=R) == 1
    box = await read_inbox(p, "heinrich", reader_agent=R)
    assert len(box) == 1 and box[0]["from"] == "agent:aaa" and "Toeplitz" in box[0]["body"]
    # LEASED, not consumed: hidden from THIS reader while the lease is live…
    assert await unread_count(p, "heinrich", reader_agent=R) == 0
    assert await read_inbox(p, "heinrich", reader_agent=R) == []
    # …but the expired lease REDELIVERS, flagged — never silently lost.
    assert await unread_count(p, "heinrich", reader_agent=R, lease_secs=0) == 1
    again = await read_inbox(p, "heinrich", reader_agent=R, lease_secs=0)
    assert len(again) == 1 and again[0]["redelivered"] is True


async def test_ack_settles_for_good(actions: Actions) -> None:
    p = actions.pool
    res = await send_message(p, from_agent="agent:x", from_project="a", to_project="b",
                             body="handle me")
    (msg,) = await read_inbox(p, "b", reader_agent=R)
    assert await ack_messages(p, "b", [msg["id"]], reader_agent=R) == 1
    assert await unread_count(p, "b", reader_agent=R, lease_secs=0) == 0
    assert await read_inbox(p, "b", reader_agent=R, lease_secs=0) == []
    assert await ack_messages(p, "b", [res["id"]], reader_agent=R) == 0  # idempotent


async def test_ack_is_scoped_to_the_recipient(actions: Actions) -> None:
    p = actions.pool
    res = await send_message(p, from_agent="agent:x", from_project="a", to_project="b",
                             body="b's mail")
    # a reader in another project can't settle b's broadcast (it isn't addressed to them)
    assert await ack_messages(p, "c", [res["id"]], reader_agent="agent:c") == 0
    assert await unread_count(p, "b", reader_agent=R) == 1


async def test_a_broadcast_is_a_group_chat_both_agents_see_it(actions: Actions) -> None:
    """The crux: two co-located agents (ux + engine, one project) BOTH see a broadcast and
    settle it INDEPENDENTLY — the old single-reader lease hid it from the second."""
    p = actions.pool
    await send_message(p, from_agent="agent:x", from_project="a", to_project="handlingtheloop",
                       body="standup: who owns the socket layer?")
    ux, engine = "agent:ux", "agent:engine"
    assert await unread_count(p, "handlingtheloop", reader_agent=ux) == 1
    assert await unread_count(p, "handlingtheloop", reader_agent=engine) == 1
    # ux reads (leases ITS copy) — engine still sees it
    (m,) = await read_inbox(p, "handlingtheloop", reader_agent=ux)
    assert await unread_count(p, "handlingtheloop", reader_agent=ux) == 0     # leased for ux
    assert await unread_count(p, "handlingtheloop", reader_agent=engine) == 1  # engine untouched
    # each settles its own copy
    await ack_messages(p, "handlingtheloop", [m["id"]], reader_agent=ux)
    (m2,) = await read_inbox(p, "handlingtheloop", reader_agent=engine)
    await ack_messages(p, "handlingtheloop", [m2["id"]], reader_agent=engine)
    assert await unread_count(p, "handlingtheloop", reader_agent=ux, lease_secs=0) == 0
    assert await unread_count(p, "handlingtheloop", reader_agent=engine, lease_secs=0) == 0


async def test_a_dm_reaches_only_its_addressee_and_reply_routes_back(actions: Actions) -> None:
    """A DM (to_agent) is private to one agent; a reply to a DM routes back to the sender as a
    DM, and settles the original."""
    p = actions.pool
    dm = await send_message(p, from_agent="agent:ux", from_project="handlingtheloop",
                            to_agent="agent:engine", body="engine, the ESP layout changed")
    assert dm["to_agent"] == "agent:engine" and dm["to"] is None
    # a co-located sibling does NOT see it — only the addressee
    assert await unread_count(p, "handlingtheloop", reader_agent="agent:ux") == 0
    assert await unread_count(p, "handlingtheloop", reader_agent="agent:engine") == 1
    (m,) = await read_inbox(p, "handlingtheloop", reader_agent="agent:engine")
    assert m.get("dm") is True and "ESP layout" in m["body"]
    # engine replies — routes back to ux privately, settles the original for engine
    reply = await send_message(p, from_agent="agent:engine", from_project="handlingtheloop",
                               body="on it — rebasing the map", reply_to=m["id"])
    assert reply["to_agent"] == "agent:ux" and reply["thread_id"] == dm["id"]
    assert await unread_count(p, "handlingtheloop", reader_agent="agent:engine",
                              lease_secs=0) == 0
    (ans,) = await read_inbox(p, "handlingtheloop", reader_agent="agent:ux")
    assert ans.get("dm") is True and ans["thread"] == dm["id"]


async def test_peek_neither_leases_nor_settles(actions: Actions) -> None:
    p = actions.pool
    await send_message(p, from_agent="agent:x", from_project="a", to_project="b", body="hi")
    peeked = await read_inbox(p, "b", reader_agent=R, mark_read=False)
    assert len(peeked) == 1
    assert await unread_count(p, "b", reader_agent=R) == 1  # untouched — no lease


async def test_send_dedups_a_client_retry(actions: Actions) -> None:
    p = actions.pool
    first = await send_message(p, from_agent="agent:x", from_project="a", to_project="b",
                               body="exactly once")
    retry = await send_message(p, from_agent="agent:x", from_project="a", to_project="b",
                               body="exactly once")
    assert retry["dedup"] is True and retry["id"] == first["id"]
    assert await unread_count(p, "b", reader_agent=R) == 1
    other = await send_message(p, from_agent="agent:x", from_project="a", to_project="b",
                               body="exactly once, again")
    assert other["dedup"] is False and other["id"] != first["id"]


async def test_reply_routes_back_threads_and_acks(actions: Actions) -> None:
    p = actions.pool
    ask = await send_message(p, from_agent="agent:asker", from_project="decepticons",
                             to_project="heinrich", body="what does your witness say?")
    (q,) = await read_inbox(p, "heinrich", reader_agent="agent:h")
    reply = await send_message(p, from_agent="agent:h", from_project="heinrich",
                               body="digit-exact agreement", reply_to=q["id"])
    assert reply["to"] == "decepticons" and reply["thread_id"] == ask["id"]
    assert await unread_count(p, "heinrich", reader_agent="agent:h", lease_secs=0) == 0
    (a2,) = await read_inbox(p, "decepticons", reader_agent="agent:asker")
    assert a2["thread"] == ask["id"] and a2["reply_to"] == q["id"]
    hop2 = await send_message(p, from_agent="agent:asker", from_project="decepticons",
                              body="and the control?", reply_to=a2["id"])
    assert hop2["to"] == "heinrich" and hop2["thread_id"] == ask["id"]


async def test_reply_does_not_ack_someone_elses_mail(actions: Actions) -> None:
    p = actions.pool
    await send_message(p, from_agent="agent:x", from_project="a", to_project="b", body="for b")
    (m,) = await read_inbox(p, "b", reader_agent=R, mark_read=False)
    await send_message(p, from_agent="agent:c", from_project="c",
                       body="butting in", reply_to=m["id"])
    assert await unread_count(p, "b", reader_agent=R) == 1  # still b's to settle


async def test_send_warns_when_the_thread_peer_already_wrote(actions: Actions) -> None:
    """The crossed-mail tax (Anubis VIII, msg 236: four in-flight crossings in one day):
    when a reply goes out while the peer's LATER note in the same thread sits unread in
    the sender's inbox, send() says so at compose time — a mirror, never a push."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    peer = "agent:soundwave-vi"
    # the peer opens a thread to alpha, then follows up in the SAME thread before alpha reads
    m1 = await send_message(actions.pool, from_agent=peer, from_project="beta",
                            to_project="alpha", body="first question")
    await send_message(actions.pool, from_agent=peer, from_project="beta",
                       to_project="alpha", body="actually, an update", reply_to=m1["id"])
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:alpha-1", session="alpha001", project="alpha", model=None, cwd=None)
    try:
        out = await srv.send("answering your first question", reply_to=m1["id"], ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert "crossed" in out and "1 unread" in out["crossed"]  # the follow-up is in flight
    # and with nothing unread in the thread, no warning (the mirror stays quiet)
    m2 = await send_message(actions.pool, from_agent=peer, from_project="beta",
                            to_project="gamma", body="solo note")
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:gamma-1", session="gamma001", project="gamma", model=None, cwd=None)
    try:
        out2 = await srv.send("a clean reply", reply_to=m2["id"], ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert "crossed" not in out2


async def test_reply_to_unknown_message_is_an_error(actions: Actions) -> None:
    with pytest.raises(ValueError, match="does not exist"):
        await send_message(actions.pool, from_agent="agent:x", from_project="a",
                           body="into the void", reply_to=999999)
    with pytest.raises(ValueError, match="no recipient"):
        await send_message(actions.pool, from_agent="agent:x", from_project="a", body="lost")


async def test_inbox_is_scoped_and_normalized(actions: Actions) -> None:
    p = actions.pool
    await send_message(p, from_agent="agent:x", from_project="a", to_project="heinrich",
                       body="yo")
    assert await unread_count(p, "decepticons", reader_agent="agent:d") == 0
    assert await read_inbox(p, "decepticons", reader_agent="agent:d") == []
    assert await unread_count(p, "heinrich", reader_agent=R) == 1
    assert await unread_count(p, "repo:heinrich", reader_agent=R) == 1  # repo: prefix normalized


async def test_operator_is_an_ordinary_reader(actions: Actions) -> None:
    p = actions.pool
    await send_message(p, from_agent="agent:x", from_project="decepticons",
                       to_project=OPERATOR_ADDR, body="finding: emergence confirmed")
    assert await unread_count(p, OPERATOR_ADDR, reader_agent=OPERATOR_ADDR) == 1
    (m,) = await read_inbox(p, OPERATOR_ADDR, reader_agent=OPERATOR_ADDR, mark_read=False)
    assert m["from_project"] == "decepticons"


async def test_lease_is_visible_to_the_group(actions: Actions) -> None:
    """A sibling holding a live lease on shared broadcast mail is VISIBLE (in_flight names it) —
    'mail 0' must never hide 'the other agent is answering the group thread right now'."""
    p = actions.pool
    await send_message(p, from_agent="agent:x", from_project="decepticons",
                       to_project="handlingtheloop", body="who takes this?")
    await read_inbox(p, "handlingtheloop", reader_agent="agent:ux")  # ux leases it
    flight = await in_flight(p, "handlingtheloop", reader_agent="agent:engine")
    assert len(flight) == 1 and flight[0]["leased_by"] == "agent:ux"
