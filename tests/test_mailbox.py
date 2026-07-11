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
    project_deliverable_count,
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


async def test_live_holder_lease_extends_and_a_dead_holder_redelivers(
        actions: Actions) -> None:
    """The live-holder extension (Anubis msg 236 / Soundwave msg 244: redelivered while
    the minted analysis still computed): a lease held by a LIVE mind stretches to the
    hold-grace hour — no self-duplicates, no sibling wakes; a holder gone stale redelivers
    at the plain lease exactly as before; the grace hour is the hard ceiling either way."""
    from src.orchestrator import mounts

    p = actions.pool
    holder = "agent:gridworker"
    await send_message(p, from_agent="agent:x", from_project="a", to_project="grid",
                       body="compute the discrimination grid")
    (m,) = await read_inbox(p, "grid", reader_agent=holder)  # leases it
    # the lease AGES past 15 min (but inside the hour) while the holder computes
    await p.execute("UPDATE message_recipients SET delivered_at = now() - interval '20 min' "
                    "WHERE agent_id=$1", holder)
    await mounts.save_mount(p, job_dir="/h/.claude/jobs/grid0001", agent_id=holder,
                            project="grid", cwd="/x", model=None, session_key="k")
    assert await unread_count(p, "grid", reader_agent=holder) == 0    # no self-duplicate
    assert await project_deliverable_count(p, "grid") == 0            # no sibling wake
    # the holder DIES (mount goes stale) → the plain lease governs: redelivered at 15 min
    await p.execute("UPDATE agent_mounts SET last_seen = now() - interval '30 min' "
                    "WHERE agent_id=$1", holder)
    assert await unread_count(p, "grid", reader_agent=holder) == 1
    assert await project_deliverable_count(p, "grid") == 1
    # a live holder past the GRACE hour is nagged again — the ceiling holds for everyone
    await p.execute("UPDATE agent_mounts SET last_seen = now() WHERE agent_id=$1", holder)
    await p.execute("UPDATE message_recipients SET delivered_at = now() - interval '2 hours' "
                    "WHERE agent_id=$1", holder)
    assert await unread_count(p, "grid", reader_agent=holder) == 1


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


# ── THE ORGANIZED DESK (operator direction, 2026-07-11: "my desk is full — fix it") ──────


async def test_desk_bands_by_sender_triage_and_heuristic(actions: Actions) -> None:
    """Sender-declared desk_kind wins; unclassified briefs band by heuristic, biased upward
    (a CRITICAL never files under fyi). Render order: decision, hands, fyi."""
    from src.orchestrator.mailbox import read_desk

    p = actions.pool
    await send_message(p, from_agent="agent:a", from_project="coldspot",
                       to_project=OPERATOR_ADDR, body="pick a signing strategy",
                       desk_kind="decision")
    await send_message(p, from_agent="agent:b", from_project="monsterhouse",
                       to_project=OPERATOR_ADDR, body="need the gemini key refilled",
                       desk_kind="hands")
    await send_message(p, from_agent="agent:c", from_project="osiris",
                       to_project=OPERATOR_ADDR, body="loop closed, all green",
                       desk_kind="fyi")
    # legacy/unclassified: the heuristic reads the body
    await send_message(p, from_agent="agent:d", from_project="coldspot",
                       to_project=OPERATOR_ADDR, body="🚨 CRITICAL: root escalation path")
    desk = await read_desk(p)
    assert [c["body"] for c in desk["needs_decision"]] == [
        "🚨 CRITICAL: root escalation path", "pick a signing strategy"]
    assert desk["needs_hands"][0]["body"] == "need the gemini key refilled"
    assert desk["fyi"][0]["body"] == "loop closed, all green"
    assert "nothing leased" in desk["note"]
    # a declared kind must be a real band
    with pytest.raises(ValueError, match="desk_kind"):
        await send_message(p, from_agent="agent:e", from_project="osiris",
                           to_project=OPERATOR_ADDR, body="x", desk_kind="urgent")


async def test_desk_folds_same_story_across_senders(actions: Actions) -> None:
    """Five agents reporting ONE fleet-wide condition become one card: newest telling leads,
    the other witnesses ride under it with their ids. Unrelated briefs never fold."""
    from src.orchestrator.mailbox import read_desk

    p = actions.pool
    story = ("Model divergence at mount: intended claude-fable-5, running claude-haiku-4-5. "
             "Either the harness demoted the seat or .osiris needs updating — {}")
    for i, proj in enumerate(("rotten-apple", "Like-Us", "neo")):
        await send_message(p, from_agent=f"agent:w{i}", from_project=proj,
                           to_project=OPERATOR_ADDR, body=story.format(proj),
                           desk_kind="fyi")
    await send_message(p, from_agent="agent:z", from_project="tony",
                       to_project=OPERATOR_ADDR,
                       body="launchpad implementation complete, awaiting audit",
                       desk_kind="fyi")
    desk = await read_desk(p)
    assert len(desk["fyi"]) == 2  # 3 tellings folded to 1 + the unrelated brief
    folded = [c for c in desk["fyi"] if "same_story" in c][0]
    assert folded["same_story"]["count"] == 3
    assert {m["project"] for m in folded["same_story"]["also"]} == {"rotten-apple", "Like-Us"}
    assert "neo" in folded["body"]  # newest telling leads


async def test_desk_thread_fold_newest_brief_speaks_for_the_thread(actions: Actions) -> None:
    """The supersession lane made real: an agent's reply_to its own earlier brief folds the
    old one under the new — the desk shows one card per thread, earlier ids listed."""
    from src.orchestrator.mailbox import read_desk

    p = actions.pool
    first = await send_message(p, from_agent="agent:a", from_project="osiris",
                               to_project=OPERATOR_ADDR, body="storm diagnosed, fix building",
                               desk_kind="fyi")
    await send_message(p, from_agent="agent:a", from_project="osiris",
                       to_project=OPERATOR_ADDR, reply_to=first["id"],
                       body="storm fixed and witnessed; sweep done", desk_kind="fyi")
    desk = await read_desk(p)
    assert len(desk["fyi"]) == 1
    card = desk["fyi"][0]
    assert "witnessed" in card["body"]
    assert card["thread_folded"]["ids"] == [first["id"]]


async def test_dim_annotates_never_settles(actions: Actions) -> None:
    """An agent may DIM a desk brief (moot + why + who); the brief leaves the bands, renders
    collapsed, and STAYS unsettled — dismissing remains the human's word alone. Dim refuses
    non-desk mail."""
    from src.orchestrator.mailbox import dim_brief, read_desk

    p = actions.pool
    res = await send_message(p, from_agent="agent:w", from_project="neo",
                             to_project=OPERATOR_ADDR, body="⚠ model divergence detected",
                             desk_kind="decision")
    out = await dim_brief(p, res["id"], because="root cause fixed in bcbdeab",
                          by="agent:fixer")
    assert "NOT settled" in out["note"]
    desk = await read_desk(p)
    assert desk["needs_decision"] == []                      # left the band
    assert desk["dimmed"][0]["id"] == res["id"]
    assert desk["dimmed"][0]["by"] == "agent:fixer"
    assert "bcbdeab" in desk["dimmed"][0]["moot"]
    # unsettled: the operator's count still includes it
    assert await unread_count(p, OPERATOR_ADDR, reader_agent=OPERATOR_ADDR) == 1
    # ...and only desk mail can be dimmed
    other = await send_message(p, from_agent="agent:w", from_project="neo",
                               to_project="osiris", body="hello project")
    with pytest.raises(ValueError, match="not an operator-desk brief"):
        await dim_brief(p, other["id"], because="x", by="agent:fixer")


async def test_desk_your_queue_derives_from_operator_owned_threads(actions: Actions) -> None:
    """The standing YOUR-QUEUE: owner='operator' open threads render on the desk, obligations
    first — the graph is the canonical waiting-on-your-hands list, not re-stated prose."""
    from src.orchestrator.capture import open_thread
    from src.orchestrator.mailbox import read_desk

    await open_thread(actions, "refill the gemini key for #37", kind="obligation",
                      owner="operator", source="agent:a")
    await open_thread(actions, "someday: pick a desk color", owner="operator",
                      source="agent:a")
    await open_thread(actions, "not yours — engine refactor", kind="obligation",
                      owner="agent:a", source="agent:a")
    desk = await read_desk(actions.pool)
    q = desk["your_queue"]["threads"]
    assert [t["summary"] for t in q] == [
        "refill the gemini key for #37", "someday: pick a desk color"]
    assert q[0]["kind"] == "obligation"
