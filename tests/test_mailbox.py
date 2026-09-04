"""The fleet mailbox — group-chat + DM, per-recipient AT-LEAST-ONCE (thread 6fa9791d).

Reading LEASES per reader (a message_recipients row); settling is separate (reply / ack /
never → redelivery). A BROADCAST (to_project) is the group chat — every agent in the project
sees it and settles independently; a DM (to_agent) reaches one agent. These tests drive the
per-reader lease cycle, the group visibility (two agents both see a broadcast), the DM lane
(only the addressee, reply routes back privately), dedup, and the reply-as-ack.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from src.actions.core import Actions
from src.orchestrator.capture import record_decision
from src.orchestrator.mailbox import (
    OPERATOR_ADDR,
    ack_messages,
    in_flight,
    project_deliverable_count,
    read_inbox,
    send_message,
    unread_count,
    unread_counts,
)

R = "agent:reader"  # a representative reader for single-agent-project tests


async def _seed(pool, project: str) -> None:
    """Register a throwaway mount for `project` — send_message now refuses an explicit
    `to=` naming a project nobody has ever mounted under (shape 3 of #117, obligation
    45e52530), so a fixture broadcasting to a synthetic project name needs to look like a
    real one first. `save_mount` upserts on job_dir, so calling this twice for the same
    project (even across tests, if the DB isn't isolated) is harmless."""
    from src.orchestrator import mounts
    await mounts.save_mount(pool, job_dir=f"/test/seed/{project}", agent_id=f"agent:seed-{project}",
                            project=project, cwd="/test", model=None, session_key=None)


async def test_reading_leases_rather_than_consumes(actions: Actions) -> None:
    p = actions.pool
    await _seed(p, "sibling-one")
    res = await send_message(p, from_agent="agent:aaa", from_project="sibling-two",
                             to_project="sibling-one", body="the Toeplitz counterfactual holds")
    assert res["id"] > 0 and res["dedup"] is False
    assert await unread_count(p, "sibling-one", reader_agent=R) == 1
    box = await read_inbox(p, "sibling-one", reader_agent=R)
    assert len(box) == 1 and box[0]["from"] == "agent:aaa" and "Toeplitz" in box[0]["body"]
    # LEASED, not consumed: hidden from THIS reader while the lease is live…
    assert await unread_count(p, "sibling-one", reader_agent=R) == 0
    assert await read_inbox(p, "sibling-one", reader_agent=R) == []
    # …but the expired lease REDELIVERS, flagged — never silently lost.
    assert await unread_count(p, "sibling-one", reader_agent=R, lease_secs=0) == 1
    again = await read_inbox(p, "sibling-one", reader_agent=R, lease_secs=0)
    assert len(again) == 1 and again[0]["redelivered"] is True


async def test_unread_counts_matches_two_separate_unread_count_calls_on_a_mixed_mailbox(
    actions: Actions,
) -> None:
    """Thread 72e45258's own residual: mount()/orient()/automount() all called `unread_count`
    twice back to back (total, then grade='ask') — the same `_DELIVERABLE_TO_READER` predicate
    scanned twice. `unread_counts` computes both in one pass via conditional aggregation; this
    proves it against a MIXED mailbox (a broadcast, an ask-graded DM, an already-acked message)
    that both counts land byte-identical to the two-call shape it replaces."""
    p = actions.pool
    await _seed(p, "mixedbox")
    await send_message(p, from_agent="agent:x", from_project="a", to_project="mixedbox",
                       body="fyi broadcast, no grade")
    await send_message(p, from_agent="agent:y", from_project="a", to_agent=R,
                       body="needs your act", grade="ask")
    acked_res = await send_message(p, from_agent="agent:z", from_project="a", to_project="mixedbox",
                                   body="already handled")
    await ack_messages(p, "mixedbox", [acked_res["id"]], reader_agent=R)

    old_total = await unread_count(p, "mixedbox", reader_agent=R)
    old_ask = await unread_count(p, "mixedbox", reader_agent=R, grade="ask")
    combined = await unread_counts(p, "mixedbox", reader_agent=R)

    assert combined == {"total": old_total, "ask": old_ask}
    assert combined == {"total": 2, "ask": 1}  # broadcast + DM live; acked settled; DM is the ask


async def test_ack_settles_for_good(actions: Actions) -> None:
    p = actions.pool
    await _seed(p, "b")
    res = await send_message(p, from_agent="agent:x", from_project="a", to_project="b",
                             body="handle me")
    (msg,) = await read_inbox(p, "b", reader_agent=R)
    assert (await ack_messages(p, "b", [msg["id"]], reader_agent=R))["settled"] == [msg["id"]]
    assert await unread_count(p, "b", reader_agent=R, lease_secs=0) == 0
    assert await read_inbox(p, "b", reader_agent=R, lease_secs=0) == []
    again = await ack_messages(p, "b", [res["id"]], reader_agent=R)  # idempotent — and it SAYS so
    assert again["settled"] == [] and "already settled" in again["skipped"][res["id"]]


async def test_ack_is_scoped_to_the_recipient(actions: Actions) -> None:
    p = actions.pool
    await _seed(p, "b")
    res = await send_message(p, from_agent="agent:x", from_project="a", to_project="b",
                             body="b's mail")
    # a reader in another project can't settle b's broadcast (it isn't addressed to them)
    foreign = await ack_messages(p, "c", [res["id"]], reader_agent="agent:c")
    assert foreign["settled"] == [] and "not addressed to you" in foreign["skipped"][res["id"]]
    assert await unread_count(p, "b", reader_agent=R) == 1


async def test_ack_learns_the_rollup_a_reader_may_settle_what_it_may_read(
    actions: Actions,
) -> None:
    """ALFRED'S FIXTURE (msg 666, 2026-07-19): DMs addressed to his -iii were READABLE by
    -iv (the rollup) but the ack's exact-id match silently no-oped — twice-acked mail
    redelivered forever, and the empty response was indistinguishable from success. The
    law: what a reader may READ it may SETTLE — an ack from any generation of the
    addressed lineage lands, and the receipt names it."""
    p = actions.pool
    dm = await send_message(p, from_agent="agent:c1b99f6e-ii", from_project="ByeByte",
                            to_agent="agent:a1f4ed01-iii", body="report for the old head")
    # the living -iv reads it via the rollup...
    (m,) = await read_inbox(p, "alfred", reader_agent="agent:a1f4ed01-iv")
    assert m["id"] == dm["id"]
    # ...and may now ACK it — the fix; before, this returned empty and the mail haunted
    out = await ack_messages(p, "alfred", [dm["id"]], reader_agent="agent:a1f4ed01-iv")
    assert out["settled"] == [dm["id"]] and out["skipped"] == {}
    assert await unread_count(p, "alfred", reader_agent="agent:a1f4ed01-iv",
                              lease_secs=0) == 0  # settled for good — no redelivery
    # an unknown id in the same call is named, not swallowed
    out2 = await ack_messages(p, "alfred", [999999], reader_agent="agent:a1f4ed01-iv")
    assert out2["settled"] == [] and out2["skipped"][999999] == "unknown id"


async def test_a_broadcast_is_a_group_chat_both_agents_see_it(actions: Actions) -> None:
    """The crux: two co-located agents (ux + engine, one project) BOTH see a broadcast and
    settle it INDEPENDENTLY — the old single-reader lease hid it from the second."""
    p = actions.pool
    await _seed(p, "handlingtheloop")
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


async def test_read_inbox_discloses_a_sidechain_senders_own_fork_identity(
    actions: Actions,
) -> None:
    """obligation 706c27dc (msg 6029): the mail layer's only is_sidechain read site used to
    check the ADDRESSEE (mcp_server.py's send()) and never the SENDER — a fork DMing in its
    parent's voice with the parent's own inherited context read as a bare unfamiliar id,
    indistinguishable from impersonation to a reader who hadn't independently caught it.
    Disclosure, not prohibition: read_inbox now surfaces the SENDER's own is_sidechain +
    patronym so a reader sees "a fork of Khnum XLII" instead of a bare agent:<hash>."""
    p = actions.pool
    from datetime import UTC, datetime

    fork_id = "agent:forkabcd1234"
    obj = await actions.create_or_find_object("Agent", fork_id, fork_id)
    await actions.assert_property(obj, "is_sidechain", "true", fork_id, datetime.now(UTC),
                                  1.0, evidence_class="self_declared")
    await actions.assert_property(obj, "patronym", "Khnum XLII.15", fork_id,
                                  datetime.now(UTC), 1.0, evidence_class="self_declared")
    await send_message(p, from_agent=fork_id, from_project="handlingtheloop",
                       to_agent="agent:reader", body="found the leak in the pool wrapper")
    (m,) = await read_inbox(p, "handlingtheloop", reader_agent="agent:reader")
    assert m["from_sidechain"] is True
    assert m["from_patronym"] == "Khnum XLII.15"


async def test_read_inbox_never_flags_an_ordinary_sender(actions: Actions) -> None:
    """The common case: an ordinary (non-fork) sender carries no is_sidechain assertion at
    all — read_inbox must not invent a flag for it."""
    p = actions.pool
    await _seed(p, "handlingtheloop")
    await send_message(p, from_agent="agent:ux", from_project="handlingtheloop",
                       to_agent="agent:reader", body="plain DM, nothing forked about it")
    (m,) = await read_inbox(p, "handlingtheloop", reader_agent="agent:reader")
    assert "from_sidechain" not in m
    assert "from_patronym" not in m


async def test_peek_neither_leases_nor_settles(actions: Actions) -> None:
    p = actions.pool
    await _seed(p, "b")
    await send_message(p, from_agent="agent:x", from_project="a", to_project="b", body="hi")
    peeked = await read_inbox(p, "b", reader_agent=R, mark_read=False)
    assert len(peeked) == 1
    assert await unread_count(p, "b", reader_agent=R) == 1  # untouched — no lease


async def test_send_dedups_a_client_retry(actions: Actions) -> None:
    p = actions.pool
    await _seed(p, "b")
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
    await _seed(p, "sibling-one")
    ask = await send_message(p, from_agent="agent:asker", from_project="sibling-two",
                             to_project="sibling-one", body="what does your witness say?")
    (q,) = await read_inbox(p, "sibling-one", reader_agent="agent:h")
    reply = await send_message(p, from_agent="agent:h", from_project="sibling-one",
                               body="digit-exact agreement", reply_to=q["id"])
    assert reply["to"] == "sibling-two" and reply["thread_id"] == ask["id"]
    assert await unread_count(p, "sibling-one", reader_agent="agent:h", lease_secs=0) == 0
    (a2,) = await read_inbox(p, "sibling-two", reader_agent="agent:asker")
    assert a2["thread"] == ask["id"] and a2["reply_to"] == q["id"]
    hop2 = await send_message(p, from_agent="agent:asker", from_project="sibling-two",
                              body="and the control?", reply_to=a2["id"])
    assert hop2["to"] == "sibling-one" and hop2["thread_id"] == ask["id"]


async def test_reply_does_not_ack_someone_elses_mail(actions: Actions) -> None:
    p = actions.pool
    await _seed(p, "b")
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
    await _seed(p, "grid")
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
    await _seed(actions.pool, "alpha")
    await _seed(actions.pool, "gamma")
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


async def test_send_tool_echoes_seat_and_lineage_head_and_honors_require_seat(
        actions: Actions) -> None:
    """The MCP surface (dd47c1da): send(to_agent=...) must echo the resolution through to the
    caller, and require_seat must refuse a blind dispatch at the tool boundary too — mailbox
    owns the semantics, mcp_server just has to pass `require_seat` through and not swallow the
    new fields on the way out."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity, claim_name

    held = "agent:c0ffee03"
    a = await actions.create_or_find_object("Agent", held, held)
    await actions.assert_property(a, "project", "bytebye", held,
                                  __import__("datetime").datetime.now(
                                      __import__("datetime").UTC), 0.9)
    await claim_name(actions, held, "Kastellan", source=held)

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:boss", session="boss0001", project="alpha", model=None, cwd=None)
    try:
        out = await srv.send("ship it", to_agent=held, ctx=ctx)
        assert out["dm_to"] == held and out["seat"] == "Kastellan I"
        assert out["lineage_head"] == held

        # require_seat=True refuses an unclaimed target, loudly, at the tool boundary
        blind = await srv.send("ship it blind", to_agent="agent:anon-0003", ctx=ctx,
                               require_seat=True)
        assert "error" in blind and "CLAIMED seat" in blind["error"]
        assert await actions.pool.fetchval(
            "SELECT count(*) FROM fleet_messages WHERE to_agent=$1", "agent:anon-0003") == 0

        # ...and succeeds when the target IS claimed
        ok = await srv.send("ship it, verified", to_agent=held, ctx=ctx, require_seat=True)
        assert ok["seat"] == "Kastellan I"
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)


async def test_send_nags_on_an_unhedged_assertion_but_never_gates_the_send(
        actions: Actions) -> None:
    """The dispatch version of measurement_smell (thread 02e0ab9c, Thoth XC's own three
    specimens as the acceptance test, msg 6189): a flat, unhedged claim about code/system
    behavior gets an advisory `assertion_nag` on the receipt, never a refusal — the
    message sends either way, same discipline record_decision's protocol_nag already
    uses. A genuine hedge, or a plain status report, gets no nag at all."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity, claim_name

    held = "agent:c0ffee04"
    a = await actions.create_or_find_object("Agent", held, held)
    await actions.assert_property(a, "project", "bytebye", held,
                                  __import__("datetime").datetime.now(
                                      __import__("datetime").UTC), 0.9)
    await claim_name(actions, held, "Nagtarget", source=held)

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:nagger1", session="nagger1", project="nag-land", model=None, cwd=None)
    try:
        # Thoth's own three specimens (msg 6189), verbatim — all three must fire
        for i, body in enumerate((
            "the Thread-kind widening excludes Decisions, as-is it would NOT have "
            "caught the specimen",
            "a genuinely fresh database is what CI has and local doesn't",
            "obsoletes is the untested hole, mirror tests/test_capture.py:5478",
        )):
            out = await srv.send(f"{body} #{i}", to_agent=held, ctx=ctx)
            assert "assertion_nag" in out, body

        # a genuine hedge does not nag
        hedged = await srv.send(
            "I verified only the denominator, reproduce it yourself", to_agent=held, ctx=ctx)
        assert "assertion_nag" not in hedged

        # a plain status report does not nag
        status = await srv.send(
            "Merged, deployed, main is 087e51d, 4110 tests passed", to_agent=held, ctx=ctx)
        assert "assertion_nag" not in status

        # never a gate — a nagged message still sends and is readable
        nagged = await srv.send(
            "the check only ever reads the newest edge", to_agent=held, ctx=ctx)
        assert "assertion_nag" in nagged
        assert await actions.pool.fetchval(
            "SELECT 1 FROM fleet_messages WHERE id=$1", nagged["sent"]) == 1
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)


async def test_send_recognizes_a_fresh_recheck_as_a_hedge(actions: Actions) -> None:
    """FRESH-VERIFICATION IS A HEDGE TOO (thread 0ae050d8/msg 6222): the nag's own live
    traffic surfaced a real calibration gap — Thoth's OWN self-corrections (real specimens,
    msg 6218/6219/6221, paraphrased here) kept firing even though each one names the exact
    re-check it just performed ("I grepped and found FOUR live callers, not zero"). The
    design note already promises this clears the nag ("if you re-read the thing you're
    describing THIS turn, say so") — the vocabulary just didn't recognize the shape until
    now. The confirmed TRUE positive from the same night (msg 6217, no re-check language
    of the sender's own) must still fire."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity, claim_name

    held = "agent:c0ffee05"
    a = await actions.create_or_find_object("Agent", held, held)
    await actions.assert_property(a, "project", "bytebye", held,
                                  __import__("datetime").datetime.now(
                                      __import__("datetime").UTC), 0.9)
    await claim_name(actions, held, "Recheckedtarget", source=held)

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:nagger2", session="nagger2", project="nag-land", model=None, cwd=None)
    try:
        # msg 6218-shaped: names the exact re-check just performed — must NOT nag
        rechecked = await srv.send(
            "so I went and grepped. settle.py:288 is what produces the closure_coverage "
            "line every /settle prints.", to_agent=held, ctx=ctx)
        assert "assertion_nag" not in rechecked

        # msg 6219-shaped: same re-check, different phrasing — must NOT nag
        rechecked2 = await srv.send(
            "I grepped after the nag fired on me — the primitive would have cost us a "
            "rebuild if I had not caught it.", to_agent=held, ctx=ctx)
        assert "assertion_nag" not in rechecked2

        # the confirmed TRUE positive shape survives: no re-check language of the
        # sender's own, still an unhedged behavioral claim
        still_fires = await srv.send(
            "the property pair was chosen BY DESIGN, no link-retraction primitive needed",
            to_agent=held, ctx=ctx)
        assert "assertion_nag" in still_fires
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)


async def test_send_mcp_wrapper_surfaces_the_redirect_and_reads_listener_off_the_head(
    actions: Actions,
) -> None:
    """THE RECEIPT INVARIANT AT THE MCP BOUNDARY (ruling 7d6815bb, Ra XXXVI's specimen
    thread e93c2470): a DM to a stale ancestor id must never let a caller believe `seat`
    names something dead beside a `listener` reading something else's pulse — every field on
    this receipt is now sourced from the SAME identity (the delivering head), and the
    divergence from the addressed id is named explicitly in `redirect`, not left for the
    caller to reconstruct by comparing fields by hand."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity, claim_name, mint_heir
    from src.orchestrator.mounts import save_mount

    ancestor = "agent:dead0099"
    a = await actions.create_or_find_object("Agent", ancestor, ancestor)
    await claim_name(actions, ancestor, "Anubis", source=ancestor)
    heir, _ = await mint_heir(actions, ancestor, a, because="test-succession",
                              succession=None)
    # the HEIR is the one that's actually live — a real mount row, not the ancestor
    await save_mount(actions.pool, job_dir="/j/heir-live", agent_id=heir, project="alpha",
                     cwd="/w", model=None, session_key=None)

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:boss", session="boss0002", project="alpha", model=None, cwd=None)
    try:
        out = await srv.send("ship it", to_agent=ancestor, want_listener=True, ctx=ctx)
        assert out["dm_to"] == ancestor                # sent to exactly the id named
        assert out["seat"] == "Anubis II"               # the HEAD's own current handle
        assert out["lineage_head"] == heir
        assert out["listener"]["live"] is True          # the head's pulse, not the ancestor's
        assert out["redirect"] == {"addressed": ancestor, "addressed_seat": "Anubis I",
                                   "delivered": heir, "delivered_seat": "Anubis II"}
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)


async def test_reply_to_unknown_message_is_an_error(actions: Actions) -> None:
    with pytest.raises(ValueError, match="does not exist"):
        await send_message(actions.pool, from_agent="agent:x", from_project="a",
                           body="into the void", reply_to=999999)
    with pytest.raises(ValueError, match="no recipient"):
        await send_message(actions.pool, from_agent="agent:x", from_project="a", body="lost")


async def test_replying_to_your_own_dm_continues_it(actions: Actions) -> None:
    """Thread 7d670c74: `reply_to` naming a message YOU sent (not one sent to you) used to
    fall into the broadcast/supersession branch built for replying to your own BROADCAST —
    for a DM (to_project normally NULL) that raised a misleading "no recipient" error rather
    than continuing the conversation with the person you were actually DMing."""
    p = actions.pool
    dm = await send_message(p, from_agent="agent:asker", from_project="handlingtheloop",
                            to_agent="agent:engine", body="engine, status?")
    reply = await send_message(p, from_agent="agent:asker", from_project="handlingtheloop",
                               body="following up — still waiting", reply_to=dm["id"])
    assert reply["to_agent"] == "agent:engine" and reply["to"] is None
    assert reply["thread_id"] == dm["id"]
    assert await unread_count(p, "handlingtheloop", reader_agent="agent:engine") == 2


async def test_replying_to_your_own_dually_addressed_dm_still_reaches_the_agent(
        actions: Actions) -> None:
    """The silent-broadcast shape of the same bug: an original send that carried BOTH
    to_agent and to_project (nothing forbids passing both) used to let a self-reply's
    `to_p` resolve to that project ALONE — the DM recipient dropped entirely, a reply meant
    for one person landing as a project-wide broadcast nobody in particular was watching.
    The fix continues the DM verbatim, including whatever project rode along with it."""
    await _seed(actions.pool, "sidechannel")
    p = actions.pool
    dm = await send_message(p, from_agent="agent:asker", from_project="handlingtheloop",
                            to_agent="agent:engine", to_project="sidechannel",
                            body="engine, status? (cc sidechannel)")
    reply = await send_message(p, from_agent="agent:asker", from_project="handlingtheloop",
                               body="following up", reply_to=dm["id"])
    assert reply["to_agent"] == "agent:engine"  # the DM recipient survives, not just the cc


async def test_replying_to_your_own_broadcast_still_supersedes_not_a_dm(
        actions: Actions) -> None:
    """Regression guard: the fix for the DM case must not touch the existing supersession
    lane — replying to your own BROADCAST (to_agent NULL) still routes onward to the
    broadcast's own project, not into a DM."""
    await _seed(actions.pool, "b")
    p = actions.pool
    first = await send_message(p, from_agent="agent:x", from_project="a", to_project="b",
                               body="draft one")
    reply = await send_message(p, from_agent="agent:x", from_project="a",
                               body="draft two, supersedes", reply_to=first["id"])
    assert reply["to"] == "b" and reply["to_agent"] is None


async def test_dm_echoes_the_resolved_seat_and_lineage_head(actions: Actions) -> None:
    """dd47c1da: alfred's build order resolved silently to a raw agent id, unverified. A DM's
    receipt now names who it actually reached — the claimed seat, and where that id's own
    succession chain currently ends — so a dispatcher can verify the order landed, not just
    that a row was written."""
    from src.orchestrator.agents import claim_name

    held = "agent:c0ffee01"
    a = await actions.create_or_find_object("Agent", held, held)
    await actions.assert_property(a, "project", "bytebye", held,
                                  __import__("datetime").datetime.now(
                                      __import__("datetime").UTC), 0.9)
    await claim_name(actions, held, "Soundwave", source=held)
    dm = await send_message(actions.pool, from_agent="agent:boss", from_project="alpha",
                            to_agent=held, body="ship the build")
    assert dm["to_agent"] == held
    assert dm["seat"] == "Soundwave I"
    assert dm["lineage_head"] == held  # no succession yet — the id IS its own lineage head


async def test_dm_to_an_anonymous_agent_echoes_a_null_seat_and_still_sends(
        actions: Actions) -> None:
    """An unclaimed id is a valid DM target (today's behavior, byte-compatible) — the echo just
    tells the truth about it: no seat, never a guess."""
    dm = await send_message(actions.pool, from_agent="agent:boss", from_project="alpha",
                            to_agent="agent:anon-0001", body="hello?")
    assert dm["dedup"] is False
    assert dm["to_agent"] == "agent:anon-0001"
    assert dm["seat"] is None
    assert dm["lineage_head"] == "agent:anon-0001"  # nothing to walk — the id stands alone


async def test_require_seat_hard_fails_on_an_unclaimed_target_no_row_written(
        actions: Actions) -> None:
    """The gate half of dd47c1da: require_seat=True refuses to dispatch into the blind — and
    the refusal must leave nothing behind for the addressee to (mis)read as a real order."""
    with pytest.raises(ValueError, match="no CLAIMED seat"):
        await send_message(actions.pool, from_agent="agent:boss", from_project="alpha",
                           to_agent="agent:anon-0002", body="ship it", require_seat=True)
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM fleet_messages WHERE to_agent=$1", "agent:anon-0002") == 0


async def test_require_seat_succeeds_on_a_claimed_target(actions: Actions) -> None:
    from src.orchestrator.agents import claim_name

    held = "agent:c0ffee02"
    a = await actions.create_or_find_object("Agent", held, held)
    await actions.assert_property(a, "project", "bytebye", held,
                                  __import__("datetime").datetime.now(
                                      __import__("datetime").UTC), 0.9)
    await claim_name(actions, held, "Anubis", source=held)
    dm = await send_message(actions.pool, from_agent="agent:boss", from_project="alpha",
                            to_agent=held, body="ship it", require_seat=True)
    assert dm["seat"] == "Anubis I" and dm["dedup"] is False
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM fleet_messages WHERE to_agent=$1", held) == 1


async def test_a_raw_id_send_reveals_a_stale_generation_via_lineage_head(
        actions: Actions) -> None:
    """The exact shape of alfred's incident: a DM addressed by a RAW agent id (not a name)
    skips resolve_handle's seat resolution entirely, so an ancestor id superseded by mint_heir
    was never caught. The echo makes it visible WITHOUT auto-redirecting the address (reaching
    an explicit id remains an act of intent — resolve_seat's grave rule, test_a_grave_is_never_
    a_delivery_target): `to_agent` stays exactly the id named.

    UPDATED FOR THE RECEIPT INVARIANT (ruling 7d6815bb, Ra XXXVI's specimen thread e93c2470):
    `seat` used to echo the ANCESTOR's own old handle ("Ptah I") beside a `lineage_head`
    naming the current one — one receipt composing an identity fact about a retired
    generation with (elsewhere) a liveness fact about the live one. `seat` is now ALWAYS
    derived from `lineage_head` (the delivering head) when one resolves — the ancestor's own
    handle only survives in the explicit `redirect` block, never as the bare `seat` field."""
    from src.orchestrator.agents import claim_name, mint_heir

    ancestor = "agent:dead0001"
    await claim_name(actions, ancestor, "Ptah", source=ancestor)
    ancestor_oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1", ancestor)
    heir, _ = await mint_heir(actions, ancestor, ancestor_oid, because="test-succession",
                              succession=None)
    dm = await send_message(actions.pool, from_agent="agent:boss", from_project="alpha",
                            to_agent=ancestor, body="ship it")
    assert dm["to_agent"] == ancestor          # sent exactly to the id named — no silent redirect
    assert dm["seat"] == "Ptah II"             # the HEAD's own current handle, not the ancestor's
    assert dm["lineage_head"] == heir          # ...and the echo still reveals it is not current
    assert dm["lineage_head"] != dm["to_agent"]
    assert dm["redirect"] == {"addressed": ancestor, "addressed_seat": "Ptah I",
                              "delivered": heir, "delivered_seat": "Ptah II"}


async def test_replying_to_a_dm_whose_sender_superseded_itself_since_sending(
        actions: Actions) -> None:
    """THE LIVE SPECIMEN (Thoth msg 3880/3882, 2026-08-09): worker sends a DM report, then
    compacts into a new generation before the reply lands. Measured live against the real
    fleet_messages history: 90 of 1727 reply DMs (5.2%, msg 188 through msg 3884, spanning
    a full month — not a papercut) landed on a `to_agent` already superseded by the time
    the reply was sent, because reply_to's implicit routing copied `ref["from_agent"]`
    VERBATIM — the raw id stamped on the ORIGINAL message — even though this same function
    already computes `lineage_head` for that id moments later, for the eligibility gate and
    the receipt echo. The fix threads that already-computed knowledge back into what
    actually gets WRITTEN, and confesses it via `redirected_from` rather than silently
    rerouting. Unlike explicit `to_agent=` addressing (test_a_raw_id_send_reveals_a_stale_
    generation_via_lineage_head, just above) reply_to's routing was never an act of intent
    about a SPECIFIC generation — nobody chooses who a reply goes back to."""
    from src.orchestrator.agents import mint_heir

    worker = "agent:0ld0001"
    boss_oid = await actions.create_or_find_object("Agent", worker, worker)
    report = await send_message(actions.pool, from_agent=worker, from_project="alpha",
                                to_agent="agent:boss", body="task done, here's the brief")
    # the worker compacts/supersedes itself AFTER sending, BEFORE the reply lands
    heir, _ = await mint_heir(actions, worker, boss_oid, because="test-succession",
                              succession=None)
    reply = await send_message(actions.pool, from_agent="agent:boss", from_project="alpha",
                               reply_to=report["id"], body="approved")
    assert reply["lineage_head"] == heir           # the code KNOWS who's actually live...
    assert reply["to_agent"] == heir               # ...and now DELIVERS there, not the ghost
    assert reply["redirected_from"] == worker       # ...and CONFESSES the redirect, never silent


async def test_replying_to_a_still_current_sender_never_reports_a_redirect(
        actions: Actions) -> None:
    """The common case (no succession happened) must stay exactly as it always has —
    `redirected_from` only appears when a redirect actually fired."""
    p = actions.pool
    await _seed(p, "alpha")
    ask = await send_message(p, from_agent="agent:asker", from_project="alpha",
                             to_agent="agent:answerer", body="status?")
    reply = await send_message(p, from_agent="agent:answerer", from_project="alpha",
                               reply_to=ask["id"], body="green")
    assert reply["to_agent"] == "agent:asker"
    assert "redirected_from" not in reply


async def test_explicit_to_agent_with_a_stale_id_never_redirects_even_via_reply_to(
        actions: Actions) -> None:
    """EDGE CASE 2, ruled explicitly (Thoth msg 3882): a caller who passes an EXPLICIT
    to_agent= alongside reply_to is exercising intent about that exact id — even when a
    reply_to ref is also present, explicit addressing wins (`if to_agent or to_project:`
    is the first branch checked) and must never be redirected. Only reply_to's OWN implicit
    routing (no to_agent passed) gets the lineage redirect."""
    from src.orchestrator.agents import mint_heir

    worker = "agent:0ld0002"
    oid = await actions.create_or_find_object("Agent", worker, worker)
    report = await send_message(actions.pool, from_agent=worker, from_project="alpha",
                                to_agent="agent:boss2", body="report")
    heir, _ = await mint_heir(actions, worker, oid, because="test-succession", succession=None)
    assert heir != worker
    # explicit to_agent=worker, even though reply_to references a message whose implicit
    # route would also land on worker — the explicit address must win, unredirected
    explicit = await send_message(actions.pool, from_agent="agent:boss2", from_project="alpha",
                                  to_agent=worker, reply_to=report["id"], body="explicit")
    assert explicit["to_agent"] == worker
    assert "redirected_from" not in explicit
    assert explicit["lineage_head"] == heir  # staleness still visible in the echo, unacted-on


async def test_reply_to_a_dm_whose_lineage_head_is_since_retired_still_refuses(
        actions: Actions) -> None:
    """EDGE CASE 1, ruled explicitly (Thoth msg 3882): the redirect must never deliver into
    a grave and report success. This was ALREADY true before the redirect fix — the
    eligibility gate runs on `lineage_head(to_a)` regardless of what `to_a` started as, so
    a since-retired head was always refused; the stale `to_agent` this fix corrects was
    never what stood between a reply and a phantom lane. Asserted explicitly here so it
    stays proven, not assumed, now that `to_a` and `lineage_head` are the same value for
    the reply-routing path."""
    from datetime import UTC, datetime

    from src.orchestrator.agents import mint_heir

    worker = "agent:0ld0003"
    oid = await actions.create_or_find_object("Agent", worker, worker)
    report = await send_message(actions.pool, from_agent=worker, from_project="alpha",
                                to_agent="agent:boss3", body="report")
    heir, heir_oid = await mint_heir(actions, worker, oid, because="test-succession",
                                     succession=None)
    await actions.assert_property(heir_oid, "retired", True, heir, datetime.now(UTC), 0.9,
                                  evidence_class="self_declared")
    with pytest.raises(ValueError, match=f"{heir}.*retired"):
        await send_message(actions.pool, from_agent="agent:boss3", from_project="alpha",
                           reply_to=report["id"], body="into the void")


# --- THE ADDRESSING REFUSAL (rulings 1a64ae9a/aee67e6d, DM 2360 — John XV/XVI, resolved
# live): a DM by NAME whose unique seat's only holder is marked used to fall through to a
# raw handle-assertion search and land on a DEAD PREDECESSOR, reported with the confidence
# of a real resolution. seat_holder_ineligible is checked before resolve_seat ever runs. ---


async def test_send_refuses_a_name_whose_only_seat_holder_is_a_healed_phantom(
    actions: Actions,
) -> None:
    """THE NEGATIVE CONTROL (mandatory per DM 2360): John's exact live shape, reproduced —
    a unique seat, one active holder, that holder false_mint='true', and an OLDER generation
    still carrying the same `handle` assertion (the ancestor the old fallback silently
    delivered to). send() must refuse loudly, naming why, and write NOTHING — never address
    the ancestor."""
    from src.orchestrator.seats import bind_holder, ensure_seat

    seat = await ensure_seat(actions, house="osiris", handle="John", source="test")
    now = datetime.now(UTC)
    ancestor = await actions.create_or_find_object("Agent", "agent:john-xiv", "agent:john-xiv")
    await actions.assert_property(ancestor, "handle", "John", "agent:john-xiv", now, 0.9,
                                  evidence_class="self_declared")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:john-xiv")
    heir = await actions.create_or_find_object("Agent", "agent:john-xvi", "agent:john-xvi")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:john-xvi")
    await actions.assert_property(heir, "false_mint", "true", "agent:john-xvi", now, 0.9,
                                  evidence_class="self_declared")

    with pytest.raises(ValueError, match="undeliverable"):
        await send_message(actions.pool, from_agent="agent:boss", from_project="osiris",
                           to_agent="John", body="the retraction unblocks you")

    assert await actions.pool.fetchval(
        "SELECT count(*) FROM fleet_messages WHERE to_agent IN "
        "('agent:john-xiv', 'agent:john-xvi')") == 0


async def test_send_refuses_a_name_whose_only_seat_holder_is_retired(
    actions: Actions,
) -> None:
    """The OR half of the guard (retired, not just false_mint) — same refusal shape."""
    from src.orchestrator.seats import bind_holder, ensure_seat

    seat = await ensure_seat(actions, house="osiris", handle="Retired1", source="test")
    now = datetime.now(UTC)
    holder = await actions.create_or_find_object("Agent", "agent:ret-send1", "agent:ret-send1")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:ret-send1")
    await actions.assert_property(holder, "retired", "true", "agent:ret-send1", now, 0.9,
                                  evidence_class="self_declared")

    with pytest.raises(ValueError, match="undeliverable"):
        await send_message(actions.pool, from_agent="agent:boss", from_project="osiris",
                           to_agent="Retired1", body="ship it")


async def test_send_still_delivers_to_a_name_with_an_eligible_seat_holder(
    actions: Actions,
) -> None:
    """REGRESSION PROOF: the ordinary, working case — a seat with one clean, live holder —
    must be completely unaffected by the new check."""
    from src.orchestrator.seats import bind_holder, ensure_seat

    seat = await ensure_seat(actions, house="osiris", handle="Clean", source="test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:clean0001")

    dm = await send_message(actions.pool, from_agent="agent:boss", from_project="osiris",
                            to_agent="Clean", body="ship it")
    assert dm["to_agent"] == seat["seat_id"]
    assert dm["seat"] == "Clean"
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM fleet_messages WHERE to_agent=$1", seat["seat_id"]) == 1


async def test_send_delivers_through_an_older_eligible_holder_despite_a_newer_marked_one(
    actions: Actions,
) -> None:
    """THE ACCEPTANCE TEST FOR THOTH'S CORRECTION (DM 2377): two active holds edges on one
    seat — the newer marked, the older eligible — the exact shape live on seat:c476e7a2
    (decision 6ce4ac5f). Before this fix, seat_holder_ineligible looked only at the newest
    edge and refused; after it, send() must DELIVER to the seat (binding_of_handle's own
    ranking, unaffected by the fix, already resolved this correctly)."""
    from src.orchestrator.seats import ensure_seat

    seat = await ensure_seat(actions, house="osiris", handle="TwoHoldersLive", source="test")
    seat_oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1", seat["seat_id"])
    now = datetime.now(UTC)
    older = await actions.create_or_find_object("Agent", "agent:live-older", "agent:live-older")
    await actions.create_link(older, seat_oid, "holds", "test", now - timedelta(minutes=5), 0.9,
                              evidence_class="self_declared")
    newer = await actions.create_or_find_object("Agent", "agent:live-newer", "agent:live-newer")
    await actions.create_link(newer, seat_oid, "holds", "test", now, 0.9,
                              evidence_class="self_declared")
    await actions.assert_property(newer, "false_mint", "true", "agent:live-newer", now, 0.9,
                                  evidence_class="self_declared")

    dm = await send_message(actions.pool, from_agent="agent:boss", from_project="osiris",
                            to_agent="TwoHoldersLive", body="ship it")
    assert dm["to_agent"] == seat["seat_id"]
    assert dm["seat"] == "TwoHoldersLive"


async def test_send_to_a_raw_seat_id_is_unaffected_by_the_new_check(
    actions: Actions,
) -> None:
    """REGRESSION PROOF: the working seat-addressed path (to_agent='seat:<id>') is an
    entirely separate branch above the name-resolution one this change touches — shown
    working here, not merely asserted unchanged."""
    from src.orchestrator.seats import bind_holder, ensure_seat

    seat = await ensure_seat(actions, house="osiris", handle="DirectSeat", source="test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:direct0001")

    dm = await send_message(actions.pool, from_agent="agent:boss", from_project="osiris",
                            to_agent=seat["seat_id"], body="ship it, addressed by seat id")
    assert dm["to_agent"] == seat["seat_id"]
    assert dm["seat"] == "DirectSeat"


async def test_send_still_refuses_a_genuinely_unknown_name(actions: Actions) -> None:
    """No seat, no agent, nothing — seat_holder_ineligible must stand aside (None) and the
    ORIGINAL "no agent named" refusal must still fire, unchanged wording."""
    with pytest.raises(ValueError, match="no agent named"):
        await send_message(actions.pool, from_agent="agent:boss", from_project="osiris",
                           to_agent="TotallyUnknownName", body="hello?")


async def test_send_still_uses_the_assertion_fallback_for_an_unseated_name(
    actions: Actions,
) -> None:
    """REGRESSION PROOF: an un-seated lineage (no Seat object at all) must keep resolving
    through the old assertion path exactly as before — seat_holder_ineligible returns None
    for 'no such seat' and must never block it."""
    now = datetime.now(UTC)
    a = await actions.create_or_find_object("Agent", "agent:unseated01", "agent:unseated01")
    await actions.assert_property(a, "handle", "Unseated", "agent:unseated01", now, 0.9,
                                  evidence_class="self_declared")

    dm = await send_message(actions.pool, from_agent="agent:boss", from_project="osiris",
                            to_agent="Unseated", body="ship it")
    assert dm["to_agent"] == "agent:unseated01"


async def test_inbox_is_scoped_and_normalized(actions: Actions) -> None:
    p = actions.pool
    await _seed(p, "sibling-one")
    await send_message(p, from_agent="agent:x", from_project="a", to_project="sibling-one",
                       body="yo")
    assert await unread_count(p, "sibling-two", reader_agent="agent:d") == 0
    assert await read_inbox(p, "sibling-two", reader_agent="agent:d") == []
    assert await unread_count(p, "sibling-one", reader_agent=R) == 1
    assert await unread_count(p, "repo:sibling-one", reader_agent=R) == 1  # repo: prefix normalized


async def test_operator_is_an_ordinary_reader(actions: Actions) -> None:
    p = actions.pool
    await send_message(p, from_agent="agent:x", from_project="sibling-two",
                       to_project=OPERATOR_ADDR, body="finding: emergence confirmed")
    assert await unread_count(p, OPERATOR_ADDR, reader_agent=OPERATOR_ADDR) == 1
    (m,) = await read_inbox(p, OPERATOR_ADDR, reader_agent=OPERATOR_ADDR, mark_read=False)
    assert m["from_project"] == "sibling-two"


async def test_lease_is_visible_to_the_group(actions: Actions) -> None:
    """A sibling holding a live lease on shared broadcast mail is VISIBLE (in_flight names it) —
    'mail 0' must never hide 'the other agent is answering the group thread right now'."""
    p = actions.pool
    await _seed(p, "handlingtheloop")
    await send_message(p, from_agent="agent:x", from_project="sibling-two",
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
    await send_message(p, from_agent="agent:b", from_project="sibling-three",
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
    for i, proj in enumerate(("sibling-eight", "Like-Us", "neo")):
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
    assert {m["project"] for m in folded["same_story"]["also"]} == {"sibling-eight", "Like-Us"}
    assert "neo" in folded["body"]  # newest telling leads
    # ONE sender's similar briefs never same-story fold (their lane is reply_to thread-fold):
    # the false-fold guard — same-story means one condition, SEVERAL witnesses (live desk
    # false pair 300 vs 237 measured 0.294; threshold 0.30 + this guard)
    solo_story = "the quokka pipeline stalled at stage {} — retry budget exhausted, halting"
    for stage in ("four", "five"):
        await send_message(p, from_agent="agent:q", from_project="tony",
                           to_project=OPERATOR_ADDR, body=solo_story.format(stage),
                           desk_kind="fyi")
    desk2 = await read_desk(p)
    quokka = [c for c in desk2["fyi"] if "quokka" in c["body"]]
    assert len(quokka) == 2 and all("same_story" not in c for c in quokka)


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
    await _seed(p, "osiris")
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


async def test_an_agents_OWN_broadcast_is_not_its_mail(actions: Actions) -> None:
    """THE SELF-ECHO (Metron V, msgs 444/446 — six blocked turns in one night, all of them to
    acknowledge his own voice). A project broadcast fanned out to its own author: every send()
    came back as unread, the blocking stop hook fired on it, and the author paid a full read
    to learn it had written the thing. Worse than noise — the author's reflexive self-ack
    marked the broadcast SETTLED, silencing the wake for the real recipient. The author is
    excluded from its own broadcast's fan-out; the DM path always was (his own A/B: msg 444
    the DM, no echo; msg 445 the broadcast reply, echoed)."""
    p = actions.pool
    author, peer = "agent:metron", "agent:deckard"
    await _seed(p, "xxit")
    res = await send_message(p, from_agent=author, from_project="xxit", to_project="xxit",
                             body="thread 413: the aligner acts only when it beats doing nothing")
    # the author never sees its own words as mail; the peer does
    assert await unread_count(p, "xxit", reader_agent=author) == 0
    assert await unread_count(p, "xxit", reader_agent=peer) == 1
    assert await read_inbox(p, "xxit", reader_agent=author) == []
    # and the wake signal stays ARMED until the real recipient settles it — the author's
    # phantom self-ack can no longer silence it
    assert await project_deliverable_count(p, "xxit", lease_secs=0) == 1
    (m,) = await read_inbox(p, "xxit", reader_agent=peer)
    await ack_messages(p, "xxit", [m["id"]], reader_agent=peer)
    assert await project_deliverable_count(p, "xxit", lease_secs=0) == 0
    assert res["id"] > 0


async def test_mail_grade_names_the_asks(actions: Actions) -> None:
    """f9449d8d — MAIL SAYS WHAT IT WANTS. The sender grades its own letter ('ask' | 'fyi'),
    the reader's count can lead with what is actionable, and ungraded mail is never guessed
    into a band — a wrong "needs nothing" on a duty-bearing letter would silence it."""
    p = actions.pool
    await _seed(p, "b")
    await send_message(p, from_agent="agent:x", from_project="a", to_project="b",
                       body="please review the seam plan", grade="ask")
    await send_message(p, from_agent="agent:x", from_project="a", to_project="b",
                       body="deploy landed, nothing needed", grade="fyi")
    await send_message(p, from_agent="agent:x", from_project="a", to_project="b",
                       body="legacy ungraded letter")
    assert await unread_count(p, "b", reader_agent=R) == 3
    assert await unread_count(p, "b", reader_agent=R, grade="ask") == 1
    assert await unread_count(p, "b", reader_agent=R, grade="fyi") == 1
    box = await read_inbox(p, "b", reader_agent=R)
    by_body = {m["body"]: m for m in box}
    assert by_body["please review the seam plan"]["grade"] == "ask"
    assert by_body["deploy landed, nothing needed"]["grade"] == "fyi"
    assert "grade" not in by_body["legacy ungraded letter"]  # honest ignorance, not a guess


async def test_mail_grade_rejects_an_invented_band(actions: Actions) -> None:
    with pytest.raises(ValueError):
        await send_message(actions.pool, from_agent="agent:x", from_project="a",
                           to_project="b", body="now", grade="urgent")


# --- the cross-project return + the scoped desk (Werner's leak, 2026-07-16) ---


async def test_cross_project_reply_returns_to_the_askers_seat_not_the_room(
    actions: Actions,
) -> None:
    """Werner's leak: a reply to a foreign project's broadcast used to return to that
    project's whole ROOM — gestalt's audit, addressed to 'whoever commissioned this',
    landed in every bytebye reader's inbox. The seat is the address now: a seat-bound
    asker gets the reply as a seat DM, invisible to housemates."""
    from datetime import UTC, datetime

    from src.orchestrator.agents import claim_name

    p = actions.pool
    asker = "agent:a5ce0001"
    a = await actions.create_or_find_object("Agent", asker, asker)
    await actions.assert_property(a, "project", "askhouse", asker,
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")
    await claim_name(actions, asker, "Commissioner", source=asker)

    await _seed(p, "farhouse")
    ask = await send_message(p, from_agent=asker, from_project="askhouse",
                             to_project="farhouse", body="audit the family")
    rep = await send_message(p, from_agent="agent:fa40b001", from_project="farhouse",
                             reply_to=ask["id"], body="audit done, exemptions listed")

    assert rep["to_agent"] is not None and rep["to_agent"].startswith("seat:")
    assert rep["to"] is None                              # never the room
    mine = await read_inbox(p, "askhouse", reader_agent=asker)
    assert any("audit done" in m["body"] for m in mine)   # the holder reads it
    housemate = await read_inbox(p, "askhouse", reader_agent="agent:b7770002")
    assert not any("audit done" in m["body"] for m in housemate)  # the housemate never sees it


async def test_cross_project_reply_to_an_unbound_asker_keeps_the_room(
    actions: Actions,
) -> None:
    """A TRANSIENT id (no object in the graph) keeps the pre-seat law: the reply returns
    to the asker's project room — a DM to an id the graph never registered would strand
    the mail; the room at least reaches the house."""
    p = actions.pool
    await _seed(p, "farhouse")
    ask = await send_message(p, from_agent="agent:0abe4003", from_project="oldhouse",
                             to_project="farhouse", body="anyone: check the gauge")
    rep = await send_message(p, from_agent="agent:fa40b001", from_project="farhouse",
                             reply_to=ask["id"], body="gauge checked")
    assert rep["to"] == "oldhouse" and rep["to_agent"] is None


async def test_sending_a_message_never_makes_the_sender_dm_eligible(actions: Actions) -> None:
    """PINS THE ROOT CAUSE of the two tests above: the 6a1dd99 GRAPH EDGES write-through
    used to `create_or_find_object("Agent", from_agent, ...)` unconditionally, silently
    minting a bare Agent object for EVERY sender — which satisfied `_dm_ineligibility`'s
    own "no Agent object = the graph has never met this mind" contract one send later,
    collapsing "known mind" into "has sent one message ever" and breaking the room-return
    law for a genuinely transient asker. A plain send must never change this id's own
    eligibility for a FUTURE reply to address it directly."""
    from src.orchestrator.mailbox import _dm_ineligibility

    p = actions.pool
    await _seed(p, "farhouse")
    await send_message(p, from_agent="agent:transient001", from_project="farhouse",
                       to_project="farhouse", body="just passing through")
    assert await _dm_ineligibility(p, "agent:transient001") == "unknown"


async def test_cross_project_reply_follows_the_mind_not_the_room(
    actions: Actions,
) -> None:
    """MAIL FOLLOWS THE MIND, NOT THE ROOM (operator ruling 2026-07-19, thread 07d64473:
    Atlas asked from the xxit room he was visiting; the reply landed on Metron, the room's
    resident). An unbound asker the GRAPH KNOWS — a registered, active agent — gets the
    reply as a DM to their living head; the room's resident never inherits it. The
    registered object is what separates a traveler from a transient id."""
    p = actions.pool
    traveler = "agent:a5ce9901"
    await actions.create_or_find_object("Agent", traveler, traveler)
    await _seed(p, "farhouse")
    ask = await send_message(p, from_agent=traveler, from_project="visitedroom",
                             to_project="farhouse", body="handing off the store build")
    rep = await send_message(p, from_agent="agent:fa40b001", from_project="farhouse",
                             reply_to=ask["id"], body="estate landed, thank you")
    assert rep["to_agent"] == traveler and rep["to"] is None   # a DM to the mind
    resident = await read_inbox(p, "visitedroom", reader_agent="agent:re51de99")
    assert not any("estate landed" in m["body"] for m in resident)  # the room never sees it


async def test_reply_to_a_retired_mind_keeps_the_room(actions: Actions) -> None:
    """The eligibility law at the reply lane (thread 21596481): a retired head is never a
    DM target — the room return is the honest fallback, exactly the old law."""
    from datetime import UTC, datetime

    p = actions.pool
    gone = "agent:0dead901"
    o = await actions.create_or_find_object("Agent", gone, gone)
    await actions.assert_property(o, "retired", True, gone, datetime.now(UTC), 0.9,
                                  evidence_class="self_declared")
    await _seed(p, "farhouse")
    ask = await send_message(p, from_agent=gone, from_project="oldroom",
                             to_project="farhouse", body="one last ask")
    rep = await send_message(p, from_agent="agent:fa40b001", from_project="farhouse",
                             reply_to=ask["id"], body="answered late")
    assert rep["to"] == "oldroom" and rep["to_agent"] is None


async def test_a_dm_to_a_dead_lineage_fails_loudly_never_parks(actions: Actions) -> None:
    """The eligibility law at the DIRECT lane (thread 21596481; the msg-192 misdelivery
    is the fixture — a DM routed to a retired phantom lane): a lineage whose newest
    generation is KNOWN-dead can never read the mail under any address — the send raises,
    NAMING who was found and why, instead of parking it. A mid-generation address whose
    HEAD lives keeps rolling up exactly as before, and an id the graph merely hasn't met
    stays deliverable (registration can lag a living mind)."""
    from datetime import UTC, datetime

    p = actions.pool
    # a whole lineage ended: base → -ii, and -ii (the head) is retired
    base = await actions.create_or_find_object("Agent", "agent:0dead902", "test")
    head = await actions.create_or_find_object("Agent", "agent:0dead902-ii", "test")
    now = datetime.now(UTC)
    await actions.assert_property(base, "succeeded_by", "agent:0dead902-ii", "test",
                                  now, 0.9, evidence_class="self_declared")
    await actions.assert_property(head, "retired", True, "test", now, 0.9,
                                  evidence_class="self_declared")
    with pytest.raises(ValueError, match="agent:0dead902-ii.*retired"):
        await send_message(p, from_agent="agent:5e4de001", from_project="x",
                           to_agent="agent:0dead902", body="into the void")
    with pytest.raises(ValueError, match="phantom"):
        await send_message(p, from_agent="agent:5e4de001", from_project="x",
                           to_agent="agent:0dead902-ii", body="straight at the corpse")
    # a LIVING head keeps the rollup: mid-generation address delivers, receipt names it
    lb = await actions.create_or_find_object("Agent", "agent:a11ce001", "test")
    lh = await actions.create_or_find_object("Agent", "agent:a11ce001-ii", "test")
    assert lb is not None and lh is not None
    await actions.assert_property(lb, "succeeded_by", "agent:a11ce001-ii", "test",
                                  now, 0.9, evidence_class="self_declared")
    ok = await send_message(p, from_agent="agent:5e4de001", from_project="x",
                            to_agent="agent:a11ce001", body="to the living")
    assert ok["lineage_head"] == "agent:a11ce001-ii"
    # ...and an id the graph has never met is still deliverable, not refused
    ok2 = await send_message(p, from_agent="agent:5e4de001", from_project="x",
                             to_agent="agent:00fresh1", body="to a stranger")
    assert ok2["to_agent"] == "agent:00fresh1"


async def test_a_dm_to_a_flagged_dead_id_delivers_when_a_live_body_still_occupies_it(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """THE HALCYON ESCAPE HATCH (obligation 6b1efacb, 2026-08-18): a retired/false_mint
    stamp is a BELIEF (exactly the zero-turn fold's own blindness this obligation's other
    half fixes) — when the harness/OS confirms a live body still sits at that id, the DM
    must still reach it, never hard-refuse on a stamp the OS itself contradicts."""
    from src.orchestrator import mounts

    p = actions.pool
    head = await actions.create_or_find_object("Agent", "agent:0live901", "test")
    await actions.assert_property(head, "false_mint", True, "test", datetime.now(UTC), 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(head, "retired", True, "test", datetime.now(UTC), 0.9,
                                  evidence_class="self_declared")
    # THE MATCH KEY IS EXACTLY 8 CHARS (registry_census keys agent_mounts.job_dir's own
    # basename against sessionId[:8]).
    await mounts.save_mount(p, job_dir="/x/jobs/0liv9012", agent_id="agent:0live901",
                            project="demo", cwd="/repo/demo", model=None, session_key=None)

    from src.orchestrator import trigger

    async def _fake_agents_json(**kw: object) -> list[dict[str, object]]:
        return [{"sessionId": "0liv9012-0000-4000-8000-000000000000", "pid": 555,
                 "cwd": "/repo/demo", "name": "[OS] StillLive"}]
    monkeypatch.setattr(trigger, "_claude_agents_json", _fake_agents_json)

    from src.orchestrator import census

    monkeypatch.setattr(
        census, "_proc_exe",
        lambda pid: "/home/x/.local/share/claude/versions/2.1.210")
    monkeypatch.setattr(census, "_proc_cwd", lambda pid: "/repo/demo")

    ok = await send_message(p, from_agent="agent:5e4de001", from_project="x",
                            to_agent="agent:0live901", body="are you still there")
    assert ok["to_agent"] == "agent:0live901"
    assert ok["lineage_head"] == "agent:0live901"
    assert ok["redirect"] is not None
    assert ok["redirect"]["delivered_despite_flag"] in ("retired", "false_mint")


async def test_desk_briefs_scope_to_the_senders_lineage(actions: Actions) -> None:
    """The scoped desk (operator ruling, 2026-07-16): an agent's chrome counts ITS OWN
    unanswered briefs — lineage-wide — never the fleet's backlog."""
    from src.orchestrator.mailbox import desk_briefs_from

    p = actions.pool
    await send_message(p, from_agent="agent:ba5e0001-ii", from_project="x",
                       to_project=OPERATOR_ADDR, body="my milestone brief")
    await send_message(p, from_agent="agent:0abe4002", from_project="y",
                       to_project=OPERATOR_ADDR, body="someone else's brief")

    assert await desk_briefs_from(p, "agent:ba5e0001") == 1        # the base sees its line's
    assert await desk_briefs_from(p, "agent:ba5e0001-iii") == 1    # any generation, same line
    assert await desk_briefs_from(p, "agent:0abe4002") == 1
    assert await desk_briefs_from(p, None) == 0                    # identity-less: nothing waits
    assert await desk_briefs_from(p, "session") == 0


async def test_a_dm_on_an_old_generation_rolls_up_to_the_living_reader(
    actions: Actions,
) -> None:
    """THE ROLLUP (operator, 2026-07-17: 'mail addressed to dead agents should roll up
    to the current live agent'): an exact-id DM parked on ANY generation of the reader's
    lineage is deliverable at read time — the count and the read agree (Atlas's split:
    the statusline counted a DM on -ii while freshly-minted -iii read an empty inbox)."""
    p = actions.pool
    await send_message(p, from_agent="agent:0ffab001", from_project="offahouse",
                       to_agent="agent:2011ab01-ii", body="for the old mind")
    # the lineage ticked twice since; the current reader is -iv
    n = await unread_count(p, "elsewhere", reader_agent="agent:2011ab01-iv")
    assert n == 1                                     # the count sees the lineage's lane
    got = await read_inbox(actions.pool, "elsewhere", reader_agent="agent:2011ab01-iv")
    assert [m["body"] for m in got] == ["for the old mind"]   # ...and so does the read
    # a STRANGER's lineage never matches
    assert await unread_count(p, "elsewhere", reader_agent="agent:aaaa9999") == 0


async def test_a_lineage_mate_inherits_settled_state_without_going_through_mint_heir(
    actions: Actions,
) -> None:
    """THE FORK POPULATION (threads af911f47/00378259, Thoth's word 2026-07-29, DM 1856 —
    Option A): mint_heir already copies message_recipients rows forward on a TRUE
    succession (agents.py). This proves the OTHER population — an identity that reaches
    a shared lineage base-prefix WITHOUT mint_heir ever running (a true fork, or a fresh
    body landing on an existing handle) — no longer re-inherits what a SIBLING generation
    already settled. -iii settles a DM; -vii (no mint_heir copy between them, simulating
    exactly that gap) must see it as already handled, not fresh."""
    p = actions.pool
    dm = await send_message(p, from_agent="agent:c1b99f6e-ii", from_project="ByeByte",
                            to_agent="agent:f0e5a001-iii", body="report for the fork test")
    # -iii itself settles it — no mint_heir copy ever runs to -vii
    (m,) = await read_inbox(p, "forkhouse", reader_agent="agent:f0e5a001-iii")
    assert m["id"] == dm["id"]
    out = await ack_messages(p, "forkhouse", [dm["id"]], reader_agent="agent:f0e5a001-iii")
    assert out["settled"] == [dm["id"]]
    # -vii has NO message_recipients row of its own (mint_heir never ran for it) — before
    # this fix, the exact-id-only settle check would show this as fresh, unread mail
    assert await unread_count(p, "forkhouse", reader_agent="agent:f0e5a001-vii") == 0
    assert await read_inbox(p, "forkhouse", reader_agent="agent:f0e5a001-vii") == []


async def test_a_genuinely_new_lineage_in_an_old_project_still_sees_standing_broadcasts(
    actions: Actions,
) -> None:
    """THE COUNTER-CASE (Thoth's own instinct, DM 1843/1856): the settle-state rollup
    only widens what counts as MY lineage already having answered — it must never touch
    the broadcast/project-wide clause. A genuinely new, UNRELATED lineage (no shared
    base-prefix with anyone) landing in an old, populated project still sees a standing
    broadcast that NOBODY in its own (empty) lineage has ever settled — exactly as
    before this fix, because the new NOT EXISTS check is scoped to MY OWN lineage's
    rows, and a fresh lineage has none."""
    p = actions.pool
    await _seed(p, "oldhouse")
    await send_message(p, from_agent="agent:veteran001", from_project="oldhouse",
                       to_project="oldhouse", body="standing, never-settled project ask")
    # a completely unrelated fresh lineage — no generation of it has ever touched this
    n = await unread_count(p, "oldhouse", reader_agent="agent:newcomer9999")
    assert n == 1
    got = await read_inbox(p, "oldhouse", reader_agent="agent:newcomer9999")
    assert [msg["body"] for msg in got] == ["standing, never-settled project ask"]


async def test_desk_briefs_count_folds_threads_and_skips_the_dimmed(
    actions: Actions,
) -> None:
    """THE BRIEFS NUMBER FOLDS AS THE PAGE FOLDS (operator ruling 2026-07-19: the pulse
    counted every unread row while the desk page thread-folded and dimmed — same word,
    different number). A superseding brief in a thread speaks for it; a moot-dimmed brief
    carries no count."""
    from src.orchestrator.mailbox import desk_briefs_from, desk_briefs_total

    p = actions.pool
    first = await send_message(p, from_agent="agent:ab12ef01", from_project="x",
                               to_project=OPERATOR_ADDR, body="milestone v1")
    await send_message(p, from_agent="agent:ab12ef01", from_project="x",
                       to_project=OPERATOR_ADDR, body="milestone v2 — supersedes v1",
                       reply_to=first["id"])
    await send_message(p, from_agent="agent:0abe5502", from_project="y",
                       to_project=OPERATOR_ADDR, body="a separate brief")
    total = await desk_briefs_total(p)
    assert total == 2  # v2 speaks for its thread; v1 rides under it
    assert await desk_briefs_from(p, "agent:ab12ef01") == 1
    assert await desk_briefs_from(p, "agent:0abe5502") == 1
    await p.execute(
        "UPDATE fleet_messages SET moot_note='landed already', moot_by='agent:x' "
        "WHERE body LIKE 'a separate brief%'")
    assert await desk_briefs_total(p) == 1  # dimmed briefs carry no count


async def test_unread_split_sums_to_unread_count(actions: Actions) -> None:
    """The statusline's two segments are the SAME predicate as orient's one number —
    split by lane, never a second formula (the copy-drift that made mail disagree)."""
    p = actions.pool
    me = "agent:ab12aa77"
    await _seed(p, "myroom")
    await send_message(p, from_agent="agent:aaa", from_project="elsewhere",
                       to_project="myroom", body="a broadcast for the room")
    await send_message(p, from_agent="agent:aaa", from_project="elsewhere",
                       to_agent=me, body="a dm for me")
    from src.orchestrator.mailbox import unread_split
    split = await unread_split(p, "myroom", reader_agent=me)
    total = await unread_count(p, "myroom", reader_agent=me)
    assert split == {"mail": 1, "dm": 1}
    assert split["mail"] + split["dm"] == total


async def test_send_tool_echoes_the_per_hop_dispatch_receipt(actions: Actions) -> None:
    """The adapter's visibility half (ruling 6c4d0b62): a DM's send() receipt carries the
    PER-HOP dispatch outcome — what actually happened on arrival, never a guess about a
    future sweep. In this hermetic world the trigger is dark, and the receipt says exactly
    that instead of pretending a wake is coming."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:alpha-1", session="alpha001", project="alpha", model=None, cwd=None)
    try:
        out = await srv.send("a word for you alone", to_agent="agent:beta-9", ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert out["dispatch"]["mode"] == "trigger-dark"
    assert "dark" in out["dispatch"]["detail"]


async def test_pause_seat_tool_stamps_the_lever_the_dispatch_reads(
    actions: Actions,
) -> None:
    """The explicit per-seat pause control (6c4d0b62 wall #2), tool to dispatch: pausing
    stamps the lever the DM push lane checks; releasing is the next word, latest wins."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity
    from src.orchestrator.trigger import _paused

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:alpha-1", session="alpha001", project="alpha", model=None, cwd=None)
    try:
        out = await srv.pause_seat(reason="deep work — hold my mail", ctx=ctx)
        assert out["paused"] == "agent:alpha-1" and out["by"] == "agent:alpha-1"
        assert await _paused(actions.pool, ["agent:alpha-1"]) == "agent:alpha-1"
        out2 = await srv.pause_seat(paused=False, ctx=ctx)
        assert out2["released"] == "agent:alpha-1"
        assert await _paused(actions.pool, ["agent:alpha-1"]) is None
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)


async def test_pause_seat_refuses_a_target_name_whose_only_holder_is_ineligible(
    actions: Actions,
) -> None:
    """task #142 punch-list item 3 (Thoth's dispatch DM 4097): pause_seat's plain-name
    branch explicitly says it "resolves like a DM address does" — it must get the SAME
    grave-delivery guard send() has, or a pause meant for a live seat could silently land
    on some OTHER, older, unmarked generation while the real seat stays unpaused."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity
    from src.orchestrator.seats import bind_holder, ensure_seat
    from src.orchestrator.trigger import _paused

    seat = await ensure_seat(actions, house="osiris", handle="PauseGhost", source="test")
    now = datetime.now(UTC)
    ancestor = await actions.create_or_find_object(
        "Agent", "agent:pauseghost-old", "agent:pauseghost-old")
    await actions.assert_property(ancestor, "handle", "PauseGhost", "agent:pauseghost-old",
                                  now, 0.9, evidence_class="self_declared")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:pauseghost-old")
    heir = await actions.create_or_find_object(
        "Agent", "agent:pauseghost-new", "agent:pauseghost-new")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:pauseghost-new")
    await actions.assert_property(heir, "false_mint", "true", "agent:pauseghost-new", now,
                                  0.9, evidence_class="self_declared")

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:pauser-1", session="pauser001", project="osiris", model=None, cwd=None)
    try:
        out = await srv.pause_seat(target="PauseGhost", ctx=ctx)
        assert "error" in out
        assert seat["seat_id"] in out["error"]
        assert await _paused(actions.pool, ["agent:pauseghost-old"]) is None
        assert await _paused(actions.pool, ["agent:pauseghost-new"]) is None
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)


async def _owner(actions: Actions, tid: object) -> str | None:
    return await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='owner' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", tid)


async def test_threads_param_transfers_ownership_to_the_dm_recipient(
    actions: Actions,
) -> None:
    """Phase 1c (decision 79533336): the ownership fact is created at the moment of
    send(), not remembered/inferred afterward. An explicit `threads=` re-points the named
    Thread's `owner` to the resolved addressee — a TRANSFER on the same mechanism
    reclassify_thread already exposes, not a new concept."""
    from src.orchestrator.capture import open_thread

    tid = await open_thread(actions, "P3 piece 1: deploy smoke races the service",
                            kind="obligation", owner="agent:thoth", source="agent:thoth")
    assert await _owner(actions, tid) == "agent:thoth"
    res = await send_message(actions.pool, from_agent="agent:thoth", from_project="osiris",
                             to_agent="agent:worker", body="yours now", grade="ask",
                             threads=[str(tid)])
    assert res["threads_stamped"] == [str(tid)]
    assert await _owner(actions, tid) == "agent:worker"


async def test_threads_param_accepts_a_short_id(actions: Actions) -> None:
    """The common case: a sender types the 8-char short id orient()/dossier() already hand
    out, not the full uuid."""
    from src.orchestrator.capture import open_thread

    tid = await open_thread(actions, "P3 piece 2: ingest registration phase 2",
                            kind="obligation", source="agent:thoth")
    res = await send_message(actions.pool, from_agent="agent:thoth", from_project="osiris",
                             to_agent="agent:worker", body="yours now",
                             threads=[str(tid)[:8]])
    assert res["threads_stamped"] == [str(tid)]
    assert await _owner(actions, tid) == "agent:worker"


async def test_threads_param_refuses_an_ambiguous_short_id_no_message_written(
    actions: Actions,
) -> None:
    """#117's law, with the live specimen that surfaced it (resolves="28842543" matched 2
    live Threads, DM 2510/2513): named-enough-to-identify is not named-enough-to-auto-
    stamp. Refusing must be loud AND must not leave a message row behind — same guarantee
    require_seat already gives (THE ECHO + THE GATE, resolved before any write)."""
    import uuid
    from datetime import UTC
    from datetime import datetime as dt

    shared = "deadbeef"
    id_a = uuid.UUID(f"{shared}-0000-4000-8000-000000000001")
    id_b = uuid.UUID(f"{shared}-0000-4000-8000-000000000002")
    for oid, summary in ((id_a, "collision candidate A"), (id_b, "collision candidate B")):
        await actions.pool.execute(
            "INSERT INTO objects (id, type, canonical, status) VALUES ($1, 'Thread', $2, "
            "'active')", oid, f"thread:manual-{oid}")
        await actions.assert_property(oid, "summary", summary, "test", dt.now(UTC), 0.9,
                                      evidence_class="self_declared")
    before = await actions.pool.fetchval("SELECT count(*) FROM fleet_messages")
    with pytest.raises(ValueError, match="matches 2 Thread"):
        await send_message(actions.pool, from_agent="agent:thoth", from_project="osiris",
                           to_agent="agent:worker", body="ambiguous dispatch",
                           threads=[shared])
    after = await actions.pool.fetchval("SELECT count(*) FROM fleet_messages")
    assert after == before


async def test_threads_param_refuses_an_unknown_ref(actions: Actions) -> None:
    with pytest.raises(ValueError, match="no Thread matches"):
        await send_message(actions.pool, from_agent="agent:thoth", from_project="osiris",
                           to_agent="agent:worker", body="ghost thread",
                           threads=["ffffffff"])


async def test_threads_param_requires_a_single_resolved_addressee(actions: Actions) -> None:
    """Ownership transfer has nowhere to land on a broadcast — a project room is not an
    addressee."""
    from src.orchestrator.capture import open_thread

    tid = await open_thread(actions, "needs an owner", kind="obligation")
    await _seed(actions.pool, "osiris")
    with pytest.raises(ValueError, match="single resolved"):
        await send_message(actions.pool, from_agent="agent:thoth", from_project="osiris",
                           to_project="osiris", body="whoever picks this up",
                           threads=[str(tid)])
    assert await _owner(actions, tid) is None


async def test_send_never_infers_thread_ownership_from_body_prose(actions: Actions) -> None:
    """NO PROSE INFERENCE, EVER (Thoth's ruling on DM 2513, citing cb38d922's own 232-
    mentions/25-edged finding): a thread's short id sitting in the message BODY must never
    move ownership on its own — only an explicit `threads=` ref does."""
    from src.orchestrator.capture import open_thread

    tid = await open_thread(actions, "mentioned but not assigned", kind="obligation",
                            owner="agent:thoth", source="agent:thoth")
    res = await send_message(
        actions.pool, from_agent="agent:thoth", from_project="osiris", to_agent="agent:worker",
        body=f"fyi, related to thread {str(tid)[:8]}, but not yours to own")
    assert "threads_stamped" not in res
    assert await _owner(actions, tid) == "agent:thoth"


async def test_threads_param_is_idempotent_across_a_dedup_retry(actions: Actions) -> None:
    from src.orchestrator.capture import open_thread

    tid = await open_thread(actions, "retry-safe dispatch", kind="obligation",
                            owner="agent:thoth", source="agent:thoth")
    first = await send_message(actions.pool, from_agent="agent:thoth", from_project="osiris",
                               to_agent="agent:worker", body="same body twice",
                               threads=[str(tid)])
    second = await send_message(actions.pool, from_agent="agent:thoth", from_project="osiris",
                                to_agent="agent:worker", body="same body twice",
                                threads=[str(tid)])
    assert first["dedup"] is False and second["dedup"] is True
    assert first["id"] == second["id"]
    assert second["threads_stamped"] == [str(tid)]
    assert await _owner(actions, tid) == "agent:worker"


async def test_send_tool_forwards_threads_and_echoes_threads_stamped(actions: Actions) -> None:
    """The MCP surface for Phase 1c (Thoth's follow-up, msg 2536): send_message owns the
    ownership-transfer semantics (tested above); mcp_server.send() just has to pass
    `threads` through and not swallow `threads_stamped` on the way out — same discipline
    as the seat/lineage_head echo test above this one."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity
    from src.orchestrator.capture import open_thread

    tid = await open_thread(actions, "P129 prep: dispatched via the MCP tool, not the "
                            "orchestrator function directly", kind="obligation",
                            owner="agent:thoth", source="agent:thoth")

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:thoth", session="thoth0001", project="osiris", model=None, cwd=None)
    try:
        out = await srv.send("yours now, via the tool", to_agent="agent:worker",
                             threads=[str(tid)[:8]], ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert out["threads_stamped"] == [str(tid)]
    assert await _owner(actions, tid) == "agent:worker"


# ═══ THE PHANTOM BROADCAST — shape 3 of #117 (obligation 45e52530): to_project used to
# write with NO existence check at all, so send(to=<seat handle>) silently filed mail into
# a project nobody would ever read from, reported as sent. ═══


async def test_send_refuses_a_to_project_nobody_has_ever_mounted_under(
    actions: Actions,
) -> None:
    with pytest.raises(ValueError, match="no such project: 'neverland'"):
        await send_message(actions.pool, from_agent="agent:x", from_project="osiris",
                           to_project="neverland", body="does anyone hear this?")
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM fleet_messages WHERE to_project='neverland'") == 0


async def test_send_still_broadcasts_to_a_project_someone_has_mounted_under(
    actions: Actions,
) -> None:
    """REGRESSION PROOF: the ordinary, working case — a project a real agent has actually
    mounted under — must be completely unaffected by the new check."""
    await _seed(actions.pool, "realproject")
    res = await send_message(actions.pool, from_agent="agent:x", from_project="osiris",
                             to_project="realproject", body="a legitimate broadcast")
    assert res["to"] == "realproject"
    assert await unread_count(actions.pool, "realproject", reader_agent=R) == 1


async def test_send_to_operator_is_unaffected_by_the_project_existence_check(
    actions: Actions,
) -> None:
    """OPERATOR_ADDR is the one carved-out sentinel — the human's desk is never a project
    anyone mounts under, and must never be refused as one."""
    res = await send_message(actions.pool, from_agent="agent:x", from_project="osiris",
                             to_project=OPERATOR_ADDR, body="a brief for the desk")
    assert res["to"] == OPERATOR_ADDR


async def test_send_refusal_hints_to_agent_when_the_string_is_a_live_seat_name(
    actions: Actions,
) -> None:
    """THE COURTESY, NEVER THE SUBSTITUTION: a `to=` string that happens to match a live
    seat/agent name gets a "did you mean to_agent=?" hint appended — but the message is
    STILL refused, never silently redirected. Explicit addressing, never guess, the same
    law resolve_thread/charter_for already run on."""
    from src.orchestrator.seats import bind_holder, ensure_seat

    seat = await ensure_seat(actions, house="osiris", handle="Sekhmet", source="test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:sekhmet0001")

    with pytest.raises(ValueError, match=r"did you mean to_agent='Sekhmet' instead of to=\?"):
        await send_message(actions.pool, from_agent="agent:x", from_project="osiris",
                           to_project="Sekhmet", body="meant this as a DM")
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM fleet_messages WHERE to_project ILIKE 'sekhmet'") == 0


async def test_send_refusal_carries_no_hint_when_the_string_matches_nothing_at_all(
    actions: Actions,
) -> None:
    """The hint is additive, never assumed: a string that is neither a known project NOR a
    resolvable seat/agent name refuses with the plain message, no dangling suggestion."""
    with pytest.raises(ValueError) as exc_info:
        await send_message(actions.pool, from_agent="agent:x", from_project="osiris",
                           to_project="totally-unrecognizable-string", body="hello?")
    assert "did you mean" not in str(exc_info.value)


async def test_send_refusal_suppresses_the_hint_when_it_would_lead_to_the_same_refusal(
    actions: Actions,
) -> None:
    """task #142 punch-list item 3 (Thoth's dispatch DM 4097): a `to=` string matching a
    name whose unique seat has only an ineligible holder must NOT get the "did you mean
    to_agent=?" courtesy — following that hint would hit the exact same undeliverable
    refusal this guard already enforces at the to_agent= branch. Pointing at a door that
    won't open is worse than no hint at all."""
    from src.orchestrator.seats import bind_holder, ensure_seat

    seat = await ensure_seat(actions, house="osiris", handle="GhostProject", source="test")
    holder = await actions.create_or_find_object(
        "Agent", "agent:ghostproj0001", "agent:ghostproj0001")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:ghostproj0001")
    await actions.assert_property(holder, "retired", "true", "agent:ghostproj0001",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")

    with pytest.raises(ValueError) as exc_info:
        await send_message(actions.pool, from_agent="agent:x", from_project="osiris",
                           to_project="GhostProject", body="meant this as a DM")
    assert "did you mean" not in str(exc_info.value)
    assert "no such project" in str(exc_info.value)


# ═══ THE READ-SIDE PRIOR-ART HOP (obligation a6198075) — send()'s own dispatch-time
# reuse of record_decision's write-time prior-art search: a DM or an 'ask'-graded
# broadcast runs the same check, on BOTH the sender's receipt and the reader's inbox. ═══


class _Ctx:
    class request_context:  # noqa: N801
        request = None
        session = object()


async def test_send_dm_surfaces_prior_art_on_senders_receipt_and_readers_inbox(
    actions: Actions,
) -> None:
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    standing = await record_decision(
        actions,
        "RULING — the kaboomquartz throttle must never fire below the cgroup's own "
        "memory.high watermark, per the operator's direct instruction 2026-08-01.",
        kind="ruling", repo="osiris", source="agent:standing-law")

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:sender-1", session="sender001", project="osiris", model=None,
        cwd=None)
    try:
        out = await srv.send(
            "dispatching a build for the kaboomquartz throttle memory.high watermark "
            "issue", to_agent="agent:worker-1", want_prior_art=True, ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)

    prior_ids = {p["id"] for p in out.get("prior_art", [])}
    assert str(standing)[:8] in prior_ids, out
    assert "prior_art_flag" in out

    row = await actions.pool.fetchrow(
        "SELECT prior_art FROM fleet_messages WHERE id=$1", out["sent"])
    assert row is not None and row["prior_art"]
    persisted_ids = {p["id"] for p in row["prior_art"]}
    assert str(standing)[:8] in persisted_ids

    inbox_msgs = await read_inbox(actions.pool, "osiris", reader_agent="agent:worker-1")
    delivered = next(m for m in inbox_msgs if m["id"] == out["sent"])
    assert "prior_art" in delivered
    assert str(standing)[:8] in {p["id"] for p in delivered["prior_art"]}


async def test_send_skips_prior_art_surfacing_on_an_ungraded_broadcast(
    actions: Actions,
) -> None:
    """The hop fires on a DM or an 'ask'-graded broadcast only — an ordinary, ungraded
    broadcast is not the shape it exists to protect (nobody is being asked to act) and
    must not pay the search cost."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    await record_decision(
        actions, "RULING — the flibbergast register resets on every warm boot, verbatim.",
        kind="ruling", repo="osiris", source="agent:standing-law2")
    await _seed(actions.pool, "osiris")

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:sender-2", session="sender002", project="osiris", model=None,
        cwd=None)
    try:
        out = await srv.send("the flibbergast register resets on every warm boot",
                             to="osiris", ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)

    assert "prior_art" not in out
    row = await actions.pool.fetchrow(
        "SELECT prior_art FROM fleet_messages WHERE id=$1", out["sent"])
    assert row is not None and row["prior_art"] is None


async def test_send_surfaces_prior_art_on_an_ask_graded_broadcast(
    actions: Actions,
) -> None:
    """The second trigger shape: not a DM, but graded 'ask' — a coordinator dispatching a
    task to a whole project, not one agent, must get the same nudge."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    standing = await record_decision(
        actions,
        "RULING — the zylophage cache must be invalidated on every merged_into write, "
        "per the operator's direct 2026-08-05 instruction.",
        kind="ruling", repo="osiris", source="agent:standing-law3")
    await _seed(actions.pool, "osiris")

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:sender-3", session="sender003", project="osiris", model=None,
        cwd=None)
    try:
        out = await srv.send(
            "please check the zylophage cache invalidation on every merged_into write",
            to="osiris", grade="ask", want_prior_art=True, ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)

    prior_ids = {p["id"] for p in out.get("prior_art", [])}
    assert str(standing)[:8] in prior_ids, out


# ═══ THE RECEIPT INVARIANT (ruling 7d6815bb) — a contract test over every DM receipt shape
# the suite can produce: `seat` always matches the DELIVERING HEAD's own claimed handle,
# `listener.live` always reads the head's liveness, and a redirect is EXPLICIT whenever the
# addressed id and the head diverge. Ra XXXVI's specimen (thread e93c2470) is the acceptance.

async def _dm_receipt_contract(
    actions: Actions, to_agent: str, *, from_agent: str = "agent:auditor",
) -> dict:
    """One real send_message call, checked against the SAME independent read the invariant
    promises never to diverge from — agent_seat(lineage_head) computed HERE, fresh, never
    trusted from the receipt's own fields, so a passing assert means the receipt agrees
    with the graph, not merely with itself."""
    from src.orchestrator.agents import agent_seat

    out = await send_message(actions.pool, from_agent=from_agent, from_project="alpha",
                             to_agent=to_agent, body="contract check")
    head = out.get("lineage_head")
    if head:
        assert out["seat"] == await agent_seat(actions.pool, head), out
    return out


async def test_contract_a_fresh_claimed_agent_receipt_matches_its_own_seat(
    actions: Actions,
) -> None:
    from src.orchestrator.agents import claim_name

    target = "agent:contract01"
    await actions.create_or_find_object("Agent", target, target)
    await claim_name(actions, target, "Contractor", source=target)
    out = await _dm_receipt_contract(actions, target)
    assert out["lineage_head"] == target
    assert out["seat"] == "Contractor I"
    assert "redirect" not in out  # addressed == delivered: nothing to redirect


async def test_contract_a_superseded_ancestor_receipt_matches_the_head_not_the_address(
    actions: Actions,
) -> None:
    from src.orchestrator.agents import claim_name, mint_heir

    ancestor = "agent:contract02"
    a = await actions.create_or_find_object("Agent", ancestor, ancestor)
    await claim_name(actions, ancestor, "Contractee", source=ancestor)
    heir, _ = await mint_heir(actions, ancestor, a, because="test-succession", succession=None)
    out = await _dm_receipt_contract(actions, ancestor)
    assert out["lineage_head"] == heir
    assert out["seat"] == "Contractee II"        # the HEAD's seat, never the ancestor's "I"
    assert out["redirect"]["addressed"] == ancestor
    assert out["redirect"]["delivered"] == heir
    assert out["redirect"]["addressed_seat"] == "Contractee I"
    assert out["redirect"]["delivered_seat"] == out["seat"]


async def test_contract_listener_liveness_reads_the_head_even_when_only_the_head_is_mounted(
    actions: Actions,
) -> None:
    """The specimen's own shape: the ancestor id has NO mount row at all (it is retired,
    nothing to probe), the head has a real one — `listener.live` must read the head's
    pulse, never report the ancestor as dead-and-therefore-everyone-dead."""
    from src.orchestrator.agents import claim_name, mint_heir
    from src.orchestrator.mounts import save_mount

    ancestor = "agent:contract03"
    a = await actions.create_or_find_object("Agent", ancestor, ancestor)
    await claim_name(actions, ancestor, "Liveprobe", source=ancestor)
    heir, _ = await mint_heir(actions, ancestor, a, because="test-succession", succession=None)
    await save_mount(actions.pool, job_dir="/j/contract03-heir", agent_id=heir,
                     project="alpha", cwd="/w", model=None, session_key=None)
    out = await _dm_receipt_contract(actions, ancestor)
    assert out["lineage_head"] == heir


# ═══ THE STATIC CHECK — nothing in the mail-receipt surface builds a `seat` key from the
# addressed id directly; every assignment routes through `lineage` (the resolved head) or
# `gate_seat` (the require_seat gate's own, deliberately-distinct, addressed-id question).

def test_static_check_no_seat_field_is_built_from_the_bare_addressed_id() -> None:
    """A grep-shaped guard, not a type check: `agent_seat(pool, to_a)` assigned directly to
    a variable literally named `seat` is exactly the regression this thread exists to catch
    (it is what the bug WAS). `gate_seat` is the one sanctioned exception — a distinct
    question (does the addressed id itself hold a seat), never echoed as the receipt's own
    `seat` field. If this ever fires, read why: either a genuine new need for the addressed
    id's own seat (name it something other than `seat`), or the regression itself."""
    import re
    from pathlib import Path

    src = Path("src/orchestrator/mailbox.py").read_text()
    # every bare `seat = ` assignment (word-boundary anchored — NOT `gate_seat = ` or any
    # other suffix match) must cite `lineage` as its source, or be the guarded default
    # (`seat: str | None = None`) — never a raw `to_a`-derived call.
    assignments = re.findall(r"(?<![\w.])seat = (.+)$", src, re.MULTILINE)
    assert assignments, "no `seat = ` assignment found at all — did the code move?"
    for rhs in assignments:
        assert "to_a" not in rhs or "lineage" in rhs, (
            f"suspicious `seat = {rhs}` in mailbox.py — this is the e93c2470 regression "
            "shape: every seat assignment must be traceable to `lineage` (the resolved "
            "head), never a bare `to_a`-derived call. `gate_seat` (require_seat's own, "
            "deliberately addressed-id-derived variable) is unaffected by this check — it "
            "is never named `seat`.")


# ═══ THE GRAPH-EDGE BLOCK IS STRUCTURALLY GUARDED (Thoth DM 5493, ruling 7d6815bb) — five
# regressions from five unguarded raw create_or_find_object calls, found by five different
# people one at a time. The law now: every edge TARGET is existence-checked, never minted
# (only the Message object this function alone owns gets created fresh); a genuine graph-
# write failure is CONFESSED (_log.warning + `graphed: False`), never silently swallowed. ═══

async def test_broadcast_to_a_project_with_no_graph_object_links_nothing_and_mints_nothing(
    actions: Actions,
) -> None:
    """`_seed` only calls save_mount — no SoftwareProject graph object exists for
    'sibling-one'. The old code minted one as a side effect of the broadcast_to edge;
    the fix must skip the edge and mint nothing."""
    p = actions.pool
    await _seed(p, "sibling-one")
    await send_message(p, from_agent="agent:bcast1", from_project="sibling-two",
                       to_project="sibling-one", body="a broadcast to an ungraphed project")
    n = await p.fetchval(
        "SELECT count(*) FROM objects WHERE canonical='repo:sibling-one' "
        "AND type='SoftwareProject'")
    assert n == 0, "a mail broadcast must never mint a SoftwareProject as a side effect"


async def test_broadcast_to_a_project_with_a_real_graph_object_links_it(
    actions: Actions,
) -> None:
    """The positive case: when the SoftwareProject genuinely already exists, the
    broadcast_to edge DOES land."""
    p = actions.pool
    await _seed(p, "sibling-graphed")
    proj_oid = await actions.create_or_find_object(
        "SoftwareProject", "repo:sibling-graphed", "test")
    res = await send_message(p, from_agent="agent:bcast2", from_project="sibling-two",
                             to_project="sibling-graphed", body="a broadcast to a real project")
    msg_oid = await p.fetchval(
        "SELECT id FROM objects WHERE canonical=$1", f"message:{res['id']}")
    assert msg_oid is not None
    linked = await p.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='broadcast_to'",
        msg_oid, proj_oid)
    assert linked == 1


async def test_dm_to_an_unknown_agent_id_mints_no_agent_object(actions: Actions) -> None:
    """An `addressed_to` edge must never mint the addressee's Agent object either —
    same law as the sender's own existence-only fix, not a second shape."""
    p = actions.pool
    await send_message(p, from_agent="agent:dm-sender", from_project="osiris",
                       to_agent="agent:never-met-before-999", body="hello stranger")
    n = await p.fetchval(
        "SELECT count(*) FROM objects WHERE canonical='agent:never-met-before-999' "
        "AND type='Agent'")
    assert n == 0


async def test_reply_to_a_message_whose_own_graph_write_never_landed_mints_no_stub(
    actions: Actions,
) -> None:
    """A `reply_to` pointing at a real fleet_messages row whose Message object was
    never created (e.g. its own graph write failed) must not mint a stub Message just
    to hang a replies_to edge off of."""
    p = actions.pool
    await _seed(p, "osiris")
    parent_id = await p.fetchval(
        "INSERT INTO fleet_messages (from_agent, from_project, to_project, body) "
        "VALUES ('agent:ghost-parent', 'osiris', 'osiris', 'never graphed') RETURNING id")
    res = await send_message(p, from_agent="agent:replier", from_project="osiris",
                             to_project="osiris", reply_to=parent_id, body="replying anyway")
    n = await p.fetchval(
        "SELECT count(*) FROM objects WHERE canonical=$1", f"message:{parent_id}")
    assert n == 0, "no stub Message should be minted for the missing parent"
    msg_oid = await p.fetchval(
        "SELECT id FROM objects WHERE canonical=$1", f"message:{res['id']}")
    n_edges = await p.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND type='replies_to'", msg_oid)
    assert n_edges == 0


async def test_no_graph_thread_object_is_ever_minted_from_a_mailbox_reply_chain(
    actions: Actions,
) -> None:
    """`thread` in send_message is fleet_messages' own integer reply-chain grouping —
    a different thing entirely from the graph's `Thread` object type (open_thread()'s
    owner/kind/status-bearing obligation). No `in_thread` edge, no minted Thread."""
    p = actions.pool
    await _seed(p, "osiris")
    first = await send_message(p, from_agent="agent:threadA", from_project="osiris",
                               to_project="osiris", body="opening a reply chain")
    await send_message(p, from_agent="agent:threadB", from_project="osiris",
                       to_project="osiris", reply_to=first["id"], body="replying in-chain")
    n = await p.fetchval(
        "SELECT count(*) FROM objects WHERE canonical LIKE 'thread:%' AND type='Thread'")
    assert n == 0
    n_edges = await p.fetchval("SELECT count(*) FROM links WHERE type='in_thread'")
    assert n_edges == 0


async def test_a_genuine_graph_write_failure_is_confessed_not_swallowed(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Force the graph-edge block to raise and confirm: (1) the relational send still
    succeeds (`id` present, no exception escapes send_message), (2) the receipt says
    `graphed: False` rather than staying silent, (3) a warning is logged — the swallow
    this whole dispatch exists to remove."""
    import logging

    from src.actions.core import Actions as RealActions

    async def _boom(self: RealActions, *a: object, **kw: object) -> None:
        raise RuntimeError("simulated graph outage")

    monkeypatch.setattr(RealActions, "create_or_find_object", _boom)

    p = actions.pool
    await _seed(p, "osiris")
    caplog_records: list[str] = []
    handler = logging.Handler()
    handler.emit = lambda record: caplog_records.append(record.getMessage())  # type: ignore[method-assign]
    logger = logging.getLogger("osiris.mailbox")
    logger.addHandler(handler)
    try:
        res = await send_message(p, from_agent="agent:willfail", from_project="osiris",
                                 to_project="osiris", body="this send's graph write will blow up")
    finally:
        logger.removeHandler(handler)

    assert res["id"] > 0
    assert res["graphed"] is False
    n = await p.fetchval("SELECT count(*) FROM fleet_messages WHERE id=$1", res["id"])
    assert n == 1, "the relational row must land regardless of the graph write's fate"
    assert any("simulated graph outage" in r for r in caplog_records), (
        "the failure must be logged, not silently swallowed"
    )


async def test_send_tool_surfaces_graphed_false_on_the_receipt(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The MCP send() wrapper must forward the honest-failure signal to the caller,
    not just bury it inside mailbox.py's own internal return value."""
    from src import mcp_server as srv
    from src.actions.core import Actions as RealActions
    from src.orchestrator.agents import AgentIdentity

    async def _boom(self: RealActions, *a: object, **kw: object) -> None:
        raise RuntimeError("simulated graph outage")

    monkeypatch.setattr(RealActions, "create_or_find_object", _boom)
    await _seed(actions.pool, "osiris")

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:willfail2", session="willfail2", project="osiris", model=None,
        cwd=None)
    try:
        out = await srv.send("this will also blow up", to="osiris", ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)

    assert out["graphed"] is False
    assert "note" in out
