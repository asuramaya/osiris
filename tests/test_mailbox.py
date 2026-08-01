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


async def test_ack_settles_for_good(actions: Actions) -> None:
    p = actions.pool
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


async def test_reply_to_unknown_message_is_an_error(actions: Actions) -> None:
    with pytest.raises(ValueError, match="does not exist"):
        await send_message(actions.pool, from_agent="agent:x", from_project="a",
                           body="into the void", reply_to=999999)
    with pytest.raises(ValueError, match="no recipient"):
        await send_message(actions.pool, from_agent="agent:x", from_project="a", body="lost")


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
    a_delivery_target): `seat` alone would look fine (the ancestor still carries its own old
    handle); `lineage_head` is what actually exposes the staleness."""
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
    assert dm["seat"] == "Ptah I"              # the ancestor still carries its own old handle
    assert dm["lineage_head"] == heir          # ...but the echo reveals it is NOT current
    assert dm["lineage_head"] != dm["to_agent"]


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
    ask = await send_message(p, from_agent="agent:0abe4003", from_project="oldhouse",
                             to_project="farhouse", body="anyone: check the gauge")
    rep = await send_message(p, from_agent="agent:fa40b001", from_project="farhouse",
                             reply_to=ask["id"], body="gauge checked")
    assert rep["to"] == "oldhouse" and rep["to_agent"] is None


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
    assert out["dispatch"]["mode"] == "pull-only"
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
