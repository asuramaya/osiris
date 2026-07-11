"""The fleet mailbox — group-chat + DM over the shared memory graph (thread 6fa9791d).

The fleet co-writes MEMORY; this is the directed CHANNEL. Two shapes:
  * a BROADCAST to a project (to_agent NULL) — the group chat: every agent working that project
    sees it and settles it INDEPENDENTLY (per-recipient read state), so two co-located agents
    (handlingtheloop ux + engine, one dir) both see the conversation;
  * a DM to a specific agent (to_agent set) — only that agent sees it.

PULL, never push (Osiris has no hands): a recipient reads its inbox when it next mounts/orients.
Messages are ephemeral coordination, not durable memory, so they live in their own table, never
the event-sourced graph — for durable knowledge, agents use record_decision/open_thread.

Delivery is AT-LEAST-ONCE per recipient (decision 56f6a0d6): reading LEASES a message FOR THAT
READER (a row in message_recipients) rather than consuming it; an ACK settles it. Settling three
ways — replying (send(reply_to=id): replying proves perception), an explicit inbox(ack=[ids]), or
never (it redelivers after the lease, flagged `redelivered`). Each reader's lease/settle is its
own; the message itself is never consumed, so a broadcast survives being read by one agent.

`operator` is a reserved reader — the human's desk (no repo, never woken), just another agent_id
in message_recipients. reply_to/thread_id are the request→reply lane: a reply to a DM routes back
to the sender as a DM; a reply to a broadcast routes to the thread's project.
"""
from __future__ import annotations

from typing import Any

import asyncpg

# The human's desk. Not a repo (the trigger never wakes it); read via inbox(project='operator').
OPERATOR_ADDR = "operator"

# THE LIVE-HOLDER EXTENSION (grievance survey 2026-07-11, two witnesses — Anubis VIII
# msg 236, Soundwave msg 244: a message redelivered while the analysis it minted was still
# computing). A lease held by a reader whose mount is LIVE stretches to the hold-grace hour
# — the holder is demonstrably present, at-least-once needs no duplicate yet. A holder gone
# stale (died, idled out) redelivers at the plain lease exactly as before. MUST match the
# stop hook's STOP_GRACE_SECS (scripts/osiris_stophook.py): if the two windows disagree,
# the hook nags about mail the inbox refuses to show.
_HOLD_GRACE_SECS = 3600

# Deliverable TO A GIVEN READER: addressed to it (a DM to_agent=me, or a broadcast to my project),
# not settled by me, and not under MY live lease (live = my mount breathes; see the
# live-holder extension above). `r` is my message_recipients row (LEFT JOINed).
# `m.read_at IS NULL` honors the LEGACY per-message settle: messages settled under the old
# single-reader model (pre-0021) carry fleet_messages.read_at and are globally suppressed so
# history doesn't resurface; new messages never set it (per-recipient state only).
_DELIVERABLE_TO_READER = (
    "((m.to_agent = $agent) OR (m.to_project = $project AND m.to_agent IS NULL)) "
    "AND m.read_at IS NULL "
    "AND r.read_at IS NULL "
    "AND (r.delivered_at IS NULL "
    "     OR r.delivered_at < now() - make_interval(secs => $grace) "
    "     OR (r.delivered_at < now() - make_interval(secs => $lease) "
    "         AND NOT EXISTS (SELECT 1 FROM agent_mounts lm WHERE lm.agent_id = $agent "
    "             AND lm.last_seen > now() - make_interval(secs => $lease))))"
)


def _norm(project: str) -> str:
    """A project address, with the optional `repo:` canonical prefix stripped."""
    return project.removeprefix("repo:").strip()


async def settle_history_at_join(
    pool: asyncpg.Pool, project: str | None, agent_id: str
) -> int:
    """A JOINER inherits the project's collective settle-state (the zombie-count fix,
    2026-07-09: a wake-minted osiris tab counted 5 broadcasts its sibling had already
    settled). Joining a group chat does not make the room's handled history your unread:
    any broadcast some OTHER reader already settled is stamped read for the newcomer at
    join. Broadcasts NOBODY settled stay deliverable — mail-at-birth still greets a fresh
    session and the wake pipeline still finds its cause. Live members are untouched: their
    per-reader unread keeps working message by message. Returns rows stamped."""
    if not project:
        return 0
    res = await pool.execute(
        "INSERT INTO message_recipients (message_id, agent_id, delivered_at, read_at) "
        "SELECT m.id, $2, now(), now() FROM fleet_messages m "
        "WHERE m.to_project = $1 AND m.to_agent IS NULL "
        "AND (m.read_at IS NOT NULL OR EXISTS (SELECT 1 FROM message_recipients r2 "
        "  WHERE r2.message_id = m.id AND r2.read_at IS NOT NULL)) "
        "ON CONFLICT (message_id, agent_id) DO NOTHING", _norm(project), agent_id)
    try:
        return int(res.split()[-1])
    except (ValueError, IndexError):
        return 0


async def send_message(
    pool: asyncpg.Pool, *, from_agent: str, from_project: str | None,
    to_project: str | None = None, to_agent: str | None = None, body: str,
    reply_to: int | None = None, dedup_window_secs: int = 600,
) -> dict[str, Any]:
    """Post a BROADCAST (to_project) or a DM (to_agent). With `reply_to` and no explicit address,
    it routes by channel: a reply to a DM goes back to that sender as a DM; a reply to a broadcast
    routes to the referenced message's project (replying to YOUR OWN broadcast routes ONWARD to
    its recipient — the desk supersession lane), joining the thread and settling the referenced
    message for the replier (replying proves perception). An identical (sender, recipient, body)
    within the dedup window returns the EXISTING id. Raises ValueError on an unknown reply_to or
    an unroutable message."""
    ref = None
    if reply_to is not None:
        ref = await pool.fetchrow(
            "SELECT id, from_agent, from_project, to_project, to_agent, thread_id "
            "FROM fleet_messages WHERE id=$1", reply_to)
        if ref is None:
            raise ValueError(f"reply_to message {reply_to} does not exist")
    if to_agent and not to_agent.startswith("agent:"):
        # a DM addressed by HUMAN NAME (a seat): resolve to the current live holder's id at
        # send time (phase 2, ruling 1e02e069). Snapshot semantics — the rare in-flight edge
        # (the seat succeeds between send and read) is documented, not handled.
        from src.actions.core import Actions
        from src.orchestrator.agents import resolve_handle
        holder = await resolve_handle(Actions(pool), to_agent)
        if holder is None:
            raise ValueError(f"no agent named '{to_agent}' — check the name or DM by agent id")
        to_agent = holder
    if to_agent or to_project:  # explicit addressing wins
        to_a = to_agent
        to_p = _norm(to_project) if to_project else None
    elif ref is not None and ref["to_agent"] == from_agent:
        to_a, to_p = ref["from_agent"], ref["from_project"]  # a DM to me → DM back to its sender
    elif ref is not None:  # a broadcast/own message → project routing (supersession lane)
        own = from_project and _norm(ref["from_project"] or "") == _norm(from_project)
        to_a = None
        to_p = _norm(ref["to_project"] or "") if own else _norm(ref["from_project"] or "")
    else:
        to_a = to_p = None
    if not to_a and not to_p:
        raise ValueError("no recipient: pass to=<project>, to_agent=<agent>, or reply_to a "
                         "message whose sender is addressable")
    thread = (ref["thread_id"] or ref["id"]) if ref is not None else None
    # the reply IS the ack — settle the referenced message for the replier, if it was addressed
    # to them (a DM to me, or a broadcast to my project)
    if ref is not None and (
        ref["to_agent"] == from_agent
        or (ref["to_agent"] is None and from_project
            and _norm(ref["to_project"] or "") == _norm(from_project))
    ):
        await pool.execute(
            "INSERT INTO message_recipients (message_id, agent_id, read_at) VALUES ($1,$2,now()) "
            "ON CONFLICT (message_id, agent_id) DO UPDATE SET read_at=COALESCE("
            "message_recipients.read_at, now())", ref["id"], from_agent)
    dup = await pool.fetchrow(
        "SELECT id, thread_id FROM fleet_messages WHERE from_agent=$1 "
        "AND to_project IS NOT DISTINCT FROM $2 AND to_agent IS NOT DISTINCT FROM $3 "
        "AND md5(body)=md5($4) AND created_at > now() - make_interval(secs => $5) "
        "ORDER BY id DESC LIMIT 1", from_agent, to_p, to_a, body, dedup_window_secs)
    if dup is not None:
        return {"id": dup["id"], "to": to_p, "to_agent": to_a,
                "thread_id": dup["thread_id"], "dedup": True}
    mid = await pool.fetchval(
        "INSERT INTO fleet_messages (from_agent, from_project, to_project, to_agent, body, "
        "reply_to, thread_id) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id",
        from_agent, from_project, to_p, to_a, body, reply_to, thread)
    return {"id": mid, "to": to_p, "to_agent": to_a, "thread_id": thread, "dedup": False}


async def unread_count(
    pool: asyncpg.Pool, reader_project: str, *, reader_agent: str, lease_secs: int = 900
) -> int:
    """How many DELIVERABLE messages await this reader — broadcasts to its project + DMs to it,
    unsettled and not under its own live lease. The number mount()/orient() surface."""
    q = ("SELECT count(*) FROM fleet_messages m "
         "LEFT JOIN message_recipients r ON r.message_id=m.id AND r.agent_id=$agent "
         "WHERE " + _DELIVERABLE_TO_READER)
    q = (q.replace("$agent", "$1").replace("$project", "$2").replace("$lease", "$3")
         .replace("$grace", "$4"))
    return await pool.fetchval(  # type: ignore[no-any-return]
        q, reader_agent, _norm(reader_project), lease_secs, _HOLD_GRACE_SECS)


async def read_inbox(
    pool: asyncpg.Pool, reader_project: str, *, reader_agent: str, mark_read: bool = True,
    limit: int = 50, lease_secs: int = 900,
) -> list[dict[str, Any]]:
    """This reader's deliverable messages, oldest first (broadcasts to its project + DMs to it).
    Reading LEASES them FOR THIS READER (a message_recipients row) — settle each by replying or
    acking; an unsettled message redelivers after the lease, flagged `redelivered`. mark_read=
    False is a pure peek. A broadcast read by one agent stays visible to the others — each has
    its own lease/settle."""
    proj = _norm(reader_project)
    q = ("SELECT m.id, m.from_agent, m.from_project, m.to_agent, m.body, m.created_at, "
         "m.reply_to, m.thread_id, COALESCE(r.deliveries,0) AS deliveries "
         "FROM fleet_messages m "
         "LEFT JOIN message_recipients r ON r.message_id=m.id AND r.agent_id=$agent "
         "WHERE " + _DELIVERABLE_TO_READER + " ORDER BY m.created_at LIMIT $limit")
    q = (q.replace("$agent", "$1").replace("$project", "$2")
         .replace("$lease", "$3").replace("$grace", "$4").replace("$limit", "$5"))
    rows = await pool.fetch(q, reader_agent, proj, lease_secs, _HOLD_GRACE_SECS, limit)
    if mark_read and rows:
        await pool.executemany(
            "INSERT INTO message_recipients (message_id, agent_id, delivered_at, deliveries) "
            "VALUES ($1,$2,now(),1) ON CONFLICT (message_id, agent_id) DO UPDATE SET "
            "delivered_at=now(), deliveries=message_recipients.deliveries+1",
            [(r["id"], reader_agent) for r in rows])
    return [
        {"id": r["id"], "from": r["from_agent"], "from_project": r["from_project"],
         "body": r["body"], "when": r["created_at"].isoformat(),
         "thread": r["thread_id"] or r["id"],
         **({"dm": True} if r["to_agent"] is not None else {}),
         **({"reply_to": r["reply_to"]} if r["reply_to"] is not None else {}),
         **({"redelivered": True} if r["deliveries"] > 0 else {})}
        for r in rows
    ]


async def in_flight(
    pool: asyncpg.Pool, reader_project: str, *, reader_agent: str, lease_secs: int = 900
) -> list[dict[str, Any]]:
    """The group's in-flight view: shared BROADCAST mail to this reader's project that ANOTHER
    agent currently holds under a live lease (unsettled). 'mail 0' must never silently mean 'a
    sibling is answering the group thread right now' (msg-78 lesson)."""
    rows = await pool.fetch(
        "SELECT m.id, m.from_project, r.agent_id AS leased_by, m.thread_id, "
        " extract(epoch FROM (now() - r.delivered_at)) AS held_secs "
        "FROM fleet_messages m JOIN message_recipients r ON r.message_id=m.id "
        "WHERE m.to_project=$1 AND m.to_agent IS NULL AND r.agent_id <> $2 "
        "AND r.read_at IS NULL AND r.delivered_at IS NOT NULL "
        "AND r.delivered_at >= now() - make_interval(secs => $3) "
        "ORDER BY r.delivered_at", _norm(reader_project), reader_agent, lease_secs)
    return [
        {"id": r["id"], "from_project": r["from_project"], "leased_by": r["leased_by"],
         "thread": r["thread_id"] or r["id"], "held_for_secs": int(r["held_secs"] or 0)}
        for r in rows
    ]


async def ack_messages(
    pool: asyncpg.Pool, reader_project: str, ids: list[int], *, reader_agent: str
) -> int:
    """Settle messages FOR THIS READER — only ones addressed to it (a DM to it, or a broadcast to
    its project). Returns how many newly settled (idempotent). Another reader acking the same
    broadcast settles only ITS own copy."""
    rows = await pool.fetch(
        "INSERT INTO message_recipients (message_id, agent_id, read_at) "
        "SELECT m.id, $3, now() FROM fleet_messages m "
        "WHERE m.id = ANY($1::bigint[]) "
        "  AND ((m.to_agent = $3) OR (m.to_project = $2 AND m.to_agent IS NULL)) "
        "ON CONFLICT (message_id, agent_id) DO UPDATE SET read_at=COALESCE("
        "message_recipients.read_at, now()) "
        "WHERE message_recipients.read_at IS NULL RETURNING message_id",
        ids, _norm(reader_project), reader_agent)
    return len(rows)


async def project_deliverable_count(
    pool: asyncpg.Pool, project: str, *, lease_secs: int = 900
) -> int:
    """For the wake dispatch: messages to this project (broadcasts + DMs to its agents) that NO
    intended recipient has settled yet. Once anyone reads a broadcast the wake stops, but the
    other agents still see it in their own inboxes — the wake ensures SOMEONE looks, without
    re-firing per sibling. (Agent-precise waking is a later phase; this is the safe project
    signal.)"""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT count(*) FROM fleet_messages m WHERE "
        "(m.to_project=$1 AND m.to_agent IS NULL) AND m.read_at IS NULL "
        "AND NOT EXISTS (SELECT 1 FROM message_recipients r WHERE r.message_id=m.id "
        "  AND r.read_at IS NOT NULL) "
        "AND (NOT EXISTS (SELECT 1 FROM message_recipients r WHERE r.message_id=m.id "
        "  AND r.delivered_at >= now() - make_interval(secs => $2))) "
        # the live-holder extension: never WAKE a sibling for a message a live mind is
        # already holding (grievance msg 244: duplicate reads while the grid computed)
        "AND NOT EXISTS (SELECT 1 FROM message_recipients r "
        "  JOIN agent_mounts lm ON lm.agent_id = r.agent_id "
        "  WHERE r.message_id = m.id "
        "  AND r.delivered_at >= now() - make_interval(secs => $3) "
        "  AND lm.last_seen > now() - make_interval(secs => $2))",
        _norm(project), lease_secs, _HOLD_GRACE_SECS)
