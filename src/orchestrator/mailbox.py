"""The fleet mailbox — directed coordination on top of the shared memory graph.

The fleet co-writes MEMORY; this is the directed CHANNEL: a message addressed to a PROJECT,
read by whoever works there (the project is stable; a session id is not). PULL, never push
(Osiris has no hands): the recipient reads its inbox when it next mounts/orients — the server
never taps a live agent on the shoulder, and an agent only perceives mail when it takes a turn.
Messages are ephemeral coordination, not durable memory, so they live in their own table,
never the event-sourced graph — for durable knowledge, agents use record_decision/open_thread.

Delivery is AT-LEAST-ONCE (decision 56f6a0d6 — the at-most-once mailbox silently swallowed
messages whenever a server bounce severed the response mid-flight): reading LEASES a message
(delivered_at) rather than consuming it; an ACK settles it (read_at). Settling happens three
ways — replying to it (send(reply_to=id): replying proves perception), an explicit
inbox(ack=[ids]), or never (it redelivers after the lease, marked `redelivered`). A severed
transport now costs a duplicate delivery, never a silent loss.

reply_to/thread_id are the request→reply lane: a reply auto-routes back to the ASKING agent's
project and joins its thread, so a question asked across the fleet comes back as a
conversation, not a loose message. `operator` is a reserved address — the human's desk: no
repo, never woken, surfaced via orient()/fleet_digest instead (the upward lane, membrane #6).
"""
from __future__ import annotations

from typing import Any

import asyncpg

# The human's desk. Not a repo (the trigger never wakes it); read via inbox(project='operator')
# from any session the operator drives, counted in orient() and fleet_digest.
OPERATOR_ADDR = "operator"

# A message is DELIVERABLE if never settled AND not currently under an unexpired lease.
_DELIVERABLE = ("read_at IS NULL AND (delivered_at IS NULL "
                "OR delivered_at < now() - make_interval(secs => ${lease}))")


def _norm(project: str) -> str:
    """A project address, with the optional `repo:` canonical prefix stripped."""
    return project.removeprefix("repo:").strip()


async def send_message(
    pool: asyncpg.Pool, *, from_agent: str, from_project: str | None,
    to_project: str | None = None, body: str, reply_to: int | None = None,
    dedup_window_secs: int = 600,
) -> dict[str, Any]:
    """Post a message. With `reply_to`, it routes back to the referenced message's sender
    project (explicit `to_project` overrides), joins its thread, and SETTLES the referenced
    message if it was addressed to the replier (replying proves perception — the natural ack).
    An identical (sender, recipient, body) within the dedup window returns the EXISTING id —
    so a client retry after a severed response can't double-post. Raises ValueError on an
    unknown reply_to or an unroutable message (no to_project and no reply source)."""
    ref = None
    if reply_to is not None:
        ref = await pool.fetchrow(
            "SELECT id, from_project, to_project, thread_id FROM fleet_messages WHERE id=$1",
            reply_to)
        if ref is None:
            raise ValueError(f"reply_to message {reply_to} does not exist")
    to = _norm(to_project) if to_project else (ref["from_project"] if ref else None)
    if not to:
        raise ValueError("no recipient: pass to=<project>, or reply_to a message whose sender "
                         "has a project")
    thread = (ref["thread_id"] or ref["id"]) if ref is not None else None
    if ref is not None and from_project and _norm(ref["to_project"]) == _norm(from_project):
        # the reply IS the ack — but only for mail that was addressed to the replier
        await pool.execute(
            "UPDATE fleet_messages SET read_at=COALESCE(read_at, now()) WHERE id=$1", ref["id"])
    dup = await pool.fetchrow(
        "SELECT id, thread_id FROM fleet_messages WHERE from_agent=$1 AND to_project=$2 "
        "AND md5(body)=md5($3) AND created_at > now() - make_interval(secs => $4) "
        "ORDER BY id DESC LIMIT 1",
        from_agent, to, body, dedup_window_secs)
    if dup is not None:
        return {"id": dup["id"], "to": to, "thread_id": dup["thread_id"], "dedup": True}
    mid = await pool.fetchval(
        "INSERT INTO fleet_messages (from_agent, from_project, to_project, body, reply_to, "
        "thread_id) VALUES ($1,$2,$3,$4,$5,$6) RETURNING id",
        from_agent, from_project, to, body, reply_to, thread)
    return {"id": mid, "to": to, "thread_id": thread, "dedup": False}


async def unread_count(pool: asyncpg.Pool, to_project: str, *, lease_secs: int = 900) -> int:
    """How many DELIVERABLE messages await this project — unsettled and not under a live
    lease. This is the number mount()/orient() surface and the trigger wakes on."""
    q = ("SELECT count(*) FROM fleet_messages WHERE to_project=$1 AND "
         + _DELIVERABLE.replace("${lease}", "$2"))
    return await pool.fetchval(q, _norm(to_project), lease_secs)  # type: ignore[no-any-return]


async def read_inbox(
    pool: asyncpg.Pool, to_project: str, *, mark_read: bool = True, limit: int = 50,
    lease_secs: int = 900, lessee: str | None = None,
) -> list[dict[str, Any]]:
    """A project's deliverable messages, oldest first. Reading LEASES them (delivered_at) —
    settle each by replying (send(reply_to=id)) or acking (ack_messages); an unsettled message
    redelivers after the lease, flagged `redelivered`. mark_read=False is a pure peek (no
    lease, nothing changes). `lessee` stamps WHO holds the lease — the owner's inbox shows it
    as in-flight instead of silence (msg-78 lesson). Each message carries its sender + thread,
    so provenance and conversation travel with it."""
    proj = _norm(to_project)
    q = ("SELECT id, from_agent, from_project, body, created_at, reply_to, thread_id, "
         "deliveries FROM fleet_messages WHERE to_project=$1 AND "
         + _DELIVERABLE.replace("${lease}", "$2") + " ORDER BY created_at LIMIT $3")
    rows = await pool.fetch(q, proj, lease_secs, limit)
    if mark_read and rows:
        await pool.execute(
            "UPDATE fleet_messages SET delivered_at=now(), deliveries=deliveries+1, "
            "leased_by=COALESCE($2, leased_by) WHERE id = ANY($1::bigint[])",
            [r["id"] for r in rows], lessee)
    return [
        {"id": r["id"], "from": r["from_agent"], "from_project": r["from_project"],
         "body": r["body"], "when": r["created_at"].isoformat(),
         "thread": r["thread_id"] or r["id"],
         **({"reply_to": r["reply_to"]} if r["reply_to"] is not None else {}),
         **({"redelivered": True} if r["deliveries"] > 0 else {})}
        for r in rows
    ]


async def in_flight(
    pool: asyncpg.Pool, to_project: str, *, lease_secs: int = 900
) -> list[dict[str, Any]]:
    """The owner's view of mail currently LEASED by someone (unsettled, live lease): who holds
    it and for how long. 'mail 0' must never silently mean 'a twin is answering your thread
    right now' — the in-flight block says so (msg-78 lesson)."""
    rows = await pool.fetch(
        "SELECT id, from_project, leased_by, thread_id, "
        " extract(epoch FROM (now() - delivered_at)) AS held_secs "
        "FROM fleet_messages WHERE to_project=$1 AND read_at IS NULL "
        "AND delivered_at IS NOT NULL AND delivered_at >= now() - make_interval(secs => $2) "
        "ORDER BY delivered_at", _norm(to_project), lease_secs)
    return [
        {"id": r["id"], "from_project": r["from_project"],
         "leased_by": r["leased_by"] or "unknown",
         "thread": r["thread_id"] or r["id"],
         "held_for_secs": int(r["held_secs"] or 0)}
        for r in rows
    ]


async def ack_messages(pool: asyncpg.Pool, to_project: str, ids: list[int]) -> int:
    """Settle messages by id — scoped to the acking project (you can only settle YOUR mail).
    Returns how many actually settled (already-settled ids don't count twice)."""
    rows = await pool.fetch(
        "UPDATE fleet_messages SET read_at=now() "
        "WHERE id = ANY($1::bigint[]) AND to_project=$2 AND read_at IS NULL RETURNING id",
        ids, _norm(to_project))
    return len(rows)
