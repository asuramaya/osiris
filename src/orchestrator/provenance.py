"""Provenance tracking, part one: read-set stamping at the MCP boundary. The
governing principle is that provenance should be established by which channel
data moved through, not by comparing text.

credence.py's independence oracle is purely structural today: it relies on
spawned_by ancestry plus backed_by_observation. Two agents with no ancestry
relation who both merely read the same upstream fact and re-asserted it still
count as fully independent corroboration, which is the citogenesis pattern
that the spawned_by clamp was built to catch at one level. This module is the
second, orthogonal piece: a durable log of what each session actually read
through the MCP layer (`stamp_read`), and the edges minted from what a
session writes back to what it read before writing it
(`stamp_possible_upstream`). These use `links.type='possible_upstream'`, an
existing link shape (type/source_id/properties, both ends already
foreign-keyed to objects), so no schema change to `links` itself was needed.

This is deliberately overbroad: it is meant to be used only to withhold a
claim of independence, never to grant one. Every object a session's write
touches gets edges to that session's whole preceding read-set, regardless of
content. A text-similarity approach to narrow this down was considered and
rejected in favor of this coarser approach, so a possible_upstream edge means
only "could plausibly trace here," not a claim that the write actually used
that read. Its absence proves nothing about independence either (a session
may have read something through an untracked channel, or before this module
existed); its presence only ever narrows what credence_props is willing to
call distinct corroboration.

The read-set log is durable rather than kept in process memory: the MCP
server restarts multiple times an hour in observed fleet practice, and an
in-memory read-set would lose most of its coverage to routine restarts,
which would defeat the point rather than merely narrow it.
"""
from __future__ import annotations

import uuid
from datetime import UTC, datetime

import asyncpg

from src.actions.core import Actions

# Channels this module tracks, matching the caller's own dispatch list. Kept
# as a real set (not just documentation) so a caller passing a typo'd channel
# name fails loudly rather than silently mislabeling every edge it mints.
READ_DOORS = frozenset({
    "inbox-lease", "inbox-peek", "recall", "dossier", "search", "graph_search",
    "read_citation", "ingest_reference",
})


async def stamp_read(
    pool: asyncpg.Pool, *, agent_id: str, door: str, object_id: uuid.UUID | None,
    read_at: datetime | None = None,
) -> None:
    """Log one read-set entry for `agent_id`. `object_id` is None when the read has no
    graph identity to anchor on: a channel result that resolved to nothing, or, for
    mail, a message whose own Message object never landed, or whose sender is the
    operator. Never minting a possible_upstream edge back to the operator's own words
    is enforced at the point of read, not by a later filter, so those calls simply
    never reach this function with a real id. A None object_id is a silent no-op,
    never an error: the caller still gets its result back either way; only the
    provenance side effect is skipped."""
    if object_id is None:
        return
    await pool.execute(
        "INSERT INTO session_reads (agent_id, door, object_id, read_at) VALUES ($1,$2,$3,$4)",
        agent_id, door, object_id, read_at or datetime.now(UTC))


async def message_object_id(pool: asyncpg.Pool, message_id: int, from_agent: str | None
                            ) -> uuid.UUID | None:
    """Resolves the existing Message object that mailbox.py's own send() path already
    mints for every message at send time (canonical `message:<id>`). This function only
    resolves that object, it never mints one, matching the same existence-checked
    principle that module holds itself to: sending mail must never be how an identity
    or a project first enters the graph, and reading mail must not be either. Returns
    None (no stamp, ever) when `from_agent` is the operator's own, matching the
    'no auto-cite, ever' principle used elsewhere for citations."""
    from src.orchestrator.seats import _OPERATOR_ACTORS

    if from_agent in _OPERATOR_ACTORS:
        return None
    row = await pool.fetchval(
        "SELECT id FROM objects WHERE type='Message' AND canonical=$1", f"message:{message_id}")
    return uuid.UUID(str(row)) if row is not None else None


async def unread_message_ids(
    pool: asyncpg.Pool, message_ids: list[int], *, agent_id: str,
) -> list[int]:
    """Enforces the rule that a message must actually have been read at startup:
    returns which of `message_ids` has `agent_id`, for this exact session generation
    only (never an ancestor's), never been returned in full through a real inbox()
    call (`inbox-lease` or `inbox-peek`, both already stamped above). mcp_server.py's
    inbox() calls this to refuse settling an id still in the returned list: a
    successor session started mid-flight begins with an empty read-set of its own, so
    acknowledging a message straight from a startup prompt's headline preview (which
    never calls inbox() and stamps nothing) is exactly what this catches. A genuine
    peek or lease satisfies it, whether from this call or an earlier one.

    This fails open, not closed, on what it cannot observe: a message whose own
    Message object never landed at send time, or one the operator sent (per
    `message_object_id`'s documented no-stamp-ever rule), returns no `object_id` to
    check session_reads against at all, so it is reported as read rather than
    flagged. That matches the fail-open posture every other reader of this table
    already holds itself to. This check gates what the graph can actually observe;
    it does not invent evidence it doesn't have."""
    if not message_ids:
        return []
    rows = await pool.fetch(
        "SELECT m.id, m.from_agent FROM fleet_messages m WHERE m.id = ANY($1::bigint[])",
        message_ids)
    unread: list[int] = []
    for r in rows:
        oid = await message_object_id(pool, r["id"], r["from_agent"])
        if oid is None:
            continue
        seen = await pool.fetchval(
            "SELECT 1 FROM session_reads WHERE agent_id=$1 AND object_id=$2 "
            "AND door IN ('inbox-lease','inbox-peek') LIMIT 1", agent_id, oid)
        if not seen:
            unread.append(int(r["id"]))
    return unread


async def stamp_possible_upstream(
    actions: Actions, *, written_object_id: uuid.UUID, source_id: str,
    observed_at: datetime | None = None,
) -> int:
    """After a fact write from `source_id` touches `written_object_id`, mint
    `possible_upstream` edges to every distinct object that source's own read-set
    contains from before this write, deduped by object (a thing read twice keeps its
    earliest channel/time, the more informative of the two). Skips an edge back to the
    written object itself, since a session that read the very object it is now writing
    to is not upstream of itself, and skips any edge that already exists for this
    exact (written_object_id, upstream, source_id) triple. The overbroad design
    accepts edges to everything read, but not the same edge minted again on every
    subsequent write in a long session. Returns the count of edges actually minted."""
    when = observed_at or datetime.now(UTC)
    rows = await actions.pool.fetch(
        "SELECT DISTINCT ON (object_id) object_id, door, read_at FROM session_reads "
        "WHERE agent_id=$1 AND read_at < $2 AND object_id != $3 "
        "ORDER BY object_id, read_at ASC", source_id, when, written_object_id)
    minted = 0
    for r in rows:
        upstream_id = r["object_id"]
        exists = await actions.pool.fetchval(
            "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 "
            "AND type='possible_upstream' AND source_id=$3",
            written_object_id, upstream_id, source_id)
        if exists:
            continue
        await actions.create_link(
            written_object_id, upstream_id, "possible_upstream", source_id, when, 1.0,
            properties={"door": r["door"], "read_at": r["read_at"].isoformat()})
        minted += 1
    return minted
