"""PROVENANCE PIECE 1, READ-SET STAMPING AT THE DOOR (thread da545039f2ba, ruling
bb3e4422 "provenance by channel, not by text" — the operator's own words: "build it —
everything goes in over a channel that the graph sees; nothing really escapes the
graph, as it is designed").

credence.py's independence oracle (its own docstring: "the SAME tree that bounds
delegation is, read upward, the INDEPENDENCE ORACLE") is purely STRUCTURAL today —
spawned_by ancestry plus backed_by_observation. Two agents with no ancestry relation
who both merely READ the same upstream fact and re-asserted it still count as fully
independent corroboration, the exact citogenesis shape the spawned_by clamp was built
to catch one level down. This module is the second, orthogonal leg: a durable log of
what each session actually read through the MCP door (`stamp_read`), and the edges
minted from what a session writes back to what it read before writing it
(`stamp_possible_upstream`) — `links.type='possible_upstream'`, an EXISTING link shape
(type/source_id/properties, both ends already FK'd to objects), no schema change to
`links` itself.

OVERBROAD BY DESIGN (the ruling's own words: "used only to WITHHOLD independence,
never to grant it"): every object a session's write touches gets edges to that
session's WHOLE preceding read-set, content-blind — the operator explicitly rejected
a text-similarity oracle in favor of this, so a possible_upstream edge is a coarse
"could plausibly trace here", not a claim the write actually used that read. Its
ABSENCE proves nothing about independence either (a session may have read something
through an untracked door, or before this module existed); its PRESENCE only ever
narrows what credence_props is willing to call distinct corroboration.

DURABLE, not in-process memory: the MCP server restarts multiple times an hour in
observed fleet practice, and an in-memory read-set would lose most of its coverage to
routine restarts — that would defeat the point rather than merely narrow it.
"""
from __future__ import annotations

import uuid
from datetime import UTC, datetime

import asyncpg

from src.actions.core import Actions

# doors this module tracks, named exactly as Thoth's own dispatch (DM 10366) lists
# them — kept as a real set (not just documentation) so a caller passing a typo'd
# door name fails loudly rather than silently mislabeling every edge it mints.
READ_DOORS = frozenset({
    "inbox-lease", "inbox-peek", "recall", "dossier", "search", "graph_search",
    "read_citation", "ingest_reference",
})


async def stamp_read(
    pool: asyncpg.Pool, *, agent_id: str, door: str, object_id: uuid.UUID | None,
    read_at: datetime | None = None,
) -> None:
    """Log one read-set entry for `agent_id`. `object_id` is None when the read has no
    graph identity to anchor on (a door result that resolved to nothing, or — for mail
    — a message whose own Message object never landed, or whose sender is the operator:
    never minting a possible_upstream edge to the operator's own words is the door-side
    refusal, not a later filter, so those calls simply never reach this function with a
    real id). A None object_id is a silent no-op, never an error — the caller's own
    door still returns its result to the agent either way; only the provenance side
    effect is skipped."""
    if object_id is None:
        return
    await pool.execute(
        "INSERT INTO session_reads (agent_id, door, object_id, read_at) VALUES ($1,$2,$3,$4)",
        agent_id, door, object_id, read_at or datetime.now(UTC))


async def message_object_id(pool: asyncpg.Pool, message_id: int, from_agent: str | None
                            ) -> uuid.UUID | None:
    """The EXISTING Message object mailbox.py's own send() path already mints for every
    message at send time (canonical `message:<id>`) — resolved, never minted, the same
    existence-checked law that module holds itself to ("sending mail must never be how
    an identity or a project first enters the graph"; reading mail must not be either).
    Returns None (no stamp, ever) when `from_agent` is the operator's own — Thoth's
    explicit instruction, matching `cites`' own 'no auto-cite, ever' principle."""
    from src.orchestrator.seats import _OPERATOR_ACTORS

    if from_agent in _OPERATOR_ACTORS:
        return None
    row = await pool.fetchval(
        "SELECT id FROM objects WHERE type='Message' AND canonical=$1", f"message:{message_id}")
    return uuid.UUID(str(row)) if row is not None else None


async def unread_message_ids(
    pool: asyncpg.Pool, message_ids: list[int], *, agent_id: str,
) -> list[int]:
    """THE FIRST-BREATH READ LAW (thread afd27e1a, Thoth mail 13003): which of
    `message_ids` has `agent_id` — THIS exact generation, never an ancestor's — never
    returned in full through a real inbox() call (`inbox-lease` or `inbox-peek`, both
    already stamped at the door above). mcp_server.py's inbox() calls this to refuse
    settling an id still in the returned list — an heir minted mid-flight starts with
    an EMPTY read-set of its own, so acking straight from the wake prompt's own
    headline preview (never an inbox() call, stamps nothing) is exactly what this
    catches; a genuine peek OR lease satisfies it, in this call or an earlier one.

    FAILS OPEN, not closed, on what it cannot observe: a message whose own Message
    object never landed at send time, or one the operator sent (`message_object_id`'s
    own documented no-stamp-ever rule), returns no `object_id` to check session_reads
    against at all — reported as read (not flagged), the same fail-open posture every
    other reader of this table already holds itself to. This law gates what the graph
    can actually see, it does not invent evidence it doesn't have."""
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
    `possible_upstream` edges to every DISTINCT object that source's own read-set
    contains from BEFORE this write (deduped by object — a thing read twice keeps its
    earliest door/time, the more informative of the two). Skips an edge back to the
    written object itself (a session that read the very object it is now writing to is
    not upstream of itself) and skips any edge that already exists for this exact
    (written_object_id, upstream, source_id) triple — the overbroad design accepts
    edges to everything read, but not the SAME edge minted again on every subsequent
    write in a long session. Returns the count of edges actually minted."""
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
