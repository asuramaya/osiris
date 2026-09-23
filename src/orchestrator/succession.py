"""succession: the full generation-chain walk. The gap this closes: the chain as one
bounded read, lineage(agent|seat, depth<=N) -> [{generation, minted_because,
wrote_anything}] per hop. A prior session needed this and fell back to raw SQL:
dossier() already answers succeeded_from/minted_because for one hop, but walking N
generations back needed N calls.

Not named lineage.py: that module already exists and means something else entirely (the
swarm lineage, i.e. sub-agent spawn-tree reconstruction from harness transcripts,
spawned_by/acts_for edges). This is about agent generations succeeding each other within
one seat (succeeded_from, seat_generation), a different axis, caught by reading the
existing module before writing a new one so it didn't clobber a real, unrelated,
load-bearing module.

Complementary to, not a duplicate of, `agents.nearest_handoff_ancestor`: that one jumps to
the nearest ancestor bearing a real handoff, for orient()'s own inheritance fallback; this
one walks and reports every hop, for a caller asking to see the whole chain. Different
questions, coordinated between the two implementations before either was built so neither
duplicated the other's job.

Kept in its own module rather than inside agents.py deliberately: agents.py was another
session's active work at the time, and the one succeeded_from hop-step this needs is small
enough to stand alone rather than force a refactor of code that had just landed.
One function per module, matching doors.py/describe.py/recall.py/smoke.py's own precedent."""
from __future__ import annotations

from typing import Any

import asyncpg

from src.orchestrator.compositions import resolve_ref

MAX_SUCCESSION_HOPS = 10  # mint_heir's own kind of bound: a chain walk never widens unbounded


async def succession_chain(
    pool: asyncpg.Pool, ref: str, *, max_hops: int = MAX_SUCCESSION_HOPS,
) -> list[dict[str, Any]]:
    """Walk an Agent's `succeeded_from` chain backward, one entry per hop:
    {agent_id, generation, minted_because, wrote_anything, session}. `ref` accepts anything
    resolve_ref does (UUID, 8-char short id, canonical, or name) for the starting agent; each
    subsequent hop follows `succeeded_from`'s own stored canonical directly (the shape it's
    actually asserted in, so no re-resolution is needed hop to hop). Stops at a root (no
    predecessor) or `max_hops`, never widening into an unbounded search. `wrote_anything`
    checks for an assertion on some other object (a Thread, a Decision, i.e. real work) or a
    sent message, in the same spirit as agents.agent_has_acted's own act-detection, not a
    bare "any self_declared assertion", which would be wrong: seat_generation/minted_because
    are themselves self_declared, stamped by the mint process on every agent regardless of
    whether it ever did anything (caught live by this module's own test: a zero-write agent's
    own mint-time bookkeeping made the naive check read as work). Deliberately simpler than
    agent_has_acted's own debounce-specific exclude-pair logic, which doesn't fit a walk
    visiting many hops with no single fixed pair to exclude.

    `session`: not a new write. `register_agent` already asserts this on every mount() call
    (agents.py, `assert_property(a, "session", identity.session, ...)`), self_declared, the
    harness session id that is the transcript filename's own stem. This walker already read
    three sibling properties off the identical `current_assertions` row; it was simply never
    asked for the fourth. The record already existed, only the reader didn't reach it,
    confirmed by reading the source rather than guessed."""
    start = await resolve_ref(pool, ref)
    if start is None:
        return []
    cur = await pool.fetchval("SELECT canonical FROM objects WHERE id=$1", start)
    if cur is None:
        return []

    out: list[dict[str, Any]] = []
    for _ in range(max_hops):
        row = await pool.fetchrow(
            "SELECT o.id, "
            " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
            "   AND a.name='seat_generation' ORDER BY a.confidence DESC, a.observed_at DESC "
            "   LIMIT 1) AS generation, "
            " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
            "   AND a.name='minted_because' ORDER BY a.confidence DESC, a.observed_at DESC "
            "   LIMIT 1) AS minted_because, "
            " (EXISTS (SELECT 1 FROM assertions x WHERE x.source_id=o.canonical "
            "     AND x.object_id != o.id) "
            "  OR EXISTS (SELECT 1 FROM fleet_messages WHERE from_agent=o.canonical)) "
            " AS wrote_anything, "
            " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
            "   AND a.name='session' ORDER BY a.confidence DESC, a.observed_at DESC "
            "   LIMIT 1) AS session "
            "FROM objects o WHERE o.canonical=$1 AND o.type='Agent'", cur)
        if row is None:
            break
        out.append({"agent_id": cur, "generation": row["generation"],
                    "minted_because": row["minted_because"],
                    "wrote_anything": row["wrote_anything"],
                    "session": row["session"]})
        nxt = await pool.fetchval(
            "SELECT a.value #>> '{}' FROM current_assertions a "
            "JOIN objects o ON o.id=a.object_id "
            "WHERE o.canonical=$1 AND a.name='succeeded_from' "
            "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", cur)
        if not nxt:
            break
        cur = str(nxt)
    return out
