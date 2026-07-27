"""succession — the full generation-CHAIN walk (task #64, ruling ad19a779: "the real gap is
the chain as one bounded read: lineage(agent|seat, depth<=N) -> [{generation,
minted_because, wrote_anything}] per hop"). Thoth's own turn needed this and fell to raw
SQL: dossier() already answers succeeded_from/minted_because for ONE hop, but walking N
generations back needed N calls.

NOT named lineage.py — that module already exists and means something else entirely (the
SWARM lineage: sub-agent spawn-tree reconstruction from harness transcripts, spawned_by/
acts_for edges). This is about agent GENERATIONS succeeding each other within one seat
(succeeded_from, seat_generation) — a different axis, caught by Read-before-Write before
it clobbered a real, unrelated, load-bearing module.

Complementary to, not a duplicate of, `agents.nearest_handoff_ancestor` (Khnum, thread
e749036e, same day) — that one JUMPS to the nearest ancestor bearing a real handoff, for
orient()'s own inheritance fallback; this one WALKS and reports every hop, for a caller
asking "show me the whole chain". Different questions, coordinated via mail (msg
1419/1423/1431) before either was built so neither duplicated the other's actual job.

Kept in its own module rather than inside agents.py deliberately: agents.py is Khnum's live
territory this campaign (the collision-watch discipline every wave this session has held
to), and the one succeeded_from hop-step this needs is small enough to stand alone rather
than force a refactor of code that just landed. One-verb-one-module, matching doors.py/
describe.py/recall.py/smoke.py's own precedent."""
from __future__ import annotations

from typing import Any

import asyncpg

from src.orchestrator.compositions import resolve_ref

MAX_SUCCESSION_HOPS = 10  # mint_heir's own kind of bound — a chain walk never widens unbounded


async def succession_chain(
    pool: asyncpg.Pool, ref: str, *, max_hops: int = MAX_SUCCESSION_HOPS,
) -> list[dict[str, Any]]:
    """Walk an Agent's `succeeded_from` chain backward, one entry per hop:
    {agent_id, generation, minted_because, wrote_anything}. `ref` accepts anything
    resolve_ref does (UUID, 8-char short id, canonical, or name) for the STARTING agent;
    each subsequent hop follows `succeeded_from`'s own stored canonical directly (the shape
    it's actually asserted in — no re-resolution needed hop to hop). Stops at a root (no
    predecessor) or `max_hops`, never widening into an unbounded search. `wrote_anything`
    checks for an assertion on some OTHER object (a Thread, a Decision — real work) or a
    sent message, same spirit as agents.agent_has_acted's own act-detection — NOT a bare
    "any self_declared assertion", which would be wrong: seat_generation/minted_because are
    THEMSELVES self_declared, stamped by the mint process on every agent regardless of
    whether it ever did anything (caught live by this module's own test — a zero-write
    phantom's OWN mint-time bookkeeping made the naive check read as work). Deliberately
    simpler than agent_has_acted's own debounce-specific exclude-PAIR logic, which doesn't
    fit a walk visiting many hops with no single fixed pair to exclude."""
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
            " AS wrote_anything "
            "FROM objects o WHERE o.canonical=$1 AND o.type='Agent'", cur)
        if row is None:
            break
        out.append({"agent_id": cur, "generation": row["generation"],
                    "minted_because": row["minted_because"],
                    "wrote_anything": row["wrote_anything"]})
        nxt = await pool.fetchval(
            "SELECT a.value #>> '{}' FROM current_assertions a "
            "JOIN objects o ON o.id=a.object_id "
            "WHERE o.canonical=$1 AND a.name='succeeded_from' "
            "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", cur)
        if not nxt:
            break
        cur = str(nxt)
    return out
