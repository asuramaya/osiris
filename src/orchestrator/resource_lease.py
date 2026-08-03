"""Resource leases — task #103/#119: an exact-key lock for anything a fleet of agents can
step on at once outside the git index (a file path, the docker daemon, a merge back into
composer).

`open_thread(assignee=)`'s `leased_to` (Alfred's ask 5, ruling dd47c1da §4.3) already ships
the right UX — surface the holder, never mint a silent duplicate — but its conflict check
(`capture.find_near_duplicate_open_thread`) is fuzzy PROSE similarity over a thread's
SUMMARY (pg_trgm/SequenceMatcher, threshold 0.60, never empirically tuned), read-then-write
with no DB-level guarantee, and repo-scoped only. Two agents about to touch the same file
with differently-worded summaries fall under threshold and get no lease at all — the tool
meant to prevent silent collision would silently collide. A host-level resource (the docker
daemon) has no repo to scope against and can't be leased through it either.

This module keeps the UX and fixes the LOOKUP: `resource_id` is matched by EQUALITY, never
similarity, and the claim is a real Postgres constraint (mirrors `helper_runs_active_claim`
exactly — 0001_initial_schema, `runner.claim_run`) rather than an application-level
read-then-write race. A lease is still STORED as a Thread (the existing object type, graph
visibility, mail integration) — `objects(type, canonical)`'s own unique index already makes
that lookup exact, since the canonical here hashes `resource_id` itself, never prose
describing it. The `resource_leases` table is the atomicity layer underneath: `current_
assertions` is a view with no indexes of its own and cannot carry the partial unique index
this needs.

Scope (Thoth's dispatch, ruling 4e4fa6cd/83421412b9cc): core + storage only. The MCP verb
that exposes this to the fleet lands separately, once src/mcp_server.py is free.

NARROWED after #103's re-scope (ruling ff3bdc37, "TREAT THE AGENTS LIKE PEOPLE WORKING ON A
REPO"): the working tree was this module's original motivating case (tonight's git races),
but per-seat worktrees dissolve tree contention structurally rather than coordinating it —
once every agent has its own checkout, there is nothing left for a tree lease to arbitrate.
The durable purpose is genuinely SHARED, non-isolable resources instead: the deploy
pipeline, the database, a running service. The MCP-facing docstrings (mcp_server.py)
deliberately no longer name `tree`/`compose-merge` as example resource_ids for this reason.
"""
from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

# Same channel/evidence taxonomy as capture.py's session-capture writes — a lease acquired
# by a live agent is SELF_DECLARED, the taxonomy's highest-trust class.
_SOURCE = "session"
_EC = EvidenceClass.SELF_DECLARED.value
_CONF = confidence_for(EvidenceClass.SELF_DECLARED)


def _canon(resource_id: str) -> str:
    """Same scheme as `capture._canon`, distinct prefix — hashes `resource_id` ITSELF, never
    prose describing it. The same resource_id always finds the same Thread: an exact key,
    not a similarity threshold. (A file path and a docker-daemon token both hash cleanly;
    nothing here cares what the string means.)"""
    return f"resource-lease:{hashlib.sha1(resource_id.encode()).hexdigest()[:12]}"


@dataclass
class LeaseResult:
    """`holder` is always who ACTUALLY holds `resource_id` right now — the caller's own
    `holder` argument iff `acquired` is True, someone else's otherwise. Alfred's #4.3 UX:
    surfaced, never a silent duplicate."""
    resource_id: str
    thread_id: uuid.UUID
    acquired: bool
    holder: str
    acquired_at: datetime


async def acquire(
    actions: Actions, resource_id: str, holder: str, *, source: str = _SOURCE,
) -> LeaseResult:
    """Claim `resource_id` for `holder`, or find out who already holds it. Atomic: the
    `resource_leases_active_claim` partial unique index (one HELD row per resource_id) is
    the actual guarantee — this never reads-then-writes, it inserts and lets Postgres decide
    the race, then reports the true winner either way (never asserts local caller order).

    Manual transaction (not `actions.atomic()`): `resource_leases` is a bespoke table, not
    something the Actions layer's own helpers write to, and `atomic()`'s yielded Actions
    exposes no raw-SQL escape hatch — this mirrors `create_or_find_object`'s own documented
    pattern for an accretion joining an existing transaction (core.py, "a throwaway Actions
    bound to the SAME connection, never a second acquire — a real deadlock otherwise")."""
    resource_id = resource_id.strip()
    holder = holder.strip()
    if not resource_id:
        raise ValueError("resource_id must be non-empty")
    if not holder:
        raise ValueError("holder must be non-empty")
    observed = datetime.now(UTC)
    async with actions.pool.acquire() as conn, conn.transaction():
        a = Actions(actions.pool, conn=conn)
        thread_id = await a.create_or_find_object(
            "Thread", _canon(resource_id), source)
        await a.assert_property(thread_id, "resource_id", resource_id, source, observed,
                                _CONF, evidence_class=_EC)
        await a.assert_property(thread_id, "status", "open", source, observed, _CONF,
                                evidence_class=_EC)
        await a.assert_property(thread_id, "kind", "resource-lease", source, observed,
                                _CONF, evidence_class=_EC)
        row = await conn.fetchrow(
            "INSERT INTO resource_leases (resource_id, thread_id, holder) "
            "VALUES ($1,$2,$3) ON CONFLICT DO NOTHING RETURNING id, acquired_at",
            resource_id, thread_id, holder,
        )
        if row is not None:
            await a.assert_property(thread_id, "owner", holder, source, observed, _CONF,
                                    evidence_class=_EC)
            return LeaseResult(resource_id=resource_id, thread_id=thread_id, acquired=True,
                               holder=holder, acquired_at=row["acquired_at"])
        existing = await conn.fetchrow(
            "SELECT holder, acquired_at FROM resource_leases "
            "WHERE resource_id=$1 AND status='held'", resource_id,
        )
        # existing is never None here: the insert only conflicts against a row the partial
        # unique index says is currently 'held', so that row is exactly this select's target.
        assert existing is not None, (
            f"resource_leases_active_claim conflicted on {resource_id!r} but no held row "
            "was found — the constraint and this read have drifted"
        )
        return LeaseResult(resource_id=resource_id, thread_id=thread_id, acquired=False,
                           holder=existing["holder"], acquired_at=existing["acquired_at"])


async def release(pool: asyncpg.Pool, resource_id: str, holder: str) -> bool:
    """Release `resource_id`, but only for the holder that actually claimed it — never let
    a different agent release a lease it doesn't hold. Returns True iff a held row was
    actually released (False for: not currently held, or held by someone else).

    THREAD RE-SYNC, ENFORCED (2026-08-03, Thoth's Phase 0 Tier 2 dispatch, msg 3354): a
    released `resource_leases` row used to leave its companion Thread's status='open' and
    owner=<holder> FOREVER — correct for `check_lease` (reads this table directly, the live
    authority) but WRONG for every graph-level reader that only ever sees Thread objects,
    never this SQL table (orient()'s open_threads, recall, briefing, dossier — a released
    lease read as a permanently-open obligation nobody was actually still holding). A
    successful release now also `resolve_thread`s the paired Thread, in the SAME
    transaction as the lease-row update — both land or neither does."""
    resource_id = resource_id.strip()
    holder = holder.strip()
    async with pool.acquire() as conn, conn.transaction():
        row = await conn.fetchrow(
            "UPDATE resource_leases SET status='released', released_at=now() "
            "WHERE resource_id=$1 AND holder=$2 AND status='held' RETURNING thread_id",
            resource_id, holder,
        )
        if row is None:
            return False
        from src.orchestrator.capture import resolve_thread

        await resolve_thread(
            Actions(pool, conn=conn), str(row["thread_id"]),
            because=f"resource lease released by {holder}", source=holder)
    return True


async def current_holder(pool: asyncpg.Pool, resource_id: str) -> dict[str, Any] | None:
    """Read-only: who holds `resource_id` right now, or None if it's free. Reads the SQL
    table directly (the authority for "held right now"), never the Thread's `owner`
    assertions, which are a historical ledger of past holders, not a live-status field."""
    row = await pool.fetchrow(
        "SELECT holder, thread_id, acquired_at FROM resource_leases "
        "WHERE resource_id=$1 AND status='held'", resource_id.strip(),
    )
    return dict(row) if row is not None else None


_MIN_REAP_AGE_SECS = 60  # a floor, not a gate -- see reap_stale's own docstring


async def reap_stale(pool: asyncpg.Pool, *, older_than_secs: int = 3600) -> int:
    """Recover leases an agent never released (a crash, a compaction, a dropped session) —
    the active-claim partial unique index would otherwise wedge that resource_id FOREVER,
    the same risk `runner.reap_stale_runs` names for `helper_runs`. Default an hour: a
    resource lease here is agent-work-paced (a whole session touching a file), not a
    machine-timescale run, so this is deliberately looser than `helper_runs`' 900s. Returns
    the count recovered.

    FLOORED (2026-08-03, Thoth's Tier 1 dispatch off the silent-authority census, decision
    497a066a): `older_than_secs` used to be fully caller-controlled with no minimum —
    `older_than_secs=0` force-released EVERY currently-held lease fleet-wide in one call, a
    mutual-exclusion break wearing a garbage collector's name; no gate was even needed to
    abuse it, the parameter itself was the weapon. A numeric floor, not an authority gate
    (ruling 577988ed: a fleet-wide single point of failure must never refuse-to-serve on a
    check that can false-positive — an identity/actor resolution can; a deterministic clamp
    on an integer cannot, so it can never wedge a legitimate reap). Refuses loudly below
    `_MIN_REAP_AGE_SECS`, never silently clamps — a caller who asked for an aggressive reap
    and got a quietly smaller one would never know its request was rewritten.

    THREAD RE-SYNC (same dispatch, same pattern as `release`'s own fix, commit 510b4a9): a
    reaped row used to leave its companion Thread's status='open' forever — correct for
    `check_lease` (reads this table directly) but wrong for every graph-level reader
    (orient's open_threads, recall, briefing, dossier) that only ever sees Thread objects.
    Each reaped row now also `resolve_thread`s its paired Thread, in the SAME transaction
    as the bulk UPDATE — both land or neither does."""
    if older_than_secs < _MIN_REAP_AGE_SECS:
        raise ValueError(
            f"older_than_secs must be >= {_MIN_REAP_AGE_SECS} — reap_stale is a crash/"
            "compaction backstop, not a way to force-release every held lease fleet-wide "
            "in one call")
    async with pool.acquire() as conn, conn.transaction():
        rows = await conn.fetch(
            "UPDATE resource_leases SET status='reaped', released_at=now() "
            "WHERE status='held' AND acquired_at < now() - make_interval(secs => $1) "
            "RETURNING id, thread_id, holder", older_than_secs,
        )
        from src.orchestrator.capture import resolve_thread

        for row in rows:
            await resolve_thread(
                Actions(pool, conn=conn), str(row["thread_id"]),
                because=f"resource lease reaped (stale, held by {row['holder']})",
                source=_SOURCE)
    return len(rows)
