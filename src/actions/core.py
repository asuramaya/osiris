"""The Actions layer — the *only* mutation path into the ontology.

Every method is atomic: the domain write, its ``audit_log`` row, and any
``outbox`` / ``object_events`` rows all commit in one transaction. UI clicks,
helper outputs, and analyst edits must all go through here. No bypassing.

Embodies the resolved rulings:
  * append-only ``assertions`` with a *backward* ``supersedes`` pointer; old
    rows are never mutated. Supersession is *within-source* (multi-source set).
  * merges are *event-sourced*: ``object_events`` is the source of truth and
    ``objects.status``/``merged_into`` are a projection updated alongside.
  * cascades flow through the durable ``outbox`` (not fire-and-forget pub/sub).
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, cast

import asyncpg

from src.ontology.catalog import check_link_type, check_object_type

Json = dict[str, Any]


class ActionError(Exception):
    """Raised when an action would violate an invariant."""


class Actions:
    def __init__(self, pool: asyncpg.Pool, conn: asyncpg.Connection | None = None) -> None:
        self.pool = pool
        # When bound (via atomic()), every write JOINS this one connection's transaction so a
        # multi-step capture is all-or-nothing — no orphan object husk from a process death
        # between the create and its summary. None = the standalone default (each call its own).
        self._conn = conn

    @asynccontextmanager
    async def _tx(self) -> AsyncIterator[asyncpg.Connection]:
        """A transaction-scoped connection: the caller's (when bound — one flat txn, no nested
        savepoint) or a freshly acquired one wrapped in its own transaction (the default)."""
        if self._conn is not None:
            yield self._conn
        else:
            async with self.pool.acquire() as conn, conn.transaction():
                yield conn

    @asynccontextmanager
    async def _read(self) -> AsyncIterator[asyncpg.Connection]:
        """Like _tx but for read helpers — bound connection or a plain acquire, no transaction."""
        if self._conn is not None:
            yield self._conn
        else:
            async with self.pool.acquire() as conn:
                yield conn

    @asynccontextmanager
    async def atomic(self) -> AsyncIterator[Actions]:
        """A transaction boundary spanning several Actions calls. `async with actions.atomic()
        as a` yields an Actions bound to ONE connection+transaction: the create, its assertions,
        and its links either all commit or all roll back. The fix for the non-atomic capture
        path (record_decision was five sequential txns — a crash between them left an orphan)."""
        async with self.pool.acquire() as conn, conn.transaction():
            yield Actions(self.pool, conn=conn)

    # --- internal plumbing (run inside an already-open transaction) -------

    async def _audit(
        self, conn: Any, action: str, actor: str, case_id: uuid.UUID | None, payload: Json
    ) -> None:
        await conn.execute(
            "INSERT INTO audit_log (action, actor, case_id, payload) VALUES ($1,$2,$3,$4)",
            action,
            actor,
            case_id,
            payload,
        )

    async def _outbox(
        self,
        conn: Any,
        event_type: str,
        object_id: uuid.UUID | None,
        case_id: uuid.UUID | None,
        payload: Json,
    ) -> None:
        await conn.execute(
            "INSERT INTO outbox (event_type, object_id, case_id, payload) VALUES ($1,$2,$3,$4)",
            event_type,
            object_id,
            case_id,
            payload,
        )

    # --- 1. create_or_find_object ----------------------------------------

    async def create_or_find_object(
        self,
        type_: str,
        canonical: str,
        actor: str,
        case_id: uuid.UUID | None = None,
        hop_distance: int = 0,
    ) -> uuid.UUID:
        """Deterministic find-or-create on (type, canonical). Emits object_created
        only on genuine creation. Adds case membership (idempotent) when scoped."""
        async with self._tx() as conn:
            # validate against the catalog USING THIS TRANSACTION'S OWN CONNECTION, never
            # a fresh self.pool.acquire() — inside atomic(), self._conn is already one of
            # the pool's N connections; a second acquire here would need an (N+1)th that
            # concurrent atomic() callers holding all N can never free (a real deadlock,
            # found live under test_concurrency.py's 40-way concurrent record_decision).
            # An accretion (an undeclared type self-declaring as a stub, task #97 workstream
            # 2) joins THIS SAME transaction — the throwaway Actions below is bound to `conn`,
            # never a second acquire, so the stub Type's creation is atomic with whatever
            # write triggered it.
            await check_object_type(conn, type_, actions=Actions(self.pool, conn=conn),
                                    actor=actor)
            row = await conn.fetchrow(
                "INSERT INTO objects (type, canonical) VALUES ($1,$2) "
                "ON CONFLICT (type, canonical) DO NOTHING RETURNING id",
                type_,
                canonical,
            )
            if row is not None:
                object_id = cast(uuid.UUID, row["id"])
                await conn.execute(
                    "INSERT INTO object_events (event_type, object_id, payload, actor, case_id) "
                    "VALUES ('create',$1,$2,$3,$4)",
                    object_id,
                    {"type": type_, "canonical": canonical},
                    actor,
                    case_id,
                )
                await self._audit(
                    conn,
                    "create_object",
                    actor,
                    case_id,
                    {"object_id": str(object_id), "type": type_, "canonical": canonical},
                )
                await self._outbox(
                    conn, "object_created", object_id, case_id, {"type": type_}
                )
            else:
                object_id = cast(
                    uuid.UUID,
                    await conn.fetchval(
                        "SELECT id FROM objects WHERE type=$1 AND canonical=$2", type_, canonical
                    ),
                )
                # THE CORPSE GATE (task #29's orphan-link write path): a canonical held
                # by a MERGED object must resolve to its living head, never the corpse —
                # 2410 in_repo/works_in links landed on merged repo:osiris because this
                # find returned whatever row owned the name, status unread. Walk
                # merged_into to the head so every canonical-keyed writer lands on the
                # survivor the merge chose.
                seen = {object_id}
                while True:
                    nxt = await conn.fetchval(
                        "SELECT merged_into FROM objects WHERE id=$1 AND status='merged'",
                        object_id)
                    if nxt is None or nxt in seen:
                        break
                    seen.add(nxt)
                    object_id = cast(uuid.UUID, nxt)

            if case_id is not None:
                await conn.execute(
                    "INSERT INTO case_objects (case_id, object_id, hop_distance) VALUES ($1,$2,$3) "
                    "ON CONFLICT (case_id, object_id) DO NOTHING",
                    case_id,
                    object_id,
                    hop_distance,
                )
            return object_id

    # --- 2. assert_property ----------------------------------------------

    async def assert_property(
        self,
        object_id: uuid.UUID,
        name: str,
        value: Any,
        source_id: str,
        observed_at: datetime,
        confidence: float,
        *,
        case_id: uuid.UUID | None = None,
        helper_run_id: uuid.UUID | None = None,
        evidence_uri: str | None = None,
        evidence_sha256: str | None = None,
        evidence_class: str | None = None,
        actor: str | None = None,
    ) -> int:
        """Append a property assertion. Supersedes the prior non-superseded
        assertion *from the same source* (within-source supersession); other
        sources' values coexist as the multi-source set. `evidence_class` records
        HOW the fact was obtained (see parsers/evidence.py)."""
        actor = actor or source_id
        async with self._tx() as conn:
            # Within-source supersession is a read-modify-write (find the non-superseded
            # prior, then INSERT pointing `supersedes` at it). Under a FLEET, the same source
            # can assert the same property concurrently — and without serialization every
            # racer reads the SAME prior and they all insert, forking the chain into N live
            # rows (caught by the concurrency stress test: 18 lived where 1 should). A
            # txn-scoped advisory lock on the exact (object,name,source) triple serializes
            # just that contended key — different triples and different SOURCES never wait,
            # so multi-source parallelism (the common fleet case) is untouched.
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext($1))",
                f"{object_id}:{name}:{source_id}",
            )
            prior_row = await conn.fetchrow(
                "SELECT a.id, a.value, a.observed_at, a.confidence, a.evidence_class "
                "FROM assertions a "
                "WHERE a.object_id=$1 AND a.name=$2 AND a.source_id=$3 "
                "  AND NOT EXISTS (SELECT 1 FROM assertions s WHERE s.supersedes=a.id) "
                "ORDER BY a.observed_at DESC, a.created_at DESC LIMIT 1",
                object_id,
                name,
                source_id,
            )
            prior = prior_row["id"] if prior_row else None
            # A byte-identical re-assertion carries ZERO information — same source re-observing
            # the same value at the same instant (a re-ingest of an unchanged commit re-asserts
            # with the identical authored-date clock). Skip it, or a 600s cron stacks unbounded
            # supersession chains (live: one commit's summary reached 286 rows). A same-value
            # assertion at a NEW observed_at still lands — "confirmed still true at T2" is real
            # information. No audit/outbox on skip: nothing happened.
            if (
                prior_row is not None
                and prior_row["value"] == value
                and prior_row["observed_at"] == observed_at
                and float(prior_row["confidence"]) == float(confidence)
                and prior_row["evidence_class"] == evidence_class
            ):
                return cast(int, prior_row["id"])
            new_id = cast(
                int,
                await conn.fetchval(
                    "INSERT INTO assertions "
                    "(object_id,name,value,source_id,case_id,helper_run_id,evidence_uri,"
                    " evidence_sha256,observed_at,confidence,supersedes,evidence_class) "
                    "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING id",
                    object_id,
                    name,
                    value,
                    source_id,
                    case_id,
                    helper_run_id,
                    evidence_uri,
                    evidence_sha256,
                    observed_at,
                    confidence,
                    prior,
                    evidence_class,
                ),
            )
            if prior is not None:
                # is_current (migration 0047, thread 2a280e07): the flip costs one
                # indexed UPDATE by primary key, in the SAME transaction as the insert
                # above — never a new read, since `prior` is exactly the row current_
                # assertions' anti-join would have excluded anyway.
                await conn.execute("UPDATE assertions SET is_current=false WHERE id=$1", prior)
            await self._audit(
                conn,
                "assert_property",
                actor,
                case_id,
                {"object_id": str(object_id), "name": name, "supersedes": prior},
            )
            await self._outbox(conn, "property_added", object_id, case_id, {"name": name})
            return new_id

    # --- 2b. supersede_assertion (the CROSS-source supersede) -------------

    async def supersede_assertion(
        self,
        object_id: uuid.UUID,
        name: str,
        superseded_id: int,
        value: Any,
        source_id: str,
        observed_at: datetime,
        confidence: float,
        because: str,
        *,
        case_id: uuid.UUID | None = None,
        evidence_class: str | None = None,
        actor: str | None = None,
    ) -> int:
        """The one legitimate way to retire ANOTHER source's assertion explicitly (thread
        52911d2a, found diagnosing b9aa7326): assert_property's own supersession is scoped
        to the SAME source only ("other sources' values coexist as the multi-source set") —
        by design, for legitimate multi-source corroboration, but it leaves a peer's
        CORRECTION of another agent's bad self-declaration structurally unable to retire it.
        Khnum's own correct_agent_house call on agent:ad1a1cb0-g40-xxiv proved this live: the
        right value landed, the wrong one stayed current, both simultaneously "current" per
        current_assertions' own definition (every row nothing else's supersedes points at).

        `because` is recorded in the audit trail — a cross-source retirement crosses
        accountability lines (someone else's self-declared word), so the justification is
        never optional the way assert_property's own routine same-source supersession
        doesn't need one.

        Raises ActionError when `superseded_id` isn't a `name` assertion on `object_id`, or
        is already superseded — the orchestrator-level `retire_assertion` is expected to
        have already checked this and returned a friendlier error dict; this is the
        last-resort invariant, matching merge_objects' own defense-in-depth."""
        actor = actor or source_id
        async with self._tx() as conn:
            target = await conn.fetchrow(
                "SELECT id FROM assertions WHERE id=$1 AND object_id=$2 AND name=$3",
                superseded_id, object_id, name,
            )
            if target is None:
                raise ActionError(
                    f"assertion {superseded_id} is not a {name!r} assertion on this object")
            already = await conn.fetchval(
                "SELECT 1 FROM assertions WHERE supersedes=$1", superseded_id)
            if already:
                raise ActionError(f"assertion {superseded_id} is already superseded")
            new_id = cast(
                int,
                await conn.fetchval(
                    "INSERT INTO assertions "
                    "(object_id,name,value,source_id,case_id,observed_at,confidence,"
                    " supersedes,evidence_class) "
                    "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id",
                    object_id,
                    name,
                    value,
                    source_id,
                    case_id,
                    observed_at,
                    confidence,
                    superseded_id,
                    evidence_class,
                ),
            )
            # is_current (migration 0047, thread 2a280e07) — same discipline as
            # assert_property's own flip, see its comment
            await conn.execute(
                "UPDATE assertions SET is_current=false WHERE id=$1", superseded_id)
            await self._audit(
                conn,
                "supersede_assertion",
                actor,
                case_id,
                {"object_id": str(object_id), "name": name, "superseded_id": superseded_id,
                 "because": because},
            )
            await self._outbox(conn, "property_added", object_id, case_id, {"name": name})
            return new_id

    # --- 2b*. assert_singular_property (the CROSS-source collapse, opt-in) -

    async def assert_singular_property(
        self,
        object_id: uuid.UUID,
        name: str,
        value: Any,
        source_id: str,
        observed_at: datetime,
        confidence: float,
        *,
        because: str = (
            "a single-current-value property collapsed across sources — a workflow "
            "transition (e.g. a thread's resolve) supersedes every open witness, unlike "
            "assert_property's own same-source-only rule (ruling 1335332e, thread 6361)"
        ),
        case_id: uuid.UUID | None = None,
        evidence_uri: str | None = None,
        evidence_sha256: str | None = None,
        evidence_class: str | None = None,
        actor: str | None = None,
    ) -> int:
        """assert_property's own supersession is scoped to the SAME source only — correct
        for multi-source corroboration (deploy_guard's alarm_schema_drift opens the same
        alarm Thread from two boot services on purpose, source=f"boot:{service}" each,
        thread 35c425f9) — but wrong for a property that is single-valued PER OBJECT
        regardless of who writes it: a Thread's own `status` in the interactive obligation
        lifecycle (one agent opens, a different one resolves — exactly one workflow state
        should ever be current, ruling 1335332e).

        `singular` is a property of the WRITE, not of the function (Thoth's scoping call,
        thread 6361 msg 6408): a caller reaches for THIS verb, instead of assert_property,
        only at the specific call sites where cross-source collapse is correct —
        resolve_thread's own status/resolved_in/resolved_because writes and
        record_decision's inline thread-close of the same shape — never open_thread's,
        which must keep coexisting so a second boot service's witness isn't destroyed.
        DEFAULT stays coexist (assert_property); this is an opt-in door, never a global
        behavior change, because a wrong default that DESTROYS a witness is categorically
        worse than a wrong default that merely leaves a leak unfixed at an un-migrated site.

        Collapses EVERY other current row for (object_id, name), not just one arbitrary
        witness — so resolving a thread with N open witnesses (N boot services corroborating
        one alarm, say) still leaves exactly one current `status` afterward, not N-1.
        Defers straight to assert_property when nothing else is current (the common case:
        nothing to collapse) or when this source itself was already the sole holder."""
        actor = actor or source_id
        async with self._tx() as conn:
            bound = Actions(self.pool, conn=conn)
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext($1))", f"{object_id}:{name}")
            current_rows = await conn.fetch(
                "SELECT id, source_id FROM assertions WHERE object_id=$1 AND name=$2 "
                "AND is_current",
                object_id, name,
            )
            others = [r for r in current_rows if r["source_id"] != source_id]
            if not others:
                return await bound.assert_property(
                    object_id, name, value, source_id, observed_at, confidence,
                    case_id=case_id, evidence_uri=evidence_uri,
                    evidence_sha256=evidence_sha256, evidence_class=evidence_class,
                    actor=actor,
                )
            own = next((r for r in current_rows if r["source_id"] == source_id), None)
            primary = own or others[0]
            new_id = await bound.supersede_assertion(
                object_id, name, primary["id"], value, source_id, observed_at, confidence,
                because, case_id=case_id, evidence_class=evidence_class, actor=actor,
            )
            remaining = [r["id"] for r in current_rows if r["id"] != primary["id"]]
            if remaining:
                await conn.execute(
                    "UPDATE assertions SET is_current=false WHERE id = ANY($1::bigint[])",
                    remaining,
                )
            return new_id

    # --- 2c. repair_stale_current_flags (the migration-0047 backfill) -----

    async def repair_stale_current_flags(
        self, *, limit: int, actor: str,
    ) -> list[int]:
        """Heal the maintained `is_current` MATERIALIZATION (migration 0047) where a real
        `supersedes` FK already excludes a row but the flip never landed (thread 09bde57e —
        a migration-time backfill-completeness gap, not an ongoing write-path bug: both
        writers of `supersedes` (assert_property above, supersede_assertion) flip it in the
        SAME transaction as their INSERT, so no live path can leave a fresh gap).

        Touches ONLY the flag, never an assertion's own content — the kernel itself
        (constitution #3) is untouched, this repairs its projection, same class of act as
        0047's own one-time backfill UPDATE. Batched (`limit`) and idempotent: a row already
        flipped drops out of the WHERE clause, so re-running to walk a large population in
        batches, or after a partial failure, is always safe. Returns the ids actually
        flipped, oldest-observed first — empty when nothing (in this batch) was stale."""
        async with self._tx() as conn:
            rows = await conn.fetch(
                "WITH batch AS ("
                " SELECT a.id FROM assertions a JOIN assertions s ON s.supersedes = a.id "
                " WHERE a.is_current ORDER BY a.observed_at ASC LIMIT $1) "
                "UPDATE assertions SET is_current=false WHERE id IN (SELECT id FROM batch) "
                "RETURNING id",
                limit,
            )
            repaired = sorted(cast(int, r["id"]) for r in rows)
            await self._audit(
                conn, "repair_stale_current_flags", actor, None,
                {"repaired_count": len(repaired), "repaired_ids": repaired},
            )
            return repaired

    # --- 3. create_link --------------------------------------------------

    async def create_link(
        self,
        from_id: uuid.UUID,
        to_id: uuid.UUID,
        type_: str,
        source_id: str,
        observed_at: datetime,
        confidence: float,
        *,
        properties: Json | None = None,
        case_id: uuid.UUID | None = None,
        helper_run_id: uuid.UUID | None = None,
        evidence_uri: str | None = None,
        evidence_sha256: str | None = None,
        evidence_class: str | None = None,
        actor: str | None = None,
    ) -> int:
        """Append a typed link. `evidence_class` records WHY the edge exists (the
        frontier's primary anchor signal; see parsers/evidence.py). (Phase 0: plain
        insert; edge consolidation/dedup and valid_until come with later phases.)"""
        actor = actor or source_id
        async with self._tx() as conn:
            # this transaction's own connection, never a fresh self.pool.acquire() — see
            # create_or_find_object's identical note (the pool-exhaustion deadlock inside
            # atomic()); the accretion path shares it the same way, see that method's note
            await check_link_type(conn, type_, actions=Actions(self.pool, conn=conn),
                                  actor=actor)
            new_id = cast(
                int,
                await conn.fetchval(
                    "INSERT INTO links "
                    "(from_id,to_id,type,properties,source_id,case_id,helper_run_id,"
                    " evidence_uri,evidence_sha256,first_seen,last_seen,confidence,evidence_class) "
                    "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$10,$11,$12) RETURNING id",
                    from_id,
                    to_id,
                    type_,
                    properties or {},
                    source_id,
                    case_id,
                    helper_run_id,
                    evidence_uri,
                    evidence_sha256,
                    observed_at,
                    confidence,
                    evidence_class,
                ),
            )
            await self._audit(
                conn,
                "create_link",
                actor,
                case_id,
                {"from_id": str(from_id), "to_id": str(to_id), "type": type_},
            )
            await self._outbox(
                conn, "link_created", from_id, case_id, {"to_id": str(to_id), "type": type_}
            )
            return new_id

    # --- 3b. invalidate_link (compensating event; never DELETE) ----------

    async def invalidate_link(
        self,
        from_id: uuid.UUID,
        to_id: uuid.UUID,
        type_: str,
        actor: str,
        when: datetime,
        *,
        case_id: uuid.UUID | None = None,
    ) -> int:
        """Deactivate every currently-active link on this (from, to, type) triple by stamping
        `valid_until` — the column the schema has reserved since Phase 0 for exactly this
        ('explicit deactivation, never delete') but that nothing had yet written to. A healed
        link is never gone: it stops counting as current (readers filter on `valid_until IS
        NULL OR valid_until > now()`), and its row stays exactly where it was created, in whose
        name, and why. Idempotent — a triple with nothing active returns 0."""
        async with self._tx() as conn:
            # this transaction's own connection — see create_or_find_object's identical note
            await check_link_type(conn, type_, actions=Actions(self.pool, conn=conn),
                                  actor=actor)
            tag = await conn.execute(
                "UPDATE links SET valid_until=$4 WHERE from_id=$1 AND to_id=$2 AND type=$3 "
                "AND valid_until IS NULL",
                from_id,
                to_id,
                type_,
                when,
            )
            n = int(tag.rsplit(" ", 1)[-1])
            if n:
                await self._audit(
                    conn,
                    "invalidate_link",
                    actor,
                    case_id,
                    {"from_id": str(from_id), "to_id": str(to_id), "type": type_},
                )
                await self._outbox(
                    conn, "link_invalidated", from_id, case_id,
                    {"to_id": str(to_id), "type": type_},
                )
            return n

    # --- 4. merge_objects (event-sourced) --------------------------------

    async def merge_objects(
        self,
        winner_id: uuid.UUID,
        loser_id: uuid.UUID,
        justification: str,
        actor: str,
        case_id: uuid.UUID | None = None,
    ) -> None:
        """Record a merge as an append-only event, update the identity
        projection, leave assertions in place (resolve-on-read), and union
        case memberships. Assertions are never rewritten — provenance survives."""
        if winner_id == loser_id:
            raise ActionError("cannot merge an object into itself")
        async with self._tx() as conn:
            rows = await conn.fetch(
                "SELECT id, status FROM objects WHERE id = ANY($1::uuid[])",
                [winner_id, loser_id],
            )
            by_id = {r["id"]: r["status"] for r in rows}
            if winner_id not in by_id or loser_id not in by_id:
                raise ActionError("both winner and loser must exist")
            if by_id[loser_id] == "merged":
                raise ActionError("loser is already merged")

            await conn.execute(
                "INSERT INTO object_events "
                "(event_type, object_id, related_id, payload, actor, case_id) "
                "VALUES ('merge',$1,$2,$3,$4,$5)",
                winner_id,
                loser_id,
                {"justification": justification},
                actor,
                case_id,
            )
            await conn.execute(
                "UPDATE objects SET status='merged', merged_into=$1 WHERE id=$2",
                winner_id,
                loser_id,
            )
            await conn.execute(
                "INSERT INTO links "
                "(from_id,to_id,type,properties,source_id,confidence,first_seen,last_seen) "
                "VALUES ($1,$2,'same_as',$3,$4,1.0,now(),now())",
                loser_id,
                winner_id,
                {"reason": "merge"},
                actor,
            )
            await conn.execute(
                "INSERT INTO case_objects "
                "(case_id, object_id, hop_distance, added_by_run, added_at) "
                "SELECT case_id, $1, hop_distance, added_by_run, added_at "
                "FROM case_objects WHERE object_id=$2 "
                "ON CONFLICT (case_id, object_id) DO NOTHING",
                winner_id,
                loser_id,
            )
            await self._audit(
                conn,
                "merge_objects",
                actor,
                case_id,
                {"winner": str(winner_id), "loser": str(loser_id), "justification": justification},
            )
            await self._outbox(
                conn, "object_merged", winner_id, case_id, {"loser": str(loser_id)}
            )

    async def unmerge_objects(
        self,
        loser_id: uuid.UUID,
        justification: str,
        actor: str,
        case_id: uuid.UUID | None = None,
    ) -> None:
        """The compensating act for a wrong merge (constitution §3: heal with events,
        never delete). Restores the merged object to active and clears the projection;
        the original merge event and its same_as link stay in the record as witnesses
        of the era. First used for the repo:osiris↔repo:thoth direction defect (task
        #29): the ruling said one project, one room — the execution buried the true
        name under the day-old fracture label, and 2410 links landed on the corpse."""
        async with self._tx() as conn:
            row = await conn.fetchrow(
                "SELECT status, merged_into FROM objects WHERE id=$1", loser_id)
            if row is None:
                raise ActionError("object does not exist")
            if row["status"] != "merged":
                raise ActionError("object is not merged — nothing to unmerge")
            await conn.execute(
                "INSERT INTO object_events "
                "(event_type, object_id, related_id, payload, actor, case_id) "
                "VALUES ('unmerge',$1,$2,$3,$4,$5)",
                loser_id,
                row["merged_into"],
                {"justification": justification},
                actor,
                case_id,
            )
            await conn.execute(
                "UPDATE objects SET status='active', merged_into=NULL WHERE id=$1",
                loser_id,
            )
            await self._audit(
                conn,
                "unmerge_objects",
                actor,
                case_id,
                {"restored": str(loser_id), "was_merged_into": str(row["merged_into"]),
                 "justification": justification},
            )
            await self._outbox(
                conn, "object_unmerged", loser_id, case_id,
                {"was_merged_into": str(row["merged_into"])},
            )

    # --- 5. split_object (Phase 0 skeleton: records lineage) -------------

    async def split_object(
        self,
        object_id: uuid.UUID,
        partition_spec: Json,
        justification: str,
        actor: str,
        case_id: uuid.UUID | None = None,
    ) -> list[uuid.UUID]:
        """Carve new objects out of ``object_id`` per ``partition_spec['parts']``
        (each ``{type, canonical}``) and record the split lineage as events.
        Full assertion re-partitioning is deferred to the ER phase; this keeps
        the primitive present, audited, and reversible via the event log."""
        parts = partition_spec.get("parts", [])
        new_ids: list[uuid.UUID] = []
        async with self._tx() as conn:
            for part in parts:
                new_id = cast(
                    uuid.UUID,
                    await conn.fetchval(
                        "INSERT INTO objects (type, canonical) VALUES ($1,$2) "
                        "ON CONFLICT (type, canonical) DO UPDATE SET canonical=EXCLUDED.canonical "
                        "RETURNING id",
                        part["type"],
                        part["canonical"],
                    ),
                )
                new_ids.append(new_id)
                await conn.execute(
                    "INSERT INTO object_events "
                    "(event_type, object_id, related_id, payload, actor, case_id) "
                    "VALUES ('split',$1,$2,$3,$4,$5)",
                    object_id,
                    new_id,
                    {"part": part},
                    actor,
                    case_id,
                )
                if case_id is not None:
                    await conn.execute(
                        "INSERT INTO case_objects (case_id, object_id) VALUES ($1,$2) "
                        "ON CONFLICT (case_id, object_id) DO NOTHING",
                        case_id,
                        new_id,
                    )
                await self._outbox(conn, "object_created", new_id, case_id, {"type": part["type"]})
            await self._audit(
                conn,
                "split_object",
                actor,
                case_id,
                {
                    "object_id": str(object_id),
                    "justification": justification,
                    "new": [str(i) for i in new_ids],
                },
            )
            return new_ids

    # --- status transitions (event-sourced; for pattern hygiene) ---------

    async def set_status(
        self,
        object_id: uuid.UUID,
        status: str,
        justification: str,
        actor: str,
        case_id: uuid.UUID | None = None,
    ) -> None:
        """Transition an object's lifecycle status, recorded as an append-only
        object_event so snapshots replay correctly. Used by pattern hygiene to
        archive stale patterns (DESIGN §11)."""
        async with self._tx() as conn:
            event = "archive" if status == "archived" else "status_change"
            await conn.execute(
                "INSERT INTO object_events (event_type, object_id, payload, actor, case_id) "
                "VALUES ($1,$2,$3,$4,$5)",
                event,
                object_id,
                {"status": status, "justification": justification},
                actor,
                case_id,
            )
            await conn.execute("UPDATE objects SET status=$1 WHERE id=$2", status, object_id)
            await self._audit(
                conn, "set_status", actor, case_id,
                {"object_id": str(object_id), "status": status},
            )

    # --- 6. tag_object ---------------------------------------------------

    async def tag_object(
        self,
        object_id: uuid.UUID,
        tag: str,
        scope: str,
        actor: str,
        case_id: uuid.UUID | None = None,
    ) -> int:
        """Tags are additive (no within-source supersede): stored as 'tag'
        assertions so they inherit provenance and audit for free."""
        async with self._tx() as conn:
            new_id = cast(
                int,
                await conn.fetchval(
                    "INSERT INTO assertions "
                    "(object_id,name,value,source_id,case_id,observed_at,confidence) "
                    "VALUES ($1,'tag',$2,$3,$4,now(),1.0) RETURNING id",
                    object_id,
                    {"tag": tag, "scope": scope},
                    actor,
                    case_id,
                ),
            )
            await self._audit(
                conn,
                "tag_object",
                actor,
                case_id,
                {"object_id": str(object_id), "tag": tag, "scope": scope},
            )
            await self._outbox(
                conn, "property_added", object_id, case_id, {"name": "tag", "tag": tag}
            )
            return new_id

    async def execute(self, query: str, *args: Any) -> str:
        """Raw SQL joining this Actions' OWN transaction when bound (inside an
        `atomic()` block), a fresh pool connection otherwise — for a caller that needs
        one extra write alongside assert_property/create_link in the SAME atomic
        sequence (a bespoke table `assert_property` has no verb for, e.g. `fleet_
        messages`/`agent_mounts`) without reaching into the private `_conn` directly.
        No audit/outbox row — this is a passthrough, not a domain write; the CALLER's
        own assert_property calls in the same block carry the audit trail."""
        async with self._tx() as conn:
            return cast(str, await conn.execute(query, *args))

    # --- read helpers (no mutation; used by callers and tests) -----------

    async def resolve_object_id(self, object_id: uuid.UUID) -> uuid.UUID:
        """Follow the merged_into chain to the current canonical (winner) id."""
        async with self._read() as conn:
            current = object_id
            for _ in range(100):
                nxt = await conn.fetchval("SELECT merged_into FROM objects WHERE id=$1", current)
                if nxt is None:
                    return current
                current = cast(uuid.UUID, nxt)
            raise ActionError("merge chain too deep (cycle?)")

    async def current_values(self, object_id: uuid.UUID, name: str) -> list[Json]:
        """Current value(s) of a property — the multi-source set, one per source."""
        async with self._read() as conn:
            rows = await conn.fetch(
                "SELECT value, source_id, confidence, observed_at FROM current_assertions "
                "WHERE object_id=$1 AND name=$2 ORDER BY source_id",
                object_id,
                name,
            )
            return [dict(r) for r in rows]
