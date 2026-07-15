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

from src.ontology.schema import check_link_type, check_object_type

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
        check_object_type(type_)  # validate against the declared semantic layer
        async with self._tx() as conn:
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
            await self._audit(
                conn,
                "assert_property",
                actor,
                case_id,
                {"object_id": str(object_id), "name": name, "supersedes": prior},
            )
            await self._outbox(conn, "property_added", object_id, case_id, {"name": name})
            return new_id

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
        check_link_type(type_)  # validate against the declared semantic layer
        async with self._tx() as conn:
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
        check_link_type(type_)
        async with self._tx() as conn:
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
