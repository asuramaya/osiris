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
from datetime import datetime
from typing import Any, cast

import asyncpg

Json = dict[str, Any]


class ActionError(Exception):
    """Raised when an action would violate an invariant."""


class Actions:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

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
        async with self.pool.acquire() as conn, conn.transaction():
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
        actor: str | None = None,
    ) -> int:
        """Append a property assertion. Supersedes the prior non-superseded
        assertion *from the same source* (within-source supersession); other
        sources' values coexist as the multi-source set."""
        actor = actor or source_id
        async with self.pool.acquire() as conn, conn.transaction():
            prior = await conn.fetchval(
                "SELECT a.id FROM assertions a "
                "WHERE a.object_id=$1 AND a.name=$2 AND a.source_id=$3 "
                "  AND NOT EXISTS (SELECT 1 FROM assertions s WHERE s.supersedes=a.id) "
                "ORDER BY a.observed_at DESC, a.created_at DESC LIMIT 1",
                object_id,
                name,
                source_id,
            )
            new_id = cast(
                int,
                await conn.fetchval(
                    "INSERT INTO assertions "
                    "(object_id,name,value,source_id,case_id,helper_run_id,evidence_uri,"
                    " evidence_sha256,observed_at,confidence,supersedes) "
                    "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING id",
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
        actor: str | None = None,
    ) -> int:
        """Append a typed link. (Phase 0: plain insert; edge consolidation/
        dedup and valid_until deactivation come with the ER/graph phases.)"""
        actor = actor or source_id
        async with self.pool.acquire() as conn, conn.transaction():
            new_id = cast(
                int,
                await conn.fetchval(
                    "INSERT INTO links "
                    "(from_id,to_id,type,properties,source_id,case_id,helper_run_id,"
                    " evidence_uri,evidence_sha256,first_seen,last_seen,confidence) "
                    "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$10,$11) RETURNING id",
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
        async with self.pool.acquire() as conn, conn.transaction():
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
        async with self.pool.acquire() as conn, conn.transaction():
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
        async with self.pool.acquire() as conn, conn.transaction():
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
        async with self.pool.acquire() as conn, conn.transaction():
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
        async with self.pool.acquire() as conn:
            current = object_id
            for _ in range(100):
                nxt = await conn.fetchval("SELECT merged_into FROM objects WHERE id=$1", current)
                if nxt is None:
                    return current
                current = cast(uuid.UUID, nxt)
            raise ActionError("merge chain too deep (cycle?)")

    async def current_values(self, object_id: uuid.UUID, name: str) -> list[Json]:
        """Current value(s) of a property — the multi-source set, one per source."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT value, source_id, confidence, observed_at FROM current_assertions "
                "WHERE object_id=$1 AND name=$2 ORDER BY source_id",
                object_id,
                name,
            )
            return [dict(r) for r in rows]
