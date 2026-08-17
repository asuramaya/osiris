"""retire_assertion — the cross-source supersede (thread 52911d2a, found diagnosing
b9aa7326): assert_property's own supersession is scoped to the SAME source only ("other
sources' values coexist as the multi-source set") — by design, for legitimate multi-source
corroboration. It leaves exactly one class unreachable: a peer's CORRECTION of another
agent's bad self-declaration can never retire it. Khnum's own correct_agent_house call on
agent:ad1a1cb0-g40-xxiv proved this live (decision d28d1459): the right value ("58") landed
from a different source, the wrong one ("2", self-declared) stayed current, both
simultaneously "current" per current_assertions' own definition (every row nothing else's
supersedes points at) — a reader without an exact ORDER BY confidence DESC, observed_at DESC
LIMIT 1 could still surface the wrong one.

Deliberately NARROW, not a general edit/delete escape hatch: it retires ONE named
assertion, by id, on a caller-named (object, name) — never a bare "whatever's current now",
so a caller must already know exactly which row is wrong (from a diagnosis, never a guess).
`because` is required: a cross-source retirement crosses accountability lines, so the
justification is not optional the way assert_property's own routine supersession isn't."""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.actions.core import ActionError, Actions
from src.orchestrator.compositions import resolve_ref
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for


async def list_assertions(actions: Actions, *, ref: str, name: str) -> dict[str, Any]:
    """READ-ONLY (task #157/#382067d9's own gap): retire_assertion needs a `superseded_id`
    — an assertions.id — and until this, NOTHING exposed one. dossier()/trace_evidence()
    both resolve through current_assertions to a single belief-winner or a bare value list;
    neither ever surfaced the row id underneath. This is the smallest possible door: every
    CURRENT (non-superseded) assertion of `name` on the object `ref` resolves to, each
    carrying its own `id` — exactly what retire_assertion's own required argument needs,
    with nothing else layered on (no write, no ranking, no bulk scope)."""
    name = (name or "").strip()
    if not name:
        return {"error": "name is required"}
    pool = actions.pool
    object_id = await resolve_ref(pool, ref)
    if object_id is None:
        return {"error": f"no object matches {ref!r}"}
    rows = await pool.fetch(
        "SELECT id, value, source_id, confidence, observed_at FROM current_assertions "
        "WHERE object_id=$1 AND name=$2 ORDER BY confidence DESC, observed_at DESC",
        object_id, name)
    return {
        "ref": ref, "object_id": str(object_id), "name": name,
        "assertions": [
            {"id": r["id"], "value": r["value"], "source": r["source_id"],
             "confidence": r["confidence"], "observed_at": r["observed_at"].isoformat()}
            for r in rows
        ],
    }


async def stale_current_flags(actions: Actions, *, limit: int = 50) -> dict[str, Any]:
    """THE READ DOOR (thread 09bde57e, Sekhmet's own blocker named — no read door for
    assertions/supersedes through the composer, hand SQL off the table): every row where
    `is_current=true` (migration 0047's maintained flag) YET a real `supersedes` FK already
    points at it from another assertion — the exact anomaly khepri's own specimen (seat:
    ddafff44, assertion 2676719) surfaced live: current_assertions kept listing a row as
    current that a genuine successor had already superseded, because the flip (assert_
    property's own same-source path, or supersede_assertion's cross-source one — both flip
    `is_current` in the SAME transaction as the INSERT, per 0047's own design) never landed
    for this specific row. Read-only, bounded (`limit` caps the sample; `count` is always
    the true total, never capped, so a caller sees the real population size even from a
    small sample)."""
    pool = actions.pool
    count = await pool.fetchval(
        "SELECT count(*) FROM assertions a JOIN assertions s ON s.supersedes = a.id "
        "WHERE a.is_current")
    rows = await pool.fetch(
        "SELECT a.id AS stale_id, a.object_id, a.name, a.value #>> '{}' AS value, "
        " a.source_id, a.observed_at, "
        " s.id AS superseding_id, s.source_id AS superseding_source, "
        " s.observed_at AS superseding_observed_at "
        "FROM assertions a JOIN assertions s ON s.supersedes = a.id "
        "WHERE a.is_current ORDER BY a.observed_at ASC LIMIT $1", limit)
    return {
        "count": count,
        "sample": [
            {"stale_id": r["stale_id"], "object_id": str(r["object_id"]), "name": r["name"],
             "value": r["value"], "source": r["source_id"],
             "observed_at": r["observed_at"].isoformat(),
             "superseding_id": r["superseding_id"], "superseding_source": r["superseding_source"],
             "superseding_observed_at": r["superseding_observed_at"].isoformat()}
            for r in rows
        ],
    }


async def repair_stale_current_flags(
    actions: Actions, *, dry_run: bool = True, limit: int = 500, actor: str | None = None,
) -> dict[str, Any]:
    """THE BACKFILL for thread 09bde57e's own kernel gap — the compensating fix for exactly
    the population `stale_current_flags` measures (123,914 of 267,305 rows at last count,
    d8225e71): `assertions.is_current` is a maintained MATERIALIZATION of the append-only
    kernel (migration 0047), not itself a kernel fact — flipping it here heals the
    projection, touches no assertion's own content, and violates nothing in constitution #3.

    `dry_run=True` (default, list-only): names how many rows WOULD flip and their ids,
    writes nothing. `dry_run=False` is the operator's own call, never automatic — flips
    `is_current=false` on up to `limit` stale rows in one batched UPDATE, oldest-observed
    first. Batched because the live population is five figures; a single UPDATE touching all
    of it at once is not the shape of a repair anyone should run unattended. Idempotent: a
    repeat call only ever sees rows STILL stale — a row already flipped drops out of the
    WHERE clause on its own, so re-running (to walk the full population in batches, or after
    a partial failure) is always safe."""
    pool = actions.pool
    total_before = await pool.fetchval(
        "SELECT count(*) FROM assertions a JOIN assertions s ON s.supersedes = a.id "
        "WHERE a.is_current")
    if dry_run:
        rows = await pool.fetch(
            "SELECT a.id FROM assertions a JOIN assertions s ON s.supersedes = a.id "
            "WHERE a.is_current ORDER BY a.observed_at ASC LIMIT $1", limit)
        ids = [r["id"] for r in rows]
        return {"dry_run": True, "total_stale": total_before, "would_repair": len(ids),
                "sample_ids": ids}
    repaired = await actions.repair_stale_current_flags(limit=limit, actor=actor or "system")
    return {"dry_run": False, "repaired": len(repaired), "repaired_ids": repaired,
            "total_stale_before": total_before,
            "total_stale_remaining": max(total_before - len(repaired), 0)}


async def retire_assertion(
    actions: Actions, *, ref: str, name: str, superseded_id: int, value: Any,
    because: str, actor: str,
) -> dict[str, Any]:
    """Retire assertion `superseded_id` on the object `ref` resolves to (any form
    resolve_ref accepts — UUID, short-id, canonical, or name), asserting `value` as the new
    current fact from `actor`. Refuses LOUDLY (an error dict, nothing written) when:
    `because` is blank; `ref` doesn't resolve; `superseded_id` isn't a `name` assertion on
    that object; it's already superseded by something else."""
    because = (because or "").strip()
    if not because:
        return {"error": "because is required — a cross-source retirement must carry its "
                         "justification, not just a value"}
    name = (name or "").strip()
    if not name:
        return {"error": "name is required"}
    pool = actions.pool
    object_id = await resolve_ref(pool, ref)
    if object_id is None:
        return {"error": f"no object matches {ref!r}"}
    target = await pool.fetchrow(
        "SELECT id, value, source_id FROM assertions WHERE id=$1 AND object_id=$2 AND name=$3",
        superseded_id, object_id, name)
    if target is None:
        return {"error": f"assertion {superseded_id} is not a {name!r} assertion on "
                         f"{ref!r} — check the id and the property name"}
    already = await pool.fetchval(
        "SELECT 1 FROM assertions WHERE supersedes=$1", superseded_id)
    if already:
        return {"error": f"assertion {superseded_id} is already superseded — nothing to "
                         "retire"}
    now = datetime.now(UTC)
    ec = EvidenceClass.SELF_DECLARED
    try:
        new_id = await actions.supersede_assertion(
            object_id, name, superseded_id, value, actor, now, confidence_for(ec), because,
            evidence_class=ec.value, actor=actor)
    except ActionError as exc:
        return {"error": str(exc)}
    return {
        "retired": {"id": superseded_id, "value": target["value"], "source": target["source_id"]},
        "now_current": {"id": new_id, "value": value, "source": actor},
        "because": because,
    }
