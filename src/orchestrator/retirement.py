"""retire_assertion: the cross-source supersede. assert_property's own supersession is
scoped to the same source only ("other sources' values coexist as the multi-source set"), by
design, for legitimate multi-source corroboration. It leaves exactly one class unreachable: a
peer's correction of another agent's bad self-declaration can never retire it. A real
correction call on an agent's house field proved this live: the right value ("58") landed
from a different source, the wrong one ("2", self-declared) stayed current, both
simultaneously "current" per current_assertions' own definition (every row nothing else's
supersedes points at), so a reader without an exact ORDER BY confidence DESC, observed_at
DESC LIMIT 1 could still surface the wrong one.

Deliberately narrow, not a general edit/delete escape hatch: it retires one named
assertion, by id, on a caller-named (object, name), never a bare "whatever's current now",
so a caller must already know exactly which row is wrong (from a diagnosis, never a guess).
`because` is required: a cross-source retirement crosses accountability lines, so the
justification is not optional, the same way assert_property's own routine supersession isn't."""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.actions.core import ActionError, Actions
from src.orchestrator.compositions import resolve_ref
from src.orchestrator.graph_layout import GRAPH_LAYOUT_SOURCE
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for


async def list_assertions(actions: Actions, *, ref: str, name: str) -> dict[str, Any]:
    """Read-only (closing an earlier gap): retire_assertion needs a `superseded_id`, an
    assertions.id, and until this addition nothing exposed one. dossier()/trace_evidence()
    both resolve through current_assertions to a single belief-winner or a bare value list;
    neither ever surfaced the row id underneath. This is the smallest possible addition:
    every current (non-superseded) assertion of `name` on the object `ref` resolves to, each
    carrying its own `id`, exactly what retire_assertion's own required argument needs,
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
    """The read path for a previously missing gap: no read path existed for
    assertions/supersedes through the composer, which pushed callers toward hand-written
    SQL. This surfaces every row where `is_current=true` (migration 0047's maintained flag)
    yet a real `supersedes` foreign key already points at it from another assertion, the
    exact anomaly a specific live case surfaced: current_assertions kept listing a row as
    current that a genuine successor had already superseded, because the flip (assert_
    property's own same-source path, or supersede_assertion's cross-source one, both flip
    `is_current` in the same transaction as the insert, per 0047's own design) never landed
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
    """The backfill for the same kernel gap `stale_current_flags` measures, the compensating
    fix for exactly the population it reports (123,914 of 267,305 rows at last count):
    `assertions.is_current` is a maintained materialization of the append-only kernel
    (migration 0047), not itself a kernel fact; flipping it here heals the projection,
    touches no assertion's own content, and violates no constitutional constraint on the
    kernel.

    `dry_run=True` (default, list-only): names how many rows would flip and their ids,
    writes nothing. `dry_run=False` is a deliberate operator call, never automatic: flips
    `is_current=false` on up to `limit` stale rows in one batched UPDATE, oldest-observed
    first. Batched because the live population is five figures; a single UPDATE touching all
    of it at once is not the shape of a repair anyone should run unattended. Idempotent: a
    repeat call only ever sees rows still stale, a row already flipped drops out of the
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
    resolve_ref accepts: UUID, short-id, canonical, or name), asserting `value` as the new
    current fact from `actor`. Refuses loudly (an error dict, nothing written) when:
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


async def retire_link(
    actions: Actions, *, from_ref: str, to_ref: str, link_type: str, because: str, actor: str,
) -> dict[str, Any]:
    """The missing verb: three independent live cases motivated this (a fuzzy-substring
    `resolves=` mis-citation, a `resolves=` mis-fire that closed the wrong Thread, and the
    original case that opened this gap). `retire_assertion` above covers the
    property-assertion half of "retract a wrongly-minted X" (confirmed already general to
    any property, not just `name`); it has no path to a link at all. `Actions.invalidate_link`
    already exists at the kernel level, event-sourced (an audit row plus an outbox
    `link_invalidated` event), idempotent, never a delete (`valid_until` stamped, the row
    stays exactly where it was created, in whose name, and why), but reaching it directly
    from a caller would be exactly the raw-mutation shortcut this project's rules forbid.
    This is that path: agent/thread-agnostic (any (from, to, type) triple, on any object
    types; the per-type paths, thread(action='resolve')'s own `resolved_by` edges,
    record_decision's own answers/grounded_by, etc., keep working unchanged, this is the
    general escape hatch for when those mint the wrong edge).

    `because` is required, the same rule `retire_assertion` already holds itself to; this
    now rides in `invalidate_link`'s own audit/outbox payload, the compensating event
    itself, never a second write nobody derives from the first. Refuses loudly (an
    error dict, nothing written) when: `because` is blank; `from_ref`/`to_ref` doesn't
    resolve; the triple has no currently-active link of the named type to retire
    (idempotent from `invalidate_link`'s own side, but a caller here almost certainly
    meant a real edge; silently returning success on a no-op would hide a typo'd
    ref/type the same way a silent drop would)."""
    because = (because or "").strip()
    if not because:
        return {"error": "because is required — retiring a link crosses the same "
                         "accountability line retire_assertion's own because already does"}
    link_type = (link_type or "").strip()
    if not link_type:
        return {"error": "link_type is required"}
    pool = actions.pool
    from_id = await resolve_ref(pool, from_ref)
    if from_id is None:
        return {"error": f"no object matches from_ref={from_ref!r}"}
    to_id = await resolve_ref(pool, to_ref)
    if to_id is None:
        return {"error": f"no object matches to_ref={to_ref!r}"}
    now = datetime.now(UTC)
    n = await actions.invalidate_link(from_id, to_id, link_type, actor, now, reason=because)
    if n == 0:
        return {"error": f"no currently-active {link_type!r} link from {from_ref!r} to "
                         f"{to_ref!r} — nothing to retire (check the refs and the type)"}
    return {"retired": {"from": from_ref, "to": to_ref, "type": link_type, "count": n},
           "because": because}


async def retire_bare_object(
    actions: Actions, *, ref: str, because: str, actor: str,
) -> dict[str, Any]:
    """The tooling gap, opened on a set of debug-script artifacts: retire_object(kind=...)
    covers only seat/project/agent, and no path existed for an arbitrary active object of
    no other kind, the exact shape a stray script or a mis-minted stub leaves behind. `ref`
    resolves via the same resolve_ref every other generic path here uses (UUID, short-id,
    canonical, or name), any object type, not scoped like retire_project's own
    SoftwareProject-only resolution.

    Refuses loudly (an error dict, nothing written) when: `because` is blank; `ref`
    doesn't resolve; the object is already non-active; any live link touches it in
    either direction (a bare object is one nothing else references and that
    references nothing; a live edge is direct evidence something still depends on
    it, the same signal retire_project's own "any open Thread pointing in" check
    looks for, generalized here to any link/any direction since a truly bare object has
    none at all); or it carries a current assertion from any source other than the
    layout heartbeat's own GRAPH_LAYOUT_SOURCE. That one exemption is deliberate,
    not an oversight: graph_x/graph_y/graph_layout_v are bookkeeping the heartbeat
    stamps on every active object regardless of meaning; this path's own founding
    cases carry nothing else, so refusing on them would make this path unable to ever
    retire the exact objects it exists for. Any other source (a name, a summary, a real
    property) is genuine evidence of content and refuses, the same rule retire_project
    already holds for commits and open threads.

    Same compensating-event mechanism as retire_project (`Actions.set_status`), never
    a delete."""
    because = (because or "").strip()
    if not because:
        return {"error": "because is required — retiring a bare object is a "
                         "deliberate act on the record"}
    pool = actions.pool
    object_id = await resolve_ref(pool, ref)
    if object_id is None:
        return {"error": f"no object matches {ref!r}"}
    row = await pool.fetchrow(
        "SELECT id, type, canonical, status FROM objects WHERE id=$1", object_id)
    if row is None:
        return {"error": f"no object matches {ref!r}"}
    if row["status"] != "active":
        return {"error": f"{row['canonical']} is already {row['status']} — nothing to "
                         "retire"}
    live_links = await pool.fetchval(
        "SELECT count(*) FROM links WHERE (from_id=$1 OR to_id=$1) "
        "AND (valid_until IS NULL OR valid_until > now())", object_id)
    if live_links:
        return {"error": f"{row['canonical']} has {live_links} live link(s) touching it "
                         "— live signal, retire_bare_object refuses"}
    other_sources = await pool.fetchval(
        "SELECT count(*) FROM current_assertions WHERE object_id=$1 AND source_id <> $2",
        object_id, GRAPH_LAYOUT_SOURCE)
    if other_sources:
        return {"error": f"{row['canonical']} carries {other_sources} assertion(s) from a "
                         "non-layout source — real evidence of content, retire_bare_object "
                         "refuses"}
    await actions.set_status(object_id, "retired", because, actor)
    return {"retired_object": row["canonical"], "id": str(object_id)[:8],
           "type": row["type"], "because": because}
