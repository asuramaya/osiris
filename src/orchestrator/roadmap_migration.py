"""One-time migration of a harness TaskList board into the graph's own Decision/Thread
objects (decision d83804c8, ruling 50c3ed90, Thoth's dispatch msg 4408). DRY-RUN BY
DEFAULT — `plan_migration` performs zero writes; `apply_migration` exists, is gated, and
is never called by this module's own `__main__` block. Applying is a separate, explicit,
future authorization (msg 4408: "nothing applied").

SCOPE: exactly one board. The four-store divergence premise that would have made this a
cross-store reconciler was measured and retracted (ruling 50c3ed90 supersedes 5273e0f3) —
there is nothing cross-store to identify or merge. `plan_migration` takes rows already
shaped like the harness tool's own JSON ({"id","subject","description","status","blocks",
"blockedBy"}) plus the `store` they came from — the same "caller supplies the data"
discipline task_sync.py already established; this module never touches ~/.claude/tasks.

WHY COMPLETED ROWS BECOME DECISIONS, NOT THREADS (decision d83804c8, Q2): 133 of 153 rows
on the one board measured are `completed`, median 1338 description chars, up to 4493 — a
decision log that has been living in a task list, not a list of open duties. Minting 133
pre-resolved Thread(kind='obligation') objects would misrepresent settled history as live
work on every future roadmap/orient() read. Only non-completed rows are Thread-shaped.
"""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.actions.core import Actions
from src.orchestrator.capture import ARCS, open_thread, reclassify_thread, record_decision
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

_SOURCE = "roadmap_migration"
_LEGACY_REF_PROPERTY = "legacy_task_ref"
_LEGACY_DEPS_PROPERTY = "legacy_task_dependencies"

# Hand-classified against capture.ARCS (decision d83804c8/msg 4408, "honest empty over
# confident wrong") by reading each of the one board's 20 live (non-completed) rows' own
# subject+description — not keyword-inferred. Genuinely dual-fit or off-taxonomy rows are
# DELIBERATELY ABSENT: an id with no entry here mints arc-less, exactly like Sekhmet's own
# unsorted rate on her sample. Completed rows (-> Decisions) never consult this map; ARCS
# is a Thread-only taxonomy.
ARC_BY_TASK_ID: dict[str, str] = {
    "30": "Security",                 # public-repo PII/blob-content sweep before push
    "41": "Fleet-Hygiene",             # reaping sweep, blind-tree walk
    "51": "Compaction-Resilience",     # soul store: transcripts resumable on any host
    "71": "Surfaces-Roadmap-Docs",     # UI convergence blocked by ledger rot
    "93": "Surfaces-Roadmap-Docs",     # project index as front door
    "108": "Fleet-Hygiene",            # cleanup-mechanism autonomy ruling
    "113": "Fleet-Hygiene",            # manager-authored-code flag, fleet-wide
    "133": "Fleet-Hygiene",            # pre-commit gate hook enforcement
    "135": "Surfaces-Roadmap-Docs",    # operator-side CLI surface
    "136": "Compaction-Resilience",    # resume broken by worktree binding
    "144": "Identity-Succession",      # agent identity ranked-evidence resolver
    "145": "Identity-Succession",      # coordinator handoff / identity_coherence
    "150": "Identity-Succession",      # lineage_root walk-completeness (this reign)
    "157": "Identity-Succession",      # seat charter declarations
    "168": "Surfaces-Roadmap-Docs",    # held-work branch/files_touched surface
}
# Left deliberately unset, checked and genuinely ambiguous or off-taxonomy on read:
# 40 (adversary licence sampling), 75 (miners redesign), 95 (case/lens cleanup, dual-fit
# with #93), 148 (harness/osiris boundary), 161 (pin schema, dual-fit identity/model).

for _arc in ARC_BY_TASK_ID.values():
    assert _arc in ARCS, f"{_arc!r} is not in capture.ARCS — the taxonomy is closed"


def _legacy_ref(task_id: str, store: str) -> dict[str, str]:
    return {"store": store, "id": task_id}


def _dependencies(task: dict[str, Any]) -> dict[str, list[str]] | None:
    blocks = task.get("blocks") or []
    blocked_by = task.get("blockedBy") or []
    if not blocks and not blocked_by:
        return None
    return {"blocks": list(blocks), "blockedBy": list(blocked_by)}


def plan_migration(tasks: list[dict[str, Any]], *, store: str) -> dict[str, Any]:
    """PURE. No DB, no writes. Splits `tasks` into a Decision plan (completed rows) and a
    Thread plan (pending/in_progress rows), each carrying its own legacy_task_ref and,
    where present, a legacy_task_dependencies payload (decision d83804c8, Q3a: the
    blocks/blockedBy DAG is preserved as inert data, no verb reads it yet, named not
    invented). Every plan entry is independently applicable; nothing here decides ORDER.

    Returns {"decisions": [...], "threads": [...], "counts": {...}} — counts include
    `unsorted` (Thread entries with no arc, honest per ARC_BY_TASK_ID's own docstring) so a
    caller never has to re-derive the aggregate line from the row lists."""
    decisions: list[dict[str, Any]] = []
    threads: list[dict[str, Any]] = []
    for task in tasks:
        legacy_ref = _legacy_ref(task["id"], store)
        deps = _dependencies(task)
        if task.get("status") == "completed":
            decisions.append({
                "task_id": task["id"],
                "summary": task["subject"],
                "rationale": task.get("description") or "",
                "legacy_task_ref": legacy_ref,
                "legacy_task_dependencies": deps,
            })
        else:
            arc = ARC_BY_TASK_ID.get(task["id"])
            body = task.get("description") or ""
            summary = task["subject"] if not body else f"{task['subject']}\n\n{body}"
            threads.append({
                "task_id": task["id"],
                "summary": summary,
                "arc": arc,
                "legacy_task_ref": legacy_ref,
                "legacy_task_dependencies": deps,
                "harness_status": task.get("status"),
            })
    unsorted = sum(1 for t in threads if t["arc"] is None)
    return {
        "decisions": decisions,
        "threads": threads,
        "counts": {
            "decisions": len(decisions),
            "threads": len(threads),
            "unsorted": unsorted,
            "total": len(decisions) + len(threads),
        },
    }


def aggregate_line(counts: dict[str, int]) -> str:
    """One line, never per-row noise (Khnum's batch-volume rule, msg 4408): "migrated N,
    M unsorted (X%)" — same shape as _fn_roadmap_open's "N more not shown". This is the
    PLAN-level line (before any write); see `run_aggregate_line` for the line a real
    `apply_migration` run emits, which adds the read-back-repair count."""
    total = counts["total"]
    pct = (100 * counts["unsorted"] / counts["threads"]) if counts["threads"] else 0.0
    return (f"migrated {total} ({counts['decisions']} decisions, {counts['threads']} "
            f"threads), {counts['unsorted']} unsorted ({pct:.0f}% of threads)")


def run_aggregate_line(counts: dict[str, int], results: list[dict[str, Any]]) -> str:
    """One line for an actual `apply_migration` run (msg 4429): plan_migration's own
    aggregate_line plus K, the count of times the open_thread dedup-discard signature
    (42176e16) actually fired and had to be repaired via reclassify_thread — the ONLY
    place this is ever observable, per Thoth's own instruction, so it must be measured
    every run rather than assumed rare from this one design pass."""
    repairs = sum(1 for r in results if r.get("arc_backfilled"))
    return f"{aggregate_line(counts)}, {repairs} read-back repairs"


async def apply_migration(
    actions: Actions, plan: dict[str, Any], *, repo: str = "osiris",
) -> dict[str, Any]:
    """NEVER CALLED BY THIS MODULE'S OWN __main__ — a separate, explicit, future
    authorization (msg 4408: "nothing applied"). Executes `plan_migration`'s output for
    real: one record_decision per decision entry, one open_thread per thread entry, then
    legacy_task_ref/legacy_task_dependencies stamped via assert_property on whichever id
    resulted.

    READS BACK EVERY THREAD MINT, NEVER TRUSTS THE RECEIPT (Sekhmet's live finding, named
    in msg 4408): open_thread's near-duplicate collision path returns the EXISTING thread's
    id with `deduped: "true"` and silently DISCARDS this call's own `arc` — 17 stamps
    attempted, 17 successful-looking receipts, zero landed, caught only by re-counting.
    After each open_thread call this function re-reads that thread's OWN current `arc`
    property directly; if it doesn't match what was requested (the dedup-discard signature),
    it re-applies the arc through `reclassify_thread` (the merged backfill door, msg 4408)
    instead of trusting the first call's return value.

    Returns one row per plan entry: {"task_id", "target": "Decision"|"Thread", "id",
    "deduped": bool, "arc_backfilled": bool} — the per-row detail exists for a caller who
    wants it, but `aggregate_line` is the one meant to reach a human."""
    conf = confidence_for(EvidenceClass.SELF_DECLARED)
    observed = datetime.now(UTC)
    results: list[dict[str, Any]] = []

    for entry in plan["decisions"]:
        did = await record_decision(
            actions, entry["summary"], kind="decision", rationale=entry["rationale"],
            repo=repo, source=_SOURCE,
        )
        await actions.assert_property(
            did, _LEGACY_REF_PROPERTY, entry["legacy_task_ref"], _SOURCE, observed, conf,
            evidence_class=EvidenceClass.SELF_DECLARED.value,
        )
        if entry["legacy_task_dependencies"]:
            await actions.assert_property(
                did, _LEGACY_DEPS_PROPERTY, entry["legacy_task_dependencies"], _SOURCE,
                observed, conf, evidence_class=EvidenceClass.SELF_DECLARED.value,
            )
        results.append({"task_id": entry["task_id"], "target": "Decision", "id": str(did),
                        "deduped": False, "arc_backfilled": False})

    for entry in plan["threads"]:
        tid = await open_thread(
            actions, entry["summary"], repo=repo, kind="obligation", arc=entry["arc"],
            source=_SOURCE,
        )
        row = await actions.pool.fetchrow(
            "SELECT a.value #>> '{}' AS arc FROM current_assertions a "
            "WHERE a.object_id=$1 AND a.name='arc' "
            "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", tid,
        )
        landed_arc = row["arc"] if row else None
        deduped = entry["arc"] is not None and landed_arc != entry["arc"]
        arc_backfilled = False
        if deduped:
            await reclassify_thread(actions, str(tid), kind="obligation",
                                    arc=entry["arc"], source=_SOURCE)
            arc_backfilled = True
        await actions.assert_property(
            tid, _LEGACY_REF_PROPERTY, entry["legacy_task_ref"], _SOURCE, observed, conf,
            evidence_class=EvidenceClass.SELF_DECLARED.value,
        )
        if entry["legacy_task_dependencies"]:
            await actions.assert_property(
                tid, _LEGACY_DEPS_PROPERTY, entry["legacy_task_dependencies"], _SOURCE,
                observed, conf, evidence_class=EvidenceClass.SELF_DECLARED.value,
            )
        results.append({"task_id": entry["task_id"], "target": "Thread", "id": str(tid),
                        "deduped": deduped, "arc_backfilled": arc_backfilled})

    return {"rows": results, "aggregate": run_aggregate_line(plan["counts"], results)}
