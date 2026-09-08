"""PROJECT HYGIENE SWEEP (thread 14fae7d3, wave 6 dispatch msg 8063): classification_laws_
heartbeat's own sibling rule, SoftwareProject junk instead of Thread classification -- a
stub minted by a test run or a stale disk census, claimed by no commit, no open Thread, no
Decision, and no governing Seat, is dead weight for the exact reason an unclaimed derived
Thread is (migration_0060.py's own EXPIRY law). Two doors feed one shared guard:

  (1) THE ONGOING MECHANICAL RULE, forever: an active SoftwareProject minted (the earliest
      object_events row, event_type='create', its own `actor` column) under the literal
      actor 'test' -- the convention test fixtures should use per this thread's own mandate
      -- OR minted by 'disk-census' whose own `on_disk_path` property no longer exists on
      THIS host's filesystem RIGHT NOW (Path.exists(), checked live -- never trusted from a
      stale snapshot).
  (2) THE ONE-SHOT LEGACY BACKLOG: named canonicals minted by live acceptance/probe runs
      against the shared dev graph BEFORE the source='test' convention existed, so their own
      create-actor is whatever agent happened to run the probe, not a reusable signal --
      dated and named explicitly here rather than re-derived by a fuzzy actor-string
      heuristic. Wave 6's dispatch (msg 8063) named ~25 candidates; measured live against
      the graph 2026-09-08, three of those are deliberately EXCLUDED and reported rather
      than forced through:
        - repo:khnum-launch-acceptance-4 carries an active Decision in_repo (itself likely
          another same-run test artifact, but this sweep never guesses past its own guard)
        - repo:realrepo carries two active `governs` edges
        - repo:deepseek-harness's own on_disk_path (/home/asuramaya/code/dsh/deepseek-
          harness) is STILL PRESENT on disk -- not stale, contradicting the dispatch's own
          characterization of it as disk-census-gone
      repo:dbghusk/repo:dbgsurv/repo:realrepo already match door (1) (actor='test' at
      creation) and are not repeated in this set.

BOTH doors share ONE guard before `retire_project` is ever called: no active Thread, no
Decision (any status), no active `governs` edge pointing in -- on top of retire_project's own
commit/open-thread/live-mount refusals, which stay the final safety net (a row that clears
this sweep's guard but still fails there is reported, never raised). Compensating retire
only (`Actions.set_status` via `retire_project`), never a DELETE; each retirement named in
the return receipt.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import asyncpg

from src.actions.core import Actions

MIGRATION_SOURCE = "hygiene:project_sweep"
_BECAUSE = "project hygiene sweep (thread 14fae7d3): unclaimed test/disk-census stub"

# One-shot legacy backlog -- see the module docstring for the three named exceptions this
# sweep deliberately leaves untouched (khnum-launch-acceptance-4, realrepo, deepseek-harness).
LEGACY_JUNK_PROJECT_CANONICALS = frozenset({
    "repo:evalab-scratch-_7w7gkze", "repo:evalab-scratch-8dckvw76",
    "repo:evalab-scratch-92iume01", "repo:evalab-scratch-tpqsxr60",
    "repo:evalab-scratch-uygj0juq", "repo:evalab-scratch-vittabnw",
    "repo:evalab-scratch-zn9u_by9",
    "repo:khnum-launch-acceptance-2", "repo:khnum-launch-acceptance-3",
    "repo:khnum-launch-acceptance-test",
    "repo:probe-clean-workdir", "repo:probe-dangling-workdir", "repo:injection-probe-workdir",
    "repo:rA", "repo:rB",
    "repo:imhotepa14faccept", "repo:toolcall-source", "repo:operator",
})

_CANDIDATES_SQL = """
    SELECT o.id, o.canonical,
        (SELECT oe.actor FROM object_events oe WHERE oe.object_id=o.id
         AND oe.event_type='create' ORDER BY oe.id ASC LIMIT 1) AS create_actor,
        (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id
         AND a.name='on_disk_path' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1)
        AS on_disk_path,
        (SELECT count(*) FROM links l JOIN objects t ON t.id=l.from_id
         WHERE l.to_id=o.id AND l.type='in_repo' AND t.type='Thread' AND t.status='active'
         AND (l.valid_until IS NULL OR l.valid_until > now())) AS n_active_threads,
        (SELECT count(*) FROM links l JOIN objects d ON d.id=l.from_id
         WHERE l.to_id=o.id AND l.type='in_repo' AND d.type='Decision'
         AND (l.valid_until IS NULL OR l.valid_until > now())) AS n_decisions,
        (SELECT count(*) FROM links l JOIN objects s ON s.id=l.from_id
         WHERE l.to_id=o.id AND l.type='governs' AND s.type='Seat'
         AND (l.valid_until IS NULL OR l.valid_until > now())) AS n_governing_seats
    FROM objects o
    WHERE o.type='SoftwareProject' AND o.status='active'
"""


def _matches_ongoing_rule(row: asyncpg.Record) -> bool:
    if row["create_actor"] == "test":
        return True
    if row["create_actor"] == "disk-census" and row["on_disk_path"]:
        return not Path(row["on_disk_path"]).exists()
    return False


async def plan_project_hygiene_sweep(pool: asyncpg.Pool) -> dict[str, Any]:
    """DRY RUN -- never writes. Every active SoftwareProject checked against both doors;
    the shared guard (no thread/decision/governing seat) applies identically regardless of
    which door matched, so a legacy-named row with an incidental decision/governs edge is
    excluded exactly like an ongoing-rule row would be -- never a special case."""
    rows = await pool.fetch(_CANDIDATES_SQL)
    to_retire: list[dict[str, Any]] = []
    guarded: list[dict[str, Any]] = []
    for row in rows:
        canonical = row["canonical"]
        if not (_matches_ongoing_rule(row) or canonical in LEGACY_JUNK_PROJECT_CANONICALS):
            continue
        if row["n_active_threads"] or row["n_decisions"] or row["n_governing_seats"]:
            guarded.append({
                "project": canonical, "n_active_threads": row["n_active_threads"],
                "n_decisions": row["n_decisions"], "n_governing_seats": row["n_governing_seats"],
            })
            continue
        to_retire.append({"id": row["id"], "project": canonical})
    return {"to_retire": to_retire, "guarded": guarded, "projects_scanned": len(rows)}


async def apply_project_hygiene_sweep(
    actions: Actions, *, actor: str = MIGRATION_SOURCE,
) -> dict[str, Any]:
    """Applies `plan_project_hygiene_sweep`'s plan through the sanctioned `retire_project`
    door -- never a hand-written status flip. `retire_project` re-checks commits/open-
    threads/live-mount itself at call time (the real safety net); a row that clears this
    sweep's own guard but still fails there is reported, not raised, so one stale row never
    sinks the whole sweep. Idempotent: an already-retired row simply isn't selected on the
    next scan (status='active' is this sweep's own population filter)."""
    from src.orchestrator.projects import retire_project

    plan = await plan_project_hygiene_sweep(actions.pool)
    retired: list[str] = []
    refused: list[dict[str, str]] = []
    for entry in plan["to_retire"]:
        result = await retire_project(
            actions, project=entry["project"], actor=actor, because=_BECAUSE)
        if "error" in result:
            refused.append({"project": entry["project"], "error": result["error"]})
        else:
            retired.append(entry["project"])
    return {
        "retired": retired, "refused": refused, "guarded": plan["guarded"],
        "projects_scanned": plan["projects_scanned"],
    }
