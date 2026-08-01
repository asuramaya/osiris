"""FLEET RECONCILE — the reaper's dry-run sweep (task #59, Thoth's build order DM 2006,
scoped first in decision 2a8875b0). NOTHING IN THIS MODULE MUTATES AN AGENT OR A SEAT — that
is deliberate, and it is the whole point of shipping this file before the one that acts.

THE NAME IS DELIBERATE. "Orphan reaper" already names two unrelated, already-working systems
(the B7 session-transcript death-rite detector, arq_worker.py's reap_orphans; the unbound-
but-live seat healer, seats.py's backfill_unbound_seats) — a third thing under either name
would make every future grep for "orphan" hit systems that have nothing to do with this one.
"Fleet reconcile" is the operator's own name for task #59; it stays collision-free.

THE PATTERN THIS FOLLOWS (my own scoping, 2a8875b0, finding 3): fold_candidates /
resolve_fold_candidate (folds.py) is the fleet's own proven propose-then-separately-gate
shape — a pure sweep that only proposes, and a completely separate, explicitly-gated resolve
step that is the sole caller of the real mutating verb. An agent judging its own proposals
IS the auto-merge the constitution forbids (folds.py's own docstring). This module composes
that sweep (find_agent_fold_candidates) rather than re-deriving fold detection, and adds the
one class of judgment that sweep was never built for: a mount row that isn't a fold at all,
because the project underneath it is already dead.

FOUR BUCKETS, Thoth's own language (DM 2006), each with the rule that produced it attached
to every row — never a bucket that just says "trust me":

  bulk_fold_swarm        — an anonymous mount that IS another session's own view (no
                            transcript, no daemon receipt) or an anon minted where a NAMED
                            lineage is the project's ONLY seat. High confidence (score >=
                            0.75), the "same-lineage swarm" class — several rows that are
                            really one mind.
  rollup_office_remount   — an anon mount at a seat's own OFFICE cwd, no lineage anchors
                            there directly, but the graph's charter names exactly one seat
                            for the room. High confidence charter-match — the office-cwd
                            re-mount rolling up to the seat that already owns the room.
  drop_ephemeral_test_cwd — a mount whose project is a SoftwareProject that is NOT active
                            (already retired, e.g. via retire_project — the exact "stub
                            cull" class: cc-test-target, tmp, nonexistent-probe, and
                            siblings, decision c62bf333). The project is already judged
                            dead; a straggler mount against it is residue, not a fold.
  leave_for_human          — anything that does not clear a bucket's own bar: a nuanced
                            fold proposal (score < 0.75 — several seats share the room, or
                            no soul at all anchors it), or a seatless anon in a room whose
                            charter names no seat (folds.py's own visitor-gate territory,
                            never this module's business to guess at).

ZERO FALSE DROPS is the bar (Thoth's own words: "a false drop here deletes a real agent's
registration and there is no undo in the UI"). That line was true when written and is
STALE now, corrected in place rather than left to mislead: it read as a missing button,
but the truth then was no undo ANYWHERE and no witness either — `drop_dead_project_mount`
was a bare `pool.execute`, unwitnessed, worse than irreversible (Thoth DM 2677). It is now
reversible and audited (`mounts.undrop_dead_project_mount`, keyed off the `audit_log` row
the drop itself leaves) — the bar this module holds itself to is unchanged; what changed
is that a false drop is no longer permanent. `reconcile_dry_run()` never drops, folds, or
retires anything — it is a REPORT, full stop.

PHASE 2 — `reconcile_execute()` (task #59 phase 2, Thoth's gate DM 2042) — is the acting
half, built only after Thoth read phase 1's live dry-run output against real data (the same
two-phase discipline folds.py already proved: propose, then a SEPARATE, explicitly-gated
act). It composes the SAME primitives this module has always named — fold_agent /
resolve_fold_candidate for buckets 1+2, mounts.drop_dead_project_mount (row-scoped, never
agent-id-wide) for bucket 3 — and does nothing to bucket 4, ever. DRY RUN IS ITS OWN DEFAULT
TOO (`execute=False`, unfold_agent's own convention): it returns the exact plan without
writing anything unless called with `execute=True`. And even once merged and deployed, the
SCHEDULED leg (arq_worker.fleet_reconcile_heartbeat) stays inert behind its own kill switch
(`osiris_fleet_reconcile_enabled`, default False) — flipping that flag is a second signature
a human gives separately from approving the diff, never a side effect of a deploy.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.config.settings import Settings, get_settings
from src.orchestrator.folds import find_agent_fold_candidates

# the confidence bar find_agent_fold_candidates already draws for itself, reused here rather
# than invented fresh: 0.75 is its own single-seat presumption (one soul, no ambiguity); 0.55
# is its own "nuanced, verify by hand" tier for a multi-seat room. Bulk-act never crosses
# below the sweep's OWN line for "verify by hand."
_HIGH_CONFIDENCE = 0.75

# each swarm/charter class the sweep can propose, and which of Thoth's own bucket names it
# earns AT high confidence. A class not listed here (there are only these three) has no
# bulk-act bucket at all and always leaves for a human, regardless of score.
_BUCKET_BY_CLASS = {
    "view-alias": "bulk_fold_swarm",
    "restart-mint": "bulk_fold_swarm",
    "charter-match": "rollup_office_remount",
}


async def _dead_project_mounts(pool: asyncpg.Pool) -> list[dict[str, Any]]:
    """Mount rows whose project is a SoftwareProject that is no longer active — the
    residue class retire_project's own refusal (mounts seen in the last 15 minutes block
    a retirement) cannot itself clean up after the fact: a project retired cleanly can
    still have STALE rows from before the 15-minute window, or a new anon mount can land
    against it after retirement (nothing stops a claude session from launching into a
    stub's old cwd). Read-only join against objects.status, which retire_project already
    set — this reuses that verdict rather than inventing a second one."""
    rows = await pool.fetch(
        "SELECT m.agent_id, m.project, m.cwd, m.job_dir, m.last_seen, "
        "       p.status AS project_status "
        "FROM agent_mounts m "
        "JOIN objects p ON p.type='SoftwareProject' AND p.canonical = 'repo:' || m.project "
        "WHERE p.status <> 'active' "
        "ORDER BY m.last_seen DESC NULLS LAST"
    )
    return [
        {
            "agent_id": r["agent_id"],
            "project": r["project"],
            "cwd": r["cwd"],
            "job_dir": r["job_dir"],
            "last_seen": r["last_seen"].isoformat() if r["last_seen"] else None,
            "bucket": "drop_ephemeral_test_cwd",
            "rule": f"project {r['project']!r} is SoftwareProject status={r['project_status']!r} "
                    "(already retired) — a mount here is residue, not a fold candidate",
        }
        for r in rows
    ]


async def reconcile_dry_run(
    pool: asyncpg.Pool, *, projects_root: Path | None = None, jobs_home: Path | None = None,
) -> dict[str, Any]:
    """THE REPORT Thoth asked to see before anything acts. Buckets every row currently
    reachable — the fold-candidate tray (refreshed by calling find_agent_fold_candidates,
    itself proposal-only and idempotent) plus the dead-project residue class the tray was
    never built to see — and names the rule that placed each one. Writes nothing except
    what find_agent_fold_candidates itself already writes (merge_candidates PROPOSAL rows
    — review-gated, never executed here). `projects_root`/`jobs_home` pass straight through
    to the sweep — the same injection seam its own tests already use, not a new one."""
    swept = await find_agent_fold_candidates(
        pool, projects_root=projects_root, jobs_home=jobs_home)
    buckets: dict[str, list[dict[str, Any]]] = {
        "bulk_fold_swarm": [], "rollup_office_remount": [],
        "drop_ephemeral_test_cwd": [], "leave_for_human": [],
    }
    for c in swept["pending"]:
        cls = str(c.get("class") or "")
        score = float(c.get("score") or 0.0)
        target = _BUCKET_BY_CLASS.get(cls)
        row = {
            "candidate_id": c["id"], "dupe": c.get("dupe"), "into": c.get("into_label"),
            "class": cls, "score": score, "signals": c.get("signals"),
        }
        if target and score >= _HIGH_CONFIDENCE:
            row["bucket"] = target
            row["rule"] = f"{cls} score {score} >= {_HIGH_CONFIDENCE} — the sweep's own " \
                           "single-seat/no-body confidence bar"
            buckets[target].append(row)
        else:
            row["bucket"] = "leave_for_human"
            row["rule"] = (
                f"{cls} score {score} < {_HIGH_CONFIDENCE} — the sweep's own 'nuanced, "
                "verify by hand' tier" if target else
                f"class {cls!r} has no bulk-act bucket — always a human's call"
            )
            buckets["leave_for_human"].append(row)

    dead_projects: set[str] = set()
    for row in await _dead_project_mounts(pool):
        buckets["drop_ephemeral_test_cwd"].append(row)
        dead_projects.add(str(row["project"]))

    for project, n in (swept.get("seatless") or {}).items():
        # a project already caught above (its own SoftwareProject is retired) gets ONE
        # verdict, not two — "drop, the project is dead" is more specific than "seatless,
        # ask a human" and supersedes it, rather than reporting the same row twice under
        # different bucket names.
        if project in dead_projects:
            continue
        buckets["leave_for_human"].append({
            "project": project, "count": n, "bucket": "leave_for_human",
            "rule": f"{n} seatless anon(s) in a room whose charter names no seat — "
                    "folds.py's own visitor-gate territory, never this module's call",
        })

    counts = {k: len(v) for k, v in buckets.items()}
    return {
        "buckets": buckets, "counts": counts, "total": sum(counts.values()),
        "examined": swept.get("examined", 0),
        "note": "REPORT ONLY — nothing folded, dropped, or retired. Every row above names "
                "its own bucket and the rule that put it there.",
    }


async def reconcile_execute(
    actions: Actions, *, actor: str, projects_root: Path | None = None,
    jobs_home: Path | None = None, execute: bool = False,
) -> dict[str, Any]:
    """THE ACTING HALF (task #59 phase 2, Thoth's gate DM 2042). DRY RUN IS THE DEFAULT
    (`execute=False`, `unfold_agent`'s own convention, folds.py): returns the exact plan —
    which candidates would be folded, which mount rows would be dropped, how many rows sit
    in leave_for_human untouched — without writing anything. `execute=True` performs it.

    Re-reads the tray itself via `reconcile_dry_run` (never trusts a caller-supplied stale
    report) — the plan and the act must see the same instant, not a report gathered a query
    or a deploy ago.

    bulk_fold_swarm + rollup_office_remount: `resolve_fold_candidate(decision='merged')`
    per candidate — the SAME estate-carrying fold folds.py already proves (mail, mount
    rows, thread ownership all move with it), never a bare kernel merge.

    drop_ephemeral_test_cwd: `mounts.drop_dead_project_mount` per row, scoped by
    (job_dir, project) — a row-scoped delete, never agent-id-wide (`release_mounts`' own
    lesson, the g40-v/vi false-succession incident).

    leave_for_human: NEVER acted on, by construction — not filtered out, not deferred,
    simply absent from every write this function performs. Thoth's own words: "a reaper
    that never punts is a reaper that will eventually eat something real."

    A single row's fold or drop failing (a race, an already-resolved candidate) is caught
    and reported inline rather than aborting the batch — ZERO FALSE DROPS means every row
    that WAS acted on must be a true positive, not that one failure may silently swallow
    the rest of a correct plan.

    POST-ACT VERIFICATION (`execute=True` only): re-reads the tray a SECOND time after
    acting and reports before/after counts — proof the acted rows actually left the tray,
    never a trusted return value from the fold/drop calls alone."""
    from src.orchestrator.folds import resolve_fold_candidate
    from src.orchestrator.mounts import drop_dead_project_mount

    report = await reconcile_dry_run(actions.pool, projects_root=projects_root,
                                     jobs_home=jobs_home)
    would_fold = [
        {"candidate_id": row["candidate_id"], "dupe": row["dupe"], "into": row["into"],
         "bucket": bucket}
        for bucket in ("bulk_fold_swarm", "rollup_office_remount")
        for row in report["buckets"][bucket]
    ]
    would_drop = [
        {"agent_id": row["agent_id"], "project": row["project"], "job_dir": row["job_dir"]}
        for row in report["buckets"]["drop_ephemeral_test_cwd"]
    ]
    plan: dict[str, Any] = {
        "would_fold": would_fold, "would_drop": would_drop,
        "left_for_human": len(report["buckets"]["leave_for_human"]),
        "execute": execute,
    }
    if not execute:
        plan["note"] = "PLAN ONLY — call with execute=True to write. Nothing touched."
        return plan

    folded, drops = [], []
    for item in would_fold:
        try:
            out = await resolve_fold_candidate(
                actions, candidate_id=item["candidate_id"], decision="merged", actor=actor)
        except Exception as exc:  # one bad row must not abort a correct plan
            out = {"error": f"{type(exc).__name__}: {exc}"}
        folded.append({**item, "result": out})
    for item in would_drop:
        try:
            out = await drop_dead_project_mount(
                actions, job_dir=item["job_dir"], project=item["project"], actor=actor)
        except Exception as exc:
            drops.append({**item, "error": f"{type(exc).__name__}: {exc}"})
            continue
        drops.append({**item, "rows_deleted": out["dropped"], "audit_id": out["audit_id"]})

    after = await reconcile_dry_run(actions.pool, projects_root=projects_root,
                                    jobs_home=jobs_home)
    plan.update({
        "folded": folded, "dropped": drops,
        "before_counts": report["counts"], "after_counts": after["counts"],
        "note": "EXECUTED — before/after counts prove the acted rows left the tray; "
                "leave_for_human rows were never touched.",
    })
    return plan


async def reconcile_scheduled_tick(
    actions: Actions, *, settings: Settings | None = None,
    projects_root: Path | None = None, jobs_home: Path | None = None,
) -> dict[str, Any]:
    """THE SCHEDULED LEG's own tick — `arq_worker.fleet_reconcile_heartbeat` calls this
    unconditionally, the same thin-shim shape `trigger_mail`/`pit_watch_heartbeat` already
    use (real logic and the flag gate live in the orchestrator tick, not the cron wrapper).

    OFF unless `osiris_fleet_reconcile_enabled` — the kill switch (Thoth's gate DM 2042):
    the code ships inert, and flipping this flag is a SECOND signature a human gives
    separately from approving the diff, never a side effect of deploying it. When on,
    composes `reconcile_execute(execute=True)` — the exact same acting verb reachable by
    hand, so the schedule and a human's own manual call are provably the same path, never
    two implementations that could drift.

    `settings` is the injected test seam (`trigger_mail_tick`'s own convention, `st =
    settings or get_settings()`) so a test can flip the flag without touching the real
    environment or monkeypatching `get_settings`."""
    st = settings or get_settings()
    if not st.osiris_fleet_reconcile_enabled:
        return {"enabled": False, "folded": [], "dropped": [],
                "note": "the reaper's scheduled leg is dark "
                        "(osiris_fleet_reconcile_enabled=0)"}
    out = await reconcile_execute(actions, actor="cron:fleet_reconcile_heartbeat",
                                  execute=True, projects_root=projects_root,
                                  jobs_home=jobs_home)
    return {"enabled": True, **out}
