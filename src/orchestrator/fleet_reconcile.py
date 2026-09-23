"""Fleet reconcile: a dry-run sweep that reports on stale or duplicate agent mounts
without mutating any agent or seat. That restriction is deliberate: this module ships
before the module that acts, so the reporting logic can be reviewed against real data
first.

The name is deliberate too. "Orphan reaper" already names two unrelated, already-working
systems (a session-transcript death-rite detector, and an unbound-but-live seat healer) -
reusing either name for a third, unrelated thing would make every future search for
"orphan" hit systems that have nothing to do with this one. "Fleet reconcile" stays
collision-free.

The pattern this follows: fold_candidates / resolve_fold_candidate (folds.py) is the
fleet's own proven propose-then-separately-gate shape - a pure sweep that only proposes,
and a completely separate, explicitly-gated resolve step that is the sole caller of the
real mutating action. An agent judging its own proposals is the auto-merge the system's
rules forbid (folds.py's own docstring makes this point). This module composes that sweep
(find_agent_fold_candidates) rather than re-deriving fold detection, and adds the one
class of judgment that sweep was never built for: a mount row that isn't a fold at all,
because the project underneath it is already dead.

Four buckets, each with the rule that produced it attached to every row, never a bucket
that just asserts trust:

  bulk_fold_swarm        - an anonymous mount that IS another session's own view (no
                            transcript, no daemon result) or an anon minted where a NAMED
                            lineage is the project's ONLY seat. High confidence (score >=
                            0.75), the "same-lineage swarm" class: several rows that are
                            really one agent.
  rollup_office_remount   - an anon mount at a seat's own office working directory, no
                            lineage anchors there directly, but the graph's charter names
                            exactly one seat for the room. High confidence charter match:
                            the office-cwd re-mount rolling up to the seat that already
                            owns the room.
  drop_ephemeral_test_cwd - a mount whose project is a SoftwareProject that is NOT active
                            (already retired, e.g. via retire_project - the "stub cull"
                            class: test targets, scratch directories, nonexistent probes,
                            and similar). The project is already judged dead; a straggler
                            mount against it is residue, not a fold.
  leave_for_human         - anything that does not clear a bucket's own bar: a nuanced
                            fold proposal (score < 0.75 - several seats share the room, or
                            no registered agent at all anchors it), or a seatless anon in a
                            room whose charter names no seat (folds.py's own visitor-gate
                            territory, never this module's business to guess at).

Zero false drops is the bar: a false drop here deletes a real agent's registration. The
drop path (mounts.undrop_dead_project_mount) is reversible and audited, keyed off the
audit_log row the drop itself leaves, so a mistaken drop is no longer permanent. That does
not lower the bar this module holds itself to; it just means a false drop, while still
something this module works to avoid entirely, is now recoverable rather than
catastrophic. reconcile_dry_run() never drops, folds, or retires anything; it is a
report, full stop.

reconcile_execute() is the acting half, built only after the dry-run output was reviewed
against real data (the same two-phase discipline folds.py already proved: propose, then a
separate, explicitly-gated act). It composes the same primitives this module has always
named - fold_agent / resolve_fold_candidate for buckets 1 and 2, mounts.drop_dead_project_mount
(row-scoped, never agent-id-wide) for bucket 3 - and does nothing to bucket 4, ever. Dry
run is its own default too (execute=False, matching unfold_agent's own convention): it
returns the exact plan without writing anything unless called with execute=True. Even
once merged and deployed, the scheduled leg (arq_worker.fleet_reconcile_heartbeat) stays
inert behind its own kill switch (osiris_fleet_reconcile_enabled, default False):
flipping that flag is a separate decision a human makes independently of approving the
code change, never a side effect of a deploy.

A later pass found the schedule itself already built, running on the same cadence since
it first shipped, and closed the one real gap that remained: what watches it. Before this,
the only signal on a scheduled tick was liveness telemetry (did it run) plus a log line
nobody is paged by - the same document-nobody-reads shape that has let other background
sweeps run unwitnessed for months elsewhere in this codebase. Three additions, all
composing existing precedented primitives, nothing new invented: (1) any tick that
actually acts fires a durable operator-desk brief with the exact before/after counts and
row ids (mailbox.send_message, the same shape used elsewhere for after-review and
sighting briefs); (2) a per-tick batch cap (_BATCH_CAP, 5) - a tick whose actionable rows
exceed it holds the whole batch to leave_for_human through the shared _held() helper and
fires a desk_kind='decision' brief, because an anomalous batch is the signature of a
classifier bug, not a thing to bulk-act on unwitnessed; (3) a consecutive-blind alarm: no
counter, no state row, open_thread's own idempotency on the alarm's fixed summary text
does the dedup, so the thread's own age IS the darkness duration, auto-resolved the tick
the census recovers. Flipping osiris_fleet_reconcile_enabled stayed a human decision,
held until this watch shipped: arming the schedule before its watch existed would have
reproduced the exact gap this work exists to close.

The state machine, named here rather than left for a future reader to infer from
branches (documentation should live where a reader actually encounters the code, not in
a separate decision record nobody rereads):

  DARK     - osiris_fleet_reconcile_enabled is off. Evaluated once, at the very top of
             reconcile_scheduled_tick, before reconcile_execute/reconcile_dry_run are
             ever called. A DARK tick never reaches any of the other three states, not
             merely unused alongside them, structurally unreachable, since the code path
             that would compute BLIND/OVER_CAP/ACTS is never entered this tick.
  BLIND    - the OS census failed. Set inside reconcile_dry_run, per row, before any row
             is ever assigned to an actionable bucket.
  OVER_CAP - the census succeeded but this tick's actionable rows (bulk_fold_swarm +
             rollup_office_remount + drop_ephemeral_test_cwd) exceeded _BATCH_CAP. Set
             by a post-loop pass inside reconcile_dry_run, after every row already has a
             bucket.
  ACTS     - neither hold fired. folded/dropped may be nonzero, or the tray may
             genuinely be empty: ACTS names "the acting code path ran," not "something
             was acted on."

BLIND and OVER_CAP can never both be true in the same tick, not by convention but by
construction: when the census is blind, every row that would have landed in an actionable
bucket is redirected to leave_for_human by the per-row blind check inside the main sweep
loop, before the actionable buckets are ever populated, so by the time the OVER_CAP
post-pass runs and sums those buckets, their combined size is already near zero, far under
_BATCH_CAP. reconcile_scheduled_tick's own "if out.get('census_blind'): state = 'BLIND'
else: state = 'OVER_CAP' if ... else 'ACTS'" encodes this as an if/elif, but the deeper
guarantee is upstream of that branch, in reconcile_dry_run itself: the two holds are
mutually exclusive because one empties the exact buckets the other measures.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.config.settings import Settings, get_settings
from src.orchestrator.folds import find_agent_fold_candidates

# The confidence bar find_agent_fold_candidates already draws for itself, reused here
# rather than invented fresh: 0.75 is its own single-seat presumption (one registered
# agent, no ambiguity); 0.55 is its own "nuanced, verify by hand" tier for a multi-seat
# room. Bulk-act never crosses below the sweep's own line for "verify by hand."
_HIGH_CONFIDENCE = 0.75

# Each swarm/charter class the sweep can propose, and which bucket it earns at high
# confidence. A class not listed here (there are only these three) has no bulk-act bucket
# at all and always leaves for a human, regardless of score.
_BUCKET_BY_CLASS = {
    "view-alias": "bulk_fold_swarm",
    "restart-mint": "bulk_fold_swarm",
    "charter-match": "rollup_office_remount",
}


_LIVE_WINDOW_SECS = 900  # The same 15-minute decay every liveness read in the fleet uses
                         # (mounts.py's own window constant, fleet()'s "live" cutoff).

# A single tick's actionable rows (bulk_fold_swarm + rollup_office_remount +
# drop_ephemeral_test_cwd combined) above this count refuse to act at all this tick: an
# anomalous batch is exactly the signature of a bug in the classifier upstream, not a
# thing to bulk-act on unwitnessed. Measured against real history before picking a number
# (merge-candidate creation over 14 days ranged from 0 to 34 per day, bursty and sparse,
# never a steady drip; the handful of real drops ever executed were human-directed
# demonstrations, none organic): 5 sits above any plausible single-tick slice of even the
# 34-in-a-day peak while still catching a genuine anomaly before it bulk-acts.
_ACTIONABLE_BUCKETS = ("bulk_fold_swarm", "rollup_office_remount", "drop_ephemeral_test_cwd")
_BATCH_CAP = 5


def _held(
    buckets: dict[str, list[dict[str, Any]]], row: dict[str, Any], rule: str,
) -> None:
    """Hold a row that would otherwise auto-act, recording the reason it was held instead;
    the rule text itself names which bucket it would have earned. Defined at module level
    rather than as a closure so the over-cap pass (a post-loop re-bucketing, not a per-row
    decision) reuses the exact same hold logic every other reason routes through: one hold
    mechanism, several reasons, never a second implementation that could drift from the
    first."""
    row["bucket"] = "leave_for_human"
    row["rule"] = rule
    buckets["leave_for_human"].append(row)


async def _ghost_flagged_agents(
    pool: asyncpg.Pool, *, live_bodies_by_cwd: Any = None,
) -> tuple[dict[str, dict[str, Any]], bool]:
    """A fifth class of anomaly: a mount row the graph calls live (last_seen within the
    fleet's own 15-minute window) with no real OS process backing its cwd - a phantom
    liveness signal the four buckets above were never built to see. It is the precise
    case where a presence check (the four buckets cover everything the sweep looks at) is
    mistaken for a coverage check (a row this sweep never looks at is invisible, not
    absent). fleet() already computes this exact signal (os_bodies/ghost_gap) at project
    grain; here it is row-scoped by cwd, following the same doctrine used by the door
    sweep: a mount may only be released on the word of the exact directory it opens into,
    the same granularity a per-row bucket decision needs.

    Returns ({agent_id: ghost row}, blind). blind=True means the OS census itself could
    not run (pgrep unavailable). The caller owns the blindness check: a blind census must
    never read as "no ghosts," only as "could not look," and reconcile_dry_run refuses to
    bucket anything into an auto-act class while blind rather than silently trusting an
    empty ghost set.

    Never auto-acted on: every ghost-flagged row lands in ghost_gap, even one that would
    otherwise have cleared an auto-act bucket's own bar (a phantom's other signals cannot
    be trusted either, since the one signal we can independently verify, whether anything
    is actually there, already failed)."""
    from src.orchestrator import census

    lookup = live_bodies_by_cwd or census.live_bodies_by_cwd
    by_cwd = lookup()
    if by_cwd is None:
        return {}, True
    rows = await pool.fetch(
        "SELECT agent_id, project, cwd, job_dir, last_seen FROM agent_mounts "
        "WHERE last_seen IS NOT NULL "
        "AND now() - last_seen < make_interval(secs => $1)", float(_LIVE_WINDOW_SECS))
    ghosts: dict[str, dict[str, Any]] = {}
    for r in rows:
        if r["cwd"] in by_cwd:
            continue
        ghosts[str(r["agent_id"])] = {
            "agent_id": r["agent_id"], "project": r["project"], "cwd": r["cwd"],
            "job_dir": r["job_dir"],
            "last_seen": r["last_seen"].isoformat() if r["last_seen"] else None,
            "bucket": "ghost_gap",
            "rule": f"agent_mounts reads this row GRAPH-LIVE (last_seen within "
                    f"{_LIVE_WINDOW_SECS}s) but no OS body backs cwd={r['cwd']!r} - "
                    "never auto-acted on, always a human's judgment",
        }
    return ghosts, False


async def _dead_project_mounts(pool: asyncpg.Pool) -> list[dict[str, Any]]:
    """Mount rows whose project is a SoftwareProject that is retired: the residue class
    retire_project's own refusal (mounts seen in the last 15 minutes block a retirement)
    cannot itself clean up after the fact. A project retired cleanly can still have stale
    rows from before the 15-minute window, or a new anon mount can land against it after
    retirement (nothing stops a session from launching into a stub's old cwd). This is a
    read-only join against objects.status, which retire_project already set; it reuses
    that verdict rather than inventing a second one.

    status <> 'active' AND <> 'merged', deliberately not just <> 'active'. This distinction
    was confirmed necessary by running this exact query against production before trusting
    it: it once matched an actively mounted session (last_seen seconds old, not residue)
    only because its project had been renamed and consolidated into another project via a
    merge, not retired. That agent's own works_in/governs edges had already migrated to the
    new, active project object, but agent_mounts.project is a plain string that nothing
    updates on a rename, so the stale label still joined to the now-merged object and read
    as dead. A merge is not a death: the label moved, the project didn't, and the mount's
    own current graph edges may already know it even when its stale text column doesn't.
    Carrying presence forward across a rename is a broader primitive that isn't built yet
    and isn't this module's job to invent; the fix this reaper owns is narrow: never treat
    a merge as a retirement."""
    rows = await pool.fetch(
        "SELECT m.agent_id, m.project, m.cwd, m.job_dir, m.last_seen, "
        "       p.status AS project_status "
        "FROM agent_mounts m "
        "JOIN objects p ON p.type='SoftwareProject' AND p.canonical = 'repo:' || m.project "
        "WHERE p.status NOT IN ('active', 'merged') "
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
                    "(already retired) - a mount here is residue, not a fold candidate",
        }
        for r in rows
    ]


async def reconcile_dry_run(
    pool: asyncpg.Pool, *, projects_root: Path | None = None, jobs_home: Path | None = None,
    live_bodies_by_cwd: Any = None,
) -> dict[str, Any]:
    """The report reviewed before anything acts. Buckets every row currently reachable:
    the fold-candidate tray (refreshed by calling find_agent_fold_candidates, itself
    proposal-only and idempotent), the dead-project residue class the tray was never
    built to see, and the ghost_gap class (a mount the graph calls live with no OS body
    backing it), and names the rule that placed each one. Writes nothing except what
    find_agent_fold_candidates itself already writes (merge_candidates proposal rows,
    review-gated, never executed here). projects_root/jobs_home pass straight through to
    the sweep, the same injection point its own tests already use, not a new one.
    live_bodies_by_cwd is the ghost check's own injection point (tests drive it with a
    fake; production defaults to the real OS census).

    A ghost overrides every other verdict: a row whose agent_id the ghost check flags
    lands in ghost_gap regardless of what bucket its other signals would have earned; a
    phantom's other signals cannot be trusted either, once the one signal independently
    checkable against the OS has already failed.

    A blind census refuses to bucket anything into an auto-act class (following the same
    rule used elsewhere: "could not look" must never read as "no ghosts"): when the OS
    census itself fails, every row that would have landed in bulk_fold_swarm/
    rollup_office_remount/drop_ephemeral_test_cwd is held in leave_for_human instead,
    named as blind-held. reconcile_execute reads buckets from here, so this alone keeps
    the acting half safe without touching it. census_blind: true at the top level names
    the reason plainly.

    An over-cap tick also refuses to bucket anything into an auto-act class: once every
    row has found its bucket, if the combined size of bulk_fold_swarm/
    rollup_office_remount/drop_ephemeral_test_cwd exceeds _BATCH_CAP (5), the whole
    actionable set is re-held in leave_for_human through the same _held() every other
    hold reason uses; an anomalous batch is exactly the signature of a bug in the
    upstream classifier, not a thing to bulk-act on unwitnessed. over_cap: true at the top
    level names the reason plainly, same shape as census_blind.

    The state machine this module implements, named rather than left to infer from
    branches: DARK (the scheduled leg's kill switch is off; this is
    reconcile_scheduled_tick's own concern, not this function's) -> BLIND (census failed
    this tick, everything held) -> OVER_CAP (census fine, batch too large, everything
    held) -> ACTS (neither hold fired, the plan's would_fold/would_drop rows are real).
    Every non-ACTS state is a structural hold: rows are never assembled into the acting
    lists in the first place, not filtered out by a check a future edit could forget."""
    swept = await find_agent_fold_candidates(
        pool, projects_root=projects_root, jobs_home=jobs_home)
    ghosts, blind = await _ghost_flagged_agents(pool, live_bodies_by_cwd=live_bodies_by_cwd)
    buckets: dict[str, list[dict[str, Any]]] = {
        "bulk_fold_swarm": [], "rollup_office_remount": [],
        "drop_ephemeral_test_cwd": [], "ghost_gap": [], "leave_for_human": [],
    }
    seen_ghosts: set[str] = set()

    for c in swept["pending"]:
        cls = str(c.get("class") or "")
        score = float(c.get("score") or 0.0)
        target = _BUCKET_BY_CLASS.get(cls)
        dupe = str(c.get("dupe") or "")
        row = {
            "candidate_id": c["id"], "dupe": c.get("dupe"), "into": c.get("into_label"),
            "class": cls, "score": score, "signals": c.get("signals"),
        }
        if dupe in ghosts:
            seen_ghosts.add(dupe)
            row.update(bucket="ghost_gap", rule=ghosts[dupe]["rule"])
            buckets["ghost_gap"].append(row)
        elif target and score >= _HIGH_CONFIDENCE:
            rule = f"{cls} score {score} >= {_HIGH_CONFIDENCE} - the sweep's own " \
                   "single-seat/no-body confidence bar"
            if blind:
                _held(buckets, row, f"[would be {target}] " + rule + " - HELD: OS census "
                      "is blind this tick, an auto-act bucket cannot be trusted without a "
                      "ghost check")
            else:
                row["bucket"], row["rule"] = target, rule
                buckets[target].append(row)
        else:
            row["bucket"] = "leave_for_human"
            row["rule"] = (
                f"{cls} score {score} < {_HIGH_CONFIDENCE} - the sweep's own 'nuanced, "
                "verify by hand' tier" if target else
                f"class {cls!r} has no bulk-act bucket - always a human's call"
            )
            buckets["leave_for_human"].append(row)

    dead_projects: set[str] = set()
    for row in await _dead_project_mounts(pool):
        agent_id = str(row.get("agent_id") or "")
        dead_projects.add(str(row["project"]))
        if agent_id in ghosts:
            seen_ghosts.add(agent_id)
            row.update(bucket="ghost_gap", rule=ghosts[agent_id]["rule"])
            buckets["ghost_gap"].append(row)
        elif blind:
            _held(buckets, row, "[would be drop_ephemeral_test_cwd] " + row["rule"] +
                  " - HELD: OS census is blind this tick, an auto-act bucket cannot be "
                  "trusted without a ghost check")
        else:
            buckets["drop_ephemeral_test_cwd"].append(row)

    for project, n in (swept.get("seatless") or {}).items():
        # A project already caught above (its own SoftwareProject is retired) gets one
        # verdict, not two: "drop, the project is dead" is more specific than "seatless,
        # ask a human" and supersedes it, rather than reporting the same row twice under
        # different bucket names.
        if project in dead_projects:
            continue
        buckets["leave_for_human"].append({
            "project": project, "count": n, "bucket": "leave_for_human",
            "rule": f"{n} seatless anon(s) in a room whose charter names no seat - "
                    "folds.py's own visitor-gate territory, never this module's call",
        })

    # Ghosts never otherwise swept: a ghost row that matched no fold candidate and no dead
    # project is still a real anomaly (invisible, not safe) and must still appear, or
    # "buckets every row" would be false for exactly the class this check exists to close.
    for agent_id, ghost_row in ghosts.items():
        if agent_id not in seen_ghosts:
            buckets["ghost_gap"].append(ghost_row)

    # Over-cap: a post-pass, not a per-row check, because the cap is a judgment about the
    # tick's total actionable volume, only knowable once every row has already found its
    # bucket. Re-bucket the whole actionable set through the same _held() every other hold
    # reason uses; the guard cannot be half-applied, since a row either keeps its earned
    # bucket or is held, never a mix within one tick.
    actionable_total = sum(len(buckets[b]) for b in _ACTIONABLE_BUCKETS)
    over_cap = actionable_total > _BATCH_CAP
    if over_cap:
        for name in _ACTIONABLE_BUCKETS:
            rows, buckets[name] = buckets[name], []
            for row in rows:
                _held(buckets, row, f"[would be {row['bucket']}] {row['rule']} - HELD: "
                      f"tick batch size {actionable_total} exceeds cap {_BATCH_CAP}, one "
                      "human look before bulk action")

    counts = {k: len(v) for k, v in buckets.items()}
    return {
        "buckets": buckets, "counts": counts, "total": sum(counts.values()),
        "examined": swept.get("examined", 0),
        "census_blind": blind,
        "over_cap": over_cap,
        "note": ("REPORT ONLY - nothing folded, dropped, or retired. Every row above names "
                 "its own bucket and the rule that put it there." +
                 (" OS CENSUS WAS BLIND THIS TICK - every row that would have auto-acted "
                  "is held in leave_for_human instead; re-run once the census can see."
                  if blind else "") +
                 (f" BATCH CAP EXCEEDED THIS TICK ({actionable_total} > {_BATCH_CAP}) - "
                  "every row that would have auto-acted is held in leave_for_human "
                  "instead; an anomalous batch needs a human's eyes before bulk action."
                  if over_cap else "")),
    }


async def reconcile_execute(
    actions: Actions, *, actor: str, projects_root: Path | None = None,
    jobs_home: Path | None = None, execute: bool = False, live_bodies_by_cwd: Any = None,
) -> dict[str, Any]:
    """The acting half. Dry run is the default (execute=False, matching unfold_agent's own
    convention in folds.py): returns the exact plan, which candidates would be folded,
    which mount rows would be dropped, how many rows sit in leave_for_human untouched,
    without writing anything. execute=True performs it.

    Re-reads the tray itself via reconcile_dry_run (never trusts a caller-supplied stale
    report) so the plan and the act see the same instant, not a report gathered a query or
    a deploy ago.

    bulk_fold_swarm + rollup_office_remount: resolve_fold_candidate(decision='merged') per
    candidate, the same full-context fold folds.py already proves (mail, mount rows,
    thread ownership all move with it), never a bare kernel merge.

    drop_ephemeral_test_cwd: mounts.drop_dead_project_mount per row, scoped by (job_dir,
    project), a row-scoped delete, never agent-id-wide (a lesson learned from an earlier
    false-succession incident elsewhere in this codebase).

    leave_for_human: never acted on, by construction, not filtered out, not deferred,
    simply absent from every write this function performs. A sweep that never defers
    anything to a human is a sweep that will eventually act on something it shouldn't.

    A single row's fold or drop failing (a race, an already-resolved candidate) is caught
    and reported inline rather than aborting the batch: zero false drops means every row
    that WAS acted on must be a true positive, not that one failure may silently swallow
    the rest of a correct plan.

    Post-act verification (execute=True only): re-reads the tray a second time after
    acting and reports before/after counts, proof the acted rows actually left the tray,
    never a trusted return value from the fold/drop calls alone.

    The desk brief: the only prior watch on this function was liveness telemetry (did the
    tick run), never whether what it did was right; the one content signal was a log line
    nobody is paged by, the same document-nobody-reads shape that has let other background
    sweeps run unwitnessed for months elsewhere. So a real execute (folded or dropped
    nonzero) now also fires a durable, addressable operator-desk brief (mailbox.send_message,
    the same shape used elsewhere for after-review and sighting briefs) carrying the exact
    before/after counts and row ids, never a summary a human has to trust. An over-cap
    tick (below) fires its own brief at desk_kind='decision' instead: that one genuinely
    needs a human call, not just a status note. Both are try/excepted: a mail hiccup must
    never unwind a landed action, the same discipline used elsewhere in this codebase."""
    from src.orchestrator.folds import resolve_fold_candidate
    from src.orchestrator.mailbox import send_message
    from src.orchestrator.mounts import drop_dead_project_mount

    report = await reconcile_dry_run(actions.pool, projects_root=projects_root,
                                     jobs_home=jobs_home,
                                     live_bodies_by_cwd=live_bodies_by_cwd)
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
        "census_blind": report["census_blind"], "over_cap": report["over_cap"],
        "execute": execute,
    }
    if not execute:
        plan["note"] = "PLAN ONLY - call with execute=True to write. Nothing touched."
        return plan

    # Over-cap: would_fold/would_drop are already empty here (reconcile_dry_run held the
    # whole actionable set to leave_for_human before this function ever read its buckets).
    # This is the decision brief, not the status one, because an anomalous batch needs a
    # human call (raise the cap? investigate the classifier?), not a status note.
    if report["over_cap"]:
        body = (f"FLEET-RECONCILE OVER CAP - a tick's actionable rows "
                f"(bulk_fold_swarm+rollup_office_remount+drop_ephemeral_test_cwd) totaled "
                f"more than the cap of {_BATCH_CAP}; the whole tick was held in "
                f"leave_for_human rather than bulk-acting on an unreviewed anomaly. "
                f"counts: {report['counts']}. actor={actor!r}.")
        try:
            sent = await send_message(actions.pool, from_agent=actor, from_project="osiris",
                                      to_project="operator", body=body, desk_kind="decision")
            plan["desk_brief_id"] = sent.get("id")
        except Exception:  # noqa: BLE001 - a mail hiccup must not mask the hold that landed
            plan["desk_brief_id"] = None
        # Same shape as an ACTS tick's plan (folded/dropped/before_counts/after_counts
        # present, not just implied by their absence): nothing moved, so before == after,
        # reusing report["counts"] rather than a second, pointless dry-run read.
        plan.update({
            "folded": [], "dropped": [],
            "before_counts": report["counts"], "after_counts": report["counts"],
            "note": ("OVER CAP - nothing acted this tick, everything held in "
                     "leave_for_human, an operator decision brief was sent."),
        })
        return plan

    folded, drops = [], []
    for item in would_fold:
        try:
            out = await resolve_fold_candidate(
                actions, candidate_id=item["candidate_id"], decision="merged", actor=actor)
        except Exception as exc:  # one bad row must not abort a correct batch
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
                                    jobs_home=jobs_home,
                                    live_bodies_by_cwd=live_bodies_by_cwd)
    plan.update({
        "folded": folded, "dropped": drops,
        "before_counts": report["counts"], "after_counts": after["counts"],
        "note": "EXECUTED - before/after counts prove the acted rows left the tray; "
                "leave_for_human rows were never touched.",
    })

    acted_folds = sum(1 for f in folded if "error" not in f.get("result", {}))
    acted_drops = sum(1 for d in drops if "error" not in d)
    if acted_folds or acted_drops:
        body = (f"FLEET-RECONCILE ACTED - folded {acted_folds}, dropped {acted_drops} "
                f"(of {len(folded)} attempted folds, {len(drops)} attempted drops). "
                f"before={report['counts']} after={after['counts']}. actor={actor!r}. "
                f"folded rows: {folded}. dropped rows: {drops}.")
        try:
            sent = await send_message(actions.pool, from_agent=actor, from_project="osiris",
                                      to_project="operator", body=body, desk_kind="fyi")
            plan["desk_brief_id"] = sent.get("id")
        except Exception:  # noqa: BLE001 - a fold/drop that landed must not unwind on a
            plan["desk_brief_id"] = None  # mail hiccup, same discipline as elsewhere
    return plan


# The consecutive-blind alarm: no counter, no state row. open_thread's own
# idempotency-on-summary-text does the dedup, and the thread's own age IS the darkness
# duration. The text must stay byte-for-byte stable across calls (the canonical hash is
# derived from it) or every tick would mint a new thread instead of finding the one
# already open.
_BLIND_ALARM_SUMMARY = (
    "FLEET-RECONCILE'S SCHEDULED TICK WENT CENSUS-BLIND - the OS census failed this tick, "
    "every auto-act row was held in leave_for_human instead of trusted; if this persists "
    "across many ticks the auto-act path is silently dark and nothing is being ghost-"
    "checked. This thread's own age is the duration - no separate counter exists. "
    "Auto-resolved the next tick the census succeeds again."
)


async def reconcile_scheduled_tick(
    actions: Actions, *, settings: Settings | None = None,
    projects_root: Path | None = None, jobs_home: Path | None = None,
    live_bodies_by_cwd: Any = None,
) -> dict[str, Any]:
    """The scheduled leg's own tick: arq_worker.fleet_reconcile_heartbeat calls this
    unconditionally, the same thin-shim shape other scheduled ticks already use elsewhere
    in this codebase (real logic and the flag gate live in the orchestrator tick, not the
    cron wrapper).

    Off unless osiris_fleet_reconcile_enabled: the kill switch. The code ships inert, and
    flipping this flag is a decision a human makes separately from approving the code
    change, never a side effect of deploying it. When on, composes
    reconcile_execute(execute=True), the exact same acting call reachable by hand, so the
    schedule and a human's own manual call are provably the same path, never two
    implementations that could drift.

    settings is the injected test seam (matching the convention used elsewhere: st =
    settings or get_settings()) so a test can flip the flag without touching the real
    environment or monkeypatching get_settings.

    state names the machine this module implements, in the return value rather than left
    for a future reader to infer from branches: DARK (flag off, nothing read or written)
    -> BLIND (OS census failed this tick, reconcile_dry_run held every auto-act row) ->
    OVER_CAP (census fine, this tick's actionable batch exceeded _BATCH_CAP, every row
    held) -> ACTS (neither hold fired; folded/dropped may be nonzero). Every non-ACTS
    state is a structural hold upstream, not a check this function performs itself.

    The consecutive-blind alarm: a BLIND tick opens _BLIND_ALARM_SUMMARY as a
    severity='alarm' obligation thread, idempotent on the summary text, so ticks 2..N
    against an already-blind census are free, no counter or state row needed; the
    thread's own age is how long the auto-act path has been dark, and severity='alarm'
    rides the same live-desk alarm filter other schema-drift alarms already use
    elsewhere, for free. The next non-blind tick resolves it, the same shape another
    schema-drift alarm elsewhere now uses to auto-resolve on a clean check (this comment
    previously said this alarm never auto-resolved; that changed, and this note is
    corrected rather than left to mislead a future reader against the actual code): a
    blind census, like a boot guard's own drift, can genuinely self-heal tick to tick as
    underlying state changes, so auto-resolving here reports reality instead of requiring
    a human to notice recovery and close it by hand. Both calls are try/excepted: a graph
    hiccup must never fail the tick that already decided whether to act."""
    from src.orchestrator.folds import _SANCTIONED_AUTO_FOLD_ACTOR

    st = settings or get_settings()
    if not st.osiris_fleet_reconcile_enabled:
        return {"enabled": False, "state": "DARK", "folded": [], "dropped": [],
                "note": "the reaper's scheduled leg is dark "
                        "(osiris_fleet_reconcile_enabled=0)"}
    out = await reconcile_execute(actions, actor=_SANCTIONED_AUTO_FOLD_ACTOR,
                                  execute=True, projects_root=projects_root,
                                  jobs_home=jobs_home, live_bodies_by_cwd=live_bodies_by_cwd)

    from src.orchestrator.capture import open_thread, resolve_thread

    if out.get("census_blind"):
        state = "BLIND"
        try:
            await open_thread(actions, summary=_BLIND_ALARM_SUMMARY, kind="obligation",
                              severity="alarm", source="cron:fleet_reconcile_heartbeat")
        except Exception:  # noqa: BLE001 - a mint hiccup must not fail the tick's verdict
            pass
    else:
        state = "OVER_CAP" if out.get("over_cap") else "ACTS"
        try:
            await resolve_thread(
                actions, _BLIND_ALARM_SUMMARY,
                because="census recovered - this tick's OS body check succeeded again",
                source="cron:fleet_reconcile_heartbeat")
        except Exception:  # noqa: BLE001 - same discipline: never fail the tick over this
            pass
    return {"enabled": True, "state": state, **out}
