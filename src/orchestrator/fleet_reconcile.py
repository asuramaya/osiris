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

TASK #108 (Thoth DM 2881/2889/2916) found the cadence itself already built — same session
lineage, 2026-07-30 — and closed the one real gap instead: WHAT WATCHES IT. Before this,
the only signal on a scheduled tick was `record_job`'s liveness telemetry (did it run) plus
a `_log.info` line nobody is paged by — the exact document-nobody-reads shape that let
sweep_ghost_doors run unwitnessed for months. Three additions, all composing existing
precedented primitives, nothing new invented: (1) any tick that actually acts fires a
durable operator-desk brief with the exact before/after counts and row ids
(`mailbox.send_message`, same shape as `greatfold.py`/`pit_watch.py`); (2) a per-tick BATCH
CAP (`_BATCH_CAP`, 5) — a tick whose actionable rows exceed it holds the WHOLE batch to
`leave_for_human` through the shared `_held()` and fires a `desk_kind='decision'` brief,
because an anomalous batch is the signature of a classifier bug, not a thing to bulk-act on
unwitnessed; (3) a CONSECUTIVE-BLIND ALARM — no counter, no state row, `open_thread`'s own
idempotency on the alarm's fixed summary text does the dedup, so the thread's own age IS the
darkness duration, auto-resolved the tick the census recovers. The resulting state machine —
DARK -> BLIND -> OVER_CAP -> ACTS, every non-ACTS state a STRUCTURAL hold — is named
explicitly in `reconcile_scheduled_tick`'s own return value (`state`) and docstring, per
ruling 2889's acceptance test: "what refuses," not "the agent will know to." Flipping
`osiris_fleet_reconcile_enabled` stays the operator's own hand, held until this watch shipped
— arming the schedule before its watch exists would have been the birth-defect version of
the exact bug this task exists to close.
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


_LIVE_WINDOW_SECS = 900  # the same 15-minute decay every liveness read in the fleet uses
                         # (mounts.py's own _DOOR_WINDOW_SECS, fleet()'s "live" cutoff)

# task #108 (Thoth DM 2889/2916): a single tick's actionable rows (bulk_fold_swarm +
# rollup_office_remount + drop_ephemeral_test_cwd combined) above this count refuse to
# act at all THIS TICK — an anomalous batch is exactly the signature of a bug in the
# classifier upstream, not a thing to bulk-act on unwitnessed. Measured against real
# history before picking a number (merge_candidates creation over 14 days: 12, 34, 1, and
# ZERO on the other 11 — bursty and sparse, never a steady drip; 2 real drops ever
# executed via audit_log, both a human-directed demonstration, none organic): 5 sits above
# any plausible single-tick slice of even the 34-in-a-day peak while still catching a
# genuine anomaly before it bulk-acts.
_ACTIONABLE_BUCKETS = ("bulk_fold_swarm", "rollup_office_remount", "drop_ephemeral_test_cwd")
_BATCH_CAP = 5


def _held(
    buckets: dict[str, list[dict[str, Any]]], row: dict[str, Any], rule: str,
) -> None:
    """A row that WOULD auto-act, held back for the stated reason instead — the rule text
    itself names which bucket it would have earned. Module-level (task #108) rather than a
    closure so the OVER-CAP pass (a post-loop re-bucketing, not a per-row decision) reuses
    the exact same hold every other reason routes through — ONE hold mechanism, several
    reasons, never a second implementation that could drift from the first."""
    row["bucket"] = "leave_for_human"
    row["rule"] = rule
    buckets["leave_for_human"].append(row)


async def _ghost_flagged_agents(
    pool: asyncpg.Pool, *, live_bodies_by_cwd: Any = None,
) -> tuple[dict[str, dict[str, Any]], bool]:
    """THE FIFTH CLASS (thread 04ad4bb8): a mount row the GRAPH calls live (last_seen
    within the fleet's own 15-minute window) with no real OS process backing its cwd — a
    phantom liveness signal the four buckets above were never built to see, and the one
    Imhotep's #117 shape-1 taxonomy names precisely: a presence check (four buckets cover
    everything the sweep looks AT) mistaken for a coverage check (a row this sweep never
    looks at is invisible, not absent). fleet() already computes this exact signal
    (os_bodies/ghost_gap, mcp_server.py) at PROJECT grain; here it is row-scoped by cwd —
    `census.live_bodies_by_cwd`'s own doctrine, shared with the door sweep: 'a door may
    only be released on the word of the exact directory it opens into,' the same
    granularity a per-row bucket decision needs.

    Returns ({agent_id: ghost row}, blind). `blind=True` means the OS census itself could
    not run (pgrep unavailable) — THE CALLER OWNS THE BLINDNESS CHECK (sweep_ghost_doors'
    own law): a blind census must never read as 'no ghosts', only as 'could not look', and
    reconcile_dry_run refuses to bucket anything into an auto-act class while blind rather
    than silently trusting an empty ghost set.

    NEVER auto-acted on — every ghost-flagged row lands in `ghost_gap`, even one that would
    otherwise have cleared an auto-act bucket's own bar (a phantom's OTHER signals cannot
    be trusted either, since the one signal we can independently verify — is anything
    actually there — already failed)."""
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
                    f"{_LIVE_WINDOW_SECS}s) but no OS body backs cwd={r['cwd']!r} — thread "
                    "04ad4bb8's own class; never auto-acted on, always a human's judgment",
        }
    return ghosts, False


async def _dead_project_mounts(pool: asyncpg.Pool) -> list[dict[str, Any]]:
    """Mount rows whose project is a SoftwareProject that is RETIRED — the residue class
    retire_project's own refusal (mounts seen in the last 15 minutes block a retirement)
    cannot itself clean up after the fact: a project retired cleanly can still have STALE
    rows from before the 15-minute window, or a new anon mount can land against it after
    retirement (nothing stops a claude session from launching into a stub's old cwd).
    Read-only join against objects.status, which retire_project already set — this reuses
    that verdict rather than inventing a second one.

    status <> 'active' AND <> 'merged', DELIBERATELY NOT just <> 'active' (found live, this
    session, running this exact query against production before trusting it: agent:c1b99f6e-
    vii/seat Werner, last_seen 2 SECONDS old — an actively mounted session, not residue —
    matched here because its project 'ByeByte' was renamed/consolidated into 'bytebye' via a
    MERGE, not retired; Werner's own works_in/governs edges had already migrated to
    repo:bytebye (ACTIVE), but agent_mounts.project is a plain string nothing updates on a
    rename, so the STALE label still joined to the now-merged object and read as dead. A
    merge is not a death — the label moved, the project didn't — and the mount's own current
    graph edges may already know it even when its stale text column doesn't. This is the
    "rename-carrying-presence-forward primitive is not built" gap (decision d1775472,
    greenday->redmonth->ballgem) surfacing here as a would-be FALSE DROP; the fix this reaper
    owns is narrow — never treat a merge as a retirement — the broader primitive stays
    unbuilt and is not this module's job to invent."""
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
                    "(already retired) — a mount here is residue, not a fold candidate",
        }
        for r in rows
    ]


async def reconcile_dry_run(
    pool: asyncpg.Pool, *, projects_root: Path | None = None, jobs_home: Path | None = None,
    live_bodies_by_cwd: Any = None,
) -> dict[str, Any]:
    """THE REPORT Thoth asked to see before anything acts. Buckets every row currently
    reachable — the fold-candidate tray (refreshed by calling find_agent_fold_candidates,
    itself proposal-only and idempotent), the dead-project residue class the tray was
    never built to see, AND the ghost_gap class (thread 04ad4bb8: a mount the graph calls
    live with no OS body backing it) — and names the rule that placed each one. Writes
    nothing except what find_agent_fold_candidates itself already writes (merge_candidates
    PROPOSAL rows — review-gated, never executed here). `projects_root`/`jobs_home` pass
    straight through to the sweep — the same injection seam its own tests already use, not
    a new one. `live_bodies_by_cwd` is the ghost-check's own injection seam (tests drive it
    with a fake; production defaults to the real OS census).

    A GHOST OVERRIDES EVERY OTHER VERDICT: a row whose agent_id the ghost check flags lands
    in `ghost_gap` regardless of what bucket its OTHER signals would have earned — a
    phantom's other signals cannot be trusted either, once the one signal independently
    checkable against the OS has already failed.

    A BLIND CENSUS REFUSES TO BUCKET ANYTHING INTO AN AUTO-ACT CLASS (sweep_ghost_doors'
    own law: 'could not look' must never read as 'no ghosts'): when the OS census itself
    fails, every row that would have landed in bulk_fold_swarm/rollup_office_remount/
    drop_ephemeral_test_cwd is held in `leave_for_human` instead, named as blind-held —
    `reconcile_execute` reads buckets from here, so this alone keeps the acting half safe
    without touching it. `census_blind: true` at the top level names the reason plainly.

    AN OVER-CAP TICK ALSO REFUSES TO BUCKET ANYTHING INTO AN AUTO-ACT CLASS (task #108,
    Thoth DM 2889/2916): once every row has found its bucket, if the combined size of
    bulk_fold_swarm/rollup_office_remount/drop_ephemeral_test_cwd exceeds `_BATCH_CAP`
    (5), the WHOLE actionable set is re-held in `leave_for_human` through the same
    `_held()` every other hold reason uses — an anomalous batch is exactly the signature
    of a bug in the upstream classifier, not a thing to bulk-act on unwitnessed.
    `over_cap: true` at the top level names the reason plainly, same shape as
    `census_blind`.

    THE STATE MACHINE THIS MODULE IMPLEMENTS, NAMED RATHER THAN LEFT TO INFER FROM
    BRANCHES (ruling 2889's own acceptance test — "what refuses"): DARK (the scheduled
    leg's kill switch is off — `reconcile_scheduled_tick`'s own concern, not this
    function's) -> BLIND (census failed this tick, everything held) -> OVER_CAP (census
    fine, batch too large, everything held) -> ACTS (neither hold fired, the plan's
    would_fold/would_drop rows are real). Every non-ACTS state is a STRUCTURAL hold —
    rows are never assembled into the acting lists in the first place, not filtered out
    by a check a future edit could forget."""
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
            rule = f"{cls} score {score} >= {_HIGH_CONFIDENCE} — the sweep's own " \
                   "single-seat/no-body confidence bar"
            if blind:
                _held(buckets, row, f"[would be {target}] " + rule + " — HELD: OS census "
                      "is blind this tick, an auto-act bucket cannot be trusted without a "
                      "ghost check")
            else:
                row["bucket"], row["rule"] = target, rule
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
        agent_id = str(row.get("agent_id") or "")
        dead_projects.add(str(row["project"]))
        if agent_id in ghosts:
            seen_ghosts.add(agent_id)
            row.update(bucket="ghost_gap", rule=ghosts[agent_id]["rule"])
            buckets["ghost_gap"].append(row)
        elif blind:
            _held(buckets, row, "[would be drop_ephemeral_test_cwd] " + row["rule"] +
                  " — HELD: OS census is blind this tick, an auto-act bucket cannot be "
                  "trusted without a ghost check")
        else:
            buckets["drop_ephemeral_test_cwd"].append(row)

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

    # GHOSTS NEVER OTHERWISE SWEPT: a ghost row that matched no fold candidate and no dead
    # project is STILL a real anomaly (thread 04ad4bb8's own point — invisible, not safe)
    # and must still appear, or "buckets every row" is false for exactly the class this
    # fix exists to close.
    for agent_id, ghost_row in ghosts.items():
        if agent_id not in seen_ghosts:
            buckets["ghost_gap"].append(ghost_row)

    # OVER-CAP (task #108): a POST-pass, not a per-row check, because the cap is a
    # judgment about the TICK's total actionable volume, only knowable once every row has
    # already found its bucket. Re-bucket the whole actionable set through the same
    # `_held()` every other hold reason uses — the guard cannot be half-applied, since a
    # row either keeps its earned bucket or is held, never a mix within one tick.
    actionable_total = sum(len(buckets[b]) for b in _ACTIONABLE_BUCKETS)
    over_cap = actionable_total > _BATCH_CAP
    if over_cap:
        for name in _ACTIONABLE_BUCKETS:
            rows, buckets[name] = buckets[name], []
            for row in rows:
                _held(buckets, row, f"[would be {row['bucket']}] {row['rule']} — HELD: "
                      f"tick batch size {actionable_total} exceeds cap {_BATCH_CAP}, one "
                      "human look before bulk action")

    counts = {k: len(v) for k, v in buckets.items()}
    return {
        "buckets": buckets, "counts": counts, "total": sum(counts.values()),
        "examined": swept.get("examined", 0),
        "census_blind": blind,
        "over_cap": over_cap,
        "note": ("REPORT ONLY — nothing folded, dropped, or retired. Every row above names "
                 "its own bucket and the rule that put it there." +
                 (" OS CENSUS WAS BLIND THIS TICK — every row that would have auto-acted "
                  "is held in leave_for_human instead; re-run once the census can see."
                  if blind else "") +
                 (f" BATCH CAP EXCEEDED THIS TICK ({actionable_total} > {_BATCH_CAP}) — "
                  "every row that would have auto-acted is held in leave_for_human "
                  "instead; an anomalous batch needs a human's eyes before bulk action."
                  if over_cap else "")),
    }


async def reconcile_execute(
    actions: Actions, *, actor: str, projects_root: Path | None = None,
    jobs_home: Path | None = None, execute: bool = False, live_bodies_by_cwd: Any = None,
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
    never a trusted return value from the fold/drop calls alone.

    THE DESK RECEIPT (task #108, Thoth DM 2889/2916 — "what watches it"): the only prior
    watch on this function was `record_job`'s own liveness telemetry (did the tick run),
    never whether what it DID was right — the one content signal was a log line nobody is
    paged by, the exact document-nobody-reads shape that let sweep_ghost_doors run
    unwitnessed for months. So a REAL execute (folded or dropped nonzero) now also fires a
    durable, addressable operator-desk brief (`mailbox.send_message`, same shape as
    `greatfold.py`'s after-review brief and `pit_watch.py`'s sighting brief) carrying the
    exact before/after counts and row ids — never a summary a human has to trust. An
    OVER-CAP tick (below) fires its own brief at `desk_kind='decision'` instead: that one
    genuinely needs a human call, not just an FYI. Both are try/excepted — a mail hiccup
    must never unwind a landed action, same discipline `greatfold.py` already proves."""
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
        plan["note"] = "PLAN ONLY — call with execute=True to write. Nothing touched."
        return plan

    # OVER-CAP: `would_fold`/`would_drop` are already empty here (reconcile_dry_run held
    # the whole actionable set to leave_for_human before this function ever read its
    # buckets) — this is the DECISION brief, not the FYI one, because an anomalous batch
    # needs a human call (raise the cap? investigate the classifier?), not a status note.
    if report["over_cap"]:
        body = (f"FLEET-RECONCILE OVER CAP — a tick's actionable rows "
                f"(bulk_fold_swarm+rollup_office_remount+drop_ephemeral_test_cwd) totaled "
                f"more than the cap of {_BATCH_CAP}; the whole tick was held in "
                f"leave_for_human rather than bulk-acting on an unreviewed anomaly. "
                f"counts: {report['counts']}. actor={actor!r}.")
        try:
            sent = await send_message(actions.pool, from_agent=actor, from_project="osiris",
                                      to_project="operator", body=body, desk_kind="decision")
            plan["desk_brief_id"] = sent.get("id")
        except Exception:  # noqa: BLE001 — a mail hiccup must not mask the hold that landed
            plan["desk_brief_id"] = None
        # same shape as an ACTS tick's plan (folded/dropped/before_counts/after_counts
        # present, not just implied by their absence) — nothing moved, so before == after,
        # reusing `report["counts"]` rather than a second, pointless dry-run read.
        plan.update({
            "folded": [], "dropped": [],
            "before_counts": report["counts"], "after_counts": report["counts"],
            "note": ("OVER CAP — nothing acted this tick, everything held in "
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
        "note": "EXECUTED — before/after counts prove the acted rows left the tray; "
                "leave_for_human rows were never touched.",
    })

    acted_folds = sum(1 for f in folded if "error" not in f.get("result", {}))
    acted_drops = sum(1 for d in drops if "error" not in d)
    if acted_folds or acted_drops:
        body = (f"FLEET-RECONCILE ACTED — folded {acted_folds}, dropped {acted_drops} "
                f"(of {len(folded)} attempted folds, {len(drops)} attempted drops). "
                f"before={report['counts']} after={after['counts']}. actor={actor!r}. "
                f"folded rows: {folded}. dropped rows: {drops}.")
        try:
            sent = await send_message(actions.pool, from_agent=actor, from_project="osiris",
                                      to_project="operator", body=body, desk_kind="fyi")
            plan["desk_brief_id"] = sent.get("id")
        except Exception:  # noqa: BLE001 — a fold/drop that landed must not unwind on a
            plan["desk_brief_id"] = None  # mail hiccup, same discipline as greatfold.py
    return plan


# task #108's consecutive-blind alarm (Thoth DM 2889/2916): NO COUNTER, NO STATE ROW —
# open_thread's own idempotency-on-summary-text does the dedup, and the thread's own age
# IS the darkness duration. The text must stay byte-for-byte stable across calls (the
# canonical hash is derived FROM it, `capture._canon`) or every tick would mint a new
# thread instead of finding the one already open.
_BLIND_ALARM_SUMMARY = (
    "FLEET-RECONCILE'S SCHEDULED TICK WENT CENSUS-BLIND — the OS census failed this tick, "
    "every auto-act row was held in leave_for_human instead of trusted; if this persists "
    "across many ticks the auto-act path is silently dark and nothing is being ghost-"
    "checked. This thread's own age is the duration — no separate counter exists. "
    "Auto-resolved the next tick the census succeeds again."
)


async def reconcile_scheduled_tick(
    actions: Actions, *, settings: Settings | None = None,
    projects_root: Path | None = None, jobs_home: Path | None = None,
    live_bodies_by_cwd: Any = None,
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
    environment or monkeypatching `get_settings`.

    `state` NAMES THE MACHINE THIS MODULE IMPLEMENTS (task #108, ruling 2889's own
    acceptance test — "what refuses"), in the return value rather than left for a future
    reader to infer from branches: DARK (flag off, nothing read or written) -> BLIND (OS
    census failed this tick, `reconcile_dry_run` held every auto-act row) -> OVER_CAP
    (census fine, this tick's actionable batch exceeded `_BATCH_CAP`, every row held) ->
    ACTS (neither hold fired; `folded`/`dropped` may be nonzero). Every non-ACTS state is
    a structural hold upstream, not a check this function performs itself.

    THE CONSECUTIVE-BLIND ALARM (task #108's third piece): a BLIND tick opens
    `_BLIND_ALARM_SUMMARY` as a `severity='alarm'` obligation Thread — idempotent on the
    summary text, so ticks 2..N against an already-blind census are free, no counter or
    state row needed; the thread's own age IS how long the auto-act path has been dark,
    and `severity='alarm'` rides the same `drift_alarms` live-desk filter
    `deploy_guard.alarm_schema_drift` already proves, for free. The next NON-blind tick
    resolves it — DELIBERATELY UNLIKE `alarm_schema_drift` (which never auto-resolves,
    because a schema drift needs a human's deploy to fix): a blind census can genuinely
    self-heal tick to tick as OS state changes, so auto-resolving here reports reality
    instead of requiring a human to notice recovery and close it by hand. Both calls are
    try/excepted — a graph hiccup must never fail the tick that already decided whether
    to act."""
    st = settings or get_settings()
    if not st.osiris_fleet_reconcile_enabled:
        return {"enabled": False, "state": "DARK", "folded": [], "dropped": [],
                "note": "the reaper's scheduled leg is dark "
                        "(osiris_fleet_reconcile_enabled=0)"}
    out = await reconcile_execute(actions, actor="cron:fleet_reconcile_heartbeat",
                                  execute=True, projects_root=projects_root,
                                  jobs_home=jobs_home, live_bodies_by_cwd=live_bodies_by_cwd)

    from src.orchestrator.capture import open_thread, resolve_thread

    if out.get("census_blind"):
        state = "BLIND"
        try:
            await open_thread(actions, summary=_BLIND_ALARM_SUMMARY, kind="obligation",
                              severity="alarm", source="cron:fleet_reconcile_heartbeat")
        except Exception:  # noqa: BLE001 — a mint hiccup must not fail the tick's verdict
            pass
    else:
        state = "OVER_CAP" if out.get("over_cap") else "ACTS"
        try:
            await resolve_thread(
                actions, _BLIND_ALARM_SUMMARY,
                because="census recovered — this tick's OS body check succeeded again",
                source="cron:fleet_reconcile_heartbeat")
        except Exception:  # noqa: BLE001 — same discipline: never fail the tick over this
            pass
    return {"enabled": True, "state": state, **out}
