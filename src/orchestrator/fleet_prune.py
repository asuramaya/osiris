"""MECHANICAL FLEET PRUNE (thread 07ca68ca, wave 8 — operator 2026-09-08: "automatic
mechanical fleet hygiene ... pruning/cleaning up tools for seats that are not actually
seats would dramatically improve the ux and mobility").

COMPOSES rather than re-derives: `fleet_reconcile.py` (task #59) already classifies most
of what this thread asked for — `bulk_fold_swarm`/`rollup_office_remount` (identity
folds), `drop_ephemeral_test_cwd` (mount residue against an already-retired project —
this is where "probe or test cwd" mostly lands in practice, since `classification_laws_
heartbeat` runs `apply_project_hygiene_sweep` immediately before this sweep in the same
tick: a stub project retired this cycle already has its residue mounts caught here, same
tick, no gap), and `ghost_gap` (false-live: graph says live, no OS body backs it). This
module adds the two classes that sweep was never built to see (an OS body the graph has
NEVER heard of; a mount row whose own anchor directory is gone from disk), and augments
`bulk_fold_swarm` rows with a `swarm_root_retired` flag when the swarm's own root has no
currently-live mount — Thoth's "swarm child of a retired root" language, folded into the
existing bucket as a descriptive refinement rather than a second acting bucket, because
the same `reconcile_execute` fold already handles it correctly regardless of the flag.

ACTING SCOPE IS DELIBERATELY NARROWER THAN REPORTING SCOPE. `prune_dry_run` reports every
bucket — fleet_reconcile's five plus this module's two — in ONE manifest, because "one
classifier over every session registry row and OS body" is a REPORTING requirement.
`prune_execute` only ever WRITES the two new buckets:

  dead_transcript — `mounts.drop_dead_transcript_mount` (reversible, audited, same shape
                    `drop_dead_project_mount` already proves).
  unclaimed_body  — bound via a plain `save_mount` upsert ONLY when `tree_seat_hint`
                    resolves the body's own cwd to a living seat with a living holder —
                    never a guess, never a mint; the seat and its holder must already be
                    real before this ever writes a row.

It NEVER calls `fleet_reconcile.reconcile_execute` itself. That machinery already has its
own scheduled leg (`fleet_reconcile_heartbeat`) gated behind its own kill switch
(`osiris_fleet_reconcile_enabled`, Thoth's gate DM 2042 — "flipping that flag is a second
signature a human gives separately from approving the diff"). Folding its acting half into
THIS sweep's own always-on heartbeat sibling would silently arm identity-folding on every
fresh install with no separate signature — exactly the birth-defect this house's own kill-
switch discipline exists to prevent. The two buckets this module DOES act on are safe by
construction (a reversible row-scoped delete on a directory that is provably gone; a bind
that only ever writes when a real seat and a real holder already exist) and need no kill
switch of their own, the same "ships mechanically, zero hand on it" bar
`apply_project_hygiene_sweep`/`apply_ghost_house_sweep` already hold themselves to as
`classification_laws_heartbeat`'s other unconditional siblings.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.orchestrator import fleet_reconcile

# the same 15-minute liveness window fleet_reconcile._LIVE_WINDOW_SECS already uses —
# reused by name rather than re-declared, so the two modules can never silently drift on
# what "live" means.
_LIVE_WINDOW_SECS = fleet_reconcile._LIVE_WINDOW_SECS


async def _dead_transcript_mounts(
    pool: asyncpg.Pool, *, exists_fn: Any = None,
) -> list[dict[str, Any]]:
    """Every `agent_mounts` row whose own `job_dir` anchor directory no longer exists on
    disk (thread 07ca68ca's "dead transcript" class) — the whisper's own promise (`~/.
    claude/jobs/<sid8>` names each session it greets) means a gone directory is
    unambiguous: nothing can re-attach to an address that no longer exists. `exists_fn`
    is the injection seam (tests drive it with a fake population, never the real
    filesystem — `census.py`'s own discipline)."""
    exists = exists_fn or (lambda p: Path(p).exists())
    rows = await pool.fetch(
        "SELECT job_dir, agent_id, project, cwd, last_seen FROM agent_mounts "
        "ORDER BY last_seen DESC NULLS LAST")
    out: list[dict[str, Any]] = []
    for r in rows:
        job_dir = r["job_dir"]
        if not job_dir or exists(job_dir):
            continue
        out.append({
            "agent_id": r["agent_id"], "project": r["project"], "cwd": r["cwd"],
            "job_dir": job_dir,
            "last_seen": r["last_seen"].isoformat() if r["last_seen"] else None,
            "bucket": "dead_transcript",
            "rule": f"job_dir {job_dir!r} no longer exists on disk — the session's own "
                    "anchor directory is gone, the row is unreachable residue",
        })
    return out


async def _unclaimed_bodies(
    pool: asyncpg.Pool, *, registry_census_fn: Any = None,
) -> list[dict[str, Any]]:
    """Every VERIFIED live OS body (`mounts.registry_census`'s own harness+/proc
    cross-check) with NO `agent_mounts` row at all — `registry_census`'s own `rowless`
    population, exactly the class a server bounce leaves behind (mounts.py's own module
    docstring: "every bounce wiped the WHOLE fleet's mounts at once"). Each row carries a
    `bind_candidate_handle` when the body's own cwd resolves through `tree_seat_hint`
    (an already-bound `tree_cwd`, or a `.osiris` pin's declared `seat=` line) — a
    resolution, never a guess: `prune_execute` only binds when this AND a living holder
    both already exist. `census.get("blind")` (the harness registry read itself failed)
    reports zero rows rather than guessing — "could not look" must never read as "nothing
    unclaimed", the same law `fleet_reconcile._ghost_flagged_agents` already holds to."""
    from src.orchestrator.mounts import registry_census
    from src.orchestrator.seats import tree_seat_hint

    census_fn = registry_census_fn or registry_census
    census = await census_fn(pool)
    if census.get("blind"):
        return []
    out: list[dict[str, Any]] = []
    for r in census.get("rowless", []):
        cwd = r.get("proc_cwd") or r.get("harness_cwd")
        row: dict[str, Any] = {
            "session_id": r["session_id"], "pid": r.get("pid"), "cwd": cwd,
            "job_dir_key": r["job_dir_key"], "bucket": "unclaimed_body",
            "rule": "a verified live OS body (harness + /proc confirmed, registry_census's "
                    "own 'rowless' population) has no agent_mounts row at all",
        }
        handle = await tree_seat_hint(pool, cwd=cwd) if cwd else None
        if handle:
            row["bind_candidate_handle"] = handle
        out.append(row)
    return out


async def prune_dry_run(
    pool: asyncpg.Pool, *, projects_root: Path | None = None, jobs_home: Path | None = None,
    live_bodies_by_cwd: Any = None, registry_census_fn: Any = None, exists_fn: Any = None,
) -> dict[str, Any]:
    """THE ONE CLASSIFIER, reporting only — never writes. Composes `fleet_reconcile.
    reconcile_dry_run`'s five buckets verbatim with this module's two new ones
    (`dead_transcript`, `unclaimed_body`) into a single manifest, and augments every
    `bulk_fold_swarm` row with `swarm_root_retired: True` when the swarm's own root
    (`into`) has no currently-live `agent_mounts` row — Thoth's "swarm child of a
    retired root" class, reported here as a refinement rather than a bucket of its own,
    since `reconcile_execute` already folds these rows correctly regardless of the flag.
    """
    reconciled = await fleet_reconcile.reconcile_dry_run(
        pool, projects_root=projects_root, jobs_home=jobs_home,
        live_bodies_by_cwd=live_bodies_by_cwd)
    buckets: dict[str, list[dict[str, Any]]] = {
        k: list(v) for k, v in reconciled["buckets"].items()
    }
    buckets["dead_transcript"] = await _dead_transcript_mounts(pool, exists_fn=exists_fn)
    buckets["unclaimed_body"] = await _unclaimed_bodies(
        pool, registry_census_fn=registry_census_fn)

    live_root_ids = {
        str(r["agent_id"]) for r in await pool.fetch(
            "SELECT agent_id FROM agent_mounts WHERE last_seen IS NOT NULL "
            "AND now() - last_seen < make_interval(secs => $1)", float(_LIVE_WINDOW_SECS))
    }
    for row in buckets["bulk_fold_swarm"]:
        into = row.get("into")
        if into and into not in live_root_ids:
            row["swarm_root_retired"] = True

    counts = {k: len(v) for k, v in buckets.items()}
    return {
        "buckets": buckets, "counts": counts, "total": sum(counts.values()),
        "examined": reconciled.get("examined", 0),
        "census_blind": reconciled["census_blind"], "over_cap": reconciled["over_cap"],
        "note": "MECHANICAL FLEET PRUNE — REPORT ONLY. fleet_reconcile's own five buckets "
                "plus dead_transcript and unclaimed_body; bulk_fold_swarm rows carry "
                "swarm_root_retired when their own root has no live mount. Nothing here "
                "acts on fleet_reconcile's own buckets — see prune_execute's docstring.",
    }


async def prune_execute(
    actions: Actions, *, actor: str, execute: bool = False,
    projects_root: Path | None = None, jobs_home: Path | None = None,
    live_bodies_by_cwd: Any = None, registry_census_fn: Any = None, exists_fn: Any = None,
) -> dict[str, Any]:
    """THE ACTING HALF — DRY RUN IS THE DEFAULT (`execute=False`), same convention as
    `reconcile_execute`. Acts ONLY on `dead_transcript` (drop, reversible/audited) and
    `unclaimed_body` (bind, only when a `tree_seat_hint` resolution AND a living seat
    holder both already exist) — see the module docstring for why `fleet_reconcile`'s own
    buckets are deliberately left untouched here, gated instead behind their own
    `fleet_reconcile_heartbeat`/`osiris_fleet_reconcile_enabled` kill switch.

    Re-reads the tray via `prune_dry_run` (never trusts a stale caller-supplied report).
    A single row's drop or bind failing is caught and reported inline, never aborting the
    batch — the same "one bad row must not sink a correct plan" discipline `reconcile_
    execute` already proves. POST-ACT VERIFICATION: re-reads the tray a second time after
    acting and reports before/after counts, proof the acted rows actually left the tray."""
    from src.orchestrator.mounts import drop_dead_transcript_mount, save_mount
    from src.orchestrator.project_identity import project_name_for_disk_path
    from src.orchestrator.seats import seat_by_handle, seat_receipt

    report = await prune_dry_run(
        actions.pool, projects_root=projects_root, jobs_home=jobs_home,
        live_bodies_by_cwd=live_bodies_by_cwd, registry_census_fn=registry_census_fn,
        exists_fn=exists_fn)
    would_drop = [
        {"job_dir": row["job_dir"], "agent_id": row.get("agent_id")}
        for row in report["buckets"]["dead_transcript"]
    ]
    would_bind = [row for row in report["buckets"]["unclaimed_body"]
                  if row.get("bind_candidate_handle")]
    plan: dict[str, Any] = {
        "would_drop_transcripts": would_drop, "would_bind": would_bind,
        "reconcile_buckets_untouched": {
            k: report["counts"][k] for k in
            ("bulk_fold_swarm", "rollup_office_remount", "drop_ephemeral_test_cwd",
             "ghost_gap", "leave_for_human")
        },
        "census_blind": report["census_blind"], "over_cap": report["over_cap"],
        "execute": execute,
    }
    if not execute:
        plan["note"] = "PLAN ONLY — call with execute=True to write. Nothing touched."
        return plan

    jobs_home = jobs_home or Path.home() / ".claude" / "jobs"
    dropped: list[dict[str, Any]] = []
    for item in would_drop:
        try:
            out = await drop_dead_transcript_mount(
                actions, job_dir=item["job_dir"], actor=actor)
        except Exception as exc:  # one bad row must not abort a correct batch
            dropped.append({**item, "error": f"{type(exc).__name__}: {exc}"})
            continue
        dropped.append({**item, **out})

    bound: list[dict[str, Any]] = []
    for row in would_bind:
        try:
            handle = row["bind_candidate_handle"]
            seat = await seat_by_handle(actions.pool, handle)
            if seat is None:
                bound.append({**row, "bound": 0,
                             "reason": f"handle {handle!r} no longer resolves to a "
                                       "living seat"})
                continue
            receipt = await seat_receipt(actions.pool, seat["seat_id"])
            holder = (receipt or {}).get("holder")
            if not holder:
                bound.append({**row, "bound": 0,
                             "reason": f"seat {handle!r} is vacant — nothing to bind "
                                       "the body to"})
                continue
            cwd = row.get("cwd")
            project = await project_name_for_disk_path(actions.pool, cwd) if cwd else None
            job_dir = str(jobs_home / row["job_dir_key"])
            await save_mount(
                actions.pool, job_dir=job_dir, agent_id=holder, project=project,
                cwd=cwd, model=None, session_key=f"sid:{row['session_id']}", alive=True)
            bound.append({**row, "bound": 1, "job_dir": job_dir, "agent_id": holder,
                         "seat_id": seat["seat_id"]})
        except Exception as exc:
            bound.append({**row, "error": f"{type(exc).__name__}: {exc}"})

    after = await prune_dry_run(
        actions.pool, projects_root=projects_root, jobs_home=jobs_home,
        live_bodies_by_cwd=live_bodies_by_cwd, registry_census_fn=registry_census_fn,
        exists_fn=exists_fn)
    plan.update({
        "dropped_transcripts": dropped, "bound": bound,
        "before_counts": report["counts"], "after_counts": after["counts"],
        "note": "EXECUTED — before/after counts prove the acted rows left the tray; "
                "fleet_reconcile's own buckets were never touched by this call.",
    })
    return plan
