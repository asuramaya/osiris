"""MECHANICAL FLEET PRUNE (thread 07ca68ca, wave 8). Every test proves ONE new bucket
lands the right row for the right reason, and that `prune_execute` acts ONLY on the two
new buckets (dead_transcript, unclaimed_body) — never on fleet_reconcile's own
identity-folding buckets, which stay behind their own kill switch.
"""
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.actions.core import Actions
from src.orchestrator.fleet_prune import prune_dry_run, prune_execute
from src.orchestrator.mounts import save_mount
from src.orchestrator.seats import bind_holder, bind_seat_tree, ensure_seat


async def _mk_agent(actions: Actions, label: str, project: str = "prunehouse") -> None:
    a = await actions.create_or_find_object("Agent", label, label)
    await actions.assert_property(a, "project", project, label, datetime.now(UTC), 0.9,
                                  evidence_class="self_declared")


def _bodies(*cwds: str) -> Any:
    live = {c: [1] for c in cwds}
    return lambda: live


async def test_dead_transcript_bucket_reports_gone_job_dir(
    actions: Actions, tmp_path: Path,
) -> None:
    p = actions.pool
    live_dir = tmp_path / "jobs" / "aaaaaaaa"
    live_dir.mkdir(parents=True)
    gone_dir = tmp_path / "jobs" / "bbbbbbbb"  # never created — a dead anchor
    await _mk_agent(actions, "agent:transcript-live")
    await _mk_agent(actions, "agent:transcript-gone")
    await save_mount(p, job_dir=str(live_dir), agent_id="agent:transcript-live",
                     project="prunehouse", cwd="/w/live", model=None, session_key=None)
    await save_mount(p, job_dir=str(gone_dir), agent_id="agent:transcript-gone",
                     project="prunehouse", cwd="/w/gone", model=None, session_key=None)

    out = await prune_dry_run(p, live_bodies_by_cwd=_bodies())

    dead = {r["job_dir"] for r in out["buckets"]["dead_transcript"]}
    assert str(gone_dir) in dead
    assert str(live_dir) not in dead
    assert out["counts"]["dead_transcript"] == 1


async def test_dead_transcript_drop_is_reversible_and_row_scoped(
    actions: Actions, tmp_path: Path,
) -> None:
    p = actions.pool
    gone_dir = tmp_path / "jobs" / "ccccccc1"
    await _mk_agent(actions, "agent:transcript-drop")
    await save_mount(p, job_dir=str(gone_dir), agent_id="agent:transcript-drop",
                     project="prunehouse", cwd="/w/gone2", model=None, session_key=None)

    out = await prune_execute(actions, actor="test", execute=True,
                              live_bodies_by_cwd=_bodies())

    assert out["execute"] is True
    dropped = out["dropped_transcripts"]
    assert len(dropped) == 1 and dropped[0]["dropped"] == 1
    row = await p.fetchrow("SELECT 1 FROM agent_mounts WHERE job_dir=$1", str(gone_dir))
    assert row is None
    # reversible + audited, same shape drop_dead_project_mount already proves
    audit = await p.fetchrow(
        "SELECT action FROM audit_log WHERE id=$1", dropped[0]["audit_id"])
    assert audit["action"] == "drop_dead_transcript_mount"


async def test_unclaimed_body_binds_when_tree_seat_hint_resolves(
    actions: Actions, tmp_path: Path,
) -> None:
    p = actions.pool
    cwd = tmp_path / "declared-tree"
    cwd.mkdir()
    seat = await ensure_seat(actions, house="prunehouse", handle="Prunerseat", source="test")
    await bind_seat_tree(actions, seat_id=seat["seat_id"], tree_cwd=str(cwd),
                         actor="operator", because="test setup")
    await actions.create_or_find_object(
        "Agent", "agent:prune-holder", "agent:prune-holder")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:prune-holder",
                      source="test")

    def fake_census(pool: Any) -> Any:
        async def _inner(_pool: Any) -> dict[str, Any]:
            return {"blind": False, "rowless": [
                {"session_id": "deadbeef00", "pid": 12345, "job_dir_key": "deadbeef",
                 "proc_cwd": str(cwd), "harness_cwd": str(cwd)},
            ]}
        return _inner

    out = await prune_dry_run(p, registry_census_fn=fake_census(p))
    unclaimed = out["buckets"]["unclaimed_body"]
    assert len(unclaimed) == 1
    assert unclaimed[0]["bind_candidate_handle"] == "Prunerseat"

    jobs_home = tmp_path / "jobs"
    executed = await prune_execute(actions, actor="test", execute=True,
                                   jobs_home=jobs_home,
                                   registry_census_fn=fake_census(p))
    bound = executed["bound"]
    assert len(bound) == 1 and bound[0]["bound"] == 1
    row = await p.fetchrow(
        "SELECT agent_id, project, cwd FROM agent_mounts WHERE job_dir=$1",
        str(jobs_home / "deadbeef"))
    assert row is not None
    assert row["agent_id"] == "agent:prune-holder"


async def test_unclaimed_body_never_binds_without_a_resolution(actions: Actions) -> None:
    p = actions.pool

    def fake_census(pool: Any) -> Any:
        async def _inner(_pool: Any) -> dict[str, Any]:
            return {"blind": False, "rowless": [
                {"session_id": "cafef00d00", "pid": 999, "job_dir_key": "cafef00d",
                 "proc_cwd": "/no/such/declared/tree", "harness_cwd": None},
            ]}
        return _inner

    out = await prune_dry_run(p, registry_census_fn=fake_census(p))
    row = out["buckets"]["unclaimed_body"][0]
    assert "bind_candidate_handle" not in row

    executed = await prune_execute(actions, actor="test", execute=True,
                                   registry_census_fn=fake_census(p))
    assert executed["bound"] == []  # no candidate handle -> nothing attempted, nothing written


async def test_prune_execute_never_touches_reconcile_buckets(
    actions: Actions, tmp_path: Path,
) -> None:
    """A high-confidence bulk_fold_swarm candidate must survive prune_execute untouched —
    fleet_reconcile's own identity-folding buckets stay behind their own kill switch."""
    p = actions.pool
    root = tmp_path / "projects"
    jobs = tmp_path / "jobs"
    slug = root / "-w-prune-swarm-repo"
    slug.mkdir(parents=True)
    (slug / "ea1baaa0-full-session.jsonl").write_text("{}\n")
    await _mk_agent(actions, "agent:pa115000")
    await _mk_agent(actions, "agent:ea1baaa0")
    await save_mount(p, job_dir=str(jobs / "pa115000"), agent_id="agent:pa115000",
                     project="prunehouse", cwd="/w/prune-swarm-repo", model=None,
                     session_key="whisper:pa115000")
    await save_mount(p, job_dir=str(jobs / "ea1baaa0"), agent_id="agent:ea1baaa0",
                     project="prunehouse", cwd="/w/prune-swarm-repo", model=None,
                     session_key="sid:conn")

    out = await prune_execute(actions, actor="test", execute=True,
                              projects_root=root, jobs_home=jobs,
                              live_bodies_by_cwd=_bodies("/w/prune-swarm-repo"))

    # the candidate is untouched by THIS call (no fold, no unfold) — still active
    st = await p.fetchval("SELECT status FROM objects WHERE canonical='agent:pa115000'")
    assert st == "active"
    assert out["reconcile_buckets_untouched"]["bulk_fold_swarm"] == 1


async def test_swarm_root_retired_flag_augments_bulk_fold_swarm(
    actions: Actions, tmp_path: Path,
) -> None:
    p = actions.pool
    root = tmp_path / "projects"
    jobs = tmp_path / "jobs"
    slug = root / "-w-retired-root-repo"
    slug.mkdir(parents=True)
    (slug / "fa1baaa1-full-session.jsonl").write_text("{}\n")
    await _mk_agent(actions, "agent:pa225001")
    await _mk_agent(actions, "agent:fa1baaa1")
    await save_mount(p, job_dir=str(jobs / "pa225001"), agent_id="agent:pa225001",
                     project="prunehouse", cwd="/w/retired-root-repo", model=None,
                     session_key="whisper:pa225001")
    # the root's own mount row is suspended (last_seen far in the past) -- no live mount
    await save_mount(p, job_dir=str(jobs / "fa1baaa1"), agent_id="agent:fa1baaa1",
                     project="prunehouse", cwd="/w/retired-root-repo", model=None,
                     session_key="sid:conn", alive=False)

    out = await prune_dry_run(p, projects_root=root, jobs_home=jobs,
                              live_bodies_by_cwd=_bodies("/w/retired-root-repo"))

    mine = [r for r in out["buckets"]["bulk_fold_swarm"] if r["dupe"] == "agent:pa225001"]
    assert mine and mine[0].get("swarm_root_retired") is True
