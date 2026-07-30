"""FLEET RECONCILE — the reaper's dry-run sweep (task #59). Every test proves ONE bucket
lands the right row for the right reason, and that reconcile_dry_run() writes nothing beyond
what find_agent_fold_candidates already writes on its own (proposal rows — review-gated,
never executed by this module).
"""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator.fleet_reconcile import reconcile_dry_run
from src.orchestrator.mounts import save_mount


async def _mk_agent(actions: Actions, label: str, project: str = "reconhouse") -> None:
    a = await actions.create_or_find_object("Agent", label, label)
    await actions.assert_property(a, "project", project, label, datetime.now(UTC), 0.9,
                                  evidence_class="self_declared")


async def test_high_confidence_view_alias_lands_in_bulk_fold_swarm(
    actions: Actions, tmp_path,
) -> None:
    p = actions.pool
    root = tmp_path / "projects"
    jobs = tmp_path / "jobs"
    slug = root / "-w-swarm-repo"
    slug.mkdir(parents=True)
    (slug / "rea1baaa-full-session.jsonl").write_text("{}\n")
    await _mk_agent(actions, "agent:a11a5000")
    await _mk_agent(actions, "agent:rea1baaa")
    await save_mount(p, job_dir=str(jobs / "a11a5000"), agent_id="agent:a11a5000",
                     project="reconhouse", cwd="/w/swarm-repo", model=None,
                     session_key="whisper:a11a5000")
    await save_mount(p, job_dir=str(jobs / "rea1baaa"), agent_id="agent:rea1baaa",
                     project="reconhouse", cwd="/w/swarm-repo", model=None,
                     session_key="sid:conn")

    out = await reconcile_dry_run(p, projects_root=root, jobs_home=jobs)

    mine = [r for r in out["buckets"]["bulk_fold_swarm"] if r["dupe"] == "agent:a11a5000"]
    assert mine, out["buckets"]
    assert mine[0]["class"] == "view-alias"
    assert "0.75" in mine[0]["rule"]
    assert out["counts"]["bulk_fold_swarm"] == 1
    # nothing folded — the row is a proposal, not an executed merge
    st = await p.fetchval("SELECT status FROM objects WHERE canonical='agent:a11a5000'")
    assert st == "active"


async def test_charter_match_lands_in_rollup_office_remount(
    actions: Actions, tmp_path,
) -> None:
    p = actions.pool
    root = tmp_path / "projects"
    jobs = tmp_path / "jobs"
    slug = root / "-w-charter-repo"
    slug.mkdir(parents=True)
    (slug / "a10ne111-full.jsonl").write_text("{}\n")
    await _mk_agent(actions, "agent:a10ne111", project="charterrecon")
    seat = await actions.create_or_find_object("Agent", "agent:c4a97e01", "agent:c4a97e01")
    await actions.assert_property(seat, "handle", "Chartreuse", "agent:c4a97e01",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")
    room = await actions.create_or_find_object("SoftwareProject", "repo:charterrecon",
                                               "repo:charterrecon")
    await actions.create_link(seat, room, "works_in", "agent:c4a97e01", datetime.now(UTC), 0.9)
    await save_mount(p, job_dir=str(jobs / "a10ne111"), agent_id="agent:a10ne111",
                     project="charterrecon", cwd="/w/charter-repo", model=None,
                     session_key="whisper:a10ne111")

    out = await reconcile_dry_run(p, projects_root=root, jobs_home=jobs)

    mine = [r for r in out["buckets"]["rollup_office_remount"]
            if r["dupe"] == "agent:a10ne111"]
    assert mine
    assert mine[0]["class"] == "charter-match"


async def test_nuanced_multi_seat_charter_match_leaves_for_human(
    actions: Actions, tmp_path,
) -> None:
    """score 0.55 (several seats share the room) must NOT bulk-act — same bar
    find_agent_fold_candidates already draws for itself, reused here."""
    p = actions.pool
    root = tmp_path / "projects"
    jobs = tmp_path / "jobs"
    slug = root / "-w-nuance-repo"
    slug.mkdir(parents=True)
    (slug / "an0ne222-full.jsonl").write_text("{}\n")
    await _mk_agent(actions, "agent:an0ne222", project="nuancerecon")
    seat_a = await actions.create_or_find_object("Agent", "agent:5eat0001", "agent:5eat0001")
    seat_b = await actions.create_or_find_object("Agent", "agent:5eat0002", "agent:5eat0002")
    for s, label in ((seat_a, "agent:5eat0001"), (seat_b, "agent:5eat0002")):
        await actions.assert_property(s, "handle", label, label, datetime.now(UTC), 0.9,
                                      evidence_class="self_declared")
    room = await actions.create_or_find_object("SoftwareProject", "repo:nuancerecon",
                                               "repo:nuancerecon")
    for s, label in ((seat_a, "agent:5eat0001"), (seat_b, "agent:5eat0002")):
        await actions.create_link(s, room, "works_in", label, datetime.now(UTC), 0.9)
    await save_mount(p, job_dir=str(jobs / "an0ne222"), agent_id="agent:an0ne222",
                     project="nuancerecon", cwd="/w/nuance-repo", model=None,
                     session_key="whisper:an0ne222")

    out = await reconcile_dry_run(p, projects_root=root, jobs_home=jobs)

    mine = [r for r in out["buckets"]["leave_for_human"] if r.get("dupe") == "agent:an0ne222"]
    assert mine, out["buckets"]
    assert "0.55" in mine[0]["rule"] or "< 0.75" in mine[0]["rule"]
    assert not any(r.get("dupe") == "agent:an0ne222"
                  for r in out["buckets"]["rollup_office_remount"])


async def test_mount_against_a_retired_project_lands_in_drop_ephemeral_test_cwd(
    actions: Actions,
) -> None:
    """The exact live specimen (decision c62bf333 — cc-test-target and 17 siblings): a
    project already retired via retire_project, with a mount row still sitting under it —
    residue, never a fold candidate, since the sweep never even looks at dead projects."""
    p = actions.pool
    proj = await actions.create_or_find_object("SoftwareProject", "repo:deadstub",
                                                "repo:deadstub")
    await actions.set_status(proj, "retired", "test: stub cull", "agent:test-actor")
    await save_mount(p, job_dir="/tmp/jobs/ghost0001", agent_id="agent:ghost0001",
                     project="deadstub", cwd="/tmp/deadstub", model=None,
                     session_key="whisper:ghost0001")

    out = await reconcile_dry_run(p)

    mine = [r for r in out["buckets"]["drop_ephemeral_test_cwd"]
            if r["agent_id"] == "agent:ghost0001"]
    assert mine, out["buckets"]
    assert "deadstub" in mine[0]["rule"] and "retired" in mine[0]["rule"]
    # a report, not an act: the mount row and the agent are both untouched
    still_there = await p.fetchval(
        "SELECT count(*) FROM agent_mounts WHERE agent_id='agent:ghost0001'")
    assert still_there == 1


async def test_seatless_anon_in_a_dead_project_gets_one_verdict_not_two(
    actions: Actions, tmp_path,
) -> None:
    """The exact live specimen (agent:357a3407 in cc-test-target, found running this
    module for real): a seatless-and-dead project must land ONLY in
    drop_ephemeral_test_cwd — the more specific verdict — never ALSO in leave_for_human
    via the seatless signal. One row, one bucket, one rule."""
    p = actions.pool
    root = tmp_path / "projects"
    jobs = tmp_path / "jobs"
    slug = root / "-w-deadseatless-repo"
    slug.mkdir(parents=True)
    (slug / "gh0st001-full.jsonl").write_text("{}\n")
    proj = await actions.create_or_find_object("SoftwareProject", "repo:deadseatless",
                                                "repo:deadseatless")
    await actions.set_status(proj, "retired", "test: stub cull", "agent:test-actor")
    await _mk_agent(actions, "agent:gh0st001", project="deadseatless")
    await save_mount(p, job_dir=str(jobs / "gh0st001"), agent_id="agent:gh0st001",
                     project="deadseatless", cwd="/w/deadseatless-repo", model=None,
                     session_key="whisper:gh0st001")

    out = await reconcile_dry_run(p, projects_root=root, jobs_home=jobs)

    assert any(r.get("agent_id") == "agent:gh0st001"
              for r in out["buckets"]["drop_ephemeral_test_cwd"])
    assert not any(r.get("project") == "deadseatless"
                  for r in out["buckets"]["leave_for_human"])


async def test_active_project_mount_never_lands_in_drop_bucket(actions: Actions) -> None:
    p = actions.pool
    await actions.create_or_find_object("SoftwareProject", "repo:livehouse", "repo:livehouse")
    await save_mount(p, job_dir="/tmp/jobs/live0001", agent_id="agent:live0001",
                     project="livehouse", cwd="/w/live", model=None,
                     session_key="whisper:live0001")

    out = await reconcile_dry_run(p)

    assert not any(r.get("agent_id") == "agent:live0001"
                  for r in out["buckets"]["drop_ephemeral_test_cwd"])


async def test_seatless_anon_lands_in_leave_for_human(actions: Actions, tmp_path) -> None:
    p = actions.pool
    root = tmp_path / "projects"
    jobs = tmp_path / "jobs"
    slug = root / "-w-seatless-repo"
    slug.mkdir(parents=True)
    (slug / "an0nseat-full.jsonl").write_text("{}\n")
    await _mk_agent(actions, "agent:an0nseat", project="seatlesshouse")
    await save_mount(p, job_dir=str(jobs / "an0nseat"), agent_id="agent:an0nseat",
                     project="seatlesshouse", cwd="/w/seatless-repo", model=None,
                     session_key="whisper:an0nseat")

    out = await reconcile_dry_run(p, projects_root=root, jobs_home=jobs)

    mine = [r for r in out["buckets"]["leave_for_human"]
            if r.get("project") == "seatlesshouse"]
    assert mine
    assert "visitor-gate" in mine[0]["rule"]


async def test_empty_fleet_reports_all_zero_counts_and_never_errors(actions: Actions) -> None:
    out = await reconcile_dry_run(actions.pool)
    assert out["total"] == sum(out["counts"].values())
    assert set(out["counts"]) == {
        "bulk_fold_swarm", "rollup_office_remount", "drop_ephemeral_test_cwd",
        "leave_for_human",
    }
