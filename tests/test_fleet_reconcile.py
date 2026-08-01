"""FLEET RECONCILE — the reaper's dry-run sweep (task #59). Every test proves ONE bucket
lands the right row for the right reason, and that reconcile_dry_run() writes nothing beyond
what find_agent_fold_candidates already writes on its own (proposal rows — review-gated,
never executed by this module).
"""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.actions.core import Actions
from src.config.settings import Settings
from src.orchestrator.fleet_reconcile import (
    reconcile_dry_run,
    reconcile_execute,
    reconcile_scheduled_tick,
)
from src.orchestrator.mounts import save_mount


async def _mk_agent(actions: Actions, label: str, project: str = "reconhouse") -> None:
    a = await actions.create_or_find_object("Agent", label, label)
    await actions.assert_property(a, "project", project, label, datetime.now(UTC), 0.9,
                                  evidence_class="self_declared")


def _bodies(*cwds: str) -> Any:
    """A fake live_bodies_by_cwd: real OS bodies at exactly these cwds — every OTHER test's
    synthetic mount rows would otherwise all ghost-flag against the REAL OS census (which
    genuinely backs none of them), so any test whose row must clear its OWN classification
    (not land in ghost_gap by construction) injects this for its own cwd(s)."""
    live = {c: [1] for c in cwds}
    return lambda: live


def _blind() -> Any:
    """A fake live_bodies_by_cwd reporting the census itself failed (pgrep unavailable) —
    None, never an empty dict; the two must stay distinguishable (sweep_ghost_doors' own
    law, reused here)."""
    return lambda: None


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

    out = await reconcile_dry_run(p, projects_root=root, jobs_home=jobs,
                                  live_bodies_by_cwd=_bodies("/w/swarm-repo"))

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

    out = await reconcile_dry_run(p, projects_root=root, jobs_home=jobs,
                                  live_bodies_by_cwd=_bodies("/w/charter-repo"))

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

    out = await reconcile_dry_run(p, projects_root=root, jobs_home=jobs,
                                  live_bodies_by_cwd=_bodies("/w/nuance-repo"))

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

    out = await reconcile_dry_run(p, live_bodies_by_cwd=_bodies("/tmp/deadstub"))

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

    out = await reconcile_dry_run(p, projects_root=root, jobs_home=jobs,
                                  live_bodies_by_cwd=_bodies("/w/deadseatless-repo"))

    assert any(r.get("agent_id") == "agent:gh0st001"
              for r in out["buckets"]["drop_ephemeral_test_cwd"])
    assert not any(r.get("project") == "deadseatless"
                  for r in out["buckets"]["leave_for_human"])


# ── ghost_gap (thread 04ad4bb8, Thoth DM 2813's hard acceptance gate) ──────────────────


async def test_a_graph_live_mount_with_no_os_body_lands_in_ghost_gap(actions: Actions) -> None:
    """THE FIFTH CLASS itself: a mount row with a fresh last_seen (graph-live) but no real
    OS process backing its cwd — the ballgem specimen (thread 04ad4bb8) — must appear
    somewhere, not vanish. No fold candidate here at all (no Agent object minted for this
    label), no dead project either — before this fix, the row was examined by NOTHING."""
    p = actions.pool
    await save_mount(p, job_dir="/tmp/jobs/phantom1", agent_id="agent:phantom1",
                     project="ballgem", cwd="/w/ballgem", model=None,
                     session_key="whisper:phantom1")

    out = await reconcile_dry_run(p, live_bodies_by_cwd=_bodies())  # no real body anywhere

    mine = [r for r in out["buckets"]["ghost_gap"] if r["agent_id"] == "agent:phantom1"]
    assert mine, out["buckets"]
    assert "no OS body" in mine[0]["rule"]
    assert out["census_blind"] is False


async def test_a_ghost_flagged_fold_candidate_overrides_bulk_fold_swarm(
    actions: Actions, tmp_path,
) -> None:
    """A row that WOULD have cleared bulk_fold_swarm's own 0.75 bar (the exact fixture
    from test_high_confidence_view_alias_lands_in_bulk_fold_swarm) must land in ghost_gap
    instead when no OS body backs it — a phantom's other signals cannot be trusted either,
    once the one independently-checkable signal has already failed."""
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

    out = await reconcile_dry_run(p, projects_root=root, jobs_home=jobs,
                                  live_bodies_by_cwd=_bodies())  # no body — both are ghosts

    assert not any(r["dupe"] == "agent:a11a5000"
                  for r in out["buckets"]["bulk_fold_swarm"])
    ghosted = [r for r in out["buckets"]["ghost_gap"] if r.get("dupe") == "agent:a11a5000"]
    assert ghosted, out["buckets"]
    assert ghosted[0]["class"] == "view-alias"  # the original classification survives...
    assert ghosted[0]["bucket"] == "ghost_gap"  # ...but the bucket does not


async def test_a_ghost_flagged_dead_project_mount_overrides_drop_ephemeral(
    actions: Actions,
) -> None:
    """The exact fixture from test_mount_against_a_retired_project_lands_in_drop_ephemeral_
    test_cwd, but bodyless: drop_ephemeral_test_cwd's own bar is cleared, and ghost status
    still overrides it — the row that was ACTUALLY dropped live this session
    (agent:357a3407) was exactly this shape, verified bodyless by hand before the drop."""
    p = actions.pool
    proj = await actions.create_or_find_object("SoftwareProject", "repo:deadstub",
                                                "repo:deadstub")
    await actions.set_status(proj, "retired", "test: stub cull", "agent:test-actor")
    await save_mount(p, job_dir="/tmp/jobs/ghost0001", agent_id="agent:ghost0001",
                     project="deadstub", cwd="/tmp/deadstub", model=None,
                     session_key="whisper:ghost0001")

    out = await reconcile_dry_run(p, live_bodies_by_cwd=_bodies())  # no body

    assert out["buckets"]["drop_ephemeral_test_cwd"] == []
    mine = [r for r in out["buckets"]["ghost_gap"] if r["agent_id"] == "agent:ghost0001"]
    assert mine, out["buckets"]


async def test_a_body_backed_mount_never_ghost_flags(actions: Actions) -> None:
    """The negative control: a mount whose cwd DOES have a real OS body must never land in
    ghost_gap, proving the check is a genuine cross-reference and not a blanket flag."""
    p = actions.pool
    await save_mount(p, job_dir="/tmp/jobs/reallive1", agent_id="agent:reallive1",
                     project="reallive", cwd="/w/reallive", model=None,
                     session_key="whisper:reallive1")

    out = await reconcile_dry_run(p, live_bodies_by_cwd=_bodies("/w/reallive"))

    assert not any(r.get("agent_id") == "agent:reallive1"
                  for r in out["buckets"]["ghost_gap"])


async def test_a_blind_census_holds_every_auto_act_row_in_leave_for_human(
    actions: Actions, tmp_path,
) -> None:
    """SWEEP_GHOST_DOORS' OWN LAW, reused: 'could not look' must never read as 'no ghosts'.
    When the OS census itself fails (pgrep unavailable), every row that would have cleared
    an auto-act bucket is held in leave_for_human instead, named as blind-held — proven
    against BOTH auto-act classes at once (a fold candidate and a dead-project mount)."""
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
    proj = await actions.create_or_find_object("SoftwareProject", "repo:deadstub",
                                                "repo:deadstub")
    await actions.set_status(proj, "retired", "test: stub cull", "agent:test-actor")
    await save_mount(p, job_dir="/tmp/jobs/ghost0001", agent_id="agent:ghost0001",
                     project="deadstub", cwd="/tmp/deadstub", model=None,
                     session_key="whisper:ghost0001")

    out = await reconcile_dry_run(p, projects_root=root, jobs_home=jobs,
                                  live_bodies_by_cwd=_blind())

    assert out["census_blind"] is True
    assert out["buckets"]["bulk_fold_swarm"] == []
    assert out["buckets"]["drop_ephemeral_test_cwd"] == []
    assert out["buckets"]["ghost_gap"] == []  # blind: no ghost VERDICT either, held not flagged
    held_ids = {r.get("dupe") or r.get("agent_id") for r in out["buckets"]["leave_for_human"]}
    assert {"agent:a11a5000", "agent:ghost0001"} <= held_ids
    held_swarm = next(r for r in out["buckets"]["leave_for_human"]
                      if r.get("dupe") == "agent:a11a5000")
    assert "would be bulk_fold_swarm" in held_swarm["rule"]
    assert "blind" in held_swarm["rule"]


async def test_reconcile_execute_never_acts_while_the_census_is_blind(
    actions: Actions, tmp_path,
) -> None:
    """The acting half needs no code of its own to stay safe here — it only ever reads
    reconcile_dry_run's own buckets, so a blind census holding everything in
    leave_for_human upstream is sufficient. Proven at the execute layer, not just the
    dry-run layer, since that is the layer that would actually have written something."""
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

    out = await reconcile_execute(actions, actor="agent:test-actor", execute=True,
                                  projects_root=root, jobs_home=jobs,
                                  live_bodies_by_cwd=_blind())

    assert out["folded"] == [] and out["dropped"] == []
    st = await p.fetchval("SELECT status FROM objects WHERE canonical='agent:a11a5000'")
    assert st == "active"  # never folded — the blind census held it back


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
    out = await reconcile_dry_run(actions.pool, live_bodies_by_cwd=_bodies())
    assert out["total"] == sum(out["counts"].values())
    assert set(out["counts"]) == {
        "bulk_fold_swarm", "rollup_office_remount", "drop_ephemeral_test_cwd",
        "ghost_gap", "leave_for_human",
    }
    assert out["census_blind"] is False


# ── reconcile_execute (task #59 phase 2) ────────────────────────────────────────────────


async def test_execute_default_is_plan_only_and_writes_nothing(
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

    plan = await reconcile_execute(actions, actor="agent:test-actor",
                                   projects_root=root, jobs_home=jobs,
                                   live_bodies_by_cwd=_bodies("/w/swarm-repo"))

    assert plan["execute"] is False
    assert any(f["dupe"] == "agent:a11a5000" for f in plan["would_fold"])
    assert "folded" not in plan and "dropped" not in plan
    st = await p.fetchval("SELECT status FROM objects WHERE canonical='agent:a11a5000'")
    assert st == "active"  # unfolded — a plan writes nothing


async def test_execute_true_folds_the_high_confidence_view_alias(
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

    out = await reconcile_execute(actions, actor="agent:test-actor", execute=True,
                                  projects_root=root, jobs_home=jobs,
                                  live_bodies_by_cwd=_bodies("/w/swarm-repo"))

    assert len(out["folded"]) == 1
    assert out["folded"][0]["result"].get("folded") == "agent:a11a5000"
    st = await p.fetchval("SELECT status FROM objects WHERE canonical='agent:a11a5000'")
    assert st == "merged"
    assert out["before_counts"]["bulk_fold_swarm"] == 1
    assert out["after_counts"]["bulk_fold_swarm"] == 0  # proof it left the tray, not trust


async def test_execute_true_drops_the_dead_project_mount_leaves_the_agent_alone(
    actions: Actions,
) -> None:
    p = actions.pool
    proj = await actions.create_or_find_object("SoftwareProject", "repo:deadstub",
                                                "repo:deadstub")
    await actions.set_status(proj, "retired", "test: stub cull", "agent:test-actor")
    await save_mount(p, job_dir="/tmp/jobs/ghost0001", agent_id="agent:ghost0001",
                     project="deadstub", cwd="/tmp/deadstub", model=None,
                     session_key="whisper:ghost0001")

    out = await reconcile_execute(actions, actor="agent:test-actor", execute=True,
                                  live_bodies_by_cwd=_bodies("/tmp/deadstub"))

    assert len(out["dropped"]) == 1
    assert out["dropped"][0]["rows_deleted"] == 1
    assert await p.fetchval(
        "SELECT count(*) FROM agent_mounts WHERE agent_id='agent:ghost0001'") == 0
    # a drop releases the RESIDUE ROW only — no Agent object was ever minted here, and
    # this proves reconcile_execute never mints or touches one on its own
    st = await p.fetchval("SELECT status FROM objects WHERE canonical='agent:ghost0001'")
    assert st is None


async def test_execute_true_never_touches_leave_for_human(
    actions: Actions, tmp_path,
) -> None:
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

    out = await reconcile_execute(actions, actor="agent:test-actor", execute=True,
                                  projects_root=root, jobs_home=jobs)

    assert out["left_for_human"] >= 1
    assert out["folded"] == []
    assert out["dropped"] == []
    st = await p.fetchval("SELECT status FROM objects WHERE canonical='agent:an0nseat'")
    assert st == "active"
    assert await p.fetchval(
        "SELECT count(*) FROM agent_mounts WHERE agent_id='agent:an0nseat'") == 1


# ── reconcile_scheduled_tick (the cron leg's own kill switch) ──────────────────────────


async def test_scheduled_tick_is_dark_by_default_and_writes_nothing(
    actions: Actions,
) -> None:
    """osiris_fleet_reconcile_enabled defaults False (Settings()'s own default, matching
    the field declared in settings.py) — the scheduled leg is inert out of the box, no
    override needed to prove it."""
    p = actions.pool
    proj = await actions.create_or_find_object("SoftwareProject", "repo:deadstub",
                                                "repo:deadstub")
    await actions.set_status(proj, "retired", "test: stub cull", "agent:test-actor")
    await save_mount(p, job_dir="/tmp/jobs/ghost0001", agent_id="agent:ghost0001",
                     project="deadstub", cwd="/tmp/deadstub", model=None,
                     session_key="whisper:ghost0001")

    out = await reconcile_scheduled_tick(actions, settings=Settings())

    assert out["enabled"] is False
    assert out["folded"] == [] and out["dropped"] == []
    assert await p.fetchval(
        "SELECT count(*) FROM agent_mounts WHERE agent_id='agent:ghost0001'") == 1


async def test_scheduled_tick_acts_once_the_flag_is_flipped_on(actions: Actions) -> None:
    p = actions.pool
    proj = await actions.create_or_find_object("SoftwareProject", "repo:deadstub",
                                                "repo:deadstub")
    await actions.set_status(proj, "retired", "test: stub cull", "agent:test-actor")
    await save_mount(p, job_dir="/tmp/jobs/ghost0001", agent_id="agent:ghost0001",
                     project="deadstub", cwd="/tmp/deadstub", model=None,
                     session_key="whisper:ghost0001")

    out = await reconcile_scheduled_tick(
        actions, settings=Settings(osiris_fleet_reconcile_enabled=True),
        live_bodies_by_cwd=_bodies("/tmp/deadstub"))

    assert out["enabled"] is True
    assert len(out["dropped"]) == 1
    assert await p.fetchval(
        "SELECT count(*) FROM agent_mounts WHERE agent_id='agent:ghost0001'") == 0
