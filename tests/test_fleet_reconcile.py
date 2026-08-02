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
    _BATCH_CAP,
    _BLIND_ALARM_SUMMARY,
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


async def test_mount_against_a_merged_project_never_lands_in_drop_ephemeral_test_cwd(
    actions: Actions,
) -> None:
    """THE LIVE FALSE-DROP CATCH (found running this exact dry run against production
    before trusting it, #59's first firing): a project consolidated via a MERGE, not a
    retirement, still has an ACTIVE successor — repo:ByeByte merged into repo:bytebye, the
    live specimen. A mount whose own works_in/governs edges already migrated to the
    successor must never be judged dead off its plain-text agent_mounts.project column,
    which nothing updates on a rename. A merge is not a death."""
    p = actions.pool
    survivor = await actions.create_or_find_object("SoftwareProject", "repo:bytebye",
                                                    "repo:bytebye")
    renamed = await actions.create_or_find_object("SoftwareProject", "repo:ByeByte",
                                                   "repo:ByeByte")
    await actions.merge_objects(survivor, renamed, "test: consolidated under one name",
                               "agent:test-actor")
    await save_mount(p, job_dir="/tmp/jobs/werner01", agent_id="agent:werner01",
                     project="ByeByte", cwd="/home/asuramaya/.osiris/seats/werner",
                     model=None, session_key="whisper:werner01")

    out = await reconcile_dry_run(
        p, live_bodies_by_cwd=_bodies("/home/asuramaya/.osiris/seats/werner"))

    assert not any(r.get("agent_id") == "agent:werner01"
                  for r in out["buckets"]["drop_ephemeral_test_cwd"])
    assert not any(r.get("agent_id") == "agent:werner01"
                  for r in out["buckets"]["ghost_gap"])
    still_there = await p.fetchval(
        "SELECT count(*) FROM agent_mounts WHERE agent_id='agent:werner01'")
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

    # fold_agent's own gate (census a5e53ed8) requires the operator's actor for a real
    # fold — reconcile_execute forwards `actor` unchanged
    out = await reconcile_execute(actions, actor="operator", execute=True,
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
    assert out["state"] == "ACTS"
    assert len(out["dropped"]) == 1
    assert await p.fetchval(
        "SELECT count(*) FROM agent_mounts WHERE agent_id='agent:ghost0001'") == 0


async def test_scheduled_tick_can_fold_because_its_own_actor_is_sanctioned(
    actions: Actions, tmp_path,
) -> None:
    """POSITIVE CONTROL for fold_agent's operator-actor gate (census a5e53ed8): the
    scheduled leg passes `actor="cron:fleet_reconcile_heartbeat"`, not an operator
    sentinel — if fold_agent's gate only recognized `seats._OPERATOR_ACTORS`, the
    already-gated cron tick would ALSO start refusing every fold it tries, breaking a
    working feature to fix an authority hole. This proves the sanctioned exception
    (`folds._SANCTIONED_AUTO_FOLD_ACTOR`) actually lands a fold, not just a drop, through
    the real scheduled path — `osiris_fleet_reconcile_enabled=True` is this tick's own,
    separate authorization; no other actor should be trusted the same way."""
    p = actions.pool
    root = tmp_path / "projects"
    jobs = tmp_path / "jobs"
    slug = root / "-w-cronswarm-repo"
    slug.mkdir(parents=True)
    (slug / "rea1cron-full-session.jsonl").write_text("{}\n")
    await _mk_agent(actions, "agent:a11acron")
    await _mk_agent(actions, "agent:rea1cron")
    await save_mount(p, job_dir=str(jobs / "a11acron"), agent_id="agent:a11acron",
                     project="reconhouse", cwd="/w/cronswarm-repo", model=None,
                     session_key="whisper:a11acron")
    await save_mount(p, job_dir=str(jobs / "rea1cron"), agent_id="agent:rea1cron",
                     project="reconhouse", cwd="/w/cronswarm-repo", model=None,
                     session_key="sid:conn")

    out = await reconcile_scheduled_tick(
        actions, settings=Settings(osiris_fleet_reconcile_enabled=True),
        projects_root=root, jobs_home=jobs,
        live_bodies_by_cwd=_bodies("/w/cronswarm-repo"))

    assert out["state"] == "ACTS"
    assert len(out["folded"]) == 1
    assert out["folded"][0]["result"].get("folded") == "agent:a11acron"
    st = await p.fetchval("SELECT status FROM objects WHERE canonical='agent:a11acron'")
    assert st == "merged"


# ── task #108: the desk receipt, the batch cap, the consecutive-blind alarm ────────────


async def test_scheduled_tick_is_dark_names_its_own_state(actions: Actions) -> None:
    """`state` is returned even when the flag is off — DARK, not inferred from `enabled`
    alone (task #108, ruling 2889's own acceptance test)."""
    out = await reconcile_scheduled_tick(actions, settings=Settings())
    assert out["enabled"] is False
    assert out["state"] == "DARK"


async def test_desk_receipt_fires_with_before_after_counts_when_a_tick_acts(
    actions: Actions, tmp_path,
) -> None:
    """task #108 piece 1: the only prior watch on a real execute was a log line nobody is
    paged by. A tick that actually folds or drops something now also fires a durable,
    addressable operator-desk brief carrying the exact before/after counts."""
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

    # fold_agent's own gate (census a5e53ed8) requires the operator's actor for a real
    # fold — reconcile_execute forwards `actor` unchanged
    out = await reconcile_execute(actions, actor="operator", execute=True,
                                  projects_root=root, jobs_home=jobs,
                                  live_bodies_by_cwd=_bodies("/w/swarm-repo"))

    assert out["desk_brief_id"] is not None
    brief = await p.fetchval(
        "SELECT body FROM fleet_messages WHERE to_project='operator' "
        "ORDER BY id DESC LIMIT 1")
    assert brief and brief.startswith("FLEET-RECONCILE ACTED")
    assert "folded 1, dropped 0" in brief
    assert "before=" in brief and "after=" in brief


async def test_batch_cap_holds_the_whole_tick_and_fires_a_decision_brief(
    actions: Actions,
) -> None:
    """task #108 piece 2: a tick whose actionable rows exceed `_BATCH_CAP` refuses to act
    on ANY of them — not a partial drain — and holds the whole batch to leave_for_human
    through the SAME `_held()` every other reason routes through, firing a
    `desk_kind='decision'` brief instead of the plain FYI: an anomalous batch is the
    signature of a bug in the classifier, not a thing to bulk-act on unwitnessed."""
    p = actions.pool
    proj = await actions.create_or_find_object("SoftwareProject", "repo:deadbatch",
                                                "repo:deadbatch")
    await actions.set_status(proj, "retired", "test: stub cull", "agent:test-actor")
    cwds = []
    for i in range(_BATCH_CAP + 1):
        cwd = f"/tmp/deadbatch{i}"
        cwds.append(cwd)
        await save_mount(p, job_dir=f"/tmp/jobs/deadbatch{i}",
                         agent_id=f"agent:deadbatch{i}", project="deadbatch", cwd=cwd,
                         model=None, session_key=f"whisper:deadbatch{i}")

    out = await reconcile_execute(actions, actor="agent:test-actor", execute=True,
                                  live_bodies_by_cwd=_bodies(*cwds))

    assert out["over_cap"] is True
    assert out["would_drop"] == []  # already re-held before the acting layer ever read it
    assert out["folded"] == [] and out["dropped"] == []
    assert out["desk_brief_id"] is not None
    for i in range(_BATCH_CAP + 1):
        assert await p.fetchval(
            "SELECT count(*) FROM agent_mounts WHERE agent_id=$1",
            f"agent:deadbatch{i}") == 1  # every row is untouched, held not dropped
    brief = await p.fetchval(
        "SELECT body FROM fleet_messages WHERE to_project='operator' "
        "ORDER BY id DESC LIMIT 1")
    assert brief and brief.startswith("FLEET-RECONCILE OVER CAP")


async def test_scheduled_tick_over_cap_state_never_drops_a_row(actions: Actions) -> None:
    p = actions.pool
    proj = await actions.create_or_find_object("SoftwareProject", "repo:deadbatch2",
                                                "repo:deadbatch2")
    await actions.set_status(proj, "retired", "test: stub cull", "agent:test-actor")
    cwds = []
    for i in range(_BATCH_CAP + 1):
        cwd = f"/tmp/deadbatch2-{i}"
        cwds.append(cwd)
        await save_mount(p, job_dir=f"/tmp/jobs/deadbatch2-{i}",
                         agent_id=f"agent:deadbatch2-{i}", project="deadbatch2", cwd=cwd,
                         model=None, session_key=f"whisper:deadbatch2-{i}")

    out = await reconcile_scheduled_tick(
        actions, settings=Settings(osiris_fleet_reconcile_enabled=True),
        live_bodies_by_cwd=_bodies(*cwds))

    assert out["state"] == "OVER_CAP"
    assert out["dropped"] == []
    for i in range(_BATCH_CAP + 1):
        assert await p.fetchval(
            "SELECT count(*) FROM agent_mounts WHERE agent_id=$1",
            f"agent:deadbatch2-{i}") == 1


async def test_consecutive_blind_alarm_opens_on_first_blind_tick_and_resolves_on_recovery(
    actions: Actions,
) -> None:
    """task #108 piece 3: no counter, no state row — `open_thread`'s own idempotency on
    the alarm's fixed summary text does the dedup, and the thread's own age is the
    darkness duration. The next tick the census recovers resolves the SAME thread —
    deliberately unlike `alarm_schema_drift`, which never auto-resolves."""
    out1 = await reconcile_scheduled_tick(
        actions, settings=Settings(osiris_fleet_reconcile_enabled=True),
        live_bodies_by_cwd=_blind())
    assert out1["state"] == "BLIND"

    status = await actions.pool.fetchval(
        "SELECT ca2.value #>> '{}' FROM current_assertions ca1 "
        "JOIN current_assertions ca2 ON ca2.object_id = ca1.object_id "
        "AND ca2.name = 'status' "
        "WHERE ca1.name = 'summary' AND ca1.value #>> '{}' = $1", _BLIND_ALARM_SUMMARY)
    assert status == "open"

    out2 = await reconcile_scheduled_tick(
        actions, settings=Settings(osiris_fleet_reconcile_enabled=True),
        live_bodies_by_cwd=_bodies())
    assert out2["state"] == "ACTS"

    status_after = await actions.pool.fetchval(
        "SELECT ca2.value #>> '{}' FROM current_assertions ca1 "
        "JOIN current_assertions ca2 ON ca2.object_id = ca1.object_id "
        "AND ca2.name = 'status' "
        "WHERE ca1.name = 'summary' AND ca1.value #>> '{}' = $1", _BLIND_ALARM_SUMMARY)
    assert status_after == "resolved"
