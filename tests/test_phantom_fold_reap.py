"""PHANTOM/FOLD BACKLOG REAP (dispatch #185 item (e), ruling 696d302c). Every test proves
ONE bucket lands the right row for the right reason, with special attention to the ZERO
FALSE DROPS bar — the boundary conditions between "auto-act" and "leave_for_human" for
each of the two auto-act buckets, since #59's own reaper found a real false drop in its
first cut and this module is asked to hold the same bar.
"""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.actions.core import Actions
from src.config.settings import Settings
from src.orchestrator.mounts import save_mount
from src.orchestrator.phantom_fold_reap import (
    _BATCH_CAP,
    _BLIND_ALARM_SUMMARY,
    phantom_fold_dry_run,
    phantom_fold_execute,
    phantom_fold_scheduled_tick,
)

NOW = datetime.now(UTC)


async def _mk_agent(actions: Actions, canonical: str, *, false_mint: bool = False,
                    retired: bool = False) -> None:
    a = await actions.create_or_find_object("Agent", canonical, canonical)
    # direct_observation (confidence 0.6), matching the REAL fold write site
    # (_fold_zero_turn_ancestors, agents.py) — reinstate_generation writes at the SAME
    # tier, so a fixture stamped at self_declared's higher 0.9 would let the ORIGINAL
    # false_mint=true assertion keep winning the confidence-ranked tiebreak forever,
    # masking a real reinstate from ever being visible to a later read.
    if false_mint:
        await actions.assert_property(a, "false_mint", True, canonical, NOW, 0.6,
                                      evidence_class="direct_observation")
    if retired:
        await actions.assert_property(a, "retired", True, canonical, NOW, 0.6,
                                      evidence_class="direct_observation")


async def _mk_project(actions: Actions, canonical: str, *, status: str = "active") -> None:
    await actions.create_or_find_object("SoftwareProject", canonical, canonical)
    if status != "active":
        await actions.pool.execute(
            "UPDATE objects SET status=$1 WHERE canonical=$2", status, canonical)


async def _live_works_in(actions: Actions, agent: str, project: str) -> None:
    a = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical=$1", agent)
    p = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical=$1", project)
    await actions.create_link(a, p, "works_in", agent, NOW, 0.9)


def _census_confirms(agent_job_dir_key: str, pid: int = 4242) -> Any:
    """A fake agents_json + read_exe/read_cwd trio confirming a live, harness/proc-
    verified body for exactly this 8-char job_dir key — registry_census's own matching
    convention (Path(job_dir).name against sessionId[:8], both exactly 8 chars)."""
    async def _agents_json(**kw: Any) -> list[dict[str, Any]]:
        return [{"sessionId": f"{agent_job_dir_key}-0000-4000-8000-000000000000",
                 "pid": pid, "cwd": "/repo/demo", "name": "[OS] PhantomTest"}]
    return {
        "agents_json": _agents_json,
        "read_exe": lambda pid: "/home/x/.local/share/claude/versions/2.1.210",
        "read_cwd": lambda pid: "/repo/demo",
    }


async def _empty_census(**kw: Any) -> list[dict[str, Any]]:
    return []


async def _blind_census(**kw: Any) -> None:
    raise OSError("pgrep unavailable")


# ═══ false-mint-live reinstatement ══════════════════════════════════════════════════

async def test_false_mint_live_auto_acts_only_when_registry_census_confirms(
    actions: Actions,
) -> None:
    """THE HIGH-CONFIDENCE CASE: false_mint=true, a fresh agent_mounts row, AND
    registry_census independently confirms a harness/proc-verified live body — both
    signals agree, this is the ONE case narrow enough to auto-reinstate."""
    await _mk_agent(actions, "agent:pf000001", false_mint=True)
    await save_mount(actions.pool, job_dir="/x/jobs/pf000001", agent_id="agent:pf000001",
                     project="demo", cwd="/repo/demo", model=None, session_key=None)

    out = await phantom_fold_dry_run(actions.pool, **_census_confirms("pf000001"))

    rows = out["buckets"]["reinstate_false_mint_live"]
    assert len(rows) == 1 and rows[0]["agent_id"] == "agent:pf000001"
    assert out["counts"]["leave_for_human"] == 0
    assert out["census_blind"] is False


async def test_false_mint_live_never_auto_acts_when_census_disagrees(
    actions: Actions,
) -> None:
    """ZERO FALSE DROPS: the graph says false_mint=true with a fresh mount, but
    registry_census finds NO real harness/proc body backing it — a graph-live claim
    alone is never proof (ghost_gap's own law). Held for a human, never reinstated."""
    await _mk_agent(actions, "agent:pf000002", false_mint=True)
    await save_mount(actions.pool, job_dir="/x/jobs/pf000002", agent_id="agent:pf000002",
                     project="demo", cwd="/repo/demo", model=None, session_key=None)

    out = await phantom_fold_dry_run(actions.pool, agents_json=_empty_census)

    assert out["buckets"]["reinstate_false_mint_live"] == []
    held = [r for r in out["buckets"]["leave_for_human"]
           if r.get("agent_id") == "agent:pf000002"]
    assert len(held) == 1
    assert "does NOT independently confirm" in held[0]["rule"]


async def test_false_mint_live_holds_everything_when_census_is_blind(
    actions: Actions,
) -> None:
    """A blind OS census (pgrep/harness read failure) must never read as 'no ghosts' —
    every reinstate_false_mint_live candidate is held instead of trusted."""
    await _mk_agent(actions, "agent:pf000003", false_mint=True)
    await save_mount(actions.pool, job_dir="/x/jobs/pf000003", agent_id="agent:pf000003",
                     project="demo", cwd="/repo/demo", model=None, session_key=None)

    out = await phantom_fold_dry_run(actions.pool, agents_json=_blind_census)

    assert out["census_blind"] is True
    assert out["buckets"]["reinstate_false_mint_live"] == []
    held = [r for r in out["buckets"]["leave_for_human"]
           if r.get("agent_id") == "agent:pf000003"]
    assert len(held) == 1 and "census is blind" in held[0]["rule"]


async def test_false_mint_live_ignores_a_stale_mount_past_the_live_window(
    actions: Actions,
) -> None:
    """A false_mint agent with NO fresh agent_mounts row at all never enters the
    candidate set in the first place — this bucket is scoped to LIVE claims only."""
    await _mk_agent(actions, "agent:pf000004", false_mint=True)
    # no save_mount call at all — no agent_mounts row, so no live claim exists to check

    out = await phantom_fold_dry_run(actions.pool, agents_json=_empty_census)

    all_ids = {r.get("agent_id") for bucket in out["buckets"].values() for r in bucket}
    assert "agent:pf000004" not in all_ids


async def test_false_mint_live_reinstates_for_real_when_executed(actions: Actions) -> None:
    """phantom_fold_execute(execute=True) actually calls reinstate_generation and the
    before/after tray proves the row left it."""
    await _mk_agent(actions, "agent:pf000005", false_mint=True)
    await save_mount(actions.pool, job_dir="/x/jobs/pf000005", agent_id="agent:pf000005",
                     project="demo", cwd="/repo/demo", model=None, session_key=None)

    out = await phantom_fold_execute(
        actions, actor="test:phantom-fold", execute=True,
        **_census_confirms("pf000005"))

    assert out["execute"] is True
    assert len(out["reinstated"]) == 1
    assert out["reinstated"][0]["result"]["ok"] is True
    assert out["before_counts"]["reinstate_false_mint_live"] == 1
    assert out["after_counts"]["reinstate_false_mint_live"] == 0
    fm = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id="
        "(SELECT id FROM objects WHERE canonical='agent:pf000005') "
        "AND name='false_mint' ORDER BY confidence DESC, observed_at DESC LIMIT 1")
    assert fm == "false"


async def test_phantom_fold_execute_dry_run_is_the_default_and_writes_nothing(
    actions: Actions,
) -> None:
    await _mk_agent(actions, "agent:pf000006", false_mint=True)
    await save_mount(actions.pool, job_dir="/x/jobs/pf000006", agent_id="agent:pf000006",
                     project="demo", cwd="/repo/demo", model=None, session_key=None)

    out = await phantom_fold_execute(
        actions, actor="test:phantom-fold", **_census_confirms("pf000006"))

    assert out["execute"] is False
    assert "reinstated" not in out
    assert len(out["would_reinstate"]) == 1
    fm = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id="
        "(SELECT id FROM objects WHERE canonical='agent:pf000006') "
        "AND name='false_mint' ORDER BY confidence DESC, observed_at DESC LIMIT 1")
    assert fm == "true"  # untouched — a plan, never an act


# ═══ duplicate-works-in cleanup ═════════════════════════════════════════════════════

async def test_duplicate_works_in_auto_acts_on_the_one_unambiguous_dead_target(
    actions: Actions,
) -> None:
    """The ONE safe auto-act shape: exactly one of an agent's live duplicate works_in
    targets is a non-active/non-merged SoftwareProject — unambiguous residue."""
    await _mk_agent(actions, "agent:pf100001")
    await _mk_project(actions, "repo:pf-alive")
    await _mk_project(actions, "repo:pf-dead", status="retired")
    await _live_works_in(actions, "agent:pf100001", "repo:pf-alive")
    await _live_works_in(actions, "agent:pf100001", "repo:pf-dead")
    await save_mount(actions.pool, job_dir="/x/jobs/pf100001", agent_id="agent:pf100001",
                     project="demo", cwd="/repo/demo", model=None, session_key=None)

    out = await phantom_fold_dry_run(actions.pool, agents_json=_empty_census)

    rows = out["buckets"]["drop_dead_project_duplicate_works_in"]
    assert len(rows) == 1
    assert rows[0]["agent_id"] == "agent:pf100001"
    assert rows[0]["stale_project"] == "repo:pf-dead"


async def test_duplicate_works_in_never_treats_a_merge_as_a_death(actions: Actions) -> None:
    """fleet_reconcile's own corrected rule (Werner/repo:bytebye, d1775472): a project
    renamed via merge is not a death — status NOT IN ('active','merged'), never bare
    <> 'active'. Two live targets, one merged, zero dead — leave for human, not
    auto-act."""
    await _mk_agent(actions, "agent:pf100002")
    await _mk_project(actions, "repo:pf-alive2")
    await _mk_project(actions, "repo:pf-merged", status="merged")
    await _live_works_in(actions, "agent:pf100002", "repo:pf-alive2")
    await _live_works_in(actions, "agent:pf100002", "repo:pf-merged")
    await save_mount(actions.pool, job_dir="/x/jobs/pf100002", agent_id="agent:pf100002",
                     project="demo", cwd="/repo/demo", model=None, session_key=None)

    out = await phantom_fold_dry_run(actions.pool, agents_json=_empty_census)

    assert out["buckets"]["drop_dead_project_duplicate_works_in"] == []
    held = [r for r in out["buckets"]["leave_for_human"]
           if r.get("agent_id") == "agent:pf100002"]
    assert len(held) == 1


async def test_duplicate_works_in_never_guesses_among_two_dead_targets(
    actions: Actions,
) -> None:
    """ZERO FALSE DROPS: two or more dead-project targets is ambiguous — which one is
    the RIGHT one to invalidate is not this sweep's call, ever."""
    await _mk_agent(actions, "agent:pf100003")
    await _mk_project(actions, "repo:pf-dead-a", status="retired")
    await _mk_project(actions, "repo:pf-dead-b", status="retired")
    await _live_works_in(actions, "agent:pf100003", "repo:pf-dead-a")
    await _live_works_in(actions, "agent:pf100003", "repo:pf-dead-b")
    await save_mount(actions.pool, job_dir="/x/jobs/pf100003", agent_id="agent:pf100003",
                     project="demo", cwd="/repo/demo", model=None, session_key=None)

    out = await phantom_fold_dry_run(actions.pool, agents_json=_empty_census)

    assert out["buckets"]["drop_dead_project_duplicate_works_in"] == []
    held = [r for r in out["buckets"]["leave_for_human"]
           if r.get("agent_id") == "agent:pf100003"]
    assert len(held) == 1


async def test_duplicate_works_in_ignores_a_dead_agent(actions: Actions) -> None:
    """Scoped to LIVE agents only (the same liveness window every check here shares) —
    no agent_mounts row, no candidate, regardless of how many works_in edges it carries."""
    await _mk_agent(actions, "agent:pf100004")
    await _mk_project(actions, "repo:pf-alive3")
    await _mk_project(actions, "repo:pf-dead3", status="retired")
    await _live_works_in(actions, "agent:pf100004", "repo:pf-alive3")
    await _live_works_in(actions, "agent:pf100004", "repo:pf-dead3")
    # no save_mount — this agent is not live

    out = await phantom_fold_dry_run(actions.pool, agents_json=_empty_census)

    all_ids = {r.get("agent_id") for bucket in out["buckets"].values() for r in bucket}
    assert "agent:pf100004" not in all_ids


async def test_duplicate_works_in_invalidates_for_real_when_executed(
    actions: Actions,
) -> None:
    await _mk_agent(actions, "agent:pf100005")
    await _mk_project(actions, "repo:pf-alive4")
    await _mk_project(actions, "repo:pf-dead4", status="retired")
    await _live_works_in(actions, "agent:pf100005", "repo:pf-alive4")
    await _live_works_in(actions, "agent:pf100005", "repo:pf-dead4")
    await save_mount(actions.pool, job_dir="/x/jobs/pf100005", agent_id="agent:pf100005",
                     project="demo", cwd="/repo/demo", model=None, session_key=None)

    out = await phantom_fold_execute(
        actions, actor="test:phantom-fold", execute=True, agents_json=_empty_census)

    assert len(out["invalidated"]) == 1
    assert "error" not in out["invalidated"][0]["result"]
    assert out["invalidated"][0]["result"]["invalidated"] == "agent:pf100005"
    assert out["invalidated"][0]["result"]["was_working_in"] == "repo:pf-dead4"
    live = await actions.pool.fetch(
        "SELECT t.canonical FROM links l JOIN objects t ON t.id=l.to_id "
        "JOIN objects a ON a.id=l.from_id "
        "WHERE a.canonical='agent:pf100005' AND l.type='works_in' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())")
    assert {r["canonical"] for r in live} == {"repo:pf-alive4"}


# ═══ report-only populations ════════════════════════════════════════════════════════

async def test_parallel_lives_is_always_report_only(actions: Actions) -> None:
    a = await actions.create_or_find_object("Agent", "agent:pf200001", "test")
    await actions.assert_property(a, "parallel_pulse_door", "agent:pf200000", "test", NOW,
                                  0.9, evidence_class="self_declared")
    await actions.assert_property(a, "predecessor_last_seen", "2026-08-01T00:00:00Z",
                                  "test", NOW, 0.9, evidence_class="self_declared")

    out = await phantom_fold_dry_run(actions.pool, agents_json=_empty_census)

    rows = out["buckets"]["parallel_lives"]
    assert any(r["agent_id"] == "agent:pf200001" for r in rows)
    assert "never auto-fold" in rows[0]["rule"]


async def test_half_healed_phantom_threads_are_counted_and_never_acted_on(
    actions: Actions,
) -> None:
    from src.orchestrator.agents import _HALF_HEAL_SRC
    from src.orchestrator.capture import open_thread

    await open_thread(actions, "HALF-HEALED PHANTOM: agent:pf300001 was flagged "
                      "false_mint but its ancestor never unwound", kind="obligation",
                      owner="operator", source=_HALF_HEAL_SRC)

    out = await phantom_fold_dry_run(actions.pool, agents_json=_empty_census)

    rows = out["buckets"]["half_healed_phantom"]
    assert any("agent:pf300001" in (r["summary"] or "") for r in rows)
    assert "never auto-complete" in rows[0]["rule"]


# ═══ batch cap and scheduled tick ═══════════════════════════════════════════════════

async def test_over_cap_holds_both_auto_act_buckets(actions: Actions) -> None:
    for i in range(_BATCH_CAP + 1):
        aid = f"agent:pfcap{i:04d}"
        await _mk_agent(actions, aid, false_mint=True)
        # 8-char job_dir basename EXACTLY (registry_census's own match key: Path(job_dir)
        # .name against sessionId[:8]) — "pfcap0000" is 9 chars and silently fails to
        # match, the recurring 8-char trap this house's own tests keep hitting.
        await save_mount(actions.pool, job_dir=f"/x/jobs/pfcp{i:04d}", agent_id=aid,
                         project="demo", cwd="/repo/demo", model=None, session_key=None)

    async def _agents_json(**kw: Any) -> list[dict[str, Any]]:
        return [{"sessionId": f"pfcp{i:04d}-0000-4000-8000-000000000000",
                 "pid": 1000 + i, "cwd": "/repo/demo", "name": "x"}
                for i in range(_BATCH_CAP + 1)]

    out = await phantom_fold_dry_run(
        actions.pool, agents_json=_agents_json,
        read_exe=lambda pid: "/home/x/.local/share/claude/versions/2.1.210",
        read_cwd=lambda pid: "/repo/demo")

    assert out["over_cap"] is True
    assert out["buckets"]["reinstate_false_mint_live"] == []
    held = [r for r in out["buckets"]["leave_for_human"] if "pfcap" in r.get("agent_id", "")]
    assert len(held) == _BATCH_CAP + 1


async def test_scheduled_tick_is_dark_by_default(actions: Actions) -> None:
    out = await phantom_fold_scheduled_tick(
        actions, settings=Settings(osiris_phantom_fold_reap_enabled=False))
    assert out == {"enabled": False, "state": "DARK", "reinstated": [], "invalidated": [],
                   "note": "the sweep's scheduled leg is dark "
                           "(osiris_phantom_fold_reap_enabled=0)"}


async def test_scheduled_tick_acts_when_enabled(actions: Actions) -> None:
    await _mk_agent(actions, "agent:pf400001", false_mint=True)
    await save_mount(actions.pool, job_dir="/x/jobs/pf400001", agent_id="agent:pf400001",
                     project="demo", cwd="/repo/demo", model=None, session_key=None)

    out = await phantom_fold_scheduled_tick(
        actions, settings=Settings(osiris_phantom_fold_reap_enabled=True),
        **_census_confirms("pf400001"))

    assert out["enabled"] is True and out["state"] == "ACTS"
    assert len(out["reinstated"]) == 1


async def test_scheduled_tick_blind_opens_and_resolves_the_alarm(actions: Actions) -> None:
    await _mk_agent(actions, "agent:pf500001", false_mint=True)
    await save_mount(actions.pool, job_dir="/x/jobs/pf500001", agent_id="agent:pf500001",
                     project="demo", cwd="/repo/demo", model=None, session_key=None)

    blind_out = await phantom_fold_scheduled_tick(
        actions, settings=Settings(osiris_phantom_fold_reap_enabled=True),
        agents_json=_blind_census)
    assert blind_out["state"] == "BLIND"
    live_out = await phantom_fold_scheduled_tick(
        actions, settings=Settings(osiris_phantom_fold_reap_enabled=True),
        **_census_confirms("pf500001"))
    assert live_out["state"] == "ACTS"
    thread_status = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions ca "
        "JOIN objects o ON o.id=ca.object_id "
        "WHERE o.type='Thread' AND ca.name='status' "
        "AND EXISTS (SELECT 1 FROM current_assertions s2 WHERE s2.object_id=o.id "
        "  AND s2.name='summary' AND s2.value #>> '{}' = $1) "
        "ORDER BY ca.confidence DESC, ca.observed_at DESC LIMIT 1",
        _BLIND_ALARM_SUMMARY)
    assert thread_status == "resolved"
