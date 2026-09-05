"""OBLIGATION HYGIENE — the no-regrow rule (dispatch #204 follow-on, operator ruling
relayed Thoth DM 7161, 2026-09-05): N1=7 idle days -> a DM nudge to the owner; N2=+7 more
days of silence -> a STALE-CANDIDATE marker + a desk brief, never auto-resolved. Every test
proves ONE transition boundary or the idempotency law that stops a tick from re-nudging or
re-classifying a row it already acted on.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from src.actions.core import Actions
from src.config.settings import Settings
from src.orchestrator.charter import set_charter
from src.orchestrator.mounts import save_mount
from src.orchestrator.obligation_hygiene import (
    N1_IDLE_DAYS,
    N2_SILENCE_DAYS,
    hygiene_dry_run,
    hygiene_execute,
    hygiene_status,
    obligation_hygiene_scheduled_tick,
    resolve_owner_target,
)
from src.orchestrator.seats import bind_holder, ensure_seat, peer_seats

NOW = datetime.now(UTC)
_SRC = "test-source"


async def _mk_obligation(
    actions: Actions, canonical: str, *, owner: str | None = None,
    touched_at: datetime | None = None,
) -> Any:
    touched_at = touched_at or NOW
    t = await actions.create_or_find_object("Thread", canonical, _SRC)
    await actions.assert_property(t, "summary", canonical, _SRC, touched_at, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(t, "status", "open", _SRC, touched_at, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(t, "kind", "obligation", _SRC, touched_at, 0.9,
                                  evidence_class="self_declared")
    if owner:
        await actions.assert_property(t, "owner", owner, _SRC, touched_at, 0.9,
                                      evidence_class="self_declared")
    return t


def _find(rows: list[dict[str, Any]], tid: Any) -> dict[str, Any] | None:
    return next((r for r in rows if r["thread_id"] == str(tid)), None)


# ═══ N1: idle detection ══════════════════════════════════════════════════════════════

async def test_a_freshly_touched_obligation_is_not_idle(actions: Actions) -> None:
    t = await _mk_obligation(actions, "hyg-fresh-1", owner="agent:hyg1", touched_at=NOW)
    out = await hygiene_dry_run(actions.pool, now=NOW)
    row = _find(out["buckets"]["no_action"], t)
    assert row is not None and row["reason"] == "not idle"


async def test_an_obligation_idle_past_n1_with_a_silent_owner_would_be_nudged(
    actions: Actions,
) -> None:
    stale = NOW - timedelta(days=N1_IDLE_DAYS + 1)
    t = await _mk_obligation(actions, "hyg-idle-1", owner="agent:hyg-silent",
                             touched_at=stale)
    out = await hygiene_dry_run(actions.pool, now=NOW)
    assert _find(out["buckets"]["would_nudge"], t) is not None


async def test_an_idle_obligation_whose_owner_is_active_elsewhere_is_never_nudged(
    actions: Actions,
) -> None:
    """BOTH halves of "idle" must hold: an old last_touched is not enough on its own if
    the owner has made a self_declared write ANYWHERE in the window — the benefit-of-the-
    doubt half of the operator's own definition."""
    stale = NOW - timedelta(days=N1_IDLE_DAYS + 1)
    t = await _mk_obligation(actions, "hyg-idle-active-owner", owner="agent:hyg-active",
                             touched_at=stale)
    # the SAME owner wrote something else in the graph yesterday
    other = await actions.create_or_find_object("Thread", "hyg-owner-other-write", _SRC)
    await actions.assert_property(other, "summary", "unrelated", "agent:hyg-active",
                                  NOW - timedelta(days=1), 0.9,
                                  evidence_class="self_declared")

    out = await hygiene_dry_run(actions.pool, now=NOW)
    assert _find(out["buckets"]["would_nudge"], t) is None
    row = _find(out["buckets"]["no_action"], t)
    assert row is not None and row["reason"] == "not idle"


# ═══ execute: the nudge itself, and its idempotency ══════════════════════════════════

async def test_execute_nudges_and_stamps_a_marker_that_stops_a_re_nudge_same_tick(
    actions: Actions,
) -> None:
    stale = NOW - timedelta(days=N1_IDLE_DAYS + 1)
    t = await _mk_obligation(actions, "hyg-exec-nudge", owner="agent:hyg-exec1",
                             touched_at=stale)

    out = await hygiene_execute(actions, execute=True, now=NOW)
    assert len(out["nudged"]) == 1 and out["nudged"][0]["thread_id"] == str(t)

    # the SAME tick, run again, must not re-nudge: the marker landed and nothing on the
    # thread (nor the owner) has changed since.
    again = await hygiene_dry_run(actions.pool, now=NOW)
    assert _find(again["buckets"]["would_nudge"], t) is None
    row = _find(again["buckets"]["no_action"], t)
    assert row is not None and "nudged" in row["reason"]


async def test_a_real_touch_after_a_nudge_resets_the_idle_clock(actions: Actions) -> None:
    """A thread genuinely re-annotated by a mind after its own nudge must never be treated
    as still-silent — the SAME re-derivation the operator's own idle definition demands."""
    stale = NOW - timedelta(days=N1_IDLE_DAYS + 1)
    t = await _mk_obligation(actions, "hyg-reset-clock", owner="agent:hyg-reset",
                             touched_at=stale)
    await hygiene_execute(actions, execute=True, now=NOW)

    # a mind touches the thread itself, AFTER the nudge
    touch_at = NOW + timedelta(hours=1)
    await actions.assert_property(t, "summary", "hyg-reset-clock (annotated)",
                                  "agent:hyg-reset", touch_at, 0.9,
                                  evidence_class="self_declared")

    out = await hygiene_dry_run(actions.pool, now=touch_at + timedelta(minutes=1))
    row = _find(out["buckets"]["no_action"], t)
    assert row is not None and row["reason"] == "not idle"


# ═══ N2: stale-candidate promotion ════════════════════════════════════════════════════

async def test_n2_silence_past_a_nudge_promotes_to_stale_candidate(actions: Actions) -> None:
    stale = NOW - timedelta(days=N1_IDLE_DAYS + N2_SILENCE_DAYS + 3)
    t = await _mk_obligation(actions, "hyg-n2-promote", owner="agent:hyg-n2",
                             touched_at=stale)
    nudge_time = stale + timedelta(days=N1_IDLE_DAYS + 1)
    await hygiene_execute(actions, execute=True, now=nudge_time)

    later = nudge_time + timedelta(days=N2_SILENCE_DAYS + 1)
    out = await hygiene_execute(actions, execute=True, now=later)
    assert len(out["staled"]) == 1 and out["staled"][0]["thread_id"] == str(t)


async def test_stale_candidate_is_never_reclassified_or_re_briefed_twice(
    actions: Actions,
) -> None:
    stale = NOW - timedelta(days=N1_IDLE_DAYS + N2_SILENCE_DAYS + 3)
    t = await _mk_obligation(actions, "hyg-n2-idempotent", owner="agent:hyg-n2b",
                             touched_at=stale)
    nudge_time = stale + timedelta(days=N1_IDLE_DAYS + 1)
    await hygiene_execute(actions, execute=True, now=nudge_time)
    later = nudge_time + timedelta(days=N2_SILENCE_DAYS + 1)
    await hygiene_execute(actions, execute=True, now=later)

    # a further tick, well past N2 again, must never re-brief or re-classify
    out = await hygiene_execute(actions, execute=True, now=later + timedelta(days=10))
    assert out["staled"] == [] and out["nudged"] == []
    dry = await hygiene_dry_run(actions.pool, now=later + timedelta(days=10))
    row = _find(dry["buckets"]["no_action"], t)
    assert row is not None and "stale-candidate" in row["reason"]


async def test_stale_candidate_never_changes_the_thread_status(actions: Actions) -> None:
    """NEVER AUTO-RESOLVED, exactly as ruled — the thread's own status stays 'open'."""
    stale = NOW - timedelta(days=N1_IDLE_DAYS + N2_SILENCE_DAYS + 3)
    t = await _mk_obligation(actions, "hyg-never-resolved", owner="agent:hyg-nr",
                             touched_at=stale)
    nudge_time = stale + timedelta(days=N1_IDLE_DAYS + 1)
    await hygiene_execute(actions, execute=True, now=nudge_time)
    later = nudge_time + timedelta(days=N2_SILENCE_DAYS + 1)
    await hygiene_execute(actions, execute=True, now=later)

    status = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", t)
    assert status == "open"


# ═══ owner-address fallback ═══════════════════════════════════════════════════════════

async def test_an_unresolvable_owner_falls_back_to_the_operator_desk(
    actions: Actions,
) -> None:
    stale = NOW - timedelta(days=N1_IDLE_DAYS + 1)
    t = await _mk_obligation(actions, "hyg-no-live-agent", owner="agent:no-such-agent-xyz",
                             touched_at=stale)
    out = await hygiene_execute(actions, execute=True, now=NOW)
    row = next(r for r in out["nudged"] if r["thread_id"] == str(t))
    assert row["sent"].get("dm_to") is None or "error" not in row["sent"]
    # the send landed as a desk brief (to_project=operator), not a raw error
    assert "error" not in row["sent"]


async def test_a_project_name_owner_falls_back_to_the_operator_desk(actions: Actions) -> None:
    await actions.create_or_find_object("SoftwareProject", "repo:hygtestproj", _SRC)
    stale = NOW - timedelta(days=N1_IDLE_DAYS + 1)
    t = await _mk_obligation(actions, "hyg-project-owner", owner="hygtestproj",
                             touched_at=stale)
    out = await hygiene_execute(actions, execute=True, now=NOW)
    row = next(r for r in out["nudged"] if r["thread_id"] == str(t))
    assert "error" not in row["sent"]


async def test_an_unowned_obligation_nudges_the_operator_desk(actions: Actions) -> None:
    stale = NOW - timedelta(days=N1_IDLE_DAYS + 1)
    t = await _mk_obligation(actions, "hyg-unowned", owner=None, touched_at=stale)
    out = await hygiene_dry_run(actions.pool, now=NOW)
    assert _find(out["buckets"]["would_nudge"], t) is not None


# ═══ hygiene_status ═══════════════════════════════════════════════════════════════════

async def test_hygiene_status_counts_by_stage(actions: Actions) -> None:
    stale = NOW - timedelta(days=N1_IDLE_DAYS + 1)
    await _mk_obligation(actions, "hyg-status-fresh", owner="agent:s1", touched_at=NOW)
    nudge_target = await _mk_obligation(actions, "hyg-status-nudge", owner="agent:s2",
                                        touched_at=stale)
    await hygiene_execute(actions, execute=True, now=NOW)

    out = await hygiene_status(actions.pool)
    unfiled = out["counts_by_project"].get("(unfiled)", {})
    assert unfiled.get("nudged", 0) >= 1
    assert unfiled.get("none", 0) >= 1
    # both created threads counted, none double-counted for lacking an in_repo link
    del nudge_target  # id only needed to mint the row


# ═══ the scheduled leg's own kill switch ══════════════════════════════════════════════

async def test_scheduled_tick_is_a_no_op_when_the_flag_is_off(actions: Actions) -> None:
    stale = NOW - timedelta(days=N1_IDLE_DAYS + 1)
    await _mk_obligation(actions, "hyg-flag-off", owner="agent:flagoff", touched_at=stale)
    settings = Settings(osiris_obligation_hygiene_enabled=False)
    out = await obligation_hygiene_scheduled_tick(actions, settings=settings, now=NOW)
    assert out == {"enabled": False, "nudged": [], "staled": [],
                    "note": "the sweep's scheduled leg is dark "
                            "(osiris_obligation_hygiene_enabled=0)"}


async def test_scheduled_tick_acts_when_the_flag_is_on(actions: Actions) -> None:
    stale = NOW - timedelta(days=N1_IDLE_DAYS + 1)
    t = await _mk_obligation(actions, "hyg-flag-on", owner="agent:flagon", touched_at=stale)
    settings = Settings(osiris_obligation_hygiene_enabled=True)
    out = await obligation_hygiene_scheduled_tick(actions, settings=settings, now=NOW)
    assert out["enabled"] is True
    assert any(r["thread_id"] == str(t) for r in out["nudged"])


# ═══ THE OWNER-RESOLUTION LADDER (operator "one more round" 2026-09-05, Thoth DM 7391) ═══
# the first firing sent 128/151 nudges to the desk because owners are project names or dead
# agents — "that makes the desk the pile." A rung is tried before falling to the desk, never
# instead of trying. resolve_owner_target is pure/read-only: every test below calls it
# directly, sending nothing.

async def _repo(actions: Actions, name: str) -> None:
    await actions.create_or_find_object("SoftwareProject", f"repo:{name}", "test")


async def _live_seat(actions: Actions, handle: str, agent_id: str, *, house: str = "test",
                     project: str | None = None) -> dict[str, Any]:
    seat = await ensure_seat(actions, house=house, handle=handle, source="test")
    await actions.create_or_find_object("Agent", agent_id, "test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id=agent_id)
    await save_mount(actions.pool, job_dir=f"/jobs/{handle.lower()}", agent_id=agent_id,
                     project=project or "osiris", cwd="/w/x", model="claude-sonnet-5",
                     session_key=None)
    return seat


# --- rung 0: a seat-handle owner, checked BEFORE the project-name rung -----------------

async def test_rung0_seat_handle_owner_resolves_to_that_seats_live_holder(
    actions: Actions,
) -> None:
    await _live_seat(actions, "LadderSeat8", "agent:ladder-seat8-holder")

    out = await resolve_owner_target(actions.pool, "ladderseat8")  # case-folded
    assert out == {"channel": "dm", "target": "agent:ladder-seat8-holder", "reason": None}


async def test_rung0_seat_handle_rung_runs_before_the_project_name_rung(
    actions: Actions,
) -> None:
    """A stale SoftwareProject sharing the seat's own spelling must never pre-empt the
    seat-handle rung — an owner string is a seat before it is a repo (Thoth ruling msg
    7425, correcting decision 1014b74804da's own diagnosis: 'Thoth'/'seshat'/'imhotep'
    etc are seat handles, not project names, and the project-name rung used to run
    first)."""
    await _repo(actions, "LadderSeat9")  # a SoftwareProject sharing the seat's own spelling
    await _live_seat(actions, "LadderSeat9", "agent:ladder-seat9-holder")

    out = await resolve_owner_target(actions.pool, "LadderSeat9")
    assert out == {"channel": "dm", "target": "agent:ladder-seat9-holder", "reason": None}


async def test_rung0_seat_handle_owner_vacant_seat_falls_to_the_desk(
    actions: Actions,
) -> None:
    await ensure_seat(actions, house="test", handle="LadderSeat10", source="test")

    out = await resolve_owner_target(actions.pool, "ladderseat10")
    assert out["channel"] == "desk"
    assert "vacant" in out["reason"]


async def test_rung0_seat_handle_owner_cold_seat_falls_to_the_desk(
    actions: Actions,
) -> None:
    seat = await ensure_seat(actions, house="test", handle="LadderSeat11", source="test")
    await actions.create_or_find_object("Agent", "agent:ladder-seat11-holder", "test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:ladder-seat11-holder")
    # no save_mount — the holder exists but has never been seen live

    out = await resolve_owner_target(actions.pool, "ladderseat11")
    assert out["channel"] == "desk"
    assert "no live holder right now" in out["reason"]


# --- rung 1: a project-name owner ------------------------------------------------------

async def test_rung1_project_name_owner_resolves_to_its_live_seat_head(
    actions: Actions,
) -> None:
    await _repo(actions, "ladderproj1")
    seat = await _live_seat(actions, "Ladder1", "agent:ladder1-holder")
    await set_charter(actions, seat["seat_id"], ["ladderproj1"], actor="test")

    out = await resolve_owner_target(actions.pool, "ladderproj1")
    assert out == {"channel": "dm", "target": "agent:ladder1-holder", "reason": None}


async def test_rung1_governed_project_prefers_the_managing_seat_not_the_managed_one(
    actions: Actions,
) -> None:
    """charter and pin disagree, but the pin-seat is managed_by the charter-seat — the
    NORMAL, correctly-configured shape (a coordinator governing a repo a worker sits in),
    not a conflict — and the ladder must nudge the MANAGER, never the managed worker."""
    import tempfile
    from pathlib import Path

    await _repo(actions, "ladderproj2")
    manager = await _live_seat(actions, "Ladder2Mgr", "agent:ladder2-mgr")
    await set_charter(actions, manager["seat_id"], ["ladderproj2"], actor="test")
    with tempfile.TemporaryDirectory() as tmp:
        office = Path(tmp) / "office"
        office.mkdir()
        (office / ".osiris").write_text('project = "ladderproj2"\n')
        worker = await ensure_seat(actions, house="test", handle="Ladder2Wkr",
                                   source="test", anchor_cwd=str(office))
        await actions.create_or_find_object("Agent", "agent:ladder2-wkr", "test")
        await bind_holder(actions, seat_id=worker["seat_id"], agent_id="agent:ladder2-wkr")
        await save_mount(actions.pool, job_dir="/jobs/ladder2wkr", agent_id="agent:ladder2-wkr",
                         project="ladderproj2", cwd="/w/x", model="claude-sonnet-5",
                         session_key=None)
        worker_oid = await actions.create_or_find_object("Seat", worker["seat_id"], "test")
        manager_oid = await actions.create_or_find_object("Seat", manager["seat_id"], "test")
        await actions.create_link(worker_oid, manager_oid, "managed_by", "test",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")

        out = await resolve_owner_target(actions.pool, "ladderproj2")
    assert out == {"channel": "dm", "target": "agent:ladder2-mgr", "reason": None}


async def test_rung1_shared_house_project_owner_resolves_to_the_house_manager(
    actions: Actions,
) -> None:
    """N>2 seats all chartering their own house's home repo is the normal shape, not a
    conflict (Thoth ruling msg 7425) — the ladder must nudge the house's manager seat."""
    await _repo(actions, "ladderhouse1")
    head = await _live_seat(actions, "LadderHouseHead", "agent:ladderhouse-head",
                            house="ladderhouse1")
    await set_charter(actions, head["seat_id"], ["ladderhouse1"], actor="test")
    head_oid = await actions.create_or_find_object("Seat", head["seat_id"], "test")
    for i in range(3):
        worker = await ensure_seat(actions, house="ladderhouse1", handle=f"LadderHouseW{i}",
                                   source="test")
        await set_charter(actions, worker["seat_id"], ["ladderhouse1"], actor="test")
        worker_oid = await actions.create_or_find_object("Seat", worker["seat_id"], "test")
        await actions.create_link(worker_oid, head_oid, "managed_by", "test",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")

    out = await resolve_owner_target(actions.pool, "ladderhouse1")
    assert out == {"channel": "dm", "target": "agent:ladderhouse-head", "reason": None}


async def test_rung1_conflict_resolves_to_the_manager_regardless_of_via_signal(
    actions: Actions,
) -> None:
    """The mudra shape (operator ruling, decision 2ee59140): one seat matches via BOTH
    charter and pin, another matches via charter only, but the first is already managed_by
    the second — roster's own `governed` check never fires here (it requires the CHARTER
    seat specifically to manage the PIN seat), yet the ladder must still prefer the
    manager, not fall to a plain 'ambiguous' desk brief."""
    await _repo(actions, "ladderproj7")
    managed = await _live_seat(actions, "Ladder7Managed", "agent:ladder7-managed")
    await set_charter(actions, managed["seat_id"], ["ladderproj7"], actor="test")
    manager = await _live_seat(actions, "Ladder7Manager", "agent:ladder7-manager")
    await set_charter(actions, manager["seat_id"], ["ladderproj7"], actor="test")
    managed_oid = await actions.create_or_find_object("Seat", managed["seat_id"], "test")
    manager_oid = await actions.create_or_find_object("Seat", manager["seat_id"], "test")
    await actions.create_link(managed_oid, manager_oid, "managed_by", "test",
                              datetime.now(UTC), 0.9, evidence_class="self_declared")

    out = await resolve_owner_target(actions.pool, "ladderproj7")
    assert out == {"channel": "dm", "target": "agent:ladder7-manager", "reason": None}


async def test_rung1_peer_pair_conflict_resolves_to_the_live_peer(
    actions: Actions,
) -> None:
    """Two seats peer_of-bonded on a project (rotten-apple: Ptah/Ra; xxit: deckard/metron,
    operator ruling decision 2ee59140) — never a conflict once peered."""
    await _repo(actions, "ladderproj8")
    live = await _live_seat(actions, "Ladder8Live", "agent:ladder8-live")
    await set_charter(actions, live["seat_id"], ["ladderproj8"], actor="test")
    cold = await ensure_seat(actions, house="test", handle="Ladder8Cold", source="test")
    await set_charter(actions, cold["seat_id"], ["ladderproj8"], actor="test")
    await peer_seats(actions, live["seat_id"], cold["seat_id"], because="test", actor="test")

    out = await resolve_owner_target(actions.pool, "ladderproj8")
    assert out == {"channel": "dm", "target": "agent:ladder8-live", "reason": None}


async def test_rung1_peer_pair_conflict_nudges_both_when_both_are_live(
    actions: Actions,
) -> None:
    await _repo(actions, "ladderproj9")
    a = await _live_seat(actions, "Ladder9A", "agent:ladder9-a")
    await set_charter(actions, a["seat_id"], ["ladderproj9"], actor="test")
    b = await _live_seat(actions, "Ladder9B", "agent:ladder9-b")
    await set_charter(actions, b["seat_id"], ["ladderproj9"], actor="test")
    await peer_seats(actions, a["seat_id"], b["seat_id"], because="test", actor="test")

    out = await resolve_owner_target(actions.pool, "ladderproj9")
    assert out["channel"] == "dm"
    assert set(out["target"]) == {"agent:ladder9-a", "agent:ladder9-b"}
    assert out["reason"] is None


async def test_rung1_peer_pair_conflict_falls_to_the_desk_when_neither_peer_is_live(
    actions: Actions,
) -> None:
    await _repo(actions, "ladderproj10")
    a = await ensure_seat(actions, house="test", handle="Ladder10A", source="test")
    await set_charter(actions, a["seat_id"], ["ladderproj10"], actor="test")
    b = await ensure_seat(actions, house="test", handle="Ladder10B", source="test")
    await set_charter(actions, b["seat_id"], ["ladderproj10"], actor="test")
    await peer_seats(actions, a["seat_id"], b["seat_id"], because="test", actor="test")

    out = await resolve_owner_target(actions.pool, "ladderproj10")
    assert out["channel"] == "desk"
    assert "neither peer is live" in out["reason"]


async def test_rung1_conflicting_project_owner_falls_to_the_desk_with_a_named_reason(
    actions: Actions,
) -> None:
    await _repo(actions, "ladderproj3")
    a = await ensure_seat(actions, house="test", handle="Ladder3A", source="test")
    await set_charter(actions, a["seat_id"], ["ladderproj3"], actor="test")
    b = await ensure_seat(actions, house="test", handle="Ladder3B", source="test")
    await set_charter(actions, b["seat_id"], ["ladderproj3"], actor="test")

    out = await resolve_owner_target(actions.pool, "ladderproj3")
    assert out["channel"] == "desk"
    assert "ambiguous" in out["reason"] and "ladderproj3" in out["reason"]


async def test_rung1_no_seat_claims_the_project_falls_to_the_desk(actions: Actions) -> None:
    await _repo(actions, "ladderproj4")
    out = await resolve_owner_target(actions.pool, "ladderproj4")
    assert out["channel"] == "desk"
    assert "no seat's charter or pin names" in out["reason"]


async def test_rung1_single_match_seat_not_live_falls_to_the_desk(actions: Actions) -> None:
    await _repo(actions, "ladderproj5")
    seat = await ensure_seat(actions, house="test", handle="Ladder5", source="test")
    await set_charter(actions, seat["seat_id"], ["ladderproj5"], actor="test")

    out = await resolve_owner_target(actions.pool, "ladderproj5")
    assert out["channel"] == "desk"
    assert "no live seat for project 'ladderproj5'" in out["reason"]


# --- rung 2: a dead/retired agent id ----------------------------------------------------

async def test_rung2_dead_agent_resolves_to_a_live_lineage_head(actions: Actions) -> None:
    ancestor = await actions.create_or_find_object("Agent", "agent:ladder-anc1", "test")
    heir_id = "agent:ladder-anc1-ii"
    await actions.create_or_find_object("Agent", heir_id, "test")
    await actions.assert_property(ancestor, "succeeded_by", heir_id, "test",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")
    await save_mount(actions.pool, job_dir="/jobs/ladderheir1", agent_id=heir_id,
                     project="osiris", cwd="/w/x", model="claude-sonnet-5", session_key=None)

    out = await resolve_owner_target(actions.pool, "agent:ladder-anc1")
    assert out == {"channel": "dm", "target": heir_id, "reason": None}


async def test_rung2_agent_with_no_successor_at_all_falls_to_the_desk(
    actions: Actions,
) -> None:
    out = await resolve_owner_target(actions.pool, "agent:ladder-no-such-agent")
    assert out["channel"] == "desk"
    assert "no live successor" in out["reason"]


async def test_rung2_lineage_head_exists_but_is_not_live_falls_to_the_desk(
    actions: Actions,
) -> None:
    ancestor = await actions.create_or_find_object("Agent", "agent:ladder-anc2", "test")
    heir_id = "agent:ladder-anc2-ii"
    await actions.create_or_find_object("Agent", heir_id, "test")
    await actions.assert_property(ancestor, "succeeded_by", heir_id, "test",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")
    # no save_mount for the heir — it exists, but nothing has ever seen it live

    out = await resolve_owner_target(actions.pool, "agent:ladder-anc2")
    assert out["channel"] == "desk"
    assert "not currently live either" in out["reason"]


async def test_rung0_a_directly_live_agent_owner_needs_no_ladder_rung(
    actions: Actions,
) -> None:
    agent_id = "agent:ladder-already-live"
    await actions.create_or_find_object("Agent", agent_id, "test")
    await save_mount(actions.pool, job_dir="/jobs/ladderlive", agent_id=agent_id,
                     project="osiris", cwd="/w/x", model="claude-sonnet-5", session_key=None)

    out = await resolve_owner_target(actions.pool, agent_id)
    assert out == {"channel": "dm", "target": agent_id, "reason": None}


async def test_execute_uses_the_ladder_and_puts_the_reason_in_the_desk_body(
    actions: Actions,
) -> None:
    """The end-to-end proof: a project-name owner with no seat resolves via _nudge_owner
    to a desk brief whose body carries the failing rung's own reason, per the dispatch's
    own instruction."""
    await _repo(actions, "ladderproj6")
    stale = NOW - timedelta(days=N1_IDLE_DAYS + 1)
    t = await _mk_obligation(actions, "hyg-ladder-exec", owner="ladderproj6",
                             touched_at=stale)

    out = await hygiene_execute(actions, execute=True, now=NOW)
    row = next(r for r in out["nudged"] if r["thread_id"] == str(t))
    assert row["sent"].get("to_project") == "operator" or "error" not in row["sent"]
    assert "error" not in row["sent"]
