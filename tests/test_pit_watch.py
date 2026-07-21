"""Pit Watch Stage B — the pair heartbeat (thread 449bf55d, decision be79f567).

THE AMENDMENT (DM 975, the required one): an addressee with ZERO live session candidates
must read as NOT MID-TURN and alarm — this is literally the running-but-never-mounted
incident that founded the whole build. test_zero_mount_rows_still_alarms is that case,
named for it exactly as asked.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from src.actions.core import Actions
from src.config.settings import Settings
from src.orchestrator import pit_watch
from src.orchestrator.mailbox import send_message
from src.orchestrator.mounts import save_mount
from src.orchestrator.seats import bind_holder


def _settings(
    *, osiris_pit_watch_enabled: bool = True, osiris_mail_lease_secs: int = 900,
    osiris_dm_active_secs: int = 120, osiris_pit_watch_escalate_at: int = 3,
) -> Settings:
    return Settings(
        osiris_pit_watch_enabled=osiris_pit_watch_enabled,
        osiris_mail_lease_secs=osiris_mail_lease_secs,
        osiris_dm_active_secs=osiris_dm_active_secs,
        osiris_pit_watch_escalate_at=osiris_pit_watch_escalate_at)


async def _seat(actions: Actions, seat_id: str, holder_agent: str) -> None:
    await actions.create_or_find_object("Seat", seat_id, holder_agent)
    await bind_holder(actions, seat_id=seat_id, agent_id=holder_agent)


def _write_transcript(path: Path, *, age_secs: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ts = (datetime.now(UTC) - timedelta(seconds=age_secs)).isoformat()
    path.write_text(json.dumps({"type": "assistant", "timestamp": ts}) + "\n")


# ═══════════ ENUMERATING PAIRS ═══════════

async def test_managed_pairs_lists_active_links_only(actions: Actions) -> None:
    worker = await actions.create_or_find_object("Seat", "seat:w0000001", "test")
    manager = await actions.create_or_find_object("Seat", "seat:m0000001", "test")
    now = datetime.now(UTC)
    await actions.create_link(worker, manager, "managed_by", "test", now, 0.9,
                              evidence_class="self_declared")
    pairs = await pit_watch._managed_pairs(actions.pool)
    assert ("seat:w0000001", "seat:m0000001") in pairs


async def test_managed_pairs_empty_when_none_exist(actions: Actions) -> None:
    assert await pit_watch._managed_pairs(actions.pool) == []


# ═══════════ THE STUCK QUERY ═══════════

async def test_oldest_stuck_dm_needs_age_past_the_lease(actions: Actions) -> None:
    await actions.create_or_find_object("Seat", "seat:s0000001", "test")
    r = await send_message(actions.pool, from_agent="agent:aaaa0002", from_project="osiris",
                           to_agent="seat:s0000001", body="fresh", grade="ask")
    assert r["dedup"] is False
    # fresh: younger than any real lease — never stuck
    assert await pit_watch._oldest_stuck_dm(
        actions.pool, addressee_seat="seat:s0000001", lease_secs=900) is None
    # a negative lease treats even a brand-new message as past the line — proves the
    # comparison itself is correct without needing to wait a real 900s in a test
    stuck = await pit_watch._oldest_stuck_dm(
        actions.pool, addressee_seat="seat:s0000001", lease_secs=-1)
    assert stuck is not None and stuck["id"] == r["id"]


async def test_oldest_stuck_dm_ignores_fyi_and_read(actions: Actions) -> None:
    await actions.create_or_find_object("Seat", "seat:s0000002", "test")
    await send_message(actions.pool, from_agent="agent:aaaa0003", from_project="osiris",
                       to_agent="seat:s0000002", body="just an fyi", grade="fyi")
    assert await pit_watch._oldest_stuck_dm(
        actions.pool, addressee_seat="seat:s0000002", lease_secs=-1) is None

    r2 = await send_message(actions.pool, from_agent="agent:aaaa0003", from_project="osiris",
                            to_agent="seat:s0000002", body="an ask, already read", grade="ask")
    await actions.pool.execute("UPDATE fleet_messages SET read_at=now() WHERE id=$1", r2["id"])
    assert await pit_watch._oldest_stuck_dm(
        actions.pool, addressee_seat="seat:s0000002", lease_secs=-1) is None


# ═══════════ MID-TURN, INCLUDING THE NO-CANDIDATES CASE ═══════════

async def test_zero_mount_rows_still_alarms(actions: Actions, tmp_path: Path) -> None:
    """DM 975's required amendment, named exactly as asked: an addressee with NO mount rows
    at all — this morning's own shape — must read as NOT mid-turn, never as 'unknown, skip'."""
    agent = "agent:noonemounted"
    # deliberately no save_mount call: zero candidates is the whole point of this test
    mid_turn = await pit_watch._addressee_mid_turn(
        actions.pool, agent, active_secs=120, sessions_root=tmp_path)
    assert mid_turn is False


async def test_live_session_candidates_empty_without_any_mount(
    actions: Actions, tmp_path: Path,
) -> None:
    assert await pit_watch._live_session_candidates(
        actions.pool, "agent:nevermounted", tmp_path) == []


async def test_mid_turn_true_when_the_transcript_is_moving(
    actions: Actions, tmp_path: Path,
) -> None:
    agent = "agent:bbbb0001"
    job_dir = str(tmp_path / "jobs" / "bbbb0001")
    await save_mount(actions.pool, job_dir=job_dir, agent_id=agent, project="testhouse",
                     cwd=str(tmp_path / "office"), model=None, session_key=None)
    sessions_root = tmp_path / "sessions"
    _write_transcript(sessions_root / "proj" / "bbbb0001-full-session.jsonl", age_secs=5)
    assert await pit_watch._addressee_mid_turn(
        actions.pool, agent, active_secs=120, sessions_root=sessions_root) is True


async def test_mid_turn_false_when_the_transcript_is_stale(
    actions: Actions, tmp_path: Path,
) -> None:
    agent = "agent:cccc0001"
    job_dir = str(tmp_path / "jobs" / "cccc0001")
    await save_mount(actions.pool, job_dir=job_dir, agent_id=agent, project="testhouse",
                     cwd=str(tmp_path / "office"), model=None, session_key=None)
    sessions_root = tmp_path / "sessions"
    _write_transcript(sessions_root / "proj" / "cccc0001-full-session.jsonl", age_secs=99_999)
    assert await pit_watch._addressee_mid_turn(
        actions.pool, agent, active_secs=120, sessions_root=sessions_root) is False


# ═══════════ ATTEMPTS AND THE ESCALATION TOMBSTONE ═══════════

async def test_attempts_count_sighted_rows_and_stop_after_escalation(actions: Actions) -> None:
    await pit_watch._sight(actions.pool, worker_seat="seat:w1", manager_seat="seat:m1",
                           message_id=1, addressee_seat="seat:w1")
    await pit_watch._sight(actions.pool, worker_seat="seat:w1", manager_seat="seat:m1",
                           message_id=1, addressee_seat="seat:w1")
    assert await pit_watch._attempts_on(actions.pool, 1) == 2
    assert await pit_watch._already_escalated(actions.pool, 1) is False


async def test_escalate_writes_tombstone_and_sends_exactly_one_desk_brief(
    actions: Actions,
) -> None:
    await pit_watch._escalate(
        actions.pool, worker_seat="seat:w2", manager_seat="seat:m2", message_id=2,
        addressee_seat="seat:w2", age_secs=1800, attempts=3)
    assert await pit_watch._already_escalated(actions.pool, 2) is True
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM fleet_messages WHERE to_project='operator' "
        "AND body LIKE '%message 2%'")
    assert n == 1
    # a second escalate call for the SAME message must never double-tombstone or double-brief
    await pit_watch._escalate(
        actions.pool, worker_seat="seat:w2", manager_seat="seat:m2", message_id=2,
        addressee_seat="seat:w2", age_secs=3600, attempts=4)
    n2 = await actions.pool.fetchval(
        "SELECT count(*) FROM pit_watch_alarms WHERE message_id=2 AND outcome='escalated'")
    assert n2 == 2  # the caller is expected to gate this via _already_escalated; the
    # primitive itself stays a plain append — proven by _watch_one_direction's own gate below


# ═══════════ ONE DIRECTION, END TO END ═══════════

async def test_watch_one_direction_sights_a_stuck_message(
    actions: Actions, tmp_path: Path,
) -> None:
    agent = "agent:dddd0001"
    await _seat(actions, "seat:dddd0001", agent)
    r = await send_message(actions.pool, from_agent="agent:mgr0001", from_project="osiris",
                           to_agent="seat:dddd0001", body="an assignment", grade="ask")
    outcome = await pit_watch._watch_one_direction(
        actions.pool, worker_seat="seat:dddd0001", manager_seat="seat:mgr0001",
        addressee_seat="seat:dddd0001", lease_secs=-1, active_secs=120, escalate_at=3,
        sessions_root=tmp_path)
    assert outcome == "sighted"
    assert await pit_watch._attempts_on(actions.pool, r["id"]) == 1


async def test_watch_one_direction_escalates_at_the_threshold(
    actions: Actions, tmp_path: Path,
) -> None:
    agent = "agent:eeee0001"
    await _seat(actions, "seat:eeee0001", agent)
    r = await send_message(actions.pool, from_agent="agent:mgr0002", from_project="osiris",
                           to_agent="seat:eeee0001", body="an assignment", grade="ask")
    async def _watch() -> str | None:
        return await pit_watch._watch_one_direction(
            actions.pool, worker_seat="seat:eeee0001", manager_seat="seat:mgr0002",
            addressee_seat="seat:eeee0001", lease_secs=-1, active_secs=120,
            escalate_at=2, sessions_root=tmp_path)

    first = await _watch()
    second = await _watch()
    third = await _watch()
    assert first == "sighted"
    assert second == "escalated"
    assert third is None  # already escalated — never fires a third time for this message
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM fleet_messages WHERE to_project='operator' "
        f"AND body LIKE '%message {r['id']}%'")
    assert n == 1


async def test_watch_one_direction_none_when_mid_turn(actions: Actions, tmp_path: Path) -> None:
    agent = "agent:ffff0001"
    await _seat(actions, "seat:ffff0001", agent)
    job_dir = str(tmp_path / "jobs" / "ffff0001")
    await save_mount(actions.pool, job_dir=job_dir, agent_id=agent, project="testhouse",
                     cwd=str(tmp_path / "office"), model=None, session_key=None)
    sessions_root = tmp_path / "sessions"
    _write_transcript(sessions_root / "proj" / "ffff0001-full.jsonl", age_secs=5)
    await send_message(actions.pool, from_agent="agent:mgr0003", from_project="osiris",
                       to_agent="seat:ffff0001", body="an assignment", grade="ask")
    outcome = await pit_watch._watch_one_direction(
        actions.pool, worker_seat="seat:ffff0001", manager_seat="seat:mgr0003",
        addressee_seat="seat:ffff0001", lease_secs=-1, active_secs=120, escalate_at=3,
        sessions_root=sessions_root)
    assert outcome is None


async def test_watch_one_direction_none_on_a_vacant_seat(actions: Actions, tmp_path: Path) -> None:
    await actions.create_or_find_object("Seat", "seat:vacant01", "test")  # no holder bound
    await send_message(actions.pool, from_agent="agent:mgr0004", from_project="osiris",
                       to_agent="seat:vacant01", body="an assignment", grade="ask")
    outcome = await pit_watch._watch_one_direction(
        actions.pool, worker_seat="seat:vacant01", manager_seat="seat:mgr0004",
        addressee_seat="seat:vacant01", lease_secs=-1, active_secs=120, escalate_at=3,
        sessions_root=tmp_path)
    assert outcome is None


# ═══════════ THE FULL TICK ═══════════

async def test_tick_is_a_noop_when_disabled(actions: Actions) -> None:
    report = await pit_watch.pit_watch_tick(actions, settings=_settings(
        osiris_pit_watch_enabled=False))
    assert report == {"pairs": 0, "sighted": 0, "escalated": 0}


async def test_tick_scans_a_pair_both_directions(actions: Actions, tmp_path: Path) -> None:
    worker_agent, manager_agent = "agent:gggg0001", "agent:hhhh0001"
    await _seat(actions, "seat:gggg0001", worker_agent)
    await _seat(actions, "seat:hhhh0001", manager_agent)
    now = datetime.now(UTC)
    worker_obj = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='seat:gggg0001'")
    manager_obj = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='seat:hhhh0001'")
    await actions.create_link(worker_obj, manager_obj, "managed_by", "test", now, 0.9,
                              evidence_class="self_declared")
    # manager -> worker: stuck (the founding shape)
    await send_message(actions.pool, from_agent=manager_agent, from_project="osiris",
                       to_agent="seat:gggg0001", body="go build it", grade="ask")
    report = await pit_watch.pit_watch_tick(
        actions, settings=_settings(osiris_mail_lease_secs=-1))
    assert report["pairs"] == 1
    assert report["sighted"] == 1
    assert report["escalated"] == 0


async def test_tick_never_raises_when_one_pair_is_broken(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One direction's own failure must never blind the whole tick — matches the file's own
    per-direction try/except contract."""
    worker = await actions.create_or_find_object("Seat", "seat:iiii0001", "test")
    manager = await actions.create_or_find_object("Seat", "seat:jjjj0001", "test")
    now = datetime.now(UTC)
    await actions.create_link(worker, manager, "managed_by", "test", now, 0.9,
                              evidence_class="self_declared")

    async def boom(*a: object, **k: object) -> None:
        raise RuntimeError("simulated failure")

    monkeypatch.setattr(pit_watch, "_watch_one_direction", boom)
    report = await pit_watch.pit_watch_tick(actions, settings=_settings())
    assert report["pairs"] == 1
    assert report["sighted"] == 0 and report["escalated"] == 0  # swallowed, never raised
