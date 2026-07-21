"""The stop-hook offload ritual (queue item 4, #49 piece 3) — above the ONE context
authority's alarm line, a QUIET stop is refused ONCE, naming what this session left
unwritten and pointing at charter.md, then never blocked again for that session.

_offload_verdict is the whole policy, pure (no I/O, no clock) — the acceptance criteria
(a)-(d) are direct unit tests against it. Everything around it (occupancy off the
transcript, the box-checks against a real graph, the block-once marker on disk) is I/O
this file exercises against a real DB and a real filesystem, the same discipline the rest
of the house's tests use.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import scripts.osiris_stophook as stophook
from src.actions.core import Actions
from src.orchestrator.capture import open_thread, record_decision
from src.orchestrator.mailbox import send_message
from src.orchestrator.mounts import save_mount
from src.orchestrator.seats import bind_holder

# ═══════════ THE PURE POLICY — acceptance (a)-(d) ═══════════

def test_a_above_alarm_and_unwritten_blocks_naming_the_boxes() -> None:
    v = stophook._offload_verdict(
        pct=85, window_assumed=False, already_blocked=False,
        boxes={"decisions recorded this session": False,
               "charter.md touched this session": True})
    assert v is not None and v["decision"] == "block"
    assert "decisions recorded this session" in v["reason"]
    assert "charter.md" in v["reason"]  # points at the offload target regardless
    # a SATISFIED box is never named as missing
    assert "charter.md touched this session:" not in v["reason"]


def test_b_a_second_stop_this_session_is_always_allowed() -> None:
    """already_blocked short-circuits everything else — the escape hatch."""
    assert stophook._offload_verdict(
        pct=99, window_assumed=False, already_blocked=True,
        boxes={"anything": False}) is None


def test_c_below_the_alarm_line_never_blocks() -> None:
    assert stophook._offload_verdict(
        pct=stophook.ALARM_PCT - 1, window_assumed=False, already_blocked=False,
        boxes={"anything": False}) is None


def test_d_an_unknown_or_assumed_window_never_blocks() -> None:
    """The Anubis VII false-eulogy law (msg 127): never alarm on a guessed denominator."""
    assert stophook._offload_verdict(
        pct=None, window_assumed=True, already_blocked=False,
        boxes={"anything": False}) is None
    assert stophook._offload_verdict(
        pct=95, window_assumed=True, already_blocked=False,
        boxes={"anything": False}) is None


def test_nothing_missing_never_blocks_even_above_the_line() -> None:
    """Fail open per box: True (satisfied) or None (unevaluable) both mean 'nothing to
    enforce' — a refusal fires only when something is genuinely, provably unwritten."""
    assert stophook._offload_verdict(
        pct=95, window_assumed=False, already_blocked=False,
        boxes={"a": True, "b": None}) is None
    assert stophook._offload_verdict(
        pct=95, window_assumed=False, already_blocked=False, boxes={}) is None
    assert stophook._offload_verdict(
        pct=95, window_assumed=False, already_blocked=False, boxes=None) is None


# ═══════════ THE BLOCK-ONCE MARKER ═══════════

def test_marker_lifecycle(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    sid = "deadbeef-0000-4000-8000-000000000000"
    assert stophook._offload_already_blocked(sid) is False
    stophook._offload_mark_blocked(sid)
    assert stophook._offload_already_blocked(sid) is True


def test_marker_needs_a_trustworthy_session_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    assert stophook._offload_marker("short") is None
    assert stophook._offload_already_blocked("short") is False
    stophook._offload_mark_blocked("short")  # a no-op, never raises


# ═══════════ THE CHARTER-FILE WITNESS ═══════════

def test_charter_touched_absent_file_cannot_be_evaluated(tmp_path: Path) -> None:
    """No charter.md here at all — a repo cwd, not an office — fails open, never punished."""
    assert stophook._charter_touched(str(tmp_path), datetime.now(UTC)) is None


def test_charter_touched_checks_mtime_against_session_start(tmp_path: Path) -> None:
    charter = tmp_path / "charter.md"
    charter.write_text("# notes\n")
    now = datetime.now(UTC)
    assert stophook._charter_touched(str(tmp_path), now - timedelta(minutes=5)) is True
    assert stophook._charter_touched(str(tmp_path), now + timedelta(minutes=5)) is False


# ═══════════ OCCUPANCY, OFF THE ONE AUTHORITY ═══════════

def _write_usage(path: Path, used: int) -> None:
    path.write_text(json.dumps({
        "type": "assistant", "isSidechain": False,
        "message": {"model": "claude-sonnet-5", "usage": {
            "input_tokens": used, "cache_read_input_tokens": 0,
            "cache_creation_input_tokens": 0, "output_tokens": 10}}}) + "\n")


def test_offload_pct_uses_the_window_hint_when_present(tmp_path: Path) -> None:
    t = tmp_path / "t.jsonl"
    _write_usage(t, 90_000)
    pct, assumed = stophook._offload_pct(
        {"transcript_path": str(t), "model": {"id": "claude-sonnet-5"}}, 100_000)
    assert pct == 90 and assumed is False


def test_offload_pct_falls_back_to_window_for_without_a_hint(tmp_path: Path) -> None:
    t = tmp_path / "t.jsonl"
    _write_usage(t, 100_000)
    pct, assumed = stophook._offload_pct(
        {"transcript_path": str(t), "model": {"id": "claude-sonnet-5"}}, None)
    assert pct == 50 and assumed is True  # 200k default, flagged assumed


def test_offload_pct_unreadable_transcript_is_unknown_not_zero(tmp_path: Path) -> None:
    pct, assumed = stophook._offload_pct(
        {"transcript_path": str(tmp_path / "missing.jsonl")}, None)
    assert pct is None and assumed is True


def test_offload_pct_no_transcript_path_is_unknown() -> None:
    assert stophook._offload_pct({}, None) == (None, True)


# ═══════════ THE BOXES, AGAINST A REAL GRAPH ═══════════

async def test_offload_boxes_detects_decisions_threads_charter_and_succession(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pg_dsn: str,
) -> None:
    monkeypatch.setattr(stophook, "DSN", pg_dsn)
    now = datetime.now(UTC)
    mounted_at = now - timedelta(minutes=10)
    agent = "agent:0ffab001-ii"
    a = await actions.create_or_find_object("Agent", agent, agent)
    # a minted heir — the fourth box (the succession/handoff note) applies to it
    await actions.assert_property(a, "minted_because", "compaction", agent, now, 0.9,
                                  evidence_class="self_declared")
    office = tmp_path / "office"
    office.mkdir()
    job_dir = str(tmp_path / "jobs" / "0ffab001")
    await save_mount(actions.pool, job_dir=job_dir, agent_id=agent, project="testhouse",
                     cwd=str(office), model=None, session_key=None)
    # mounted_at is a DB default at INSERT — pin it to a known past time so 'this
    # session's window' is unambiguous for the test
    await actions.pool.execute(
        "UPDATE agent_mounts SET mounted_at=$1 WHERE job_dir=$2", mounted_at, job_dir)

    sid = "0ffab001-0000-4000-8000-000000000000"
    # nothing written yet, no charter.md: everything genuinely unwritten or unevaluable
    boxes = await stophook._offload_boxes(sid, str(office))
    assert boxes is not None
    assert boxes["decisions recorded this session"] is False
    assert boxes["threads trued this session (opened or resolved)"] is False
    assert boxes["charter.md touched this session"] is None  # no such file here
    assert boxes["a live succession/handoff note (this lineage was minted)"] is False

    # now the session writes everything back
    await record_decision(actions, "a real ruling this session", source=agent)
    await open_thread(actions, "an obligation left for the heir", kind="obligation",
                      source=agent)
    (office / "charter.md").write_text("# notes\n")

    boxes2 = await stophook._offload_boxes(sid, str(office))
    assert boxes2 is not None
    assert boxes2["decisions recorded this session"] is True
    assert boxes2["threads trued this session (opened or resolved)"] is True
    assert boxes2["charter.md touched this session"] is True
    assert boxes2["a live succession/handoff note (this lineage was minted)"] is True


async def test_offload_boxes_a_non_minted_session_carries_no_succession_box(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pg_dsn: str,
) -> None:
    """The succession box only applies when THIS agent was itself born by a mint — an
    ordinary session is never told to leave a handoff note nobody asked for."""
    monkeypatch.setattr(stophook, "DSN", pg_dsn)
    agent = "agent:1eaf0001"
    await actions.create_or_find_object("Agent", agent, agent)
    office = tmp_path / "office2"
    office.mkdir()
    job_dir = str(tmp_path / "jobs" / "1eaf0001")
    await save_mount(actions.pool, job_dir=job_dir, agent_id=agent, project="testhouse",
                     cwd=str(office), model=None, session_key=None)
    sid = "1eaf0001-0000-4000-8000-000000000000"
    boxes = await stophook._offload_boxes(sid, str(office))
    assert boxes is not None
    assert "a live succession/handoff note (this lineage was minted)" not in boxes


async def test_offload_boxes_unresolvable_session_returns_none(
    actions: Actions, monkeypatch: pytest.MonkeyPatch, pg_dsn: str,
) -> None:
    monkeypatch.setattr(stophook, "DSN", pg_dsn)
    assert await stophook._offload_boxes("00000000-none-anywhere", "/nowhere") is None


# ═══════════ THE PIT WATCH, STAGE A — the stop confession + pending-is-a-state ═══════════
# _stage_a_confession is the whole policy, pure — same discipline as _offload_verdict above.

def test_confession_never_fires_without_a_manager_message_on_record() -> None:
    """No DM from the manager on record is not a stall — nothing to be behind on."""
    assert stophook._stage_a_confession(
        leased={"id": "x", "summary": "s"}, manager_dm_at=None, my_dm_at=None) is None


def test_confession_never_fires_once_i_already_replied() -> None:
    now = datetime.now(UTC)
    assert stophook._stage_a_confession(
        leased={"id": "x", "summary": "s"},
        manager_dm_at=now - timedelta(minutes=5), my_dm_at=now) is None


def test_confession_fires_when_the_manager_spoke_and_i_never_answered() -> None:
    now = datetime.now(UTC)
    body = stophook._stage_a_confession(
        leased={"id": "abcdef1234567890", "summary": "fix the thing"},
        manager_dm_at=now, my_dm_at=None)
    assert body == "stopping; assignment abcdef12 (fix the thing): in progress"


def test_confession_fires_when_my_own_reply_is_older_than_the_managers_latest() -> None:
    now = datetime.now(UTC)
    body = stophook._stage_a_confession(
        leased={"id": "abcdef1234567890", "summary": ""},
        manager_dm_at=now, my_dm_at=now - timedelta(minutes=10))
    assert body == "stopping; assignment abcdef12: in progress"


# ═══════════ IDENTITY, PRE-MOUNT — resolving who is stopping from the office alone ═══════════

async def test_resolve_identity_via_session_row_when_mounted(
    actions: Actions, tmp_path: Path,
) -> None:
    agent = "agent:aaaa0001"
    await actions.create_or_find_object("Agent", agent, agent)
    seat = "seat:aaaa0001"
    seat_obj = await actions.create_or_find_object("Seat", seat, agent)
    now = datetime.now(UTC)
    await actions.assert_property(seat_obj, "handle", "worker1", agent, now, 0.9,
                                  evidence_class="self_declared")
    await bind_holder(actions, seat_id=seat, agent_id=agent)
    job_dir = str(tmp_path / "jobs" / "aaaa0001")
    await save_mount(actions.pool, job_dir=job_dir, agent_id=agent, project="testhouse",
                     cwd=str(tmp_path / "somewhere"), model=None, session_key=None)
    sid = "aaaa0001-0000-4000-8000-000000000000"
    identity = await stophook._resolve_worker_identity(
        actions.pool, sid, str(tmp_path / "somewhere"))
    assert identity == {"agent_id": agent, "seat_id": seat}


async def test_resolve_identity_falls_back_to_the_office_when_unmounted(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The exact gap that bit Seshat: a session that has never called mount() THIS turn —
    find_session_row returns nothing — but its cwd IS a seat's office, single-tenant by
    construction, so the directory alone names who's stopping."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    agent = "agent:bbbb0001"
    await actions.create_or_find_object("Agent", agent, agent)
    seat = "seat:bbbb0001"
    seat_obj = await actions.create_or_find_object("Seat", seat, agent)
    now = datetime.now(UTC)
    await actions.assert_property(seat_obj, "handle", "worker2", agent, now, 0.9,
                                  evidence_class="self_declared")
    await bind_holder(actions, seat_id=seat, agent_id=agent)
    office = tmp_path / ".osiris" / "seats" / "worker2"
    office.mkdir(parents=True)
    # NO save_mount at all — this session has never touched agent_mounts
    never_mounted_sid = "cccccccc-0000-4000-8000-000000000000"
    identity = await stophook._resolve_worker_identity(
        actions.pool, never_mounted_sid, str(office))
    assert identity == {"agent_id": agent, "seat_id": seat}


async def test_resolve_identity_none_outside_any_office(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An ordinary code-repo cwd (not under ~/.osiris/seats) with no mount row resolves to
    nothing — the fallback must never misfire on a plain working tree."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    sid = "dddddddd-0000-4000-8000-000000000000"
    identity = await stophook._resolve_worker_identity(
        actions.pool, sid, str(tmp_path / "code" / "osiris"))
    assert identity is None


# ═══════════ THE LEASE, THE MANAGER, THE GAP ═══════════

async def test_leased_assignment_finds_the_open_obligation_owned_by_my_seat(
    actions: Actions,
) -> None:
    agent, seat = "agent:eeee0001", "seat:eeee0001"
    await open_thread(actions, "stage A test assignment", kind="obligation", owner=seat,
                      source=agent)
    leased = await stophook._leased_assignment(actions.pool, seat, agent)
    assert leased is not None
    assert leased["summary"] == "stage A test assignment"


async def test_leased_assignment_none_when_nothing_is_open_for_me(actions: Actions) -> None:
    assert await stophook._leased_assignment(
        actions.pool, "seat:ffff0001", "agent:ffff0001") is None


async def test_manager_seat_resolves_the_managed_by_link(actions: Actions) -> None:
    worker = await actions.create_or_find_object("Seat", "seat:1111aaaa", "test")
    manager = await actions.create_or_find_object("Seat", "seat:2222bbbb", "test")
    now = datetime.now(UTC)
    await actions.create_link(worker, manager, "managed_by", "test", now, 0.9,
                              evidence_class="self_declared")
    assert await stophook._manager_seat(actions.pool, "seat:1111aaaa") == "seat:2222bbbb"


async def test_manager_seat_none_when_unmanaged(actions: Actions) -> None:
    await actions.create_or_find_object("Seat", "seat:3333cccc", "test")
    assert await stophook._manager_seat(actions.pool, "seat:3333cccc") is None


async def test_mail_gap_reads_both_directions(actions: Actions) -> None:
    worker_agent, worker_seat = "agent:44440001", "seat:44440001"
    manager_agent, manager_seat = "agent:55550001", "seat:55550001"
    await actions.create_or_find_object("Seat", worker_seat, "test")
    await actions.create_or_find_object("Seat", manager_seat, "test")
    r1 = await send_message(actions.pool, from_agent=manager_agent, from_project="osiris",
                            to_agent=worker_seat, body="assignment", grade="ask")
    r2 = await send_message(actions.pool, from_agent=worker_agent, from_project="osiris",
                            to_agent=manager_seat, body="ack", grade="fyi")
    assert r1["dedup"] is False and r2["dedup"] is False
    manager_to_me, me_to_manager = await stophook._mail_gap(
        actions.pool, worker_seat, manager_seat, worker_agent)
    assert manager_to_me is not None and me_to_manager is not None
    assert me_to_manager >= manager_to_me  # I replied after the assignment landed


async def test_assert_pending_writes_a_readable_state(
    actions: Actions, monkeypatch: pytest.MonkeyPatch, pg_dsn: str,
) -> None:
    monkeypatch.setattr(stophook, "DSN", pg_dsn)
    agent = "agent:66660001"
    await stophook._assert_pending(agent)
    state = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='state' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", agent)
    assert state == "pending"


# ═══════════ INTEGRATION — _stage_a_async end to end ═══════════

async def test_stage_a_sends_the_confession_when_the_manager_is_owed_a_reply(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pg_dsn: str,
) -> None:
    monkeypatch.setattr(stophook, "DSN", pg_dsn)
    worker_agent, worker_seat = "agent:77770001", "seat:77770001"
    manager_agent, manager_seat = "agent:88880001", "seat:88880001"
    worker_obj = await actions.create_or_find_object("Seat", worker_seat, worker_agent)
    manager_obj = await actions.create_or_find_object("Seat", manager_seat, manager_agent)
    now = datetime.now(UTC)
    await actions.assert_property(worker_obj, "handle", "worker7", worker_agent, now, 0.9,
                                  evidence_class="self_declared")
    await actions.create_link(worker_obj, manager_obj, "managed_by", "test", now, 0.9,
                              evidence_class="self_declared")
    await bind_holder(actions, seat_id=worker_seat, agent_id=worker_agent)
    await open_thread(actions, "stage A integration assignment", kind="obligation",
                      owner=worker_seat, source=manager_agent)
    await send_message(actions.pool, from_agent=manager_agent, from_project="osiris",
                       to_agent=worker_seat, body="go build it", grade="ask")

    job_dir = str(tmp_path / "jobs" / "77770001")
    await save_mount(actions.pool, job_dir=job_dir, agent_id=worker_agent, project="testhouse",
                     cwd=str(tmp_path / "office"), model=None, session_key=None)
    sid = "77770001-0000-4000-8000-000000000000"

    before = await actions.pool.fetchval("SELECT count(*) FROM fleet_messages")
    await stophook._stage_a_async({"cwd": str(tmp_path / "office"), "session_id": sid})
    after = await actions.pool.fetchval("SELECT count(*) FROM fleet_messages")
    assert after == before + 1
    row = await actions.pool.fetchrow(
        "SELECT from_agent, to_agent, body, grade FROM fleet_messages ORDER BY id DESC LIMIT 1")
    assert row["from_agent"] == worker_agent
    assert row["to_agent"] == manager_seat
    assert row["grade"] == "fyi"
    assert "stopping; assignment" in row["body"]
    assert "stage A integration assignment" in row["body"]


async def test_stage_a_asserts_pending_when_nothing_is_leased(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pg_dsn: str,
) -> None:
    monkeypatch.setattr(stophook, "DSN", pg_dsn)
    agent, seat = "agent:99990001", "seat:99990001"
    seat_obj = await actions.create_or_find_object("Seat", seat, agent)
    now = datetime.now(UTC)
    await actions.assert_property(seat_obj, "handle", "worker9", agent, now, 0.9,
                                  evidence_class="self_declared")
    await bind_holder(actions, seat_id=seat, agent_id=agent)
    job_dir = str(tmp_path / "jobs" / "99990001")
    await save_mount(actions.pool, job_dir=job_dir, agent_id=agent, project="testhouse",
                     cwd=str(tmp_path / "office9"), model=None, session_key=None)
    sid = "99990001-0000-4000-8000-000000000000"

    await stophook._stage_a_async({"cwd": str(tmp_path / "office9"), "session_id": sid})
    state = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='state' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", agent)
    assert state == "pending"


def test_stage_a_never_raises_when_the_database_is_unreachable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Acceptance: a stop with mail machinery down still stops clean."""
    monkeypatch.setattr(stophook, "DSN", "postgresql://u:p@127.0.0.1:1/osiris")
    stophook._stage_a({"cwd": "/nowhere", "session_id": "deadbeef0000"})  # must not raise
