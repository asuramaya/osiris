"""stophook_logic (task #180 piece 2 (b)) — the pure halves osiris_stophook.py's own
`_deliverable`/`_offload_boxes` now delegate to, and the /stop route calls directly
against the shared pool. tests/test_stophook.py already covers the FULL behavioral
surface (project resolution, self-echo, the settle-state rollup, seat-office cwd
correction) through those wrappers; this file only proves the extracted functions work
against a bare Pool (not just a Connection) — the shape the /stop route actually uses.

STAGE A/B/C (dispatch 5441 LEG 1 parity fix): `compute_stop_stage_a` and its helpers are
PORTED VERBATIM from osiris_stophook.py's own `_stage_a_async` family — see that file's
THE PIT WATCH / STAGE C section headers for the full founding rationale, and
tests/test_stophook.py for the exhaustive edge-case coverage of the identical logic before
the port. This file proves the ported copy, now taking a bare `pool` (no second one-off
connection, no DSN), reproduces the same behavior end to end — it does not re-derive every
edge case test_stophook.py already owns."""
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from src.actions.core import Actions
from src.orchestrator.mounts import find_mount, save_mount
from src.orchestrator.seats import bind_holder
from src.orchestrator.stophook_logic import (
    _confess_if_parked,
    _leased_assignment,
    _resolve_worker_identity,
    _stage_a_confession,
    compute_stop_deliverable,
    compute_stop_offload,
    compute_stop_stage_a,
)
from src.parsers.base import EvidenceClass


def _write_transcript(path: Path, *entries: dict) -> None:
    import json

    path.write_text("\n".join(json.dumps(e) for e in entries) + "\n")


async def test_compute_stop_deliverable_works_against_a_pool_not_just_a_connection(
    actions: Actions,
) -> None:
    out = await compute_stop_deliverable(actions.pool, cwd="/nowhere", session_id="")
    assert out == {"n": 0, "senders": [], "window": None, "bands": {}, "project": None}


async def test_compute_stop_offload_unresolvable_session_returns_none(
    actions: Actions,
) -> None:
    out = await compute_stop_offload(actions.pool, session_id="00000000-none", cwd="/nowhere")
    assert out is None


async def test_compute_stop_deliverable_counts_unread_mail_for_a_mounted_session(
    actions: Actions,
) -> None:
    a = "agent:stophooklogic1"
    obj = await actions.create_or_find_object("Agent", a, a)
    await actions.assert_property(obj, "project", "logicproj", a, datetime.now(UTC), 0.9,
                                  evidence_class=EvidenceClass.SELF_DECLARED.value)
    sid = "logicses-0000-4000-8000-000000000000"  # find_session_row's lane 1 matches on
    # job_dir containing '/jobs/' + sid[:8] ("logicses")
    await save_mount(actions.pool, job_dir="/j/jobs/logicses", agent_id=a, project="logicproj",
                     cwd="/lp/office", model="claude-fable-5", session_key=None)
    from src.orchestrator.mailbox import send_message

    await send_message(actions.pool, from_agent="agent:other", from_project="logicproj",
                       to_agent=a, body="hi", grade="ask")

    out = await compute_stop_deliverable(actions.pool, cwd="/lp/office", session_id=sid)
    assert out["n"] == 1
    assert out["bands"] == {"ask": 1, "fyi": 0}


# ═══════════ STAGE A/B/C, PORTED — dispatch 5441 LEG 1 parity fix ═══════════

async def test_resolve_worker_identity_via_a_real_mount_row(
    actions: Actions, tmp_path: Path,
) -> None:
    agent, seat = "agent:sllogic01", "seat:sllogic01"
    seat_obj = await actions.create_or_find_object("Seat", seat, agent)
    now = datetime.now(UTC)
    await actions.assert_property(seat_obj, "handle", "worklogic1", agent, now, 0.9,
                                  evidence_class="self_declared")
    await bind_holder(actions, seat_id=seat, agent_id=agent)
    job_dir = str(tmp_path / "jobs" / "sllogic0")
    await save_mount(actions.pool, job_dir=job_dir, agent_id=agent, project="testhouse",
                     cwd=str(tmp_path / "somewhere"), model=None, session_key=None)
    sid = "sllogic0-0000-4000-8000-000000000000"
    identity = await _resolve_worker_identity(actions.pool, sid, str(tmp_path / "somewhere"))
    assert identity == {"agent_id": agent, "seat_id": seat}


async def test_resolve_worker_identity_self_restores_via_the_seat_office(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The #178 residual fallback: a session that never called an osiris MCP tool this
    turn still resolves via its cwd being a seat's own single-tenant office, and mints a
    durable mount row so it doesn't stay permanently rowless in registry_census."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    agent, seat = "agent:sllogic02", "seat:sllogic02"
    seat_obj = await actions.create_or_find_object("Seat", seat, agent)
    now = datetime.now(UTC)
    await actions.assert_property(seat_obj, "handle", "worklogic2", agent, now, 0.9,
                                  evidence_class="self_declared")
    await bind_holder(actions, seat_id=seat, agent_id=agent)
    office = tmp_path / ".osiris" / "seats" / "worklogic2"
    office.mkdir(parents=True)
    never_mounted_sid = "eeeeeeee-0000-4000-8000-000000000000"
    identity = await _resolve_worker_identity(actions.pool, never_mounted_sid, str(office))
    assert identity == {"agent_id": agent, "seat_id": seat}
    row = await find_mount(actions.pool, job_dir=str(tmp_path / ".claude" / "jobs" / "eeeeeeee"))
    assert row is not None
    assert row.agent_id == agent


async def test_resolve_worker_identity_none_outside_any_office(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    sid = "ffffffff-0000-4000-8000-000000000000"
    identity = await _resolve_worker_identity(
        actions.pool, sid, str(tmp_path / "code" / "osiris"))
    assert identity is None


async def test_leased_assignment_finds_the_open_obligation_owned_by_my_seat(
    actions: Actions,
) -> None:
    seat = "seat:sllogic03"
    from src.orchestrator.capture import open_thread

    await open_thread(actions, "stage A integration assignment", kind="obligation", owner=seat)
    leased = await _leased_assignment(actions.pool, seat, "agent:sllogic03")
    assert leased is not None
    assert "stage A integration assignment" in (leased["summary"] or "")


async def test_leased_assignment_none_when_nothing_is_open_for_me(actions: Actions) -> None:
    leased = await _leased_assignment(actions.pool, "seat:nothing-here", "agent:nothing-here")
    assert leased is None


def test_stage_a_confession_pure_ball_in_my_court() -> None:
    now = datetime.now(UTC)
    leased = {"id": "obligation-uuid", "summary": "do the thing"}
    assert _stage_a_confession(
        leased=leased, manager_dm_at=now, my_dm_at=None) is not None
    assert _stage_a_confession(
        leased=leased, manager_dm_at=None, my_dm_at=None) is None  # no manager msg, no stall
    assert _stage_a_confession(
        leased=leased, manager_dm_at=now, my_dm_at=now) is None  # I already spoke since


async def test_confess_if_parked_sends_fyi_when_parked_and_no_real_ask(
    actions: Actions, tmp_path: Path,
) -> None:
    worker_agent, manager_seat = "agent:sllogicpk1", "seat:sllogicpkmgr"
    await actions.create_or_find_object("Seat", manager_seat, "agent:sllogicpkmgr")
    transcript = tmp_path / "t.jsonl"
    _write_transcript(
        transcript,
        {"type": "assistant", "isSidechain": False,
         "message": {"content": "Want me to proceed straight into that now?"}},
    )
    before = await actions.pool.fetchval("SELECT count(*) FROM fleet_messages")
    await _confess_if_parked(
        actions.pool, payload={"transcript_path": str(transcript)}, agent_id=worker_agent,
        project="testhouse", manager_seat=manager_seat)
    after = await actions.pool.fetchval("SELECT count(*) FROM fleet_messages")
    assert after == before + 1
    row = await actions.pool.fetchrow(
        "SELECT from_agent, to_agent, grade, body FROM fleet_messages ORDER BY id DESC LIMIT 1")
    assert row["from_agent"] == worker_agent
    assert row["to_agent"] == manager_seat
    assert row["grade"] == "fyi"
    assert "likely parked" in row["body"]


async def test_compute_stop_stage_a_asserts_pending_when_nothing_is_leased(
    actions: Actions, tmp_path: Path,
) -> None:
    agent, seat = "agent:slstagea1", "seat:slstagea1"
    seat_obj = await actions.create_or_find_object("Seat", seat, agent)
    now = datetime.now(UTC)
    await actions.assert_property(seat_obj, "handle", "stageaworker1", agent, now, 0.9,
                                  evidence_class="self_declared")
    await bind_holder(actions, seat_id=seat, agent_id=agent)
    job_dir = str(tmp_path / "jobs" / "slstagea")
    await save_mount(actions.pool, job_dir=job_dir, agent_id=agent, project="testhouse",
                     cwd=str(tmp_path / "office"), model=None, session_key=None)
    sid = "slstagea-0000-4000-8000-000000000000"

    await compute_stop_stage_a(
        actions.pool, payload={}, session_id=sid, cwd=str(tmp_path / "office"))
    state = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='state' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", agent)
    assert state == "pending"


async def test_compute_stop_stage_a_stamps_context_pct_even_for_an_unseated_agent(
    actions: Actions, tmp_path: Path,
) -> None:
    agent = "agent:slstagea2"
    await actions.create_or_find_object("Agent", agent, agent)
    job_dir = str(tmp_path / "jobs" / "slstage2")
    await save_mount(actions.pool, job_dir=job_dir, agent_id=agent, project="testhouse",
                     cwd=str(tmp_path / "office2"), model=None, session_key=None)
    sid = "slstage2-0000-4000-8000-000000000000"

    await compute_stop_stage_a(
        actions.pool, payload={}, session_id=sid, cwd=str(tmp_path / "office2"), pct=45)
    pct = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='context_pct' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", agent)
    assert pct == "45"


async def test_compute_stop_stage_a_no_identity_is_a_silent_noop(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    before = await actions.pool.fetchval("SELECT count(*) FROM fleet_messages")
    await compute_stop_stage_a(
        actions.pool, payload={}, session_id="00000000-0000-4000-8000-000000000000",
        cwd=str(tmp_path / "code" / "osiris"))
    after = await actions.pool.fetchval("SELECT count(*) FROM fleet_messages")
    assert after == before  # nobody to attribute this to — no confession, no assertion, no crash


async def test_compute_stop_stage_a_sends_the_leased_confession_end_to_end(
    actions: Actions, tmp_path: Path,
) -> None:
    """The full integration path: a seated worker with an open obligation owned by their
    seat, and a manager who spoke more recently than the worker did, earns exactly one
    courtesy fyi DM to the manager — the SAME behavior test_stophook.py already proves for
    the pre-port `_stage_a_async`, now proven for the ported `compute_stop_stage_a`."""
    from src.orchestrator.capture import open_thread
    from src.orchestrator.mailbox import send_message

    worker_agent, worker_seat = "agent:slstagea3", "seat:slstagea3"
    manager_agent, manager_seat = "agent:slstageamgr", "seat:slstageamgr"
    worker_obj = await actions.create_or_find_object("Seat", worker_seat, worker_agent)
    manager_obj = await actions.create_or_find_object("Seat", manager_seat, manager_agent)
    now = datetime.now(UTC)
    await actions.assert_property(worker_obj, "handle", "stageaworker3", worker_agent, now,
                                  0.9, evidence_class="self_declared")
    await actions.create_link(worker_obj, manager_obj, "managed_by", "test", now, 0.9,
                              evidence_class="self_declared")
    await bind_holder(actions, seat_id=worker_seat, agent_id=worker_agent)
    await open_thread(actions, "stage A integration assignment", kind="obligation",
                      owner=worker_seat)
    await send_message(actions.pool, from_agent=manager_agent, from_project="testhouse",
                       to_agent=worker_seat, body="go do the thing", grade="ask")
    job_dir = str(tmp_path / "jobs" / "slstage3")
    await save_mount(actions.pool, job_dir=job_dir, agent_id=worker_agent, project="testhouse",
                     cwd=str(tmp_path / "office3"), model=None, session_key=None)
    sid = "slstage3-0000-4000-8000-000000000000"

    before = await actions.pool.fetchval("SELECT count(*) FROM fleet_messages")
    await compute_stop_stage_a(
        actions.pool, payload={}, session_id=sid, cwd=str(tmp_path / "office3"))
    after = await actions.pool.fetchval("SELECT count(*) FROM fleet_messages")
    assert after == before + 1
    row = await actions.pool.fetchrow(
        "SELECT from_agent, to_agent, grade, body FROM fleet_messages ORDER BY id DESC LIMIT 1")
    assert row["from_agent"] == worker_agent
    assert row["to_agent"] == manager_seat
    assert row["grade"] == "fyi"
    assert "stage A integration assignment" in row["body"]


async def test_compute_stop_stage_a_practice_check_disabled_by_default_sends_nothing(
    actions: Actions, tmp_path: Path,
) -> None:
    """Stage C (ruling DM 3059) stays disarmed by default even through the ported path —
    a turn that would trip the practice-violation fingerprint sends nothing when
    `osiris_stage_c_practice_check_enabled` is unset."""
    from src.config.settings import get_settings

    assert get_settings().osiris_stage_c_practice_check_enabled is False
    agent, seat = "agent:slstagea4", "seat:slstagea4"
    mgr_agent, mgr_seat = "agent:slstageamgr4", "seat:slstageamgr4"
    worker_obj = await actions.create_or_find_object("Seat", seat, agent)
    manager_obj = await actions.create_or_find_object("Seat", mgr_seat, mgr_agent)
    now = datetime.now(UTC)
    await actions.assert_property(worker_obj, "handle", "stageaworker4", agent, now, 0.9,
                                  evidence_class="self_declared")
    await actions.create_link(worker_obj, manager_obj, "managed_by", "test", now, 0.9,
                              evidence_class="self_declared")
    await bind_holder(actions, seat_id=seat, agent_id=agent)
    job_dir = str(tmp_path / "jobs" / "slstage4")
    await save_mount(actions.pool, job_dir=job_dir, agent_id=agent, project="testhouse",
                     cwd=str(tmp_path / "office4"), model=None, session_key=None)
    sid = "slstage4-0000-4000-8000-000000000000"

    before = await actions.pool.fetchval("SELECT count(*) FROM fleet_messages")
    await compute_stop_stage_a(
        actions.pool, payload={}, session_id=sid, cwd=str(tmp_path / "office4"))
    after = await actions.pool.fetchval("SELECT count(*) FROM fleet_messages")
    assert after == before  # no leased obligation either, so state='pending' is the only write
