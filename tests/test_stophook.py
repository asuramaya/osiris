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
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import scripts.osiris_stophook as stophook
from src.actions.core import Actions
from src.orchestrator.capture import (
    open_thread,
    record_decision,
    record_practice,
    refute_practice,
)
from src.orchestrator.mailbox import send_message
from src.orchestrator.mounts import find_mount, save_mount
from src.orchestrator.seats import bind_holder


@pytest.fixture(autouse=True)
def _no_real_stop_route(monkeypatch: pytest.MonkeyPatch) -> None:
    """task #180 piece 2 (b): every test in this file calls `_deliverable`/`_offload_boxes`
    directly, bypassing `_stop_via_http` entirely — but a FUTURE test exercising `main()`
    on a box where a real osiris-mcp happens to be listening on :8790 (this dev box, most
    of the time) would otherwise silently hit the LIVE shared server. Same isolation shape
    as test_statusline.py's own `_no_real_heartbeat_route`."""
    monkeypatch.setattr(stophook, "STOP_URL", "http://127.0.0.1:1/stop")


# ═══════════ THE PURE POLICY — acceptance (a)-(d) ═══════════

def test_a_above_alarm_and_unwritten_blocks_naming_the_boxes() -> None:
    v = stophook._offload_verdict(
        pct=85, window_assumed=False, already_blocked=False,
        boxes={"decisions recorded this session": False,
               "standing orders touched this session": True})
    assert v is not None and v["decision"] == "block"
    assert "decisions recorded this session" in v["reason"]
    assert "charter.md" in v["reason"]  # points at the offload target regardless
    # a SATISFIED box is never named as missing
    assert "standing orders touched this session:" not in v["reason"]


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


def test_offload_verdict_names_settle_and_the_harder_line_still_ahead() -> None:
    """Leg 1 (Thoth, msg 1381, decision 33b7cb10): the soft tier points at settle() by
    name and names the harder line still ahead, rather than the pre-/settle raw-verb copy."""
    v = stophook._offload_verdict(
        pct=85, window_assumed=False, already_blocked=False,
        boxes={"decisions recorded this session": False}, hard=False)
    assert v is not None
    assert "settle()" in v["reason"]
    assert f"{stophook.HARD_ALARM_PCT}%" in v["reason"]


def test_offload_verdict_hard_tier_says_this_was_the_last_nudge() -> None:
    v = stophook._offload_verdict(
        pct=97, window_assumed=False, already_blocked=False,
        boxes={"decisions recorded this session": False}, hard=True)
    assert v is not None
    assert "harder nudge" in v["reason"]
    assert "settle now" in v["reason"]


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


def test_marker_soft_and_hard_tiers_are_independent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The two-tier re-arm (Thoth, msg 1381: 'block-once-then-silent is exactly how Seshat
    climbed 80->past-seam unnudged'): the soft (ALARM_PCT) block firing must NOT silence
    the hard (HARD_ALARM_PCT) block, and vice versa — distinct markers, distinct fates."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    sid = "deadbeef-0000-4000-8000-000000000000"
    assert stophook._offload_already_blocked(sid, hard=False) is False
    assert stophook._offload_already_blocked(sid, hard=True) is False
    stophook._offload_mark_blocked(sid, hard=False)
    assert stophook._offload_already_blocked(sid, hard=False) is True
    assert stophook._offload_already_blocked(sid, hard=True) is False  # unaffected
    stophook._offload_mark_blocked(sid, hard=True)
    assert stophook._offload_already_blocked(sid, hard=True) is True


def test_marker_needs_a_trustworthy_session_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    assert stophook._offload_marker("short") is None
    assert stophook._offload_already_blocked("short") is False
    stophook._offload_mark_blocked("short")  # a no-op, never raises


# ═══════════ THE CHARTER-FILE WITNESS — moved to tests/test_settle.py (ruling c5b184cd):
# standing_orders_touched now lives in src.orchestrator.settle, promoted out of this hook so /settle
# and the Stop hook read one implementation. ═══════════


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
    # a minted heir — the fourth box (the obligation-opened-this-session note) applies to it
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
    assert boxes["standing orders touched this session"] is None  # no such file here
    assert boxes["an obligation opened this session (this lineage was minted)"] is False

    # now the session writes everything back
    await record_decision(actions, "a real ruling this session", source=agent)
    await open_thread(actions, "an obligation left for the heir", kind="obligation",
                      source=agent)
    (office / "charter.md").write_text("# notes\n")

    boxes2 = await stophook._offload_boxes(sid, str(office))
    assert boxes2 is not None
    assert boxes2["decisions recorded this session"] is True
    assert boxes2["threads trued this session (opened or resolved)"] is True
    assert boxes2["standing orders touched this session"] is True
    assert boxes2["an obligation opened this session (this lineage was minted)"] is True


async def test_offload_boxes_resolves_the_seat_office_over_a_corrected_mount_cwd(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pg_dsn: str,
) -> None:
    """THE SAME #128-CLASS SPECIMEN test_settle.py's own
    test_settle_tool_resolves_the_seat_office_over_a_corrected_mount_cwd reproduces for the
    MCP settle() call site (Khnum, commit 17b20e0, Thoth DM 3076/3096) — reproduced here for
    THIS call site: a seated agent's mount cwd reads as the bare office CONTAINER, not this
    agent's real office at <container>/<handle>. Before this fix, _offload_boxes passed that
    corrupted cwd straight to settle_boxes, standing_orders_touched found no charter.md at the
    container root, and returned None (fog-of-war) — even with a real, 11-day-stale
    charter.md sitting in the seat's actual office the whole time. The offload ritual fires
    unattended at every turn-end above the alarm line, nobody watching to notice a silent
    None where a real False belonged."""
    monkeypatch.setattr(stophook, "DSN", pg_dsn)
    monkeypatch.setattr("src.orchestrator.offices._DEFAULT_OFFICE_ROOT", tmp_path / "seats")
    container = tmp_path / "seats"
    container.mkdir()
    real_office = container / "thoth"
    real_office.mkdir()
    mounted_at = datetime.now(UTC) - timedelta(minutes=5)
    (real_office / "charter.md").write_text("# eleven days old, untouched this session\n")
    old_time = (mounted_at - timedelta(days=11)).timestamp()
    os.utime(real_office / "charter.md", (old_time, old_time))

    agent = "agent:0ffh00k01"
    seat_id = "seat:0ffh00k01"
    seat_oid = await actions.create_or_find_object("Seat", seat_id, agent)
    await actions.assert_property(seat_oid, "handle", "Thoth", agent, mounted_at, 0.9,
                                  evidence_class="self_declared")
    await bind_holder(actions, seat_id=seat_id, agent_id=agent)

    sid = "0ffh00k1-0000-4000-8000-000000000000"
    job_dir = str(tmp_path / "jobs" / sid[:8])  # find_session_row matches sid[:8] == job_dir
    await save_mount(actions.pool, job_dir=job_dir, agent_id=agent, project="osiris",
                     cwd=str(container), model=None, session_key=None)  # the CORRUPTED cwd
    await actions.pool.execute(
        "UPDATE agent_mounts SET mounted_at=$1 WHERE job_dir=$2", mounted_at, job_dir)
    boxes = await stophook._offload_boxes(sid, str(container))  # the hook's own payload cwd

    assert boxes is not None
    assert boxes["standing orders touched this session"] is False, boxes  # not None


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
    assert "an obligation opened this session (this lineage was minted)" not in boxes


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
    # #178 residual: the seat-fallback branch must ALSO self-restore a durable mount row,
    # since piece (b)'s own self-restore never fires here (no MCP tool call happened) —
    # without this a seat that only ever stops stays permanently rowless in registry_census.
    row = await find_mount(
        actions.pool, job_dir=str(tmp_path / ".claude" / "jobs" / "cccccccc"))
    assert row is not None
    assert row.agent_id == agent
    assert row.cwd == str(office)


async def test_resolve_identity_self_restore_never_clobbers_a_real_mount(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A session that DOES have a real mount row takes the first branch entirely — the
    self-restore helper must never even be reached, let alone overwrite it."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    agent = "agent:eeee0001"
    await actions.create_or_find_object("Agent", agent, agent)
    seat = "seat:eeee0001"
    seat_obj = await actions.create_or_find_object("Seat", seat, agent)
    now = datetime.now(UTC)
    await actions.assert_property(seat_obj, "handle", "worker3", agent, now, 0.9,
                                  evidence_class="self_declared")
    await bind_holder(actions, seat_id=seat, agent_id=agent)
    job_dir = str(tmp_path / "jobs" / "eeee0001")
    await save_mount(actions.pool, job_dir=job_dir, agent_id=agent, project="realproject",
                     cwd=str(tmp_path / "somewhere-real"), model="claude-sonnet-5",
                     session_key=None)
    sid = "eeee0001-0000-4000-8000-000000000000"
    identity = await stophook._resolve_worker_identity(
        actions.pool, sid, str(tmp_path / "somewhere-real"))
    assert identity == {"agent_id": agent, "seat_id": seat}
    row = await find_mount(actions.pool, job_dir=job_dir)
    assert row is not None
    assert row.project == "realproject"  # untouched by the self-restore path


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


# ═══════════ THE PROJECT RESOLUTION FIX (msg 1888) — the live specimen: Thoth's own turn,
# cwd the bare seat-office CONTAINER, basename-guessed "seats", a phantom project. This
# wasn't just a wrong label: `_deliverable`'s own broadcast-mail clause (`m.to_project=$2`)
# silently MISSED real mail addressed to the seat's actual house. ═══════════

async def test_deliverable_resolves_the_real_house_not_the_container_basename(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pg_dsn: str,
) -> None:
    """THE LIVE SPECIMEN, exactly: a seated agent sitting at the bare office root resolves
    its house through the seat, never the basename "seats"."""
    monkeypatch.setattr(stophook, "DSN", pg_dsn)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    fake_root = tmp_path / ".osiris" / "seats"
    fake_root.mkdir(parents=True)
    monkeypatch.setattr("src.orchestrator.offices._DEFAULT_OFFICE_ROOT", fake_root)

    agent, worker_seat = "agent:dlv10001", "seat:dlv10001"
    head = await actions.create_or_find_object("Seat", "seat:dlv1head", "test")
    await actions.assert_property(head, "house", "osiris", "test", datetime.now(UTC), 0.9)
    worker = await actions.create_or_find_object("Seat", worker_seat, "test")
    await actions.create_link(worker, head, "managed_by", "test", datetime.now(UTC), 0.9,
                              evidence_class="self_declared")
    await actions.create_or_find_object("Agent", agent, agent)
    await bind_holder(actions, seat_id=worker_seat, agent_id=agent)
    job_dir = str(tmp_path / "jobs" / "dlv10001")
    await save_mount(actions.pool, job_dir=job_dir, agent_id=agent, project="seats",
                     cwd=str(fake_root), model=None, session_key=None)
    sid = "dlv10001-0000-4000-8000-000000000000"

    n, senders, window, bands, project = await stophook._deliverable(str(fake_root), sid)
    assert project == "osiris"


async def test_deliverable_no_longer_blind_to_broadcast_mail_from_the_container_root(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pg_dsn: str,
) -> None:
    """THE ACTUAL DEFECT, not just cosmetics (msg 1888: "worse than the read"): under the
    old `Path(cwd).name` fallback, this session's resolved project ("seats") never matched
    a broadcast sent `to_project="osiris"` — real mail sat undelivered. Fixed, the broadcast
    is now found."""
    monkeypatch.setattr(stophook, "DSN", pg_dsn)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    fake_root = tmp_path / ".osiris" / "seats"
    fake_root.mkdir(parents=True)
    monkeypatch.setattr("src.orchestrator.offices._DEFAULT_OFFICE_ROOT", fake_root)

    agent, worker_seat = "agent:dlv20001", "seat:dlv20001"
    head = await actions.create_or_find_object("Seat", "seat:dlv2head", "test")
    await actions.assert_property(head, "house", "osiris", "test", datetime.now(UTC), 0.9)
    worker = await actions.create_or_find_object("Seat", worker_seat, "test")
    await actions.create_link(worker, head, "managed_by", "test", datetime.now(UTC), 0.9,
                              evidence_class="self_declared")
    await actions.create_or_find_object("Agent", agent, agent)
    await bind_holder(actions, seat_id=worker_seat, agent_id=agent)
    job_dir = str(tmp_path / "jobs" / "dlv20001")
    await save_mount(actions.pool, job_dir=job_dir, agent_id=agent, project="seats",
                     cwd=str(fake_root), model=None, session_key=None)
    # send_message refuses a to_project nobody has ever mounted under (f6f3e43e, shape 3 of
    # #117) -- the "seats" mount above is a DIFFERENT project than the broadcast's own
    # to_project="osiris" below, so 'osiris' needs its own throwaway, alive=False seed.
    await save_mount(actions.pool, job_dir="/test/seed/osiris", agent_id="agent:seed-osiris",
                     project="osiris", cwd="/test", model=None, session_key=None, alive=False)
    sid = "dlv20001-0000-4000-8000-000000000000"
    await send_message(actions.pool, from_agent="agent:someoneelse", from_project="osiris",
                       to_project="osiris", body="a real broadcast for the house", grade="fyi")

    n, senders, window, bands, project = await stophook._deliverable(str(fake_root), sid)
    assert n == 1
    assert project == "osiris"


async def test_stage_a_confession_from_project_is_the_house_not_the_container_basename(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pg_dsn: str,
) -> None:
    """The write side of the same fix: a confession sent from the bare office root stamps
    the real house on `from_project`, never the phantom "seats" (osiris_stophook.py's
    former `send_message(..., from_project=Path(cwd).name, ...)`)."""
    monkeypatch.setattr(stophook, "DSN", pg_dsn)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    fake_root = tmp_path / ".osiris" / "seats"
    fake_root.mkdir(parents=True)
    monkeypatch.setattr("src.orchestrator.offices._DEFAULT_OFFICE_ROOT", fake_root)

    worker_agent, worker_seat = "agent:dlv30001", "seat:dlv30001"
    manager_agent, manager_seat = "agent:dlv3mgr1", "seat:dlv3mgr1"
    worker_obj = await actions.create_or_find_object("Seat", worker_seat, worker_agent)
    manager_obj = await actions.create_or_find_object("Seat", manager_seat, manager_agent)
    now = datetime.now(UTC)
    await actions.assert_property(worker_obj, "handle", "dlv3worker", worker_agent, now,
                                  0.9, evidence_class="self_declared")
    # the manager IS the house-bearing head here (a single managed_by hop) — a seat has
    # exactly one active manager; splitting "who to confess to" from "where the house
    # derives" would need a second hop, not a second link off the same worker
    await actions.assert_property(manager_obj, "house", "osiris", "test", now, 0.9)
    await actions.create_link(worker_obj, manager_obj, "managed_by", "test", now, 0.9,
                              evidence_class="self_declared")
    await bind_holder(actions, seat_id=worker_seat, agent_id=worker_agent)

    transcript = tmp_path / "t3.jsonl"
    _write_transcript(
        transcript,
        {"type": "assistant", "isSidechain": False,
         "message": {"content": "Want me to proceed straight into that now?"}},
    )
    job_dir = str(tmp_path / "jobs" / "dlv30001")
    await save_mount(actions.pool, job_dir=job_dir, agent_id=worker_agent, project="seats",
                     cwd=str(fake_root), model=None, session_key=None)
    sid = "dlv30001-0000-4000-8000-000000000000"

    await stophook._stage_a_async(
        {"cwd": str(fake_root), "session_id": sid, "transcript_path": str(transcript)})
    row = await actions.pool.fetchrow(
        "SELECT from_project FROM fleet_messages ORDER BY id DESC LIMIT 1")
    assert row["from_project"] == "osiris"


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


# _manager_seat's own duplicate query is GONE (msg 1888) — the hook now calls
# seats.manager_of_seat directly, already covered by
# test_manager_of_seat_resolves_the_managed_by_link / test_manager_of_seat_none_when_unmanaged
# in test_seats.py; no separate hook-local copy left to test here.


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


async def test_assert_context_pct_writes_a_readable_value(
    actions: Actions, monkeypatch: pytest.MonkeyPatch, pg_dsn: str,
) -> None:
    """Leg 2 (Thoth, msg 1381, decision 33b7cb10): the manager-visible occupancy stamp,
    same idiom as _assert_pending above, same reason — a manager can't route around a
    seam it can't see."""
    monkeypatch.setattr(stophook, "DSN", pg_dsn)
    agent = "agent:pctwrite1"
    await stophook._assert_context_pct(agent, 83)
    pct = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='context_pct' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", agent)
    assert pct == "83"


# ═══════════ THE PARKED CONFESSION, STAGE B — thread 3c4fe1dc ═══════════
# A turn that ends on a question mark with no mail ask actually sent is a question asked
# into an empty room — the exact failure the operator caught live (Imhotep, 2026-07-27).

def _write_transcript(path: Path, *entries: dict) -> None:
    path.write_text("\n".join(json.dumps(e) for e in entries) + "\n")


def test_last_assistant_text_reads_the_final_real_assistant_turn(tmp_path: Path) -> None:
    t = tmp_path / "t.jsonl"
    _write_transcript(
        t,
        {"type": "assistant", "isSidechain": False,
         "message": {"content": "an older turn, not the answer"}},
        {"type": "user", "message": {"content": "noise in between"}},
        {"type": "assistant", "isSidechain": False,
         "message": {"content": "Want me to proceed straight into that now?"}},
    )
    assert stophook._last_assistant_text(str(t)) == "Want me to proceed straight into that now?"


def test_last_assistant_text_extracts_from_content_blocks(tmp_path: Path) -> None:
    t = tmp_path / "t.jsonl"
    _write_transcript(
        t,
        {"type": "assistant", "isSidechain": False, "message": {"content": [
            {"type": "tool_use", "name": "Bash", "input": {}},
            {"type": "text", "text": "should I proceed?"},
        ]}},
    )
    assert stophook._last_assistant_text(str(t)) == "should I proceed?"


def test_last_assistant_text_skips_sidechain_and_missing_file(tmp_path: Path) -> None:
    t = tmp_path / "t.jsonl"
    _write_transcript(
        t,
        {"type": "assistant", "isSidechain": True,
         "message": {"content": "a subagent turn, not mine"}},
    )
    assert stophook._last_assistant_text(str(t)) is None
    assert stophook._last_assistant_text(str(tmp_path / "missing.jsonl")) is None
    assert stophook._last_assistant_text("") is None


def test_parked_on_a_question_pure() -> None:
    assert stophook._parked_on_a_question("proceed straight into that now?") is True
    assert stophook._parked_on_a_question("trailing whitespace after the mark? \n") is True
    assert stophook._parked_on_a_question("done. committed 76ead13.") is False
    assert stophook._parked_on_a_question("") is False
    assert stophook._parked_on_a_question(None) is False


def test_practice_violation_pure() -> None:
    """PRACTICE v2 layer 3 v2 (Thoth, msgs 1800/1801, DO-BEFORE-DEPLOY): a reversal cue
    ALONE is too cheap a trigger, and topical overlap alone is too — both are required
    IN THE SAME SENTENCE, not merely co-present anywhere in a long turn (SPECIMEN 2:
    Imhotep's settle turn tripped on "rather than" in one sentence while the practice's
    topic only appeared in an unrelated later sentence). Quoting the practice's own
    wording verbatim is citation, not reversal, and must not fire either (SPECIMEN 1:
    quoting Practice 0e6ce6f5 verbatim tripped its own "never")."""
    practices = [{"id": "abc12345",
                 "statement": "batch small commits into one PR for this class of change"}]
    # a genuine violation: reversal cue + on-topic, but reworded/reordered — not a quote
    hit = stophook._practice_violation(
        "Let's stop doing small batch commits for this kind of change — one big commit "
        "instead. Done.", practices)
    assert hit == {"practice_id": "abc12345",
                   "statement": practices[0]["statement"], "cues": ["stop"]}
    # SPECIMEN 1: quoting the practice's own text verbatim is citation, not reversal —
    # even when the practice's own wording itself carries a cue word
    quoting_practice = [{"id": "de456789",
                         "statement": "never batch small commits into one PR for this "
                                      "class of change"}]
    assert stophook._practice_violation(
        "As recorded: never batch small commits into one PR for this class of change.",
        quoting_practice) is None
    # SPECIMEN 2: a reversal cue in one sentence, the practice's topic only mentioned in
    # an UNRELATED later sentence of the same turn — not proximate, so not flagged
    assert stophook._practice_violation(
        "Let's do this rather than the other approach, for unrelated reasons. Separately, "
        "batch small commits into one PR for this class of change, as usual.",
        practices) is None
    # cue present, but no topical overlap with any standing practice — no false positive
    assert stophook._practice_violation("never eat the last slice of pizza", practices) is None
    # topical overlap present, but no reversal cue anywhere — a plain mention
    assert stophook._practice_violation(
        "batch small commits into one PR for this class of change, as usual", practices) is None
    assert stophook._practice_violation(None, practices) is None
    assert stophook._practice_violation("never mind", []) is None


def test_practice_violation_does_not_flag_reporting_a_violation_while_quoting_it() -> None:
    """TASK #104 (Thoth's DM 2228; thread b318a9d3): six false positives one night, same
    shape — a genuine REPORT of someone ELSE's violation reads as reversal language in
    its OWN sentence ("they skipped checking git status... instead of verifying every
    path"), while the practice cited as evidence is quoted verbatim in the NEXT sentence,
    not the same one. The original quote-suppressor (SPECIMEN 1 in the test above) only
    ever checked the SAME sentence the cue lived in, so it was blind to this. THE
    SHARPEST SPECIMEN (Thoth's own framing, msg 2228): msg 2211 — Sekhmet quoted practice
    6aeb2067 while correctly reporting a violation of it BY SOMEONE ELSE, and Stage C
    flagged HER for it. Confirmed against the real (unmodified) detector before this fix
    landed: this exact text reproduced the false positive with no cross-practice
    collision needed."""
    practice = {"id": "6aeb2067-3d0f-4f34-ae71-ed10ad05d2cc",
               "statement": "A stash's blast radius is the FILE, not the AUTHOR — before "
                            "pathspec-scoping `git stash push -- <files>` to \"agent X's "
                            "known files,\" check the FULL current `git status --short` "
                            "for every path about to be targeted, not just the one agent "
                            "you have in mind."}
    text = (
        "Confirmed: they skipped checking git status before stashing — instead of "
        "verifying every path, they assumed it was scoped to one agent. Exactly "
        "practice 6aeb2067's own warning: \"A stash's blast radius is the FILE, not "
        "the AUTHOR — before pathspec-scoping git stash push -- <files> to agent X's "
        "known files, check the FULL current git status --short for every path about "
        "to be targeted, not just the one agent you have in mind.\""
    )
    assert stophook._practice_violation(text, [practice]) is None
    # control: quoting practice A elsewhere in the turn must not blanket-shield a
    # GENUINE, unrelated violation of a DIFFERENT practice B in the same turn — the fix
    # widens the scope of "is THIS practice quoted", not "suppress this whole turn"
    practice_b = {"id": "violate1-eeee-4eee-8eee-eeeeeeeeeeee",
                 "statement": "dispatches and un-parks go as DMs, never as broadcast replies"}
    mixed = text + (" Separately: let's stop sending DMs for un-parks and just reply on "
                    "the broadcast thread instead, it's fine.")
    hit = stophook._practice_violation(mixed, [practice, practice_b])
    assert hit is not None and hit["practice_id"] == "violate1"


async def test_sent_a_real_ask_true_only_for_a_recent_ask_from_this_agent(
    actions: Actions,
) -> None:
    agent = "agent:asktest1"
    # send_message refuses a to_project nobody has ever mounted under (f6f3e43e, shape 3 of
    # #117) -- alive=False registers 'osiris' as existing without a live pulse.
    await save_mount(actions.pool, job_dir="/test/seed/osiris", agent_id="agent:seed-osiris",
                     project="osiris", cwd="/test", model=None, session_key=None, alive=False)
    await send_message(actions.pool, from_agent=agent, from_project="osiris",
                       to_project="osiris", body="a real question", grade="ask")
    assert await stophook._sent_a_real_ask(actions.pool, agent) is True
    assert await stophook._sent_a_real_ask(actions.pool, "agent:neverasked") is False


async def test_sent_a_real_ask_ignores_an_old_ask_outside_the_window(
    actions: Actions,
) -> None:
    agent = "agent:askstale1"
    await save_mount(actions.pool, job_dir="/test/seed/osiris", agent_id="agent:seed-osiris",
                     project="osiris", cwd="/test", model=None, session_key=None, alive=False)
    r = await send_message(actions.pool, from_agent=agent, from_project="osiris",
                           to_project="osiris", body="an old question", grade="ask")
    await actions.pool.execute(
        "UPDATE fleet_messages SET created_at=now() - interval '1 hour' WHERE id=$1", r["id"])
    assert await stophook._sent_a_real_ask(actions.pool, agent, within_secs=300) is False


async def test_stage_a_confesses_when_parked_on_a_question_with_no_mail_ask_sent(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pg_dsn: str,
) -> None:
    monkeypatch.setattr(stophook, "DSN", pg_dsn)
    worker_agent, worker_seat = "agent:parked001", "seat:parked001"
    manager_agent, manager_seat = "agent:parkmgr01", "seat:parkmgr01"
    worker_obj = await actions.create_or_find_object("Seat", worker_seat, worker_agent)
    manager_obj = await actions.create_or_find_object("Seat", manager_seat, manager_agent)
    now = datetime.now(UTC)
    await actions.assert_property(worker_obj, "handle", "parkedworker", worker_agent, now,
                                  0.9, evidence_class="self_declared")
    await actions.create_link(worker_obj, manager_obj, "managed_by", "test", now, 0.9,
                              evidence_class="self_declared")
    await bind_holder(actions, seat_id=worker_seat, agent_id=worker_agent)

    office = tmp_path / "office"
    office.mkdir()
    transcript = tmp_path / "t.jsonl"
    _write_transcript(
        transcript,
        {"type": "assistant", "isSidechain": False,
         "message": {"content": "Want me to proceed straight into that now?"}},
    )
    job_dir = str(tmp_path / "jobs" / "prkedqst")  # EXACTLY 8 chars — find_session_row's contract
    await save_mount(actions.pool, job_dir=job_dir, agent_id=worker_agent, project="testhouse",
                     cwd=str(office), model=None, session_key=None)
    sid = "prkedqst-0000-4000-8000-000000000000"  # sid[:8] == "prkedqst"

    before = await actions.pool.fetchval("SELECT count(*) FROM fleet_messages")
    await stophook._stage_a_async(
        {"cwd": str(office), "session_id": sid, "transcript_path": str(transcript)})
    after = await actions.pool.fetchval("SELECT count(*) FROM fleet_messages")
    assert after == before + 1
    row = await actions.pool.fetchrow(
        "SELECT from_agent, to_agent, body, grade FROM fleet_messages ORDER BY id DESC LIMIT 1")
    assert row["from_agent"] == worker_agent
    assert row["to_agent"] == manager_seat
    assert row["grade"] == "fyi"
    assert "likely parked" in row["body"]
    assert "proceed straight into that now?" in row["body"]


async def test_stage_a_does_not_confess_when_the_question_already_went_out_as_mail(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pg_dsn: str,
) -> None:
    """The fine case: a real ask already left via mail (grade='ask') — the trailing '?' in
    the transcript is just narrating a question someone WILL actually see, not one asked
    into an empty room. No duplicate confession."""
    monkeypatch.setattr(stophook, "DSN", pg_dsn)
    worker_agent, worker_seat = "agent:parked002", "seat:parked002"
    manager_agent, manager_seat = "agent:parkmgr02", "seat:parkmgr02"
    worker_obj = await actions.create_or_find_object("Seat", worker_seat, worker_agent)
    manager_obj = await actions.create_or_find_object("Seat", manager_seat, manager_agent)
    now = datetime.now(UTC)
    await actions.assert_property(worker_obj, "handle", "parkedworker2", worker_agent, now,
                                  0.9, evidence_class="self_declared")
    await actions.create_link(worker_obj, manager_obj, "managed_by", "test", now, 0.9,
                              evidence_class="self_declared")
    await bind_holder(actions, seat_id=worker_seat, agent_id=worker_agent)
    await send_message(actions.pool, from_agent=worker_agent, from_project="osiris",
                       to_agent=manager_seat, body="Want me to proceed straight into that now?",
                       grade="ask")

    office = tmp_path / "office2"
    office.mkdir()
    transcript = tmp_path / "t2.jsonl"
    _write_transcript(
        transcript,
        {"type": "assistant", "isSidechain": False,
         "message": {"content": "Want me to proceed straight into that now?"}},
    )
    job_dir = str(tmp_path / "jobs" / "prkedqs2")
    await save_mount(actions.pool, job_dir=job_dir, agent_id=worker_agent, project="testhouse",
                     cwd=str(office), model=None, session_key=None)
    sid = "prkedqs2-0000-4000-8000-000000000000"

    before = await actions.pool.fetchval("SELECT count(*) FROM fleet_messages")
    await stophook._stage_a_async(
        {"cwd": str(office), "session_id": sid, "transcript_path": str(transcript)})
    after = await actions.pool.fetchval("SELECT count(*) FROM fleet_messages")
    assert after == before  # no NEW confession — the real ask already covers it


# ═══════════ STAGE C — THE TURN-END PRACTICE AUDIT ═══════════

async def test_active_practices_excludes_refuted_and_reads_the_statement(
    actions: Actions,
) -> None:
    """Same shape as _fn_practices, hand-duplicated because this file's bare
    asyncpg.connect has no JSON codec — but a REFUTED practice is excluded here (unlike
    practices()'s own on-demand listing, which still shows one, flagged): dead law must
    never trip a live-turn audit."""
    live = await record_practice(actions, "route every dispatch through the DM lane")
    dead = await record_practice(actions, "always vendor node_modules by hand")
    d = await record_decision(actions, "vendoring node_modules by hand was a maintenance trap")
    await refute_practice(actions, str(dead), killed_by=str(d))

    out = await stophook._active_practices(actions.pool)
    ids = {p["id"] for p in out}
    assert str(live) in ids
    assert str(dead) not in ids
    live_row = next(p for p in out if p["id"] == str(live))
    assert live_row["statement"] == "route every dispatch through the DM lane"


def _enable_stage_c(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stage C is OFF by default (Settings.osiris_stage_c_practice_check_enabled=False,
    decision 54280c72: 5 days live, 20 flags in one 24h sample, 14/14 verified false, zero
    confirmed true positives anywhere in its deployment history). The tests below exercise
    the DETECTION logic itself (quote suppression, topical-overlap gating, dedup) — that
    logic still needs proving correct even disarmed by default, so each one explicitly
    arms it rather than relying on a default that could silently flip again later."""
    import src.config.settings as settings_module

    monkeypatch.setattr(
        settings_module, "get_settings",
        lambda: settings_module.Settings(osiris_stage_c_practice_check_enabled=True))


async def test_stage_a_practice_check_disabled_by_default_sends_nothing(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pg_dsn: str,
) -> None:
    """THE ACCEPTANCE TEST FOR THE KILL SWITCH ITSELF (Thoth ruling, DM 3059, decision
    54280c72): the EXACT genuine-violation scenario the very next test proves DOES confess
    once armed must send NOTHING with no override at all -- Settings()'s own real default,
    not a monkeypatched one, so a regression that silently re-enables Stage C is caught
    here rather than assumed away by always testing through the arm helper."""
    monkeypatch.setattr(stophook, "DSN", pg_dsn)
    worker_agent, worker_seat = "agent:pviol000", "seat:pviol000"
    manager_agent, manager_seat = "agent:pvmgr000", "seat:pvmgr000"
    worker_obj = await actions.create_or_find_object("Seat", worker_seat, worker_agent)
    manager_obj = await actions.create_or_find_object("Seat", manager_seat, manager_agent)
    now = datetime.now(UTC)
    await actions.assert_property(worker_obj, "handle", "pviolworker0", worker_agent, now,
                                  0.9, evidence_class="self_declared")
    await actions.create_link(worker_obj, manager_obj, "managed_by", "test", now, 0.9,
                              evidence_class="self_declared")
    await bind_holder(actions, seat_id=worker_seat, agent_id=worker_agent)
    await record_practice(
        actions, "batch small commits into one PR for this class of change")

    office = tmp_path / "office0"
    office.mkdir()
    transcript = tmp_path / "t0.jsonl"
    _write_transcript(
        transcript,
        {"type": "assistant", "isSidechain": False, "message": {"content":
            "Let's stop doing small batch commits for this kind of change — one big "
            "commit instead. Done."}},
    )
    job_dir = str(tmp_path / "jobs" / "pviolat0")
    await save_mount(actions.pool, job_dir=job_dir, agent_id=worker_agent, project="testhouse",
                     cwd=str(office), model=None, session_key=None)
    sid = "pviolat0-0000-4000-8000-000000000000"

    before = await actions.pool.fetchval("SELECT count(*) FROM fleet_messages")
    await stophook._stage_a_async(
        {"cwd": str(office), "session_id": sid, "transcript_path": str(transcript)})
    after = await actions.pool.fetchval("SELECT count(*) FROM fleet_messages")
    assert after == before  # disarmed by default -- the same turn that WOULD confess below


async def test_stage_a_confesses_when_a_turn_violates_a_standing_practice(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pg_dsn: str,
) -> None:
    """The v3 gap (c54e8176's own second case): a turn can violate standing law WITHOUT
    ever recording a decision — no write, so layer 1's write-time check never fires. Stage
    C catches it off the turn's own tail text at stop time, courtesy-DM only, never a
    block."""
    monkeypatch.setattr(stophook, "DSN", pg_dsn)
    _enable_stage_c(monkeypatch)
    worker_agent, worker_seat = "agent:pviol001", "seat:pviol001"
    manager_agent, manager_seat = "agent:pvmgr001", "seat:pvmgr001"
    worker_obj = await actions.create_or_find_object("Seat", worker_seat, worker_agent)
    manager_obj = await actions.create_or_find_object("Seat", manager_seat, manager_agent)
    now = datetime.now(UTC)
    await actions.assert_property(worker_obj, "handle", "pviolworker", worker_agent, now,
                                  0.9, evidence_class="self_declared")
    await actions.create_link(worker_obj, manager_obj, "managed_by", "test", now, 0.9,
                              evidence_class="self_declared")
    await bind_holder(actions, seat_id=worker_seat, agent_id=worker_agent)
    await record_practice(
        actions, "batch small commits into one PR for this class of change")

    office = tmp_path / "office"
    office.mkdir()
    transcript = tmp_path / "t.jsonl"
    _write_transcript(
        transcript,
        {"type": "assistant", "isSidechain": False, "message": {"content":
            "Let's stop doing small batch commits for this kind of change — one big "
            "commit instead. Done."}},
    )
    job_dir = str(tmp_path / "jobs" / "pviolat1")
    await save_mount(actions.pool, job_dir=job_dir, agent_id=worker_agent, project="testhouse",
                     cwd=str(office), model=None, session_key=None)
    sid = "pviolat1-0000-4000-8000-000000000000"

    before = await actions.pool.fetchval("SELECT count(*) FROM fleet_messages")
    await stophook._stage_a_async(
        {"cwd": str(office), "session_id": sid, "transcript_path": str(transcript)})
    after = await actions.pool.fetchval("SELECT count(*) FROM fleet_messages")
    assert after == before + 1
    row = await actions.pool.fetchrow(
        "SELECT from_agent, to_agent, body, grade FROM fleet_messages ORDER BY id DESC LIMIT 1")
    assert row["from_agent"] == worker_agent
    assert row["to_agent"] == manager_seat
    assert row["grade"] == "fyi"
    assert "may have violated standing Practice" in row["body"]
    assert "batch small commits into one PR for this class of change" in row["body"]
    assert "stop" in row["body"]


async def test_stage_a_does_not_confess_when_a_turn_merely_mentions_a_practice_topic(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pg_dsn: str,
) -> None:
    """Topical overlap alone, with no reversal cue, is just a plain mention — not flagged.
    Distinguishes this from the violation case above by wording only."""
    monkeypatch.setattr(stophook, "DSN", pg_dsn)
    _enable_stage_c(monkeypatch)
    worker_agent, worker_seat = "agent:pviol002", "seat:pviol002"
    manager_agent, manager_seat = "agent:pvmgr002", "seat:pvmgr002"
    worker_obj = await actions.create_or_find_object("Seat", worker_seat, worker_agent)
    manager_obj = await actions.create_or_find_object("Seat", manager_seat, manager_agent)
    now = datetime.now(UTC)
    await actions.assert_property(worker_obj, "handle", "pviolworker2", worker_agent, now,
                                  0.9, evidence_class="self_declared")
    await actions.create_link(worker_obj, manager_obj, "managed_by", "test", now, 0.9,
                              evidence_class="self_declared")
    await bind_holder(actions, seat_id=worker_seat, agent_id=worker_agent)
    await record_practice(
        actions, "batch small commits into one PR for this class of change")

    office = tmp_path / "office2"
    office.mkdir()
    transcript = tmp_path / "t2.jsonl"
    _write_transcript(
        transcript,
        {"type": "assistant", "isSidechain": False, "message": {"content":
            "Batching small commits into one PR for this class of change, as usual. Done."}},
    )
    job_dir = str(tmp_path / "jobs" / "pviolat2")
    await save_mount(actions.pool, job_dir=job_dir, agent_id=worker_agent, project="testhouse",
                     cwd=str(office), model=None, session_key=None)
    sid = "pviolat2-0000-4000-8000-000000000000"

    before = await actions.pool.fetchval("SELECT count(*) FROM fleet_messages")
    await stophook._stage_a_async(
        {"cwd": str(office), "session_id": sid, "transcript_path": str(transcript)})
    after = await actions.pool.fetchval("SELECT count(*) FROM fleet_messages")
    assert after == before  # topical mention only, no reversal cue — nothing to flag


async def test_stage_a_does_not_confess_when_the_turn_quotes_the_practice_verbatim(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pg_dsn: str,
) -> None:
    """The live specimen (Thoth, msg 1800): a settle turn that QUOTES a standing
    Practice's own text back verbatim tripped the Practice's own "never" — citation,
    not reversal. Reproduces it end to end: the practice's own statement carries a cue
    word, and the turn cites it directly."""
    monkeypatch.setattr(stophook, "DSN", pg_dsn)
    _enable_stage_c(monkeypatch)
    worker_agent, worker_seat = "agent:pviol003", "seat:pviol003"
    manager_agent, manager_seat = "agent:pvmgr003", "seat:pvmgr003"
    worker_obj = await actions.create_or_find_object("Seat", worker_seat, worker_agent)
    manager_obj = await actions.create_or_find_object("Seat", manager_seat, manager_agent)
    now = datetime.now(UTC)
    await actions.assert_property(worker_obj, "handle", "pviolworker3", worker_agent, now,
                                  0.9, evidence_class="self_declared")
    await actions.create_link(worker_obj, manager_obj, "managed_by", "test", now, 0.9,
                              evidence_class="self_declared")
    await bind_holder(actions, seat_id=worker_seat, agent_id=worker_agent)
    await record_practice(
        actions, "never batch small commits into one PR for this class of change")

    office = tmp_path / "office3"
    office.mkdir()
    transcript = tmp_path / "t3.jsonl"
    _write_transcript(
        transcript,
        {"type": "assistant", "isSidechain": False, "message": {"content":
            "As recorded: never batch small commits into one PR for this class of "
            "change. Settling now."}},
    )
    job_dir = str(tmp_path / "jobs" / "pviolat3")
    await save_mount(actions.pool, job_dir=job_dir, agent_id=worker_agent, project="testhouse",
                     cwd=str(office), model=None, session_key=None)
    sid = "pviolat3-0000-4000-8000-000000000000"

    before = await actions.pool.fetchval("SELECT count(*) FROM fleet_messages")
    await stophook._stage_a_async(
        {"cwd": str(office), "session_id": sid, "transcript_path": str(transcript)})
    after = await actions.pool.fetchval("SELECT count(*) FROM fleet_messages")
    assert after == before  # quoting the practice's own text is citation, not reversal


async def test_stage_a_does_not_confess_when_reporting_someone_elses_violation_while_quoting_it(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pg_dsn: str,
) -> None:
    """TASK #104 (Thoth's DM 2228; thread b318a9d3), end to end — the named regression
    case (msg 2211): reporting a violation BY SOMEONE ELSE reads as reversal language in
    its own sentence, while the practice cited as evidence is quoted verbatim one
    sentence over. Same discipline as the msg-1800 test above, but proving the WIDER
    (whole-turn, not same-sentence) quote scope this task added."""
    monkeypatch.setattr(stophook, "DSN", pg_dsn)
    _enable_stage_c(monkeypatch)
    worker_agent, worker_seat = "agent:pviol004", "seat:pviol004"
    manager_agent, manager_seat = "agent:pvmgr004", "seat:pvmgr004"
    worker_obj = await actions.create_or_find_object("Seat", worker_seat, worker_agent)
    manager_obj = await actions.create_or_find_object("Seat", manager_seat, manager_agent)
    now = datetime.now(UTC)
    await actions.assert_property(worker_obj, "handle", "pviolworker4", worker_agent, now,
                                  0.9, evidence_class="self_declared")
    await actions.create_link(worker_obj, manager_obj, "managed_by", "test", now, 0.9,
                              evidence_class="self_declared")
    await bind_holder(actions, seat_id=worker_seat, agent_id=worker_agent)
    await record_practice(
        actions, "A stash's blast radius is the FILE, not the AUTHOR — before "
                 "pathspec-scoping git stash push to agent X's known files, check the "
                 "FULL current git status for every path about to be targeted, not "
                 "just the one agent you have in mind.")

    office = tmp_path / "office4"
    office.mkdir()
    transcript = tmp_path / "t4.jsonl"
    _write_transcript(
        transcript,
        {"type": "assistant", "isSidechain": False, "message": {"content":
            "Confirmed: they skipped checking git status before stashing — instead of "
            "verifying every path, they assumed it was scoped to one agent. Exactly "
            "the practice's own warning: \"A stash's blast radius is the FILE, not the "
            "AUTHOR — before pathspec-scoping git stash push to agent X's known files, "
            "check the FULL current git status for every path about to be targeted, "
            "not just the one agent you have in mind.\""}},
    )
    job_dir = str(tmp_path / "jobs" / "pviolat4")
    await save_mount(actions.pool, job_dir=job_dir, agent_id=worker_agent, project="testhouse",
                     cwd=str(office), model=None, session_key=None)
    sid = "pviolat4-0000-4000-8000-000000000000"

    before = await actions.pool.fetchval("SELECT count(*) FROM fleet_messages")
    await stophook._stage_a_async(
        {"cwd": str(office), "session_id": sid, "transcript_path": str(transcript)})
    after = await actions.pool.fetchval("SELECT count(*) FROM fleet_messages")
    assert after == before  # reporting a violation is not the reporter's own reversal


# ═══════════ STAGE C DEDUP — thread e96ed0c5 ═══════════

async def test_already_flagged_today_true_only_for_the_same_agent_practice_pair_today(
    actions: Actions,
) -> None:
    agent = "agent:dedupflag1"
    # send_message refuses a to_project nobody has ever mounted under (f6f3e43e, shape 3 of
    # #117) -- alive=False registers 'osiris' as existing without a live pulse.
    await save_mount(actions.pool, job_dir="/test/seed/osiris", agent_id="agent:seed-osiris",
                     project="osiris", cwd="/test", model=None, session_key=None, alive=False)
    await send_message(
        actions.pool, from_agent=agent, from_project="osiris", to_project="osiris",
        body="stopping; this turn may have violated standing Practice abc12345 "
             "(\"some statement\") — reversal language found (stop); a heuristic flag, "
             "not a verdict, worth a look",
        grade="fyi")
    assert await stophook._already_flagged_today(actions.pool, agent, "abc12345") is True
    # a DIFFERENT practice id is not covered by the same flag
    assert await stophook._already_flagged_today(actions.pool, agent, "zzz99999") is False
    # a different agent never flagged today
    assert await stophook._already_flagged_today(
        actions.pool, "agent:neverflagged", "abc12345") is False


async def test_already_flagged_today_ignores_a_flag_from_a_prior_day(
    actions: Actions,
) -> None:
    agent = "agent:dedupflag2"
    await save_mount(actions.pool, job_dir="/test/seed/osiris", agent_id="agent:seed-osiris",
                     project="osiris", cwd="/test", model=None, session_key=None, alive=False)
    r = await send_message(
        actions.pool, from_agent=agent, from_project="osiris", to_project="osiris",
        body="stopping; this turn may have violated standing Practice def67890 "
             "(\"some statement\") — reversal language found (stop); a heuristic flag, "
             "not a verdict, worth a look",
        grade="fyi")
    await actions.pool.execute(
        "UPDATE fleet_messages SET created_at=now() - interval '1 day' WHERE id=$1", r["id"])
    assert await stophook._already_flagged_today(actions.pool, agent, "def67890") is False


async def test_stage_a_sends_the_practice_confession_once_per_agent_practice_per_day(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pg_dsn: str,
) -> None:
    """Thread e96ed0c5 (Thoth, msg 1819): the SAME (agent, practice) violating turn,
    hit twice in one day (two separate stops), sends its courtesy confession only once —
    detection still runs both times, only the duplicate DM is suppressed."""
    _enable_stage_c(monkeypatch)
    monkeypatch.setattr(stophook, "DSN", pg_dsn)
    worker_agent, worker_seat = "agent:pviol005", "seat:pviol005"
    manager_agent, manager_seat = "agent:pvmgr005", "seat:pvmgr005"
    worker_obj = await actions.create_or_find_object("Seat", worker_seat, worker_agent)
    manager_obj = await actions.create_or_find_object("Seat", manager_seat, manager_agent)
    now = datetime.now(UTC)
    await actions.assert_property(worker_obj, "handle", "pviolworker5", worker_agent, now,
                                  0.9, evidence_class="self_declared")
    await actions.create_link(worker_obj, manager_obj, "managed_by", "test", now, 0.9,
                              evidence_class="self_declared")
    await bind_holder(actions, seat_id=worker_seat, agent_id=worker_agent)
    await record_practice(
        actions, "batch small commits into one PR for this class of change")

    office = tmp_path / "office5"
    office.mkdir()
    transcript = tmp_path / "t5.jsonl"
    _write_transcript(
        transcript,
        {"type": "assistant", "isSidechain": False, "message": {"content":
            "Let's stop doing small batch commits for this kind of change — one big "
            "commit instead. Done."}},
    )
    job_dir = str(tmp_path / "jobs" / "pviolat5")
    await save_mount(actions.pool, job_dir=job_dir, agent_id=worker_agent, project="testhouse",
                     cwd=str(office), model=None, session_key=None)
    sid = "pviolat5-0000-4000-8000-000000000000"

    before = await actions.pool.fetchval("SELECT count(*) FROM fleet_messages")
    await stophook._stage_a_async(
        {"cwd": str(office), "session_id": sid, "transcript_path": str(transcript)})
    after_first = await actions.pool.fetchval("SELECT count(*) FROM fleet_messages")
    assert after_first == before + 1  # the first hit today still confesses

    # push the first confession's own created_at outside send_message's OWN identical-
    # body dedup window (600s default) — without this, a second identical send would be
    # silently deduped by that generic mechanism, and the test would prove nothing about
    # THIS task's (agent, practice)-per-day dedup specifically. CLAMPED TO THE UTC DAY
    # BOUNDARY: a bare `now() - 700s` crosses midnight for the first 11m40s of every UTC
    # day, moving the first confession to YESTERDAY, so the per-day dedup correctly
    # does not fire and the second send lands — the test then fails on a clock, not a
    # defect (caught live 2026-08-17 00:07 UTC, deterministic in that window, green at
    # 23:44). Inside that window the clamp leaves the row within send's own 600s
    # window, so the assertion still holds — via the generic dedup rather than the
    # per-day one — a weaker proof for ~12 minutes a day, never a false red.
    await actions.pool.execute(
        "UPDATE fleet_messages SET created_at=greatest("
        "  now() - interval '700 seconds', date_trunc('day', now()) + interval '1 second') "
        "WHERE id=(SELECT id FROM fleet_messages WHERE from_agent=$1 "
        "ORDER BY id DESC LIMIT 1)", worker_agent)
    await stophook._stage_a_async(
        {"cwd": str(office), "session_id": sid, "transcript_path": str(transcript)})
    after_second = await actions.pool.fetchval("SELECT count(*) FROM fleet_messages")
    assert after_second == after_first  # the second hit, same day, same practice: no duplicate


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


async def test_stage_a_stamps_context_pct_alongside_pending_when_both_apply(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pg_dsn: str,
) -> None:
    """Leg 2 integration: a seated worker with nothing leased gets BOTH state='pending'
    (Pit Watch, unchanged) AND context_pct (new) from the SAME stop, when a good pct
    rides along."""
    monkeypatch.setattr(stophook, "DSN", pg_dsn)
    agent, seat = "agent:pctstage1", "seat:pctstage1"
    seat_obj = await actions.create_or_find_object("Seat", seat, agent)
    now = datetime.now(UTC)
    await actions.assert_property(seat_obj, "handle", "workerpct", agent, now, 0.9,
                                  evidence_class="self_declared")
    await bind_holder(actions, seat_id=seat, agent_id=agent)
    job_dir = str(tmp_path / "jobs" / "pctstage")  # EXACTLY 8 chars — find_session_row's contract
    await save_mount(actions.pool, job_dir=job_dir, agent_id=agent, project="testhouse",
                     cwd=str(tmp_path / "officepct"), model=None, session_key=None)
    sid = "pctstage1-0000-4000-8000-000000000000"  # sid[:8] == "pctstage"

    await stophook._stage_a_async(
        {"cwd": str(tmp_path / "officepct"), "session_id": sid}, 88)
    row = await actions.pool.fetchrow(
        "SELECT "
        " (SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "   WHERE o.canonical=$1 AND a.name='state' LIMIT 1) AS state, "
        " (SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "   WHERE o.canonical=$1 AND a.name='context_pct' LIMIT 1) AS pct", agent)
    assert row["state"] == "pending"
    assert row["pct"] == "88"


async def test_stage_a_stamps_context_pct_even_for_an_unseated_agent(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pg_dsn: str,
) -> None:
    """context_pct is useful to any co-agent glancing at orient(), not only a manager —
    it must not be gated on seat-binding the way the confession/pending machinery is."""
    monkeypatch.setattr(stophook, "DSN", pg_dsn)
    agent = "agent:pctnoseat1"
    await actions.create_or_find_object("Agent", agent, agent)
    job_dir = str(tmp_path / "jobs" / "pctnosea")  # EXACTLY 8 chars — find_session_row's contract
    await save_mount(actions.pool, job_dir=job_dir, agent_id=agent, project="testhouse",
                     cwd=str(tmp_path / "office10"), model=None, session_key=None)
    sid = "pctnoseat-0000-4000-8000-000000000000"  # sid[:8] == "pctnosea"

    await stophook._stage_a_async(
        {"cwd": str(tmp_path / "office10"), "session_id": sid}, 45)
    pct = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='context_pct' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", agent)
    assert pct == "45"
    state = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='state' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", agent)
    assert state is None  # pending is a seat-scoped concept — never invented for no seat


def test_stage_a_never_raises_when_the_database_is_unreachable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Acceptance: a stop with mail machinery down still stops clean."""
    monkeypatch.setattr(stophook, "DSN", "postgresql://u:p@127.0.0.1:1/osiris")
    stophook._stage_a({"cwd": "/nowhere", "session_id": "deadbeef0000"})  # must not raise


# ═══════════ task #180 piece 2 (b): `_stop_via_http`, tried before any direct connect ══════

def test_stop_via_http_returns_none_when_the_route_is_unreachable() -> None:
    """STOP_URL is redirected to a guaranteed-closed port by the autouse fixture above —
    proves the degrade path directly: connection failure is a quiet None, never a raised
    exception the caller would have to catch."""
    assert stophook._stop_via_http("deliverable", cwd="/nowhere", session_id="x") is None


def test_stop_via_http_parses_a_real_response(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {"result": {"n": 2, "senders": ["agent:x"], "window": 200000,
                          "bands": {"ask": 1, "fyi": 1}, "project": "osiris"}}

    class _FakeResp:
        def __enter__(self) -> _FakeResp:
            return self

        def __exit__(self, *a: object) -> None:
            return None

        def read(self) -> bytes:
            return json.dumps(payload).encode()

    monkeypatch.setattr("urllib.request.urlopen", lambda *a, **k: _FakeResp())
    result = stophook._stop_via_http("deliverable", cwd="/repo", session_id="x")
    assert result == payload["result"]


def test_stop_via_http_a_server_side_error_is_also_a_quiet_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeResp:
        def __enter__(self) -> _FakeResp:
            return self

        def __exit__(self, *a: object) -> None:
            return None

        def read(self) -> bytes:
            return b'{"error": "unknown phase"}'

    monkeypatch.setattr("urllib.request.urlopen", lambda *a, **k: _FakeResp())
    assert stophook._stop_via_http("bogus", cwd="/repo", session_id="x") is None


def test_stop_via_http_a_legitimate_null_offload_result_still_reaches_the_route(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The 'offload' phase's own None (an unresolvable session) IS None, indistinguishable
    from a route failure at this layer on purpose (both degrade the caller to its own
    direct-connect fallback — worst case one redundant DB round-trip, never a wrong
    answer). This test proves the response was genuinely PARSED (the fake urlopen was
    actually called), not short-circuited by the OSError path."""
    called = []

    class _FakeResp:
        def __enter__(self) -> _FakeResp:
            return self

        def __exit__(self, *a: object) -> None:
            return None

        def read(self) -> bytes:
            return b'{"result": null}'

    def _fake_urlopen(*a: object, **k: object) -> _FakeResp:
        called.append(True)
        return _FakeResp()

    monkeypatch.setattr("urllib.request.urlopen", _fake_urlopen)
    assert stophook._stop_via_http("offload", cwd="/repo", session_id="x") is None
    assert called == [True]
