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
from src.orchestrator.mounts import save_mount

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
