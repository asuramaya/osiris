"""The context lens — a mind's view of its own mortality (operator request, 2026-07-09).
Pure file-parsing over the harness's own transcript records: no graph, no containers."""
from __future__ import annotations

import json
from pathlib import Path

from src.orchestrator.context_lens import detail, glance, last_usage, window_for


def _entry(*, input_t: int, cache_read: int = 0, cache_new: int = 0, out: int = 100,
           sidechain: bool = False) -> str:
    return json.dumps({
        "type": "assistant", "isSidechain": sidechain, "timestamp": "2026-07-09T12:00:00Z",
        "message": {"model": "claude-opus-4-8", "usage": {
            "input_tokens": input_t, "cache_read_input_tokens": cache_read,
            "cache_creation_input_tokens": cache_new, "output_tokens": out}}})


def _boundary(ts: str) -> str:
    return json.dumps({"type": "system", "subtype": "compact_boundary", "timestamp": ts})


def test_window_tier_reads_the_display_id() -> None:
    assert window_for("claude-opus-4-8[1m]") == (1_000_000, False)
    assert window_for("claude-opus-4-8") == (200_000, True)   # assumed default, flagged
    assert window_for(None) == (200_000, True)
    # self-correction: an occupancy past 200k on a live session proves the default wrong
    assert window_for("claude-fable-5", used=286_000) == (1_000_000, True)
    assert window_for("claude-fable-5", used=150_000) == (200_000, True)
    # the operator's word beats every heuristic
    import os
    os.environ["OSIRIS_CONTEXT_WINDOW"] = "500000"
    try:
        assert window_for("claude-fable-5", used=286_000) == (500_000, False)
    finally:
        del os.environ["OSIRIS_CONTEXT_WINDOW"]


def test_glance_reads_the_last_main_loop_usage(tmp_path: Path) -> None:
    t = tmp_path / "s.jsonl"
    t.write_text("\n".join([
        _entry(input_t=1000, cache_read=50_000),
        _entry(input_t=2000, cache_read=118_000, cache_new=5_000),
        _entry(input_t=9_999_999, sidechain=True),      # a sub-agent's usage is not the seat's
        json.dumps({"type": "user", "message": {"content": "hi"}}),
    ]) + "\n")
    g = glance(t, "claude-opus-4-8")
    assert g is not None
    assert g["used"] == 2000 + 118_000 + 5_000          # the LAST main-loop turn, fresh+cached
    assert g["pct"] == round(100 * 125_000 / 200_000)   # 62% of the assumed 200k
    # the same occupancy on a 1M tab is a calm 12%
    assert glance(t, "claude-opus-4-8[1m]")["pct"] == 12  # type: ignore[index]


def test_glance_survives_garbage_and_absence(tmp_path: Path) -> None:
    empty = tmp_path / "empty.jsonl"
    empty.write_text("not json\n{\"type\":\"user\"}\n")
    assert glance(empty, "x") is None
    assert last_usage(tmp_path / "missing.jsonl") is None


def test_detail_counts_the_deaths_and_sounds_the_alarm(tmp_path: Path) -> None:
    t = tmp_path / "s.jsonl"
    t.write_text("\n".join([
        _entry(input_t=1000),
        _boundary("2026-07-09T10:00:00Z"),
        _entry(input_t=2000),
        _boundary("2026-07-09T11:00:00Z"),
        _entry(input_t=4_000, cache_read=170_000),       # 174k of 200k = 87% — alarm range
    ]) + "\n")
    d = detail(t, "claude-opus-4-8")
    assert d["compactions_this_session"] == 2
    assert d["last_compaction_at"] == "2026-07-09T11:00:00Z"
    assert d["used"] == 174_000 and d["pct"] == 87 and d["window_assumed"] is True
    assert d["remaining"] == 26_000
    assert d["assistant_turns"] == 3
    assert "Write back NOW" in d["warning"]              # the ritual, tied to the numbers
    # the harness's own window (stamped from the payload) beats every heuristic: same
    # transcript, 1M hint → a calm 17%, no warning, nothing assumed
    hinted = detail(t, "claude-opus-4-8", window_hint=1_000_000)
    assert hinted["window"] == 1_000_000 and hinted["window_assumed"] is False
    assert hinted["pct"] == 17 and "warning" not in hinted
    # a young transcript measures honestly
    young = tmp_path / "young.jsonl"
    young.write_text(json.dumps({"type": "user"}) + "\n")
    assert "error" in detail(young, None)
