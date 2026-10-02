"""The log of moments osiris's own context figure and the harness's differ. Every test uses
a scratch state dir (an explicit path, or HOME pointed at tmp_path), never the real one."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import scripts.osiris_hook as osiris_hook
from src.orchestrator import context_disagreement as cd


def _record(path: Path, harness: int | None, derived: int | None, **over: Any) -> bool:
    kwargs: dict[str, Any] = {
        "harness_pct": harness, "derived_pct": derived, "window": 1_000_000,
        "model": "claude-sonnet-5", "session_id": "sess-1", "path": path, "now": 1000.0}
    kwargs.update(over)
    return cd.record_if_different(**kwargs)


def test_agreeing_figures_write_nothing(tmp_path: Path) -> None:
    log = tmp_path / "state" / "log.jsonl"
    assert _record(log, 56, 56) is False
    assert not log.exists() and not log.parent.exists()


def test_a_one_point_difference_is_not_a_disagreement(tmp_path: Path) -> None:
    """Two correct roundings of one ratio can differ by a point."""
    log = tmp_path / "log.jsonl"
    assert _record(log, 57, 56) is False
    assert _record(log, 55, 56) is False
    assert not log.exists()


def test_a_two_point_difference_writes_one_json_line(tmp_path: Path) -> None:
    log = tmp_path / "state" / "log.jsonl"
    assert _record(log, 59, 56) is True
    (line,) = log.read_text().splitlines()
    assert json.loads(line) == {
        "ts": 1000.0, "session_id": "sess-1", "harness_pct": 59, "derived_pct": 56,
        "window": 1_000_000, "model": "claude-sonnet-5"}


def test_an_unknown_figure_writes_nothing(tmp_path: Path) -> None:
    log = tmp_path / "log.jsonl"
    assert _record(log, None, 40) is False
    assert _record(log, 40, None) is False
    assert not log.exists()


def test_an_identical_repeat_is_dropped_but_a_change_is_kept(tmp_path: Path) -> None:
    log = tmp_path / "log.jsonl"
    assert _record(log, 59, 56) is True
    assert _record(log, 59, 56, now=1001.0) is False  # the status line renders constantly
    assert _record(log, 60, 56, now=1002.0) is True   # the gap moved
    assert _record(log, 60, 56, session_id="sess-2", now=1003.0) is True  # another session
    assert len(log.read_text().splitlines()) == 3


def test_the_log_is_capped_and_keeps_the_newest(tmp_path: Path, monkeypatch: Any) -> None:
    monkeypatch.setattr(cd, "CAP_LINES", 5)
    monkeypatch.setattr(cd, "_TRIM_BYTES", 200)
    log = tmp_path / "log.jsonl"
    for n in range(40):
        _record(log, 70 + (n % 25), 40, now=float(n), session_id=f"s{n}")
    lines = log.read_text().splitlines()
    assert len(lines) <= 5 + 2  # trimmed back near the cap on the next oversized write
    assert json.loads(lines[-1])["session_id"] == "s39"
    assert not (tmp_path / "log.jsonl.tmp").exists()


def test_an_unwritable_path_is_swallowed_not_raised(tmp_path: Path) -> None:
    blocker = tmp_path / "blocker"
    blocker.write_text("a file where a directory is needed")
    assert _record(blocker / "log.jsonl", 80, 50) is False


def test_read_recent_is_newest_first_skips_bad_lines_and_honours_the_limit(
    tmp_path: Path,
) -> None:
    log = tmp_path / "log.jsonl"
    _record(log, 60, 50, now=1.0, session_id="a")
    with log.open("a") as fh:
        fh.write("not json at all\n")
    _record(log, 70, 50, now=2.0, session_id="b")
    _record(log, 80, 50, now=3.0, session_id="c")
    assert [e["session_id"] for e in cd.read_recent(10, log)] == ["c", "b", "a"]
    assert [e["session_id"] for e in cd.read_recent(2, log)] == ["c", "b"]
    assert cd.read_recent(5, tmp_path / "missing.jsonl") == []


def test_summarize_names_which_side_reads_higher(tmp_path: Path) -> None:
    log = tmp_path / "log.jsonl"
    _record(log, 60, 50, session_id="a")   # harness higher by 10
    _record(log, 40, 45, session_id="b")   # osiris higher by 5
    _record(log, 61, 50, session_id="c")   # harness higher by 11
    out = cd.summarize(cd.read_recent(10, log))
    assert out == {"entries": 3, "largest_gap": 11, "harness_higher": 2, "osiris_higher": 1}
    assert cd.summarize([]) == {
        "entries": 0, "largest_gap": None, "harness_higher": 0, "osiris_higher": 0}


# --- the hook: both figures in hand, a scratch HOME -----------------------------------------

def _transcript(tmp_path: Path, used: int) -> Path:
    path = tmp_path / "t.jsonl"
    entry = {"type": "assistant", "message": {"usage": {
        "input_tokens": used, "cache_read_input_tokens": 0,
        "cache_creation_input_tokens": 0, "output_tokens": 5}}}
    path.write_text(json.dumps(entry) + "\n")
    return path


def _scratch_log(tmp_path: Path, monkeypatch: Any) -> Path:
    monkeypatch.setenv("HOME", str(tmp_path))
    return tmp_path / ".local" / "state" / "osiris" / "context_pct_disagreements.jsonl"


def test_the_hook_logs_a_disagreement_from_a_real_transcript(
    tmp_path: Path, monkeypatch: Any,
) -> None:
    log = _scratch_log(tmp_path, monkeypatch)
    transcript = _transcript(tmp_path, 100_000)  # 10% of a 1,000,000 window
    osiris_hook._note_context_disagreement(
        {}, 14, 1_000_000, "claude-sonnet-5", "sess-9", str(transcript))
    entry = json.loads(log.read_text())
    assert (entry["harness_pct"], entry["derived_pct"], entry["window"]) == (14, 10, 1_000_000)
    assert entry["session_id"] == "sess-9" and entry["model"] == "claude-sonnet-5"


def test_the_hook_writes_nothing_when_the_figures_agree(
    tmp_path: Path, monkeypatch: Any,
) -> None:
    log = _scratch_log(tmp_path, monkeypatch)
    transcript = _transcript(tmp_path, 100_000)
    osiris_hook._note_context_disagreement(
        {}, 10, 1_000_000, "claude-sonnet-5", "sess-9", str(transcript))
    osiris_hook._note_context_disagreement(
        {}, 11, 1_000_000, "claude-sonnet-5", "sess-9", str(transcript))
    assert not log.exists()


def test_the_hook_never_logs_against_a_guessed_window(tmp_path: Path, monkeypatch: Any) -> None:
    """With no window from the harness and no [1m] tier the denominator is a guess, so a
    difference from the harness's own figure would prove nothing."""
    log = _scratch_log(tmp_path, monkeypatch)
    monkeypatch.delenv("OSIRIS_CONTEXT_WINDOW", raising=False)
    transcript = _transcript(tmp_path, 100_000)
    osiris_hook._note_context_disagreement(
        {}, 14, None, "claude-sonnet-5", "sess-9", str(transcript))
    assert not log.exists()


def test_the_hook_swallows_every_failure(tmp_path: Path, monkeypatch: Any) -> None:
    _scratch_log(tmp_path, monkeypatch)

    def _boom(**kwargs: Any) -> bool:
        raise RuntimeError("disk on fire")

    monkeypatch.setattr(osiris_hook, "_cd_record", _boom)
    transcript = _transcript(tmp_path, 100_000)
    osiris_hook._note_context_disagreement(
        {}, 14, 1_000_000, "claude-sonnet-5", "sess-9", str(transcript))  # must not raise
    osiris_hook._note_context_disagreement(
        {}, 14, 1_000_000, "claude-sonnet-5", "sess-9", str(tmp_path / "no-such-file"))


def test_the_statusline_renders_first_and_still_exits_zero_when_logging_fails(
    tmp_path: Path, monkeypatch: Any,
) -> None:
    """The comparison runs after the line is printed: even a logger that raises cannot
    cost the render or change the exit code."""
    _scratch_log(tmp_path, monkeypatch)
    monkeypatch.setattr(osiris_hook, "_statusline_cache_path",
                        lambda project: tmp_path / "cache" / f"{project}.json")
    monkeypatch.setattr(osiris_hook, "_post", lambda url, data, timeout=3: None)

    def _boom(**kwargs: Any) -> bool:
        raise RuntimeError("disk on fire")

    monkeypatch.setattr(osiris_hook, "_cd_record", _boom)
    out: list[str] = []
    monkeypatch.setattr("builtins.print", lambda s="": out.append(s))
    transcript = _transcript(tmp_path, 100_000)
    rc = osiris_hook._cmd_statusline({
        "workspace": {"current_dir": "/repo"}, "model": {"id": "claude-sonnet-5"},
        "session_id": "sess-9", "transcript_path": str(transcript),
        "context_window": {"used_percentage": 14, "context_window_size": 1_000_000}})
    assert rc == 0
    assert any("ctx 14%" in line for line in out)  # the harness's figure is what renders


# --- the read door: osiris context-pct-log -------------------------------------------------

def test_the_read_door_says_so_when_nothing_is_recorded(
    tmp_path: Path, monkeypatch: Any, capsys: Any,
) -> None:
    from src.cli import main

    monkeypatch.setenv("HOME", str(tmp_path))
    assert main(["context-pct-log"]) == 0
    out = capsys.readouterr().out
    assert "nothing recorded" in out and str(tmp_path) in out


def test_the_read_door_lists_entries_newest_first_with_a_summary(
    tmp_path: Path, monkeypatch: Any, capsys: Any,
) -> None:
    from src.cli import main

    monkeypatch.setenv("HOME", str(tmp_path))
    log = cd.log_path()
    _record(log, 60, 50, session_id="older-session", now=1_700_000_000.0)
    _record(log, 40, 47, session_id="newer-session", now=1_700_000_100.0)
    assert main(["context-pct-log", "--limit", "5"]) == 0
    out = capsys.readouterr().out
    assert "2 recorded, largest gap +10 points" in out
    assert out.index("newer-se") < out.index("older-se")  # newest first
    assert "harness 40%  osiris 47%" in out


def test_the_read_door_json_is_one_line_with_the_log_path(
    tmp_path: Path, monkeypatch: Any, capsys: Any,
) -> None:
    from src.cli import main

    monkeypatch.setenv("HOME", str(tmp_path))
    _record(cd.log_path(), 60, 50)
    assert main(["context-pct-log", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["summary"]["entries"] == 1
    assert payload["entries"][0]["harness_pct"] == 60
    assert payload["log"].startswith(str(tmp_path))
