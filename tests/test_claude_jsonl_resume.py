"""Resuming a transcript read from a saved byte offset: same rows as a full read at every
cut, and a safe fall back to the scan when the file is not what the offset was saved for."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from src.ingest.harness import SessionLocator, TurnRow, claude_jsonl
from src.ingest.harness.claude_jsonl import ClaudeJsonlAdapter


def _line(kind: str, n: int, model: str | None = None, text: str | None = None) -> str:
    entry: dict[str, Any] = {"type": kind, "timestamp": f"2026-10-01T00:00:{n % 60:02d}Z",
                             "message": {"content": text or f"turn {n}"}}
    if model:
        entry["message"]["model"] = model
        entry["message"]["usage"] = {"input_tokens": n, "output_tokens": 1}
    return json.dumps(entry)


LINES = [
    _line("user", 0), _line("assistant", 1, "claude-fable-5"),
    json.dumps({"type": "system", "subtype": "hook"}),
    "[1, 2, 3]", '{"type": "assistant", "message": {"mod', "",
    _line("user", 2), _line("assistant", 3, "claude-opus-4-8"),
    json.dumps({"type": "file-history-snapshot"}),
    _line("user", 4), _line("assistant", 5, "claude-opus-4-8"), _line("user", 6),
]


def _loc(path: Path) -> SessionLocator:
    return SessionLocator(anchor_sid="abcd1234", session_id=path.stem, harness="claude-code",
                          source_path=str(path), cwd=None, project=None)


def _file(tmp_path: Path, lines: list[str] = LINES) -> Path:
    path = tmp_path / "abcd1234-0000-4000-8000-000000000000.jsonl"
    path.write_text("\n".join(lines) + "\n")
    return path


def _resume_of(row: TurnRow) -> tuple[int, int]:
    assert row.byte_offset is not None and row.line_crc is not None
    return row.byte_offset, row.line_crc


def test_every_row_carries_the_place_of_its_own_line(tmp_path: Path) -> None:
    path = _file(tmp_path)
    raw = path.read_bytes()
    for row in ClaudeJsonlAdapter().read_turns(_loc(path)):
        assert row.byte_offset is not None
        assert raw[row.byte_offset:].startswith(b"{")
        assert raw[row.byte_offset - 1:row.byte_offset] in (b"", b"\n")


def test_resuming_from_the_saved_place_equals_the_tail_of_a_full_read(tmp_path: Path) -> None:
    path = _file(tmp_path)
    a = ClaudeJsonlAdapter()
    full = list(a.read_turns(_loc(path)))
    for row in full[1:]:
        tail = list(a.read_turns(_loc(path), since_idx=row.turn_idx, resume=_resume_of(row)))
        assert tail == [t for t in full if t.turn_idx >= row.turn_idx], row.turn_idx


def test_a_seek_parses_only_the_new_lines(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _file(tmp_path)
    a = ClaudeJsonlAdapter()
    full = list(a.read_turns(_loc(path)))
    saved = full[-2]
    parsed: list[str] = []
    real = claude_jsonl.json.loads

    def _spy(s: Any, *x: Any, **kw: Any) -> Any:
        parsed.append(str(s)[:30])
        return real(s, *x, **kw)

    monkeypatch.setattr(claude_jsonl.json, "loads", _spy)
    tail = list(a.read_turns(_loc(path), since_idx=saved.turn_idx, resume=_resume_of(saved)))
    assert [t.turn_idx for t in tail] == [t.turn_idx for t in full[-2:]]
    assert len(parsed) == 2          # the saved line and the one after it, nothing before


def test_lines_appended_after_the_saved_place_are_read_with_the_right_indexes(
    tmp_path: Path,
) -> None:
    path = _file(tmp_path)
    a = ClaudeJsonlAdapter()
    saved = list(a.read_turns(_loc(path)))[-1]
    with path.open("a") as f:
        f.write(json.dumps({"type": "system"}) + "\n" + _line("assistant", 7, "claude-opus-4-8")
                + "\n" + _line("user", 8) + "\n")
    tail = list(a.read_turns(_loc(path), since_idx=saved.turn_idx, resume=_resume_of(saved)))
    scan = [t for t in a.read_turns(_loc(path)) if t.turn_idx >= saved.turn_idx]
    assert tail == scan and len(tail) == 3


def test_a_truncated_file_falls_back_to_the_scan(tmp_path: Path) -> None:
    path = _file(tmp_path)
    a = ClaudeJsonlAdapter()
    saved = list(a.read_turns(_loc(path)))[-1]
    path.write_text("\n".join(LINES[:3]) + "\n")          # shorter than the saved offset
    assert (list(a.read_turns(_loc(path), since_idx=saved.turn_idx, resume=_resume_of(saved)))
            == [t for t in a.read_turns(_loc(path)) if t.turn_idx >= saved.turn_idx])


def test_a_rewritten_file_or_a_mid_line_offset_falls_back_to_the_scan(tmp_path: Path) -> None:
    path = _file(tmp_path)
    a = ClaudeJsonlAdapter()
    saved = list(a.read_turns(_loc(path)))[-2]
    off, crc = _resume_of(saved)
    scan = [t for t in a.read_turns(_loc(path)) if t.turn_idx >= saved.turn_idx]
    # an offset that lands inside a line
    assert list(a.read_turns(_loc(path), since_idx=saved.turn_idx, resume=(off + 3, crc))) == scan
    # a wrong checksum: the line there is not the one that was saved
    assert list(a.read_turns(_loc(path), since_idx=saved.turn_idx, resume=(off, crc ^ 1))) == scan
    # the same offset, but the bytes before it were rewritten (a different line sits there)
    rewritten = [ln if "turn 4" not in ln else _line("user", 4, text="a different line!")
                 for ln in LINES]
    path.write_text("\n".join(rewritten) + "\n")
    assert (list(a.read_turns(_loc(path), since_idx=saved.turn_idx, resume=(off, crc)))
            == [t for t in a.read_turns(_loc(path)) if t.turn_idx >= saved.turn_idx])


def test_a_model_command_before_the_saved_place_keeps_its_flag_in_the_store_not_the_tail(
    tmp_path: Path,
) -> None:
    """On a seek the deliberate flag is judged on the part being read; the turns already
    stored kept theirs, and the session reading ORs them. (A full scan sets it on all.)"""
    cmd = _line("user", 0, text="<command-name>/model</command-name>")
    path = _file(tmp_path, [cmd, _line("assistant", 1, "claude-fable-5"),
                            _line("user", 2), _line("assistant", 3, "claude-opus-4-8")])
    a = ClaudeJsonlAdapter()
    full = list(a.read_turns(_loc(path)))
    assert all(t.swap_deliberate for t in full if t.role == "assistant")
    saved = full[2]
    tail = list(a.read_turns(_loc(path), since_idx=saved.turn_idx, resume=_resume_of(saved)))
    assert [t.swap_deliberate for t in tail if t.role == "assistant"] == [False]
