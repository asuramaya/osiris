"""An incremental read returns exactly the tail of a full read, without parsing the prefix."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from src.ingest.harness import SessionLocator, claude_jsonl
from src.ingest.harness.claude_jsonl import ClaudeJsonlAdapter


def _line(kind: str, n: int, model: str | None = None) -> str:
    entry: dict[str, Any] = {"type": kind, "timestamp": f"2026-10-01T00:00:{n % 60:02d}Z",
                             "message": {"content": f"turn {n}"}}
    if model:
        entry["message"]["model"] = model
        entry["message"]["usage"] = {"input_tokens": n, "output_tokens": 1}
    return json.dumps(entry)


def _write(tmp_path: Path) -> SessionLocator:
    lines = [
        _line("user", 0), _line("assistant", 1, "claude-fable-5"),
        json.dumps({"type": "system", "subtype": "hook"}),        # a dict line, not a turn
        "[1, 2, 3]",                                              # valid JSON, not a dict
        '{"type": "assistant", "message": {"mod',                 # truncated: not a dict
        "",                                                       # blank
        _line("user", 2), _line("assistant", 3, "claude-opus-4-8"),
        json.dumps({"type": "file-history-snapshot"}),
        _line("user", 4), _line("assistant", 5, "claude-opus-4-8"),
        _line("user", 6),
    ]
    path = tmp_path / "abcd1234-0000-4000-8000-000000000000.jsonl"
    path.write_text("\n".join(lines) + "\n")
    return SessionLocator(anchor_sid="abcd1234", session_id=path.stem, harness="claude-code",
                          source_path=str(path), cwd=None, project=None)


def test_every_incremental_read_is_the_tail_of_the_full_read(tmp_path: Path) -> None:
    loc = _write(tmp_path)
    adapter = ClaudeJsonlAdapter()
    full = list(adapter.read_turns(loc, since_idx=0))
    assert [t.role for t in full] == ["user", "assistant"] * 3 + ["user"]
    for since in range(0, 12):
        tail = list(adapter.read_turns(loc, since_idx=since))
        assert tail == [t for t in full if t.turn_idx >= since], since


def test_the_already_ingested_prefix_is_never_parsed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _write(tmp_path)
    adapter = ClaudeJsonlAdapter()
    full = list(adapter.read_turns(loc, since_idx=0))
    cut = full[-2].turn_idx                       # only the last two turns are new
    parsed: list[str] = []
    real = claude_jsonl.json.loads

    def _spy(s: Any, *a: Any, **kw: Any) -> Any:
        parsed.append(str(s)[:40])
        return real(s, *a, **kw)

    monkeypatch.setattr(claude_jsonl.json, "loads", _spy)
    tail = list(adapter.read_turns(loc, since_idx=cut))
    assert [t.turn_idx for t in tail] == [t.turn_idx for t in full[-2:]]
    # the model-command scan (substring filtered) parses nothing here, and each new line is
    # parsed once: two new lines, two parses, none of the nine before them
    assert len(parsed) == 2, parsed
