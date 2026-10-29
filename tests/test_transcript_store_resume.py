"""The store saves where each ingest left off in the source file, and the next ingest of a
grown transcript seeks there instead of scanning, with the same rows as a one-shot ingest."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.actions.core import Actions
from src.ingest.harness import SessionLocator
from src.ingest.harness.claude_jsonl import ClaudeJsonlAdapter
from src.ingest.transcript_store import TranscriptStore


def _line(kind: str, n: int, model: str | None = None) -> str:
    entry: dict[str, Any] = {"type": kind, "timestamp": f"2026-10-01T00:00:{n % 60:02d}Z",
                             "message": {"content": f"turn {n}"}}
    if model:
        entry["message"]["model"] = model
        entry["message"]["usage"] = {"input_tokens": n + 1, "output_tokens": 1}
    return json.dumps(entry)


def _loc(path: Path, anchor: str) -> SessionLocator:
    return SessionLocator(anchor_sid=anchor, session_id=path.stem, harness="claude-code",
                          source_path=str(path), cwd=None, project=None)


def _write(path: Path, count: int) -> None:
    lines = []
    for i in range(count):
        lines.append(_line("user", i))
        lines.append(json.dumps({"type": "system", "n": i}))
        lines.append(_line("assistant", i, "claude-opus-4-8"))
    path.write_text("\n".join(lines) + "\n")


async def _turns(actions: Actions, anchor: str) -> list[tuple[Any, ...]]:
    rows = await actions.pool.fetch(
        "SELECT turn_idx, role, model, tokens_in, recorded_at, swap_deliberate "
        "FROM harness_turns WHERE harness='claude-code' AND anchor_sid=$1 ORDER BY turn_idx",
        anchor)
    return [tuple(r.values()) for r in rows]


async def test_each_ingest_saves_the_place_of_its_last_turn(
    actions: Actions, tmp_path: Path,
) -> None:
    path = tmp_path / "aaaa1111-0000-4000-8000-000000000000.jsonl"
    _write(path, 5)
    store = TranscriptStore(actions.pool)
    await store._ingest_locator(ClaudeJsonlAdapter(), _loc(path, "aaaa1111"))
    row = await actions.pool.fetchrow(
        "SELECT last_turn_idx, last_byte_offset, last_line_crc, last_offset_idx "
        "FROM harness_sessions WHERE anchor_sid='aaaa1111'")
    assert row["last_offset_idx"] == row["last_turn_idx"]
    raw = path.read_bytes()
    assert raw[row["last_byte_offset"]:].startswith(b'{"type": "assistant"')
    assert raw[row["last_byte_offset"]:].count(b"\n") == 1      # it is the final line


async def test_a_grown_transcript_is_ingested_by_seeking_with_the_same_rows(
    actions: Actions, tmp_path: Path, monkeypatch: Any,
) -> None:
    path = tmp_path / "bbbb2222-0000-4000-8000-000000000000.jsonl"
    twin = tmp_path / "cccc3333-0000-4000-8000-000000000000.jsonl"
    _write(path, 20)
    store = TranscriptStore(actions.pool)
    adapter = ClaudeJsonlAdapter()
    await store._ingest_locator(adapter, _loc(path, "bbbb2222"))

    with path.open("a") as f:
        for i in range(20, 26):
            f.write(_line("user", i) + "\n" + _line("assistant", i, "claude-fable-5") + "\n")
    # the same final file ingested in one go, as the reference
    twin.write_bytes(path.read_bytes())
    await store._ingest_locator(adapter, _loc(twin, "cccc3333"))

    seen: list[Any] = []
    real = ClaudeJsonlAdapter.read_turns

    def _spy(self: ClaudeJsonlAdapter, locator: SessionLocator, *, since_idx: int = 0,
             resume: Any = None) -> Any:
        seen.append((since_idx, resume))
        return real(self, locator, since_idx=since_idx, resume=resume)

    monkeypatch.setattr(ClaudeJsonlAdapter, "read_turns", _spy)
    reading = await store._ingest_locator(adapter, _loc(path, "bbbb2222"))
    assert seen and seen[0][1] is not None, "the second ingest should resume from a saved place"
    assert reading is not None and reading.current == "claude-fable-5"
    assert reading.history == ("claude-opus-4-8", "claude-fable-5")

    mine = [t[1:] for t in await _turns(actions, "bbbb2222")]
    ref = [t[1:] for t in await _turns(actions, "cccc3333")]
    assert mine == ref and len(mine) == 2 * 26


async def test_a_rewritten_transcript_is_still_ingested_without_trusting_the_old_place(
    actions: Actions, tmp_path: Path,
) -> None:
    path = tmp_path / "dddd4444-0000-4000-8000-000000000000.jsonl"
    _write(path, 10)
    store = TranscriptStore(actions.pool)
    adapter = ClaudeJsonlAdapter()
    await store._ingest_locator(adapter, _loc(path, "dddd4444"))
    before = await actions.pool.fetchval(
        "SELECT last_byte_offset FROM harness_sessions WHERE anchor_sid='dddd4444'")
    # replaced by different, longer content: the saved offset now sits mid-line
    lines = [_line("user", i) + "  " for i in range(0, 80)]
    path.write_text("\n".join(lines) + "\n")
    await store._ingest_locator(adapter, _loc(path, "dddd4444"))
    after = await actions.pool.fetchrow(
        "SELECT last_turn_idx, last_byte_offset, last_offset_idx FROM harness_sessions "
        "WHERE anchor_sid='dddd4444'")
    assert after["last_offset_idx"] == after["last_turn_idx"]
    assert after["last_byte_offset"] >= before          # advanced to the new final line
    assert path.read_bytes()[after["last_byte_offset"]:].count(b"\n") == 1
