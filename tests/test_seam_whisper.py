"""THE AMBIENT SEAM WHISPER (decision d80621a7 piece 1) — the waist tells a mind how close
its next seam is, because the agent near the ceiling is exactly the agent not thinking to
ask. Witnesses: the tail-read % with the mtime cache, the known-window-only law (no alarm
on a guessed denominator), and the tier line (seam-soon vs WRITE BACK NOW at the house
ALARM_PCT — one authority)."""
from __future__ import annotations

import json
import os
from pathlib import Path

import src.mcp_server as srv
from src.orchestrator.context_lens import ALARM_PCT


def _transcript(tmp_path: Path, used: int) -> Path:
    t = tmp_path / "abc123.jsonl"
    t.write_text(json.dumps({
        "type": "assistant",
        "message": {"usage": {"input_tokens": used, "cache_read_input_tokens": 0,
                              "cache_creation_input_tokens": 0, "output_tokens": 5}},
    }) + "\n")
    return t


def test_seam_pct_reads_the_tail_and_caches_on_mtime(tmp_path: Path, monkeypatch) -> None:
    t = _transcript(tmp_path, used=150_000)
    monkeypatch.setattr(srv, "_seam_locate", lambda job: t)
    srv._seam_pcts.clear()
    assert srv._seam_pct_sync("job1", None, 200_000) == 75
    # same mtime → the cache answers even though the file changed underneath
    st = t.stat()
    _transcript(tmp_path, used=190_000)
    os.utime(t, (st.st_atime, st.st_mtime))
    assert srv._seam_pct_sync("job1", None, 200_000) == 75
    # a NEW mtime re-measures
    os.utime(t, (st.st_atime + 5, st.st_mtime + 5))
    assert srv._seam_pct_sync("job1", None, 200_000) == 95


def test_seam_pct_refuses_a_guessed_window(tmp_path: Path, monkeypatch) -> None:
    t = _transcript(tmp_path, used=150_000)
    monkeypatch.setattr(srv, "_seam_locate", lambda job: t)
    srv._seam_pcts.clear()
    # no window hint and an unknown model → the % would be a guess; the whisper stays silent
    assert srv._seam_pct_sync("job2", None, None) is None


def test_seam_note_tiers_and_silences() -> None:
    assert srv._seam_note(None, 70) is None
    assert srv._seam_note(65, 70) is None
    assert srv._seam_note(90, 0) is None            # 0 disables the whisper entirely
    soon = srv._seam_note(72, 70)
    assert soon is not None and "seam soon" in soon and "72%" in soon
    alarm = srv._seam_note(ALARM_PCT, 70)
    assert alarm is not None and "WRITE BACK NOW" in alarm
