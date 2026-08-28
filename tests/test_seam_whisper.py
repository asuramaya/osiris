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


# --- ONCE PER CROSSING, NOT ONCE PER CALL (thread e2326ab7, Soundwave XIV's decepticons
# report): `_seam_note` alone fires on EVERY call inside a band — `_seam_note_once` debounces
# it to fire only when the band actually changes, the same once-per-crossing discipline the
# offload ritual's own marker files already have. ------------------------------------------

def test_seam_band_tiers_and_the_floor() -> None:
    assert srv._seam_band(None, 70, ALARM_PCT) is None
    assert srv._seam_band(65, 70, ALARM_PCT) is None       # below the whisper floor
    assert srv._seam_band(90, 0, ALARM_PCT) is None        # 0 disables the whisper entirely
    assert srv._seam_band(72, 70, ALARM_PCT) == "seam"
    assert srv._seam_band(ALARM_PCT, 70, ALARM_PCT) == "alarm"


def test_seam_note_once_fires_on_first_entry_then_falls_silent() -> None:
    srv._seam_last_band.clear()
    first = srv._seam_note_once("agent:wallpaper1", 65, 63)
    assert first is not None and "seam soon" in first
    # ~40 consecutive calls at an unchanged pct: the exact wallpaper Thoth's dispatch named
    for _ in range(40):
        assert srv._seam_note_once("agent:wallpaper1", 65, 63) is None
    # a DIFFERENT pct still inside the SAME band is still wallpaper, not news
    assert srv._seam_note_once("agent:wallpaper1", 71, 63) is None


def test_seam_note_once_re_fires_on_a_real_escalation() -> None:
    srv._seam_last_band.clear()
    srv._seam_note_once("agent:escalate1", 65, 63)
    alarm = srv._seam_note_once("agent:escalate1", ALARM_PCT, 63)
    assert alarm is not None and "WRITE BACK NOW" in alarm
    # the alarm tier is ALSO once-per-crossing, not once-per-call
    assert srv._seam_note_once("agent:escalate1", ALARM_PCT + 5, 63) is None


def test_seam_note_once_re_arms_after_dropping_below_the_floor() -> None:
    srv._seam_last_band.clear()
    srv._seam_note_once("agent:rearm1", 65, 63)
    assert srv._seam_note_once("agent:rearm1", 65, 63) is None
    # a real write-back/compaction happened — pct falls back under the whisper floor
    assert srv._seam_note_once("agent:rearm1", 10, 63) is None
    # climbing back into the seam band is real news again, not a repeat
    again = srv._seam_note_once("agent:rearm1", 65, 63)
    assert again is not None and "seam soon" in again


def test_seam_note_once_tracks_each_agent_independently() -> None:
    srv._seam_last_band.clear()
    srv._seam_note_once("agent:solo1", 65, 63)
    # a DIFFERENT agent's first crossing is real news, unaffected by agent:solo1's own state
    fresh = srv._seam_note_once("agent:solo2", 65, 63)
    assert fresh is not None
