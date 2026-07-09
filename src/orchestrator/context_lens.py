"""The context lens — a mind's view of its own mortality (operator request, 2026-07-09).

Under the mind ruling (a882b334) a compaction is a death, and the harness gives an agent no
native sense of how close it is: /context is the OPERATOR's tool, invisible from inside the
loop. This reads the harness's own record — the last main-loop usage block in the session
transcript — and answers two askers:

  * the CHROME (statusline): one cheap glance, `ctx 62%`, tail-read only, per render;
  * the AGENT (context_window MCP tool): the full detail — occupancy breakdown, window tier,
    headroom, how many deaths this session has already had — so a mind can decide to write
    back BEFORE the seam instead of trusting the summary to carry it.

Window tiers come from the harness's display id: a bracketed variant (claude-opus-4-8[1m])
is the 1M-context tier of the same weights; the bare id gets the 200k default. The bracket
never reaches identity logic (normalize_model strips it) — here it is exactly the signal.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

WINDOW_DEFAULT = 200_000
WINDOW_1M = 1_000_000
# occupancy above this is the write-back alarm: compaction can land any turn
ALARM_PCT = 80


def window_for(raw_model: str | None, used: int | None = None) -> tuple[int, bool]:
    """(window tokens, assumed) — the tier of this tab's context window. Signals, strongest
    first: the OSIRIS_CONTEXT_WINDOW env override (the operator's word); a `[1m]` display id
    (the harness marks the 1M tier); the SELF-CORRECTION — an occupancy already past 200k on
    a live session proves the 200k default wrong (fable tabs report a bare id but run 1M on
    this box); else the 200k default, flagged assumed. Erring low is safe: the alarm fires
    early, never late."""
    import os
    env = os.environ.get("OSIRIS_CONTEXT_WINDOW")
    if env and env.isdigit():
        return int(env), False
    if raw_model and "[1m]" in raw_model:
        return WINDOW_1M, False
    if used is not None and used > WINDOW_DEFAULT:
        return WINDOW_1M, True
    return WINDOW_DEFAULT, True


def _usage_of(entry: dict[str, Any]) -> dict[str, int] | None:
    """The context-occupancy numbers of one main-loop assistant entry, or None."""
    if entry.get("type") != "assistant" or entry.get("isSidechain"):
        return None
    u = (entry.get("message") or {}).get("usage")
    if not isinstance(u, dict) or "input_tokens" not in u:
        return None
    return {
        "input": int(u.get("input_tokens") or 0),
        "cache_read": int(u.get("cache_read_input_tokens") or 0),
        "cache_creation": int(u.get("cache_creation_input_tokens") or 0),
        "output_last_turn": int(u.get("output_tokens") or 0),
    }


def last_usage(path: Path, *, tail_bytes: int = 262_144) -> dict[str, int] | None:
    """The MOST RECENT main-loop usage block, read from the transcript's tail only — the
    chrome calls this per render, so it must never scan an 84MB file. None when the tail
    holds no usage (a brand-new session, or a tail full of tool results)."""
    try:
        size = path.stat().st_size
        with path.open("rb") as fh:
            fh.seek(max(0, size - tail_bytes))
            tail = fh.read().decode("utf-8", errors="replace")
    except OSError:
        return None
    for line in reversed(tail.splitlines()):
        if '"usage"' not in line:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:  # the seek landed mid-line — expected for the first line
            continue
        u = _usage_of(entry)
        if u is not None:
            return u
    return None


def occupancy(usage: dict[str, int]) -> int:
    """Context tokens occupied at that turn: everything the model was fed (fresh + cached).
    The turn's own output joins the NEXT turn's input, so it is reported but not summed."""
    return usage["input"] + usage["cache_read"] + usage["cache_creation"]


def glance(path: Path, raw_model: str | None) -> dict[str, Any] | None:
    """The chrome's one-liner: {used, window, pct, assumed}. None when unreadable — the
    statusline omits the segment rather than lying."""
    u = last_usage(path)
    if u is None:
        return None
    used = occupancy(u)
    window, assumed = window_for(raw_model, used)
    return {"used": used, "window": window, "pct": round(100 * used / window),
            "assumed": assumed}


def detail(path: Path, raw_model: str | None) -> dict[str, Any]:
    """The agent's full self-knowledge: occupancy breakdown, window tier, headroom, and this
    session's death toll (compact boundaries — each one was a mind, ruling a882b334). One
    full scan; an agent asks rarely, so the cost is honest."""
    compactions = 0
    last_compaction_at: str | None = None
    last: dict[str, int] | None = None
    turns = 0
    try:
        with path.open() as fh:
            for line in fh:
                if '"compact_boundary"' in line:
                    try:
                        e = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if e.get("subtype") == "compact_boundary":
                        compactions += 1
                        last_compaction_at = e.get("timestamp") or last_compaction_at
                    continue
                if '"usage"' not in line:
                    continue
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    continue
                u = _usage_of(e)
                if u is not None:
                    last, turns = u, turns + 1
    except OSError:
        return {"error": "transcript unreadable — no self-knowledge without the record"}
    if last is None:
        return {"error": "no usage recorded yet — too young to measure"}
    used = occupancy(last)
    window, assumed = window_for(raw_model, used)
    pct = round(100 * used / window)
    out: dict[str, Any] = {
        "used": used, "window": window, "window_assumed": assumed, "pct": pct,
        "remaining": max(0, window - used),
        "breakdown": last,
        "assistant_turns": turns,
        "compactions_this_session": compactions,
        "last_compaction_at": last_compaction_at,
    }
    if pct >= ALARM_PCT:
        out["warning"] = (
            f"context {pct}% full — a compaction (a DEATH, ruling a882b334) can land any "
            "turn. Write back NOW: record_decision / resolve_thread anything still only in "
            "your head; what is not in the graph does not exist for your heir.")
    return out
