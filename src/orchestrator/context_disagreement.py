"""A record of the moments osiris's own context figure and the harness's disagree.

The harness computes its own `used_percentage` and hands it to hooks and the status line
only; osiris derives its figure from the transcript (see context_lens). The two have never
been compared on paired readings. This keeps the evidence: the status line hook, which holds
both numbers on every render, appends one JSON line here when they differ by MORE than one
point, and nothing at all when they agree, so a healthy install writes nothing.

Stdlib only, like context_lens: the status line hook imports it and must stay cheap and
fail-open. The log is small and capped, an identical repeat of the last line is dropped, and
every write error is swallowed: a diagnostic must never cost a render or block a session.
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

DEFAULT_LOG = "~/.local/state/osiris/context_pct_disagreements.jsonl"
# log only when the two figures differ by MORE than this many points: one point is what
# two correct roundings of the same ratio can legitimately differ by
THRESHOLD_POINTS = 1
CAP_LINES = 200
_TAIL_BYTES = 4096
# trimming is checked by size so the common write is one append and one stat
_TRIM_BYTES = CAP_LINES * 320


def log_path(path: str | os.PathLike[str] | None = None) -> Path:
    return Path(path or DEFAULT_LOG).expanduser()


def _last_entry(p: Path) -> dict[str, Any] | None:
    try:
        size = p.stat().st_size
        with p.open("rb") as fh:
            fh.seek(max(0, size - _TAIL_BYTES))
            tail = fh.read().decode("utf-8", errors="replace")
    except OSError:
        return None
    for line in reversed(tail.splitlines()):
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(entry, dict):
            return entry
    return None


def _trim(p: Path) -> None:
    try:
        if p.stat().st_size <= _TRIM_BYTES:
            return
        lines = p.read_text().splitlines()
        if len(lines) <= CAP_LINES:
            return
        tmp = p.with_name(p.name + ".tmp")
        tmp.write_text("\n".join(lines[-CAP_LINES:]) + "\n")
        os.replace(tmp, p)
    except OSError:
        return


def record_if_different(
    *, harness_pct: int | None, derived_pct: int | None, window: int | None,
    model: str | None, session_id: str,
    path: str | os.PathLike[str] | None = None, now: float | None = None,
) -> bool:
    """Append one line when the figures differ by more than THRESHOLD_POINTS. Returns True
    only when a line was written. Writes nothing when either figure is unknown, when they
    agree, or when the last line already says the same thing for this session."""
    if harness_pct is None or derived_pct is None:
        return False
    if abs(harness_pct - derived_pct) <= THRESHOLD_POINTS:
        return False
    p = log_path(path)
    entry = {
        "ts": round(time.time() if now is None else now, 3),
        "session_id": session_id, "harness_pct": harness_pct, "derived_pct": derived_pct,
        "window": window, "model": model,
    }
    try:
        last = _last_entry(p)
        if last is not None and all(
                last.get(k) == entry[k] for k in ("session_id", "harness_pct", "derived_pct")):
            return False
        p.parent.mkdir(parents=True, exist_ok=True)
        line = (json.dumps(entry, separators=(",", ":")) + "\n").encode()
        fd = os.open(p, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
        try:
            os.write(fd, line)
        finally:
            os.close(fd)
        _trim(p)
        return True
    except OSError:
        return False


def read_recent(
    limit: int = 20, path: str | os.PathLike[str] | None = None,
) -> list[dict[str, Any]]:
    """The most recent entries, newest first. Unreadable lines are skipped."""
    p = log_path(path)
    try:
        lines = p.read_text().splitlines()
    except OSError:
        return []
    out: list[dict[str, Any]] = []
    for line in reversed(lines):
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(entry, dict):
            out.append(entry)
        if len(out) >= max(1, limit):
            break
    return out


def summarize(entries: list[dict[str, Any]]) -> dict[str, Any]:
    """How the harness and osiris differ across `entries`: how many, the largest gap, and
    which side read higher how often (positive gap = the harness read higher)."""
    gaps = [int(e["harness_pct"]) - int(e["derived_pct"]) for e in entries
            if isinstance(e.get("harness_pct"), int) and isinstance(e.get("derived_pct"), int)]
    return {
        "entries": len(gaps),
        "largest_gap": max(gaps, key=abs) if gaps else None,
        "harness_higher": sum(1 for g in gaps if g > 0),
        "osiris_higher": sum(1 for g in gaps if g < 0),
    }
