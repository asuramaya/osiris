"""MEMORY PROFILING SEAM (thread e6fd3772 piece 2) — repeatable, without restarting a LIVE
daemon. Deliberately opt-in and inert by default: `maybe_start()` is a no-op unless
OSIRIS_PROFILE_MEMORY is set truthy, so importing this module costs nothing on the two real
production daemons (osiris-mcp, osiris-worker) unless a SECOND, scratch instance is started
with the env var set — the profiling method Thoth's dispatch named as the safe one (a
`--profile`-shaped flag on a scratch-port instance sharing the same DSN, never the live
process).

SIGUSR1, not an HTTP route: the arq worker has no request/response cycle to hang a route off
of, and a signal handler is the one dump mechanism that works identically for both daemons
without adding a second, divergent implementation. `kill -USR1 <scratch-pid>` writes a JSON
snapshot to OSIRIS_PROFILE_DUMP_PATH (default /tmp/osiris-memtop-<pid>.json) and returns —
never blocks, never touches the live daemon's own pid.
"""
from __future__ import annotations

import json
import os
import signal
import tracemalloc
from pathlib import Path


def _enabled() -> bool:
    return os.environ.get("OSIRIS_PROFILE_MEMORY", "").strip() not in ("", "0", "false", "False")


def _dump_path() -> Path:
    configured = os.environ.get("OSIRIS_PROFILE_DUMP_PATH")
    if configured:
        return Path(configured)
    return Path(f"/tmp/osiris-memtop-{os.getpid()}.json")  # noqa: S108 — opt-in debug artifact only


def _top_n(n: int = 25) -> list[dict[str, object]]:
    """Top-N allocation SITES by current retained size, grouped by (file, lineno) —
    tracemalloc's own 'lineno' grouping, the finest-grained one that still fits a short
    report. `n=25` (wider than the 5 the deliverable asks for) so a caller can re-group by
    top-level module/package afterward without re-running the snapshot."""
    snapshot = tracemalloc.take_snapshot()
    stats = snapshot.statistics("lineno")
    return [
        {"file": stat.traceback[0].filename, "line": stat.traceback[0].lineno,
         "size_kb": round(stat.size / 1024, 1), "count": stat.count}
        for stat in stats[:n]
    ]


def _dump(_signum: int, _frame: object) -> None:
    try:
        payload = {"pid": os.getpid(), "top": _top_n()}
        _dump_path().write_text(json.dumps(payload, indent=2))
    except Exception:  # noqa: BLE001 — a signal handler must never raise past the runtime
        pass


def maybe_start() -> None:
    """Call once, at import time, from BOTH daemon entrypoints. No-op unless
    OSIRIS_PROFILE_MEMORY is set — the two real production processes never pay for this."""
    if not _enabled():
        return
    if not tracemalloc.is_tracing():
        tracemalloc.start(10)  # 10 frames of traceback per allocation — enough to name a caller
    signal.signal(signal.SIGUSR1, _dump)
