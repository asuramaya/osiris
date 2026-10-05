"""THE DISK AND WAL BRAKE: every job that rewrites many rows checks this before each slice.

Why it exists: bulk rewrites (the soul-line re-encode, the dictionary pass, the fact-history
fold, the cold-tier moves, the retention deletes) each write the new rows AND log them to the
write-ahead log, and the log is archived raw. Together they wrote about 40 GB an hour for seven
hours, the archive grew from 71 GB to 260 GB, the root disk reached zero free and Postgres
crash-looped. A rewrite is a background convenience; the disk is not.

The brake pauses a job (it logs one line and does nothing this tick, never an error) when
  - the free space on the root filesystem is below max(100 GB, 10% of the disk), or
  - more than a budget (default 8 GB) of WAL was written in the last hour.
It resumes by itself the next tick the condition clears: free space returns when the archive
is pruned, and the hourly WAL figure falls as the burst ages out of the window.

The WAL figure is the growth of Postgres's own write position. A worker restart must not
forget it, so the position is sampled into the `watermarks` table (one JSON row) and compared
with the oldest sample inside the last hour.

Both limits can be tuned without a code change through the environment:
OSIRIS_DISK_FREE_FLOOR_GB (default 100) and OSIRIS_WAL_BUDGET_GB_PER_HOUR (default 8)."""
from __future__ import annotations

import json
import logging
import os
import shutil
import time
from typing import Any

import asyncpg

_log = logging.getLogger("osiris.disk_brake")
_last_logged: dict[str, float] = {}

GB = 1024 ** 3
DEFAULT_FREE_FLOOR_GB = 100.0
DEFAULT_FREE_FLOOR_FRACTION = 0.10
DEFAULT_WAL_BUDGET_GB = 8.0
WINDOW_SECS = 3600
SAMPLE_EVERY_SECS = 60
SAMPLES_KEY = "brake:wal_samples"
DISK_PATH = "/"


def _float_env(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return default


def free_floor_bytes(total_bytes: int) -> int:
    """max(the configured floor, 10% of the disk)."""
    floor_gb = _float_env("OSIRIS_DISK_FREE_FLOOR_GB", DEFAULT_FREE_FLOOR_GB)
    return int(max(floor_gb * GB, DEFAULT_FREE_FLOOR_FRACTION * total_bytes))


def wal_budget_bytes() -> int:
    return int(_float_env("OSIRIS_WAL_BUDGET_GB_PER_HOUR", DEFAULT_WAL_BUDGET_GB) * GB)


def disk_usage() -> tuple[int, int]:
    """(free, total) bytes of the root filesystem."""
    usage = shutil.disk_usage(DISK_PATH)
    return usage.free, usage.total


async def _wal_position(pool: asyncpg.Pool) -> int | None:
    """Bytes of WAL ever written (the current write position); None on a standby or when the
    server cannot say."""
    try:
        value = await pool.fetchval("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')")
    except Exception:  # noqa: BLE001 - a missing figure never stops a job on its own
        return None
    return int(value) if value is not None else None


async def wal_written_last_hour(pool: asyncpg.Pool, *, now: float | None = None) -> int | None:
    """WAL bytes written since the oldest sample inside the last hour (so, over the available
    span until an hour of samples exists), recording a fresh sample at most once a minute.
    None when there is no position or no earlier sample to compare with."""
    from src.orchestrator.monitor import get_cursor, set_cursor

    position = await _wal_position(pool)
    if position is None:
        return None
    now = time.time() if now is None else now
    try:
        raw = await get_cursor(pool, SAMPLES_KEY)
        samples: list[list[float]] = json.loads(raw) if raw else []
    except Exception:  # noqa: BLE001
        samples = []
    samples = [s for s in samples if now - s[0] <= WINDOW_SECS * 2 and s[1] <= position]
    window = [s for s in samples if now - s[0] <= WINDOW_SECS]
    written: int | None = None
    if window:
        written = max(0, position - int(window[0][1]))
    if not samples or now - samples[-1][0] >= SAMPLE_EVERY_SECS:
        samples.append([now, position])
        try:
            await set_cursor(pool, SAMPLES_KEY, json.dumps(samples))
        except Exception:  # noqa: BLE001
            pass
    return written


async def brake_state(pool: asyncpg.Pool, *, now: float | None = None) -> dict[str, Any]:
    """The brake's current reading, for a job to act on and a status view to show."""
    free, total = disk_usage()
    floor = free_floor_bytes(total)
    budget = wal_budget_bytes()
    wal = await wal_written_last_hour(pool, now=now)
    reasons: list[str] = []
    if free < floor:
        reasons.append(f"root disk free {free / GB:.0f} GB is below the {floor / GB:.0f} GB floor")
    if wal is not None and wal > budget:
        reasons.append(f"{wal / GB:.1f} GB of WAL written in the last hour is over the "
                       f"{budget / GB:.0f} GB budget")
    return {
        "paused": bool(reasons),
        "reason": "; ".join(reasons) if reasons else None,
        "label": "paused: disk/WAL budget" if reasons else "running",
        "free_gb": round(free / GB, 1), "floor_gb": round(floor / GB, 1),
        "wal_gb_last_hour": None if wal is None else round(wal / GB, 2),
        "wal_budget_gb": round(budget / GB, 1),
    }


async def pause_reason(pool: asyncpg.Pool, job: str) -> str | None:
    """None when `job` may rewrite rows now; otherwise the reason it must wait. The wait is
    logged as an info line at most once a minute per job: waiting is the brake working, not
    a fault. If the brake itself cannot read the disk it pauses the job: a rewrite must not
    start blind."""
    try:
        state = await brake_state(pool)
    except Exception as exc:  # noqa: BLE001
        reason = f"the disk/WAL brake could not read its state ({exc!r})"
    else:
        if not state["paused"]:
            return None
        reason = str(state["reason"])
    now = time.monotonic()
    if now - _last_logged.get(job, -1e9) >= 60:
        _last_logged[job] = now
        _log.info("%s paused by the disk/WAL brake: %s", job, reason)
    return reason
