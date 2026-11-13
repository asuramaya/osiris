"""Catching up a nightly worker job the box slept through.

The worker's daily jobs run from arq cron slots (02:15 housekeeping, 03:30 retention, 03:45 cold
tier fold, and the 03:00 compaction). arq has no catch-up: a slot the box was asleep for is lost.
A systemd timer with `Persistent=true` catches up when the box wakes; this gives three of those
jobs the same behaviour, under guards.

NOT COMPACTION, on purpose. A compaction rewrite is hours of heavy disk work (and needs free disk
for the copy and the disk/WAL brake to stay clear), and the operator's ruling is that it runs
overnight. pg_repack keeps its stall to tens of milliseconds, but the I/O and the daytime
interactive load are the cost, and the space it reclaims is not urgent: the next window is enough.
A missed compaction night is made visible (backup-status `missed_runs`), never made up by day.

The decision is pure (`decide`): what to run, or why not. The worker's heartbeat acts on it."""
from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from typing import Any

from src.db.session_lock import session_lock_connection
from src.orchestrator.missed_runs import GRACE, WORKER_DAILY_JOBS, Slot

# the jobs that may be made up, with the reason for each exclusion written beside it
EXCLUDED = {"db_compaction_heartbeat":
            "hours of heavy disk work, ruled overnight; the next window is enough"}
CATCHUP_JOBS: tuple[tuple[str, Slot], ...] = tuple(
    (name, slot) for name, _label, slot in WORKER_DAILY_JOBS if name not in EXCLUDED)

# A catch-up is not started within this long of the job's next real slot, nor during the night
# window when the real jobs (and the compaction rewrite) own the database.
NEXT_SLOT_MARGIN = timedelta(minutes=60)
NIGHT_START = (1, 15)
NIGHT_END = (5, 0)
# per-job time limit of a catch-up: the same as the job's own cron timeout
TIMEOUTS = {"storage_housekeeping_heartbeat": 1500.0, "retention_heartbeat": 600.0,
            "soul_cold_tier_heartbeat": 600.0}


def _in_night(now: datetime) -> bool:
    minutes = now.hour * 60 + now.minute
    return NIGHT_START[0] * 60 + NIGHT_START[1] <= minutes < NIGHT_END[0] * 60 + NIGHT_END[1]


def decide(
    slot: Slot, blob: dict[str, Any], *, now: datetime, brake_reason: str | None,
) -> tuple[str | None, str]:
    """(slot to make up, "") when the job should be caught up now, else (None, why not).

    Once per missed slot: a slot that was already tried (`catchup_tried`) is never tried again, so
    a catch-up that failed or timed out waits for the next real slot, no retry storm."""
    due = slot.latest_at_or_before(now)
    raw = blob.get("last_run")
    if not raw:
        return None, "no run on record (a fresh install; the startup runs cover it)"
    last_run = datetime.fromisoformat(raw).astimezone(now.tzinfo)
    if last_run >= due:
        return None, "the latest slot has its run"
    if now - due <= GRACE:
        return None, "the slot has only just come due"
    if blob.get("catchup_tried") == due.isoformat():
        return None, "this slot was already caught up once"
    next_slot = due + timedelta(days=1)
    if next_slot - now < NEXT_SLOT_MARGIN:
        return None, "the next real slot is under an hour away"
    if _in_night(now):
        return None, "the night window belongs to the real jobs"
    if brake_reason:
        return None, f"the disk/WAL brake is engaged ({brake_reason})"
    return due.isoformat(), ""


@contextlib.asynccontextmanager
async def exclusive(pool: Any, name: str) -> AsyncIterator[bool]:
    """Hold a session advisory lock named for this daily job for the block, yielding whether it
    was taken. The real nightly run and a catch-up both take it, so they can never overlap: the
    one that finds it held skips. The lock rides a side connection of its own, not a slot of the
    worker's pool (the job holds it for minutes to hours and uses the pool meanwhile), and is
    released with it, so a crashed run never leaves it held."""
    async with session_lock_connection(pool) as conn:
        got = bool(await conn.fetchval(
            "SELECT pg_try_advisory_lock(hashtext($1))", f"osiris-daily:{name}"))
        try:
            yield got
        finally:
            if got:
                with contextlib.suppress(Exception):
                    await conn.fetchval(
                        "SELECT pg_advisory_unlock(hashtext($1))", f"osiris-daily:{name}")
