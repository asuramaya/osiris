"""The daytime compaction fallback: a box shut overnight never reaches the 03:00 window, so after
two missed nights ONE pg_repack run is allowed by day, on a quiet box, never VACUUM FULL, and
written down as a fallback (receipt, run marker, backup-status missed_runs)."""
from __future__ import annotations

import json
import time
from datetime import datetime
from types import SimpleNamespace
from typing import Any

import pytest
from src.actions.core import Actions
from src.orchestrator import db_compaction, missed_runs
from src.orchestrator.db_compaction import (
    FALLBACK_KIND,
    GB,
    RUN_KEY,
    compact_tonight,
    decide_fallback,
    last_completed_run,
    last_receipts,
    maybe_compact_by_day,
    missed_nights,
)
from src.orchestrator.monitor import get_cursor, set_cursor

SCRATCH = "compaction_fallback_scratch"


def at(day: int, hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime(2026, 11, day, hour, minute, second).astimezone()


# --- the pure rules --------------------------------------------------------------------------

def test_a_missed_night_needs_its_whole_window_to_have_passed() -> None:
    last = at(13, 3, 0, 11)  # the Nov 13 run, 11 s into its slot
    assert missed_nights(last, at(14, 4, 0)) == 0   # Nov 14's window is still open
    assert missed_nights(last, at(14, 10)) == 1     # Nov 14 missed
    assert missed_nights(last, at(15, 10)) == 2     # Nov 14 and 15
    assert missed_nights(last, at(15, 4, 59)) == 1  # Nov 15's window has not closed
    assert missed_nights(None, at(15, 10)) == 0     # no history, nothing to judge from


def _ok(**kw: Any) -> tuple[bool, str]:
    args: dict[str, Any] = dict(
        enabled=True, now=at(15, 10), last_run=at(13, 3, 0, 11), load=1.0, max_load=6.0,
        chain_running=False, brake_reason=None, repack_ready_now=True)
    args.update(kw)
    return decide_fallback(**args)


def test_one_missed_night_does_not_run_and_two_do() -> None:
    assert _ok(now=at(14, 10)) == (False, "1 night(s) missed since the last run, 2 needed")
    assert _ok(now=at(15, 10)) == (True, "")


@pytest.mark.parametrize("kw, word", [
    ({"load": 9.5}, "busy"),
    ({"chain_running": True}, "gate chain is running"),
    ({"chain_running": None}, "could not tell"),
    ({"load": None}, "could not read the load"),
    ({"brake_reason": "disk 91% full"}, "brake"),
    ({"repack_ready_now": False}, "pg_repack is not ready"),
    ({"repack_ready_now": None}, "pg_repack is not ready"),
    ({"enabled": False}, "switched off"),
    ({"now": at(15, 23)}, "outside the daytime hours"),
    ({"now": at(16, 3, 30)}, "outside the daytime hours"),  # never in the night window
])
def test_every_condition_can_stop_the_fallback(kw: dict[str, Any], word: str) -> None:
    ok, why = _ok(**kw)
    assert ok is False and word in why


# --- the run, on the real database -----------------------------------------------------------

async def _bloat(actions: Actions) -> None:
    pool = actions.pool
    await pool.execute(f"DROP TABLE IF EXISTS {SCRATCH}")
    await pool.execute(f"CREATE TABLE {SCRATCH} (id int PRIMARY KEY, pad text NOT NULL)")
    await pool.execute(
        f"INSERT INTO {SCRATCH} SELECT g, repeat('x', 1000) || g FROM generate_series(1, 20000) g")
    await pool.execute(f"DELETE FROM {SCRATCH} WHERE id % 20 <> 0")
    await pool.execute(f"VACUUM {SCRATCH}")


@pytest.fixture
def scratch_only(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    from src.orchestrator import disk_brake

    monkeypatch.setattr(db_compaction, "TABLES", (SCRATCH,))
    monkeypatch.setattr(db_compaction, "MIN_BLOAT_BYTES", 1024 * 1024)
    monkeypatch.setattr(disk_brake, "disk_usage", lambda: (900 * GB, 2000 * GB))
    rewrites: list[str] = []

    async def _repack(pool: Any, table: str, **kw: Any) -> str:
        rewrites.append(table)
        return "pg_repack"

    async def _ready(pool: Any, exec_fn: Any = None) -> bool:
        return True

    monkeypatch.setattr(db_compaction, "rewrite_table", _repack)
    monkeypatch.setattr(db_compaction, "repack_ready", _ready)
    return rewrites


async def _no_brake(pool: Any) -> None:
    return None


async def _chain_idle() -> bool:
    return False


async def _chain_busy() -> bool:
    return True


async def _marker(actions: Actions, when: datetime, kind: str = "night") -> None:
    await set_cursor(actions.pool, RUN_KEY, json.dumps({"at": when.isoformat(), "kind": kind}))


async def _call(actions: Actions, **kw: Any) -> dict[str, Any]:
    args: dict[str, Any] = dict(
        enabled=True, max_load=6.0, window_secs=600.0, now=at(15, 10), load_fn=lambda: 1.0,
        chain_fn=_chain_idle, brake_fn=_no_brake)
    args.update(kw)
    return await maybe_compact_by_day(actions.pool, **args)


async def test_one_missed_night_runs_nothing(
    actions: Actions, scratch_only: list[str],
) -> None:
    await _bloat(actions)
    await _marker(actions, at(14, 3, 0, 5))  # last night ran; only Nov 15 could be missed
    out = await _call(actions, now=at(15, 10))
    assert out["ran"] is False and "night(s) missed" in out["why"] and scratch_only == []


async def test_two_missed_nights_run_once_as_a_daytime_fallback(
    actions: Actions, scratch_only: list[str],
) -> None:
    await _bloat(actions)
    await _marker(actions, at(13, 3, 0, 5))
    out = await _call(actions)
    assert out["ran"] is True and scratch_only == [SCRATCH]
    receipt = (await last_receipts(actions.pool))[SCRATCH]
    assert receipt["run_kind"] == FALLBACK_KIND and receipt["action"] == "rewritten"
    assert receipt["engine"] == "pg_repack"
    done = await last_completed_run(actions.pool)
    assert done is not None and done[1] == FALLBACK_KIND
    # the marker ends the condition: a second look the same day finds nothing missed
    again = await _call(actions)
    assert again["ran"] is False and scratch_only == [SCRATCH]


async def test_a_busy_box_skips(actions: Actions, scratch_only: list[str]) -> None:
    await _bloat(actions)
    await _marker(actions, at(13, 3, 0, 5))
    out = await _call(actions, load_fn=lambda: 14.0)
    assert out["ran"] is False and "busy" in out["why"] and scratch_only == []
    assert json.loads(await get_cursor(actions.pool, RUN_KEY) or "{}")["kind"] == "night"


async def test_a_running_gate_chain_skips(actions: Actions, scratch_only: list[str]) -> None:
    await _bloat(actions)
    await _marker(actions, at(13, 3, 0, 5))
    out = await _call(actions, chain_fn=_chain_busy)
    assert out["ran"] is False and "gate chain" in out["why"] and scratch_only == []


async def test_a_clear_disk_brake_is_needed(actions: Actions, scratch_only: list[str]) -> None:
    await _bloat(actions)
    await _marker(actions, at(13, 3, 0, 5))

    async def _paused(pool: Any) -> str:
        return "WAL 40 GB in the last hour"

    out = await _call(actions, brake_fn=_paused)
    assert out["ran"] is False and "brake" in out["why"] and scratch_only == []


async def test_without_pg_repack_nothing_runs_and_vacuum_full_is_never_used(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The test database has no pg_repack extension: the fallback refuses, and the table is not
    rewritten by any other means."""
    from src.orchestrator import disk_brake

    monkeypatch.setattr(db_compaction, "TABLES", (SCRATCH,))
    monkeypatch.setattr(db_compaction, "MIN_BLOAT_BYTES", 1024 * 1024)
    monkeypatch.setattr(disk_brake, "disk_usage", lambda: (900 * GB, 2000 * GB))
    await _bloat(actions)
    before = (await db_compaction.table_sizes(actions.pool, SCRATCH))["total_bytes"]
    await _marker(actions, at(13, 3, 0, 5))
    out = await _call(actions)
    assert out["ran"] is False and "pg_repack is not ready" in out["why"]
    # and even called directly, a repack-only run skips the table instead of VACUUM FULL
    receipts = await compact_tonight(
        actions.pool, window_secs=600.0, run_kind=FALLBACK_KIND, repack_only=True)
    assert receipts[SCRATCH]["action"] == "skipped"
    assert "never rewrites with VACUUM FULL" in receipts[SCRATCH]["reason"]
    after = (await db_compaction.table_sizes(actions.pool, SCRATCH))["total_bytes"]
    assert after == before


# --- what the desk and backup-status say -----------------------------------------------------

class _Pool:
    def __init__(self, job: dict[str, str], marker: dict[str, str] | None) -> None:
        self.rows = [{"key": "job:db_compaction_heartbeat", "cursor": json.dumps(job)}]
        if marker:
            self.rows.append({"key": missed_runs.COMPACTION_RUN_KEY,
                              "cursor": json.dumps(marker)})

    async def fetch(self, sql: str, keys: list[str]) -> list[dict[str, str]]:
        return [r for r in self.rows if r["key"] in keys]


async def test_missed_runs_reads_the_run_marker_not_the_no_op_fire_at_wake() -> None:
    assert missed_runs.COMPACTION_RUN_KEY == RUN_KEY
    # the cron fired at wake (job record: 08:51) but the gate made it a no-op: no run happened
    job = {"last_run": at(14, 8, 51).isoformat()}
    marker = {"at": at(13, 3, 0, 5).isoformat(), "kind": "night"}
    out = await missed_runs.section(_Pool(job, marker), [], now=at(14, 9))
    [problem] = [p for p in out["problems"] if "compaction" in p]
    assert "has not run yet" in problem and "last ran at 03:00 on 11-13" in problem


async def test_missed_runs_says_when_the_run_was_the_daytime_fallback() -> None:
    job = {"last_run": at(15, 10, 20).isoformat()}
    marker = {"at": at(15, 10, 20).isoformat(), "kind": FALLBACK_KIND}
    out = await missed_runs.section(_Pool(job, marker), [], now=at(15, 12))
    [problem] = [p for p in out["problems"] if "compaction" in p]
    assert "ran as a catch-up at 10:20" in problem


# --- the cron --------------------------------------------------------------------------------

async def test_a_tick_that_waited_out_a_sleep_does_nothing() -> None:
    from src.workers.arq_worker import compaction_fallback_heartbeat

    class _Boom:
        def __getattr__(self, name: str) -> Any:
            raise AssertionError("a stale tick must not touch the database")

    ctx = {"score": (time.time() - 3600) * 1000.0,
           "cascade": SimpleNamespace(actions=SimpleNamespace(pool=_Boom()))}
    assert await compaction_fallback_heartbeat(ctx) == 0


def test_the_fallback_cron_is_registered_every_fifteen_minutes() -> None:
    from src.workers.arq_worker import WorkerSettings

    crons = {getattr(c.coroutine, "__name__", ""): c for c in WorkerSettings.cron_jobs}
    job = crons["compaction_fallback_heartbeat"]
    assert job.minute == {5, 20, 35, 50} and job.run_at_startup is False
    # the nightly job is still the only one with the 03:00 slot
    assert crons["db_compaction_heartbeat"].hour == {3}
