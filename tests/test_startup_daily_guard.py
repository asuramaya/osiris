"""A daily job's startup run catches up a missed slot; it does not repeat on every restart."""
from __future__ import annotations

import json
import time
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from src.actions.core import Actions
from src.orchestrator.monitor import set_cursor
from src.workers import arq_worker

DAY = 86400


def _ctx(actions: Actions, *, booting: bool = True) -> dict[str, Any]:
    ctx: dict[str, Any] = {"cascade": SimpleNamespace(actions=actions)}
    ctx["boot_serialize_until"] = time.monotonic() + (30 if booting else -30)
    return ctx


def _job(name: str, ran: list[str]) -> Any:
    async def job(ctx: dict[str, Any]) -> int:
        ran.append(name)
        return 1
    job.__name__ = name
    return job


async def _seed(actions: Actions, name: str, **blob: Any) -> None:
    await set_cursor(actions.pool, f"job:{name}", json.dumps(blob))


def _ago(hours: float) -> str:
    return (datetime.now(UTC) - timedelta(hours=hours)).isoformat()


async def test_a_daily_job_that_ran_today_is_not_run_again_at_a_restart(actions: Actions) -> None:
    ran: list[str] = []
    await _seed(actions, "neighborhood_pass", last_ok=_ago(5), every=DAY)
    wrapped = arq_worker.watched(_job("neighborhood_pass", ran), every=DAY, heavy=True)

    assert await wrapped(_ctx(actions)) == 0
    assert ran == []


async def test_a_daily_job_that_missed_its_slot_still_catches_up_at_startup(
    actions: Actions,
) -> None:
    ran: list[str] = []
    await _seed(actions, "retention_heartbeat", last_ok=_ago(40), every=DAY)
    wrapped = arq_worker.watched(_job("retention_heartbeat", ran), every=DAY)

    assert await wrapped(_ctx(actions)) == 1
    assert ran == ["retention_heartbeat"]


async def test_a_daily_job_that_never_ran_runs_at_startup(actions: Actions) -> None:
    ran: list[str] = []
    wrapped = arq_worker.watched(_job("soul_cold_tier_heartbeat", ran), every=DAY)

    assert await wrapped(_ctx(actions)) == 1 and ran == ["soul_cold_tier_heartbeat"]


async def test_a_daily_job_whose_last_run_failed_is_retried_at_startup(actions: Actions) -> None:
    ran: list[str] = []
    # it ran an hour ago but failed: there is no recent success, only a recent attempt
    await _seed(actions, "retention_heartbeat", last_run=_ago(1), last_error="boom",
                last_ok=_ago(60), every=DAY)
    wrapped = arq_worker.watched(_job("retention_heartbeat", ran), every=DAY)

    assert await wrapped(_ctx(actions)) == 1 and ran == ["retention_heartbeat"]


async def test_a_scheduled_slot_outside_the_boot_window_always_runs(actions: Actions) -> None:
    ran: list[str] = []
    await _seed(actions, "retention_heartbeat", last_ok=_ago(1), every=DAY)
    wrapped = arq_worker.watched(_job("retention_heartbeat", ran), every=DAY)

    assert await wrapped(_ctx(actions, booting=False)) == 1  # the guard is a startup rule only
    assert ran == ["retention_heartbeat"]


async def test_a_frequent_job_is_unchanged_by_the_guard(actions: Actions) -> None:
    ran: list[str] = []
    await _seed(actions, "reap_runs", last_ok=_ago(0.01), every=300)
    wrapped = arq_worker.watched(_job("reap_runs", ran), every=300)

    assert await wrapped(_ctx(actions)) == 1 and ran == ["reap_runs"]


async def test_an_unreadable_record_does_not_block_the_startup_run(actions: Actions) -> None:
    ran: list[str] = []
    await set_cursor(actions.pool, "job:retention_heartbeat", "{not json")
    wrapped = arq_worker.watched(_job("retention_heartbeat", ran), every=DAY)

    assert await wrapped(_ctx(actions)) == 1 and ran == ["retention_heartbeat"]


async def test_the_catch_up_path_is_not_held_back_by_the_guard(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    # the daily catch-up runs the job through `_run_watched` directly, so a recent-success
    # check at startup never reaches it
    ran: list[str] = []
    job = _job("retention_heartbeat", ran)

    out = await arq_worker._run_watched(job, _ctx(actions), DAY, catchup_for="slot")

    assert out == 1 and ran == ["retention_heartbeat"]


def test_every_daily_job_that_runs_at_startup_is_listed_so_the_guard_is_known_to_cover_it() -> None:
    """The jobs with a period of an hour or more that start at worker start: each goes through
    `watched`, so each gets the guard. A new one must be added to this list on purpose."""
    from src.workers.arq_worker import WorkerSettings

    daily_startup = sorted(
        getattr(c.coroutine, "__name__", "") for c in WorkerSettings.cron_jobs
        if getattr(c, "run_at_startup", False) and getattr(c, "timeout", 0) is not None
        and _period(c) >= arq_worker._DAILY_FLOOR_SECS)

    assert daily_startup == ["neighborhood_pass", "retention_heartbeat",
                             "soul_cold_tier_heartbeat"]


def _period(cron_job: Any) -> int:
    """A cron's rough period in seconds from the hours/minutes it fires on."""
    if cron_job.hour is not None and not isinstance(cron_job.hour, str):
        hours = len(cron_job.hour)
        return DAY // max(hours, 1) if hours else DAY
    return 0
