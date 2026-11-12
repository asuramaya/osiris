"""A nightly worker job the box slept through is made up by day, under guards, and never the
compaction."""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

import pytest
from src.actions.core import Actions
from src.orchestrator import daily_catchup as dc
from src.orchestrator import missed_runs as mr
from src.orchestrator.monitor import get_cursor, record_job, set_cursor

CHI = ZoneInfo("America/Chicago")
HOUSE = mr.Slot(2, 15)


def at(day: int, hh: int, mm: int = 0) -> datetime:
    return datetime(2026, 11, day, hh, mm, tzinfo=CHI)


def blob(last_run: datetime | None, **extra: Any) -> dict[str, Any]:
    return {"last_run": last_run.isoformat() if last_run else None, **extra}


def test_an_overdue_job_is_caught_up_with_the_slot_it_is_making_up() -> None:
    due, why = dc.decide(HOUSE, blob(at(11, 2, 15)), now=at(12, 10, 54), brake_reason=None)

    assert due == at(12, 2, 15).isoformat() and why == ""


@pytest.mark.parametrize("case,last,now,brake,tried,expect", [
    ("latest slot already has its run", at(12, 2, 15), at(12, 10, 54), None, None,
     "has its run"),
    ("slot only just came due", at(11, 2, 15), at(12, 2, 40), None, None, "only just"),
    ("already caught up once", at(11, 2, 15), at(12, 10, 54), None, at(12, 2, 15),
     "already caught up"),
    ("next real slot under an hour away", at(11, 2, 15), at(13, 1, 30), None, None,
     "under an hour"),
    ("the night window", at(11, 2, 15), at(12, 3, 10), None, None, "night window"),
    ("the brake is engaged", at(11, 2, 15), at(12, 10, 54), "disk below the floor", None,
     "brake is engaged"),
    ("no run on record", None, at(12, 10, 54), None, None, "no run on record"),
])
def test_the_guards(case: str, last: datetime | None, now: datetime, brake: str | None,
                    tried: datetime | None, expect: str) -> None:
    extra = {"catchup_tried": tried.isoformat()} if tried else {}

    due, why = dc.decide(HOUSE, blob(last, **extra), now=now, brake_reason=brake)

    assert due is None and expect in why, case


def test_compaction_is_never_in_the_catch_up_set_and_the_reason_is_written_down() -> None:
    names = {name for name, _ in dc.CATCHUP_JOBS}

    assert names == {"storage_housekeeping_heartbeat", "retention_heartbeat",
                     "soul_cold_tier_heartbeat"}
    assert "db_compaction_heartbeat" not in names
    assert "overnight" in dc.EXCLUDED["db_compaction_heartbeat"]
    assert set(dc.TIMEOUTS) == names


async def test_two_runs_of_one_job_cannot_overlap(actions: Actions) -> None:
    async with dc.exclusive(actions.pool, "retention_heartbeat") as first:
        async with dc.exclusive(actions.pool, "retention_heartbeat") as second:
            async with dc.exclusive(actions.pool, "another_job") as other:
                assert (first, second, other) == (True, False, True)
    async with dc.exclusive(actions.pool, "retention_heartbeat") as again:
        assert again is True  # released with the first


async def test_the_nightly_job_skips_when_a_catch_up_holds_its_lock(actions: Actions) -> None:
    from src.workers import arq_worker

    ran: list[str] = []

    async def retention_heartbeat(ctx: dict[str, Any]) -> int:
        ran.append("ran")
        return 7

    job = arq_worker._exclusive_daily(retention_heartbeat)
    ctx = {"cascade": SimpleNamespace(actions=actions)}

    assert await job(ctx) == 7 and ran == ["ran"]
    async with dc.exclusive(actions.pool, "retention_heartbeat"):
        assert await job(ctx) == 0 and ran == ["ran"]  # skipped, not run twice
    assert job.__name__ == "retention_heartbeat"  # `watched` still sees the same job


# ---- the heartbeat, end to end over the real job record ----------------------------------------


class _Clock(datetime):
    fixed = at(12, 10, 54)

    @classmethod
    def now(cls, tz: Any = None) -> datetime:  # type: ignore[override]
        return cls.fixed


@pytest.fixture
def worker(monkeypatch: pytest.MonkeyPatch) -> Any:
    from src.orchestrator import disk_brake
    from src.workers import arq_worker

    ran: list[str] = []

    def make(name: str, fail: bool = False) -> Any:
        async def job(ctx: dict[str, Any]) -> int:
            ran.append(name)
            if fail:
                raise RuntimeError("boom")
            return 1
        job.__name__ = name
        return job

    async def calm(pool: Any, job: str) -> None:
        return None

    monkeypatch.setattr(arq_worker, "datetime", _Clock)
    monkeypatch.setattr(disk_brake, "pause_reason", calm)
    for name in ("storage_housekeeping_heartbeat", "retention_heartbeat",
                 "soul_cold_tier_heartbeat"):
        monkeypatch.setattr(arq_worker, name, make(name))
    return SimpleNamespace(mod=arq_worker, ran=ran, make=make, monkeypatch=monkeypatch)


async def _seed(actions: Actions, last_runs: dict[str, datetime | None]) -> None:
    for name, when in last_runs.items():
        await set_cursor(actions.pool, f"job:{name}", json.dumps(blob(when)))


async def _blob(actions: Actions, name: str) -> dict[str, Any]:
    return json.loads(await get_cursor(actions.pool, f"job:{name}") or "{}")


_OVERDUE = {"storage_housekeeping_heartbeat": at(11, 2, 15), "retention_heartbeat": at(11, 3, 30),
            "soul_cold_tier_heartbeat": at(11, 3, 45)}


async def test_one_overdue_job_is_run_per_tick_and_marked_as_a_catch_up(
    actions: Actions, worker: Any,
) -> None:
    await _seed(actions, _OVERDUE)
    ctx = {"cascade": SimpleNamespace(actions=actions)}

    assert await worker.mod.daily_catchup_heartbeat(ctx) == 1
    assert worker.ran == ["storage_housekeeping_heartbeat"]  # one, not all three
    house = await _blob(actions, "storage_housekeeping_heartbeat")
    assert house["catchup"] == at(12, 2, 15).isoformat()
    assert house["catchup_tried"] == at(12, 2, 15).isoformat() and house["fails"] == 0

    assert await worker.mod.daily_catchup_heartbeat(ctx) == 1  # the next tick: the next job
    assert await worker.mod.daily_catchup_heartbeat(ctx) == 1
    assert await worker.mod.daily_catchup_heartbeat(ctx) == 0  # nothing left to make up
    assert worker.ran == ["storage_housekeeping_heartbeat", "retention_heartbeat",
                          "soul_cold_tier_heartbeat"]  # and never db_compaction_heartbeat


async def test_a_catch_up_that_fails_is_recorded_and_not_retried_for_that_slot(
    actions: Actions, worker: Any,
) -> None:
    await _seed(actions, {"retention_heartbeat": at(11, 3, 30)})
    worker.monkeypatch.setattr(worker.mod, "retention_heartbeat",
                               worker.make("retention_heartbeat", fail=True))
    ctx = {"cascade": SimpleNamespace(actions=actions)}

    assert await worker.mod.daily_catchup_heartbeat(ctx) == 1
    failed = await _blob(actions, "retention_heartbeat")
    assert failed["fails"] == 1 and "boom" in failed["last_error"]

    assert await worker.mod.daily_catchup_heartbeat(ctx) == 0  # no retry storm
    assert worker.ran == ["retention_heartbeat"]


async def test_nothing_runs_while_the_brake_is_engaged(actions: Actions, worker: Any) -> None:
    from src.orchestrator import disk_brake

    async def engaged(pool: Any, job: str) -> str:
        return "the disk is below its floor"

    worker.monkeypatch.setattr(disk_brake, "pause_reason", engaged)
    await _seed(actions, _OVERDUE)

    assert await worker.mod.daily_catchup_heartbeat(
        {"cascade": SimpleNamespace(actions=actions)}) == 0
    assert worker.ran == []


async def test_an_ordinary_run_clears_the_catch_up_mark_and_a_catch_up_shows_in_missed_runs(
    actions: Actions,
) -> None:
    await record_job(actions.pool, "retention_heartbeat", every=86400,
                     catchup_for=at(12, 3, 30).isoformat())
    assert (await _blob(actions, "retention_heartbeat"))["catchup"]
    await record_job(actions.pool, "retention_heartbeat", every=86400)
    assert "catchup" not in await _blob(actions, "retention_heartbeat")

    sentence = mr.assess("the nightly retention pass", mr.Slot(3, 30), at(12, 10, 59),
                         at(12, 11), catchup=True)["sentence"]
    assert sentence == ("the nightly retention pass was due at 03:30 on 11-12 and ran as a "
                        "catch-up at 10:59, 7.5 h late (the box was probably asleep or off)")


def test_the_cron_is_registered_and_the_three_jobs_keep_their_names() -> None:
    from src.workers.arq_worker import WorkerSettings

    crons = {getattr(c.coroutine, "__name__", "") for c in WorkerSettings.cron_jobs}

    assert "daily_catchup_heartbeat" in crons
    assert {"storage_housekeeping_heartbeat", "retention_heartbeat",
            "soul_cold_tier_heartbeat", "db_compaction_heartbeat"} <= crons
    assert timedelta(minutes=60) == dc.NEXT_SLOT_MARGIN
