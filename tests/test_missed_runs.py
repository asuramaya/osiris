"""A nightly run that did not happen on time is said out loud, from each job's own last run."""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pytest
from src.orchestrator import missed_runs as mr

CHI = ZoneInfo("America/Chicago")


def at(day: int, hh: int, mm: int = 0) -> datetime:
    return datetime(2026, 11, day, hh, mm, tzinfo=CHI)


DUMP = mr.Slot(4, 30)


def test_the_dump_that_caught_up_at_wake_is_reported_late_with_how_late() -> None:
    got = mr.assess("the 04:30 dump", DUMP, at(12, 10, 54), at(12, 17))

    assert got["status"] == "late"
    assert got["sentence"] == ("the 04:30 dump was due at 04:30 on 11-12 and ran at 10:54, "
                               "6.4 h late (the box was probably asleep or off)")


def test_a_run_within_the_grace_is_on_time() -> None:
    assert mr.assess("dump", DUMP, at(12, 4, 31), at(12, 17))["status"] == "on_time"
    assert mr.assess("dump", DUMP, at(12, 4, 59), at(12, 17))["status"] == "on_time"
    assert mr.assess("dump", DUMP, at(12, 5, 5), at(12, 17))["status"] == "late"


def test_a_job_that_is_due_and_has_not_run_is_waiting() -> None:
    got = mr.assess("the nightly housekeeping", mr.Slot(2, 15), at(11, 2, 15), at(12, 10, 54))

    assert got["status"] == "waiting"
    assert got["sentence"] == ("the nightly housekeeping was due at 02:15 on 11-12 and has not "
                               "run yet; it last ran at 02:15 on 11-11 (the box was probably "
                               "off, or the worker restarted after the slot)")


def test_a_slot_that_has_only_just_come_due_is_not_yet_a_problem() -> None:
    assert mr.assess("housekeeping", mr.Slot(2, 15), at(11, 2, 15), at(12, 2, 25))[
        "status"] == "on_time"


def test_a_job_with_no_run_on_record_says_so() -> None:
    assert mr.assess("dump", DUMP, None, at(12, 17))["status"] == "never"


def test_a_weekly_slot_looks_for_its_own_day() -> None:
    sunday = mr.Slot(2, 0, weekday=6)  # Sun 02:00; 2026-11-15 is a Sunday
    thursday = at(12, 17)

    assert sunday.latest_at_or_before(thursday) == at(8, 2, 0)  # the Sunday before
    got = mr.assess("weekly base backup", sunday, at(8, 2, 0), thursday)
    assert got["status"] == "on_time"
    assert mr.assess("weekly base backup", sunday, at(1, 2, 0), thursday)["status"] == "waiting"


def test_only_problems_are_listed_worst_first_and_old_ones_age_out() -> None:
    now = at(12, 17)
    jobs = [("dump", DUMP, at(12, 10, 54)),                      # late
            ("housekeeping", mr.Slot(2, 15), at(11, 2, 15)),     # waiting
            ("clean-up", mr.Slot(4, 40), at(12, 4, 40)),         # on time: not listed
            ("old", mr.Slot(4, 0), at(1, 4, 0))]                 # waiting, but weeks ago? due today
    got = mr.recent_problems(jobs, now)

    assert [p["label"] for p in got] == ["housekeeping", "old", "dump"]  # waiting, waiting, late
    assert mr.recent_problems([("clean-up", mr.Slot(4, 40), at(12, 4, 40))], now) == []


@pytest.mark.parametrize("spec,expected", [
    ("*-*-* 04:30:00", mr.Slot(4, 30)),
    ("Sun 02:00:00", mr.Slot(2, 0, 6)),
    ("Sat 03:00:00", mr.Slot(3, 0, 5)),
    ("Mon *-*-* 05:00:00", mr.Slot(5, 0, 0)),
    ("*:0/15", None), ("*:17", None), ("daily", None), ("", None),
])
def test_calendar_lines_become_slots_and_frequent_ones_do_not(
    spec: str, expected: mr.Slot | None,
) -> None:
    assert mr.slot_from_calendar(spec) == expected


def test_a_systemd_stamp_is_read_as_local_time_and_n_a_is_none() -> None:
    got = mr.parse_systemd_stamp("Thu 2026-11-12 10:54:47 CST")

    assert got is not None and (got.year, got.month, got.day, got.hour, got.minute) == (
        2026, 11, 12, 10, 54)
    assert got.tzinfo is not None
    assert mr.parse_systemd_stamp("n/a") is None and mr.parse_systemd_stamp(None) is None


class _Pool:
    def __init__(self, last_runs: dict[str, str] | None, fail: bool = False) -> None:
        self.last_runs = last_runs or {}
        self.fail = fail

    async def fetch(self, sql: str, keys: list[str]) -> list[dict[str, str]]:
        if self.fail:
            raise OSError("db down")
        return [{"key": k, "cursor": json.dumps({"last_run": v})}
                for k, v in ((k, self.last_runs.get(k[4:])) for k in keys) if v]


async def test_the_section_lists_a_late_timer_and_a_worker_job_that_has_not_run() -> None:
    timers: list[dict[str, Any]] = [
        {"unit": "osiris-backup.timer", "label": "dumps (pg_dump, daily)",
         "schedule": "*-*-* 04:30:00", "active_state": "active",
         "last_trigger": "Thu 2026-11-12 10:54:47 CST"},
        {"unit": "osiris-wal-pull.timer", "label": "WAL pull", "schedule": "*:0/15",
         "active_state": "active", "last_trigger": "Thu 2026-11-12 16:45:03 CST"},
    ]
    last = {"storage_housekeeping_heartbeat": at(11, 2, 15).isoformat(),
            "db_compaction_heartbeat": at(11, 3, 0).isoformat(),
            "retention_heartbeat": at(12, 3, 30).isoformat(),
            "soul_cold_tier_heartbeat": at(12, 3, 45).isoformat()}

    out = await mr.section(_Pool(last), timers, now=at(12, 17))

    assert out["checked"] == 5  # the every-15-minute timer has no slot to miss
    assert out["problems"] == [
        "the nightly housekeeping was due at 02:15 on 11-12 and has not run yet; it last ran "
        "at 02:15 on 11-11 (the box was probably off, or the worker restarted after the slot)",
        "the nightly database compaction was due at 03:00 on 11-12 and has not run yet; it "
        "last ran at 03:00 on 11-11 (the box was probably off, or the worker restarted after "
        "the slot)",
        "dumps (pg_dump, daily) was due at 04:30 on 11-12 and ran at 10:54, 6.4 h late "
        "(the box was probably asleep or off)"]


async def test_an_unreadable_job_record_is_named_and_never_raises() -> None:
    out = await mr.section(_Pool(None, fail=True), [], now=at(12, 17))

    assert out["unread"] == ["the worker's job record"] and out["problems"] == []


async def test_a_timer_systemd_says_nothing_about_is_unread_not_missed() -> None:
    timers = [{"unit": "osiris-backup.timer", "label": "dumps", "schedule": "*-*-* 04:30:00"}]

    out = await mr.section(_Pool({}), timers, now=at(12, 17))

    assert out["unread"][:1] == ["osiris-backup.timer"]


def test_the_worker_slot_table_matches_the_crons_it_describes() -> None:
    from src.workers.arq_worker import WorkerSettings

    crons = {getattr(c.coroutine, "__name__", ""): c for c in WorkerSettings.cron_jobs}
    for name, _label, slot in mr.WORKER_DAILY_JOBS:
        job = crons[name]
        assert job.hour == {slot.hour} and job.minute == {slot.minute}, name


def test_a_slot_never_lands_in_the_future() -> None:
    now = at(12, 4, 0)

    assert DUMP.latest_at_or_before(now) == at(11, 4, 30)
    assert DUMP.latest_at_or_before(now) <= now
    assert now - DUMP.latest_at_or_before(now) < timedelta(days=1)


async def test_a_newly_installed_timer_that_has_not_fired_yet_is_not_a_problem() -> None:
    timers = [{"unit": "osiris-envelope-check.timer", "label": "daily envelope check",
               "schedule": "*-*-* 09:20:00", "active_state": "active",
               "next_elapse": "Fri 2026-11-13 09:20:00 CST"}]

    out = await mr.section(_Pool({}), timers, now=at(12, 17))

    assert not any("envelope" in p for p in out["problems"]) and "unread" not in out
