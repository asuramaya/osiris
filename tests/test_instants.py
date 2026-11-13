"""Every reader of a restic or receipt time reads it as an instant: an offset, 'Z', nanoseconds
and a time with no zone all compare and subtract without a wrong order or a TypeError."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from src.orchestrator import offload_staleness, readiness, restic_locks, scheduled_drill
from src.orchestrator.instants import parse_time

NOW = datetime(2026, 11, 13, 18, 0, 0, tzinfo=UTC)


@pytest.mark.parametrize("text,expected", [
    ("2026-11-13T11:05:00.123456789-06:00", datetime(2026, 11, 13, 17, 5, 0, 123456, tzinfo=UTC)),
    ("2026-11-13T17:05:00Z", datetime(2026, 11, 13, 17, 5, 0, tzinfo=UTC)),
    ("2026-11-13T17:05:00+00:00", datetime(2026, 11, 13, 17, 5, 0, tzinfo=UTC)),
    ("2026-11-13T17:05:00", datetime(2026, 11, 13, 17, 5, 0, tzinfo=UTC)),
    ("2026-11-13T23:35:00+05:30", datetime(2026, 11, 13, 18, 5, 0, tzinfo=UTC)),
    ("garbage", None), ("", None), (None, None), (12345, None),
])
def test_parse_time_gives_an_aware_utc_instant_or_none(text: Any, expected: Any) -> None:
    assert parse_time(text) == expected


def test_two_zones_compare_by_instant_not_by_text() -> None:
    later_in_text = parse_time("2026-11-13T14:00:00Z")
    earlier_in_text = parse_time("2026-11-13T12:00:00-06:00")  # 18:00 UTC: the later instant

    assert later_in_text is not None and earlier_in_text is not None
    assert earlier_in_text > later_in_text


def test_a_drill_receipt_with_no_zone_does_not_crash_the_due_check() -> None:
    receipt = {"last_attempt_at": "2026-11-13T17:00:00", "last_error": "boom",
               "last_passed_at": "2026-11-01T00:00:00"}  # naive, as an older receipt may be
    offload = {"last_successful_offload": "2026-11-12T00:00:00+00:00"}

    due = scheduled_drill.drill_due(receipt, offload, now=NOW)

    assert due is False  # a failure one hour ago holds off the retry; no TypeError


def test_a_drill_that_passed_in_another_zone_is_not_due_until_a_week_later() -> None:
    passed = (NOW - timedelta(days=6)).astimezone(
        datetime.now().astimezone().tzinfo).isoformat()

    assert scheduled_drill.drill_due({"last_passed_at": passed},
                                     {"last_successful_offload": "x"}, now=NOW) is False
    old = (NOW - timedelta(days=8)).isoformat().replace("+00:00", "Z")
    assert scheduled_drill.drill_due({"last_passed_at": old},
                                     {"last_successful_offload": "x"}, now=NOW) is True


def test_a_naive_offload_time_does_not_break_the_stale_verdict() -> None:
    target = {"last_successful_offload": "2026-11-03T18:00:00",   # naive: ten days before NOW
              "last_attempt_at": "2026-11-10T16:30:05.411452+00:00"}

    out = offload_staleness.annotate_target(target, now=NOW, stale_days=7)

    assert out["days_since_last_success"] == 10
    assert out["offload_verdict"].startswith("stale")


def test_readiness_compares_a_naive_pass_with_an_aware_first_offload() -> None:
    target = {"name": "nas", "kind": "restic", "enabled": True, "path_or_url": "sftp:nas:/r",
              "first_successful_offload": "2026-11-01T00:00:00+00:00",
              "last_successful_offload": "2026-11-02T00:00:00+00:00"}
    receipts = {"sftp:nas:/r": {"last_passed_at": "2026-11-05T00:00:00"}}  # naive, later

    step = readiness._restore_test_step([target], receipts)

    assert step["status"] == "done"  # no TypeError, and the later pass counts


def test_a_lock_written_with_a_local_offset_and_nanoseconds_is_read() -> None:
    got = restic_locks._parse_time("2026-11-13T11:05:00.123456789-06:00")

    assert got == datetime(2026, 11, 13, 17, 5, 0, 123456, tzinfo=UTC)
    assert restic_locks._parse_time("nope") is None


def test_the_recovery_copy_age_survives_a_receipt_time_with_no_zone(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.orchestrator import recovery_copies

    monkeypatch.setenv(recovery_copies._RECEIPTS_ENV, str(tmp_path / "copies.json"))
    src = tmp_path / "recovery.json"
    src.write_text("{}")
    monkeypatch.setattr(recovery_copies, "source_file", lambda path=None: src)
    sha = recovery_copies._sha256(src)
    recent = (datetime.now(UTC) - timedelta(hours=1)).replace(tzinfo=None).isoformat()
    recovery_copies._write_receipt("nas", {"sha256": sha, "last_copied_at": recent,
                                           "last_error": None})
    target = {"name": "nas", "kind": "restic", "path_or_url": "sftp:truenas:/r"}

    # an hour-old copy with a zone-less time is current: nothing is copied again
    assert recovery_copies.copy_to_target(target, sftp_copy=lambda s, u: "should not run") is None
