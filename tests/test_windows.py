from __future__ import annotations

from datetime import UTC, datetime, timedelta

from src.orchestrator.windows import due_buckets, parse_duration


def test_parse_duration() -> None:
    assert parse_duration("7d") == timedelta(days=7)
    assert parse_duration("12h") == timedelta(hours=12)
    assert parse_duration("30m") == timedelta(minutes=30)


def test_first_run_backfills_bounded_by_lookback() -> None:
    now = datetime(2026, 5, 28, 12, 0, tzinfo=UTC)
    buckets = due_buckets(
        {"bucket": "7d", "slide": "1d", "lookback": "30d"}, now=now, last_bucket=None
    )
    # ~30 daily slide-aligned windows up to now (aligned to midnight)
    assert 29 <= len(buckets) <= 31
    assert all(b.hour == 0 for b in buckets)  # slide-aligned to the day grid
    assert buckets == sorted(buckets)


def test_subsequent_run_continues_after_last_bucket() -> None:
    now = datetime(2026, 5, 28, 12, 0, tzinfo=UTC)
    last = datetime(2026, 5, 25, 0, 0, tzinfo=UTC)
    buckets = due_buckets(
        {"bucket": "7d", "slide": "1d", "lookback": "30d"}, now=now, last_bucket=last
    )
    # only the days after `last`, up to today: 26th, 27th, 28th
    assert buckets == [
        datetime(2026, 5, 26, tzinfo=UTC),
        datetime(2026, 5, 27, tzinfo=UTC),
        datetime(2026, 5, 28, tzinfo=UTC),
    ]


def test_max_buckets_cap() -> None:
    now = datetime(2026, 5, 28, tzinfo=UTC)
    buckets = due_buckets(
        {"bucket": "7d", "slide": "1d", "lookback": "365d"}, now=now, last_bucket=None,
        max_buckets=10,
    )
    assert len(buckets) == 10
