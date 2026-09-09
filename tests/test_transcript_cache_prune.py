"""The cache prune (soul-store lane item 4, thread 78efd46d) — pure logic only; the CLI's
DB collection and file deletion are a thin, dry-run-first shell exercised separately."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

from scripts.osiris_transcript_cache_prune import SessionRow, find_prunable_sessions

NOW = datetime(2026, 9, 8, 12, 0, tzinfo=UTC)


def _s(anchor: str, mtime_days_ago: float | None, ingested_days_ago: float) -> SessionRow:
    mtime = NOW - timedelta(days=mtime_days_ago) if mtime_days_ago is not None else None
    return SessionRow(
        anchor_sid=anchor, source_path=f"/tmp/{anchor}.jsonl", file_mtime=mtime,
        last_ingested_at=NOW - timedelta(days=ingested_days_ago))


def test_a_dead_fully_captured_session_is_prunable() -> None:
    s = _s("dead1", mtime_days_ago=60, ingested_days_ago=1)  # store saw it AFTER it went quiet
    assert find_prunable_sessions([s], now=NOW) == [s]


def test_a_recently_active_session_is_never_pruned() -> None:
    s = _s("live1", mtime_days_ago=2, ingested_days_ago=1)
    assert find_prunable_sessions([s], now=NOW) == []


def test_right_at_the_dead_after_boundary_is_prunable() -> None:
    """>= the window, not only strictly over it."""
    s = _s("boundary1", mtime_days_ago=30, ingested_days_ago=1)
    assert find_prunable_sessions([s], now=NOW, dead_after=timedelta(days=30)) == [s]


def test_just_under_the_boundary_is_not_prunable() -> None:
    s = _s("young1", mtime_days_ago=29.9, ingested_days_ago=31)
    assert find_prunable_sessions([s], now=NOW, dead_after=timedelta(days=30)) == []


def test_a_file_the_store_has_not_fully_seen_is_never_pruned() -> None:
    """The load-bearing safety check: file mtime NEWER than last_ingested_at means the
    store hasn't captured this file's latest bytes — deleting it would lose real
    content, even if the file looks old by the calendar (a stale ingest cursor, not a
    dead session)."""
    s = _s("stale_ingest1", mtime_days_ago=60, ingested_days_ago=90)
    assert find_prunable_sessions([s], now=NOW) == []


def test_an_already_missing_file_is_skipped_not_double_pruned() -> None:
    s = _s("gone1", mtime_days_ago=None, ingested_days_ago=1)
    assert find_prunable_sessions([s], now=NOW) == []


def test_a_custom_dead_after_window_is_honored() -> None:
    s = _s("week_old", mtime_days_ago=8, ingested_days_ago=1)
    assert find_prunable_sessions([s], now=NOW, dead_after=timedelta(days=7)) == [s]
    assert find_prunable_sessions([s], now=NOW, dead_after=timedelta(days=30)) == []


def test_mixed_population_only_prunes_the_eligible_ones() -> None:
    dead = _s("dead2", mtime_days_ago=90, ingested_days_ago=1)
    live = _s("live2", mtime_days_ago=1, ingested_days_ago=1)
    stale_ingest = _s("stale2", mtime_days_ago=90, ingested_days_ago=120)
    gone = _s("gone2", mtime_days_ago=None, ingested_days_ago=1)
    plan = find_prunable_sessions([dead, live, stale_ingest, gone], now=NOW)
    assert plan == [dead]
