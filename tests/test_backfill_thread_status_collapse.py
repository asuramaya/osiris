"""Lane 1 backfill (Thoth msg 6435): pure classification logic, no DB needed — the refusal
and reopen-exclusion rules are the safety-critical part of
scripts/backfill_thread_status_collapse.py."""
from __future__ import annotations

from datetime import UTC, datetime

from scripts.backfill_thread_status_collapse import _classify


def _row(value: str, at: datetime) -> dict[str, object]:
    return {"v": value, "observed_at": at}


def test_classify_collapses_when_resolved_is_newest() -> None:
    rows = [
        _row("open", datetime(2026, 1, 1, tzinfo=UTC)),
        _row("resolved", datetime(2026, 1, 2, tzinfo=UTC)),
    ]
    assert _classify(rows) == "collapse"


def test_classify_excludes_a_genuine_reopen() -> None:
    rows = [
        _row("resolved", datetime(2026, 1, 1, tzinfo=UTC)),
        _row("open", datetime(2026, 1, 2, tzinfo=UTC)),
    ]
    assert _classify(rows) == "reopen"


def test_classify_refuses_a_third_status_value() -> None:
    rows = [
        _row("open", datetime(2026, 1, 1, tzinfo=UTC)),
        _row("retracted", datetime(2026, 1, 2, tzinfo=UTC)),
        _row("resolved", datetime(2026, 1, 3, tzinfo=UTC)),
    ]
    assert _classify(rows) == "unexpected"


def test_classify_refuses_a_tie_at_the_newest_timestamp() -> None:
    tied = datetime(2026, 1, 2, tzinfo=UTC)
    rows = [
        _row("open", datetime(2026, 1, 1, tzinfo=UTC)),
        _row("resolved", tied),
        _row("open", tied),
    ]
    assert _classify(rows) == "unexpected"


def test_classify_collapses_multi_witness_agreeing_on_resolved() -> None:
    rows = [
        _row("open", datetime(2026, 1, 1, tzinfo=UTC)),
        _row("resolved", datetime(2026, 1, 2, tzinfo=UTC)),
        _row("resolved", datetime(2026, 1, 3, tzinfo=UTC)),
    ]
    assert _classify(rows) == "collapse"
