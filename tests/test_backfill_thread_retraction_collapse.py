"""Wave 3 Lane A (Thoth msg 6503): pure classification logic, no DB needed."""
from __future__ import annotations

from datetime import UTC, datetime

from scripts.backfill_thread_retraction_collapse import _classify


def _row(value: str, at: datetime) -> dict[str, object]:
    return {"v": value, "observed_at": at}


def test_classify_collapses_the_plain_open_retracted_shape() -> None:
    rows = [
        _row("open", datetime(2026, 1, 1, tzinfo=UTC)),
        _row("retracted", datetime(2026, 1, 2, tzinfo=UTC)),
    ]
    assert _classify(rows) == "collapse"


def test_classify_refuses_open_newest() -> None:
    rows = [
        _row("retracted", datetime(2026, 1, 1, tzinfo=UTC)),
        _row("open", datetime(2026, 1, 2, tzinfo=UTC)),
    ]
    assert _classify(rows) == "unexpected"


def test_classify_refuses_a_third_value() -> None:
    rows = [
        _row("open", datetime(2026, 1, 1, tzinfo=UTC)),
        _row("retracted", datetime(2026, 1, 2, tzinfo=UTC)),
        _row("resolved", datetime(2026, 1, 3, tzinfo=UTC)),
    ]
    assert _classify(rows) == "unexpected"


def test_classify_refuses_a_tie() -> None:
    tied = datetime(2026, 1, 2, tzinfo=UTC)
    rows = [_row("open", tied), _row("retracted", tied)]
    assert _classify(rows) == "unexpected"


def test_classify_refuses_more_than_two_rows() -> None:
    rows = [
        _row("open", datetime(2026, 1, 1, tzinfo=UTC)),
        _row("retracted", datetime(2026, 1, 2, tzinfo=UTC)),
        _row("retracted", datetime(2026, 1, 3, tzinfo=UTC)),
    ]
    assert _classify(rows) == "unexpected"
