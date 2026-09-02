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


def test_classify_collapses_open_retracted_resolved_when_resolved_is_newest() -> None:
    """Wave 2 ruling (msg 6482 item 2): a mind's later resolve outranks the janitor's
    earlier retraction of machine-authored mined noise — the exact shape read and ruled on
    for all 4 live specimens (thread:28124c00 et al., each carrying real self_declared
    resolve prose)."""
    rows = [
        _row("open", datetime(2026, 1, 1, tzinfo=UTC)),
        _row("retracted", datetime(2026, 1, 2, tzinfo=UTC)),
        _row("resolved", datetime(2026, 1, 3, tzinfo=UTC)),
    ]
    assert _classify(rows) == "collapse"


def test_classify_refuses_retracted_newest_alongside_resolved() -> None:
    """Never ruled on — 'retracted' outranking a 'resolved' witness is a different shape
    than the one actually read; refuse rather than guess."""
    rows = [
        _row("open", datetime(2026, 1, 1, tzinfo=UTC)),
        _row("resolved", datetime(2026, 1, 2, tzinfo=UTC)),
        _row("retracted", datetime(2026, 1, 3, tzinfo=UTC)),
    ]
    assert _classify(rows) == "unexpected"


def test_classify_refuses_open_newest_alongside_retracted_and_resolved() -> None:
    rows = [
        _row("resolved", datetime(2026, 1, 1, tzinfo=UTC)),
        _row("retracted", datetime(2026, 1, 2, tzinfo=UTC)),
        _row("open", datetime(2026, 1, 3, tzinfo=UTC)),
    ]
    assert _classify(rows) == "unexpected"


def test_classify_refuses_a_value_outside_the_known_set() -> None:
    rows = [
        _row("open", datetime(2026, 1, 1, tzinfo=UTC)),
        _row("weird", datetime(2026, 1, 2, tzinfo=UTC)),
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
