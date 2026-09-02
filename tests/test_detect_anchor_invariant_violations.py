"""THE ANCHOR INVARIANT detector (Thoth msg 6546), piece 1 — pure classification logic
against a fabricated pool, no live DB needed for the axis-separation behavior itself."""
from __future__ import annotations

from datetime import UTC, datetime

import pytest
from scripts.detect_anchor_invariant_violations import scan


class _FakePool:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    async def fetch(self, *args: object) -> list[dict]:
        return self._rows


def _row(seat: str, handle: str, value: str, source: str, at: datetime) -> dict:
    return {"seat": seat, "handle": handle, "id": len(value), "v": value,
            "source_id": source, "observed_at": at, "confidence": 0.9}


async def test_scan_flags_a_single_outside_root_anchor_without_multi_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OSIRIS_OFFICE_ROOT", "/fake/seats")
    pool = _FakePool([
        _row("seat:aaa", "Alice", "/other/place", "console", datetime(2026, 1, 1, tzinfo=UTC)),
    ])
    result = await scan(pool)  # type: ignore[arg-type]
    assert len(result["outside_root"]) == 1
    assert result["outside_root"][0]["seat"] == "seat:aaa"
    assert result["multi_current"] == []


async def test_scan_flags_multi_current_row_inside_root_separately(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OSIRIS_OFFICE_ROOT", "/fake/seats")
    pool = _FakePool([
        _row("seat:bbb", "Bob", "/fake/seats/bob", "console", datetime(2026, 1, 1, tzinfo=UTC)),
        _row("seat:bbb", "Bob", "/fake/seats/bob", "agent:x", datetime(2026, 1, 2, tzinfo=UTC)),
    ])
    result = await scan(pool)  # type: ignore[arg-type]
    assert result["outside_root"] == []
    assert len(result["multi_current"]) == 1


async def test_scan_reports_the_both_axes_target_population(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OSIRIS_OFFICE_ROOT", "/fake/seats")
    pool = _FakePool([
        _row("seat:ccc", "Carl", "/fake/seats/carl", "console", datetime(2026, 1, 1, tzinfo=UTC)),
        _row("seat:ccc", "Carl", "/other/repo", "agent:y", datetime(2026, 1, 2, tzinfo=UTC)),
    ])
    result = await scan(pool)  # type: ignore[arg-type]
    outside_seats = {r["seat"] for r in result["outside_root"]}
    multi_seats = {m["seat"] for m in result["multi_current"]}
    assert outside_seats == {"seat:ccc"}
    assert multi_seats == {"seat:ccc"}
