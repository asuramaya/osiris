"""The telemetry reader (neo's second instrument, task #35): retained-events files
eaten into harness_telemetry — normalized columns only, spend-gated per file, the raw
payload never duplicated. Real Postgres via the actions fixture."""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest_asyncio
from src.actions.core import Actions
from src.ingest.telemetry import TelemetryStore, _rows_of_file


def _write_events(path: Path, events: list[dict], *, mtime: int = 1_000_000_000) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps({"event_type": "ClaudeCodeInternalEvent", "event_data": e})
             for e in events]
    lines.insert(1, "not json at all")  # junk lines are skipped, never fatal
    path.write_text("\n".join(lines) + "\n")
    os.utime(path, times=(mtime, mtime))
    return path


_EVENTS = [
    {"event_name": "tengu_api_success", "client_timestamp": "2026-07-03T04:20:00.927Z",
     "session_id": "06a325a9-x", "device_id": "dev-1",
     "model": "claude-haiku-4-5-20251001",
     "env": {"version": "2.1.199", "platform": "linux", "arch": "x64"}},
    {"event_name": "tengu_api_error", "client_timestamp": "2026-07-04T05:00:00.000Z",
     "session_id": "0a750357-y", "device_id": "dev-1"},
]


def test_rows_of_file_normalizes_and_skips_junk(tmp_path: Path) -> None:
    p = _write_events(tmp_path / "1p_failed_events.a.b.json", _EVENTS)
    rows = _rows_of_file(p)
    assert len(rows) == 2
    ref, ts, event, sid, _parent, dev, ver, model, plat, arch = rows[0]
    assert ref == "1p_failed_events.a.b.json:0"
    assert ts is not None and ts.year == 2026
    assert event == "tengu_api_success"
    assert sid == "06a325a9-x"
    assert dev == "dev-1"
    assert (ver, model, plat, arch) == (
        "2.1.199", "claude-haiku-4-5-20251001", "linux", "x64")
    # the second event has no env block — absent fields land as None, never ""
    assert rows[1][6] is None and rows[1][8] is None


@pytest_asyncio.fixture
async def tel(actions: Actions, tmp_path: Path) -> TelemetryStore:
    return TelemetryStore(actions.pool, root=tmp_path / "telemetry")


async def test_backfill_eats_and_the_spend_gate_holds(
    tel: TelemetryStore, tmp_path: Path,
) -> None:
    _write_events(tmp_path / "telemetry" / "1p_failed_events.a.b.json", _EVENTS)
    assert await tel.backfill() == 1
    n = await tel.pool.fetchval("SELECT count(*) FROM harness_telemetry")
    assert n == 2
    # unchanged file: one stat + one row lookup, no re-read, no new rows
    assert await tel.backfill() == 0
    assert await tel.pool.fetchval("SELECT count(*) FROM harness_telemetry") == 2
    # a grown file re-eats; ON CONFLICT keeps the old rows single
    _write_events(
        tmp_path / "telemetry" / "1p_failed_events.a.b.json",
        [*_EVENTS, {"event_name": "tengu_exit", "session_id": "06a325a9-x"}],
        mtime=2_000_000_000)
    assert await tel.backfill() == 1
    assert await tel.pool.fetchval("SELECT count(*) FROM harness_telemetry") == 3


async def test_summary_speaks_and_absence_is_none(
    tel: TelemetryStore, tmp_path: Path,
) -> None:
    assert await tel.summary() is None  # nothing eaten: absence, not zeros
    _write_events(tmp_path / "telemetry" / "1p_failed_events.a.b.json", _EVENTS)
    await tel.backfill()
    s = await tel.summary()
    assert s is not None
    assert s["events"] == 2
    assert s["sessions"] == 2
    assert s["devices"] == 1
    assert s["files"] == 1
    assert s["bytes"] > 0
    assert s["oldest"] is not None and str(s["oldest"])[:4] == "2026"
    assert {e["event"] for e in s["top_events"]} == {
        "tengu_api_success", "tengu_api_error"}


async def test_missing_root_is_a_quiet_zero(tel: TelemetryStore) -> None:
    assert await tel.backfill() == 0  # no ~/.claude/telemetry: nothing to measure
