"""TOOL-CALL TELEMETRY (task #167, dispatch msg 4029/4034) — which MCP tool is expensive.
The hot path only ever touches an in-memory dict; a background task flushes it to
`mcp_tool_stats` every 60s, decoupled from any individual call. These tests exercise the
pure accumulation logic and the flush/read path directly — never BoundedMCP.call_tool
itself, which needs a live MCP session to invoke."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
import src.mcp_server as srv
from src.actions.core import Actions


@pytest.fixture(autouse=True)
def _clean_tool_stats(monkeypatch: pytest.MonkeyPatch) -> None:
    """Module globals are process-wide state — reset before AND after every test so one
    test's counts can never leak into the next (the same discipline `_seam_pcts.clear()`
    uses in test_seam_whisper.py)."""
    srv._tool_call_stats.clear()
    srv._tool_stats_window_start = None
    yield
    srv._tool_call_stats.clear()
    srv._tool_stats_window_start = None


@pytest.fixture
def _use_test_pool(actions: Actions, monkeypatch: pytest.MonkeyPatch) -> None:
    """`_flush_tool_stats_once`/`tool_traffic` reach the DB via the module's own lazy
    `_pool_get()` (the same pattern `_pool_get` itself uses for the real server) — swapped
    here for the test's isolated testcontainers pool instead of the real DATABASE_URL."""
    async def _fake_pool_get() -> object:
        return actions.pool

    monkeypatch.setattr(srv, "_pool_get", _fake_pool_get)


def test_record_tool_call_accumulates_count_and_ms() -> None:
    srv._record_tool_call("orient", 12.5)
    srv._record_tool_call("orient", 7.5)
    srv._record_tool_call("mount", 3.0)
    assert srv._tool_call_stats["orient"] == {"count": 2, "total_ms": 20.0}
    assert srv._tool_call_stats["mount"] == {"count": 1, "total_ms": 3.0}


def test_record_tool_call_counts_a_failed_call_too() -> None:
    """The try/finally in BoundedMCP.call_tool times a raising call the same as a
    succeeding one — a counter that only saw successes would report the expensive/broken
    calls as cheap. This test proves the accumulator itself has no success-only bias;
    BoundedMCP.call_tool's own try/finally wiring is what actually guarantees the call."""
    srv._record_tool_call("dossier", 4.0)
    assert srv._tool_call_stats["dossier"]["count"] == 1


@pytest.mark.asyncio
async def test_flush_writes_the_batch_and_clears_the_live_dict(
    actions: Actions, _use_test_pool: None,
) -> None:
    srv._tool_stats_window_start = datetime.now(UTC) - timedelta(seconds=60)
    srv._record_tool_call("orient", 10.0)
    srv._record_tool_call("orient", 20.0)
    srv._record_tool_call("roster", 5.0)

    await srv._flush_tool_stats_once()

    assert srv._tool_call_stats == {}  # the live dict is empty again, ready for the next window
    rows = await actions.pool.fetch(
        "SELECT tool_name, call_count, total_ms FROM mcp_tool_stats ORDER BY tool_name")
    assert [dict(r) for r in rows] == [
        {"tool_name": "orient", "call_count": 2, "total_ms": 30.0},
        {"tool_name": "roster", "call_count": 1, "total_ms": 5.0},
    ]


@pytest.mark.asyncio
async def test_flush_of_an_empty_window_writes_nothing(
    actions: Actions, _use_test_pool: None,
) -> None:
    await srv._flush_tool_stats_once()
    assert await actions.pool.fetchval("SELECT count(*) FROM mcp_tool_stats") == 0


@pytest.mark.asyncio
async def test_tool_traffic_reports_persisted_and_live_windows_plus_blind_spots(
    actions: Actions, _use_test_pool: None,
) -> None:
    await actions.pool.execute(
        "INSERT INTO mcp_tool_stats (tool_name, window_start, window_end, call_count, "
        "total_ms) VALUES ('orient', now() - interval '30 seconds', now(), 3, 90.0)")
    srv._record_tool_call("mount", 4.0)  # still in the live, unflushed window

    out = await srv.tool_traffic(window_minutes=5)

    assert out["persisted"] == [
        {"tool": "orient", "calls": 3, "total_ms": 90.0, "avg_ms": 30.0}]
    assert out["current_unflushed_window"] == [
        {"tool": "mount", "calls": 1, "total_ms": 4.0, "avg_ms": 4.0}]
    # rule 2 from msg 4034: the blind population lives IN the output, not only in a decision
    assert any("osiris-console" in s for s in out["blind_spots"])
    assert any("osiris-worker" in s for s in out["blind_spots"])
    assert any("osiris-pulse" in s for s in out["blind_spots"])
    assert any("osiris-manager" in s for s in out["blind_spots"])
    assert "MCP tool calls" in out["measures"]
