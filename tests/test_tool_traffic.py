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


def test_record_tool_call_accumulates_count_and_ms_per_tool_and_caller() -> None:
    srv._record_tool_call("orient", "agent:thoth", 12.5)
    srv._record_tool_call("orient", "agent:thoth", 7.5)
    srv._record_tool_call("orient", "agent:seshat", 9.0)
    srv._record_tool_call("mount", "agent:thoth", 3.0)
    assert srv._tool_call_stats[("orient", "agent:thoth")] == {"count": 2, "total_ms": 20.0}
    assert srv._tool_call_stats[("orient", "agent:seshat")] == {"count": 1, "total_ms": 9.0}
    assert srv._tool_call_stats[("mount", "agent:thoth")] == {"count": 1, "total_ms": 3.0}


def test_record_tool_call_counts_a_failed_call_too() -> None:
    """The try/finally in BoundedMCP.call_tool times a raising call the same as a
    succeeding one — a counter that only saw successes would report the expensive/broken
    calls as cheap. This test proves the accumulator itself has no success-only bias;
    BoundedMCP.call_tool's own try/finally wiring is what actually guarantees the call."""
    srv._record_tool_call("dossier", "agent:thoth", 4.0)
    assert srv._tool_call_stats[("dossier", "agent:thoth")]["count"] == 1


def test_caller_for_is_cache_only_never_reattaches(monkeypatch: pytest.MonkeyPatch) -> None:
    """task #170: the hot path must never pay for a DB round trip just to attribute a
    telemetry row. An uncached connection is 'unattributed', not a reattach attempt."""
    from src.orchestrator.agents import AgentIdentity

    assert srv._caller_for(None) == "unattributed"

    class _FakeCtx:
        pass

    fake_ctx = _FakeCtx()
    monkeypatch.setattr(srv, "_conn_key", lambda ctx: "sid:test")
    # uncached — no entry in _agents for this key
    assert srv._caller_for(fake_ctx) == "unattributed"  # type: ignore[arg-type]

    srv._agents["sid:test"] = AgentIdentity(
        agent_id="agent:c38f8f3b-xxx", session="s", project=None, model=None, cwd=None)
    try:
        # the lineage ROOT, not the raw per-generation id — a seat's generations fold
        # to one caller (same discipline doors.py's _record uses)
        assert srv._caller_for(fake_ctx) == "agent:c38f8f3b"  # type: ignore[arg-type]
    finally:
        del srv._agents["sid:test"]


@pytest.mark.asyncio
async def test_flush_writes_the_batch_by_tool_and_caller_and_clears_the_live_dict(
    actions: Actions, _use_test_pool: None,
) -> None:
    srv._tool_stats_window_start = datetime.now(UTC) - timedelta(seconds=60)
    srv._record_tool_call("orient", "agent:thoth", 10.0)
    srv._record_tool_call("orient", "agent:thoth", 20.0)
    srv._record_tool_call("orient", "agent:seshat", 5.0)
    srv._record_tool_call("roster", "unattributed", 5.0)

    await srv._flush_tool_stats_once()

    assert srv._tool_call_stats == {}  # the live dict is empty again, ready for the next window
    rows = await actions.pool.fetch(
        "SELECT tool_name, caller, call_count, total_ms FROM mcp_tool_stats "
        "ORDER BY tool_name, caller")
    assert [dict(r) for r in rows] == [
        {"tool_name": "orient", "caller": "agent:seshat", "call_count": 1, "total_ms": 5.0},
        {"tool_name": "orient", "caller": "agent:thoth", "call_count": 2, "total_ms": 30.0},
        {"tool_name": "roster", "caller": "unattributed", "call_count": 1, "total_ms": 5.0},
    ]


@pytest.mark.asyncio
async def test_flush_of_an_empty_window_writes_nothing(
    actions: Actions, _use_test_pool: None,
) -> None:
    await srv._flush_tool_stats_once()
    assert await actions.pool.fetchval("SELECT count(*) FROM mcp_tool_stats") == 0


@pytest.mark.asyncio
async def test_tool_traffic_reports_both_cuts_persisted_and_live_plus_blind_spots(
    actions: Actions, _use_test_pool: None,
) -> None:
    await actions.pool.execute(
        "INSERT INTO mcp_tool_stats (tool_name, caller, window_start, window_end, "
        "call_count, total_ms) VALUES "
        "('orient', 'agent:thoth', now() - interval '30 seconds', now(), 2, 60.0), "
        "('orient', 'agent:seshat', now() - interval '30 seconds', now(), 1, 30.0)")
    srv._record_tool_call("mount", "agent:thoth", 4.0)  # still in the live, unflushed window

    out = await srv.tool_traffic(window_minutes=5)

    assert out["persisted"] == [
        {"tool": "orient", "calls": 3, "total_ms": 90.0, "avg_ms": 30.0}]
    assert sorted(out["persisted_by_caller"], key=lambda r: r["caller"]) == [
        {"caller": "agent:seshat", "calls": 1, "total_ms": 30.0, "avg_ms": 30.0},
        {"caller": "agent:thoth", "calls": 2, "total_ms": 60.0, "avg_ms": 30.0},
    ]
    assert out["current_unflushed_window"] == [
        {"tool": "mount", "calls": 1, "total_ms": 4.0, "avg_ms": 4.0}]
    assert out["current_unflushed_by_caller"] == [
        {"caller": "agent:thoth", "calls": 1, "total_ms": 4.0, "avg_ms": 4.0}]
    # rule 2 from msg 4034: the blind population lives IN the output, not only in a decision
    assert any("osiris-console" in s for s in out["blind_spots"])
    assert any("osiris-worker" in s for s in out["blind_spots"])
    assert any("osiris-pulse" in s for s in out["blind_spots"])
    assert any("osiris-manager" in s for s in out["blind_spots"])
    assert any("CACHE-ONLY" in s for s in out["blind_spots"])  # task #170's own named limit
    # #199 lane 2 (Thoth dispatch 6780/6793): the CLI itself bypasses MCP for several
    # tools (unmerge, stop, ...) — a zero reading on one of those is not evidence of
    # disuse, confessed here so the next reader of tool_traffic() sees it directly.
    assert any("THE CLI ITSELF" in s and "unmerge" in s for s in out["blind_spots"])
    # #203 (Seshat, 2026-09-03): the console (src/api/app.py) also bypasses this tool for
    # get_console, AND independently duplicates create_room's own SQL rather than calling
    # it — both confessed here, not left to a zero reading alone.
    assert any("get_console" in s and "app.py" in s for s in out["blind_spots"])
    assert any("duplicate implementation" in s and "create_room" in s for s in out["blind_spots"])
    assert "MCP tool calls" in out["measures"]
