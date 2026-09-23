"""TOOL-CALL TELEMETRY: which MCP tool is expensive.
The frequently-exercised path only ever touches an in-memory dict; a background task flushes it to
`mcp_tool_stats` every 60s, decoupled from any individual call. These tests exercise the
pure accumulation logic and the flush/read path directly, never BoundedMCP.call_tool
itself, which needs a live MCP session to invoke."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
import src.mcp_server as srv
from src.actions.core import Actions


@pytest.fixture(autouse=True)
def _clean_tool_stats(monkeypatch: pytest.MonkeyPatch) -> None:
    """Module globals are process-wide state, reset before AND after every test so one
    test's counts can never leak into the next (the same discipline `_seam_pcts.clear()`
    uses in test_seam_whisper.py). `_in_flight_calls` and `_watchdog_task` (THE STALL
    WATCHDOG) are the same class of module-global state, cleaned up
    the identical way, and any watchdog task a test started is cancelled here rather
    than left running past its own test."""
    srv._tool_call_stats.clear()
    srv._tool_stats_window_start = None
    srv._in_flight_calls.clear()
    yield
    srv._tool_call_stats.clear()
    srv._tool_stats_window_start = None
    srv._in_flight_calls.clear()
    if srv._watchdog_task is not None:
        srv._watchdog_task.cancel()
        srv._watchdog_task = None


@pytest.fixture
def _use_test_pool(actions: Actions, monkeypatch: pytest.MonkeyPatch) -> None:
    """`_flush_tool_stats_once`/`tool_traffic` reach the DB via the module's own lazy
    `_pool_get()` (the same pattern `_pool_get` itself uses for the real server), swapped
    here for the test's isolated testcontainers pool instead of the real DATABASE_URL."""
    async def _fake_pool_get() -> object:
        return actions.pool

    monkeypatch.setattr(srv, "_pool_get", _fake_pool_get)


def test_response_byte_size_measures_the_actual_json_wire_size() -> None:
    """A byte table needs a real number
    to read, not another live probe call. This must match len(json.dumps(...).encode())
    exactly: it's the same shape fn_metadata.convert_result serializes next."""
    import json

    payload = {"a": 1, "b": "x" * 50}
    assert srv._response_byte_size(payload) == len(json.dumps(payload).encode("utf-8"))


def test_response_byte_size_degrades_to_zero_on_an_unserializable_payload() -> None:
    """Best-effort, never a crash: telemetry must not be able to break a real response.
    `default=str` already rescues an ordinary custom object (which is the point: most
    real payloads still measure something useful); a circular reference is the genuine
    failure json can never serialize regardless of `default`, proving the fallback fires."""
    circular: dict[str, object] = {}
    circular["self"] = circular
    assert srv._response_byte_size(circular) == 0


def test_record_tool_call_accumulates_count_and_ms_per_tool_and_caller() -> None:
    srv._record_tool_call("orient", "agent:workerb", 12.5)
    srv._record_tool_call("orient", "agent:workerb", 7.5)
    srv._record_tool_call("orient", "agent:workera", 9.0)
    srv._record_tool_call("mount", "agent:workerb", 3.0)
    assert srv._tool_call_stats[("orient", "agent:workerb", "")] == {
        "count": 2, "total_ms": 20.0, "total_bytes": 0.0}
    assert srv._tool_call_stats[("orient", "agent:workera", "")] == {
        "count": 1, "total_ms": 9.0, "total_bytes": 0.0}
    assert srv._tool_call_stats[("mount", "agent:workerb", "")] == {
        "count": 1, "total_ms": 3.0, "total_bytes": 0.0}


def test_record_tool_call_counts_a_failed_call_too() -> None:
    """The try/finally in BoundedMCP.call_tool times a raising call the same as a
    succeeding one. A counter that only saw successes would report the expensive/broken
    calls as cheap. This test proves the accumulator itself has no success-only bias;
    BoundedMCP.call_tool's own try/finally wiring is what actually guarantees the call."""
    srv._record_tool_call("dossier", "agent:workerb", 4.0)
    assert srv._tool_call_stats[("dossier", "agent:workerb", "")]["count"] == 1


def test_record_tool_call_keeps_action_a_separate_dimension() -> None:
    """An object-type dispatcher's own bare tool_name
    must never collapse every action into one bucket: (tool, caller, action) is three
    independent axes, not two plus a label. Ordinary, non-dispatcher calls default to
    action='' and stay exactly as before (proven by the two tests above, unchanged)."""
    srv._record_tool_call("seat", "agent:workerb", 5.0, "mint")
    srv._record_tool_call("seat", "agent:workerb", 3.0, "stop")
    srv._record_tool_call("seat", "agent:workerb", 2.0, "mint")
    assert srv._tool_call_stats[("seat", "agent:workerb", "mint")] == {
        "count": 2, "total_ms": 7.0, "total_bytes": 0.0}
    assert srv._tool_call_stats[("seat", "agent:workerb", "stop")] == {
        "count": 1, "total_ms": 3.0, "total_bytes": 0.0}


def test_caller_for_is_cache_only_never_reattaches(monkeypatch: pytest.MonkeyPatch) -> None:
    """task #170: the hot path must never pay for a DB round trip just to attribute a
    telemetry row. An uncached connection is 'unattributed', not a reattach attempt."""
    from src.orchestrator.agents import AgentIdentity

    assert srv._caller_for(None) == "unattributed"

    class _FakeCtx:
        pass

    fake_ctx = _FakeCtx()
    monkeypatch.setattr(srv, "_conn_key", lambda ctx: "sid:test")
    # uncached: no entry in _agents for this key
    assert srv._caller_for(fake_ctx) == "unattributed"  # type: ignore[arg-type]

    srv._agents["sid:test"] = AgentIdentity(
        agent_id="agent:c38f8f3b-xxx", session="s", project=None, model=None, cwd=None)
    try:
        # the lineage ROOT, not the raw per-generation id: a seat's generations fold
        # to one caller (same discipline doors.py's _record uses)
        assert srv._caller_for(fake_ctx) == "agent:c38f8f3b"  # type: ignore[arg-type]
    finally:
        del srv._agents["sid:test"]


@pytest.mark.asyncio
async def test_flush_writes_the_batch_by_tool_and_caller_and_clears_the_live_dict(
    actions: Actions, _use_test_pool: None,
) -> None:
    srv._tool_stats_window_start = datetime.now(UTC) - timedelta(seconds=60)
    srv._record_tool_call("orient", "agent:workerb", 10.0, response_bytes=100)
    srv._record_tool_call("orient", "agent:workerb", 20.0, response_bytes=200)
    srv._record_tool_call("orient", "agent:workera", 5.0, response_bytes=50)
    srv._record_tool_call("roster", "unattributed", 5.0, response_bytes=30)

    await srv._flush_tool_stats_once()

    assert srv._tool_call_stats == {}  # the live dict is empty again, ready for the next window
    rows = await actions.pool.fetch(
        "SELECT tool_name, caller, call_count, total_ms, response_bytes FROM mcp_tool_stats "
        "ORDER BY tool_name, caller")
    assert [dict(r) for r in rows] == [
        {"tool_name": "orient", "caller": "agent:workera", "call_count": 1, "total_ms": 5.0,
         "response_bytes": 50},
        {"tool_name": "orient", "caller": "agent:workerb", "call_count": 2, "total_ms": 30.0,
         "response_bytes": 300},
        {"tool_name": "roster", "caller": "unattributed", "call_count": 1, "total_ms": 5.0,
         "response_bytes": 30},
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
        "call_count, total_ms, response_bytes) VALUES "
        "('orient', 'agent:workerb', now() - interval '30 seconds', now(), 2, 60.0, 2000), "
        "('orient', 'agent:workera', now() - interval '30 seconds', now(), 1, 30.0, 1000)")
    srv._record_tool_call("mount", "agent:workerb", 4.0, response_bytes=40)  # still unflushed

    out = await srv.tool_traffic(window_minutes=5)

    assert out["persisted"] == [
        {"tool": "orient", "calls": 3, "total_ms": 90.0, "avg_ms": 30.0,
         "total_bytes": 3000, "avg_bytes": 1000.0}]
    assert sorted(out["persisted_by_caller"], key=lambda r: r["caller"]) == [
        {"caller": "agent:workera", "calls": 1, "total_ms": 30.0, "avg_ms": 30.0,
         "total_bytes": 1000, "avg_bytes": 1000.0},
        {"caller": "agent:workerb", "calls": 2, "total_ms": 60.0, "avg_ms": 30.0,
         "total_bytes": 2000, "avg_bytes": 1000.0},
    ]
    assert out["current_unflushed_window"] == [
        {"tool": "mount", "calls": 1, "total_ms": 4.0, "avg_ms": 4.0,
         "total_bytes": 40, "avg_bytes": 40.0}]
    assert out["current_unflushed_by_caller"] == [
        {"caller": "agent:workerb", "calls": 1, "total_ms": 4.0, "avg_ms": 4.0,
         "total_bytes": 40, "avg_bytes": 40.0}]
    # the blind population lives IN the output, not only in a decision
    assert any("osiris-console" in s for s in out["blind_spots"])
    assert any("osiris-worker" in s for s in out["blind_spots"])
    assert any("osiris-pulse" in s for s in out["blind_spots"])
    assert any("osiris-manager" in s for s in out["blind_spots"])
    assert any("CACHE-ONLY" in s for s in out["blind_spots"])  # task #170's own named limit
    # the CLI itself bypasses MCP for several
    # tools (unmerge, stop, ...); a zero reading on one of those is not evidence of
    # disuse, confessed here so the next reader of tool_traffic() sees it directly.
    assert any("THE CLI ITSELF" in s and "unmerge" in s for s in out["blind_spots"])
    # the console (src/api/app.py) also bypasses this tool for
    # get_console, confessed here, not left to a zero reading alone. (The sibling
    # create_room duplicate-SQL confession this line used to pair with was retired
    # alongside the MCP tool itself, after a room-creation code path deletion; app.py now
    # calls the one shared orchestrator function, no duplicate left to confess.)
    assert any("get_console" in s and "app.py" in s for s in out["blind_spots"])
    assert "MCP tool calls" in out["measures"]


@pytest.mark.asyncio
async def test_tool_traffic_persisted_total_bytes_is_a_real_int_not_a_decimal(
    actions: Actions, _use_test_pool: None,
) -> None:
    """FOUND LIVE (first real 24h read against production after the response_bytes
    deploy): `response_bytes` is a `bigint` column, and Postgres's own SUM(bigint) rule
    ALWAYS promotes to `numeric` regardless of the actual values. asyncpg decodes that
    as a `Decimal`, which json.dumps renders as a STRING in the real MCP wire response.
    A bare `== 3000` equality assertion does NOT catch this (Decimal('3000') == 3000 is
    True in Python), which is exactly why the prior tests all stayed green through the
    bug. Assert the actual type, not just the value."""
    await actions.pool.execute(
        "INSERT INTO mcp_tool_stats (tool_name, caller, window_start, window_end, "
        "call_count, total_ms, response_bytes) VALUES "
        "('orient', 'agent:workerb', now() - interval '30 seconds', now(), 2, 60.0, 2000)")

    out = await srv.tool_traffic(window_minutes=5)

    row = out["persisted"][0]
    assert type(row["total_bytes"]) is int, (
        f"total_bytes came back as {type(row['total_bytes'])}, not int, it will render "
        "as a JSON string over the wire, not a number")
    assert type(row["avg_bytes"]) is float


@pytest.mark.asyncio
async def test_tool_traffic_breaks_a_dispatcher_down_by_action(
    actions: Actions, _use_test_pool: None,
) -> None:
    """`seat` alone folds 30 actions into one tool_name:
    persisted_by_action/current_unflushed_by_action are the cut that keeps attribution
    at the real, per-verb grain under a dispatcher, exactly the way persisted_by_caller
    already does for WHO instead of WHAT."""
    await actions.pool.execute(
        "INSERT INTO mcp_tool_stats (tool_name, caller, action, window_start, "
        "window_end, call_count, total_ms, response_bytes) VALUES "
        "('seat', 'agent:workerb', 'mint', now() - interval '30 seconds', now(), 3, 90.0, 300), "
        "('seat', 'agent:workerb', 'stop', now() - interval '30 seconds', now(), 1, 5.0, 50), "
        "('orient', 'agent:workerb', '', now() - interval '30 seconds', now(), 2, 20.0, 200)")
    srv._record_tool_call("seat", "agent:workera", 6.0, "mint", response_bytes=60)

    out = await srv.tool_traffic(window_minutes=5)

    assert out["persisted_by_action"] == [
        {"tool": "seat", "action": "mint", "calls": 3, "total_ms": 90.0, "avg_ms": 30.0,
         "total_bytes": 300, "avg_bytes": 100.0},
        {"tool": "seat", "action": "stop", "calls": 1, "total_ms": 5.0, "avg_ms": 5.0,
         "total_bytes": 50, "avg_bytes": 50.0},
    ]  # 'orient' with action='' is excluded, not a dispatcher call
    assert out["current_unflushed_by_action"] == [
        {"tool": "seat", "action": "mint", "calls": 1, "total_ms": 6.0, "avg_ms": 6.0,
         "total_bytes": 60, "avg_bytes": 60.0}]


@pytest.mark.asyncio
async def test_tool_traffic_alias_decay_instrument_reads_the_absorbed_action_not_the_bare_name(
    actions: Actions, _use_test_pool: None,
) -> None:
    """THE ALIAS-DECAY RULE (an alias is removed only at zero traffic) would
    misfire the moment a fold lands: mint_seat's own bare tool_name reads permanently
    zero after the seat dispatcher shipped, because every real caller now goes
    through seat(action='mint') instead. A naive zero-traffic reading on the alias
    alone would wrongly call it dead. This instrument reads BOTH halves: the alias's own
    (expected-zero) traffic and the dispatcher action that actually absorbed it, and
    only calls a name eligible for removal when both are genuinely zero."""
    await actions.pool.execute(
        "INSERT INTO mcp_tool_stats (tool_name, caller, action, window_start, "
        "window_end, call_count, total_ms) VALUES "
        "('seat', 'agent:workerb', 'mint', now() - interval '30 seconds', now(), 4, 40.0)")

    out = await srv.tool_traffic(window_minutes=5)
    by_alias = {r["alias"]: r for r in out["retired_alias_traffic"]}

    mint = by_alias["mint_seat"]
    assert mint["own_name_calls"] == 0  # the alias itself is never called directly anymore
    assert mint["absorbed_into"] == "seat(action='mint')"
    assert mint["absorbed_action_calls"] == 4  # real usage, just under the new name
    assert mint["eligible_for_removal"] is False  # real traffic exists, just relocated

    stop = by_alias["stop"]  # never called at all, either as itself or as an action
    assert stop["own_name_calls"] == 0
    assert stop["absorbed_action_calls"] == 0
    assert stop["eligible_for_removal"] is True


# --- THE STALL WATCHDOG (THE OSIRIS-MCP MAIN-THREAD STALL incident): in-flight ---------
# --- visibility + a background task that logs every thread's own stack the moment ------
# --- a call passes the stall threshold --------------------------------------------------

def test_log_all_thread_stacks_logs_every_live_thread() -> None:
    class _FakeLog:
        def __init__(self) -> None:
            self.messages: list[str] = []

        def warning(self, msg: str) -> None:
            self.messages.append(msg)

    log = _FakeLog()
    srv._log_all_thread_stacks(log, reason="test reason 12345")
    assert len(log.messages) == 1
    assert "test reason 12345" in log.messages[0]
    assert "live thread" in log.messages[0]
    assert "thread " in log.messages[0]  # at least one "--- thread <id> ---" section


@pytest.mark.asyncio
async def test_watchdog_logs_once_when_a_call_crosses_the_threshold(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    import asyncio
    import contextlib
    import logging
    import time

    monkeypatch.setattr(srv, "_WATCHDOG_POLL_INTERVAL_S", 0.01)
    monkeypatch.setattr(srv, "_WATCHDOG_STALL_THRESHOLD_S", 0.02)
    monkeypatch.setattr(srv, "_WATCHDOG_REPEAT_INTERVAL_S", 60.0)  # no repeat within this test
    call_id = next(srv._in_flight_next_id)
    started = time.monotonic() - 1.0
    srv._in_flight_calls[call_id] = {
        "tool": "search", "caller": "agent:workerb", "started_at": started,
        "next_log_at": started + 0.02,
    }

    task = asyncio.create_task(srv._watchdog_loop())
    try:
        with caplog.at_level(logging.WARNING, logger="osiris.mcp.watchdog"):
            await asyncio.sleep(0.08)  # several poll intervals, proves ONCE here
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    slow_records = [r for r in caplog.records if "SLOW TOOL CALL" in r.message]
    assert len(slow_records) == 1  # not re-fired again within the (long) repeat interval
    assert "'search'" in slow_records[0].message
    assert "agent:workerb" in slow_records[0].message


@pytest.mark.asyncio
async def test_watchdog_logs_again_every_repeat_interval_while_the_call_stays_in_flight(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    """The fuller spec: when any tool call passes 10s, then every
    30s while it runs. A call still running well past the first log must keep
    reminding the journal, not go silent after one warning."""
    import asyncio
    import contextlib
    import logging
    import time

    monkeypatch.setattr(srv, "_WATCHDOG_POLL_INTERVAL_S", 0.01)
    monkeypatch.setattr(srv, "_WATCHDOG_STALL_THRESHOLD_S", 0.02)
    monkeypatch.setattr(srv, "_WATCHDOG_REPEAT_INTERVAL_S", 0.03)
    call_id = next(srv._in_flight_next_id)
    started = time.monotonic()
    srv._in_flight_calls[call_id] = {
        "tool": "search", "caller": "agent:workerb", "started_at": started,
        "next_log_at": started + 0.02,
    }

    task = asyncio.create_task(srv._watchdog_loop())
    try:
        with caplog.at_level(logging.WARNING, logger="osiris.mcp.watchdog"):
            await asyncio.sleep(0.15)  # several repeat intervals, call never removed
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    slow_records = [r for r in caplog.records if "SLOW TOOL CALL" in r.message]
    assert len(slow_records) >= 2  # the first threshold hit, plus at least one repeat


@pytest.mark.asyncio
async def test_watchdog_never_logs_a_call_under_the_threshold(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    import asyncio
    import contextlib
    import logging
    import time

    monkeypatch.setattr(srv, "_WATCHDOG_POLL_INTERVAL_S", 0.01)
    monkeypatch.setattr(srv, "_WATCHDOG_STALL_THRESHOLD_S", 5.0)
    call_id = next(srv._in_flight_next_id)
    started = time.monotonic()
    srv._in_flight_calls[call_id] = {
        "tool": "search", "caller": "agent:workerb", "started_at": started,
        "next_log_at": started + 5.0,
    }

    task = asyncio.create_task(srv._watchdog_loop())
    try:
        with caplog.at_level(logging.WARNING, logger="osiris.mcp.watchdog"):
            await asyncio.sleep(0.05)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    assert not any("SLOW TOOL CALL" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_ensure_watchdog_task_is_idempotent() -> None:
    srv._ensure_watchdog_task()
    first = srv._watchdog_task
    assert first is not None and not first.done()
    srv._ensure_watchdog_task()
    assert srv._watchdog_task is first  # no second task spawned while one is live
    first.cancel()


@pytest.mark.asyncio
async def test_tool_traffic_carries_the_in_flight_list(
    actions: Actions, _use_test_pool: None,
) -> None:
    """tool_traffic() is itself an in-flight call by the time its own body runs
    (BoundedMCP.call_tool registers the entry before the tool body starts). This test
    calls `srv.tool_traffic` directly (never through BoundedMCP), so no such entry
    exists here; only the synthetic ones this test seeds are expected."""
    import time

    fresh_id = next(srv._in_flight_next_id)
    stale_id = next(srv._in_flight_next_id)
    fresh_started = time.monotonic()
    stale_started = time.monotonic() - 42.0
    srv._in_flight_calls[fresh_id] = {
        "tool": "mount", "caller": "agent:workerb", "started_at": fresh_started,
        "next_log_at": fresh_started + srv._WATCHDOG_STALL_THRESHOLD_S}
    srv._in_flight_calls[stale_id] = {
        "tool": "search", "caller": "agent:workera", "started_at": stale_started,
        "next_log_at": stale_started + srv._WATCHDOG_REPEAT_INTERVAL_S}

    out = await srv.tool_traffic(window_minutes=5)

    by_id = {r["call_id"]: r for r in out["in_flight"]}
    assert by_id[fresh_id]["tool"] == "mount"
    assert by_id[fresh_id]["caller"] == "agent:workerb"
    assert by_id[fresh_id]["elapsed_secs"] < 1.0
    assert by_id[stale_id]["elapsed_secs"] >= 42.0
    # sorted longest-in-flight first: the row an operator actually needs to see
    assert out["in_flight"][0]["call_id"] == stale_id


@pytest.mark.asyncio
async def test_watchdog_proves_itself_against_a_synthetic_blocking_tool(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    """THE WATCHDOG PROVING ITSELF: the
    watchdog task and a "tool call" task genuinely running CONCURRENTLY (two real
    asyncio tasks racing, not one test pre-seeding a stale timestamp and checking it
    once). The tool task mirrors BoundedMCP.call_tool's own in-flight register/pop
    shape exactly (mirrors, not calls: BoundedMCP.call_tool itself needs a live MCP
    client session's own request context several calls deep, `_conn_key`'s own
    `ctx.request_context.request`, confirmed live: calling it directly from a bare
    unit test raises `ValueError: Context is not available outside of a request`
    before ever reaching the watchdog logic this test wants to exercise, hence
    mirroring the shape rather than fighting that boundary).

    The original spec names a 12s blocking tool; this test scales every timing constant
    down together (poll/threshold/tool-duration all shrunk by the same factor,
    preserving "the tool call comfortably outlives the stall threshold") so the suite
    doesn't spend 12 real seconds proving it. The RATIO under test, not the literal
    12s, is what the mechanism cares about.

    NAMED LIMITATION, not silently glossed over: this synthetic tool `await`s
    (`asyncio.sleep`), which cooperatively yields the event loop back to the watchdog
    task, the shape a call doing many small awaited DB round-trips has (arguably the
    incident's own early "crawled" phase, 45-180s calls that were still eventually
    answering). A GENUINELY blocking synchronous call (the incident's actual later
    phase, a `path.read_text()` call on the loop thread, never awaited) freezes
    the ENTIRE event loop, including this watchdog's own polling task, so it could
    never fire mid-block by construction: no in-process asyncio watchdog can preempt
    a truly synchronous stall. That is exactly why faulthandler.register(SIGUSR1) (an
    OS SIGNAL, not a coroutine) is the other, non-optional half of this approach: it is
    the one mechanism that still works when this one structurally cannot."""
    import asyncio
    import contextlib
    import logging
    import time

    monkeypatch.setattr(srv, "_WATCHDOG_POLL_INTERVAL_S", 0.01)
    monkeypatch.setattr(srv, "_WATCHDOG_STALL_THRESHOLD_S", 0.05)
    monkeypatch.setattr(srv, "_WATCHDOG_REPEAT_INTERVAL_S", 60.0)

    tool_name = "test_synthetic_slow_tool"

    async def _tool_call() -> dict[str, str]:
        """Mirrors BoundedMCP.call_tool's own in-flight register/pop shape (mcp_server.py,
        the same block, minus the ctx-dependent caller resolution this test has no live
        session to supply)."""
        call_id = next(srv._in_flight_next_id)
        t0 = time.monotonic()
        srv._in_flight_calls[call_id] = {
            "tool": tool_name, "caller": "agent:test", "started_at": t0,
            "next_log_at": t0 + srv._WATCHDOG_STALL_THRESHOLD_S,
        }
        try:
            await asyncio.sleep(0.15)  # scaled-down stand-in for the dispatched "12s"
            return {"ok": True}
        finally:
            srv._in_flight_calls.pop(call_id, None)

    watchdog = asyncio.create_task(srv._watchdog_loop())
    try:
        with caplog.at_level(logging.WARNING, logger="osiris.mcp.watchdog"):
            result = await _tool_call()
    finally:
        watchdog.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await watchdog

    assert result == {"ok": True}  # the call itself still completes and returns cleanly
    slow_records = [r for r in caplog.records if "SLOW TOOL CALL" in r.message]
    assert slow_records, "the watchdog never fired for a call that ran well past its own threshold"
    assert tool_name in slow_records[0].message


@pytest.mark.asyncio
async def test_tool_traffic_in_flight_is_empty_with_nothing_running(
    actions: Actions, _use_test_pool: None,
) -> None:
    out = await srv.tool_traffic(window_minutes=5)
    assert out["in_flight"] == []
