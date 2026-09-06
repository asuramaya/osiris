"""`/diag/memory` — read-only, BOUNDED memory diagnostics (thread 4746e7f4, operator "why
osiris uses so much ram" 2026-09-06). `_FakeRequest` mirrors test_heartbeat_route.py's own
pattern for exercising a `@mcp.custom_route` handler directly, no ASGI stack needed.

THE REDESIGN (same thread, ~23:20Z the same night): the FIRST version had no bound at
all — tracemalloc(25) traced every allocation on the live server indefinitely; its own
bookkeeping alone peaked over 1 GB within minutes, the event loop starved, /heartbeat
timed out fleet-wide, SIGTERM did not stop it, only SIGKILL did. These tests prove the
new safety rails directly: a bounded window (auto-stops on its own, no poller required),
5 frames not 25, a refusal to start a second concurrent window, and a refusal to start
at all when RSS is already high.
"""
from __future__ import annotations

import asyncio
import json

import pytest


class _FakeRequest:
    def __init__(self, query: dict[str, str] | None = None) -> None:
        self.query_params = query or {}


@pytest.fixture(autouse=True)
async def _clean_tracemalloc():
    import tracemalloc

    from src import mcp_server as srv

    if tracemalloc.is_tracing():
        tracemalloc.stop()
    task = srv._diag_window.get("task")
    if task is not None:
        task.cancel()
    srv._diag_window["task"] = None
    srv._diag_window["started_at"] = None
    yield
    if tracemalloc.is_tracing():
        tracemalloc.stop()
    task = srv._diag_window.get("task")
    if task is not None:
        task.cancel()
    srv._diag_window["task"] = None
    srv._diag_window["started_at"] = None


async def test_diag_memory_route_disabled_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    from src import mcp_server as srv

    monkeypatch.delenv("OSIRIS_MEMORY_DIAG_ENABLED", raising=False)
    out = await srv.diag_memory_route(_FakeRequest())
    assert out.status_code == 404
    payload = json.loads(out.body)
    assert "disabled" in payload["error"]


async def test_diag_memory_route_first_call_starts_a_bounded_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src import mcp_server as srv

    monkeypatch.setenv("OSIRIS_MEMORY_DIAG_ENABLED", "1")
    out = await srv.diag_memory_route(_FakeRequest())
    payload = json.loads(out.body)
    assert payload["started"] is True
    assert "rss_kb" in payload and "swap_kb" in payload
    assert payload["window_s"] == srv._MEMORY_DIAG_WINDOW_S
    assert srv._diag_window["task"] is not None  # the guard task is armed


async def test_diag_memory_route_second_call_never_restarts_the_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The whole point: a second call while a window is already running must NEVER
    restart tracing — there is no ?reset=1 anymore. It just reports the live snapshot."""
    from src import mcp_server as srv

    monkeypatch.setenv("OSIRIS_MEMORY_DIAG_ENABLED", "1")
    first = json.loads((await srv.diag_memory_route(_FakeRequest())).body)
    guard_task = srv._diag_window["task"]
    _ = [object() for _ in range(1000)]  # something to allocate and find
    out = await srv.diag_memory_route(_FakeRequest())
    payload = json.loads(out.body)
    assert "started" not in payload
    assert isinstance(payload["top_allocations"], list)
    assert len(payload["top_allocations"]) <= srv._MEMORY_DIAG_MAX_FRAMES
    assert payload["top_allocations"]
    assert "tracemalloc_current_kb" in payload
    assert payload["window_remaining_s"] is not None
    assert payload["window_remaining_s"] <= first["window_s"]
    assert srv._diag_window["task"] is guard_task  # the SAME guard task, never replaced


async def test_diag_memory_route_stop_ends_the_window_early(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import tracemalloc

    from src import mcp_server as srv

    monkeypatch.setenv("OSIRIS_MEMORY_DIAG_ENABLED", "1")
    await srv.diag_memory_route(_FakeRequest())
    out = await srv.diag_memory_route(_FakeRequest({"stop": "1"}))
    payload = json.loads(out.body)
    assert payload["stopped"] is True
    assert not tracemalloc.is_tracing()
    assert srv._diag_window["task"] is None

    # a fresh call after ?stop=1 starts a genuinely NEW window, not a continuation
    again = json.loads((await srv.diag_memory_route(_FakeRequest())).body)
    assert again["started"] is True


async def test_diag_memory_route_refuses_to_start_over_the_rss_line(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src import mcp_server as srv

    monkeypatch.setenv("OSIRIS_MEMORY_DIAG_ENABLED", "1")
    monkeypatch.setattr(
        srv, "_proc_mem_kb",
        lambda: {"rss_kb": srv._MEMORY_DIAG_RSS_REFUSE_KB + 1, "swap_kb": 0})
    out = await srv.diag_memory_route(_FakeRequest())
    assert out.status_code == 409
    payload = json.loads(out.body)
    assert "refused" in payload["error"]
    assert "started" not in payload
    import tracemalloc
    assert not tracemalloc.is_tracing()  # never even started


async def test_diag_window_guard_auto_stops_after_its_hard_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """THE ACTUAL FIX for the incident: the window ends ON ITS OWN, with nobody polling
    — a background task, not a poller's own continued cooperation."""
    import tracemalloc

    from src import mcp_server as srv

    monkeypatch.setattr(srv, "_MEMORY_DIAG_CHECK_INTERVAL_S", 0.01)
    tracemalloc.start(srv._MEMORY_DIAG_MAX_FRAMES)
    task = asyncio.create_task(srv._diag_window_guard(-1.0))  # deadline already past
    await asyncio.wait_for(task, timeout=2.0)
    assert not tracemalloc.is_tracing()
    assert srv._diag_window["task"] is None


async def test_diag_window_guard_aborts_early_on_the_rss_tripwire(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The OTHER half: even inside a still-valid duration window, a live RSS breach ends
    tracing immediately rather than waiting out the full 300s — tracemalloc's own
    overhead is worst exactly when memory is already tight."""
    import time
    import tracemalloc

    from src import mcp_server as srv

    monkeypatch.setattr(srv, "_MEMORY_DIAG_CHECK_INTERVAL_S", 0.01)
    monkeypatch.setattr(
        srv, "_proc_mem_kb",
        lambda: {"rss_kb": srv._MEMORY_DIAG_RSS_REFUSE_KB + 1, "swap_kb": 0})
    tracemalloc.start(srv._MEMORY_DIAG_MAX_FRAMES)
    far_future_deadline = time.monotonic() + 300.0  # the hard cap has NOT elapsed
    task = asyncio.create_task(srv._diag_window_guard(far_future_deadline))
    await asyncio.wait_for(task, timeout=2.0)  # must not wait out the full 300s
    assert not tracemalloc.is_tracing()
