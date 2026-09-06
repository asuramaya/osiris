"""`/diag/memory` — read-only memory diagnostics (thread 4746e7f4, operator "why osiris
uses so much ram" 2026-09-06). `_FakeRequest` mirrors test_heartbeat_route.py's own
pattern for exercising a `@mcp.custom_route` handler directly, no ASGI stack needed.
"""
from __future__ import annotations

import json

import pytest


class _FakeRequest:
    def __init__(self, query: dict[str, str] | None = None) -> None:
        self.query_params = query or {}


@pytest.fixture(autouse=True)
def _clean_tracemalloc():
    import tracemalloc

    if tracemalloc.is_tracing():
        tracemalloc.stop()
    yield
    if tracemalloc.is_tracing():
        tracemalloc.stop()


async def test_diag_memory_route_disabled_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    from src import mcp_server as srv

    monkeypatch.delenv("OSIRIS_MEMORY_DIAG_ENABLED", raising=False)
    out = await srv.diag_memory_route(_FakeRequest())
    assert out.status_code == 404
    payload = json.loads(out.body)
    assert "disabled" in payload["error"]


async def test_diag_memory_route_first_call_starts_tracing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src import mcp_server as srv

    monkeypatch.setenv("OSIRIS_MEMORY_DIAG_ENABLED", "1")
    out = await srv.diag_memory_route(_FakeRequest())
    payload = json.loads(out.body)
    assert payload["started"] is True
    assert "rss_kb" in payload and "swap_kb" in payload


async def test_diag_memory_route_second_call_reports_top_allocations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src import mcp_server as srv

    monkeypatch.setenv("OSIRIS_MEMORY_DIAG_ENABLED", "1")
    await srv.diag_memory_route(_FakeRequest())  # starts tracing
    _ = [object() for _ in range(1000)]  # something to allocate and find
    out = await srv.diag_memory_route(_FakeRequest())
    payload = json.loads(out.body)
    assert "started" not in payload
    assert isinstance(payload["top_allocations"], list)
    assert payload["top_allocations"]  # something was allocated since tracing started
    assert "tracemalloc_current_kb" in payload


async def test_diag_memory_route_reset_restarts_tracing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src import mcp_server as srv

    monkeypatch.setenv("OSIRIS_MEMORY_DIAG_ENABLED", "1")
    await srv.diag_memory_route(_FakeRequest())  # starts tracing
    await srv.diag_memory_route(_FakeRequest())  # a normal snapshot
    out = await srv.diag_memory_route(_FakeRequest({"reset": "1"}))
    payload = json.loads(out.body)
    assert payload["started"] is True  # reset re-baselines, same as a fresh start
