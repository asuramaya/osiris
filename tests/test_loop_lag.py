"""The loop-lag sampler names the code that blocks the event loop."""
from __future__ import annotations

import asyncio
import time

from src.orchestrator.loop_lag import LoopLag


def _blocking_wait(seconds: float) -> None:
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        pass


async def test_a_blocked_loop_is_reported_with_the_blocking_function() -> None:
    lag = LoopLag(threshold_ms=100, interval_ms=20, sample_ms=10)
    lag.start()
    try:
        await asyncio.sleep(0.1)  # heartbeat established
        _blocking_wait(0.5)       # the loop cannot tick for half a second
        await asyncio.sleep(0.2)  # let the watcher see it recover
    finally:
        lag.stop()
    snap = lag.snapshot()
    assert snap["stalls"] == 1
    assert 300 <= snap["worst_ms"] <= 900
    assert "_blocking_wait" in snap["top_sites"][0]["site"]
    assert snap["top_sites"][0]["blocked_ms"] >= 200


async def test_an_idle_loop_reports_nothing() -> None:
    lag = LoopLag(threshold_ms=100, interval_ms=20, sample_ms=10)
    lag.start()
    try:
        for _ in range(10):
            await asyncio.sleep(0.03)
    finally:
        lag.stop()
    assert lag.snapshot()["stalls"] == 0
