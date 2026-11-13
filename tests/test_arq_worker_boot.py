"""The worker boot spike: fifteen run_at_startup crons used to fire
concurrently in the same ~3s window, racing for CPU/memory during the single costliest
moment of the process's life. These tests prove the two new safety rails in isolation,
without a real Postgres/Redis-backed CascadeContext: `watched()`'s boot-serialization
lock (jobs queue one at a time only within the boot grace window, unlocked after), and
`_boot_memtrace`'s bounded tracemalloc window (5 frames, a hard cap, self-terminating).
"""
from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace
from typing import Any

import pytest
from src.workers import arq_worker


@pytest.fixture(autouse=True)
def _patch_record_job(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _noop(*args: object, **kwargs: object) -> None:
        return None

    monkeypatch.setattr(arq_worker, "record_job", _noop)


@pytest.fixture(autouse=True)
def _fresh_boot_lock(monkeypatch: pytest.MonkeyPatch) -> None:
    # the module lock binds to the first event loop that contends it; every test has its own loop
    monkeypatch.setattr(arq_worker, "_boot_lock", asyncio.Lock())


def _fake_ctx(*, boot_serialize_until: float | None) -> dict[str, Any]:
    return {
        "cascade": SimpleNamespace(actions=SimpleNamespace(pool=object())),
        "boot_serialize_until": boot_serialize_until,
    }


async def test_watched_serializes_concurrent_calls_inside_the_boot_window() -> None:
    order: list[str] = []

    async def slow_job(ctx: dict[str, Any]) -> int:
        order.append("start")
        await asyncio.sleep(0.05)
        order.append("end")
        return 1

    wrapped = arq_worker.watched(slow_job, every=60)
    ctx = _fake_ctx(boot_serialize_until=time.monotonic() + 3600.0)
    await asyncio.gather(wrapped(ctx), wrapped(ctx))
    # serialized: the first call's "end" lands before the second call's "start"
    assert order == ["start", "end", "start", "end"]


async def test_a_waiter_behind_a_long_holder_skips_instead_of_timing_out(
        monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(arq_worker, "_BOOT_LOCK_WAIT_S", 0.05)
    ran: list[str] = []

    async def long_job(ctx: dict[str, Any]) -> int:
        await asyncio.sleep(0.3)
        ran.append("long")
        return 1

    async def light_job(ctx: dict[str, Any]) -> int:
        ran.append("light")
        return 1

    long_w = arq_worker.watched(long_job, every=60)
    light_w = arq_worker.watched(light_job, every=60)
    ctx = _fake_ctx(boot_serialize_until=time.monotonic() + 3600.0)
    task = asyncio.create_task(long_w(ctx))
    await asyncio.sleep(0.01)
    assert await light_w(ctx) == 0  # gave up, no error
    assert await task == 1
    assert ran == ["long"]  # the light job never ran; it is not a failure


async def test_a_heavy_job_never_holds_the_boot_lock() -> None:
    order: list[str] = []

    async def heavy_job(ctx: dict[str, Any]) -> int:
        order.append("heavy")
        await asyncio.sleep(0.2)
        return 1

    async def light_job(ctx: dict[str, Any]) -> int:
        order.append("light")
        return 1

    heavy_w = arq_worker.watched(heavy_job, every=60, heavy=True)
    light_w = arq_worker.watched(light_job, every=60)
    ctx = _fake_ctx(boot_serialize_until=time.monotonic() + 0.1)
    heavy_t = asyncio.create_task(heavy_w(ctx))
    await asyncio.sleep(0.01)
    assert await light_w(ctx) == 1  # not queued behind the heavy job
    assert not arq_worker._boot_lock.locked()
    assert await heavy_t == 1  # ran once the window elapsed
    assert order == ["light", "heavy"]


async def test_watched_runs_concurrently_past_the_boot_window() -> None:
    order: list[str] = []

    async def slow_job(ctx: dict[str, Any]) -> int:
        order.append("start")
        await asyncio.sleep(0.05)
        order.append("end")
        return 1

    wrapped = arq_worker.watched(slow_job, every=60)
    ctx = _fake_ctx(boot_serialize_until=time.monotonic() - 1.0)  # already elapsed
    await asyncio.gather(wrapped(ctx), wrapped(ctx))
    # concurrent: both "start"s land before either "end"
    assert order == ["start", "start", "end", "end"]


async def test_watched_with_no_boot_deadline_never_serializes() -> None:
    """A ctx that never went through startup() (no boot_serialize_until key at all)
    must behave exactly like the past-deadline case: never blocked, never a KeyError."""
    order: list[str] = []

    async def slow_job(ctx: dict[str, Any]) -> int:
        order.append("start")
        await asyncio.sleep(0.05)
        order.append("end")
        return 1

    wrapped = arq_worker.watched(slow_job, every=60)
    ctx = _fake_ctx(boot_serialize_until=None)
    await asyncio.gather(wrapped(ctx), wrapped(ctx))
    assert order == ["start", "start", "end", "end"]


async def test_boot_memtrace_auto_stops_after_its_window_and_logs(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    import tracemalloc

    if tracemalloc.is_tracing():
        tracemalloc.stop()
    monkeypatch.setattr(arq_worker, "_WORKER_MEMTRACE_CHECK_INTERVAL_S", 0.01)
    with caplog.at_level("WARNING", logger="osiris.worker"):
        await arq_worker._boot_memtrace(0.03)
    assert not tracemalloc.is_tracing()
    assert any("boot memtrace" in r.message for r in caplog.records)


async def test_boot_memtrace_aborts_early_on_the_rss_tripwire(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import tracemalloc

    if tracemalloc.is_tracing():
        tracemalloc.stop()
    monkeypatch.setattr(arq_worker, "_WORKER_MEMTRACE_CHECK_INTERVAL_S", 0.01)

    async def _hot(*args: object, **kwargs: object) -> dict[str, int | None]:
        return {"rss_kb": arq_worker._WORKER_MEMTRACE_RSS_REFUSE_KB + 1, "swap_kb": 0}

    monkeypatch.setattr(arq_worker, "_proc_mem_kb", _hot)
    await arq_worker._boot_memtrace(300.0)  # must not wait out the full 300s
    assert not tracemalloc.is_tracing()


# --- stale cron ticks ------------------------------------------------------------------------

def _tick_ctx(late_secs: float) -> dict[str, Any]:
    ctx = _fake_ctx(boot_serialize_until=None)
    ctx["score"] = (time.time() - late_secs) * 1000.0
    return ctx


@pytest.fixture(autouse=True)
def _fresh_stale_counters(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(arq_worker, "_stale_skipped", {})
    monkeypatch.setattr(arq_worker, "_stale_logged_at", {})


async def test_a_short_cadence_tick_hours_late_is_dropped_unrun() -> None:
    ran: list[int] = []

    async def job(ctx: dict[str, Any]) -> int:
        ran.append(1)
        return 1

    wrapped = arq_worker.watched(job, every=5)
    assert await wrapped(_tick_ctx(23093.0)) == 0
    assert ran == []  # the replayed tick of 6.4 h ago did no work
    assert await wrapped(_tick_ctx(2.0)) == 1  # a fresh tick runs as always
    assert ran == [1]


async def test_ordinary_queue_lag_and_daily_jobs_are_never_dropped() -> None:
    ran: list[str] = []

    async def quick(ctx: dict[str, Any]) -> int:
        ran.append("quick")
        return 1

    async def nightly(ctx: dict[str, Any]) -> int:
        ran.append("nightly")
        return 1

    # 20 s behind a 5 s cadence is 4 cadences but under the 30 s floor: queue lag, not a backlog
    assert await arq_worker.watched(quick, every=5)(_tick_ctx(20.0)) == 1
    # a daily job hours late is the catch-up and night-window gates' business, never dropped here
    assert await arq_worker.watched(nightly, every=86400)(_tick_ctx(23093.0)) == 1
    # a tick with no score (not from the cron queue) is not judged
    assert await arq_worker.watched(quick, every=5)(
        _fake_ctx(boot_serialize_until=None)) == 1
    assert ran == ["quick", "nightly", "quick"]


async def test_stale_ticks_log_once_per_window_with_the_count(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def job(ctx: dict[str, Any]) -> int:
        return 1

    wrapped = arq_worker.watched(job, every=5)
    with caplog.at_level("INFO", logger="osiris.worker"):
        for _ in range(50):
            await wrapped(_tick_ctx(23000.0))
    lines = [r.message for r in caplog.records if "stale tick" in r.message]
    assert len(lines) == 1 and "job: skipped 1 stale tick" in lines[0]
    assert arq_worker._stale_skipped["job"] == 49  # counted, reported with the next line
