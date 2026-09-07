"""The worker boot spike (thread 0c03a685): fifteen run_at_startup crons used to fire
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
    ctx = _fake_ctx(boot_serialize_until=time.monotonic() + 10.0)
    await asyncio.gather(wrapped(ctx), wrapped(ctx))
    # serialized: the first call's "end" lands before the second call's "start"
    assert order == ["start", "end", "start", "end"]


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
    must behave exactly like the past-deadline case — never blocked, never a KeyError."""
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
