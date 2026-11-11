"""The two scans that used to hold the event loop (measured with the loop-lag sampler):
the decision near-duplicate comparison and the vector matrix build. Each now runs its CPU
work on a worker thread, and the loop keeps ticking while it does."""
from __future__ import annotations

import asyncio
import threading
import time
from typing import Any

import pytest
from src.actions.core import Actions
from src.orchestrator import capture, semantics
from src.orchestrator.capture import find_near_duplicate_decision, record_decision


async def test_decision_dedup_compares_off_the_event_loop(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    d = await record_decision(actions, "order is load-bearing, never reorder the steps",
                              repo="lagproj")
    main = threading.get_ident()
    seen: list[int] = []
    real = capture._first_exact

    def spy(candidates: Any, norm_new: str) -> Any:
        seen.append(threading.get_ident())
        return real(candidates, norm_new)

    monkeypatch.setattr(capture, "_first_exact", spy)
    hit = await find_near_duplicate_decision(
        actions.pool, "Order is load-bearing; never reorder the steps!", repo="lagproj")
    assert hit == d  # still the same answer
    assert seen and all(t != main for t in seen)


async def test_the_ratio_fallback_also_runs_off_the_loop(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    d = await record_decision(actions, "order is load-bearing, never reorder the steps",
                              repo="lagproj2")
    main = threading.get_ident()
    seen: list[int] = []
    real = capture._best_ratio

    def spy(candidates: Any, norm_new: str) -> Any:
        seen.append(threading.get_ident())
        return real(candidates, norm_new)

    async def no_trgm(pool: Any) -> bool:
        return False

    monkeypatch.setattr(capture, "_best_ratio", spy)
    monkeypatch.setattr(capture, "_pg_trgm_enabled", no_trgm)
    hit = await find_near_duplicate_decision(
        actions.pool, "order is load-bearing, never reorder these steps", repo="lagproj2")
    assert hit == d
    assert seen and all(t != main for t in seen)


def test_the_comparison_key_is_memoized_so_a_second_call_normalizes_nothing_new() -> None:
    capture._dedup_key.cache_clear()
    texts = [f"ruling number {i} about something" for i in range(50)]
    for t in texts:
        capture._dedup_key(t)
    before = capture._dedup_key.cache_info()
    for t in texts:
        capture._dedup_key(t)
    after = capture._dedup_key.cache_info()
    assert after.misses == before.misses and after.hits == before.hits + 50
    assert capture._dedup_key("STATE OF THE BOARD: Hello,  World") == "hello world"


def test_unit_rows_scales_every_row_to_length_one_and_leaves_a_zero_row_zero() -> None:
    import numpy as np

    mat = semantics._unit_rows([np.array([[3.0, 4.0], [0.0, 0.0]], dtype=np.float32),
                                np.array([[0.0, 2.0]], dtype=np.float32)])
    assert mat.shape == (3, 2)
    assert np.allclose(mat[0], [0.6, 0.8])
    assert np.allclose(mat[1], [0.0, 0.0])
    assert np.allclose(mat[2], [0.0, 1.0])


async def test_the_matrix_build_normalizes_on_a_worker_thread_and_the_loop_keeps_ticking(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = actions.pool
    oid = await actions.create_or_find_object("Decision", "lag-matrix-probe", "test")
    await pool.execute(
        "INSERT INTO search_vectors (object_id, field, model, text_hash, vec) "
        "VALUES ($1,'summary','lagmodel','h',$2)", oid, [0.3, 0.4, 0.0])
    semantics._matrix_cache.clear()
    main = threading.get_ident()
    seen: list[int] = []
    real = semantics._unit_rows

    def slow(parts: Any) -> Any:
        seen.append(threading.get_ident())
        time.sleep(0.15)  # stands in for the ~190 ms of whole-matrix numpy work
        return real(parts)

    monkeypatch.setattr(semantics, "_unit_rows", slow)
    ticks = 0

    async def ticker() -> None:
        nonlocal ticks
        while True:
            await asyncio.sleep(0.01)
            ticks += 1

    t = asyncio.create_task(ticker())
    try:
        packed = await semantics._matrix(pool, "lagmodel")
    finally:
        t.cancel()
    assert packed is not None
    assert seen and all(x != main for x in seen)
    assert ticks >= 5  # the loop ran during the 150 ms, it was not held
    semantics._matrix_cache.clear()
