"""create_pool's application_name tagging (task #180 piece 2 (c)) — the per-daemon
pg_stat_activity grouping fleet()'s new pool_health surface depends on."""
from __future__ import annotations

import asyncio

from src.db.pool import create_pool, pool_acquire_wait_stats


async def test_create_pool_tags_connections_with_application_name(pg_dsn: str) -> None:
    pool = await create_pool(pg_dsn, min_size=1, max_size=1, application_name="osiris-test-tag")
    try:
        name = await pool.fetchval("SELECT current_setting('application_name')")
        assert name == "osiris-test-tag"
    finally:
        await pool.close()


async def test_create_pool_without_application_name_keeps_the_default(pg_dsn: str) -> None:
    """No caller is forced to opt in — the existing untagged shape must survive unchanged."""
    pool = await create_pool(pg_dsn, min_size=1, max_size=1)
    try:
        name = await pool.fetchval("SELECT current_setting('application_name')")
        assert name != "osiris-test-tag"
    finally:
        await pool.close()


async def test_acquire_wait_stats_start_empty(pg_dsn: str) -> None:
    pool = await create_pool(pg_dsn, min_size=1, max_size=1)
    try:
        assert pool_acquire_wait_stats(pool) == {"count": 0}
    finally:
        await pool.close()


async def test_acquire_wait_stats_unwrapped_pool_reads_empty() -> None:
    """A pool this module never created (no subclass swap) has nothing to read — the
    read side degrades honestly rather than raising."""
    class _Bare:
        pass

    assert pool_acquire_wait_stats(_Bare()) == {"count": 0}  # type: ignore[arg-type]


async def test_acquire_wait_stats_records_both_calling_forms(pg_dsn: str) -> None:
    """Both `await pool.acquire()` and `async with pool.acquire() as conn:` — the two
    forms asyncpg's own PoolAcquireContext supports — must each add a sample."""
    pool = await create_pool(pg_dsn, min_size=1, max_size=2)
    try:
        conn = await pool.acquire()
        await pool.release(conn)
        async with pool.acquire() as conn2:
            assert conn2 is not None
        stats = pool_acquire_wait_stats(pool)
        assert stats["count"] == 2
        assert stats["p50_ms"] >= 0
        assert stats["max_ms"] >= stats["p50_ms"]
    finally:
        await pool.close()


async def test_acquire_wait_stats_measures_real_contention(pg_dsn: str) -> None:
    """The whole point of thread e4a5755a: prove queueing is actually visible, not just
    that the plumbing runs. A max_size=1 pool, one task holds the only connection for
    200ms while a second task's acquire() blocks behind it — its wait must measurably
    include that hold time, not read as near-zero."""
    pool = await create_pool(pg_dsn, min_size=1, max_size=1)
    try:
        release_event = asyncio.Event()

        async def _hold() -> None:
            async with pool.acquire():
                release_event.set()
                await asyncio.sleep(0.2)

        holder = asyncio.create_task(_hold())
        await release_event.wait()  # the holder now owns the pool's one connection
        async with pool.acquire():
            pass  # this acquire() had to wait behind _hold()'s own release
        await holder
        stats = pool_acquire_wait_stats(pool)
        assert stats["count"] == 2
        assert stats["max_ms"] >= 150  # the contended acquire, well under the 200ms hold
    finally:
        await pool.close()
