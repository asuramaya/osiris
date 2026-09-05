from __future__ import annotations

import json
import time
from collections import deque
from typing import Any

import asyncpg
import asyncpg.pool as _asyncpg_pool_module

# ACQUIRE-WAIT INSTRUMENTATION (thread e4a5755a): the only existing pool_health surface
# (pg_activity_by_app) reads pg_stat_activity — an INSTANTANEOUS backend-count snapshot,
# which cannot answer "does anything actually queue for a connection," because queueing
# happens client-side, before a backend is even involved. A bounded per-pool sample deque
# (never unbounded — a long-lived daemon pool must not leak memory one sample at a time)
# is the cheapest way to answer it: time acquire()'s own wait specifically (not the whole
# checkout-to-release lifetime), read back via `pool_acquire_wait_stats(pool)`
# (src/orchestrator/pool_health.py wires it in).
#
# WHY A SUBCLASS, NOT INSTANCE MONKEYPATCHING: asyncpg.Pool is a plain (non-Cython)
# class but declares __slots__ — `pool.acquire = ...` on a live instance raises
# AttributeError (confirmed live: "attribute 'acquire' is read-only"), so the wrap has
# to happen at the CLASS level instead: briefly swap `asyncpg.pool.Pool` for
# `_TimedPool` for the span of `create_pool`'s own call below — see that function's
# own comment for exactly how wide the swap has to stay and why.
_ACQUIRE_WAIT_SAMPLES_MAXLEN = 1000


class _TimedAcquireContext:
    """Wraps asyncpg's own PoolAcquireContext, timing only the WAIT — from calling
    acquire() to actually holding a connection — never the connection's own held
    duration (that's the caller's business, not this instrument's). Supports both
    calling forms PoolAcquireContext does: `await pool.acquire()` and
    `async with pool.acquire() as conn:`."""

    __slots__ = ("_inner", "_samples", "_start")

    def __init__(self, inner: Any, samples: deque[float]) -> None:
        self._inner = inner
        self._samples = samples
        self._start = 0.0

    def __await__(self) -> Any:
        self._start = time.monotonic()
        conn = yield from self._inner.__await__()
        self._samples.append(time.monotonic() - self._start)
        return conn

    async def __aenter__(self) -> Any:
        self._start = time.monotonic()
        conn = await self._inner.__aenter__()
        self._samples.append(time.monotonic() - self._start)
        return conn

    async def __aexit__(self, *exc: Any) -> Any:
        return await self._inner.__aexit__(*exc)


class _TimedPool(_asyncpg_pool_module.Pool):  # type: ignore[misc]
    """`asyncpg.pool.Pool` has no `__dict__` of its own (slotted), but a SUBCLASS that
    declares no `__slots__` of its own gets one automatically — that's where the
    samples deque lives, created lazily on first `acquire()` (never in an overridden
    `__init__`, whose exact signature/defaults this repo must not have to keep in sync
    with asyncpg's own)."""

    def acquire(self, *, timeout: float | None = None) -> _TimedAcquireContext:
        samples = self.__dict__.setdefault(
            "_osiris_acquire_wait_samples", deque(maxlen=_ACQUIRE_WAIT_SAMPLES_MAXLEN))
        return _TimedAcquireContext(super().acquire(timeout=timeout), samples)


def pool_acquire_wait_stats(pool: asyncpg.Pool) -> dict[str, Any]:
    """Read-only. `{}` for a pool never wrapped by this module's own create_pool (the
    samples deque is instance-only, nothing to read). Percentiles computed from
    whatever is currently in the bounded window — a recent-history sample, never a
    lifetime histogram (the maxlen deque's own point)."""
    samples = getattr(pool, "_osiris_acquire_wait_samples", None)
    if not samples:
        return {"count": 0}
    ordered: list[float] = sorted(samples)
    n = len(ordered)

    def _pct(p: float) -> float:
        idx = min(n - 1, int(p * n))
        return ordered[idx]

    return {
        "count": n, "p50_ms": round(_pct(0.50) * 1000, 3),
        "p99_ms": round(_pct(0.99) * 1000, 3), "max_ms": round(ordered[-1] * 1000, 3),
    }


async def _init_connection(conn: Any) -> None:
    """Register JSON/JSONB codecs so Python dicts pass to/from jsonb columns directly."""
    for typename in ("json", "jsonb"):
        await conn.set_type_codec(
            typename,
            encoder=json.dumps,
            decoder=json.loads,
            schema="pg_catalog",
        )


async def create_pool(
    dsn: str, *, min_size: int = 1, max_size: int = 10, application_name: str | None = None,
) -> asyncpg.Pool:
    """`application_name` (task #180 piece 2 (c)): tags every connection this pool opens so
    `pg_stat_activity` can be grouped BY DAEMON, not read as one undifferentiated blob —
    `asyncpg` forwards it straight into `server_settings` per-connection, no DSN mangling
    needed. Optional and appended-only: every existing caller with no name to give keeps
    Postgres's own default (the client library name), unchanged."""
    server_settings = {"application_name": application_name} if application_name else None
    real_pool_cls = _asyncpg_pool_module.Pool
    _asyncpg_pool_module.Pool = _TimedPool
    try:
        # THE SWAP MUST STAY LIVE ACROSS THIS AWAIT, not just the call expression: in
        # this repo's own test harness (tests/conftest.py's live-DB guard), asyncpg.
        # create_pool is ITSELF wrapped by an `async def` — calling it only builds a
        # coroutine object, the guard's own body (and its inner, real `Pool(...)`
        # construction) doesn't run until awaited. Restoring the class before that
        # await would let the construction see the REAL Pool again, silently losing
        # the instrument (caught live: the wait-stats tests read count=0 with a
        # call-then-restore-then-await ordering). The tradeoff this accepts: two
        # coroutines calling create_pool CONCURRENTLY in the same process could race
        # this module-global swap — checked every real call site in src/ (none do;
        # each daemon builds its own pool(s) sequentially, never via asyncio.gather)
        # — worst case of a race is a pool silently NOT wrapped, never a crash.
        pool = await asyncpg.create_pool(
            dsn=dsn, init=_init_connection, min_size=min_size, max_size=max_size,
            server_settings=server_settings,
        )
    finally:
        _asyncpg_pool_module.Pool = real_pool_cls
    assert pool is not None
    return pool
