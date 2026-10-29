from __future__ import annotations

import json
import time
from collections import deque
from typing import Any, cast

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


# THE ACQUIRE BOUND (2026-09-29 wedge): asyncpg's own acquire() default is timeout=None,
# wait forever. A pool-starvation deadlock (every connection held by a coroutine that is
# itself parked in a second acquire) therefore never surfaced as an error: tool calls
# just hung and the watchdog logged "SLOW TOOL CALL" every 30s with nothing to name. A
# pool built by create_pool(..., acquire_timeout=N) now bounds every acquire (including
# the implicit ones behind pool.fetch/fetchval/execute) and raises PoolStarved, a named
# TimeoutError, instead of parking forever. An explicit acquire(timeout=...) still wins.


class PoolStarved(TimeoutError):
    """No pooled connection came free within the pool's acquire bound. Named so a caller
    (or the fleet reading the error) can tell pool starvation, usually a holder parked in
    a nested acquire while every slot is taken, apart from a lock wait or a slow query."""


class _TimedAcquireContext:
    """Wraps asyncpg's own PoolAcquireContext, timing only the WAIT — from calling
    acquire() to actually holding a connection — never the connection's own held
    duration (that's the caller's business, not this instrument's). Supports both
    calling forms PoolAcquireContext does: `await pool.acquire()` and
    `async with pool.acquire() as conn:`."""

    __slots__ = ("_inner", "_samples", "_start", "_timeout", "_max_size")

    def __init__(self, inner: Any, samples: deque[float], *, timeout: float | None = None,
                 max_size: int | None = None) -> None:
        self._inner = inner
        self._samples = samples
        self._start = 0.0
        self._timeout = timeout
        self._max_size = max_size

    def _starved(self, exc: BaseException) -> PoolStarved:
        return PoolStarved(
            f"pool acquire starved: no connection came free within {self._timeout}s "
            f"(pool max_size={self._max_size}); every slot is held, most likely by a "
            "holder waiting on a second acquire from the same pool (see the asyncio task "
            "dump for the coroutine)")

    def __await__(self) -> Any:
        self._start = time.monotonic()
        try:
            conn = yield from self._inner.__await__()
        except TimeoutError as exc:
            raise self._starved(exc) from exc
        self._samples.append(time.monotonic() - self._start)
        return conn

    async def __aenter__(self) -> Any:
        self._start = time.monotonic()
        try:
            conn = await self._inner.__aenter__()
        except TimeoutError as exc:
            raise self._starved(exc) from exc
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
        if timeout is None:
            timeout = self.__dict__.get("_osiris_acquire_timeout")
        return _TimedAcquireContext(super().acquire(timeout=timeout), samples,
                                    timeout=timeout, max_size=self.get_max_size())


# PINNED POOL (2026-09-29 wedge): a coroutine that takes a transaction-scoped advisory
# lock (mint_lock, _seat_lock, _peer_lock) holds one pooled connection for the lock's
# whole life. Its body used to keep calling `actions.pool.*`, each a SECOND acquire from
# the same pool: with N such holders in flight (a statusline /heartbeat burst fanning
# live_succession across N lineages, or any gather of mint-locked calls) and N >= the pool
# size, every slot is a locked holder waiting for a slot. Measured live: 5 mint: holders
# idle-in-transaction on their lock, 3 more slots churning through lock_timeout waiters,
# every holder parked in pool.acquire() forever.
#
# The structural fix: the lock yields a PinnedPool, a pool-shaped view of the locked
# connection. Everything the body does through it (pool.fetch*/execute, pool.acquire(),
# Actions(pinned) and every helper that takes a pool) runs on that ONE connection, so a
# locked body can never ask the pool for a second slot. Each call runs in its own
# SAVEPOINT, which keeps the old per-call failure isolation (a failed best-effort read no
# longer poisons the lock's transaction) while nothing escapes the lock's connection.
# The body's writes now commit when the lock releases (the same instant the next waiter
# can see them), and an exception escaping the lock body rolls them back together.
# Single-task by construction: two tasks driving one connection concurrently is refused
# by asyncpg itself ("another operation is in progress"), loud, never a hang.


class _PinnedAcquire:
    """acquire() on a PinnedPool: both calling forms asyncpg's PoolAcquireContext has,
    handing back the one pinned connection, never a new one."""

    __slots__ = ("_conn",)

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    def __await__(self) -> Any:
        return self.__aenter__().__await__()

    async def __aenter__(self) -> asyncpg.Connection:
        return self._conn

    async def __aexit__(self, *exc: Any) -> None:
        return None


class PinnedPool:
    """A pool-shaped view of ONE already-acquired connection. See the module comment."""

    __slots__ = ("_conn",)

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    @property
    def connection(self) -> asyncpg.Connection:
        return self._conn

    def acquire(self, *, timeout: float | None = None) -> _PinnedAcquire:
        return _PinnedAcquire(self._conn)

    async def release(self, connection: Any, *, timeout: float | None = None) -> None:  # noqa: ASYNC109 - asyncpg.Pool signature
        if connection is not self._conn:
            raise asyncpg.InterfaceError("PinnedPool.release: not the pinned connection")

    async def execute(self, query: str, *args: Any, timeout: float | None = None) -> str:  # noqa: ASYNC109 - asyncpg.Pool signature
        async with self._conn.transaction():
            return cast(str, await self._conn.execute(query, *args, timeout=timeout))

    async def executemany(self, command: str, args: Any, *,
                          timeout: float | None = None) -> None:  # noqa: ASYNC109 - asyncpg.Pool signature
        async with self._conn.transaction():
            await self._conn.executemany(command, args, timeout=timeout)

    async def fetch(self, query: str, *args: Any, timeout: float | None = None,  # noqa: ASYNC109 - asyncpg.Pool signature
                    record_class: Any = None) -> list[Any]:
        async with self._conn.transaction():
            return cast(list[Any], await self._conn.fetch(
                query, *args, timeout=timeout, record_class=record_class))

    async def fetchval(self, query: str, *args: Any, column: int = 0,
                       timeout: float | None = None) -> Any:  # noqa: ASYNC109 - asyncpg.Pool signature
        async with self._conn.transaction():
            return await self._conn.fetchval(query, *args, column=column, timeout=timeout)

    async def fetchrow(self, query: str, *args: Any, timeout: float | None = None,  # noqa: ASYNC109 - asyncpg.Pool signature
                       record_class: Any = None) -> Any:
        async with self._conn.transaction():
            return await self._conn.fetchrow(
                query, *args, timeout=timeout, record_class=record_class)


def pin(pool: asyncpg.Pool, conn: asyncpg.Connection) -> asyncpg.Pool:
    """The pool-typed handle a locked body uses instead of `pool`: every helper in this
    codebase takes an `asyncpg.Pool` and only ever calls acquire()/fetch*/execute on it,
    which PinnedPool implements on the one pinned connection (the cast is that duck-typed
    contract, nothing more). An already-pinned pool on the same connection is returned
    as is, so a nested lock taken through a pinned pool stays on the one connection."""
    if isinstance(pool, PinnedPool) and pool.connection is conn:
        return pool
    return cast(asyncpg.Pool, PinnedPool(conn))


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
    acquire_timeout: float | None = None,
) -> asyncpg.Pool:
    """`application_name` (task #180 piece 2 (c)): tags every connection this pool opens so
    `pg_stat_activity` can be grouped BY DAEMON, not read as one undifferentiated blob —
    `asyncpg` forwards it straight into `server_settings` per-connection, no DSN mangling
    needed. Optional and appended-only: every existing caller with no name to give keeps
    Postgres's own default (the client library name), unchanged.

    `acquire_timeout` bounds every acquire this pool serves (see PoolStarved): None keeps
    asyncpg's wait-forever default for callers that have not opted in."""
    # JIT OFF for every pool (measured 2026-10-29): the mail predicate behind /heartbeat and
    # /stop is costed at ~16 million by the planner (a hashed SubPlan multiplied per row), far
    # past jit_above_cost, so Postgres spent 257 of its 305 ms compiling the query and ran it
    # in 48 ms. JIT pays off on long analytic scans; this house runs short transactional reads.
    server_settings = {"jit": "off"}
    if application_name:
        server_settings["application_name"] = application_name
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
    if acquire_timeout is not None and hasattr(pool, "__dict__"):
        pool.__dict__["_osiris_acquire_timeout"] = acquire_timeout
    return pool
