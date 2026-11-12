"""A connection for a holder of a SESSION-level advisory lock.

A session lock lives as long as the connection that took it. A holder that takes that
connection from the shared pool pins one slot for as long as it holds, and it usually goes on
calling the pool for its own work, so a few holders can use every slot while each of them waits
for one more (the 2026-09-29 wedge, and the seat spawn lock in 2026-11). The rule: a session
lock is taken on a side connection (`open_side_connection`), never on a `pool.acquire()` slot.

`session_lock_connection` hands the holder that side connection and closes it on every exit,
which drops the lock with it even when the holder's own unlock was cancelled. Only when the pool
did not record how it connects (or the side connection could not be opened) does it fall back to
a pool slot, loudly, with the old behaviour: `pool.release` resets the connection, which runs
`pg_advisory_unlock_all()`. `tests/test_session_lock_guard.py` fails when a session lock is
taken on a pool slot anywhere else in src."""
from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import AsyncIterator
from typing import Any

import asyncpg

from src.db.pool import open_side_connection

_log = logging.getLogger(__name__)

CLOSE_BOUND = 5.0
# how long a holder waits for the server to have a connection free before it gives up and
# raises (a nightly job is retried by its next slot; a full server is never answered by quietly
# taking a pool slot, which is the pattern this module exists to end)
FULL_SERVER_WAIT = 60.0
FULL_SERVER_STEP = 1.0


async def close_side_connection(side: asyncpg.Connection) -> None:
    """Close a side connection, bounded. A close that is cancelled or stuck is replaced by
    `terminate()`, and a cancellation is passed on afterwards, never swallowed."""
    cancelled: BaseException | None = None
    try:
        await asyncio.wait_for(asyncio.shield(side.close()), timeout=CLOSE_BOUND)
    except asyncio.CancelledError as exc:
        cancelled = exc
        side.terminate()
    except BaseException:  # noqa: BLE001 - stuck or failed: drop it
        side.terminate()
    if cancelled is not None:
        raise cancelled


@contextlib.asynccontextmanager
async def session_lock_connection(pool: Any) -> AsyncIterator[asyncpg.Connection]:
    """The connection a session-level advisory lock is taken and held on (see the module
    docstring). Closed (or released) on every exit. `open_side_connection` returns None only
    when the pool did not record how it connects (the pool-slot fallback); a failed connect
    raises, and a full server is waited out for `FULL_SERVER_WAIT` before the raise is passed
    on."""
    deadline = time.monotonic() + FULL_SERVER_WAIT
    while True:
        try:
            side = await open_side_connection(pool)
            break
        except asyncpg.TooManyConnectionsError:
            if time.monotonic() >= deadline:
                raise
            await asyncio.sleep(FULL_SERVER_STEP)
    if side is None:
        _log.warning("no side connection for a session lock: holding a pool slot instead")
        async with pool.acquire() as conn:
            yield conn
        return
    try:
        yield side
    finally:
        await close_side_connection(side)
