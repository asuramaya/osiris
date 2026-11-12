"""NIGHTLY DATABASE COMPACTION (operator ruling: compaction is automatic and runs overnight).

Deleting or moving rows leaves the file the size it was: the freed space is reusable by new
rows but is not returned to the operating system until the table is rewritten. After the
storage clean-ups (the transcript re-encode, the fact-history fold, the audit and layout
retirements) the big tables are mostly empty space, so a night step rewrites the ones that are.

Per big table (soul_lines with its TOAST, assertions_hot and assertions_cold, audit_log,
outbox) it measures the dead and free space and rewrites the table when more than 30% of it
and more than 2 GB is reclaimable. Rewrites run one table at a time, biggest first, inside a
window (03:00 to 05:00): pg_repack when the extension and binary exist (no long lock),
otherwise VACUUM FULL under a lock_timeout, with a statement timeout that is the time left in
the window, so a rewrite that will not finish by then is cancelled and rolled back, never left
holding the table. A table is skipped, with the reason recorded, when
  - the disk/WAL brake is paused,
  - the free space would not cover the rewrite (the table's size for the copy plus the same
    again for the WAL it writes, and still leave the brake's floor), or
  - a bulk drain on that table is still running (the housekeeping steps and the transcript
    re-encode record it), because compacting a table that is still being emptied wastes the
    rewrite.
The before and after sizes of every attempt are kept in the watermarks table and shown in
backup-status."""
from __future__ import annotations

import asyncio
import json
import logging
import shutil
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg

_log = logging.getLogger("osiris.db_compaction")

GB = 1024 ** 3
TABLES = ("soul_lines", "assertions_hot", "assertions_cold", "audit_log", "outbox")
MIN_BLOAT_FRACTION = 0.30
MIN_BLOAT_BYTES = 2 * GB
# A table that is small by design (its live rows fit in a few hundred MB) can be mostly empty
# space and still never reach the 2 GB floor above, so it would never be rewritten: the
# outbox sat at 709 MB reclaimable of 1.08 GB (65%) and was skipped night after night.
# The audit log is the same shape: mostly retired duplicate rows, 1.28 GB reclaimable of 1.67 GB.
# The assertions tables are the same shape since the history fold and the duplicate-abstention
# collapse: assertions_cold is 78% empty (1.3 GB of 1.7 GB) and assertions_hot 64% empty, both
# under the 2 GB floor, so neither would ever be rewritten.
MIN_BLOAT_BYTES_BY_TABLE = {
    "outbox": 256 * 1024 ** 2, "audit_log": 256 * 1024 ** 2,
    "assertions_hot": 256 * 1024 ** 2, "assertions_cold": 256 * 1024 ** 2,
}
# VACUUM FULL needs ACCESS EXCLUSIVE, and a queued exclusive request blocks every new reader and
# writer of the table behind it while it waits. A short wait that is retried later costs the fleet
# at most a few seconds; a long one stalls it that long and then fails anyway.
LOCK_TIMEOUT_SECS = 3.0
LOCK_ATTEMPTS = 4
MAX_RETRY_GAP_SECS = 300.0
RECEIPT_KEY = "compaction:last"
DRAIN_KEY = "drain:"
# A bulk-delete backfill that is not finished holds its own flag per target (the nightly
# housekeeping overwrites `drain:<table>` every pass, which would clear it mid-drain). It lapses
# when the backfill has not been called again for this long, so an abandoned run cannot keep a
# table from ever being compacted.
BACKFILL_DRAIN_KEY = "drain:backfill:"
BACKFILL_DRAIN_LAPSE = timedelta(days=7)
# The nightly housekeeping rewrites the flag; one older than this was left by a step that stopped.
DRAIN_FLAG_MAX_AGE = timedelta(hours=36)


# --- measuring ------------------------------------------------------------------------------

async def table_sizes(pool: asyncpg.Pool, table: str) -> dict[str, int]:
    row = await pool.fetchrow(
        "SELECT pg_total_relation_size(c.oid) AS total, pg_relation_size(c.oid) AS heap, "
        " COALESCE(pg_total_relation_size(NULLIF(c.reltoastrelid, 0)), 0) AS toast "
        "FROM pg_class c WHERE c.oid = $1::regclass", table)
    assert row is not None
    return {"total_bytes": int(row["total"]), "heap_bytes": int(row["heap"]),
            "toast_bytes": int(row["toast"])}


async def _approx(pool: asyncpg.Pool, oid_sql: str, table: str) -> int | None:
    """Reclaimable bytes (dead tuples plus free space) of one relation through pgstattuple's
    approximate scan, which skips pages the visibility map says are all-visible. None when
    the extension is not there and cannot be created."""
    try:
        await pool.execute("CREATE EXTENSION IF NOT EXISTS pgstattuple")
        value = await pool.fetchval(
            "SELECT COALESCE(dead_tuple_len + approx_free_space, 0)::bigint "
            f"FROM pgstattuple_approx({oid_sql})", table)
    except Exception:  # noqa: BLE001 - the extension is optional
        return None
    return int(value) if value is not None else None


async def reclaimable_bytes(pool: asyncpg.Pool, table: str) -> tuple[int, str]:
    """(bytes a rewrite would give back, how it was measured). pgstattuple_approx over the
    table and its TOAST relation when available; otherwise the dead-tuple share of the heap
    from the statistics, which misses space autovacuum already recycled (it says so)."""
    main = await _approx(pool, "$1::regclass", table)
    if main is not None:
        toast = await _approx(
            pool, "(SELECT NULLIF(reltoastrelid, 0) FROM pg_class WHERE oid = $1::regclass)",
            table) or 0
        return main + toast, "pgstattuple_approx"
    row = await pool.fetchrow(
        "SELECT s.n_live_tup AS live, s.n_dead_tup AS dead, pg_relation_size(c.oid) AS heap "
        "FROM pg_stat_user_tables s JOIN pg_class c ON c.oid = s.relid "
        "WHERE c.oid = $1::regclass", table)
    if row is None or (row["live"] + row["dead"]) == 0:
        return 0, "statistics (empty)"
    share = row["dead"] / (row["live"] + row["dead"])
    return int(row["heap"] * share), "statistics (dead tuples only)"


def worth_rewriting(total_bytes: int, reclaimable: int, table: str | None = None) -> bool:
    floor = min(MIN_BLOAT_BYTES, MIN_BLOAT_BYTES_BY_TABLE.get(table or "", MIN_BLOAT_BYTES))
    return total_bytes > 0 and reclaimable > floor \
        and reclaimable / total_bytes > MIN_BLOAT_FRACTION


# --- the drain flags ------------------------------------------------------------------------

async def set_drain(pool: asyncpg.Pool, tables: tuple[str, ...], running: bool) -> None:
    from src.orchestrator.monitor import set_cursor

    stamp = json.dumps({"running": running, "at": datetime.now(UTC).isoformat()})
    for table in tables:
        await set_cursor(pool, DRAIN_KEY + table, stamp)


async def set_backfill_drain(
    pool: asyncpg.Pool, target: str, tables: tuple[str, ...], running: bool,
) -> None:
    """Record that a bulk-delete backfill is (or is no longer) emptying `tables`."""
    from src.orchestrator.monitor import set_cursor

    await set_cursor(pool, BACKFILL_DRAIN_KEY + target, json.dumps({
        "running": running, "tables": list(tables), "at": datetime.now(UTC).isoformat()}))


async def _backfill_drain_running(pool: asyncpg.Pool, table: str) -> bool:
    rows = await pool.fetch(
        "SELECT cursor FROM watermarks WHERE key LIKE $1", BACKFILL_DRAIN_KEY + "%")
    now = datetime.now(UTC)
    for row in rows:
        try:
            flag = json.loads(row["cursor"])
            at = datetime.fromisoformat(flag["at"])
        except (ValueError, KeyError, TypeError):
            continue
        if flag.get("running") and table in flag.get("tables", []) \
                and now - at < BACKFILL_DRAIN_LAPSE:
            return True
    return False


async def drain_running(pool: asyncpg.Pool, table: str) -> bool:
    """A bulk drain is still emptying `table`: a housekeeping step flagged it unfinished, a
    bulk-delete backfill is mid-run (its own flag, see `BACKFILL_DRAIN_KEY`), or (for
    soul_lines) the transcript re-encode or encryption is mid-pass."""
    from src.orchestrator import soul_encrypt_progress, soul_recompress
    from src.orchestrator.monitor import get_cursor

    if table == "soul_lines" and any(
            read().get("state") == "running"
            for read in (soul_recompress.read_progress, soul_encrypt_progress.read_progress)):
        return True
    if await _backfill_drain_running(pool, table):
        return True
    raw = await get_cursor(pool, DRAIN_KEY + table)
    if not raw:
        return False
    try:
        flag = json.loads(raw)
        running = bool(flag.get("running"))
        stamped = datetime.fromisoformat(str(flag["at"])) if flag.get("at") else None
    except (ValueError, TypeError, AttributeError):
        return False
    # A flag that nothing has rewritten for a day and a half is stale, not a drain in
    # progress: the housekeeping that sets it runs nightly, so a failing step would otherwise
    # leave "running" on forever and the table would never be rewritten.
    if running and stamped is not None:
        if stamped.tzinfo is None:
            stamped = stamped.replace(tzinfo=UTC)
        if datetime.now(UTC) - stamped > DRAIN_FLAG_MAX_AGE:
            return False
    return running


# --- rewriting ------------------------------------------------------------------------------

def _repack_available() -> bool:
    return shutil.which("pg_repack") is not None


async def _installed(pool: asyncpg.Pool, name: str) -> bool:
    return bool(await pool.fetchval("SELECT 1 FROM pg_extension WHERE extname = $1", name))


class LockNotAcquired(RuntimeError):
    """The table lock could not be had within the short lock timeout on any attempt."""

    def __init__(self, table: str, attempts: int, timeout_secs: float, gap_secs: float) -> None:
        super().__init__(table)
        self.reason = (f"could not get the table lock within {timeout_secs:g} s on {attempts} "
                       f"tries spaced about {gap_secs:.0f} s apart: something kept it busy; "
                       "nothing was changed")


async def rewrite_table(
    pool: asyncpg.Pool, table: str, *, seconds_left: float, dsn: str | None = None,
    lock_attempts: int = LOCK_ATTEMPTS, lock_timeout_secs: float = LOCK_TIMEOUT_SECS,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], Awaitable[Any]] = asyncio.sleep,
) -> str:
    """Rewrite `table` and return the engine used. pg_repack when it is installed (no long
    lock); otherwise VACUUM FULL, which holds the table exclusively for the whole rewrite. Its
    lock is asked for with a SHORT timeout and tried `lock_attempts` times spaced across the
    time left, so a busy table is retried later instead of stalling every reader behind a long
    wait; if it is never free, `LockNotAcquired`. Cancelled when `seconds_left` runs out."""
    if dsn and _repack_available() and await _installed(pool, "pg_repack"):
        proc = await asyncio.create_subprocess_exec(
            "pg_repack", "--dbname", dsn, "--table", table, "--no-superuser-check",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
        try:
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=max(1.0, seconds_left))
        except TimeoutError:
            proc.kill()
            raise
        if proc.returncode != 0:
            raise RuntimeError(f"pg_repack failed: {out.decode(errors='replace')[-300:]}")
        return "pg_repack"
    started = clock()
    gap = min(MAX_RETRY_GAP_SECS, max(1.0, seconds_left / (lock_attempts + 1)))
    for attempt in range(lock_attempts):
        left = seconds_left - (clock() - started)
        if attempt > 0 and left <= 1:
            break  # no time left to retry; the first try always runs (and is cancelled by time)
        async with pool.acquire() as conn:
            await conn.execute(f"SET lock_timeout = '{int(lock_timeout_secs * 1000)}ms'")
            await conn.execute(f"SET statement_timeout = '{max(1, int(left * 1000))}ms'")
            try:
                await conn.execute(f"VACUUM (FULL, ANALYZE) {table}")
                return "VACUUM FULL"
            except asyncpg.exceptions.LockNotAvailableError:
                pass  # busy: wait and try again
            finally:
                await conn.execute("RESET statement_timeout")
                await conn.execute("RESET lock_timeout")
        if attempt < lock_attempts - 1:
            await sleep(gap)
    raise LockNotAcquired(table, lock_attempts, lock_timeout_secs, gap)


# --- the night run --------------------------------------------------------------------------

async def compact_tonight(
    pool: asyncpg.Pool, *, window_secs: float, tables: tuple[str, ...] | None = None,
    dsn: str | None = None, clock: Callable[[], float] = time.monotonic,
    brake: Callable[[asyncpg.Pool], Awaitable[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    """Look at every big table, biggest reclaimable space first, and rewrite the ones worth it
    until the window's hard stop. Returns {table: receipt}; every receipt says what was done
    or why not (and the sizes before and after a rewrite)."""
    from src.orchestrator import disk_brake
    from src.orchestrator.monitor import set_cursor

    brake = brake or disk_brake.brake_state
    started = clock()
    receipts: dict[str, Any] = {}
    measured: list[tuple[int, str, dict[str, int], str]] = []
    for table in tables or TABLES:
        try:
            sizes = await table_sizes(pool, table)
            reclaim, method = await reclaimable_bytes(pool, table)
        except Exception as exc:  # noqa: BLE001 - one unreadable table never stops the rest
            receipts[table] = {"action": "skipped", "reason": f"could not measure: {exc}"}
            continue
        measured.append((reclaim, table, sizes, method))
    for reclaim, table, sizes, method in sorted(measured, reverse=True):
        base = {"table": table, "before_bytes": sizes["total_bytes"],
                "reclaimable_bytes": reclaim, "measured_by": method,
                "at": datetime.now(UTC).isoformat()}
        left = window_secs - (clock() - started)
        if not worth_rewriting(sizes["total_bytes"], reclaim, table):
            receipts[table] = {**base, "action": "skipped", "reason": "not enough dead space"}
        elif left <= 60:
            receipts[table] = {**base, "action": "skipped", "reason": "the window is over"}
        elif await drain_running(pool, table):
            receipts[table] = {**base, "action": "skipped",
                               "reason": "a bulk drain is still running on it"}
        else:
            state = await brake(pool)
            free = int(state["free_gb"] * GB)
            floor = int(state["floor_gb"] * GB)
            need = 2 * sizes["total_bytes"]
            if state["paused"]:
                receipts[table] = {**base, "action": "skipped",
                                   "reason": f"paused: disk/WAL budget ({state['reason']})"}
            elif free - need < floor:
                receipts[table] = {**base, "action": "skipped",
                                   "reason": f"needs about {need / GB:.0f} GB free and would "
                                             f"leave less than the {floor / GB:.0f} GB floor"}
            else:
                try:
                    engine = await rewrite_table(pool, table, seconds_left=left, dsn=dsn)
                    after = (await table_sizes(pool, table))["total_bytes"]
                    receipts[table] = {**base, "action": "rewritten", "engine": engine,
                                       "after_bytes": after}
                except LockNotAcquired as exc:
                    receipts[table] = {**base, "action": "skipped", "reason": exc.reason}
                except Exception as exc:  # noqa: BLE001 - window cancel, repack error
                    receipts[table] = {**base, "action": "failed", "reason": repr(exc)}
        _log.info("db compaction %s: %s", table, receipts[table].get("action"))
    await set_cursor(pool, RECEIPT_KEY, json.dumps(receipts))
    return receipts


async def last_receipts(pool: asyncpg.Pool) -> dict[str, Any]:
    from src.orchestrator.monitor import get_cursor

    raw = await get_cursor(pool, RECEIPT_KEY)
    try:
        return dict(json.loads(raw)) if raw else {}
    except ValueError:
        return {}


async def compaction_status(pool: asyncpg.Pool) -> dict[str, Any]:
    """For backup-status: the current size of each big table and what the last night run did
    to it (sizes before and after, or why it was skipped). Cheap: sizes only, no scans."""
    last = await last_receipts(pool)
    tables: dict[str, Any] = {}
    for table in TABLES:
        try:
            sizes = await table_sizes(pool, table)
        except Exception as exc:  # noqa: BLE001
            tables[table] = {"error": str(exc)}
            continue
        tables[table] = {"total_gb": round(sizes["total_bytes"] / GB, 2),
                         "last_run": last.get(table)}
    return {"window": "03:00 to 05:00", "tables": tables}
