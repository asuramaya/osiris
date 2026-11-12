"""NIGHTLY DATABASE COMPACTION (operator ruling: compaction is automatic and runs overnight).

Deleting or moving rows leaves the file the size it was: the freed space is reusable by new
rows but is not returned to the operating system until the table is rewritten. After the
storage clean-ups (the transcript re-encode, the fact-history fold, the audit and layout
retirements) the big tables are mostly empty space, so a night step rewrites the ones that are.

Per big table (soul_lines with its TOAST, assertions_hot and assertions_cold, audit_log,
outbox) it measures the dead and free space and rewrites the table when more than 30% of it
and more than 2 GB is reclaimable. Rewrites run one table at a time, biggest first, inside a
window (03:00 to 05:00): pg_repack when the extension is installed and the database container's
client answers (no long lock),
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
import contextlib
import json
import logging
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


def _size(n: int) -> str:
    return f"{n / GB:.1f} GB" if n >= GB else f"{n / 1024 ** 2:.0f} MB"


def skip_reason(total_bytes: int, reclaimable: int, table: str | None = None) -> str:
    """Why a table is under the rewrite thresholds, naming the rule that applied and the
    numbers. "Not enough dead space" beside 1.2 GB reclaimable read as a contradiction: the
    space was there, it was under this table's floor."""
    floor = min(MIN_BLOAT_BYTES, MIN_BLOAT_BYTES_BY_TABLE.get(table or "", MIN_BLOAT_BYTES))
    reasons = []
    if reclaimable <= floor:
        reasons.append(f"{_size(reclaimable)} reclaimable is at or below this table's "
                       f"{_size(floor)} floor")
    if total_bytes <= 0 or reclaimable / total_bytes <= MIN_BLOAT_FRACTION:
        share = 100 * reclaimable / total_bytes if total_bytes > 0 else 0
        reasons.append(f"{share:.0f}% reclaimable is at or below the "
                       f"{MIN_BLOAT_FRACTION:.0%} minimum")
    return "; ".join(reasons) or "below the rewrite thresholds"


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

# --- pg_repack, run inside the database container ------------------------------------------
#
# pg_repack rewrites a table with only a moment of exclusive lock at the start and the end. It
# is not installed on this box today, and everything here stays inert until it is: the choice
# needs BOTH the extension in the database AND a working `pg_repack` client in the container.
# The client runs inside the container (`docker exec`), where its version always matches the
# extension's, because the host has no client of its own. The container's `timeout` stops it
# with SIGINT at the end of the allowance (pg_repack cleans up after itself on SIGINT) and only
# then, as a last resort after a grace period, kills it. A SIGKILL on the client used to be the
# way a run was ended; it leaves pg_repack's log table and a trigger on the table behind.
PG_CONTAINER = "osiris-pg"
REPACK_STOP_GRACE_SECS = 60
REPACK_WAIT_TIMEOUT_SECS = 5
REPACK_PROBE_SECS = 15
# less than this left in the window and a repack is not started: its stop signal goes out at
# (time left - 90 s), and a rewrite that cannot finish would only be stopped and thrown away
REPACK_MIN_SECS = 120
# the exit code `_docker_exec` reports when the docker client itself stopped answering: not a
# process exit status, so it can never be confused with `timeout`'s own 124
CLIENT_UNANSWERED = 1000

ExecFn = Callable[[list[str], float], Awaitable[tuple[int, str]]]


async def _docker_exec(args: list[str], timeout_secs: float) -> tuple[int, str]:
    """Run `docker <args>` and return (exit code, output). Never raises: no docker binary, a
    container that is not there or a command that does not finish read as a failed run. A client
    that stops answering is terminated (the client only; what it started in the container is
    not killed), awaited so it leaves no zombie, and reported as `CLIENT_UNANSWERED`."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "docker", *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    except OSError as exc:
        return 127, f"{type(exc).__name__}: {exc}"
    try:
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout_secs)
    except TimeoutError:
        proc.terminate()
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(proc.wait(), timeout=5)
        return CLIENT_UNANSWERED, "the docker client did not answer in time"
    return int(proc.returncode or 0), out.decode(errors="replace")


def _container() -> str:
    import os

    return os.environ.get("OSIRIS_PG_CONTAINER") or PG_CONTAINER


async def _installed(pool: asyncpg.Pool, name: str) -> bool:
    return bool(await pool.fetchval("SELECT 1 FROM pg_extension WHERE extname = $1", name))


async def repack_ready(pool: asyncpg.Pool, exec_fn: ExecFn | None = None) -> bool:
    """Can a table be rewritten with pg_repack right now: the extension is in the database and
    the container answers `pg_repack --version`. Any other answer, including a box with no
    docker, no container or no extension, is False and the caller rewrites with VACUUM FULL."""
    if not await _installed(pool, "pg_repack"):
        return False
    code, _out = await (exec_fn or _docker_exec)(
        ["exec", _container(), "pg_repack", "--version"], REPACK_PROBE_SECS)
    return code == 0


def repack_command(
    table: str, *, stop_after_secs: int, user: str, database: str,
) -> list[str]:
    """The `docker` arguments for one repack. `timeout` inside the container sends SIGINT after
    `stop_after_secs` and SIGKILL only `REPACK_STOP_GRACE_SECS` later if it is still running.
    `--no-kill-backend` leaves other sessions alone (pg_repack would cancel one that held a
    conflicting lock past the wait), and the wait for its brief exclusive locks is short."""
    return ["exec", _container(), "timeout", "--signal=INT",
            f"--kill-after={REPACK_STOP_GRACE_SECS}", f"{stop_after_secs}s",
            "pg_repack", "-U", user, "-d", database, "--table", table,
            "--no-superuser-check", "--no-kill-backend",
            f"--wait-timeout={REPACK_WAIT_TIMEOUT_SECS}"]


def _user_and_database(dsn: str | None) -> tuple[str, str]:
    from urllib.parse import unquote, urlsplit

    parts = urlsplit(dsn or "")
    return (unquote(parts.username) if parts.username else "osiris",
            unquote(parts.path.lstrip("/")) or "osiris")


class _LockBusy(RuntimeError):
    """pg_repack gave up on its short wait for the table's brief locks and skipped the table."""


# pg_repack exits 0 when it skips a table because a conflicting lock was not released within
# --wait-timeout (checked against the live extension: exit 0 and these two lines)
_SKIPPED_ON_LOCK = ("Skipping repack", "timed out, do not cancel conflicting backends")


async def _repack(
    table: str, *, seconds_left: float, dsn: str | None, exec_fn: ExecFn | None,
) -> str:
    user, database = _user_and_database(dsn)
    stop_after = max(30, int(seconds_left) - REPACK_STOP_GRACE_SECS - 30)
    code, out = await (exec_fn or _docker_exec)(
        repack_command(table, stop_after_secs=stop_after, user=user, database=database),
        stop_after + REPACK_STOP_GRACE_SECS + 30)
    if code == 0:
        if any(mark in out for mark in _SKIPPED_ON_LOCK):
            raise _LockBusy(table)
        return "pg_repack"
    tail = out[-300:]
    if code == CLIENT_UNANSWERED:
        raise RuntimeError(
            "the docker client did not answer in time, so the state of the repack is unknown: "
            "it may still be running in the container, and a run that was cut off can leave a "
            f"trigger or log table in the repack schema behind: {tail}")
    if code == 124:
        raise RuntimeError(
            f"pg_repack was stopped when the time allowed ran out (after {stop_after} s) and "
            f"cleaned up after itself; the table was not rewritten: {tail}")
    if code == 137:
        raise RuntimeError(
            f"pg_repack did not stop on SIGINT within {REPACK_STOP_GRACE_SECS} s and was "
            "killed: it may have left its log table and a trigger on the table. Run "
            "DROP EXTENSION pg_repack; CREATE EXTENSION pg_repack; in the database before "
            f"anything else writes to {table}: {tail}")
    raise RuntimeError(f"pg_repack failed (exit {code}): {tail}")


class RewriteSkipped(RuntimeError):
    """A rewrite that was not attempted, or gave way to something busy; nothing was changed.
    `reason` goes into the night's receipt as a skip, not a failure."""

    reason: str


class LockNotAcquired(RewriteSkipped):
    """The table lock could not be had within the short lock timeout on any attempt."""

    def __init__(self, table: str, attempts: int, timeout_secs: float, gap_secs: float) -> None:
        super().__init__(table)
        self.reason = (f"could not get the table lock within {timeout_secs:g} s on {attempts} "
                       f"tries spaced about {gap_secs:.0f} s apart: something kept it busy; "
                       "nothing was changed")


class WindowTooShort(RewriteSkipped):
    """Not enough of the window is left to start a repack that could finish."""

    def __init__(self, table: str, seconds_left: float) -> None:
        super().__init__(table)
        self.reason = (f"only {seconds_left:.0f} s of the window are left and a repack needs "
                       f"at least {REPACK_MIN_SECS} s to run and be stopped cleanly; "
                       "nothing was changed")


async def rewrite_table(
    pool: asyncpg.Pool, table: str, *, seconds_left: float, dsn: str | None = None,
    lock_attempts: int = LOCK_ATTEMPTS, lock_timeout_secs: float = LOCK_TIMEOUT_SECS,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], Awaitable[Any]] = asyncio.sleep,
    exec_fn: ExecFn | None = None,
) -> str:
    """Rewrite `table` and return the engine used. pg_repack when the extension is installed
    and the container's client answers (no long lock; see `repack_ready`); otherwise VACUUM
    FULL, which holds the table exclusively for the whole rewrite. Its lock is asked for with a
    SHORT timeout and tried `lock_attempts` times spaced across the time left, so a busy table
    is retried later instead of stalling every reader behind a long wait; if it is never free,
    `LockNotAcquired`. Cancelled when `seconds_left` runs out."""
    started = clock()
    if await repack_ready(pool, exec_fn):
        if seconds_left < REPACK_MIN_SECS:
            raise WindowTooShort(table, seconds_left)
        gap = min(MAX_RETRY_GAP_SECS, max(1.0, seconds_left / (lock_attempts + 1)))
        for attempt in range(lock_attempts):
            left = seconds_left if attempt == 0 else seconds_left - (clock() - started)
            if attempt > 0 and left < REPACK_MIN_SECS:
                break
            try:
                return await _repack(table, seconds_left=left, dsn=dsn, exec_fn=exec_fn)
            except _LockBusy:
                pass  # busy: pg_repack skipped the table; wait and try again
            if attempt < lock_attempts - 1:
                await sleep(gap)
        raise LockNotAcquired(table, lock_attempts, REPACK_WAIT_TIMEOUT_SECS, gap)
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
            receipts[table] = {**base, "action": "skipped",
                               "reason": skip_reason(sizes["total_bytes"], reclaim, table)}
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
                except RewriteSkipped as exc:
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
        run = last.get(table)
        if isinstance(run, dict) and "before_bytes" in run:
            # the same size definition as total_gb, but as it was when that run measured it
            run = {**run, "before_gb": round(run["before_bytes"] / GB, 2)}
        tables[table] = {"total_gb": round(sizes["total_bytes"] / GB, 2), "last_run": run}
    return {"window": "03:00 to 05:00",
            "sizes": "every size is the table's total on disk (heap, indexes and toast); "
                     "total_gb is now, a last run's before_gb is as of its own 'at' time",
            "tables": tables}
