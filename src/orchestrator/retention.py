"""Retention for the two highest-churn append-only tables: outbox and audit_log, kept
bounded so unpruned growth does not burn compute indefinitely.

Measured live 2026-08-17: outbox 3.20M rows/608MB (accumulating since 2026-07-02, ~70k
rows/day), audit_log 3.22M rows/689MB (since 2026-06-29, ~66k rows/day), both NEVER
pruned since the tables were created. Same shape as the telemetry 90-day prune at
compositions.py:512 (an interval cutoff, an indexed delete), scaled up for row counts two
orders of magnitude larger: telemetry's `search_log` prune is a single unconditional
DELETE run opportunistically after every search because the table stays small between
runs; outbox/audit_log needed years to reach millions of rows unpruned, so a single
unbatched DELETE here would hold a lock and generate WAL for a multi-million-row
transaction. BATCHED (a bounded id range per statement) is the load-bearing difference,
not a stylistic one.

COLD BY DEFAULT: unlike a reversible, contradiction-gated merge with its own undo verb,
a retention DELETE has no unmerge, the row is gone. `execute=False` is not just the
default, it is required explicit opt-in every time; there is no environment-variable
"gate stays off unless X" shape here at all, deliberately, so a prune can never fire
just because nobody bothered to unset a flag."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg

# The outbox window, in days (the operator's storage ruling): ONE value for the CLI's
# default and the daily heartbeat. The heartbeat used to pass 90 while this module's
# default said 30, so the table kept three times what was meant and nothing looked wrong.
OUTBOX_RETENTION_DAYS = 30

# outbox: only PUBLISHED rows are eligible, an unpublished row (published_at IS NULL) is
# still awaiting the worker's own drain and must never be touched, no matter its age (a
# stuck/lagging worker is a SEPARATE alarm, reap_stale_runs's own class of problem, not
# a reason for retention to quietly erase the backlog it should be flagged for instead).
_OUTBOX_ELIGIBLE = "published_at IS NOT NULL AND created_at < $1"
# audit_log has no publish/consume state: pure age-based retention.
_AUDIT_ELIGIBLE = "created_at < $1"

# THE GRAPH IS NEVER A RETENTION TARGET: `table` is f-string-interpolated straight into the
# DELETE below, so the one thing standing between this module and a graph-eating prune is
# this allowlist, never trust a caller (present or future) to only ever pass
# "outbox"/"audit_log" by convention.
_ALLOWED_TABLES = frozenset({"outbox", "audit_log"})


def _guard_table(table: str) -> None:
    if table not in _ALLOWED_TABLES:
        raise ValueError(
            f"retention refuses an unlisted table {table!r}, only "
            f"{sorted(_ALLOWED_TABLES)} are eligible for this module's DELETE; "
            "the graph (objects/assertions/current_assertions/links/soul_lines/"
            "harness_turns/fleet_messages, or any other object-store table) is never one")


async def _dry_run(pool: asyncpg.Pool, table: str, where: str, days: int) -> dict[str, Any]:
    _guard_table(table)
    cutoff = datetime.now(UTC) - timedelta(days=days)
    count = await pool.fetchval(f"SELECT count(*) FROM {table} WHERE {where}", cutoff)
    return {"table": table, "days": days, "cutoff": cutoff.isoformat(),
           "eligible": count, "executed": False}


async def _apply(
    pool: asyncpg.Pool, table: str, where: str, days: int, batch_size: int,
) -> dict[str, Any]:
    _guard_table(table)
    cutoff = datetime.now(UTC) - timedelta(days=days)
    deleted = 0
    while True:
        n = await pool.execute(
            f"DELETE FROM {table} WHERE id IN "
            f"(SELECT id FROM {table} WHERE {where} ORDER BY id LIMIT $2)",
            cutoff, batch_size)
        # asyncpg's Execute return is "DELETE <n>": the count is the only signal a
        # batch ran dry (0 rows matched, nothing left before the cutoff)
        n_deleted = int(n.split()[-1])
        deleted += n_deleted
        if n_deleted < batch_size:
            break
    return {"table": table, "days": days, "cutoff": cutoff.isoformat(),
           "deleted": deleted, "executed": True}


async def outbox_retention(
    pool: asyncpg.Pool, *, days: int = OUTBOX_RETENTION_DAYS, execute: bool = False,
    batch_size: int = 5000,
) -> dict[str, Any]:
    """PUBLISHED outbox rows older than `days`, 30 is a generous replay/debug window for
    events the worker has already durably delivered; an unpublished row is NEVER eligible
    regardless of age. `execute=False` (the default) only counts; `execute=True` deletes
    in batches of `batch_size`, looping until a batch comes back short (nothing left)."""
    if execute:
        return await _apply(pool, "outbox", _OUTBOX_ELIGIBLE, days, batch_size)
    return await _dry_run(pool, "outbox", _OUTBOX_ELIGIBLE, days)


async def audit_log_retention(
    pool: asyncpg.Pool, *, days: int = 90, execute: bool = False, batch_size: int = 5000,
) -> dict[str, Any]:
    """audit_log rows older than `days`, 90 matches the telemetry search_log precedent
    (compositions.py:512) and stays generous enough for the rare forensic case
    (`mounts.undrop_dead_project_mount` reads a specific audit_log row by id; 90 days is
    ample for a drop anyone would still want to reverse). `execute=False` (the default)
    only counts; `execute=True` deletes in batches of `batch_size`."""
    if execute:
        return await _apply(pool, "audit_log", _AUDIT_ELIGIBLE, days, batch_size)
    return await _dry_run(pool, "audit_log", _AUDIT_ELIGIBLE, days)


# THE DUPLICATE WRITE, RETIRED (storage redesign, one write per fact): every property
# assertion once also wrote an audit_log row ('assert_property') restating what the
# assertions row already holds (who spoke, when, what it replaced). Actions.assert_property
# no longer writes it unless the actor differs from the source; this retires the ones
# already written. A row goes ONLY when an assertion exists that says the same thing: same
# object and name, source equal to the audit row's actor, the same transaction timestamp
# (both columns default to now(), which is fixed per transaction), and the same
# `supersedes`. An audit row with no such assertion carries information the assertions
# table lacks (an actor that is not the source, or a fact since removed) and is kept.
# The rest of the audit table (links, objects, mount drops, ...) is untouched, and nothing
# here reads or writes the graph.
#
# TWO PASSES, because the match key differs. A re-assertion names the row it replaced, so
# the assertion is found by that id through the `supersedes` index (one row). A FIRST
# assertion of a triple has no predecessor, so it is found through (object, name), which
# is cheap only because there is exactly one such row per triple: matching every row that
# way instead walks chains tens of thousands of rows deep (measured 20 s per 200 rows).
_ASSERT_AUDIT_MATCH = (
    " a.source_id = audit_log.actor"
    " AND a.created_at = audit_log.created_at")
_ASSERT_AUDIT_PASSES = (
    # `supersedes` is the id of the row replaced, so it already fixes the object and the
    # name: equating those too only tempts the planner into the (object, name) index, which
    # for the busiest chains is tens of thousands of rows deep per lookup.
    ("chained",
     "action = 'assert_property' AND audit_log.payload->>'supersedes' IS NOT NULL AND EXISTS ("
     " SELECT 1 FROM assertions a"
     " WHERE a.supersedes = (audit_log.payload->>'supersedes')::bigint AND" + _ASSERT_AUDIT_MATCH
     + ")"),
    ("first",
     "action = 'assert_property' AND audit_log.payload->>'supersedes' IS NULL AND EXISTS ("
     " SELECT 1 FROM assertions a"
     " WHERE a.object_id = (audit_log.payload->>'object_id')::uuid"
     " AND a.name = audit_log.payload->>'name' AND a.supersedes IS NULL AND"
     + _ASSERT_AUDIT_MATCH + ")"),
)


async def assert_property_audit_retirement(
    pool: asyncpg.Pool, *, execute: bool = False, batch_size: int = 5000,
    max_seconds: float | None = None,
) -> dict[str, Any]:
    """Delete the assert_property audit rows that duplicate an assertions row, in id-ordered
    batches of `batch_size`, each its own short statement. `execute=False` (the default)
    counts the first `batch_size * 20` candidates of each pass only (a full count over
    millions of rows is a long scan), so `eligible` is a floor and `capped` says whether it
    is the whole answer. `execute=True` deletes; `max_seconds` stops between batches once
    spent, and the result's `finished` says whether the job ran dry (a later run simply
    continues from where the table now stands)."""
    import time

    if not execute:
        cap = batch_size * 20
        eligible = 0
        capped = False
        for _name, where in _ASSERT_AUDIT_PASSES:
            n = await pool.fetchval(
                f"SELECT count(*) FROM (SELECT 1 FROM audit_log WHERE {where} LIMIT $1) t", cap)
            eligible += n
            capped = capped or n >= cap
        return {"table": "audit_log", "action": "assert_property", "eligible": eligible,
               "capped": capped, "executed": False}
    started = time.monotonic()
    deleted = 0
    finished = True
    for _name, where in _ASSERT_AUDIT_PASSES:
        cursor = 0
        while True:
            rows = await pool.fetch(
                "DELETE FROM audit_log WHERE id IN ("
                f" SELECT id FROM audit_log WHERE id > $1 AND {where}"
                " ORDER BY id LIMIT $2) RETURNING id", cursor, batch_size)
            deleted += len(rows)
            if len(rows) < batch_size:
                break
            cursor = max(r["id"] for r in rows)
            if max_seconds is not None and time.monotonic() - started >= max_seconds:
                finished = False
                break
        if not finished:
            break
    return {"table": "audit_log", "action": "assert_property", "deleted": deleted,
           "finished": finished, "executed": True}


# THE LAYOUT HISTORY, RETIRED (storage redesign): the layout heartbeat used to store every
# object's position as three property assertions (graph_x, graph_y, graph_layout_v), a new
# row per object per re-layout; about 2.1M rows nothing reads. Positions now live in the
# graph_layout table. A row is removed only when the layout heartbeat wrote it (its own
# source) AND the object already has its position in graph_layout, so an object that has
# not been copied across keeps its assertions until it is.
_LAYOUT_PROPERTY_NAMES = ("graph_x", "graph_y", "graph_layout_v")
_LAYOUT_SOURCE = "cron:graph_layout"
_LAYOUT_WINDOW = 50000


async def retire_layout_history(
    pool: asyncpg.Pool, *, execute: bool = False, window: int = _LAYOUT_WINDOW,
    max_seconds: float | None = None,
) -> dict[str, Any]:
    """Delete the layout heartbeat's graph_x/graph_y/graph_layout_v assertion rows (current
    and superseded, hot and cold) for every object whose position is in graph_layout, in id
    windows of `window`. `execute=False` counts the first few windows only; `execute=True`
    deletes, stopping between windows once `max_seconds` is spent (`finished` says whether
    the whole id range was covered; a later run continues, and rows already gone cost
    nothing to skip)."""
    import time

    high = await pool.fetchval("SELECT max(id) FROM assertions_hot") or 0
    where = (
        "name = ANY($3::text[]) AND source_id = $4 AND EXISTS ("
        " SELECT 1 FROM graph_layout g WHERE g.object_id = {t}.object_id)")
    if not execute:
        sample = 0
        for table in ("assertions_hot", "assertions_cold"):
            sample += await pool.fetchval(
                f"SELECT count(*) FROM {table} WHERE id > $1 AND id <= $2 AND "
                + where.format(t=table), 0, min(window * 4, high),
                list(_LAYOUT_PROPERTY_NAMES), _LAYOUT_SOURCE)
        return {"eligible": sample, "sampled": window * 4 < high, "executed": False}
    started = time.monotonic()
    deleted = 0
    lo = 0
    finished = True
    while lo < high:
        hi = lo + window
        for table in ("assertions_hot", "assertions_cold"):
            n = await pool.fetchval(
                f"WITH d AS (DELETE FROM {table} WHERE id > $1 AND id <= $2 AND "
                + where.format(t=table) + " RETURNING 1) SELECT count(*) FROM d",
                lo, hi, list(_LAYOUT_PROPERTY_NAMES), _LAYOUT_SOURCE)
            deleted += n
        lo = hi
        if max_seconds is not None and time.monotonic() - started >= max_seconds and lo < high:
            finished = False
            break
    return {"deleted": deleted, "finished": finished, "executed": True}
