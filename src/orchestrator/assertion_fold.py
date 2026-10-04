"""FOLD NO-OP RE-ASSERTIONS (storage redesign, tiered fact history): a source that asserts
the same value again and again used to write a new row each time, so two thirds of the
assertions table (5.4M of 8.7M rows, measured) is a row identical to the one it replaced.
`Actions.assert_property` has refused to write those since its no-op guard; this retires the
ones already written.

THE RULE, as narrow as it can be stated. A row R is folded into the row P it supersedes
only when ALL of these hold:
  - R is superseded (is_current is false) and older than `min_age_days`, so a live writer
    is never racing a fold: the writer reads only the current row, and the current row is
    never touched except to repoint its `supersedes`.
  - P is the same fact from the same place: same object, name and source, and the same
    value, confidence, evidence class, evidence uri, evidence hash, case and helper run.
    Nothing R said that P did not say is lost.
The fold deletes R, moves the later `observed_at` onto P (the same "confirmed still true at
T2" the write-side guard keeps in place), and repoints whatever pointed at R to P. Folding
front to back collapses a whole run of identical rows to the first one, so first-seen time
(P's own created_at) and last-seen time (observed_at) both survive. A current row is never
deleted and an entry with a different value is never merged.

WHY THE CONSTITUTION'S "NEVER DELETE" DOES NOT BLOCK THIS: nothing the graph knows is lost.
The deleted row is a byte-for-byte restatement of its predecessor on every provenance field,
the supersession chain stays connected, and the receipt counts every row. It was authorized
by the storage redesign ruling (fold no-op re-assertions, move old history to cold).

SET-BASED, BOUNDED: rows are examined in id windows; each window is one short transaction
(one candidate read, two repoint statements, two bump statements, two deletes) and its
delete count must equal the candidate count or the transaction rolls back. Rows may live in
either the hot or the cold table, so every statement addresses both physical tables.

The disk footprint does not shrink by itself: deleted rows leave reusable space, and the
file shrinks only when the table is rewritten (see compact_assertion_tables)."""
from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg

DEFAULT_WINDOW = 20000
DEFAULT_MIN_AGE_DAYS = 7

_SAME_AS_PREDECESSOR = (
    " p.object_id = r.object_id AND p.name = r.name AND p.source_id = r.source_id"
    " AND p.value = r.value AND p.confidence = r.confidence"
    " AND p.evidence_class IS NOT DISTINCT FROM r.evidence_class"
    " AND p.evidence_uri IS NOT DISTINCT FROM r.evidence_uri"
    " AND p.evidence_sha256 IS NOT DISTINCT FROM r.evidence_sha256"
    " AND p.case_id IS NOT DISTINCT FROM r.case_id"
    " AND p.helper_run_id IS NOT DISTINCT FROM r.helper_run_id")

# The predecessor is fetched by id inside a LATERAL with OFFSET 0 on purpose: written as a
# plain join, the planner also sees "same object and name" and picks the (object, name)
# index for it, which for the busiest chains is tens of thousands of rows per lookup
# (measured: minutes per 20000-id window). The fence leaves it only the primary key.
_CANDIDATES_SQL = (
    "SELECT r.id, r.supersedes AS pid, r.observed_at FROM assertions r "
    "CROSS JOIN LATERAL (SELECT * FROM assertions p0 WHERE p0.id = r.supersedes OFFSET 0) p "
    "WHERE r.id > $1 AND r.id <= $2 AND NOT r.is_current AND r.created_at < $3 AND"
    + _SAME_AS_PREDECESSOR + " ORDER BY r.id")

_PHYSICAL = ("assertions_hot", "assertions_cold")


def _cutoff(min_age_days: int) -> datetime:
    return datetime.now(UTC) - timedelta(days=min_age_days)


async def plan_fold(
    pool: asyncpg.Pool, *, min_age_days: int = DEFAULT_MIN_AGE_DAYS, sample_ids: int = 2_000_000,
) -> dict[str, Any]:
    """Count the foldable rows among the first `sample_ids` ids above zero (a full count is
    a long scan); `eligible` is a floor when `sampled` is true."""
    high = await pool.fetchval("SELECT max(id) FROM assertions_hot") or 0
    upper = min(sample_ids, high)
    n = await pool.fetchval(
        "SELECT count(*) FROM (" + _CANDIDATES_SQL.replace("ORDER BY r.id", "") + ") t",
        0, upper, _cutoff(min_age_days))
    return {"eligible": n, "sampled": upper < high, "executed": False,
           "min_age_days": min_age_days}


async def _fold_window(conn: asyncpg.Connection, rows: list[asyncpg.Record]) -> int:
    ids = [r["id"] for r in rows]
    root: dict[int, int] = {}
    latest: dict[int, datetime] = {}
    for r in rows:  # ascending id: a predecessor folded earlier in this window resolves first
        target = root.get(r["pid"], r["pid"])
        root[r["id"]] = target
        if target not in latest or r["observed_at"] > latest[target]:
            latest[target] = r["observed_at"]
    old = list(root)
    new = [root[o] for o in old]
    roots = list(latest)
    stamps = [latest[t] for t in roots]
    for table in _PHYSICAL:
        await conn.execute(
            f"UPDATE {table} t SET supersedes = m.new_id "
            "FROM unnest($1::bigint[], $2::bigint[]) AS m(old_id, new_id) "
            "WHERE t.supersedes = m.old_id AND t.id <> ALL($3::bigint[])", old, new, ids)
        await conn.execute(
            f"UPDATE {table} t SET observed_at = GREATEST(t.observed_at, m.at) "
            "FROM unnest($1::bigint[], $2::timestamptz[]) AS m(rid, at) "
            "WHERE t.id = m.rid AND t.observed_at < m.at", roots, stamps)
    deleted = 0
    for table in _PHYSICAL:
        deleted += await conn.fetchval(
            f"WITH d AS (DELETE FROM {table} WHERE id = ANY($1::bigint[]) RETURNING 1) "
            "SELECT count(*) FROM d", ids)
    if deleted != len(ids):
        raise RuntimeError(
            f"fold window deleted {deleted} rows for {len(ids)} candidates; the transaction "
            "rolls back rather than leave the chain half repointed")
    return deleted


async def apply_fold(
    pool: asyncpg.Pool, *, min_age_days: int = DEFAULT_MIN_AGE_DAYS, window: int = DEFAULT_WINDOW,
    max_seconds: float | None = None,
) -> dict[str, Any]:
    """Fold every foldable row, window by window, from the oldest id up. `max_seconds` stops
    between windows once spent (`finished` says whether the whole id range was covered); a
    later run starts over from the oldest id, which is cheap because folded rows are gone."""
    started = time.monotonic()
    cutoff = _cutoff(min_age_days)
    high = await pool.fetchval("SELECT max(id) FROM assertions_hot") or 0
    lo = 0
    folded = 0
    windows = 0
    finished = True
    while lo < high:
        hi = lo + window
        async with pool.acquire() as conn, conn.transaction():
            rows = await conn.fetch(_CANDIDATES_SQL, lo, hi, cutoff)
            if rows:
                folded += await _fold_window(conn, rows)
                windows += 1
        lo = hi
        if max_seconds is not None and time.monotonic() - started >= max_seconds and lo < high:
            finished = False
            break
    return {"folded": folded, "windows": windows, "finished": finished, "executed": True,
           "min_age_days": min_age_days, "reached_id": min(lo, high)}


async def compact_assertion_tables(pool: asyncpg.Pool) -> dict[str, Any]:
    """Rewrite both assertion tables so the freed space is returned to the operating system
    (`VACUUM FULL`). It takes an ACCESS EXCLUSIVE lock on each table in turn, so every read
    and write of the graph waits for it: run it in a quiet minute, never from a cron."""
    sizes: dict[str, Any] = {}
    for table in _PHYSICAL:
        before = await pool.fetchval("SELECT pg_total_relation_size($1::regclass)", table)
        async with pool.acquire() as conn:
            await conn.execute(f"VACUUM (FULL, ANALYZE) {table}")
        after = await pool.fetchval("SELECT pg_total_relation_size($1::regclass)", table)
        sizes[table] = {"before_bytes": before, "after_bytes": after}
    return sizes
