"""MIGRATION 0064, THE EVENT LOG COLD PARTITION (thread THE VAULT BUILD, dispatch wave
12): the live-data archiver paired with alembic/versions/0064_assertions_hot_cold.py's
own schema half (read that file's docstring first -- it has the full A-vs-B reasoning
for why this is a hot/cold table split behind an updatable view, not native declarative
partitioning).

ELIGIBILITY, and why it is NOT simply "created_at < this month" as the dispatch's own
prose suggested: `is_current=true` rows are NEVER moved to cold, regardless of age. Two
independent reasons converge on this, both load-bearing:

  1. current_assertions (migration 0047) reads `FROM assertions a WHERE a.is_current`,
     resolved by OID directly against assertions_hot (see the schema migration's
     docstring) -- if a genuinely old but still-current fact (a project's `name`,
     asserted once and never corrected) were archived, current_assertions would go
     BLIND to it. That is a row lost from the live view, not merely relocated --
     exactly what "never a row lost" rules out.
  2. Actions.assert_property's own within-source supersession lookup (src/actions/
     core.py) finds "the current row for this (object,name,source)" by querying
     `assertions` (now the hot/cold view) for the non-superseded row. As long as that
     row is always in assertions_hot -- guaranteed by never moving is_current=true rows
     -- every existing write-path query keeps working with ZERO code changes. Move a
     live winner and the very next write to that triple could silently fork the
     supersession chain (the exact failure class 0047's own advisory-lock comment
     documents for concurrent writers, only now caused by an archiver, not a race).

So the real eligibility predicate is: `is_current = false AND created_at < cutoff`. In
practice this still empties the vast majority of the historical corpus into cold (0047
measured is_current=true at ~4.5% of the whole table on the real corpus) -- "hot holds
the current month" becomes, honestly, "hot holds every live winner plus a trailing
window of not-yet-superseded-long-enough history," which is what actually satisfies
"recall and search, out of the hot path" without going blind on an old-but-still-true
fact.

NOT THROUGH ACTIONS (constitution point 4's Actions waist), DELIBERATELY: this migration
mints no new fact and asserts nothing -- it relocates already-written rows, unchanged,
between two physical tables that the `assertions` view already presents as one logical
history. No audit_log/outbox entry is warranted for the same reason VACUUM doesn't get
one: it's storage-tier housekeeping on facts that already exist, not a new observation.
This is the same class of exception migration 0047's own is_current backfill already
claimed (a raw SQL UPDATE, not an Actions call) for the identical reason.

BOUNDED BATCHES, COMPENSATING, NOTHING LOST: each batch is its own short transaction --
`SELECT ... FOR UPDATE SKIP LOCKED LIMIT batch_size` claims a bounded slice (SKIP LOCKED
so this can safely run alongside live traffic without blocking a concurrent writer that
happens to touch the same old, is_current=false row for some other reason), copies it
into assertions_cold, verifies the COPY count before doing anything else, and only then
deletes the identical id set from assertions_hot, verifying THAT count too. Nothing here
is ever a blind DELETE: it is a copy-verify-delete triple, scoped to the exact ids just
copied, in the same transaction as the copy. A crash between batches loses nothing --
every completed batch already committed both sides; the next run's SELECT simply finds a
smaller remaining set (idempotent: a row already in cold no longer matches the hot-side
WHERE clause).

THE RECEIPT is the final, loud, count-preserving assertion: before = hot+cold at the
start, after = hot+cold at the end. They must be equal -- this function RAISES if they
are not, rather than returning a receipt that quietly says otherwise.

RUN-START-SCOPED RECONCILIATION (fixed after a production false positive, wave 12's own
run: 4,034,150 rows moved correctly across 807 batches, then the final check raised
anyway because the rest of the live fleet wrote 2,996 brand-new rows into assertions_hot
during the ~7 minute run): a naive before/after snapshot of `count(hot)+count(cold)`
cannot distinguish "a row went missing" from "the rest of the world kept writing while
this ran" -- both move the number. The fix is to count only rows with `created_at` before
a single `run_start` timestamp captured once at the very top of `apply_migration_0064`,
on both the before- and after- side. A pre-existing row is counted identically both
times regardless of whether it moved to cold, stayed in hot, or (if something were
genuinely wrong) got lost or duplicated -- so a real defect still raises. A row written
DURING the run has `created_at >= run_start` and is excluded from both counts, so it can
never move this number and can never trigger a spurious raise.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, cast

import asyncpg

MIGRATION_SOURCE = "migration:0064_assertions_hot_cold"
DEFAULT_BATCH_SIZE = 5000

_COLS = (
    "id, object_id, name, value, source_id, case_id, helper_run_id, evidence_uri, "
    "evidence_sha256, observed_at, confidence, supersedes, created_at, evidence_class, "
    "is_current"
)
# Measured live (see tests/test_migration_0064.py's own compression probe): a plain
# `INSERT ... SELECT value FROM ...` copies the TOASTed bytes VERBATIM -- it does not
# recompress into the destination column's own SET COMPRESSION method, so a naive mover
# would silently keep every archived value pglz-compressed forever, no matter what
# assertions_cold's own DDL says. Forcing a text round-trip on the way through detoasts
# and reconstructs the jsonb value, so Postgres re-TOASTs it fresh against the
# destination column's compression setting (lz4) -- the only way this migration's own
# "compressed cold" claim is actually true of the archived bytes, not just of the schema.
_COLS_SELECT = _COLS.replace("value,", "(value::text::jsonb) AS value,")


def default_cutoff(now: datetime | None = None) -> datetime:
    """The start of the current calendar month (UTC) -- everything superseded and
    older than this is cold-eligible. A caller (a test, an operator's own dry run)
    may always pass an explicit `cutoff` instead; this is only the live default."""
    now = now or datetime.now(UTC)
    return datetime(now.year, now.month, 1, tzinfo=UTC)


@dataclass
class MoveReceipt:
    source: str
    cutoff: str
    batch_size: int
    batches_run: int
    rows_examined: int
    rows_moved: int
    before: dict[str, int]
    after: dict[str, int]
    count_preserved: bool
    vacuum_note: str
    skipped_locked: bool = field(default=False)

    def as_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "cutoff": self.cutoff,
            "batch_size": self.batch_size,
            "batches_run": self.batches_run,
            "rows_examined": self.rows_examined,
            "rows_moved": self.rows_moved,
            "before": self.before,
            "after": self.after,
            "count_preserved": self.count_preserved,
            "vacuum_note": self.vacuum_note,
        }


class ReconciliationError(RuntimeError):
    """Raised loudly, never swallowed: before-count != after-count across
    assertions_hot + assertions_cold. See this module's own docstring."""


async def plan_migration_0064(
    pool: asyncpg.Pool, *, cutoff: datetime | None = None,
) -> dict[str, Any]:
    """DRY RUN -- read-only, never writes. Reports exactly what apply_migration_0064
    would move, without moving it."""
    cutoff = cutoff or default_cutoff()
    eligible = cast(
        int,
        await pool.fetchval(
            "SELECT count(*) FROM assertions_hot WHERE is_current=false AND created_at < $1",
            cutoff,
        ),
    )
    hot_total = cast(int, await pool.fetchval("SELECT count(*) FROM assertions_hot"))
    cold_total = cast(int, await pool.fetchval("SELECT count(*) FROM assertions_cold"))
    return {
        "cutoff": cutoff.isoformat(),
        "eligible_rows": eligible,
        "hot_total": hot_total,
        "cold_total": cold_total,
        "total": hot_total + cold_total,
    }


async def apply_migration_0064(
    pool: asyncpg.Pool,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    cutoff: datetime | None = None,
) -> dict[str, Any]:
    """Moves every is_current=false row older than `cutoff` from assertions_hot into
    assertions_cold, in bounded batches (see module docstring for the copy-verify-delete
    shape), and returns a count-preserving receipt. Raises ReconciliationError -- loudly,
    never silently -- if the total row count across both tables changed.

    RECONCILIATION IS RUN-START-SCOPED, deliberately, so a concurrent live fleet writing
    brand-new rows into assertions_hot during this run can never trip a false positive:
    `run_start` is captured once, before the before-count is even taken, and both the
    before- and after-count only count rows with `created_at < run_start`. Every row that
    existed when the run began has `created_at < run_start` by definition and is counted
    identically on both sides, whether it stayed in hot, moved to cold, or (if something
    were genuinely wrong) vanished or duplicated -- a real loss or duplication among that
    pre-existing population still changes this count and still raises. Every row written
    by the rest of the fleet DURING the run has `created_at >= run_start` (it did not
    exist yet when run_start was captured) and is excluded from BOTH counts, so ordinary
    concurrent traffic can never move this number and can never cause a spurious
    ReconciliationError -- exactly the false-positive class that fired in production."""
    cutoff = cutoff or default_cutoff()
    run_start = datetime.now(UTC)
    before_hot = cast(
        int,
        await pool.fetchval(
            "SELECT count(*) FROM assertions_hot WHERE created_at < $1", run_start,
        ),
    )
    before_cold = cast(
        int,
        await pool.fetchval(
            "SELECT count(*) FROM assertions_cold WHERE created_at < $1", run_start,
        ),
    )
    before_total = before_hot + before_cold

    examined = 0
    moved = 0
    batches = 0

    while True:
        async with pool.acquire() as conn, conn.transaction():
            id_rows = await conn.fetch(
                "SELECT id FROM assertions_hot WHERE is_current=false AND created_at < $1 "
                "ORDER BY id LIMIT $2 FOR UPDATE SKIP LOCKED",
                cutoff,
                batch_size,
            )
            batch_ids = [cast(int, r["id"]) for r in id_rows]
            if not batch_ids:
                break
            examined += len(batch_ids)

            copied = cast(
                int,
                await conn.fetchval(
                    f"WITH moved_rows AS ("
                    f"  INSERT INTO assertions_cold ({_COLS}) "
                    f"  SELECT {_COLS_SELECT} FROM assertions_hot "
                    f"  WHERE id = ANY($1::bigint[]) "
                    f"  RETURNING 1"
                    f") SELECT count(*) FROM moved_rows",
                    batch_ids,
                ),
            )
            if copied != len(batch_ids):
                raise ReconciliationError(
                    f"batch {batches}: expected to copy {len(batch_ids)} rows into "
                    f"assertions_cold, copied {copied} -- refusing to delete anything "
                    "from assertions_hot for this batch"
                )

            deleted = cast(
                int,
                await conn.fetchval(
                    "WITH del AS (DELETE FROM assertions_hot WHERE id = ANY($1::bigint[]) "
                    "RETURNING 1) SELECT count(*) FROM del",
                    batch_ids,
                ),
            )
            if deleted != len(batch_ids):
                raise ReconciliationError(
                    f"batch {batches}: copied {copied} rows into assertions_cold but only "
                    f"deleted {deleted} of {len(batch_ids)} from assertions_hot -- the "
                    "transaction is about to roll back rather than leave a duplicate"
                )
            moved += len(batch_ids)
            batches += 1

    after_hot = cast(
        int,
        await pool.fetchval(
            "SELECT count(*) FROM assertions_hot WHERE created_at < $1", run_start,
        ),
    )
    after_cold = cast(
        int,
        await pool.fetchval(
            "SELECT count(*) FROM assertions_cold WHERE created_at < $1", run_start,
        ),
    )
    after_total = after_hot + after_cold

    dead_tup = cast(
        int | None,
        await pool.fetchval(
            "SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname='assertions_hot'",
        ),
    )
    if dead_tup is not None:
        vacuum_note = (
            f"assertions_hot keeps its old on-disk footprint (~{dead_tup} dead tuples "
            "pending reclaim by autovacuum) until a VACUUM FULL is run; autovacuum will "
            "reuse the space for new rows but will not shrink the file. This migration "
            "does not run VACUUM FULL -- it takes an ACCESS EXCLUSIVE lock, so the shrink "
            "is the operator's own call."
        )
    else:
        vacuum_note = (
            "assertions_hot keeps its old on-disk footprint (dead-tuple count not "
            "available from pg_stat_user_tables) until a VACUUM FULL is run; autovacuum "
            "will reuse the space for new rows but will not shrink the file. This "
            "migration does not run VACUUM FULL -- it takes an ACCESS EXCLUSIVE lock, so "
            "the shrink is the operator's own call."
        )

    receipt = MoveReceipt(
        source=MIGRATION_SOURCE,
        cutoff=cutoff.isoformat(),
        batch_size=batch_size,
        batches_run=batches,
        rows_examined=examined,
        rows_moved=moved,
        before={"hot": before_hot, "cold": before_cold, "total": before_total},
        after={"hot": after_hot, "cold": after_cold, "total": after_total},
        count_preserved=before_total == after_total,
        vacuum_note=vacuum_note,
    )
    if before_total != after_total:
        raise ReconciliationError(
            f"COUNT MISMATCH after migration 0064: before={before_total} "
            f"after={after_total} -- receipt={receipt.as_dict()!r}"
        )
    return receipt.as_dict()
