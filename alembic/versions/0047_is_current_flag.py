"""is_current — a maintained flag replacing current_assertions' per-read anti-join
(thread 2a280e07, decision b25fb381: the volume that batching cannot touch)

Decision b25fb381 measured this live on today's corpus: once a caller's object batch
isn't small relative to the corpus (exactly what orient()'s own already-merged _table
batching does), current_assertions' `WHERE NOT EXISTS (SELECT 1 FROM assertions s WHERE
s.supersedes=a.id)` anti-join degrades from a cheap per-object index probe into a Merge
Anti Join that scans the ENTIRE assertions table (2,650,934 rows) and essentially the
ENTIRE assertions_supersedes_idx every single call — a cost that is structural (it must
re-derive "not superseded" over the full history to answer), not caller-side, and grows
every day this append-only kernel never shrinks.

THE FIX, exactly as thread 2a280e07 named it: maintain "current" as a flag ON WRITE
instead of re-deriving it on every READ. `is_current` starts true, and is flipped false
in the SAME transaction as the ONE row that ever legitimately retires it — audited
against the actual write path (src/actions/core.py) before this migration was written:
exactly two call sites ever set `supersedes` to a real value, and both already hold the
target row's id as a local variable at INSERT time (assert_property's own same-source
`prior` lookup; supersede_assertion's explicit `superseded_id` argument) — so the flip
costs one indexed UPDATE by primary key, not a new read.

WHY THIS DOES NOT TOUCH #102's WITHIN-VS-CROSS-SOURCE DISTINCTION (Thoth's own question,
msg 4214 constraint 2): `is_current` is flipped ONLY at the two sites that already decide
supersession today, with IDENTICAL scope — assert_property still supersedes same-source-
only; supersede_assertion is still the sole explicit, audited, `because`-justified
cross-source retirement. Nothing new picks a winner across sources: current_assertions
keeps its EXACT existing multiplicity (one row per still-current assertion, potentially
many per (object, name) across different sources) — winning_props (the actual cross-
source winner, resolved at read time, migration 0015) is unchanged, still reading FROM
current_assertions, now backed by an index seek instead of a full scan of history.

CORRECTNESS, verified against the REAL corpus before writing this migration, read-only,
no schema touched (three live checks, 2,650,934 rows): zero self-referencing rows
(`supersedes = id`), zero forked chains (two rows both pointing `supersedes` at the same
prior — the historical failure mode assert_property's own advisory-lock comment names),
and — the decisive one — for every single row, `NOT EXISTS (SELECT 1 FROM assertions s
WHERE s.supersedes=a.id)` (the anti-join's own definition) agrees EXACTLY with
`id IN (SELECT id FROM current_assertions)` (the view's actual output). Backfill below is
that same formula, run once, instead of forever.

WHY THIS IS FOUR AUTOCOMMIT BLOCKS, NOT ONE TRANSACTION (Thoth's reproduced-twice live
deadlock, msg 4228 — DROP VIEW current_assertions vs a live reader, `Process ... waits
for AccessExclusiveLock on relation ...; blocked by ...; Process ... waits for
AccessShareLock ...; blocked by ...`): alembic wraps a whole upgrade run in ONE ambient
transaction by default. Postgres never releases a lock before COMMIT — so `ALTER TABLE
ADD COLUMN`'s AccessExclusiveLock, though metadata-only and near-instant BY ITSELF (the
PG11+ fast-default path, no table rewrite), would otherwise stay HELD for the entire rest
of the migration: the ~115s backfill, the index build, and the view swap — blocking every
concurrent reader of current_assertions (five live agents plus osiris-worker/pulse, never
not live) for that whole window, and setting up exactly the AB-BA cycle Thoth caught: this
transaction holding the table exclusively while wanting the view exclusively, a live
reader holding the view (briefly, resolving its query) while wanting the table. Splitting
each DDL step into its own `autocommit_block()` (committing before entering, so each lock
is acquired-and-released independently) breaks that cycle structurally, not by luck:
  1. ADD COLUMN — its own block; commits and releases in well under a second.
  2. Backfill UPDATE — plain RowExclusiveLock only, compatible with concurrent
     AccessShareLock reads; safe to run for its full duration inside an ordinary
     transaction, blocks no reader.
  3. CREATE INDEX CONCURRENTLY — ShareUpdateExclusiveLock only (blocks other DDL, not
     reads or writes); cannot run inside any transaction block at all, hence its own.
  4. The view swap — the one step that genuinely needs AccessExclusiveLock on the view.
     By now nothing else in this migration holds any lock, so a live reader's own
     AccessShareLock (held only for the instant it resolves its query) is the only thing
     it can ever wait on — a real but short window, bounded here with a session-level
     `lock_timeout` so a rare live collision fails FAST and RETRYABLY instead of
     deadlocking, exactly Thoth's own constraint. `CREATE OR REPLACE VIEW` (not DROP+
     CREATE) — legal here because `is_current` only APPENDS to the frozen column list
     from step 1, never reorders or removes a column — also collapses two exclusive-lock
     acquisitions into one and removes the DROP..CREATE gap where a concurrent reader
     would otherwise see "relation does not exist".

Same precedent as 0005 (evidence_class) for the view's own shape: `SELECT a.*`, which
Postgres freezes to the column list at view-creation time.
"""
from __future__ import annotations

from alembic import op

revision = "0047"
down_revision = "0046"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("ALTER TABLE assertions ADD COLUMN is_current boolean NOT NULL DEFAULT true")

    # one-time full pass — exactly the anti-join's own formula, run once instead of per
    # read. RowExclusiveLock only: concurrent readers are never blocked by this, only
    # concurrent writers to the same rows, an ordinary and expected serialization.
    op.execute(
        """
        UPDATE assertions a SET is_current = false
        WHERE EXISTS (SELECT 1 FROM assertions s WHERE s.supersedes = a.id)
        """
    )

    # partial index: only the ~4.5% of rows that are actually current (measured live:
    # 119,901 of 2,650,934) — the read path's new O(matching rows) seek, replacing the
    # anti-join's O(corpus) scan. CONCURRENTLY: blocks no reader, no writer, only other
    # DDL on this table — cannot run inside a transaction block, hence its own.
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY assertions_is_current_idx ON assertions "
            "(object_id, name) WHERE is_current"
        )

    # the view swap — see the module docstring for why this is the one genuinely
    # contentious step and why it is bounded rather than left to deadlock
    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = '5s'")
        op.execute(
            "CREATE OR REPLACE VIEW current_assertions AS "
            "SELECT a.* FROM assertions a WHERE a.is_current"
        )
        op.execute("SET lock_timeout = 0")


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = '5s'")
        op.execute(
            "CREATE OR REPLACE VIEW current_assertions AS "
            "SELECT a.* FROM assertions a "
            "WHERE NOT EXISTS (SELECT 1 FROM assertions s WHERE s.supersedes = a.id)"
        )
        op.execute("SET lock_timeout = 0")
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY assertions_is_current_idx")
    with op.get_context().autocommit_block():
        op.execute("ALTER TABLE assertions DROP COLUMN is_current")
