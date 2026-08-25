"""objects: a created_at-ordered index, for the console's browse surface (#196, Thoth msg
5600). Measured, not assumed: the objects table (31,189 rows today) carries exactly three
indexes — objects_pkey, objects_type_canonical_key (type, canonical), objects_type_idx
(type alone) — and NONE order by created_at. /objects (src/api/app.py) already does
`ORDER BY created_at DESC` per type via a window function, and the browse surface's own
paging (this same task) is about to add a keyset cursor on (created_at, id) for genuine
"load more" beyond the current flat top-N. Both need this index; neither has it today, so
every page — the first included — sorts the live table from a type-only or full scan.

Two indexes, not one: `objects_type_created_idx (type, created_at DESC, id)` serves the
common per-type-filtered browse/keyset case (the existing query's own PARTITION BY type
ORDER BY created_at DESC shape); `objects_created_idx (created_at DESC, id)` serves the
no-type-filter ("All") case, which re-sorts the whole scoped_objects CTE by created_at
after the per-type windowing. `id` trails both as the keyset tiebreaker — created_at alone
is not unique, so a cursor on created_at alone can silently skip or repeat rows at a tie.

CONCURRENTLY (own autocommit_block, ruling in 0047's own docstring): live table, live
readers on osiris-console right now — ShareUpdateExclusiveLock only, blocks other DDL,
never a reader or writer.

Revision ID: 0054
Revises: 0053
"""
from alembic import op

revision = "0054"
down_revision = "0053"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS objects_type_created_idx "
            "ON objects (type, created_at DESC, id)"
        )
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS objects_created_idx "
            "ON objects (created_at DESC, id)"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS objects_created_idx")
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS objects_type_created_idx")
