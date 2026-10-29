"""Two indexes behind the statusline render (/heartbeat) and the turn-end hook (/stop).

1. objects(canonical): the lookups that name an object without its type.

The only index that can answer `WHERE canonical = $1` is objects_type_canonical_key
(type, canonical), and Postgres cannot seek into it without the leading type, so every such
lookup walked the whole index: 547 buffers for one row. A real-session /heartbeat makes eight
of them (the seat's house, its manager, its holds), a Stop hook six, on 65,858 objects.
Measured 2026-10-29 with EXPLAIN (ANALYZE, BUFFERS) on the house lookup: 547 buffer hits
through the composite index; with this one the same lookup is a handful.

2. message_recipients(delivered_at) WHERE read_at IS NULL AND delivered_at IS NOT NULL: the
"in flight" count every render makes ("delivered to someone in the last lease window and not
yet read"). 487,726 of the 489,640 recipient rows are read, so this partial index holds a
few thousand rows. Without it the count joined every project broadcast to all its recipients
(about 32 each) and filtered them: 193,000 buffer touches on a copy of the live tables, 34
with the index, same answer (7).

CONCURRENTLY (own autocommit_block, as 0054): a live table with live readers, so it takes
only a ShareUpdateExclusiveLock and never blocks a read or a write.

Revision ID: 0077
Revises: 0076
"""
from alembic import op

revision = "0077"
down_revision = "0076"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS objects_canonical_idx "
            "ON objects (canonical)"
        )
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS message_recipients_inflight_idx "
            "ON message_recipients (delivered_at) "
            "WHERE read_at IS NULL AND delivered_at IS NOT NULL"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS message_recipients_inflight_idx")
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS objects_canonical_idx")
