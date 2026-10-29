"""Two things mount() pays for on every call, made cheap.

1. harness_sessions gets a stored place to resume reading a transcript from.
   `last_byte_offset` is the byte offset in the source file of the line holding the last
   ingested turn, `last_line_crc` the crc32 of that line (stripped), `last_offset_idx` the
   turn index that offset belongs to. An incremental ingest used to read and decode the whole
   file just to COUNT the lines it already held (about 1 s for a 278 MB transcript, on every
   mount whose seat had produced anything since); with these it seeks to the offset and reads
   only what was added. All three are NULL for a session ingested before this migration, and
   the reader falls back to the scan (and records the offset) the first time it sees one.

2. An expression index for the operator-desk brief count `mailbox.desk_briefs_from`, which
   mount() asks for every call. The count is for one agent lineage, but the query could only
   reach the unread operator briefs as a whole (about 2,000 of them, 1,029 heap blocks) and
   discard all but a handful by reading each row's `from_agent`. A lineage's id is the first
   14 characters of `from_agent` (`agent:` plus 8 hex), so a partial index on
   `left(from_agent, 14)` over unread operator briefs makes the lookup a seek.
   EXPLAIN ANALYZE on this database: 6.4 ms, scanning ~2,064 candidate rows, before.

The index is built CONCURRENTLY (own autocommit block, as 0054, 0077 and 0078): a live table
with live readers.

Revision ID: 0079
Revises: 0078
"""
from alembic import op

revision = "0079"
down_revision = "0078"
branch_labels = None
depends_on = None

_INDEX = "fleet_messages_operator_unread_lineage_idx"


def upgrade() -> None:
    op.execute(
        "ALTER TABLE harness_sessions "
        "ADD COLUMN IF NOT EXISTS last_byte_offset bigint, "
        "ADD COLUMN IF NOT EXISTS last_line_crc bigint, "
        "ADD COLUMN IF NOT EXISTS last_offset_idx bigint"
    )
    with op.get_context().autocommit_block():
        op.execute(
            f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {_INDEX} "
            "ON fleet_messages (left(from_agent, 14)) "
            "WHERE to_project = 'operator' AND read_at IS NULL"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_INDEX}")
    op.execute(
        "ALTER TABLE harness_sessions "
        "DROP COLUMN IF EXISTS last_offset_idx, "
        "DROP COLUMN IF EXISTS last_line_crc, "
        "DROP COLUMN IF EXISTS last_byte_offset"
    )
