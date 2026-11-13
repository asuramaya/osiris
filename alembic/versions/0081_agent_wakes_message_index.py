"""agent_wakes: an index on (message_id, woke_at DESC).

The worker's `trigger_mail` cron ran a constant 4.8 s every 30 s, day and night, because the
sweep of unread direct messages (`trigger._dms_with_unread`) asks, for each of the ~12,000
never-read messages, "what was the last wake for this message?" and `agent_wakes` had no index
on `message_id`: every one of those lookups read the whole table (5,077 rows, 971,000 buffer
hits per sweep, EXPLAIN ANALYZE 4.3 s, plus a JIT compile). The once-per-message brake in
dispatch_dm and the abandoned/wake-count lookups ask the same question per message.

`(message_id, woke_at DESC)` answers "the latest wake of message N" with one index seek and
no sort. Built CONCURRENTLY (own autocommit block, as 0054, 0077, 0078 and 0079): a live table,
ShareUpdateExclusiveLock only, never a reader or a writer.

A concurrent build that fails part way (a deadlock, a cancel, a crash) leaves an INVALID index
behind, and `CREATE INDEX CONCURRENTLY IF NOT EXISTS` then sees the name and does nothing, so a
retry would "succeed" with an index the planner never uses. So the migration first looks for an
invalid index of this name and drops it, then builds.

Revision ID: 0081
Revises: 0080
"""
import sqlalchemy as sa
from alembic import op

# a leftover from a failed concurrent build: present in pg_class, not valid in pg_index
INVALID_SQL = (
    "SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid "
    "WHERE c.relname = 'agent_wakes_message_time' AND NOT i.indisvalid"
)

revision = "0081"
down_revision = "0080"
branch_labels = None
depends_on = None


def upgrade() -> None:
    leftover = op.get_bind().execute(sa.text(INVALID_SQL)).scalar()
    with op.get_context().autocommit_block():
        if leftover:
            op.execute("DROP INDEX CONCURRENTLY IF EXISTS agent_wakes_message_time")
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS agent_wakes_message_time "
            "ON agent_wakes (message_id, woke_at DESC)"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS agent_wakes_message_time")
