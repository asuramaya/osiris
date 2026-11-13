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

A concurrent build that fails part way leaves an INVALID index behind, which
`CREATE INDEX CONCURRENTLY IF NOT EXISTS` would silently keep; `src.db.concurrent_index` (the
shared helper for every concurrent migration) drops such a leftover before it builds.

Revision ID: 0081
Revises: 0080
"""
from alembic import op
from src.db.concurrent_index import create_index_concurrently, drop_index_concurrently

revision = "0081"
down_revision = "0080"
branch_labels = None
depends_on = None


def upgrade() -> None:
    create_index_concurrently(
        op, "agent_wakes_message_time", "agent_wakes", "(message_id, woke_at DESC)")


def downgrade() -> None:
    drop_index_concurrently(op, "agent_wakes_message_time")
