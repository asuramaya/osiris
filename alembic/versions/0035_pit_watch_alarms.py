"""pit_watch_alarms — the pair-heartbeat's own ledger (Pit Watch Stage B, thread 449bf55d)

For each managed_by pair, a tick alarms on an ask-graded DM unread beyond the mail lease
window while its addressee is not mid-turn. Append-only, deriving current state by
aggregate query rather than a mutable status column — the same idiom agent_wakes already
uses for this exact subsystem (mode <> 'abandoned' counts live attempts), and the shape
bug 1dbd3d0c's own fix text prescribes ("a failed mode row with retry-after... rather than
naked deletes"). A message stops appearing in the tick's stuck-query the moment it's read
or settled, so no separate 'resolved' tombstone is needed — only 'escalated' stops the
count, mirroring agent_wakes' 'abandoned'.

Revision ID: 0035
Revises: 0034
Create Date: 2026-07-21
"""
from __future__ import annotations

from alembic import op

revision = "0035"
down_revision = "0034"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE pit_watch_alarms (
            id            bigserial PRIMARY KEY,
            worker_seat   text NOT NULL,
            manager_seat  text NOT NULL,
            message_id    bigint NOT NULL,
            addressee_seat text NOT NULL,
            outcome       text NOT NULL,  -- 'sighted' | 'escalated'
            created_at    timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        "CREATE INDEX pit_watch_alarms_message ON pit_watch_alarms (message_id, created_at DESC)"
    )
    op.execute(
        "CREATE INDEX pit_watch_alarms_pair ON pit_watch_alarms (worker_seat, manager_seat, "
        "created_at DESC)"
    )


def downgrade() -> None:
    op.execute("DROP TABLE pit_watch_alarms")
