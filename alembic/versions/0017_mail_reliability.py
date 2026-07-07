"""mail reliability — at-least-once delivery, reply correlation, durable mounts

Revision ID: 0017
Revises: 0016
Create Date: 2026-07-07

The mailbox was at-most-once: read_inbox consumed (read_at) BEFORE the response provably
reached the agent, so a severed transport (server bounce, OOM-kill — diagnosed 56f6a0d6)
silently swallowed messages. This makes delivery a LEASE (delivered_at) that an ACK settles
(read_at) — an unsettled message redelivers after the lease instead of vanishing. reply_to /
thread_id make the request→reply lane first-class (a reply routes back to the asker and IS the
ack — replying proves perception). agent_mounts is the durable half of the mount registry: the
in-memory dict dies with every server bounce (wiping the whole fleet's identities at once);
this table lets any call re-attach by the client's job_dir instead of hard-failing
"mount(cwd) first" until the agent notices.
"""
from __future__ import annotations

from alembic import op

revision = "0017"
down_revision = "0016"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE fleet_messages
            ADD COLUMN delivered_at timestamptz,
            ADD COLUMN deliveries   int NOT NULL DEFAULT 0,
            ADD COLUMN reply_to     bigint REFERENCES fleet_messages(id),
            ADD COLUMN thread_id    bigint
        """
    )
    op.execute(
        """
        CREATE TABLE agent_mounts (
            job_dir     text PRIMARY KEY,
            agent_id    text NOT NULL,
            project     text,
            cwd         text NOT NULL,
            model       text,
            session_key text,
            mounted_at  timestamptz NOT NULL DEFAULT now(),
            last_seen   timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("CREATE INDEX agent_mounts_project ON agent_mounts (project, last_seen DESC)")


def downgrade() -> None:
    op.execute("DROP TABLE agent_mounts")
    op.execute(
        "ALTER TABLE fleet_messages DROP COLUMN delivered_at, DROP COLUMN deliveries, "
        "DROP COLUMN reply_to, DROP COLUMN thread_id"
    )
