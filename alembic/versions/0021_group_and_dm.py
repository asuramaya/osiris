"""fleet mail becomes group-chat + DM — per-recipient read state (thread 6fa9791d)

Revision ID: 0021
Revises: 0020
Create Date: 2026-07-09

The mailbox was a single-reader work-queue: reading a project's inbox LEASED the message on
fleet_messages itself, so the FIRST agent to read hid it from every other agent in the project.
Two co-located agents (handlingtheloop ux + engine, one dir) could not both see a conversation.

Two additions turn it into group-chat + DM:
  * to_agent — a message addressed to a specific agent (a DM). NULL = a project BROADCAST (the
    group chat: every agent in the project sees it).
  * message_recipients — read/lease state PER (message, reader). Each reader (a fleet agent, or
    the reserved 'operator' desk) tracks its OWN delivered/read, so a broadcast is visible to
    all and settled independently. The single-reader lease on fleet_messages is retired; its
    per-message state columns stay for history but the live logic reads message_recipients.
"""
from __future__ import annotations

from alembic import op

revision = "0021"
down_revision = "0020"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE fleet_messages ADD COLUMN to_agent text")
    # a DM has no project recipient — to_project becomes nullable (a message must have a
    # to_project OR a to_agent; the mailbox layer enforces that, not the schema)
    op.execute("ALTER TABLE fleet_messages ALTER COLUMN to_project DROP NOT NULL")
    op.execute("CREATE INDEX ix_fleet_messages_to_agent ON fleet_messages (to_agent)")
    op.execute(
        """
        CREATE TABLE message_recipients (
            message_id   bigint      NOT NULL REFERENCES fleet_messages(id) ON DELETE CASCADE,
            agent_id     text        NOT NULL,
            delivered_at timestamptz,
            read_at      timestamptz,
            deliveries   int         NOT NULL DEFAULT 0,
            PRIMARY KEY (message_id, agent_id)
        )
        """
    )
    op.execute("CREATE INDEX ix_message_recipients_agent ON message_recipients (agent_id)")


def downgrade() -> None:
    op.execute("DROP TABLE message_recipients")
    op.execute("DROP INDEX ix_fleet_messages_to_agent")
    op.execute("ALTER TABLE fleet_messages ALTER COLUMN to_project SET NOT NULL")
    op.execute("ALTER TABLE fleet_messages DROP COLUMN to_agent")
