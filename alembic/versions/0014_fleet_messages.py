"""fleet_messages — the fleet mailbox: directed agent-to-agent coordination

Revision ID: 0014
Revises: 0013
Create Date: 2026-07-05

The fleet already shares MEMORY (the graph); this adds directed COORDINATION — a message
addressed to a PROJECT (the stable address, vs an ephemeral session id), read by whoever works
there. PULL, never push (Osiris has no hands): the recipient reads its inbox when it next
mounts/orients; the server never interrupts a live agent. A message is ephemeral coordination,
NOT durable memory, so it lives in its own plain table (like dev_pulses / handoffs / llm_usage),
never the event-sourced entity graph — the memory stays the durable WHY, the mailbox stays
disposable chatter (channel separation, not steganography: you own the graph, hide from whom?).
"""
from __future__ import annotations

from alembic import op

revision = "0014"
down_revision = "0013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE fleet_messages (
            id           bigserial PRIMARY KEY,
            from_agent   text NOT NULL,
            from_project text,
            to_project   text NOT NULL,
            body         text NOT NULL,
            created_at   timestamptz NOT NULL DEFAULT now(),
            read_at      timestamptz
        )
        """
    )
    op.execute(
        "CREATE INDEX fleet_messages_inbox ON fleet_messages (to_project, read_at, created_at)")


def downgrade() -> None:
    op.execute("DROP TABLE fleet_messages")
