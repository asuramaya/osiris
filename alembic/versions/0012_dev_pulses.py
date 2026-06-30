"""dev_pulses — the heartbeat's log (off-the-clock insight for the developer persona)

Revision ID: 0012
Revises: 0011
Create Date: 2026-06-30

The autonomic loop that makes the developer persona come alive. Each `pulse` senses which
repos changed (HEAD moved), re-ingests them, re-runs the lenses, and records a SNAPSHOT of key
metrics plus the FINDINGS (the delta vs the prior pulse) — so the operator returns to a "what
changed since I last looked" digest assembled while they were away. Operational telemetry, not
graph data (like alerts/watermarks), so a plain append-only table, not the event-sourced ledger.
"""
from __future__ import annotations

from alembic import op

revision = "0012"
down_revision = "0011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE dev_pulses (
            id        bigserial PRIMARY KEY,
            ran_at    timestamptz NOT NULL DEFAULT now(),
            synced    jsonb NOT NULL DEFAULT '[]',
            snapshot  jsonb NOT NULL,
            findings  jsonb NOT NULL DEFAULT '[]'
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE dev_pulses")
