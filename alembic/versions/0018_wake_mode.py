"""agent_wakes.mode — the dispatch ledger learns HOW each wake happened

Revision ID: 0018
Revises: 0017
Create Date: 2026-07-08

Resume-not-mint (thread 9f2ddb44, heinrich's problem shape): the trigger's dispatch order
becomes deliver → resume → mint, and the ledger must record which lane fired — 'resume'
(the owner's own session continued via claude --resume) vs 'mint' (a fresh twin). The
alternation guard reads this column: a resume that never leased its mail is not retried —
the next wake for that message mints.
"""
from __future__ import annotations

from alembic import op

revision = "0018"
down_revision = "0017"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE agent_wakes ADD COLUMN mode text NOT NULL DEFAULT 'mint'")


def downgrade() -> None:
    op.execute("ALTER TABLE agent_wakes DROP COLUMN mode")
