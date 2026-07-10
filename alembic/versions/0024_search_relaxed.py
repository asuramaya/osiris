"""search_log.relaxed — which hits came from the ANY-term fallback, not the strict match

Revision ID: 0024
Revises: 0023
Create Date: 2026-07-10

The progressive OR-relaxation closed the recorded zero-hit gap entirely (ruling 40e68cb1:
11/11 historical misses now hit), which retired zero-hits as the embeddings tripwire. The
next honest trigger is relaxed-hit QUALITY — 5 any-term hits are not 5 relevant hits — and
that needs the log to remember which searches only survived on relaxation.
"""
from __future__ import annotations

from alembic import op

revision = "0024"
down_revision = "0023"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE search_log ADD COLUMN relaxed boolean NOT NULL DEFAULT false")


def downgrade() -> None:
    op.execute("ALTER TABLE search_log DROP COLUMN relaxed")
