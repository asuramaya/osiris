"""fleet_messages.leased_by — a lease is visible to the mailbox owner, never a black hole

Revision ID: 0019
Revises: 0018
Create Date: 2026-07-08

Live gap (msg 78): a twin leased and answered the owner's mail while the owner's tab read
'mail 0' — true for DELIVERABLE, a lie for 'nothing happening'. The lease now records WHO
holds it, so the owner's inbox (and chrome) can say '1 in flight — leased by agent:… 3m ago'
instead of silence.
"""
from __future__ import annotations

from alembic import op

revision = "0019"
down_revision = "0018"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE fleet_messages ADD COLUMN leased_by text")


def downgrade() -> None:
    op.execute("ALTER TABLE fleet_messages DROP COLUMN leased_by")
