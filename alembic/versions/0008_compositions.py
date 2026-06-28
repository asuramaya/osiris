"""compositions — the composer's primitive (saved op-trees over the graph)

Revision ID: 0008
Revises: 0007
Create Date: 2026-06-28

A composition is a saved, forkable spec the substrate executes — the unit that lets
opinion live in what the USER composes, not welded into engine code (a `discrepancy`
becomes a composition, not a .py). It generalizes the subscription (a watch is a
composition with a tripwire execution); the lens is a composition run on demand.
"""
from __future__ import annotations

from alembic import op

revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE compositions (
            id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            name       text NOT NULL UNIQUE,
            kind       text NOT NULL DEFAULT 'lens',  -- lens | watch | ...
            spec       jsonb NOT NULL,                -- the op-tree
            created_at timestamptz NOT NULL DEFAULT now()
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE compositions")
