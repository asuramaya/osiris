"""agent_mounts.model_raw — the harness's undoctored display id (context-window tier)

Revision ID: 0022
Revises: 0021
Create Date: 2026-07-09

Naming v3's normalization (the [1m] false-mint fix) made agent_mounts.model canonical —
claude-opus-4-8[1m] is stored bare because a display variant of the same weights is the same
mind. But the bracket carries real information the bare id loses: WHICH context-window tier
the tab runs ([1m] = 1M tokens vs the 200k default). The context lens (operator request,
2026-07-09) needs it to size the window. model_raw keeps the harness's exact string, stamped
by the statusline heartbeat; identity logic never reads it.
"""
from __future__ import annotations

from alembic import op

revision = "0022"
down_revision = "0021"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE agent_mounts ADD COLUMN model_raw text")


def downgrade() -> None:
    op.execute("ALTER TABLE agent_mounts DROP COLUMN model_raw")
