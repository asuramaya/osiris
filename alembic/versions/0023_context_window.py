"""agent_mounts.context_window_size — the harness's own window tier, stamped per render

Revision ID: 0023
Revises: 0022
Create Date: 2026-07-09

The context lens guessed the window from the display id and was confidently wrong (a bare
claude-fable-5 at 125k rendered ctx 63% of an assumed 200k while /context said 13% of 1M).
The statusline payload turns out to carry the truth first-class — context_window.
context_window_size — so the heartbeat stamps it here and the context_window() tool reads
the same number the operator's /context shows, no inference.
"""
from __future__ import annotations

from alembic import op

revision = "0023"
down_revision = "0022"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE agent_mounts ADD COLUMN context_window_size bigint")


def downgrade() -> None:
    op.execute("ALTER TABLE agent_mounts DROP COLUMN context_window_size")
