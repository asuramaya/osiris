"""compositions learn how often they want to be alive — refresh_secs (ruling cf9286b2)

Revision ID: 0040
Revises: 0039
Create Date: 2026-07-29

AUTO-REFRESH (operator, 2026-07-29: "ui needs auto refresh definitely"): volatility is a
property of the QUESTION, not the app — mail and the fleet strip want seconds, docs and
the decision log want never. refresh_secs is nullable; NULL means MANUAL ONLY and is the
default for every composition unless explicitly set. Same incremental-ALTER pattern as
0027 (description/section) — one nullable column on the existing compositions table.
"""
from __future__ import annotations

from alembic import op

revision = "0040"
down_revision = "0039"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE compositions ADD COLUMN refresh_secs integer")


def downgrade() -> None:
    op.execute("ALTER TABLE compositions DROP COLUMN refresh_secs")
