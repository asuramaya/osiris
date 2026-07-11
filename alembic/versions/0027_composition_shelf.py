"""compositions learn to say what they are — description + section (ruling 923c380f)

Revision ID: 0027
Revises: 0026
Create Date: 2026-07-11

The lens clusterfuck, measured: 19 flat chips where `briefing` sits beside `family-drift`
at equal rank, named in builder-dialect, zero self-description. The composer shell groups
the shelf by PURPOSE and explains each lens:

* section — which shelf: arrive | wall | memory | fleet | engine | casework. Unsectioned
  compositions render under 'more' (never hidden).
* description — one line of "when to open this", shown in the sidebar.
"""
from __future__ import annotations

from alembic import op

revision = "0027"
down_revision = "0026"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE compositions ADD COLUMN description text")
    op.execute("ALTER TABLE compositions ADD COLUMN section text")


def downgrade() -> None:
    op.execute("ALTER TABLE compositions DROP COLUMN description")
    op.execute("ALTER TABLE compositions DROP COLUMN section")
