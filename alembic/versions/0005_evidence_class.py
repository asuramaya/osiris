"""evidence_class — persist HOW each fact was obtained

Revision ID: 0005
Revises: 0004
Create Date: 2026-06-24

Confidence used to be ~11 uncoordinated magic numbers across parsers, with no way
to ask "why is this fact believed?". We replace that with an evidence-class
taxonomy (parsers/evidence.py) and persist the class on the row it grades, so the
crawl frontier and the subject report can reason about it. Nullable: legacy rows
and analyst edits stay NULL (treated as unknown / medium by the frontier).

Note: current_assertions is `SELECT a.*`, which Postgres freezes to the column
list at view-creation time — so the new column does NOT appear until the view is
recreated. We drop and recreate it here.
"""
from __future__ import annotations

from alembic import op

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE assertions ADD COLUMN evidence_class text")
    op.execute("ALTER TABLE links ADD COLUMN evidence_class text")
    op.execute("DROP VIEW current_assertions")
    op.execute(
        """
        CREATE VIEW current_assertions AS
            SELECT a.*
            FROM assertions a
            WHERE NOT EXISTS (SELECT 1 FROM assertions s WHERE s.supersedes = a.id)
        """
    )


def downgrade() -> None:
    op.execute("DROP VIEW current_assertions")
    op.execute("ALTER TABLE assertions DROP COLUMN evidence_class")
    op.execute("ALTER TABLE links DROP COLUMN evidence_class")
    op.execute(
        """
        CREATE VIEW current_assertions AS
            SELECT a.*
            FROM assertions a
            WHERE NOT EXISTS (SELECT 1 FROM assertions s WHERE s.supersedes = a.id)
        """
    )
