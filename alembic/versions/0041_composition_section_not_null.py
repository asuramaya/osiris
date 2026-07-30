"""compositions.section becomes NOT NULL — the missing "must be sectioned" invariant (task #94)

Revision ID: 0041
Revises: 0040
Create Date: 2026-07-30

Five agent-authored compositions (seat-repair-sweep-*, *-project-distribution) landed with
room_id=NULL AND section=NULL and rendered nowhere reachable: room=NULL excludes a
composition from every room-scoped read (only the rarely-visited god view sees it), and
section=NULL had no structural backstop at all — the CLIENT'S OWN `c.section||'_more'`
fallback (osiris.js) only ever protected the render, never the write.

Root cause: neither the MCP save_composition tool nor the HTTP /compositions route ever ask
for a section, so a fresh save through either always upserts section=NULL directly (the
UPDATE branch's COALESCE-keeps-prior only protects a RE-save, never a genuine CREATE — see
compositions.save_composition's own docstring, updated alongside this migration). The
application-level fix defaults a genuine CREATE's missing section to '_more' (the client's
own existing fallback label, not a new sentinel); this migration is the DB-level backstop —
belt-and-suspenders against any future write path that bypasses save_composition entirely,
same discipline the table's other NOT NULL columns (name, kind, spec) already apply.
"""
from __future__ import annotations

from alembic import op

revision = "0041"
down_revision = "0040"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("UPDATE compositions SET section='_more' WHERE section IS NULL")
    op.execute("ALTER TABLE compositions ALTER COLUMN section SET DEFAULT '_more'")
    op.execute("ALTER TABLE compositions ALTER COLUMN section SET NOT NULL")


def downgrade() -> None:
    op.execute("ALTER TABLE compositions ALTER COLUMN section DROP NOT NULL")
    op.execute("ALTER TABLE compositions ALTER COLUMN section DROP DEFAULT")
