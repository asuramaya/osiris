"""rooms — the stance switcher (segmentation, not separation) (W2)

Revision ID: 0010
Revises: 0009
Create Date: 2026-06-28

A Room is a saved STANCE the operator switches between (journalist / broker / engineer —
all three at once). It scopes the WORK ARTIFACTS — cases and compositions — to a beat,
but NOT the entity graph: objects/links stay global, so resolution is global and cross-room
connections light up if they're real. Segmentation over the one shared store, never separate
databases (the an earlier attempt trap). `room_id` is nullable: NULL = unassigned, visible only in "All".
"""
from __future__ import annotations

from alembic import op

revision = "0010"
down_revision = "0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE rooms (
            id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            name       text NOT NULL UNIQUE,
            config     jsonb NOT NULL DEFAULT '{}',  -- emoji / default_view / sources (later)
            created_at timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    # the work artifacts carry a room; the graph never does (segmentation, not separation)
    op.execute("ALTER TABLE cases ADD COLUMN room_id uuid REFERENCES rooms(id) ON DELETE SET NULL")
    op.execute(
        "ALTER TABLE compositions ADD COLUMN room_id uuid REFERENCES rooms(id) ON DELETE SET NULL"
    )
    op.execute("CREATE INDEX cases_room_idx ON cases (room_id)")
    op.execute("CREATE INDEX compositions_room_idx ON compositions (room_id)")


def downgrade() -> None:
    op.execute("DROP INDEX compositions_room_idx")
    op.execute("DROP INDEX cases_room_idx")
    op.execute("ALTER TABLE compositions DROP COLUMN room_id")
    op.execute("ALTER TABLE cases DROP COLUMN room_id")
    op.execute("DROP TABLE rooms")
