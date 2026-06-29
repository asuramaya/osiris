"""console_state — the shared cursor (real-time Claude↔front sync)

Revision ID: 0011
Revises: 0010
Create Date: 2026-06-29

One shared CONSOLE that both Claude (via MCP) and the browser read and write, so the front
end literally becomes the conversation: when Claude focuses an object or runs a lens, the
screen follows; when the operator navigates, Claude can read their cursor. Single operator /
single box ⇒ a SINGLETON row (id='default') — no multi-session machinery. `rev` is a
monotonic counter; each side ignores stream events it authored (echo-suppression), and
`updated_by` records who moved last. Mirrors how rooms scope work, not the graph: this is
view state, never entity data.
"""
from __future__ import annotations

from alembic import op

revision = "0011"
down_revision = "0010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE console_state (
            id                text PRIMARY KEY DEFAULT 'default',
            room_id           uuid REFERENCES rooms(id) ON DELETE SET NULL,
            composition       text,
            view              text,
            focused_object_id uuid REFERENCES objects(id) ON DELETE SET NULL,
            working_spec      jsonb,
            updated_by        text NOT NULL DEFAULT 'human',
            rev               bigint NOT NULL DEFAULT 0,
            updated_at        timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("INSERT INTO console_state (id) VALUES ('default')")


def downgrade() -> None:
    op.execute("DROP TABLE console_state")
