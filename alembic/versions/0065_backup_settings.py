"""backup_settings — the backup config panel's write half (Wave 21, operator's word
2026-09-11, thread f04cce36 piece 3)

The scope report (annotated on f04cce36) found this whole domain has no config layer at
all today: every setting is a literal baked into a systemd unit file or a Python default-
argument constant. This table is the FIRST one. Single operator / single box, exactly the
same reasoning `console_state` (0011) already used: a SINGLETON row (id='default'), a
monotonic `rev`, `updated_by` recording who moved it last — the write door's own
`backup_settings_write` stamps the caller's resolved identity there, same law
`charter_for`'s own `because`/actor testimony holds.

Revision ID: 0065
Revises: 0064
Create Date: 2026-09-12
"""
from __future__ import annotations

from alembic import op

revision = "0065"
down_revision = "0064"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE backup_settings (
            id                  text PRIMARY KEY DEFAULT 'default',
            vault_path          text,
            timer_schedules     jsonb NOT NULL DEFAULT '{}'::jsonb,
            offbox_repositories jsonb NOT NULL DEFAULT '[]'::jsonb,
            updated_by          text NOT NULL DEFAULT 'operator',
            rev                 bigint NOT NULL DEFAULT 0,
            updated_at          timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("INSERT INTO backup_settings (id) VALUES ('default')")


def downgrade() -> None:
    op.execute("DROP TABLE backup_settings")
