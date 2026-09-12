"""settings — THE SETTINGS MENU's own substrate (ruling be1b2e47, operator 2026-09-12;
thread f4498ab304e4 piece 1, Thoth's GO mail 10040), generalizing backup_settings' own
singleton-table shape (migration 0066) into ONE table every future knob writes into.

WHY NOT ANOTHER SINGLETON TABLE PER KNOB: backup_settings/console_state each needed their
own migration for their own fixed column set — fine for one feature, not for "every
configuration knob osiris has" (settings.py's own ~80 fields, unit-file literals, Python
default constants). This table is EAV-shaped but SCOPED, so a new knob never needs a new
migration: `key` names it (dotted, e.g. 'backup.vault_path'), `scope`/`scope_id` place it
(box-wide today; project/seat room left for later), `value` is its jsonb payload.

DECLARATIONS LIVE IN PYTHON, NOT HERE (src/config/settings_registry.py's own SettingSpec
tuple, ontology/schema.py's own declarative-catalog pattern) — this table holds VALUES
only. A row need not exist for a knob to have a default; `settings(action='list')` reads
the Python registry for metadata and this table only for what's actually been SET.

METADATA-ONLY DDL: a fresh CREATE TABLE, no backfill, no lock-safety concern (Practice
513326d6 governs ALTER on a pre-existing table under concurrent load — irrelevant here).

Revision ID: 0067
Revises: 0066
"""
from __future__ import annotations

from alembic import op

revision = "0067"
down_revision = "0066"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE settings (
            key         text NOT NULL,
            scope       text NOT NULL DEFAULT 'box',
            scope_id    text NOT NULL DEFAULT '',
            value       jsonb NOT NULL,
            updated_by  text NOT NULL,
            rev         bigint NOT NULL DEFAULT 0,
            updated_at  timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (key, scope, scope_id)
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE settings")
