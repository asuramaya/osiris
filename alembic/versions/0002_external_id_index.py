"""external_id reverse-lookup index on assertions

Revision ID: 0002
Revises: 0001
Create Date: 2026-05-28

STIX/ATT&CK objects use canonical = STIX id (e.g. intrusion-set--<uuid>); the
human/MITRE handle (G0032, T1566) lives as an `external_id` property. Helpers
link OSINT findings to existing pattern objects by that handle, so the reverse
lookup external_id -> object must be indexed. Partial expression index keeps it
tiny (only external_id assertions) and extracts the jsonb scalar as text.
"""
from __future__ import annotations

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "CREATE INDEX assertions_external_id_idx ON assertions ((value #>> '{}')) "
        "WHERE name = 'external_id'"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS assertions_external_id_idx")
