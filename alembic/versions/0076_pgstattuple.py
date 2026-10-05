"""pgstattuple: the night compaction measures dead and free space with it (storage redesign)

Revision ID: 0076
Revises: 0075

The nightly database compaction (src/orchestrator/db_compaction.py) decides which big tables
to rewrite from pgstattuple_approx. Without the extension it falls back to the dead-tuple
statistics, which miss space autovacuum already recycled, so a table that is mostly empty
after a big delete could read as "not enough dead space". Creating it here means the
database role no longer needs the right to create extensions at run time.

Idempotent on a live box. The upgrade records, in the watermarks table, that THIS migration
created the extension; the downgrade drops it only then, so an extension an operator
installed by hand is never removed by a downgrade.
"""
from __future__ import annotations

from alembic import op

revision = "0076"
down_revision = "0075"
branch_labels = None
depends_on = None

_MARKER = "migration:0076:created_pgstattuple"


def upgrade() -> None:
    op.execute(
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pgstattuple') THEN
                CREATE EXTENSION pgstattuple;
                INSERT INTO watermarks (key, cursor) VALUES ('{_MARKER}', '1')
                ON CONFLICT (key) DO UPDATE SET cursor = '1', updated_at = now();
            END IF;
        END
        $$
        """
    )


def downgrade() -> None:
    op.execute(
        f"""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM watermarks WHERE key = '{_MARKER}') THEN
                DROP EXTENSION IF EXISTS pgstattuple;
                DELETE FROM watermarks WHERE key = '{_MARKER}';
            END IF;
        END
        $$
        """
    )
