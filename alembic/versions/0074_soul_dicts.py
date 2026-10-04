"""soul_dicts: the trained compression dictionaries for soul lines (storage redesign)

Revision ID: 0074
Revises: 0073

A transcript line is small and its JSON keys repeat on every line, so zstd with a dictionary
trained on a sample of the real lines stores them in roughly half the space plain zstd needs.
The dictionary is derived from private transcripts, so `sealed_dict` holds it encrypted with
the soul key (it rides a key rotation and every backup like the lines themselves) and it only
ever exists decrypted in a process's memory. A line compressed with one names its dictionary id
inside its own sealed envelope, so an older dictionary stays readable for as long as any line
uses it. `active` marks the one new lines are written with. Idempotent for a retried deploy.
"""
from __future__ import annotations

from alembic import op

revision = "0074"
down_revision = "0073"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS soul_dicts (
            id           serial PRIMARY KEY,
            created_at   timestamptz NOT NULL DEFAULT now(),
            sealed_dict  bytea NOT NULL,
            trained_rows integer NOT NULL,
            sample_bytes bigint NOT NULL,
            active       boolean NOT NULL DEFAULT false
        )
        """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS soul_dicts")
