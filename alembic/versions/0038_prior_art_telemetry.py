"""prior_art telemetry — search_log learns which kind of standing knowledge a write collided with

Revision ID: 0038
Revises: 0037
Create Date: 2026-07-27

THE THAW (ruling 1e6d7367, piece 6, "INSTRUMENT IT"): every strong prior-art hit at write
time is a MEASURED re-derivation event, logged regardless of whether the caller acts on it
(cites/supersedes/implements/confirms/refutes/acknowledges/ignores) — that population,
aggregated over time, is the fleet's re-derivation ratchet metric graph1000 was missing.
Same incremental-ALTER pattern as 0024 (relaxed) and 0025 (fuzzy/semantic): two nullable
columns on the existing search_log table, no new table.
"""
from __future__ import annotations

from alembic import op

revision = "0038"
down_revision = "0037"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE search_log ADD COLUMN prior_art_kind text")
    op.execute("ALTER TABLE search_log ADD COLUMN prior_art_strong boolean")


def downgrade() -> None:
    op.execute("ALTER TABLE search_log DROP COLUMN prior_art_strong")
    op.execute("ALTER TABLE search_log DROP COLUMN prior_art_kind")
