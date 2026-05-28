"""helper_cache — persistent response cache to make deep expansion cheap

Revision ID: 0004
Revises: 0003
Create Date: 2026-05-28

Deep/repeat cascade expansion costs source fetches, not compute. Caching the raw
connector response per (helper, object) means re-expanding a case, or promoting
after a federate preview, reuses the fetch instead of re-hitting the source.
Combined with idempotent objects + the active-claim dedup, this unlocks
arbitrary expansion depth on a single box: the object universe is finite and
nothing is fetched twice within its TTL.
"""
from __future__ import annotations

from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE helper_cache (
            helper_id        text NOT NULL,
            object_canonical text NOT NULL,
            response         jsonb NOT NULL,
            fetched_at       timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (helper_id, object_canonical)
        )
        """
    )
    op.execute("CREATE INDEX helper_cache_fetched_idx ON helper_cache (fetched_at)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS helper_cache CASCADE")
