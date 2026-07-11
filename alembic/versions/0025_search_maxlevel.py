"""search max-level — pg_trgm, the semantic vector store, and the telemetry to watch both

Revision ID: 0025
Revises: 0024
Create Date: 2026-07-11

Operator ruling a0cfcca1: max-level the search engine before the composer resumes. Three
pieces land here: (1) pg_trgm — shipped with contrib, never installed; unlocks the trigram
rung (typo/fuzzy tolerance) fn_search's relaxation ladder was missing. (2) search_vectors —
one embedding per (object, field) winner text, model-stamped, hash-watermarked so backfill
is incremental by construction. real[] not pgvector: the corpus is ~6.5k rows and the image
has no vector extension — brute-force cosine over an in-process cache is right-sized, and
the column type carries no such dependency. (3) search_log learns which door found each
hit set (fuzzy / semantic) — the quality telemetry the hybrid engine is judged by.
"""
from __future__ import annotations

from alembic import op

revision = "0025"
down_revision = "0024"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    op.execute(
        "CREATE TABLE search_vectors ("
        " object_id uuid NOT NULL REFERENCES objects(id) ON DELETE CASCADE,"
        " field text NOT NULL,"
        " model text NOT NULL,"
        " text_hash text NOT NULL,"
        " vec real[] NOT NULL,"
        " embedded_at timestamptz NOT NULL DEFAULT now(),"
        " PRIMARY KEY (object_id, field))")
    op.execute("ALTER TABLE search_log ADD COLUMN fuzzy boolean NOT NULL DEFAULT false")
    op.execute("ALTER TABLE search_log ADD COLUMN semantic boolean NOT NULL DEFAULT false")


def downgrade() -> None:
    op.execute("ALTER TABLE search_log DROP COLUMN semantic")
    op.execute("ALTER TABLE search_log DROP COLUMN fuzzy")
    op.execute("DROP TABLE search_vectors")
    op.execute("DROP EXTENSION IF EXISTS pg_trgm")
