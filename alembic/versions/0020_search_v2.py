"""search v2 — the FTS engine's index + the misses log (rung 1, thread 0deaec4f)

Revision ID: 0020
Revises: 0019
Create Date: 2026-07-08

The old search read only the `name` property — the front door answered from door plaques.
Two pieces:

  * a partial GIN expression index over the TEXT-bearing assertion values (name, summary,
    rationale) — fn_search queries repeat the same expression so the planner uses it. The
    graph IS the corpus: no shadow table, no sync problem.
  * search_log — retrieval telemetry (telemetry citizenship, like llm_usage: not graph
    data, never event-sourced). Every fn_search call logs (query, caller, hits, top rank);
    the ZERO-HIT RATE is the objective tripwire for unparking embeddings (two independent
    witnesses say lexical-first: our corpus shape + Karpathy's LLM-wiki gist).
"""
from __future__ import annotations

from alembic import op

revision = "0020"
down_revision = "0019"
branch_labels = None
depends_on = None

_TSV = "to_tsvector('english', value #>> '{}')"


def upgrade() -> None:
    op.execute(
        f"CREATE INDEX ix_assertions_fts ON assertions USING gin ({_TSV}) "
        "WHERE name IN ('name','summary','rationale')"
    )
    op.execute(
        """
        CREATE TABLE search_log (
            id          bigserial PRIMARY KEY,
            query       text        NOT NULL,
            caller      text,
            hits        int         NOT NULL,
            top_rank    real,
            searched_at timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("CREATE INDEX ix_search_log_at ON search_log (searched_at)")


def downgrade() -> None:
    op.execute("DROP TABLE search_log")
    op.execute("DROP INDEX ix_assertions_fts")
