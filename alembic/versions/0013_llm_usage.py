"""llm_usage — per-call token/cost telemetry for the inference seam

Revision ID: 0013
Revises: 0012
Create Date: 2026-07-05

The auto-ingest (session-miner) and the document extractors borrow a model per call; until
now nothing recorded what they spent, so the cost was an ESTIMATE (per-call size x the rate
cap). Each completion now logs its usage — tokens in/out, cache hits, and (on the CLI
backend) the real cost_usd straight from the response envelope. Operational telemetry like
dev_pulses / watermarks, so a plain append-only table, not the event-sourced ledger.
"""
from __future__ import annotations

from alembic import op

revision = "0013"
down_revision = "0012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE llm_usage (
            id                    bigserial PRIMARY KEY,
            ran_at                timestamptz NOT NULL DEFAULT now(),
            purpose               text NOT NULL,
            model                 text NOT NULL,
            input_tokens          integer NOT NULL DEFAULT 0,
            output_tokens         integer NOT NULL DEFAULT 0,
            cache_read_tokens     integer NOT NULL DEFAULT 0,
            cache_creation_tokens integer NOT NULL DEFAULT 0,
            cost_usd              double precision,
            duration_ms           integer
        )
        """
    )
    op.execute("CREATE INDEX llm_usage_ran_at ON llm_usage (ran_at DESC)")


def downgrade() -> None:
    op.execute("DROP TABLE llm_usage")
