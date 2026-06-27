"""collection_jobs — placeful-satellite dispatch queue

Revision ID: 0007
Revises: 0006
Create Date: 2026-06-26

The placeless core can't do vantage-bound collection (residential IP, session-bound
browser, a network only reachable from elsewhere). It DISPATCHES such work as a job;
a placeful satellite agent claims it, collects locally, and returns results through
the Actions waist. Coordination is this table (PG = the bus), not an RPC mesh — a
satellite can run on any box that can reach Postgres.

Atomic claim via FOR UPDATE SKIP LOCKED on the queued partial index, mirroring the
helper_runs claim: two satellites never take the same job.
"""
from __future__ import annotations

from alembic import op

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE collection_jobs (
            id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            kind        text NOT NULL,        -- which collector to run (e.g. browser_fetch)
            target      text NOT NULL,        -- what to collect (url / handle / ...)
            vantage     text,                 -- required vantage tag; null = any satellite
            case_id     uuid REFERENCES cases(id),
            status      text NOT NULL DEFAULT 'queued',  -- queued|claimed|done|failed
            claimed_by  text,                 -- satellite id that took it
            result      jsonb,
            error       text,
            created_at  timestamptz NOT NULL DEFAULT now(),
            claimed_at  timestamptz,
            finished_at timestamptz
        )
        """
    )
    op.execute(
        "CREATE INDEX collection_jobs_queued_idx ON collection_jobs (created_at) "
        "WHERE status='queued'"
    )


def downgrade() -> None:
    op.execute("DROP TABLE collection_jobs")
