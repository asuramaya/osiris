"""sweep_ledger — per-enqueue completion tracking for the death rite's doorbell (Finding A,
thread 5177057a; Thoth's design approval, DM 1326).

PreCompact -> sweep_route only ever ENQUEUED an async arq job with no confirmation it ran.
The existing fallback (B7, the orphan reaper) catches a transcript that never got ANY
successful sweep — but mark_swept's watermark is a one-time-ever boolean per transcript file,
so once a lineage's FIRST sweep ever succeeds, the reaper is permanently blind to that same
file afterward. A dropped enqueue on compaction #2/#3/#N (a session compacting every few
minutes is exactly this shape) is never caught by anything.

This ledger tracks ONE ROW PER ENQUEUE ATTEMPT (not one row per transcript, ever) so a
lightweight watchdog can retry a specific stuck attempt without needing a stored attempt
counter: the retry ceiling is computed from `enqueued_at`'s age at query time. Two indexes,
one per access pattern — not the single index literally named in the ask, flagged plainly in
the build report: a partial index on the still-pending rows (the watchdog's exact query
shape, and self-shrinking as rows complete) and a per-session index (orient()'s hot-path
point lookup, checking the CALLING lineage's own most recent sweep before trusting it).

Revision ID: 0036
Revises: 0035
Create Date: 2026-07-26
"""
from __future__ import annotations

from alembic import op

revision = "0036"
down_revision = "0035"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE sweep_ledger (
            id              bigserial PRIMARY KEY,
            transcript_path text NOT NULL,
            session_id      text NOT NULL,
            enqueued_at     timestamptz NOT NULL DEFAULT now(),
            completed_at    timestamptz
        )
        """
    )
    op.execute(
        "CREATE INDEX sweep_ledger_pending_idx ON sweep_ledger (enqueued_at) "
        "WHERE completed_at IS NULL"
    )
    op.execute(
        "CREATE INDEX sweep_ledger_session_idx ON sweep_ledger (session_id, enqueued_at DESC)"
    )


def downgrade() -> None:
    op.execute("DROP TABLE sweep_ledger")
