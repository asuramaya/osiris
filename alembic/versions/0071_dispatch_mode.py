"""dispatch_mode: persist dispatch_dm's own per-attempt verdict on the message
(WAVE 27 BUG 3, Thoth DM 11799/11853, thread bc517864)

Revision ID: 0071
Revises: 0070

dispatch_dm already computes an honest per-hop mode (nudged / resumed / mid-turn / ...)
on every call — the immediate leg from send() AND the ~60s backstop sweep
(trigger_mail_tick) both call it, and the sweep already re-evaluates a mid-turn
addressee fresh each tick, falling through to a real nudge once the transcript goes
idle (no new re-dispatch machinery needed — that half already works). What was
missing: the verdict itself vanished into a cron log line and was never written back
onto the message, so nothing durable ever answered "was this specific DM caught
mid-turn, and when did we last check". `dispatch_mode`/`dispatch_mode_at` on
fleet_messages close that — the stop hook's own informational nudge stays exactly
that (informational), this is the audit trail underneath it.
"""
from __future__ import annotations

from alembic import op

revision = "0071"
down_revision = "0070"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE fleet_messages ADD COLUMN dispatch_mode text")
    op.execute("ALTER TABLE fleet_messages ADD COLUMN dispatch_mode_at timestamptz")


def downgrade() -> None:
    op.execute("ALTER TABLE fleet_messages DROP COLUMN dispatch_mode_at")
    op.execute("ALTER TABLE fleet_messages DROP COLUMN dispatch_mode")
