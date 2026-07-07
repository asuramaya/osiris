"""agent_wakes — the fleet trigger-hook's visible, rate-capping wake ledger

The trigger-hook (mailbox → wake) lets the WORKER (the alarm clock, rule #2) spawn an agent in a
project when it has unread mail, so coordination doesn't wait for the operator to hand-trigger.
The named danger is the A↔B ping-pong (recursion). This ledger is the bound AND the visibility:
every wake is recorded (which project, on whose message, when), so (a) the per-project rate cap
reads off it — a loop hits the cap on both sides and halts — and (b) the chain is auditable
(membrane #6: the loop may close, but never silently). Ephemeral coordination like the mailbox —
its own plain table, never the event-sourced graph.
"""
from __future__ import annotations

from alembic import op

revision = "0016"
down_revision = "0015"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE agent_wakes (
            id          bigserial PRIMARY KEY,
            to_project  text NOT NULL,
            from_agent  text,
            message_id  bigint,
            woke_at     timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("CREATE INDEX agent_wakes_project_time ON agent_wakes (to_project, woke_at DESC)")


def downgrade() -> None:
    op.execute("DROP TABLE agent_wakes")
