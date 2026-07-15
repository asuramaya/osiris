"""body_usage — the meter's OTHER dimension: resource-seconds beside the vendor's dollars

Revision ID: 0030
Revises: 0029

`llm_usage` answers "what did the vendor charge?" — a dollar figure the CLI hands us for free.
It has never been able to answer the OTHER question: what did the HAND ITSELF cost, in cores
and RAM, independent of which provider tier summoned it? Ruling `7ff54707` settles that
receipts are UNIFORM across tiers — a body summoned on plain-Linux cgroups must cost exactly as
legibly as one summoned on Ra's hypervisor. "A hand you cannot cost is a hand you cannot
govern." `body_usage` is the sibling ledger this makes possible: one row per dissolved body,
keyed on its `handle` (unique — a body is reaped once), carrying the receipt's own numbers
(core-seconds, wall-seconds, the RAM envelope and its peak, RAM-gib-seconds, exit cause) plus
the seat/project it ran for. Operational telemetry like `llm_usage`, not the event-sourced graph.
"""

from __future__ import annotations

from alembic import op

revision = "0030"
down_revision = "0029"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE body_usage (
            id                    bigserial PRIMARY KEY,
            handle                text NOT NULL UNIQUE,
            provider              text NOT NULL,
            kind                  text NOT NULL,
            project               text,
            seat_anchor           text,
            core_seconds          double precision NOT NULL,
            wall_seconds          double precision NOT NULL,
            ram_envelope_bytes    bigint NOT NULL,
            ram_peak_bytes        bigint,
            ram_gib_seconds       double precision NOT NULL,
            exit_cause            text NOT NULL,
            started_at            timestamptz NOT NULL,
            ended_at              timestamptz NOT NULL,
            receipt_mtime         timestamptz NOT NULL,
            created_at            timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    # windowed reads (the ceiling/digest's "what did bodies cost today?") filter on the EVENT
    # date, never created_at — same discipline as llm_usage_ran_at.
    op.execute("CREATE INDEX body_usage_receipt_mtime ON body_usage (receipt_mtime DESC)")


def downgrade() -> None:
    op.execute("DROP TABLE body_usage")
