"""The telemetry reader — neo's second instrument, ported (task #35).

Claude Code retains failed-to-send telemetry on disk (~/.claude/telemetry/
1p_failed_events.*.json) — event names, session ids, device id, model, platform, CLI
version — and nothing ever reads or prunes it. The ancestor's forensics answered "what's
retained on your disk"; this table is that answer in the store's grain: one row per
retained event, NORMALIZED COLUMNS ONLY — the raw payload is deliberately NOT copied
(a forensics lens measures retention, it must never amplify it; source_ref points back
to the authoritative row on disk, the same law as harness_turns).

harness_telemetry_files is the spend gate's ledger: a file whose mtime hasn't moved
since its last ingest costs one stat and one row lookup, never a read.

Revision ID: 0034
Revises: 0033
"""
from alembic import op

revision = "0034"
down_revision = "0033"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE harness_telemetry (
            source_ref        text PRIMARY KEY,
            recorded_at       timestamptz,
            event             text NOT NULL,
            session_id        text,
            parent_session_id text,
            device_id         text,
            version           text,
            model             text,
            platform          text,
            arch              text,
            ingested_at       timestamptz NOT NULL DEFAULT now()
        )
    """)
    op.execute("CREATE INDEX harness_telemetry_session ON harness_telemetry (session_id)")
    op.execute("CREATE INDEX harness_telemetry_time ON harness_telemetry (recorded_at)")
    op.execute("""
        CREATE TABLE harness_telemetry_files (
            file             text PRIMARY KEY,
            last_ingested_at timestamptz NOT NULL,
            rows             int NOT NULL DEFAULT 0,
            bytes            bigint NOT NULL DEFAULT 0
        )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS harness_telemetry")
    op.execute("DROP TABLE IF EXISTS harness_telemetry_files")
