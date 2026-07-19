"""Harness-agnostic transcript store (ruling be741d3e, 2026-07-18).

Osiris ate transcripts N×M: every reader (identity, swap-detect, cost, miner backfill)
spoke the harness's NATIVE format directly. A non-Claude mind mounting — the first glm
session on the graph — exposed it: the JSONL read path found nothing, identity fell back
to the seat's inherited intent, and the graph silently believed a glm mind was Fable.

This is the normalized INDEX: per-turn rows eaten from any harness via an adapter, keyed
on the same 8-char anchor the rest of the identity system uses. The harness's own file
stays AUTHORITATIVE (the store keeps source_ref per turn for re-probe on high-stakes
verdicts); the store is a DERIVED fast-read layer that collapses N×M to N adapters + 1
read site. Pattern matches agent_mounts / body_usage: PLAIN TABLE state beside the
event-sourced kernel, not graph objects.

Revision ID: 0032
Revises: 0031
"""
from alembic import op

revision = "0032"
down_revision = "0031"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE harness_sessions (
            anchor_sid       text NOT NULL,
            harness          text NOT NULL,
            session_id       text NOT NULL,
            cwd              text,
            project          text,
            source_path      text NOT NULL,
            first_seen_at    timestamptz NOT NULL DEFAULT now(),
            last_ingested_at timestamptz NOT NULL DEFAULT now(),
            last_turn_idx    bigint NOT NULL DEFAULT 0,
            PRIMARY KEY (harness, anchor_sid)
        )
    """)
    op.execute("CREATE INDEX harness_sessions_cwd ON harness_sessions (cwd)")
    op.execute("CREATE INDEX harness_sessions_project ON harness_sessions (project)")
    op.execute("""
        CREATE TABLE harness_turns (
            anchor_sid      text NOT NULL,
            harness         text NOT NULL,
            turn_idx        bigint NOT NULL,
            role            text NOT NULL,
            model           text,
            provider        text,
            tokens_in       int,
            tokens_out      int,
            cache_read      int,
            cache_write     int,
            cost_usd        double precision,
            duration_ms     int,
            recorded_at     timestamptz,
            ingested_at     timestamptz NOT NULL DEFAULT now(),
            is_summary      boolean NOT NULL DEFAULT false,
            swap_deliberate boolean,
            source_ref      text,
            FOREIGN KEY (harness, anchor_sid)
                REFERENCES harness_sessions(harness, anchor_sid) ON DELETE CASCADE,
            PRIMARY KEY (harness, anchor_sid, turn_idx)
        )
    """)
    op.execute(
        "CREATE INDEX harness_turns_session ON harness_turns (harness, anchor_sid, turn_idx)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS harness_turns")
    op.execute("DROP TABLE IF EXISTS harness_sessions")
