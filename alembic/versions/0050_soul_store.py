"""The soul store, piece 1 — verbatim ingest + hash chain (task #51, ruling 62dc6397).

Osiris eats transcripts twice, now for two different reasons. harness_sessions/
harness_turns (0032) is a DERIVED, disposable index of per-turn FACTS (role, model,
tokens) — rederive() already deletes and resets it after a schema change, on purpose,
because the harness's own file stays authoritative and nothing is lost by forgetting the
index. The soul store is the opposite promise: INFINITE RETENTION (operator ruling
62dc6397, "I want infinite retention in an impenetrable pyramid like an Egyptian
pharaoh") of the RAW BYTES themselves, so Osiris remains the durable copy once a source
transcript is rotated off disk or a laptop dies. Those two promises cannot share a table
or an FK ON DELETE CASCADE — a future rederive()-shaped reset must never be able to
cascade into content nothing else remembers. Two new tables, no relationship to 0032's.

soul_sessions: this store's OWN tiny freshness-gate mirror (source_path, last_line_idx,
last_ingested_at, last_hash) — deliberately not harness_sessions, same reasoning.

soul_lines: one row per RAW JSONL LINE (not per parsed turn) — raw_line is the line's
exact text, untouched. line_hash chains: sha256(prev_hash_bytes + raw_line_bytes),
prev_hash NULL at line_idx 0 — a gap or a tampered line breaks the chain from that point
on, detectable by re-walking and recomputing. No semantic filtering: compaction-summary
and meta lines are stored exactly like every other line, because re-materialize's whole
job is byte-equivalence, not interpretation (that stays harness_turns' job).

Piece 1 covers the claude-code harness only (line-oriented JSONL) — Crush is SQLite-
backed with no line-oriented "raw" concept, a different verbatim strategy out of scope
here on purpose, to keep this piece bounded.

Both tables are CREATE TABLE on names that don't exist yet — no autocommit_block/
CONCURRENTLY need (#166's lesson concerns locks against tables already carrying live
readers/writers; these start empty).

Revision ID: 0050
Revises: 0049
"""
from alembic import op

revision = "0050"
down_revision = "0049"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE soul_sessions (
            harness          text NOT NULL,
            anchor_sid       text NOT NULL,
            source_path      text NOT NULL,
            first_seen_at    timestamptz NOT NULL DEFAULT now(),
            last_ingested_at timestamptz NOT NULL DEFAULT now(),
            last_line_idx    bigint NOT NULL DEFAULT 0,
            last_hash        text,
            PRIMARY KEY (harness, anchor_sid)
        )
    """)
    op.execute("""
        CREATE TABLE soul_lines (
            harness      text NOT NULL,
            anchor_sid   text NOT NULL,
            line_idx     bigint NOT NULL,
            raw_line     text NOT NULL,
            line_hash    text NOT NULL,
            prev_hash    text,
            ingested_at  timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (harness, anchor_sid, line_idx)
        )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS soul_lines")
    op.execute("DROP TABLE IF EXISTS soul_sessions")
