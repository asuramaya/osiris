"""The soul store's cold tier (thread 78efd46d, wave 12 item 2, operator ruling via
decision 64ec1905: "memory gets tiers not deletion (soul store cold tier one compressed
row per session after 30 days unread)").

soul_lines holds one row per raw JSONL line, forever — correct for a hot/recently-touched
session, but a session nobody has read or resumed in 30+ days pays the same per-line
storage cost as one touched an hour ago. This table gives such a session ONE row instead:
its full content gzip-compressed (the exact bytes `_stream_verified_write`'s own writer
would produce — every line plus its trailing newline, concatenated), plus the metadata
needed to keep it honest without the per-line rows: `last_hash` (the hash chain's own
final link, captured before the fold) lets a cold read RECOMPUTE the whole chain from the
decompressed content and compare against this one stored value — the cold tier's own
version of `verify_chain`'s law ("never trust a stored hash in isolation"), collapsed to
a single comparison since per-line hashes no longer exist to re-derive from their
neighbors.

`line_count`/`total_bytes` are receipts from fold time (a fresh install's own soul_lines_
cold rows always agree with a live recompute; a corrupted gzip blob would not) — not load-
bearing for reads, but exactly what a "shrunk by a measured factor" report reads to prove
the win without re-decompressing every cold row just to count.

No FK to soul_sessions (same reasoning as 0050's own soul_lines/soul_sessions split) — a
fold is a projection of soul_lines' content, not a new relationship, and this table must
survive independent of whatever soul_sessions itself does. CREATE TABLE on a name that
doesn't exist yet — no autocommit_block/CONCURRENTLY need (0050's own note).

Revision ID: 0063
Revises: 0062
"""
from alembic import op

revision = "0063"
down_revision = "0062"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE soul_lines_cold (
            harness       text NOT NULL,
            anchor_sid    text NOT NULL,
            line_count    bigint NOT NULL,
            total_bytes   bigint NOT NULL,
            last_hash     text,
            content_gzip  bytea NOT NULL,
            folded_at     timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (harness, anchor_sid)
        )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS soul_lines_cold")
