"""The overhead lens — neo's eye, ported onto the transcript store (task #34).

The ancestor (github.com/asuramaya/neo, "what's hiding inside your ~/.claude folder")
measured what the HARNESS itself costs: system-reminder injections, the hidden channels
(subagent sidechains, compactions) that multiply a session's data beyond what the
operator sees, and the cache-vs-fresh split that decides what any of it costs. Osiris
ate transcripts for identity and spend but never measured the harness's own overhead.

This grafts neo's channel taxonomy onto the store's grain:
- harness_sessions grows CHANNEL columns: a session row is now either a 'primary'
  (the operator-visible window) or a channel of one ('sidechain' — a Task-tool
  subagent's own transcript under <session>/subagents/, agent-typed from its meta.json;
  'compaction' kept for the ancestor's layout where compaction wrote its own file).
  parent_sid ties a channel to its primary; source_bytes is the on-disk size at last
  ingest (neo's honest fallback when a channel reports no token usage).
- harness_turns grows the per-turn overhead facts: reminders (system-reminder blocks
  injected into a user turn; NULL = not measured, never zero-by-assumption) and
  is_compaction (the isCompactSummary line itself — distinct from is_summary, which
  also covers isMeta and so cannot count compactions).

Revision ID: 0033
Revises: 0032
"""
from alembic import op

revision = "0033"
down_revision = "0032"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE harness_sessions "
        "  ADD COLUMN channel text NOT NULL DEFAULT 'primary', "
        "  ADD COLUMN parent_sid text, "
        "  ADD COLUMN agent_type text, "
        "  ADD COLUMN source_bytes bigint"
    )
    op.execute(
        "CREATE INDEX harness_sessions_parent "
        "  ON harness_sessions (harness, parent_sid) WHERE parent_sid IS NOT NULL"
    )
    op.execute(
        "ALTER TABLE harness_turns "
        "  ADD COLUMN reminders int, "
        "  ADD COLUMN is_compaction boolean NOT NULL DEFAULT false"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS harness_sessions_parent")
    op.execute(
        "ALTER TABLE harness_sessions "
        "  DROP COLUMN IF EXISTS channel, DROP COLUMN IF EXISTS parent_sid, "
        "  DROP COLUMN IF EXISTS agent_type, DROP COLUMN IF EXISTS source_bytes"
    )
    op.execute(
        "ALTER TABLE harness_turns "
        "  DROP COLUMN IF EXISTS reminders, DROP COLUMN IF EXISTS is_compaction"
    )
