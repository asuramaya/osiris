"""mcp_tool_stats.response_bytes — the byte-per-call column context diet round 3 needs
(Thoth DM 7667, off decision 32b0c88f).

Round 2 (dossier/roster opt-ins) had to substitute live-representative measurement for
the dispatch's own literal ask — a true 24h bytes-per-call log — because this table
never carried a bytes column: BoundedMCP.call_tool (mcp_server.py) times every call but
never sizes the response it already holds in hand. Adding the column here and wiring
the size-and-record write in the same commit means round 3's own byte table is
MEASURED against real fleet traffic, not re-derived from a handful of live probe calls.

GRAIN: same per-window aggregate as `total_ms` — summed bytes across every call in the
(tool, caller, action) bucket for that flush window, never a per-call row (this table
has always been an aggregate, not an event log; a per-call bytes distribution would need
a different table entirely and isn't asked for here).

MIGRATION DISCIPLINE (post-0047 house rule, decisions 259e5c5b/a8026bf0): ADD COLUMN in
its own autocommit_block, same reasoning as 0048/0056 — small, low-row-count telemetry
table, no backfill needed (existing rows read 0 bytes for a window already flushed
before this column existed, an honest "unmeasured", not a wrong number).
"""
from __future__ import annotations

from alembic import op

revision = "0057"
down_revision = "0056"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            "ALTER TABLE mcp_tool_stats ADD COLUMN response_bytes bigint NOT NULL DEFAULT 0"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("ALTER TABLE mcp_tool_stats DROP COLUMN response_bytes")
