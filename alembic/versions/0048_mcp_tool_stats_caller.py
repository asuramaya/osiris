"""mcp_tool_stats.caller — which AGENT, not just which tool (task #170, Thoth msg 4279)

mcp_tool_stats (0046) answers "which tool is expensive" but has no caller column — it
cannot distinguish 5 calls from 5 different agents from 5 calls from one busy agent, so
a reading from it is a claim about TOOLS that may actually be a claim about whoever was
busiest that hour. Decision 700b6148 named this plainly while reporting the table's
first real numbers; this closes it.

GRAIN CHOSEN, AND WHY (Thoth's own ask: "measure what the row count becomes before you
commit to a grain; if per-agent is too wide, per-seat or per-project may be the honest
compromise"): measured live before writing this — total Agent objects ever: 7,979;
distinct agents active in the last 1/5/15/60 minutes: 9 each. Raw agent_id is a
per-GENERATION identity (a seat mints a new agent_id on every succession/compaction —
this same seat has been "-xxviii"/"-xxix"/"-xxx" all in one session tonight), so grouping
by it would fragment one caller's real cost across dozens of historical rows and answer
a narrower, noisier question than the one being asked. The LINEAGE ROOT (agents.py's own
`_generation()` — already used the identical way in doors.py's `_record`) folds a seat's
generations back to one stable identity, string-only, already-computed from the id we
already have — no new DB query, and in practice one lineage root per named seat (Thoth,
Seshat, Khnum, ...), matching what "per-seat" would give without needing a seat-binding
lookup on the hot path. Row growth is bounded by CONCURRENTLY ACTIVE lineages in a given
60s window (measured: 9 today), not the 7,979-deep historical population — the same
bounding principle that already makes per-tool grouping cheap (only tools actually
called get a row, never the full declared tool surface).

WRITE PATH: rides the EXISTING flush (0046's own background task), not a second write —
`caller` is resolved CACHE-ONLY at the BoundedMCP.call_tool dispatch layer (a lookup in
the already-live `_agents` in-memory identity cache, keyed by `_conn_key`), never a new
`_ident_for` reattach call, which can hit Postgres. A call on a connection whose identity
hasn't been cached yet (the rare case: the very first call before mount()/orient() has
resolved it) is bucketed under the literal 'unattributed' string rather than paying for
a query just to attribute a telemetry row — named in tool_traffic()'s own blind_spots,
not silently absorbed into a real caller's total.

MIGRATION DISCIPLINE (post-0047 house rule, decisions 259e5c5b/a8026bf0): ADD COLUMN in
its own autocommit_block so its (brief, metadata-only) exclusive lock is never held
across anything else — mcp_tool_stats has no backfill to run and is a small, low-row-
count telemetry table, so the actual deadlock shape 0047 hit (a long-held lock crossing
a slow backfill) cannot recur here, but the same discipline applies on principle. NOT
NULL DEFAULT 'unattributed': every future row supplies a real value (a lineage root or
that literal), so no caller has to special-case NULL to read this table.
"""
from __future__ import annotations

from alembic import op

revision = "0048"
down_revision = "0047"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            "ALTER TABLE mcp_tool_stats ADD COLUMN caller text NOT NULL "
            "DEFAULT 'unattributed'"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("ALTER TABLE mcp_tool_stats DROP COLUMN caller")
