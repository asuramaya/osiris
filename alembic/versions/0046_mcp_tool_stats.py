"""mcp_tool_stats — per-tool call telemetry for the MCP surface (task #167, dispatch msg
4029/4034)

Revision ID: 0046
Revises: 0045
Create Date: 2026-08-11

Nothing in this house could answer "which MCP tool is expensive" (found while scoping the
363k/sec assertions_supersedes_idx load, decision 978962ad) except hand-bracketing one call
at a time. `search_log`/`llm_usage` already do exactly this kind of per-call operational
telemetry for ONE tool each (search, and the inference seam) — never generalized. This
extends that same shape rather than inventing a new one: plain append-only table, telemetry
citizenship like its siblings, never the event-sourced graph.

One row per (tool, 60s window) — a background flush of an in-memory counter at
BoundedMCP.call_tool, not a per-call INSERT (the thing being measured IS the fleet's
most-called surface; a synchronous write on every call would defeat the point).
"""
from __future__ import annotations

from alembic import op

revision = "0046"
down_revision = "0045"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE mcp_tool_stats (
            id           bigserial PRIMARY KEY,
            tool_name    text             NOT NULL,
            window_start timestamptz      NOT NULL,
            window_end   timestamptz      NOT NULL,
            call_count   integer          NOT NULL,
            total_ms     double precision NOT NULL
        )
        """
    )
    op.execute("CREATE INDEX ix_mcp_tool_stats_window ON mcp_tool_stats (window_start DESC)")


def downgrade() -> None:
    op.execute("DROP TABLE mcp_tool_stats")
