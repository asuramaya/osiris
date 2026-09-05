"""mcp_tool_stats.action — per-ACTION attribution under an object-type dispatcher (task
#202, Thoth msg 7039/7040/7059)

The #202 seat dispatcher (403a744) folded 24 standalone tools into `seat(action=...)`
— tool_traffic() still ranks by bare tool_name alone, so every one of those 30 actions
now collapses into one "seat" row. That defeats the whole point of this table (0046/
0048's own reasoning: a total over too wide a scope is how the next reader gets misled)
the moment a dispatcher exists: "seat is expensive" answers nothing about WHICH action
is actually driving the cost, the same blind spot 0048 closed for caller vs tool.

It also blocks the alias-decay rule this house already runs on (mint_seat/stop/
charter_for/... kept alive as hidden meta={"deprecated": True} aliases, "removed only
at zero traffic") — those hidden names are never called directly anymore (every real
caller goes through `seat(action=...)`), so their own tool_name rows read permanently
zero regardless of real usage, which would misread a heavily-used alias-turned-action
as long-dead. Grouping by (tool_name, action) together gives the true traffic figure
for what used to be `mint_seat` — it is now `('seat', 'mint')`, not `('mint_seat', '')`.

GRAIN: `action` is the dispatcher's own `action` argument when the call supplies one
(any dispatcher tool, present and future — never seat-specific), empty string '' for
every ordinary, non-dispatcher tool call. NOT NULL DEFAULT '' — same "no caller ever
special-cases NULL" discipline 0048 already established for `caller`.

MIGRATION DISCIPLINE (post-0047 house rule, decisions 259e5c5b/a8026bf0): ADD COLUMN in
its own autocommit_block, same reasoning as 0048 — small, low-row-count telemetry
table, no backfill, but the discipline applies on principle regardless of size.
"""
from __future__ import annotations

from alembic import op

revision = "0056"
down_revision = "0055"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            "ALTER TABLE mcp_tool_stats ADD COLUMN action text NOT NULL DEFAULT ''"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("ALTER TABLE mcp_tool_stats DROP COLUMN action")
