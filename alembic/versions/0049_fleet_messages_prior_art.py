"""fleet_messages.prior_art — the read-side hop of prior-art surfacing (obligation
a6198075, operator's own critique: "why does 'read the graph before rederiving' have
to be a mail instruction, why is that not architecture?")

Prior-art surfacing (record_decision/record_practice, thread 44635c42/ruling 1e6d7367)
only ever fired at WRITE time — nothing fired when a task was DISPATCHED or RECEIVED,
so a coordinator re-asking an already-answered question got no signal, and neither did
the worker who picked it up. Two live specimens Thoth traced the same night this was
scoped: dispatching a grep for a defect an already-open thread (aa6b52af) already named
verbatim, and dispatching a reader fix for what was actually a write-time bug.

send()'s MCP wrapper now runs the SAME search-based prior-art check record_decision
already uses (reusing `_surface_prior_art`, not a second matcher) against a message's
body when grade='ask' or the message is a DM, and persists the hits here — computed
ONCE at send time, not re-run every time a reader opens their inbox. NULLABLE, no
backfill: only new sends going forward carry this; every prior message reads as
"never checked," which is honest (nothing retroactively claims a check that never ran).

MIGRATION DISCIPLINE (post-0047 house rule, decisions 259e5c5b/a8026bf0): ADD COLUMN in
its own autocommit_block; no backfill, no lock held across anything else.
"""
from __future__ import annotations

from alembic import op

revision = "0049"
down_revision = "0048"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("ALTER TABLE fleet_messages ADD COLUMN prior_art jsonb NULL")


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("ALTER TABLE fleet_messages DROP COLUMN prior_art")
