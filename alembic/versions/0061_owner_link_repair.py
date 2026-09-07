"""migration 0061 -- owner-law repair: in_repo link, not a nonexistent repo property

Census 583e2669 (msg 7999), Thoth's ruling (msg 8000/8001, decision 431537e5's own
follow-on): migration 0060's own `repo` derivation for a Thread read off a `repo`
PROPERTY assertion that structurally never exists -- `link_repo` (capture.py) attaches
a project ONLY via the `in_repo` LINK, never a property. This silently misfired
migration 0060's own project-coordinator fallback on both its empty-owner rung and its
present-but-unresolvable-owner rung, for every thread, since the day it shipped.

Fixed at the source, migration_0060.py's `_OPEN_THREADS_SQL` (idempotent by
construction, not forked into a new module -- the bug was in the read, not the law),
plus `resolve_owner_seat` (owner_normalization.py): an `agent:<...>` owner shaped like
an id that resolves to no live seat used to return None immediately, the one prefix
that never fell through to the bare-handle or project-coordinator rungs a plain string
already got. Both fixes are shared with thread b5ae6773's write-time gate, so this
migration is simply `apply_migration_0060` re-invoked with the corrected code live --
every row the deploy gap, the repo-derivation bug, or the agent-id short-circuit left
stranded gets its one more touch:
  - 8 unowned rows: the coordinator fallback can now actually find their project.
  - 5 rows owned by the literal string 'osiris' (pre-c0e542c regrowth, closed for new
    writes as of main 548421f): resolve to osiris's own coordinating seat.
  - agent:deckard / agent:d00dbe16 (4 rows, malformed, no live seat behind either):
    now fall through to their project's coordinator instead of stopping cold.
rotten-apple's 15 (Ptah/Ra, no coordinator -- a genuine management conflict, not a data
bug) and compacttest's 1 (an expected, deliberately-retired test seat) are left
untouched by design; both stay on the operator's desk, not this migration's.

Census correction on the record (msg 8001): a merged/archived Thread keeping its own
'open' status PROPERTY assertion forever is not a defect -- `merge_objects`
(actions/core.py) documents this as deliberate ("assertions are never rewritten --
provenance survives"; resolve on read). Every reader that matters here (this migration,
graph_lint's own checks) already gates on `o.status='active'` first; the 92-row
discrepancy was an ad hoc census SQL missing that same gate, not a graph gap -- no
change made for it, on purpose.
"""
from __future__ import annotations

import asyncio
import os

revision = "0061"
down_revision = "0060"
branch_labels = None
depends_on = None


async def _apply() -> None:
    from src.actions.core import Actions
    from src.db.pool import create_pool
    from src.orchestrator.migration_0060 import apply_migration_0060

    dsn = os.environ.get("DATABASE_URL", "postgresql://osiris:osiris@127.0.0.1:5432/osiris")
    pool = await create_pool(
        dsn, min_size=1, max_size=2,
        application_name="osiris-migration:0061-owner-link-repair")
    try:
        await apply_migration_0060(Actions(pool))
    finally:
        await pool.close()


def upgrade() -> None:
    asyncio.run(_apply())


def downgrade() -> None:
    # Compensating-event law (constitution #3): a migration never DELETEs a fact it
    # wrote, and every prior value is preserved in history regardless -- nothing to
    # reverse.
    pass
