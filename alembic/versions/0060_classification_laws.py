"""migration 0060 -- the three classification laws (owner, kind, expiry)

Thread 0af7b202, decision 0d863363's own "ships mechanically, never a coordinator's hand
pass" mandate, #203 to zero. Census before: 242 open, 108 unowned, 87 handle-owned, 94
kindless.

(1) OWNER LAW: an owner is a Seat's own canonical or the literal 'operator', nothing
    else -- resolved via `resolve_owner_seat` (owner_normalization.py, shared with
    thread b5ae6773's write-time refusal gate). An unresolvable row is left untouched,
    NEVER folded into a surfacing Thread (unlike migration 0059) -- the law's own text
    says so explicitly.
(2) KIND LAW: a derived thread is never an obligation; a kindless thread's own summary
    evidence_class decides its kind (derived -> finding, else -> task).
(3) EXPIRY: a derived thread, open, older than 30 days, with no cites/noted_in activity,
    closes via the real `resolve_thread` door with because='expired unclaimed'.

Needs `resolve_owner_seat`'s own async application code (roster()'s governed/shared-
house/conflict classification, lineage_head's own forward walk) -- not a second raw-SQL
re-derivation, same reasoning migration 0059 already gave for the same shape. upgrade()
opens its own short-lived asyncpg pool via asyncio.run() and calls the app code directly
(no DDL here to keep transactionally coupled to, same as 0059).
"""
from __future__ import annotations

import asyncio
import os

revision = "0060"
down_revision = "0059"
branch_labels = None
depends_on = None


async def _apply() -> None:
    from src.actions.core import Actions
    from src.db.pool import create_pool
    from src.orchestrator.migration_0060 import apply_migration_0060

    dsn = os.environ.get("DATABASE_URL", "postgresql://osiris:osiris@127.0.0.1:5432/osiris")
    pool = await create_pool(
        dsn, min_size=1, max_size=2,
        application_name="osiris-migration:0060-classification-laws")
    try:
        await apply_migration_0060(Actions(pool))
    finally:
        await pool.close()


def upgrade() -> None:
    asyncio.run(_apply())


def downgrade() -> None:
    # Compensating-event law (constitution #3): a migration never DELETEs a fact it
    # wrote, and every prior value (owner, kind, thread status) is preserved in history
    # regardless -- nothing to reverse.
    pass
