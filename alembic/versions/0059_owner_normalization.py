"""owner normalization -- every open obligation's owner resolved off a project name, a
dead agent id, or an empty value, onto a durable seat

Ruling 0d863363 item 2 (thread 6d8f87a3): a bare project-name owner resolves to that
project's coordinating seat, a dead/retired generation id resolves to its lineage's live
head, an empty owner (with a repo on record) resolves the same way as a project name. A
row nothing can resolve a coordinator for folds into ONE Thread on the operator's own
backlog per affected project, never one per obligation.

Ships MECHANICALLY (ruling 0d863363: "the shape should carry over to other projects...
as if we were pushing out updates to a stranger's machine") -- never a coordinator's hand
pass. A stranger's install gets the identical normalization on their next `osiris migrate`.

The actual logic (src/orchestrator/owner_normalization.py) needs roster()'s own governed/
shared-house/conflict classification and lineage_head's forward succession walk -- both
already-async, already-tested app code, not something worth re-deriving as raw SQL a
second, drift-prone way (unlike migration 0058, which was cheap and self-contained
enough in pure SQL to stay that way). Alembic migrations otherwise run over a sync
psycopg connection (env.py's own "no sync drivers... application runtime uses asyncpg"
rule, migrations are the one deliberate exception) -- this migration is the first to also
open its OWN short-lived asyncpg pool and run the async application code directly, since
the work here has no schema/DDL half to keep transactionally coupled to (like 0058, a
pure data write, safe to run outside alembic's own sync transaction).
"""
from __future__ import annotations

import asyncio
import os

revision = "0059"
down_revision = "0058"
branch_labels = None
depends_on = None


async def _apply() -> None:
    from src.actions.core import Actions
    from src.db.pool import create_pool
    from src.orchestrator.owner_normalization import apply_owner_normalization

    dsn = os.environ.get("DATABASE_URL", "postgresql://osiris:osiris@127.0.0.1:5432/osiris")
    pool = await create_pool(
        dsn, min_size=1, max_size=2,
        application_name="osiris-migration:0059-owner-normalization")
    try:
        await apply_owner_normalization(Actions(pool))
    finally:
        await pool.close()


def upgrade() -> None:
    asyncio.run(_apply())


def downgrade() -> None:
    # Compensating-event law (constitution #3): a migration never DELETEs a fact it
    # wrote, and the prior owner is preserved in history regardless (multi-source
    # corroboration, never a supersede) -- nothing to reverse.
    pass
