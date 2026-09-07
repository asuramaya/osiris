"""migration 0062 -- the worktree fold: ballgem-wt-358/359/361/363/nleg, and any sibling

Thread 922d920c ("trees as a first-class shape"), census 583e2669's own ballgem-wt-*
residue. Before `census_trees`'s own worktree fix (this same thread): a git worktree
living as a SIBLING directory under a census root -- its own `.git` a FILE, not a
directory, the one signal the old walk couldn't tell apart from a real repo -- minted a
phantom SoftwareProject keyed on the worktree dir's own basename.

NOT hardcoded to the five known rows: every active SoftwareProject carrying an
`on_disk_path` is checked LIVE against disk (the same `worktree_parent_path` the fixed
census now uses); a genuine worktree folds into a new Worktree object linked
`worktree_of` its real parent (minting the parent too if it was never censused), then
the old phantom row merges into it via the kernel's own type-agnostic `merge_objects`
(status='merged', `merged_into`, a `same_as` witness -- reversible, nothing deleted). A
path no longer on disk, or a genuine repo root, is left untouched.

Needs `plan_migration_0062`/`apply_migration_0062`'s own async application code (live
git plumbing reads, the same `_mint_or_find_repo` choke point census_trees itself uses)
-- not a raw-SQL re-derivation. upgrade() opens its own short-lived asyncpg pool via
asyncio.run() and calls the app code directly, same shape as 0060/0061.
"""
from __future__ import annotations

import asyncio
import os

revision = "0062"
down_revision = "0061"
branch_labels = None
depends_on = None


async def _apply() -> None:
    from src.actions.core import Actions
    from src.db.pool import create_pool
    from src.orchestrator.migration_0062 import apply_migration_0062

    dsn = os.environ.get("DATABASE_URL", "postgresql://osiris:osiris@127.0.0.1:5432/osiris")
    pool = await create_pool(
        dsn, min_size=1, max_size=2,
        application_name="osiris-migration:0062-worktree-fold")
    try:
        await apply_migration_0062(Actions(pool))
    finally:
        await pool.close()


def upgrade() -> None:
    asyncio.run(_apply())


def downgrade() -> None:
    # Compensating-event law (constitution #3): a migration never DELETEs a fact it
    # wrote, and merge_objects' own fold is already reversible via unmerge_objects on a
    # per-row basis when a specific fold turns out wrong -- nothing to reverse in bulk.
    pass
