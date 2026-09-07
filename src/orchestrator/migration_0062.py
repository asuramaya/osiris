"""MIGRATION 0062, THE WORKTREE FOLD (thread 922d920c, census 583e2669's own ballgem-
wt-358/359/361/363/nleg residue): before `census_trees`'s own worktree fix (this same
thread), a git worktree living as a SIBLING directory under a census root -- its own
`.git` a FILE, the one signal the old walk couldn't tell apart from a real repo -- minted
a phantom SoftwareProject, keyed on the worktree dir's own basename, instead of being
filed as a Worktree of its real parent.

NOT HARDCODED TO BALLGEM'S FIVE (a general repair, not a one-off patch for the five rows
that happened to get measured): every ACTIVE SoftwareProject carrying an `on_disk_path`
is checked LIVE against the disk, via the exact same `worktree_parent_path` the fixed
census now uses -- any other pre-existing misfile, fleet-wide, anywhere this same bug
landed before its fix, self-heals through this one migration too. A path that no longer
exists on disk (the census's own on_disk_path staleness bound, unrelated to this fix)
simply reports "not a worktree" and is left alone, same as any real repo would be.

COMPENSATING, NOTHING DELETED: mints a Worktree object (never retypes the old row in
place -- there is no "retype" primitive in this kernel, and inventing one for a five-row
repair would be new surface for old surface's sake), links it `worktree_of` its parent
(minting the parent too, via the same `_mint_or_find_repo` choke point census_trees
itself uses, if it was somehow never censused), then folds the OLD SoftwareProject into
the NEW Worktree via `Actions.merge_objects` -- the type-agnostic kernel primitive
`fold_project`/`fold_agent`/`fold_seat` are themselves built on, used directly here
because no type-specific wrapper exists yet for a SoftwareProject-into-Worktree fold and
none of fold_project's own SoftwareProject-only estate-reassignment logic (its actor
gate, its live-session guard) applies to a phantom row nothing was ever seated on.
Everything the old row carried (in_repo/works_in edges, prior testimony) stays exactly
where it was; a reader resolves through `merged_into`/`same_as`, the same discipline
every other fold in this graph already relies on.
"""
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

MIGRATION_SOURCE = "migration:0062_worktree_fold"
_EC = EvidenceClass.DIRECT_OBSERVATION.value
_CONF = confidence_for(EvidenceClass.DIRECT_OBSERVATION)

_PATHED_PROJECTS_SQL = """
    SELECT o.id, o.canonical,
        (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id
         AND a.name='name' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS name,
        (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id
         AND a.name='on_disk_path' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1)
        AS on_disk_path
    FROM objects o
    WHERE o.type='SoftwareProject' AND o.status='active'
      AND EXISTS (SELECT 1 FROM current_assertions a WHERE a.object_id=o.id
                  AND a.name='on_disk_path')
"""


async def plan_migration_0062(pool: asyncpg.Pool) -> dict[str, Any]:
    """DRY RUN -- never writes, never shells past a read-only git plumbing check."""
    from src.orchestrator.capture import _resolve_repo
    from src.orchestrator.project_identity import git_current_branch, worktree_parent_path

    rows = await pool.fetch(_PATHED_PROJECTS_SQL)
    to_fold: list[dict[str, Any]] = []
    for row in rows:
        path = row["on_disk_path"]
        if not path:
            continue
        parent_path = worktree_parent_path(path)
        if parent_path is None:
            continue  # not a worktree (gone from disk, or genuinely a real repo root)
        name = row["name"] or row["canonical"].removeprefix("repo:")
        parent_name = None
        parent_obj = await _resolve_repo(pool, Path(parent_path).name)
        if parent_obj is not None:
            parent_name = await pool.fetchval(
                "SELECT a.value #>> '{}' FROM current_assertions a "
                "WHERE a.object_id=$1 AND a.name='name' "
                "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", parent_obj)
        to_fold.append({
            "old_id": row["id"], "old_canonical": row["canonical"], "name": name,
            "on_disk_path": path, "branch": git_current_branch(path),
            "parent_path": parent_path,
            "parent_name": parent_name or Path(parent_path).name,
            "parent_already_registered": parent_obj is not None,
        })
    return {"to_fold": to_fold, "projects_scanned": len(rows)}


async def apply_migration_0062(actions: Actions) -> dict[str, Any]:
    """Applies `plan_migration_0062`'s own plan: mint-or-find the parent, mint the
    Worktree, link `worktree_of`, fold the old SoftwareProject row into it."""
    from src.orchestrator.capture import _mint_or_find_repo, _resolve_repo

    now = datetime.now(UTC)
    plan = await plan_migration_0062(actions.pool)
    folded: list[str] = []

    for entry in plan["to_fold"]:
        parent_obj = await _resolve_repo(actions.pool, entry["parent_name"])
        if parent_obj is None:
            try:
                parent_obj = await _mint_or_find_repo(
                    actions, entry["parent_name"], now, source=MIGRATION_SOURCE,
                    evidence_class=_EC, confidence=_CONF)
                await actions.assert_property(parent_obj, "on_disk_path", entry["parent_path"],
                                              MIGRATION_SOURCE, now, _CONF,
                                              evidence_class=_EC)
            except ValueError:
                continue  # malformed parent name -- refuse this entry, keep walking

        tree_obj = await actions.create_or_find_object(
            "Worktree", f"worktree:{entry['name']}", MIGRATION_SOURCE)
        await actions.assert_property(tree_obj, "on_disk_path", entry["on_disk_path"],
                                      MIGRATION_SOURCE, now, _CONF, evidence_class=_EC)
        if entry["branch"]:
            await actions.assert_property(tree_obj, "branch", entry["branch"],
                                          MIGRATION_SOURCE, now, _CONF, evidence_class=_EC)
        exists = await actions.pool.fetchval(
            "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='worktree_of' "
            "AND (valid_until IS NULL OR valid_until > now()) LIMIT 1", tree_obj, parent_obj)
        if not exists:
            await actions.create_link(tree_obj, parent_obj, "worktree_of", MIGRATION_SOURCE,
                                      now, _CONF, evidence_class=_EC)

        if tree_obj != entry["old_id"]:
            already_merged = await actions.pool.fetchval(
                "SELECT status='merged' FROM objects WHERE id=$1", entry["old_id"])
            if not already_merged:
                await actions.merge_objects(
                    winner_id=tree_obj, loser_id=entry["old_id"],
                    justification=f"census pre-fix worktree misfile, migration 0062: "
                    f"{entry['old_canonical']} was really a worktree of "
                    f"{entry['parent_name']!r}",
                    actor=MIGRATION_SOURCE)
                folded.append(entry["old_canonical"])

    return {"folded": folded, "projects_scanned": plan["projects_scanned"]}
