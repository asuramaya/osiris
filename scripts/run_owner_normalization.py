#!/usr/bin/env python3
"""OWNER-LAW RESIDUE RE-RUN (operator's word via Thoth msg 8606/8618, 2026-09-09):
migration 0059 (src/orchestrator/owner_normalization.py) normalizes every open
obligation's owner onto a durable seat or the literal 'operator' at the moment it runs,
but it is a one-time Alembic migration -- obligations opened or re-owned AFTER 0059
applied are never re-swept. graph_lint(check='unresolvable-owner') found 14 residue rows,
all owner='rotten-apple' (a peer-governed project with no manager on record, so this
resolves to the literal 'operator' per operator ruling thread 614680c6 -- not a fold
thread), plus a 15th open obligation carrying no owner at all.

This is the SANCTIONED PATH the operator's word asked for: re-invoke the exact same
tested resolver 0059 used (plan_owner_normalization / apply_owner_normalization), not a
hand-written raw-SQL reassignment. Documented idempotent in RESULT, not row count --
a re-run mints a fresh same-value assertion confirming "still true at T2" rather than a
skip, which is the same law assert_property already enforces everywhere else.

`--skip-project` (operator ruling via Thoth DM 8650: rotten-apple's own peer_of/
managed_by contradiction between its two governing seats is "that project's own data
defect, for the operator, not ours to touch" -- no resolver patch, no --apply on it)
excludes matching projects from what --apply actually writes/folds, repeatable. The
GENUINELY unowned row (no repo at all, project=None in the plan) is never named by this
flag -- it has no project string to skip and folds under the operator's own ruling that
"the fold is the correct mechanical answer" for it.

`dry_run=True` is the hard default -- pass --apply to write.

Usage: uv run python scripts/run_owner_normalization.py [--apply] [--skip-project NAME ...]
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from typing import Any

from src.actions.core import Actions
from src.config.dev_env import refuse_silent_live_db
from src.db.pool import create_pool
from src.orchestrator.owner_normalization import (
    apply_owner_normalization,
    plan_owner_normalization,
)

DSN = os.environ.get("DATABASE_URL", "postgresql://osiris:osiris@127.0.0.1:5601/osiris")


async def run(*, apply: bool, skip_projects: frozenset[str] = frozenset()) -> dict[str, Any]:
    refusal = refuse_silent_live_db("run_owner_normalization")
    if refusal is not None:
        print(refusal, file=sys.stderr)
        raise SystemExit(1)
    pool = await create_pool(
        DSN, min_size=1, max_size=2,
        application_name="osiris-script:run-owner-normalization")

    plan = await plan_owner_normalization(pool)
    print(f"obligations scanned: {plan['obligations_scanned']}")
    print(f"resolvable: {plan['resolved_count']}")
    print(f"no coordinator (would fold): {plan['no_coordinator_projects']} project(s)")
    for entry in plan["resolved"]:
        print(f"  {entry['thread']}: {entry['current_owner']!r} -> {entry['new_owner']!r}"
              f" ({entry['class']})")
    if skip_projects:
        print(f"skipping (per operator ruling): {sorted(skip_projects)}")

    if not apply:
        print("\ndry run — pass --apply to write")
        await pool.close()
        return {"ok": True, "apply": False, **plan}

    result = await apply_owner_normalization(Actions(pool), skip_projects=skip_projects)
    print(f"\nwritten: {len(result['written'])}, surfaced: {len(result['surfaced'])}")
    await pool.close()
    return {"ok": True, "apply": True, **result}


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--apply", action="store_true", help="write; default is a dry-run report")
    p.add_argument("--skip-project", action="append", default=[],
                   help="exclude this project from --apply; repeatable")
    args = p.parse_args()
    asyncio.run(run(apply=args.apply, skip_projects=frozenset(args.skip_project)))
