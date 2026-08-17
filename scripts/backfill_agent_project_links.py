#!/usr/bin/env python3
"""Backfill the mint_heir edge leak (thread 20af2c95, measured fleet-wide 2026-08-03).

906 of 6,245 live works_in/governs edges pointed from a retired/superseded generation
rather than its lineage's current living head -- mint_heir minted a fresh edge for each
heir but never invalidated the ancestor's, and fold_agent's own estate-move never covered
works_in/governs at all. Both write-side gaps are fixed (mint_heir, folds._move_agent_
estate); this is the one-time repair for the 906 edges that already existed before those
fixes landed. Thin CLI over src.orchestrator.agents.backfill_agent_project_links, which
does the real work via the SAME move_agent_project_links both write-side fixes use.

Dry-run by default, writes nothing. Idempotent: a second run (or an agent someone fixed by
hand in between) finds nothing left to do for that agent.

Usage: uv run python scripts/backfill_agent_project_links.py [--apply] [--base agent:<id> ...]
       (default: dry-run report, every off-head agent; repeat --base to scope to specific
       lineage bases -- a staged rollout, same pattern backfill_seat_bindings.py already uses)
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys

from src.actions.core import Actions
from src.config.dev_env import refuse_silent_live_db
from src.db.pool import create_pool
from src.orchestrator.agents import backfill_agent_project_links

DSN = os.environ.get("DATABASE_URL", "postgresql://osiris:osiris@127.0.0.1:5601/osiris")


async def run(apply: bool, bases: list[str], actor: str) -> None:
    # thread 86d562e0: this DSN's own fallback IS the live fleet graph, no isolated dev
    # instance exists on this box — refuse a silent one-off run against it.
    refusal = refuse_silent_live_db("backfill_agent_project_links")
    if refusal is not None:
        print(refusal, file=sys.stderr)
        raise SystemExit(1)
    pool = await create_pool(DSN, min_size=1, max_size=2)
    actions = Actions(pool)
    only = set(bases) or None
    out = await backfill_agent_project_links(actions, actor=actor, dry_run=not apply,
                                             only_bases=only)
    for p in out["plan"]:
        if p.get("note"):
            print(f"{p['agent']} (status={p['status']}, head={p['head']}) -> SKIPPED "
                  f"({p['note']})")
        elif apply:
            print(f"{p['agent']} (status={p['status']}) -> {p['head']}: moved {p['moved']}")
        else:
            print(f"{p['agent']} (status={p['status']}) -> {p['head']}: "
                  f"would move {p['would_move']} edge(s)")
    if only is not None:
        print(f"scoped to {len(only)} lineage base(s) — {out['scoped_out']} other "
              "off-head agent(s) left untouched")
    if apply:
        print(f"backfill applied — {out['scoped']} agent(s) touched, "
              f"totals {out['moved_total']}")
    else:
        print(f"dry run — {out['total_off_head']} off-head agent(s) fleet-wide, "
              f"{out['scoped']} in scope (pass --apply to write)")
    await pool.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--apply", action="store_true", help="write; default is a dry-run report")
    p.add_argument("--base", action="append", default=[],
                   help="scope to this lineage base (repeatable); default is every "
                        "off-head agent")
    p.add_argument("--actor", default="backfill:20af2c95",
                   help="source_id/actor stamped on every moved link (default: "
                        "backfill:20af2c95, naming the thread this repair closes)")
    args = p.parse_args()
    asyncio.run(run(args.apply, args.base, args.actor))
