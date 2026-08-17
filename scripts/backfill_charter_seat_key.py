#!/usr/bin/env python3
"""Migrate `governs` off the Agent and onto the Seat (operator ruling 1db1ff41).

Before this, a charter originated from whichever Agent generation happened to declare it, and
a successor re-declaring couldn't heal an ancestor's grant (invalidate_link needs the exact
from_id) -- so a lineage's EFFECTIVE charter (what orient()/charter()'s lineage-walk showed,
742df26) could silently accumulate repos nobody meant to keep. This is a thin CLI over
src.orchestrator.charter.migrate_charter_to_seat: it resolves every Agent-origin governs link
to the Seat its lineage currently holds, re-declares the union as a fresh Seat-origin charter
(through set_charter itself), and heals the old Agent-origin links.

Dry-run by default, writes nothing. Idempotent: a second run (or a seat someone migrated by
hand in between) finds nothing left to do.

Usage: uv run python scripts/backfill_charter_seat_key.py [--apply] [--seat seat:<id> ...]
       (default: dry-run report, every lineage with a declared charter; repeat --seat to scope
       to specific seats -- a staged rollout, same shape as backfill_seat_bindings.py)
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys

from src.actions.core import Actions
from src.config.dev_env import refuse_silent_live_db
from src.db.pool import create_pool
from src.orchestrator.charter import migrate_charter_to_seat

DSN = os.environ.get("DATABASE_URL", "postgresql://osiris:osiris@127.0.0.1:5601/osiris")


async def run(apply: bool, seats: list[str]) -> None:
    # thread 86d562e0: this DSN's own fallback IS the live fleet graph, no isolated dev
    # instance exists on this box — refuse a silent one-off run against it.
    refusal = refuse_silent_live_db("backfill_charter_seat_key")
    if refusal is not None:
        print(refusal, file=sys.stderr)
        raise SystemExit(1)
    pool = await create_pool(DSN, min_size=1, max_size=2)
    actions = Actions(pool)
    only = set(seats) or None
    out = await migrate_charter_to_seat(actions, dry_run=not apply, only_seats=only)
    for p in out["plan"]:
        print(f"{p['seat_id']} -> {', '.join(p['repos'])}  (from {', '.join(p['from_agents'])})")
    for u in out["unresolved"]:
        print(f"UNRESOLVED: {u['agent_id']} governed {u['repo']!r} -- {u['note']}")
    for r in out["rejected"]:
        print(f"REJECTED at {r['seat_id']}: {r['rejected']}")
    if only is not None:
        print(f"scoped to {len(only)} seat(s)")
    if apply:
        print(f"migrated {out['seats_migrated']} seat(s) of {len(out['plan'])} in scope")
    else:
        print(f"dry run -- {len(out['plan'])} seat(s) in scope, "
              f"{len(out['unresolved'])} agent-link(s) unresolved (pass --apply to write)")
    await pool.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--apply", action="store_true", help="write; default is a dry-run report")
    p.add_argument("--seat", action="append", default=[],
                   help="scope to this seat id (repeatable); default is every migratable seat")
    args = p.parse_args()
    asyncio.run(run(args.apply, args.seat))
