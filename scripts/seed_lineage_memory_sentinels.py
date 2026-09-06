#!/usr/bin/env python3
"""Seed `.osiris-lineage` sentinels for every currently-active seat's memory directory
(thread 4dcc1849, decision f9e47d3c) — the one-time step Thoth's confirmation (DM 7763
item (a)) required before the archive rule ships.

Without this, every currently-active lineage's own memory dir has no sentinel the
instant the archive-on-collision logic ships, so its own very next mount() reports
`memory_migration_needed` — safe (archiving only fires on a NAMED-DIFFERENT sentinel,
none exist yet), but a fleet-wide false-alarm flood the moment every live session
reconnects. This attributes today's real content to its true current holder, once,
deliberately, rather than leaving it to accumulate as noise.

Dry-run by default, writes nothing. Idempotent: a second run finds nothing left to do
for any seat already sentineled (by this script or by an ordinary mount() since).

Usage: uv run python scripts/seed_lineage_memory_sentinels.py [--apply]
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys

from src.config.dev_env import refuse_silent_live_db
from src.db.pool import create_pool
from src.orchestrator.lineage_memory import apply_lineage_memory_seed, plan_lineage_memory_seed

DSN = os.environ.get("DATABASE_URL", "postgresql://osiris:osiris@127.0.0.1:5601/osiris")


async def run(apply: bool) -> None:
    refusal = refuse_silent_live_db("seed_lineage_memory_sentinels")
    if refusal is not None:
        print(refusal, file=sys.stderr)
        raise SystemExit(1)
    pool = await create_pool(
        DSN, min_size=1, max_size=2,
        application_name="osiris-script:seed-lineage-memory-sentinels")
    plan = await plan_lineage_memory_seed(pool)
    for item in plan:
        verb = "seeded" if apply else "would seed"
        print(f"{item['handle']} ({item['seat']}) {item['cwd']} -> {verb} "
              f"{item['lineage_root']} at {item['memory_dir']}")
    if apply:
        apply_lineage_memory_seed(plan)
        print(f"applied — {len(plan)} sentinel(s) written")
    else:
        print(f"dry run — {len(plan)} directory(ies) would be seeded (pass --apply to write)")
    await pool.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--apply", action="store_true", help="write; default is a dry-run report")
    args = p.parse_args()
    asyncio.run(run(args.apply))
