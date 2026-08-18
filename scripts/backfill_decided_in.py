#!/usr/bin/env python3
"""Backfill task #101's named gap (thread 32e2d5cb): a decision citing a commit sha
BEFORE gitlog reaches it mints no `decided_in` edge at write time, and nothing ever
retries it. This is the batch cure, a thin CLI over
src.orchestrator.capture.backfill_decided_in: a backward pass over every active,
unmerged Decision, re-running the exact same citation-scan/prefix-match logic the live
path (record_decision) already trusts — mechanical and safe because the failure mode
(ruling c5ab0dcb's Mode B, omission) is a race, not an ambiguity: the same matcher run
later succeeds because the commit has since been ingested, not because it got smarter.

Dry-run by default, writes nothing. Idempotent: a second run finds nothing new to mint.
Every citation that could NOT be resolved is named on its own line — a skip is a finding
(the commit was never ingested at all), never silence.

Usage: uv run python scripts/backfill_decided_in.py [--apply]
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys

from src.actions.core import Actions
from src.config.dev_env import refuse_silent_live_db
from src.db.pool import create_pool
from src.orchestrator.capture import backfill_decided_in

DSN = os.environ.get("DATABASE_URL", "postgresql://osiris:osiris@127.0.0.1:5601/osiris")


async def run(apply: bool) -> None:
    # thread 86d562e0: this DSN's own fallback IS the live fleet graph, no isolated dev
    # instance exists on this box — refuse a silent one-off run against it.
    refusal = refuse_silent_live_db("backfill_decided_in")
    if refusal is not None:
        print(refusal, file=sys.stderr)
        raise SystemExit(1)
    pool = await create_pool(
        DSN, min_size=1, max_size=2,
        application_name="osiris-script:backfill-decided-in")
    actions = Actions(pool)
    out = await backfill_decided_in(actions, dry_run=not apply)
    for skip in out["skipped"]:
        print(f"SKIP  {skip['decision']}  commit {skip['sha']} — never ingested")
    print(f"scanned {out['scanned']} active Decision(s); "
          f"{out['already_had']} already had decided_in; "
          f"{len(out['skipped'])} citation(s) unresolvable")
    if apply:
        print(f"minted {out['minted']} new decided_in edge(s)")
    else:
        print(f"dry run — {out['minted']} edge(s) would mint (pass --apply to write)")
    await pool.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--apply", action="store_true", help="write; default is a dry-run report")
    args = p.parse_args()
    asyncio.run(run(args.apply))
