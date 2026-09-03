#!/usr/bin/env python3
"""THE ANCHOR INVARIANT (Thoth msg 6546, operator's words: "the anchor should always end up
under ~/.osiris/seats hence the call of migration"). Read-only detector — piece 1 of 4.

Thin wrapper over identity_heal.detect_anchor_invariant_violations (the same function now
also ARMED at deploy time, informationally, in cli.cmd_deploy — this script exists for an
ad-hoc terminal run, never a second copy of the query).

Usage: uv run python scripts/detect_anchor_invariant_violations.py
"""
from __future__ import annotations

import asyncio
import os

from src.actions.core import Actions
from src.db.pool import create_pool
from src.orchestrator.identity_heal import detect_anchor_invariant_violations
from src.orchestrator.offices import _default_office_root

DSN = os.environ.get("DATABASE_URL", "postgresql://osiris:osiris@127.0.0.1:5601/osiris")


async def main() -> None:
    pool = await create_pool(DSN, min_size=1, max_size=2,
                              application_name="osiris-script:detect-anchor-invariant")
    root = _default_office_root()
    print(f"office root: {root}")

    result = await detect_anchor_invariant_violations(Actions(pool))

    total_seats = await pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Seat' AND status='active'")
    no_anchor = await pool.fetchval(
        "SELECT count(*) FROM objects o WHERE o.type='Seat' AND o.status='active' "
        "AND NOT EXISTS (SELECT 1 FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='anchor_cwd')")

    print(f"\ntotal active seats: {total_seats}")
    print(f"seats with NO anchor_cwd at all: {no_anchor}")
    print(f"\ncurrent anchor_cwd rows OUTSIDE the office root: {len(result['outside_root'])}")
    for r in result["outside_root"]:
        print(f"  {r['seat']} ({r['handle']}): {r['value']!r} "
              f"src={r['source_id']} @{r['observed_at']}")

    print(f"\nseats with >1 CURRENT anchor_cwd row (the supersession-leak shape): "
          f"{len(result['multi_current'])}")
    for m in result["multi_current"]:
        print(f"  {m['seat']} ({m['handle']}):")
        for r in m["rows"]:
            print(f"    {r['value']!r} src={r['source_id']} @{r['observed_at']}")

    both = {m["seat"] for m in result["multi_current"]} & \
           {r["seat"] for r in result["outside_root"]}
    print(f"\nseats hitting BOTH axes (multi-row AND at least one outside-root value — the "
          f"repair's actual target population, piece 4): {sorted(both)}")

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
