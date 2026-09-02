#!/usr/bin/env python3
"""THE ANCHOR INVARIANT (Thoth msg 6546, operator's words: "the anchor should always end up
under ~/.osiris/seats hence the call of migration"). Read-only detector — piece 1 of 4.

Reports, for every active Seat:
  (a) a current anchor_cwd OUTSIDE the office root (`offices._default_office_root()`,
      OSIRIS_OFFICE_ROOT-aware — never a second copy of that resolution)
  (b) more than one CURRENT anchor_cwd row at all (the supersession-leak shape: nothing
      overwrote the good value, a second one was added beside it, so every LIMIT-1 read
      is a coin flip)

These are separate axes on purpose (#103/#141's own law: a surface that says "these
disagree," never one that silently collapses). A seat can be outside-root with only one
current row (a clean, deliberate — if invariant-violating — anchor) or multi-row while
still resolving inside the root (the corrupted-but-lucky case). Piece 4's repair only ever
acts where BOTH are true for a specific value: an outside-root row coexisting with an
inside-root row on the same seat.

Usage: uv run python scripts/detect_anchor_invariant_violations.py
"""
from __future__ import annotations

import asyncio
import os
from typing import Any

import asyncpg
from src.db.pool import create_pool
from src.orchestrator.offices import _default_office_root

DSN = os.environ.get("DATABASE_URL", "postgresql://osiris:osiris@127.0.0.1:5601/osiris")


async def scan(pool: asyncpg.Pool) -> dict[str, list[dict[str, Any]]]:
    root = str(_default_office_root())
    rows = await pool.fetch(
        "SELECT o.canonical AS seat, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='handle' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS handle, "
        " a.id, a.value #>> '{}' AS v, a.source_id, a.observed_at, a.confidence "
        "FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE a.name='anchor_cwd' AND o.type='Seat' AND o.status='active' "
        "ORDER BY o.canonical, a.observed_at"
    )
    by_seat: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        by_seat.setdefault(r["seat"], []).append(dict(r))

    outside_root: list[dict[str, Any]] = []
    multi_current: list[dict[str, Any]] = []
    for seat, seat_rows in by_seat.items():
        handle = seat_rows[0]["handle"]
        if len(seat_rows) > 1:
            multi_current.append({
                "seat": seat, "handle": handle,
                "rows": [{"value": r["v"], "source_id": r["source_id"],
                          "observed_at": r["observed_at"].isoformat()} for r in seat_rows],
            })
        for r in seat_rows:
            v = r["v"]
            if v and not (v == root or v.startswith(root.rstrip("/") + "/")):
                outside_root.append({
                    "seat": seat, "handle": handle, "value": v,
                    "source_id": r["source_id"], "observed_at": r["observed_at"].isoformat(),
                })

    return {"outside_root": outside_root, "multi_current": multi_current,
            "no_anchor_at_all": []}


async def main() -> None:
    pool = await create_pool(DSN, min_size=1, max_size=2,
                              application_name="osiris-script:detect-anchor-invariant")
    root = _default_office_root()
    print(f"office root: {root}")

    result = await scan(pool)

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
