#!/usr/bin/env python3
"""Backfill the orphan seats (thread 749bf530 / occupancy piece C, 9f566244).

mint_heir's automatic succession only ever MOVES an existing `holds` link (follow_binding,
lineage-wide) -- it never CREATES one from nothing. A seat whose original claim predates the
Seat-object binding (5cef856b) has therefore sat unbound through every generation since,
however many times it has changed hands -- its current holder calling claim_name again would
fix it in one act, but nothing prompts that call. This is the batch cure, a thin CLI over
src.orchestrator.seats.backfill_unbound_seats: it finds every active Seat with no active
holds link and proposes binding it to whoever the assertion world already calls its live
holder -- the same fallback resolve_seat uses for an un-seated lineage, run in bulk.

Dry-run by default, writes nothing. Idempotent: a second run (or a seat someone fixed by hand
in between) finds nothing left to do.

Usage: uv run python scripts/backfill_seat_bindings.py [--apply] [--seat seat:<id> ...]
       (default: dry-run report, every unbound seat; repeat --seat to scope to specific ones —
       the operator's own staged rollout, e.g. Thoth-first, fleet-wide only once that's clean)
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys

from src.actions.core import Actions
from src.config.dev_env import refuse_silent_live_db
from src.db.pool import create_pool
from src.orchestrator.seats import backfill_unbound_seats

DSN = os.environ.get("DATABASE_URL", "postgresql://osiris:osiris@127.0.0.1:5601/osiris")


async def run(apply: bool, seats: list[str]) -> None:
    # thread 86d562e0: this DSN's own fallback IS the live fleet graph, no isolated dev
    # instance exists on this box — refuse a silent one-off run against it.
    refusal = refuse_silent_live_db("backfill_seat_bindings")
    if refusal is not None:
        print(refusal, file=sys.stderr)
        raise SystemExit(1)
    pool = await create_pool(
        DSN, min_size=1, max_size=2,
        application_name="osiris-script:backfill-seat-bindings")
    actions = Actions(pool)
    only = set(seats) or None
    out = await backfill_unbound_seats(actions, dry_run=not apply, only_seats=only)
    for p in out["plan"]:
        if p.get("holder"):
            tail = " (" + p["note"] + ")" if p.get("note") else ""
            print(f"{p['seat_id']} ({p['handle']!r}, {p['house']}) -> {p['holder']}"
                  f" [live={p['live']}]{tail}")
        else:
            print(f"{p['seat_id']} ({p['handle']!r}, {p['house']}) -> UNRESOLVED"
                  + (f" ({p['note']})" if p.get("note") else ""))
    if only is not None:
        print(f"scoped to {len(only)} seat(s) — {out['scoped_out']} other unbound seat(s) "
              "left untouched")
    if apply:
        print(f"bound {out['bound']} of {len(out['plan'])} in scope")
    else:
        print(f"dry run — {out['resolvable']} of {len(out['plan'])} in scope have a "
              "resolvable holder (pass --apply to write)")
    await pool.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--apply", action="store_true", help="write; default is a dry-run report")
    p.add_argument("--seat", action="append", default=[],
                   help="scope to this seat id (repeatable); default is every unbound seat")
    args = p.parse_args()
    asyncio.run(run(args.apply, args.seat))
