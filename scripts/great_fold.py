"""THE GREAT FOLD's driver — the campaign runs THROUGH the machine (greatfold.py), never
around it. Dry-run is the default everywhere; --execute is the deliberate act.

    .venv/bin/python scripts/great_fold.py survey            # roster + evidence + conflicts
    .venv/bin/python scripts/great_fold.py census            # the honest numbers
    .venv/bin/python scripts/great_fold.py seat thoth        # one seat, dry-run
    .venv/bin/python scripts/great_fold.py seat thoth --execute
    .venv/bin/python scripts/great_fold.py visits            # doorbell sweep, dry-run
    .venv/bin/python scripts/great_fold.py visits --execute

Every executed seat run files its after-review brief on the operator's desk by itself
(fold_seat does it — the machine briefs, not the driver)."""
from __future__ import annotations

import argparse
import asyncio
import json

import asyncpg
from src.actions.core import Actions
from src.config.settings import get_settings
from src.orchestrator.greatfold import (
    demote_visits,
    fold_census,
    fold_seat,
    survey_seats,
)

ACTOR = "ceremony:great-fold"


def _show(obj: object) -> None:
    print(json.dumps(obj, indent=2, default=str))


async def _main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("survey")
    sub.add_parser("census")
    p_seat = sub.add_parser("seat")
    p_seat.add_argument("handle")
    p_seat.add_argument("--execute", action="store_true")
    p_seat.add_argument("--actor", default=ACTOR)
    p_vis = sub.add_parser("visits")
    p_vis.add_argument("--execute", action="store_true")
    p_vis.add_argument("--limit", type=int, default=None)
    p_vis.add_argument("--actor", default=ACTOR)
    args = ap.parse_args()

    pool = await asyncpg.create_pool(get_settings().database_url, min_size=1, max_size=4)
    try:
        actions = Actions(pool)
        if args.cmd == "survey":
            sv = await survey_seats(pool)
            slim = {h: {"house": s.get("house"), "seat_id": s.get("seat_id"),
                        "resident": s["resident_signed"] or s["resident_claimed"],
                        "signed_bases": sorted(s["signed"]),
                        "claimed_bases": sorted(s["claimed"])}
                    for h, s in sv["seats"].items()}
            _show({"seats": slim, "conflicts": sv["conflicts"],
                   "seatless_names": sv["seatless_names"]})
        elif args.cmd == "census":
            _show(await fold_census(pool))
        elif args.cmd == "seat":
            _show(await fold_seat(actions, handle=args.handle, actor=args.actor,
                                  execute=args.execute))
        elif args.cmd == "visits":
            _show(await demote_visits(actions, actor=args.actor, execute=args.execute,
                                      limit=args.limit))
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(_main())
