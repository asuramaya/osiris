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
import sys

from src.actions.core import Actions
from src.config.dev_env import refuse_silent_live_db
from src.config.settings import get_settings
from src.db.pool import create_pool
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
    p_camp = sub.add_parser("campaign")  # every seat, ONE survey — dry unless --execute
    p_camp.add_argument("--execute", action="store_true")
    p_camp.add_argument("--actor", default=ACTOR)
    p_vis = sub.add_parser("visits")
    p_vis.add_argument("--execute", action="store_true")
    p_vis.add_argument("--limit", type=int, default=None)
    p_vis.add_argument("--actor", default=ACTOR)
    args = ap.parse_args()

    # thread 86d562e0: get_settings().database_url's class default silently targets
    # 127.0.0.1:5432 — inert only by accident today, no real guard. Refuse a silent
    # one-off run rather than trust that accident.
    refusal = refuse_silent_live_db("great_fold")
    if refusal is not None:
        print(refusal, file=sys.stderr)
        raise SystemExit(1)
    # the HOUSE pool, never bare asyncpg: its jsonb codecs are what Actions' event
    # writes encode through — the bare pool refuses the first kernel write
    pool = await create_pool(get_settings().database_url, min_size=1, max_size=4)
    try:
        actions = Actions(pool)
        if args.cmd == "survey":
            sv = await survey_seats(pool)
            slim = {h: {"house": s.get("house"), "seat_id": s.get("seat_id"),
                        "resident": (s.get("resident_named") or s["resident_signed"]
                                     or s["resident_claimed"]),
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
        elif args.cmd == "campaign":
            sv = await survey_seats(pool)
            out: dict[str, object] = {}
            for h in sorted(sv["seats"]):
                r = await fold_seat(actions, handle=h, actor=args.actor,
                                    execute=args.execute, survey=sv)
                if "error" in r:
                    out[h] = {"skipped": r["error"]}
                    continue
                out[h] = {
                    "resident": r["resident"], "head": r["living_head"],
                    "seat_id": r.get("seat_id"),
                    "will_fold": [f["label"] for f in r["will_fold"]],
                    "flagged": r["flagged"],
                    **({"folded": len(r["folded"]), "refused": r["refused"],
                        "seat_minted": r["seat_minted"], "briefed": r["briefed"]}
                       if args.execute else {}),
                }
            _show(out)
        elif args.cmd == "visits":
            _show(await demote_visits(actions, actor=args.actor, execute=args.execute,
                                      limit=args.limit))
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(_main())
