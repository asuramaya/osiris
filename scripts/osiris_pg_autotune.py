#!/usr/bin/env python
"""Postgres autotune — the scheduled/on-deploy runner for src.orchestrator.pg_autotune
(ruling 45b251ed: "this should be mechanical and not depend on me, autotune
dynamically?"). Computes GUCs from THIS host's live RAM/CPU + the measured daemon
envelope, applies every reloadable one immediately (no interruption to a live backend),
persists any restart-required one via `ALTER SYSTEM` (free, takes effect at the next
restart from any cause), and CONFESSES the before/after into a `job:pg-autotune`
watermark — `pool_health`/`osiris_fleet_glance.py`'s own `job:%` sick-job convention —
so a successor or the fleet glance reads what the machine decided, not just that it ran.

NEVER RESTARTS POSTGRES ITSELF — that stays the operator's/Thoth's own hand (CLAUDE.md's
own law: a worker never restarts services). A restart-required change always prints as
DEFERRED, loudly, never silently dropped.

    .venv/bin/python scripts/osiris_pg_autotune.py [--headroom N]

Runs via deploy/osiris-pg-autotune.timer (scheduled) and from `osiris deploy` itself (on
deploy) — see src/cli.py's cmd_deploy.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

DSN = os.environ.get("DATABASE_URL", "postgresql://osiris:osiris@127.0.0.1:5601/osiris")
_CURSOR_KEY = "job:pg-autotune"
_EVERY_SECS = 86400  # daily — matches deploy/osiris-pg-autotune.timer's own cadence


async def run(*, headroom_target: int = 40) -> dict:
    from src.db.pool import create_pool
    from src.orchestrator.monitor import set_cursor
    from src.orchestrator.pg_autotune import apply_tuning, plan_tuning
    from src.orchestrator.pool_health import pg_activity_by_app

    pool = await create_pool(DSN, min_size=1, max_size=2,
                             application_name="osiris-script:pg-autotune")
    try:
        health = await pg_activity_by_app(pool)
        fixed_budget = health["fixed_budget"] or 56
        plan = await plan_tuning(pool, fixed_budget=fixed_budget, headroom_target=headroom_target)
        result = await apply_tuning(pool, plan)
        report = {
            "at": datetime.now(UTC).isoformat(),
            "host": plan["host"],
            "fixed_budget": fixed_budget,
            "applied": result["applied"],
            "deferred": result["deferred"],
        }
        await set_cursor(pool, _CURSOR_KEY, json.dumps(
            {"last_ok": report["at"], "every": _EVERY_SECS,
             "applied": result["applied"], "deferred": result["deferred"]}))
        return report
    finally:
        await pool.close()


def main() -> None:
    headroom = 40
    if "--headroom" in sys.argv:
        headroom = int(sys.argv[sys.argv.index("--headroom") + 1])
    report = asyncio.run(run(headroom_target=headroom))
    print(json.dumps(report, indent=2, default=str))
    if report["deferred"]:
        print(f"\n⚠ {len(report['deferred'])} restart-required change(s) PERSISTED but NOT "
              "applied — a human restart of osiris-pg picks these up. Not this script's hand.",
              file=sys.stderr)


if __name__ == "__main__":
    main()
