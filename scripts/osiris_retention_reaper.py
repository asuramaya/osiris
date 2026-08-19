#!/usr/bin/env python
"""Retention reaper — the scheduled runner for src.orchestrator.retention (msg 5397 leg
2: "the 898,826-row outbox and audit_log tables get a scheduled reaper with a stated
horizon, defensible and documented, not a periodic sweep somebody remembers").

Horizons are retention.py's own defaults and their own justification lives there:
outbox 30 days (published rows only — an unpublished row is never eligible, no matter
its age), audit_log 90 days (matches the telemetry search_log precedent, stays generous
for the rare forensic undrop). This script is the only thing that ever passes
`execute=True` on a schedule — a human-run `--dry-run` is available for inspection
without touching anything.

CONFESSES into a `job:retention-reaper` watermark (the same `job:%` convention
osiris_fleet_glance.py already scans for staleness) with the before/after row counts —
never a silent sweep.

    .venv/bin/python scripts/osiris_retention_reaper.py [--dry-run]

Runs via deploy/osiris-retention-reaper.timer.
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
_CURSOR_KEY = "job:retention-reaper"
_EVERY_SECS = 86400  # daily — matches deploy/osiris-retention-reaper.timer's own cadence


async def run(*, execute: bool) -> dict:
    from src.db.pool import create_pool
    from src.orchestrator.monitor import set_cursor
    from src.orchestrator.retention import audit_log_retention, outbox_retention

    pool = await create_pool(DSN, min_size=1, max_size=2,
                             application_name="osiris-script:retention-reaper")
    try:
        outbox = await outbox_retention(pool, execute=execute)
        audit = await audit_log_retention(pool, execute=execute)
        report = {"at": datetime.now(UTC).isoformat(), "outbox": outbox, "audit_log": audit}
        await set_cursor(pool, _CURSOR_KEY, json.dumps(
            {"last_ok": report["at"], "every": _EVERY_SECS,
             "outbox_deleted": outbox.get("deleted"), "audit_log_deleted": audit.get("deleted")}))
        return report
    finally:
        await pool.close()


def main() -> None:
    execute = "--dry-run" not in sys.argv
    report = asyncio.run(run(execute=execute))
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
