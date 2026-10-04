#!/usr/bin/env python
"""Storage housekeeping by hand: the same steps the daily job runs
(storage_housekeeping_heartbeat), without its time budgets, so the first large backlog can
be worked down in one sitting instead of over several nights.

  1. retire the audit_log rows that only restate an assertions row,
  2. remove the layout heartbeat's old position assertions (graph_x, graph_y,
     graph_layout_v), now stored in the graph_layout table,
  3. fold the superseded assertion rows that restate the row they replaced,
  4. move superseded history older than a week from the hot table to the cold one,
  5. optionally (--compact) rewrite both assertion tables so the freed space returns to the
     operating system. That takes an ACCESS EXCLUSIVE lock on each table in turn, so every
     read and write of the graph waits for it: run it in a quiet minute.

Cold by default: without --execute it only counts what each step would touch. Run it
after the database upgrade that creates the graph_layout table (alembic upgrade head).

    .venv/bin/python scripts/osiris_assertions_housekeeping.py [--execute [--compact]]
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

DSN = os.environ.get("DATABASE_URL", "postgresql://osiris:osiris@127.0.0.1:5601/osiris")
HOT_WINDOW_DAYS = 7


async def run(*, execute: bool, compact: bool) -> dict:
    from src.db.pool import create_pool
    from src.orchestrator.assertion_fold import apply_fold, compact_assertion_tables, plan_fold
    from src.orchestrator.migration_0064 import apply_migration_0064, plan_migration_0064
    from src.orchestrator.retention import (
        assert_property_audit_retirement,
        retire_layout_history,
    )

    pool = await create_pool(DSN, min_size=1, max_size=2,
                             application_name="osiris-script:assertions-housekeeping")
    try:
        report: dict = {"at": datetime.now(UTC).isoformat(), "executed": execute}
        cutoff = datetime.now(UTC) - timedelta(days=HOT_WINDOW_DAYS)
        if not execute:
            report["audit_duplicates"] = await assert_property_audit_retirement(pool)
            report["layout_history"] = await retire_layout_history(pool)
            report["fold"] = await plan_fold(pool)
            report["archive"] = await plan_migration_0064(pool, cutoff=cutoff)
            return report
        report["audit_duplicates"] = await assert_property_audit_retirement(
            pool, execute=True)
        report["layout_history"] = await retire_layout_history(pool, execute=True)
        report["fold"] = await apply_fold(pool)
        report["archive"] = await apply_migration_0064(pool, cutoff=cutoff)
        if compact:
            report["compact"] = await compact_assertion_tables(pool)
        return report
    finally:
        await pool.close()


def main() -> None:
    execute = "--execute" in sys.argv
    compact = "--compact" in sys.argv
    if compact and not execute:
        raise SystemExit("--compact rewrites the tables: it needs --execute")
    report = asyncio.run(run(execute=execute, compact=compact))
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
