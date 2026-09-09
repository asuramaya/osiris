#!/usr/bin/env python3
"""ZERO-RECIPIENT DM BACKLOG CLOSURE (thread 9d1d41c8's own audit, wave 13 item 1,
operator's word 2026-09-09). graph_lint(check='zero-recipient-dm') found 67 historical DMs
with no message_recipients row at all -- mostly the pre-fix tree-ingest-alarm's own daily
mail to seat:478130b0 (2026-08-19 through 09-02, fixed going forward by thread 358ac1ae)
plus a handful from the DM-loss class 24f52959 already fixed. This closes the BACKLOG
mechanically: NEVER a delete (fleet_messages stays the full historical record, constitution
#3) -- a compensating message_recipients row per orphaned DM, so the check reads zero live
and stays a live-only forward signal instead of re-reporting the same dead rows forever.

THE MARKER: message_recipients has no free-text column, so the compensating row's own
`agent_id` carries the closure's own testimony -- 'system:undeliverable-superseded-by-
<thread>' -- naming the tracking Thread this run opens (idempotent on its own summary; a
re-run finds the SAME thread, never mints a twin) -- rather than impersonating a real
reader. A row this shape settles the check's own NOT EXISTS population without ever
claiming the original addressee actually read it.

`dry_run=True` is the hard default (no flag flips it silently) -- pass --apply to write.

Usage: uv run python scripts/close_zero_recipient_dm_backlog.py [--apply]
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from typing import Any

from src.actions.core import Actions
from src.config.dev_env import refuse_silent_live_db
from src.db.pool import create_pool
from src.orchestrator.mailbox import close_zero_recipient_dm_backlog, zero_recipient_dm_rows

DSN = os.environ.get("DATABASE_URL", "postgresql://osiris:osiris@127.0.0.1:5601/osiris")

_TRACKING_SUMMARY = (
    "ZERO-RECIPIENT DM BACKLOG CLOSURE (wave 13 item 1, operator's word 2026-09-09): the "
    "historical zero-recipient-dm population graph_lint's own audit found, closed with a "
    "compensating message_recipients row per orphaned DM -- never a delete."
)


async def run(*, apply: bool) -> dict[str, Any]:
    refusal = refuse_silent_live_db("close_zero_recipient_dm_backlog")
    if refusal is not None:
        print(refusal, file=sys.stderr)
        raise SystemExit(1)
    pool = await create_pool(
        DSN, min_size=1, max_size=2,
        application_name="osiris-script:close-zero-recipient-dm-backlog")
    actions = Actions(pool)

    rows = await zero_recipient_dm_rows(pool)
    before = len(rows)
    print(f"zero-recipient DMs found: {before}")

    if not apply:
        for r in rows[:10]:
            print(f"  DM #{r['id']} {r['from_agent']} -> {r['to_agent']} ({r['created_at']})")
        if before > 10:
            print(f"  ... and {before - 10} more")
        print("\ndry run — pass --apply to write")
        await pool.close()
        return {"ok": True, "apply": False, "before": before}

    from src.orchestrator.capture import open_thread

    thread_id = await open_thread(
        actions, _TRACKING_SUMMARY, kind="task", arc="Fleet-Hygiene",
        source="script:close_zero_recipient_dm_backlog")
    result = await close_zero_recipient_dm_backlog(pool, thread_ref=str(thread_id))
    print(f"closed {len(result['closed_message_ids'])} rows, marker={result['marker']!r}")
    print(f"before={result['before']} after={result['after']}")
    await pool.close()
    return {"ok": True, "apply": True, "thread": str(thread_id), **result}


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--apply", action="store_true", help="write; default is a dry-run report")
    args = p.parse_args()
    asyncio.run(run(apply=args.apply))
