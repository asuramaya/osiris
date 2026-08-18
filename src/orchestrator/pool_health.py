"""pool_health — the pg_stat_activity-by-daemon surface (task #180 piece 2 (c), msg 5253).

Bounded per-daemon pools (src/db/pool.py's new `application_name` kwarg, src/config/
settings.py's osiris_{mcp,worker,api,manager}_pool_size) tag every connection a daemon
opens, so `pg_stat_activity.application_name` finally answers "which daemon is holding
these backends" instead of reading as one undifferentiated blob of `asyncpg_...` client
names. `fleet()` folds this in as a best-effort surface, same fail-open shape as
os_bodies/ghost_gap beside it.

TX RATE IS A CUMULATIVE COUNTER, NOT A LIVE RATE: `pg_stat_database.xact_commit` +
`xact_rollback` are running totals since the last stats reset (`pg_stat_reset()`), not a
per-second figure — a single `fleet()` call has no prior sample to diff against. Naming
it `tx_total` (not `tx_rate`) is deliberate: a caller who wants an actual rate takes two
readings and divides by the elapsed time themselves; this module does not guess a window
nobody gave it. The 01:35Z baseline (23 backends, ~8,300 tx/min idle) this task cites was
itself derived that way — two `pg_stat_database` reads, not a single instantaneous one."""
from __future__ import annotations

from typing import Any

import asyncpg


async def pg_activity_by_app(pool: asyncpg.Pool) -> dict[str, Any]:
    """Best-effort — any read failure returns an empty shape, never raises. Scoped to
    THIS database only (`datname = current_database()`): a shared Postgres instance can
    carry other databases' backends, which are not this house's business to report."""
    rows = await pool.fetch(
        "SELECT COALESCE(application_name, '') AS application_name, count(*) AS n "
        "FROM pg_stat_activity WHERE datname = current_database() "
        "GROUP BY application_name ORDER BY n DESC"
    )
    by_application = {r["application_name"] or "(unnamed)": int(r["n"]) for r in rows}
    totals = await pool.fetchrow(
        "SELECT xact_commit, xact_rollback, numbackends "
        "FROM pg_stat_database WHERE datname = current_database()"
    )
    return {
        "by_application": by_application,
        "backends": sum(by_application.values()),
        "tx_total": {
            "xact_commit": int(totals["xact_commit"]) if totals else None,
            "xact_rollback": int(totals["xact_rollback"]) if totals else None,
        },
    }
