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
itself derived that way — two `pg_stat_database` reads, not a single instantaneous one.

THE ENVELOPE (msg 5340, "fleet() pool_health shows the envelope"): `caps` rides beside
`by_application` — each daemon's CONFIGURED bound (`osiris_{mcp,worker,api,manager}_
pool_size`, the same settings `src/db/pool.py`'s callers already pass as `max_size`)
next to its current live backend count and the resulting utilization percentage. A
reader gets "how full is each pool RIGHT NOW relative to its own ceiling" in the same
call that already answers "which daemon holds these backends" — the fixed-budget-vs-
`max_connections` arithmetic `docs/DEPLOY.md`'s own envelope section already reasons
about, but read live instead of asserted from memory."""
from __future__ import annotations

from typing import Any

import asyncpg

# Mirrors src/config/settings.py's own defaults — kept here as a fallback ONLY for a
# caller with no live Settings object to hand (never authoritative; `caps` always prefers
# a live `get_settings()` read when available, see `pg_activity_by_app`'s own body).
_KNOWN_DAEMON_POOL_SETTINGS = {
    "osiris-mcp": "osiris_mcp_pool_size",
    "osiris-worker": "osiris_worker_pool_size",
    "osiris-console": "osiris_api_pool_size",
    "osiris-manager": "osiris_manager_pool_size",
}


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
    max_conn_row = await pool.fetchval("SHOW max_connections")
    max_connections = int(max_conn_row) if max_conn_row is not None else None

    from src.config.settings import get_settings
    settings = get_settings()
    caps: dict[str, Any] = {}
    fixed_budget = 0
    for app, setting_name in _KNOWN_DAEMON_POOL_SETTINGS.items():
        cap = getattr(settings, setting_name, None)
        if cap is None:
            continue
        fixed_budget += cap
        current = by_application.get(app, 0)
        caps[app] = {
            "current": current, "cap": cap,
            "pct": round(100 * current / cap, 1) if cap else None,
        }

    return {
        "by_application": by_application,
        "backends": sum(by_application.values()),
        "tx_total": {
            "xact_commit": int(totals["xact_commit"]) if totals else None,
            "xact_rollback": int(totals["xact_rollback"]) if totals else None,
        },
        "caps": caps,
        "max_connections": max_connections,
        "fixed_budget": fixed_budget,
        "headroom": (max_connections - fixed_budget) if max_connections is not None else None,
    }
