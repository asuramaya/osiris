"""Retention for the two highest-churn append-only tables — outbox and audit_log
(thread e6fd3772 piece 1, operator's word: "resource management ... to not burn compute").

Measured live 2026-08-17: outbox 3.20M rows/608MB (accumulating since 2026-07-02, ~70k
rows/day), audit_log 3.22M rows/689MB (since 2026-06-29, ~66k rows/day) — both NEVER
pruned since the tables were created. Same shape as the telemetry 90-day prune at
compositions.py:512 (an interval cutoff, an indexed delete), scaled up for row counts two
orders of magnitude larger: telemetry's `search_log` prune is a single unconditional
DELETE run opportunistically after every search because the table stays small between
runs; outbox/audit_log needed years to reach millions of rows unpruned, so a single
unbatched DELETE here would hold a lock and generate WAL for a multi-million-row
transaction — BATCHED (a bounded id range per statement) is the load-bearing difference,
not a stylistic one.

COLD BY DEFAULT (the operator's own word, via Thoth's dispatch): unlike #108's casefold/
remote_url deploy steps (a reversible, contradiction-gated merge with its own undo verb),
a retention DELETE has no unmerge — the row is gone. `execute=False` is not just the
default, it is required explicit opt-in every time; there is no environment-variable
"gate stays off unless X" shape here at all, deliberately, so a prune can never fire
just because nobody bothered to unset a flag."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg

# outbox: only PUBLISHED rows are eligible — an unpublished row (published_at IS NULL) is
# still awaiting the worker's own drain and must never be touched, no matter its age (a
# stuck/lagging worker is a SEPARATE alarm — reap_stale_runs's own class of problem — not
# a reason for retention to quietly erase the backlog it should be flagged for instead).
_OUTBOX_ELIGIBLE = "published_at IS NOT NULL AND created_at < $1"
# audit_log has no publish/consume state — pure age-based retention.
_AUDIT_ELIGIBLE = "created_at < $1"


async def _dry_run(pool: asyncpg.Pool, table: str, where: str, days: int) -> dict[str, Any]:
    cutoff = datetime.now(UTC) - timedelta(days=days)
    count = await pool.fetchval(f"SELECT count(*) FROM {table} WHERE {where}", cutoff)
    return {"table": table, "days": days, "cutoff": cutoff.isoformat(),
           "eligible": count, "executed": False}


async def _apply(
    pool: asyncpg.Pool, table: str, where: str, days: int, batch_size: int,
) -> dict[str, Any]:
    cutoff = datetime.now(UTC) - timedelta(days=days)
    deleted = 0
    while True:
        n = await pool.execute(
            f"DELETE FROM {table} WHERE id IN "
            f"(SELECT id FROM {table} WHERE {where} ORDER BY id LIMIT $2)",
            cutoff, batch_size)
        # asyncpg's Execute return is "DELETE <n>" — the count is the only signal a
        # batch ran dry (0 rows matched, nothing left before the cutoff)
        n_deleted = int(n.split()[-1])
        deleted += n_deleted
        if n_deleted < batch_size:
            break
    return {"table": table, "days": days, "cutoff": cutoff.isoformat(),
           "deleted": deleted, "executed": True}


async def outbox_retention(
    pool: asyncpg.Pool, *, days: int = 30, execute: bool = False, batch_size: int = 5000,
) -> dict[str, Any]:
    """PUBLISHED outbox rows older than `days` — 30 is a generous replay/debug window for
    events the worker has already durably delivered; an unpublished row is NEVER eligible
    regardless of age. `execute=False` (the default) only counts; `execute=True` deletes
    in batches of `batch_size`, looping until a batch comes back short (nothing left)."""
    if execute:
        return await _apply(pool, "outbox", _OUTBOX_ELIGIBLE, days, batch_size)
    return await _dry_run(pool, "outbox", _OUTBOX_ELIGIBLE, days)


async def audit_log_retention(
    pool: asyncpg.Pool, *, days: int = 90, execute: bool = False, batch_size: int = 5000,
) -> dict[str, Any]:
    """audit_log rows older than `days` — 90 matches the telemetry search_log precedent
    (compositions.py:512) and stays generous enough for the rare forensic case
    (`mounts.undrop_dead_project_mount` reads a specific audit_log row by id; 90 days is
    ample for a drop anyone would still want to reverse). `execute=False` (the default)
    only counts; `execute=True` deletes in batches of `batch_size`."""
    if execute:
        return await _apply(pool, "audit_log", _AUDIT_ELIGIBLE, days, batch_size)
    return await _dry_run(pool, "audit_log", _AUDIT_ELIGIBLE, days)
