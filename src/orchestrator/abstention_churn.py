"""Collapse the churn a repeated abstention left behind.

Before the abstention writer became idempotent, two sweeps rewrote one unchanged verdict
(`derivation_abstained_in_repo`) every pass, leaving thousands of superseded copies per
object. This keeps the first and the latest unresolved row per object, hot and cold, and
deletes the copies between them. A resolved marker row is never touched.

This deletes history, so it is a backfill target that does nothing until it is told to:
dry run by default, an apply needs a `because`, and it works in small id chunks (one short
transaction each) so it never holds a long lock. A run that hits its time budget reports
`finished: False`; running it again continues, since rows already gone cost nothing."""
from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any

import asyncpg

_NAME = "derivation_abstained_in_repo"
_TABLES = ("assertions_hot", "assertions_cold")
_BY_OBJECT_CAP = 500

# every unresolved abstention row, both tiers, ranked oldest-first and newest-first per
# object; the rows a collapse drops are those that are neither the first nor the latest
_PLAN_SQL = f"""
WITH ab AS (
  SELECT id, object_id, observed_at, 'assertions_hot' AS tier, pg_column_size(a.*) AS bytes
  FROM assertions_hot a WHERE name='{_NAME}' AND NOT (value ? 'resolved')
  UNION ALL
  SELECT id, object_id, observed_at, 'assertions_cold', pg_column_size(a.*)
  FROM assertions_cold a WHERE name='{_NAME}' AND NOT (value ? 'resolved')),
ranked AS (
  SELECT *,
    first_value(id) OVER (PARTITION BY object_id ORDER BY observed_at, id) AS first_id,
    first_value(id) OVER (PARTITION BY object_id ORDER BY observed_at DESC, id DESC)
      AS latest_id,
    first_value(tier) OVER (PARTITION BY object_id ORDER BY observed_at DESC, id DESC)
      AS latest_tier
  FROM ab)
SELECT id, object_id, observed_at, tier, bytes, first_id, latest_id, latest_tier
FROM ranked
"""


async def _plan(pool: asyncpg.Pool) -> tuple[dict[uuid.UUID, dict[str, Any]], int, int]:
    rows = await pool.fetch(_PLAN_SQL)
    objects: dict[uuid.UUID, dict[str, Any]] = {}
    for r in rows:
        o = objects.setdefault(r["object_id"], {
            "first_id": r["first_id"], "latest_id": r["latest_id"],
            "latest_tier": r["latest_tier"], "drop": {t: [] for t in _TABLES},
            "bytes": 0, "oldest": None, "newest": None})
        if r["id"] in (r["first_id"], r["latest_id"]):
            continue
        o["drop"][r["tier"]].append(r["id"])
        o["bytes"] += r["bytes"]
        if o["oldest"] is None or r["observed_at"] < o["oldest"]:
            o["oldest"] = r["observed_at"]
        if o["newest"] is None or r["observed_at"] > o["newest"]:
            o["newest"] = r["observed_at"]
    return objects, len(rows), sum(len(o["drop"][t]) for o in objects.values() for t in _TABLES)


async def collapse_abstention_churn(
    pool: asyncpg.Pool, *, actor: str, dry_run: bool = True, because: str | None = None,
    chunk_rows: int = 2000, max_seconds: float = 25.0,
) -> dict[str, Any]:
    """Keep the first and the latest unresolved `derivation_abstained_in_repo` row per
    object, hot and cold, and drop the rest.

    DRY RUN IS THE DEFAULT and returns the totals, the bytes, the oldest and newest dropped
    row, and the count per object; `dry_run=False` REQUIRES a non-blank `because`. The apply
    first points the kept latest row's `supersedes` at the kept first row (so the chain stays
    whole, with nothing dangling), then deletes the copies between them in chunks of
    `chunk_rows`, one short transaction each, stopping between chunks once `max_seconds` is
    spent. A copy is deleted only if it is not current. Idempotent: a second run finds
    nothing left to drop."""
    if not dry_run and not (because or "").strip():
        return {"error": "collapsing abstention history without a because is an un-audited "
                         "repair: cite who authorized it"}
    objects, total_rows, drop_rows = await _plan(pool)
    droppers = {k: v for k, v in objects.items() if any(v["drop"][t] for t in _TABLES)}
    dated = [v for v in droppers.values() if v["oldest"] is not None]
    summary: dict[str, Any] = {
        "dry_run": dry_run, "total_rows": total_rows, "objects": len(objects),
        "objects_with_drops": len(droppers), "drop_rows": drop_rows,
        "drop_hot": sum(len(v["drop"]["assertions_hot"]) for v in droppers.values()),
        "drop_cold": sum(len(v["drop"]["assertions_cold"]) for v in droppers.values()),
        "drop_bytes": sum(v["bytes"] for v in droppers.values()),
        "oldest": min((v["oldest"] for v in dated), default=None),
        "newest": max((v["newest"] for v in dated), default=None),
        "by_object": sorted(
            ({"object_id": str(k),
              "drop_rows": sum(len(v["drop"][t]) for t in _TABLES), "bytes": v["bytes"]}
             for k, v in droppers.items()),
            key=lambda e: (-e["drop_rows"], e["object_id"]))[:_BY_OBJECT_CAP],
        "because": because if not dry_run else None}
    if dry_run:
        return summary
    started = time.monotonic()
    deleted = 0
    repointed = 0
    finished = True
    for obj, v in droppers.items():
        if v["first_id"] != v["latest_id"]:
            res = await pool.execute(
                f"UPDATE {v['latest_tier']} SET supersedes=$1 "
                "WHERE id=$2 AND supersedes IS DISTINCT FROM $1", v["first_id"], v["latest_id"])
            repointed += int(res.split()[-1])
        for table in _TABLES:
            ids = v["drop"][table]
            for i in range(0, len(ids), chunk_rows):
                if time.monotonic() - started >= max_seconds:
                    finished = False
                    break
                async with pool.acquire() as conn, conn.transaction():
                    n = await conn.fetchval(
                        f"WITH d AS (DELETE FROM {table} WHERE id = ANY($1::bigint[]) "
                        f"AND object_id=$2 AND name='{_NAME}' AND NOT is_current "
                        "RETURNING 1) SELECT count(*) FROM d",
                        ids[i:i + chunk_rows], obj)
                deleted += int(n)
                await asyncio.sleep(0.02)
            if not finished:
                break
        if not finished:
            break
    summary.update({"deleted": deleted, "repointed": repointed, "finished": finished,
                    "actor": actor})
    return summary
