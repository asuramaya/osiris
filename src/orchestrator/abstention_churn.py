"""Collapse the churn a repeated abstention left behind.

Before the abstention writer became idempotent, two sweeps rewrote one unchanged verdict
(`derivation_abstained_in_repo`) every pass, leaving thousands of superseded copies per
object. This folds them the way `assertion_fold` does, and never loses a distinct verdict:
rows are grouped by everything that makes two rows the same fact (object, source, value,
confidence, evidence and case), and each group keeps its first and its latest row. Only the
copies between them go, hot and cold. A resolved marker row is never touched, and neither is a
row that is current.

Every row that pointed at a deleted copy is repointed to the group's first row in the same
transaction as the delete, so no `supersedes` pointer is ever left pointing at nothing, not even
for a moment. The plan runs in the database and is read one object, one chunk at a time:
nothing holds the whole table in Python, and the dry run and the apply use one query, so they
count the same rows.

This deletes history, so it is a backfill target that does nothing until it is told to: dry run
by default, an apply needs a `because`, and it records what it did as a decision. Each chunk is
one short transaction. A run that hits its time budget reports `finished: False`; running it
again continues, since the plan is recomputed from what is left."""
from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any

import asyncpg

_NAME = "derivation_abstained_in_repo"
_TABLES = ("assertions_hot", "assertions_cold")
_BY_OBJECT_CAP = 500
_GROUP_COLUMNS = ("object_id, source_id, vh, confidence, evidence_class, evidence_uri, "
                  "evidence_sha256, case_id, helper_run_id")


def _classified_sql(*, scoped: bool) -> str:
    """Every unresolved abstention row of the name ($1), both tiers, each with its verdict:
    `keep` (the first or the latest of its group), `current` (would go, but is current),
    `chain` (would go, but the group's first row points straight at it) or `drop`.
    `scoped` limits it to one object ($2)."""
    scope = "AND object_id = $2" if scoped else ""
    cols = ("id, object_id, observed_at, is_current, supersedes, source_id, "
            "md5(value::text) AS vh, confidence, evidence_class, evidence_uri, "
            "evidence_sha256, case_id, helper_run_id, pg_column_size(a.*) AS bytes")
    return f"""
WITH ab AS (
  SELECT {cols}, 'assertions_hot'::text AS tier
  FROM assertions_hot a WHERE name = $1 AND NOT (value ? 'resolved') {scope}
  UNION ALL
  SELECT {cols}, 'assertions_cold'::text AS tier
  FROM assertions_cold a WHERE name = $1 AND NOT (value ? 'resolved') {scope}),
ranked AS (
  SELECT ab.*,
    row_number() OVER w_asc AS rn_asc, row_number() OVER w_desc AS rn_desc,
    first_value(id) OVER w_asc AS first_id
  FROM ab
  WINDOW w_asc AS (PARTITION BY {_GROUP_COLUMNS} ORDER BY observed_at, id),
         w_desc AS (PARTITION BY {_GROUP_COLUMNS} ORDER BY observed_at DESC, id DESC)),
verdicts AS (
  SELECT r.id, r.object_id, r.observed_at, r.tier, r.bytes, r.first_id,
    CASE
      WHEN NOT (r.rn_asc > 1 AND r.rn_desc > 1) THEN 'keep'
      WHEN r.is_current THEN 'current'
      WHEN EXISTS (SELECT 1 FROM ranked s WHERE s.id = r.first_id AND s.supersedes = r.id)
        THEN 'chain'
      ELSE 'drop'
    END AS verdict
  FROM ranked r)
"""


# one row per object, aggregated in the database: this is all the plan Python ever holds
_PLAN_TAIL = """
SELECT object_id,
  count(*) AS total_rows,
  count(*) FILTER (WHERE verdict = 'drop') AS drop_rows,
  count(*) FILTER (WHERE verdict = 'drop' AND tier = 'assertions_hot') AS drop_hot,
  count(*) FILTER (WHERE verdict = 'drop' AND tier = 'assertions_cold') AS drop_cold,
  COALESCE(sum(bytes) FILTER (WHERE verdict = 'drop'), 0) AS drop_bytes,
  min(observed_at) FILTER (WHERE verdict = 'drop') AS oldest,
  max(observed_at) FILTER (WHERE verdict = 'drop') AS newest,
  count(*) FILTER (WHERE verdict = 'current') AS skipped_current,
  count(*) FILTER (WHERE verdict = 'chain') AS skipped_chain,
  count(DISTINCT first_id) AS distinct_values,
  count(DISTINCT first_id) - count(DISTINCT first_id) FILTER (WHERE verdict <> 'drop')
    AS distinct_values_lost
FROM verdicts GROUP BY object_id
"""
_PLAN_SQL = _classified_sql(scoped=False) + _PLAN_TAIL

# one object's drops: oldest first, with the row each one folds into
_CANDIDATES_SQL = _classified_sql(scoped=True) + """
SELECT id, tier, first_id FROM verdicts WHERE verdict = 'drop'
ORDER BY observed_at, id LIMIT $3
"""
_PLAN_BATCH = 50_000  # drop ids read per object at a time (a few hundred kilobytes)

_DANGLING_SQL = """
SELECT count(*) FROM (
  SELECT supersedes FROM assertions_hot WHERE object_id = $1 AND name = $2
    AND supersedes IS NOT NULL
  UNION ALL
  SELECT supersedes FROM assertions_cold WHERE object_id = $1 AND name = $2
    AND supersedes IS NOT NULL) a
WHERE NOT EXISTS (SELECT 1 FROM assertions_hot h WHERE h.id = a.supersedes)
  AND NOT EXISTS (SELECT 1 FROM assertions_cold c WHERE c.id = a.supersedes)
"""


# the one-object form of the plan: the same classification, scoped
_PLAN_ONE_SQL = _classified_sql(scoped=True) + _PLAN_TAIL

# objects that hold more than two unresolved rows: the only ones a fold can change
_OBJECTS_SQL = """
SELECT object_id, count(*) AS n FROM (
  SELECT object_id FROM assertions_hot WHERE name = $1 AND NOT (value ? 'resolved')
  UNION ALL
  SELECT object_id FROM assertions_cold WHERE name = $1 AND NOT (value ? 'resolved')) t
GROUP BY object_id HAVING count(*) > 2 ORDER BY count(*), object_id
"""


async def _plan(pool: asyncpg.Pool, obj: uuid.UUID | None = None) -> list[asyncpg.Record]:
    """The plan, aggregated per object inside the database (one row per object)."""
    if obj is None:
        return list(await pool.fetch(_PLAN_SQL, _NAME))
    return list(await pool.fetch(_PLAN_ONE_SQL, _NAME, obj))


def _total(rows: list[asyncpg.Record], key: str) -> int:
    return int(sum(r[key] for r in rows))


async def _fold_chunk(
    pool: asyncpg.Pool, obj: uuid.UUID, cand: list[asyncpg.Record],
) -> dict[str, Any]:
    """One transaction: repoint every row that points at a copy to the group's first row, then
    delete the copies. Reports what it touched against what was planned."""
    ids = [r["id"] for r in cand]
    targets = [r["first_id"] for r in cand]
    async with pool.acquire() as conn, conn.transaction():
        found = repointed = 0
        for table in _TABLES:
            found += int(await conn.fetchval(
                f"SELECT count(*) FROM {table} WHERE supersedes = ANY($1::bigint[]) "
                "AND id <> ALL($1::bigint[])", ids))
            res = await conn.execute(
                f"UPDATE {table} t SET supersedes = m.new_id "
                "FROM unnest($1::bigint[], $2::bigint[]) AS m(old_id, new_id) "
                "WHERE t.supersedes = m.old_id AND t.id <> ALL($1::bigint[])", ids, targets)
            repointed += int(res.split()[-1])
        deleted: list[int] = []
        for table in _TABLES:
            tier_ids = [r["id"] for r in cand if r["tier"] == table]
            if not tier_ids:
                continue
            deleted += [r["id"] for r in await conn.fetch(
                f"DELETE FROM {table} WHERE id = ANY($1::bigint[]) AND object_id = $2 "
                f"AND name = '{_NAME}' AND NOT is_current RETURNING id", tier_ids, obj)]
    return {"planned": len(ids), "deleted": deleted, "repointed": repointed,
            "repoint_misses": max(found - repointed, 0)}


def _summarize(plan: list[asyncpg.Record], *, dry_run: bool, because: str | None,
               ) -> dict[str, Any]:
    droppers = [r for r in plan if r["drop_rows"]]
    oldest = [r["oldest"] for r in droppers if r["oldest"] is not None]
    newest = [r["newest"] for r in droppers if r["newest"] is not None]
    return {
        "dry_run": dry_run, "total_rows": _total(plan, "total_rows"), "objects": len(plan),
        "objects_with_drops": len(droppers), "drop_rows": _total(plan, "drop_rows"),
        "drop_hot": _total(plan, "drop_hot"), "drop_cold": _total(plan, "drop_cold"),
        "drop_bytes": _total(plan, "drop_bytes"),
        "skipped_current": _total(plan, "skipped_current"),
        "skipped_chain": _total(plan, "skipped_chain"),
        "distinct_values": _total(plan, "distinct_values"),
        "distinct_values_lost": _total(plan, "distinct_values_lost"),
        "oldest": min(oldest, default=None), "newest": max(newest, default=None),
        "by_object": [
            {"object_id": str(r["object_id"]), "drop_rows": r["drop_rows"],
             "bytes": r["drop_bytes"]}
            for r in sorted(droppers, key=lambda r: (-r["drop_rows"], str(r["object_id"])))
            [:_BY_OBJECT_CAP]],
        "because": because if not dry_run else None}


async def collapse_abstention_churn(
    pool: asyncpg.Pool, *, actor: str, dry_run: bool = True, because: str | None = None,
    chunk_rows: int = 2000, max_seconds: float = 25.0,
) -> dict[str, Any]:
    """Fold the repeated `derivation_abstained_in_repo` rows: per group of identical rows the
    first and the latest stay, the copies between go, hot and cold.

    DRY RUN IS THE DEFAULT and returns the totals, the bytes, the oldest and newest dropped row,
    the count per object, `skipped_current` (rows that are current and so stay) and
    `distinct_values_lost` (always 0: every distinct value keeps its first and latest row).
    `dry_run=False` REQUIRES a non-blank `because`. The apply works one object at a time, in
    chunks of `chunk_rows`, one short transaction each: it repoints everything that pointed at a
    copy to the group's first row, then deletes the copies, so nothing dangles. It reads each
    object's plan with the same query the dry run uses, so the two count the same rows. It
    stops between chunks once `max_seconds` is spent. `finished` is true only when every object
    was checked again and nothing remains to fold. A delete or repoint that touched fewer rows
    than planned (a row moved between the hot and cold tier, or became current) is counted in
    `delete_misses` and `repoint_misses`, never passed silently. A decision records what was
    done. Idempotent: a second run finds nothing left to fold."""
    if not dry_run and not (because or "").strip():
        return {"error": "collapsing abstention history without a because is an un-audited "
                         "repair: cite who authorized it"}
    started = time.monotonic()
    if dry_run:
        return _summarize(await _plan(pool), dry_run=True, because=because)
    plan: list[asyncpg.Record] = []
    deleted_ids: list[int] = []
    repointed = delete_misses = repoint_misses = stuck = 0
    dangling_before = dangling_after = 0
    out_of_time = False
    objects = await pool.fetch(_OBJECTS_SQL, _NAME)
    for entry in objects:
        obj = entry["object_id"]
        if time.monotonic() - started >= max_seconds:
            out_of_time = True
            break
        plan += await _plan(pool, obj)
        dangling_before += int(await pool.fetchval(_DANGLING_SQL, obj, _NAME))
        object_stuck = False
        while not out_of_time and not object_stuck:
            batch = list(await pool.fetch(_CANDIDATES_SQL, _NAME, obj, _PLAN_BATCH))
            if not batch:
                break
            for i in range(0, len(batch), chunk_rows):
                if time.monotonic() - started >= max_seconds:
                    out_of_time = True
                    break
                chunk = await _fold_chunk(pool, obj, batch[i:i + chunk_rows])
                deleted_ids += chunk["deleted"]
                repointed += chunk["repointed"]
                repoint_misses += chunk["repoint_misses"]
                delete_misses += chunk["planned"] - len(chunk["deleted"])
                if not chunk["deleted"]:
                    object_stuck = True  # nothing could be deleted: never plan it again
                    break
                await asyncio.sleep(0.02)
        dangling_after += int(await pool.fetchval(_DANGLING_SQL, obj, _NAME))
        if object_stuck:
            stuck += 1
    summary = _summarize(plan, dry_run=False, because=because)
    summary.update({
        "deleted": len(deleted_ids), "repointed": repointed, "delete_misses": delete_misses,
        "repoint_misses": repoint_misses, "objects_stuck": stuck,
        "objects_checked": len(plan), "objects_left": len(objects) - len(plan),
        "dangling_before": dangling_before, "dangling_after": dangling_after,
        "finished": not out_of_time and stuck == 0, "actor": actor})
    if deleted_ids:
        summary["audit"] = await _record_audit(pool, summary, deleted_ids, actor, because or "")
    return summary


async def _record_audit(
    pool: asyncpg.Pool, summary: dict[str, Any], deleted_ids: list[int], actor: str,
    because: str,
) -> dict[str, Any]:
    """Records what a collapse run deleted as a decision, so the record outlives the tool
    result: how many rows, the id range, the actor and the reason."""
    from src.actions.core import Actions
    from src.orchestrator.capture import record_decision

    lo, hi = min(deleted_ids), max(deleted_ids)
    text = (f"Abstention churn collapse run by {actor}: deleted {len(deleted_ids)} repeated "
            f"abstention rows (ids {lo} to {hi}) across {summary['objects_checked']} objects, "
            f"repointed {summary['repointed']} rows, dangling pointers "
            f"{summary['dangling_before']} before and {summary['dangling_after']} after. "
            f"Reason given: {because}")
    rationale = (f"finished {summary['finished']}, delete misses {summary['delete_misses']}, "
                 f"repoint misses {summary['repoint_misses']}, objects stuck "
                 f"{summary['objects_stuck']}, objects left {summary['objects_left']}, current "
                 f"rows kept {summary['skipped_current']}. Every distinct value keeps its "
                 "first and latest row.")
    try:
        decision = await record_decision(
            Actions(pool), text, kind="decision", rationale=rationale, repo="osiris",
            source=actor)
    except Exception as exc:  # noqa: BLE001 - the rows are gone: report, never hide it
        return {"recorded": False, "error": f"{type(exc).__name__}: {exc}"}
    return {"recorded": True, "decision": str(decision)}
