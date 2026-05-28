"""Windowed pattern helpers — the rolling-window scheduler (DESIGN §11).

A windowed helper (e.g. tgstat_channel_behavior) runs once per slide-aligned
window over every matching object in every active case, re-emitting as windows
advance. Resolved lifecycle (#4):
  * dedupe per (helper, object, case, window_bucket) via the active-claim index;
  * APPEND evidence (ObservedData with a per-window canonical = a new object) and
    SUPERSEDE judgment (the rolling assessment is a property on a stable object,
    so the existing within-source supersession applies);
  * BUDGET-ONLY termination — runs until the case is archived or rate credits run
    out (no idle dormancy);
  * BACKFILL bounded — on first attach, backfill from now-lookback, capped by
    max_buckets and the rate budget, then roll forward.

A single tick() call (driven by an Arq cron in production) advances all windows.
"""

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from src.orchestrator.cascade import CascadeContext
from src.orchestrator.runner import claim_run, execute_claimed, load_input_object
from src.parsers.base import InputObject

# Windowed connectors get the window bounds, unlike point-in-time connectors.
WindowedConnector = Callable[[InputObject, datetime, datetime], Awaitable[dict[str, Any]]]

_EPOCH = datetime(2000, 1, 1, tzinfo=UTC)
_DURATION = re.compile(r"^(\d+)([dhm])$")
_UNIT = {"d": "days", "h": "hours", "m": "minutes"}


def parse_duration(text: str) -> timedelta:
    m = _DURATION.match(text.strip())
    if not m:
        raise ValueError(f"bad duration {text!r} (expected like '7d', '12h', '30m')")
    return timedelta(**{_UNIT[m.group(2)]: int(m.group(1))})


def _align(t: datetime, slide: timedelta) -> datetime:
    n = int((t - _EPOCH) / slide)
    return _EPOCH + n * slide


def due_buckets(
    windowing: dict[str, Any],
    *,
    now: datetime,
    last_bucket: datetime | None,
    max_buckets: int = 64,
) -> list[datetime]:
    """Slide-aligned window starts that are due. First run backfills from
    now-lookback; subsequent runs continue after the last completed bucket.
    Capped at max_buckets (the rate budget bounds it further at run time)."""
    slide = parse_duration(windowing["slide"])
    lookback = parse_duration(windowing.get("lookback", "30d"))
    aligned_now = _align(now, slide)
    begin = (last_bucket + slide) if last_bucket is not None else _align(now - lookback, slide)
    buckets: list[datetime] = []
    b = begin
    while b <= aligned_now and len(buckets) < max_buckets:
        buckets.append(b)
        b += slide
    return buckets


async def tick(
    ctx: CascadeContext,
    windowed_connectors: dict[str, WindowedConnector],
    *,
    now: datetime,
    max_buckets_per_object: int = 64,
) -> dict[str, int]:
    """Advance every windowed helper over every matching object in active cases.
    Returns counts of runs / budget-blocked / claim-skipped."""
    out = {"runs": 0, "budget_blocked": 0, "skipped": 0}

    for manifest in ctx.manifests.values():
        if manifest.windowing is None:
            continue
        connector = windowed_connectors.get(manifest.id)
        if connector is None:
            continue
        bucket_len = parse_duration(manifest.windowing["bucket"])

        rows = await ctx.pool.fetch(
            "SELECT o.id AS oid, co.case_id AS case_id, co.hop_distance AS hop "
            "FROM objects o "
            "JOIN case_objects co ON co.object_id = o.id "
            "JOIN cases c ON c.id = co.case_id "
            "WHERE o.type = $1 AND c.archived_at IS NULL AND o.status = 'active'",
            manifest.consumes.type,
        )
        for row in rows:
            oid, case_id, hop = row["oid"], row["case_id"], row["hop"]
            last = await ctx.pool.fetchval(
                "SELECT max(window_bucket) FROM helper_runs "
                "WHERE helper_id=$1 AND object_id=$2 AND case_id=$3 AND status='done'",
                manifest.id,
                oid,
                case_id,
            )
            buckets = due_buckets(
                manifest.windowing, now=now, last_bucket=last, max_buckets=max_buckets_per_object
            )
            if not buckets:
                continue
            input_object = await load_input_object(ctx.pool, oid)
            for bucket in buckets:
                if not await ctx.ledger.reserve_rate_credit(case_id):
                    out["budget_blocked"] += 1
                    break  # budget-only termination — stop advancing this object
                run_id = await claim_run(
                    ctx.actions, manifest.id, oid, case_id, manifest.tier, window_bucket=bucket
                )
                if run_id is None:
                    await ctx.ledger.refund_rate_credit(case_id)
                    out["skipped"] += 1
                    continue
                response = await connector(input_object, bucket, bucket + bucket_len)
                await execute_claimed(
                    ctx.actions, manifest, response, input_object, case_id, run_id,
                    input_hop=int(hop or 0),
                )
                out["runs"] += 1
    return out
