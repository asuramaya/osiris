"""Placeful satellite — vantage-bound collection dispatched by the placeless core.

The hosted kernel can't reach everything from one place: some collection needs a
residential IP, a logged-in browser session, or a network only reachable from a
particular box. So the core DISPATCHES a collection job; a satellite agent running
AT that vantage claims it, runs the collector locally, and returns the results
through the same Actions narrow waist — they land in the central graph exactly like
any other emit. The only coupling is Postgres (the bus); a satellite is a thin
process that can live anywhere it can reach the DB.

This is the Phase 6 minimal proof: dispatch → atomic claim (one satellite per job) →
collect (injected, vantage-bound) → emit → mark done. The collector is the seam where
a real browser/residential fetch plugs in.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from src.actions.core import Actions
from src.orchestrator.monitor import WatchItem
from src.parsers.evidence import confidence_for

logger = logging.getLogger("osiris.satellite")


@dataclass
class CollectionJob:
    id: uuid.UUID
    kind: str
    target: str
    vantage: str | None
    case_id: uuid.UUID | None


# A collector runs AT the satellite's vantage and returns what it found as WatchItems
# (the same shape a source tick materializes). Injected — a real one drives a browser.
Collector = Callable[[CollectionJob], Awaitable[list[WatchItem]]]


async def dispatch_collection(
    pool: Any, kind: str, target: str, *, vantage: str | None = None,
    case_id: uuid.UUID | None = None,
) -> uuid.UUID:
    """The core enqueues a vantage-bound collection job for a satellite to claim."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "INSERT INTO collection_jobs (kind, target, vantage, case_id) "
        "VALUES ($1,$2,$3,$4) RETURNING id",
        kind, target, vantage, case_id,
    )


async def claim_collection_job(
    pool: Any, satellite_id: str, *, vantages: list[str]
) -> CollectionJob | None:
    """Atomically claim one queued job this satellite can serve: a job with no vantage
    requirement, or one whose vantage this satellite provides. FOR UPDATE SKIP LOCKED —
    two satellites never take the same job."""
    row = await pool.fetchrow(
        "UPDATE collection_jobs SET status='claimed', claimed_by=$1, claimed_at=now() "
        "WHERE id = ("
        "  SELECT id FROM collection_jobs WHERE status='queued' "
        "    AND (vantage IS NULL OR vantage = ANY($2::text[])) "
        "  ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED"
        ") RETURNING id, kind, target, vantage, case_id",
        satellite_id, vantages,
    )
    if row is None:
        return None
    return CollectionJob(row["id"], row["kind"], row["target"], row["vantage"], row["case_id"])


async def run_satellite_once(
    actions: Actions, satellite_id: str, collectors: dict[str, Collector],
    *, vantages: list[str],
) -> str:
    """Claim one job, run its collector at this vantage, emit the results into the
    central graph through Actions, and mark the job done/failed. Returns an outcome
    tag. A collector blowing up fails just that job — never the satellite loop."""
    job = await claim_collection_job(actions.pool, satellite_id, vantages=vantages)
    if job is None:
        return "idle"
    collector = collectors.get(job.kind)
    if collector is None:
        await actions.pool.execute(
            "UPDATE collection_jobs SET status='failed', finished_at=now(), "
            "error=$2 WHERE id=$1", job.id, f"no collector for kind {job.kind!r}",
        )
        return "no_collector"
    observed = datetime.now(UTC)
    try:
        items = await collector(job)
        for item in items:
            oid = await actions.create_or_find_object(
                item.type, item.canonical, f"satellite:{satellite_id}", job.case_id
            )
            for name, value in item.properties.items():
                if value is None:
                    continue
                await actions.assert_property(
                    oid, name, value, f"satellite:{satellite_id}", observed,
                    confidence_for(item.evidence_class), case_id=job.case_id,
                    evidence_class=item.evidence_class.value,
                )
    except Exception as exc:
        await actions.pool.execute(
            "UPDATE collection_jobs SET status='failed', finished_at=now(), "
            "error=$2 WHERE id=$1", job.id, f"{type(exc).__name__}: {exc}"[:500],
        )
        return f"failed:{type(exc).__name__}"
    await actions.pool.execute(
        "UPDATE collection_jobs SET status='done', finished_at=now(), "
        "result=$2 WHERE id=$1", job.id, {"items": len(items)},
    )
    return "collected"


# The satellite's collector registry — empty by default (the proof injects fakes; a
# real deploy registers vantage-bound collectors here, e.g. a browser/residential fetch).
COLLECTORS: dict[str, Collector] = {}


def register_default_collectors() -> None:
    """Register the vantage-bound collectors a real satellite serves. Kept OUT of the
    placeless kernel (imported lazily, only at the satellite process) so the core never
    depends on a placeful scraper. The Harris collector's LIVE fetch is the WALL — it
    runs only on a box with county-portal access."""
    from src.ingest.harris_foreclosure import harris_collector
    COLLECTORS.setdefault("harris_foreclosure", harris_collector)


async def _run_loop(poll_secs: float = 2.0) -> None:  # pragma: no cover - process entrypoint
    """The satellite agent: poll for dispatched jobs at this vantage and serve them.
    A thin process — its only dependency is Postgres (the bus)."""
    import asyncio

    from src.config.settings import get_settings
    from src.db.pool import create_pool

    settings = get_settings()
    register_default_collectors()  # wire the vantage-bound collectors at the satellite
    vantages = [v.strip() for v in settings.osiris_satellite_vantages.split(",") if v.strip()]
    pool = await create_pool(settings.database_url)
    actions = Actions(pool)
    logger.info(
        "satellite %s up; vantages=%s; collectors=%s",
        settings.osiris_satellite_id, vantages, sorted(COLLECTORS),
    )
    try:
        while True:
            out = await run_satellite_once(
                actions, settings.osiris_satellite_id, COLLECTORS, vantages=vantages
            )
            await asyncio.sleep(poll_secs if out == "idle" else 0)
    finally:
        await pool.close()


def main() -> None:  # pragma: no cover - process entrypoint
    import asyncio

    asyncio.run(_run_loop())


if __name__ == "__main__":  # pragma: no cover
    main()
