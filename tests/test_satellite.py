"""Phase 6 — the placeful satellite (minimal working proof).

The placeless core dispatches a vantage-bound collection job; a satellite agent at
that vantage claims it (atomically — one satellite per job), collects locally via an
injected collector, and the results land in the central graph through the Actions
waist. Coordination is only Postgres.
"""
from __future__ import annotations

import asyncio
import uuid

from src.actions.core import Actions
from src.orchestrator.monitor import WatchItem
from src.orchestrator.satellite import (
    CollectionJob,
    claim_collection_job,
    dispatch_collection,
    run_satellite_once,
)
from src.parsers.base import EvidenceClass


async def _collector(job: CollectionJob) -> list[WatchItem]:
    """A fake vantage-bound collector: 'fetched' the target, found one account."""
    return [WatchItem("Account", f"acct:{job.target}", {"source_url": job.target},
                      evidence_class=EvidenceClass.DIRECT_OBSERVATION)]


async def test_dispatch_then_satellite_collects_into_the_graph(
    actions: Actions, case_id: str
) -> None:
    cid = uuid.UUID(case_id)
    job_id = await dispatch_collection(
        actions.pool, "browser_fetch", "example.com/profile", vantage="residential",
        case_id=cid,
    )
    assert job_id is not None

    # a satellite WITHOUT that vantage can't serve it
    assert await run_satellite_once(
        actions, "sat-eu", {"browser_fetch": _collector}, vantages=["datacenter"]
    ) == "idle"

    # the satellite AT the right vantage claims, collects, emits, and marks it done
    out = await run_satellite_once(
        actions, "sat-res", {"browser_fetch": _collector}, vantages=["residential"]
    )
    assert out == "collected"

    status = await actions.pool.fetchval(
        "SELECT status FROM collection_jobs WHERE id=$1", job_id
    )
    assert status == "done"
    # the collected node is in the central graph, attributed to the satellite
    src = await actions.pool.fetchval(
        "SELECT source_id FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical='acct:example.com/profile' AND a.name='source_url'"
    )
    assert src == "satellite:sat-res"


async def test_claim_is_atomic_single_satellite_per_job(
    actions: Actions
) -> None:
    await dispatch_collection(actions.pool, "k", "t")  # no vantage -> any satellite
    a, b = await asyncio.gather(
        claim_collection_job(actions.pool, "sat-a", vantages=[]),
        claim_collection_job(actions.pool, "sat-b", vantages=[]),
    )
    assert len([x for x in (a, b) if x is not None]) == 1  # exactly one winner


async def test_vantageless_job_served_by_any_satellite(actions: Actions) -> None:
    await dispatch_collection(actions.pool, "browser_fetch", "t")  # vantage NULL
    out = await run_satellite_once(
        actions, "sat-x", {"browser_fetch": _collector}, vantages=[]
    )
    assert out == "collected"


async def test_unknown_kind_fails_the_job_not_the_satellite(actions: Actions) -> None:
    jid = await dispatch_collection(actions.pool, "mystery", "t")
    out = await run_satellite_once(actions, "sat", {}, vantages=[])
    assert out == "no_collector"
    row = await actions.pool.fetchrow("SELECT status, error FROM collection_jobs WHERE id=$1", jid)
    assert row["status"] == "failed" and "no collector" in row["error"]


async def test_collector_error_fails_just_that_job(actions: Actions) -> None:
    await dispatch_collection(actions.pool, "boom", "t")

    async def broken(job: CollectionJob) -> list[WatchItem]:
        raise RuntimeError("residential proxy down")

    out = await run_satellite_once(actions, "sat", {"boom": broken}, vantages=[])
    assert out.startswith("failed:")
    status = await actions.pool.fetchval("SELECT status FROM collection_jobs")
    assert status == "failed"


# --- D5: the Harris county collector (the real beat's vantage-bound last mile) ---

async def test_harris_collector_emits_property_watchitems() -> None:
    """The demo fetch drives the SAME collector the live scrape will — proving the seam:
    notices → graded Property WatchItems. The live fetch is the WALL (needs portal access)."""
    from src.ingest.harris_foreclosure import demo_fetch, harris_collector, make_harris_collector

    coll = make_harris_collector(fetch=demo_fetch)
    job = CollectionJob(uuid.uuid4(), "harris_foreclosure", "", None, None)
    items = await coll(job)
    assert items and all(i.type == "Property" for i in items)
    assert all(i.canonical.startswith("harris-notice:") for i in items)
    assert items[0].evidence_class is EvidenceClass.AUTHORITATIVE_API

    # the LIVE collector hits the wall until a satellite runs it at a vantage with portal access
    live_job = CollectionJob(uuid.uuid4(), "harris_foreclosure", "", None, None)
    try:
        await harris_collector(live_job)
        raise AssertionError("live_fetch should not be implemented yet")
    except NotImplementedError as exc:
        assert "integration point" in str(exc)


async def test_satellite_runs_the_harris_collector_into_the_graph(
    actions: Actions, case_id: str
) -> None:
    """Dispatch a harris job → a satellite at the county vantage collects → Property objects
    land in the CENTRAL graph (the demo collector stands in for the placeful scrape)."""
    from src.ingest.harris_foreclosure import demo_fetch, make_harris_collector

    cid = uuid.UUID(case_id)
    await dispatch_collection(actions.pool, "harris_foreclosure", "", vantage="harris-portal",
                              case_id=cid)
    out = await run_satellite_once(
        actions, "sat-harris", {"harris_foreclosure": make_harris_collector(fetch=demo_fetch)},
        vantages=["harris-portal"],
    )
    assert out == "collected"
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Property' AND canonical LIKE 'harris-notice:%'")
    assert n >= 8  # the demo notices, now in the graph via the satellite waist
