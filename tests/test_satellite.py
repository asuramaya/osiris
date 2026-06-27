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
