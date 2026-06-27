"""The failure drill — the deployment foundation.

Phase 2's claim is structural: the worker is fate-isolated from the API, and a
crashed worker recovers without double-emitting. We prove the mechanism, not a
process kill:

  * the API request path ENQUEUES heavy work, it does not run it inline (so a
    runaway expansion can never block or crash the console);
  * the active-claim partial unique index admits exactly one worker per
    (helper, object, case) — no double dispatch;
  * a run orphaned by a crash is recovered by the reaper, then re-claimed;
  * re-emitting the same facts is idempotent — no duplicate objects.
"""
from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import httpx
import pytest_asyncio
from src.actions.core import Actions
from src.api.app import create_app
from src.orchestrator.runner import claim_run, reap_stale_runs

NOW = datetime(2026, 6, 26, tzinfo=UTC)


class _FakeArq:
    """Records enqueues so a test can assert the API handed work to the queue."""

    def __init__(self) -> None:
        self.jobs: list[tuple[str, tuple[object, ...]]] = []

    async def enqueue_job(self, name: str, *args: object) -> object:
        self.jobs.append((name, args))
        return type("Job", (), {"job_id": "fake-job-1"})()


@pytest_asyncio.fixture
async def client_with_fake_arq(
    actions: Actions,
) -> AsyncIterator[tuple[httpx.AsyncClient, _FakeArq]]:
    app = create_app(actions.pool)
    app.state.pool = actions.pool  # lifespan is skipped under ASGITransport
    fake = _FakeArq()
    app.state.arq = fake
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c, fake


# --- the cut: the API enqueues, it never runs heavy work inline --------------

async def test_expand_enqueues_and_does_no_inline_work(
    client_with_fake_arq: tuple[httpx.AsyncClient, _FakeArq], actions: Actions
) -> None:
    client, fake = client_with_fake_arq
    cid = await actions.pool.fetchval(
        "INSERT INTO cases (name, owner) VALUES ('drill','analyst:test') RETURNING id"
    )
    r = await client.post(f"/cases/{cid}/expand")
    assert r.status_code == 200
    assert r.json()["started"] is True
    # the request handed the work to the worker queue...
    assert fake.jobs == [("expand_case_job", (str(cid),))]
    # ...and did NO heavy work itself: no helper_runs were claimed in the request path
    assert await actions.pool.fetchval("SELECT count(*) FROM helper_runs") == 0


# --- the claim: exactly one worker per (helper, object, case) -----------------

async def test_concurrent_claim_has_a_single_winner(actions: Actions, case_id: str) -> None:
    cid = uuid.UUID(case_id)
    oid = await actions.create_or_find_object("Organization", "cik:1", "edgar", cid)
    a, b = await asyncio.gather(
        claim_run(actions, "h1", oid, cid, "open"),
        claim_run(actions, "h1", oid, cid, "open"),
    )
    won = [x for x in (a, b) if x is not None]
    assert len(won) == 1  # the partial unique index admits one; no double dispatch


# --- the recovery: a crashed run is reaped, then re-claimed -------------------

async def test_orphaned_run_blocks_then_reaper_recovers_it(
    actions: Actions, case_id: str
) -> None:
    cid = uuid.UUID(case_id)
    oid = await actions.create_or_find_object("Organization", "cik:2", "edgar", cid)

    r1 = await claim_run(actions, "h1", oid, cid, "open")
    assert r1 is not None
    # while r1 is active a second claim is refused (this is the wedge a crash leaves)
    assert await claim_run(actions, "h1", oid, cid, "open") is None

    # simulate a crash: r1 is stuck 'running', now orphaned (backdate past the window)
    await actions.pool.execute(
        "UPDATE helper_runs SET created_at = now() - interval '1 hour' WHERE id=$1", r1
    )
    recovered = await reap_stale_runs(actions.pool)  # default 900s threshold
    assert recovered == 1

    # the claim is released -> the restarted worker re-claims a FRESH run
    r2 = await claim_run(actions, "h1", oid, cid, "open")
    assert r2 is not None and r2 != r1


async def test_reaper_spares_recent_and_human_wait_runs(
    actions: Actions, case_id: str
) -> None:
    cid = uuid.UUID(case_id)
    oid = await actions.create_or_find_object("Organization", "cik:3", "edgar", cid)
    await claim_run(actions, "h1", oid, cid, "open")  # fresh 'running' — too new to reap
    # a human-wait run, backdated: must NOT be reaped (24h handoffs are legitimate)
    hw = await claim_run(actions, "h2", oid, cid, "gated", status="awaiting_human")
    await actions.pool.execute(
        "UPDATE helper_runs SET created_at = now() - interval '1 hour' WHERE id=$1", hw
    )
    assert await reap_stale_runs(actions.pool) == 0
    assert await actions.pool.fetchval(
        "SELECT status FROM helper_runs WHERE id=$1", hw
    ) == "awaiting_human"


# --- no double-emit: re-running the same facts is idempotent ------------------

async def test_reemit_is_idempotent_no_duplicate_objects(
    actions: Actions, case_id: str
) -> None:
    """A reaped run re-executes; emitting the same facts twice must not duplicate.
    create_or_find_object is idempotent on canonical, and within-source supersession
    keeps one current value — so a crash-then-retry can't fork the graph."""
    cid = uuid.UUID(case_id)
    for _ in range(2):  # the original (crashed) emit + the retry's re-emit
        oid = await actions.create_or_find_object("Organization", "cik:9", "edgar", cid)
        await actions.assert_property(oid, "name", "Acme", "edgar", NOW, 0.85)

    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE canonical='cik:9'"
    ) == 1
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical='cik:9' AND a.name='name'"
    ) == 1
