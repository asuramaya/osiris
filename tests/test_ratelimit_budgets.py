from __future__ import annotations

import uuid

import redis.asyncio as aioredis
from src.actions.core import Actions
from src.orchestrator.budgets import BudgetLedger
from src.orchestrator.ratelimit import RateLimiter


async def test_token_bucket_grants_then_denies(redis_client: aioredis.Redis) -> None:
    limiter = RateLimiter(redis_client)
    # capacity 2, slow refill -> first two grants, third denied within the same instant
    assert await limiter.acquire("origin.test", rps=0.001, capacity=2) is True
    assert await limiter.acquire("origin.test", rps=0.001, capacity=2) is True
    assert await limiter.acquire("origin.test", rps=0.001, capacity=2) is False
    # a different origin has its own bucket
    assert await limiter.acquire("other.test", rps=0.001, capacity=2) is True


async def test_rate_credit_reserve_and_exhaust(
    actions: Actions, redis_client: aioredis.Redis
) -> None:
    case_id = await actions.pool.fetchval(
        "INSERT INTO cases (name, owner, budgets) VALUES ('b','analyst:test',$1) RETURNING id",
        {"rate_credits": 2},
    )
    ledger = BudgetLedger(actions.pool, redis_client)
    cid = uuid.UUID(str(case_id))
    assert await ledger.reserve_rate_credit(cid) is True
    assert await ledger.reserve_rate_credit(cid) is True
    assert await ledger.reserve_rate_credit(cid) is False  # exhausted
    await ledger.refund_rate_credit(cid)
    assert await ledger.reserve_rate_credit(cid) is True   # refund restored one


async def test_budget_check_hop_and_per_object(
    actions: Actions, redis_client: aioredis.Redis
) -> None:
    case_id = await actions.pool.fetchval(
        "INSERT INTO cases (name, owner, budgets) VALUES ('b','analyst:test',$1) RETURNING id",
        {"max_hop_distance": 1, "max_helpers_per_object": 2},
    )
    cid = uuid.UUID(str(case_id))
    obj = await actions.create_or_find_object("Domain", "h.test", "analyst:test", cid)
    ledger = BudgetLedger(actions.pool, redis_client)

    assert (await ledger.check(cid, obj, hop_distance=1)).allowed is True
    assert (await ledger.check(cid, obj, hop_distance=2)).reason == "max_hop_distance"

    # two DISTINCT helpers on one object hit the per-object cap (breadth, not
    # depth — re-runs of the same helper don't count, so windowed helpers are ok)
    for helper in ("hx", "hy"):
        await actions.pool.execute(
            "INSERT INTO helper_runs (helper_id, object_id, case_id, status, tier) "
            "VALUES ($3,$1,$2,'done','open')",
            obj,
            cid,
            helper,
        )
    assert (await ledger.check(cid, obj, hop_distance=0)).reason == "max_helpers_per_object"
