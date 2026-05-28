"""Per-case budgets — the cascade terminator (DESIGN §6).

Cascades stop because budgets hit zero, not because the queue drained. Three
gates, all checked at *dispatch*:
  * rate credits     — atomic Redis decrement (reserve, refund on no-op routes)
  * hop distance      — graph distance from the seed (case_objects.hop_distance)
  * helpers/object    — cap re-runs against one hot entity

Budgets live in cases.budgets (jsonb); the Redis credit counter is seeded from
it once via SET NX so concurrent workers share one authoritative balance.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

import asyncpg
import redis.asyncio as aioredis

DEFAULT_BUDGETS: dict[str, Any] = {
    "rate_credits": 100,
    "max_hop_distance": 2,
    "max_helpers_per_object": 5,
    "max_human_handoffs": 25,
}


@dataclass
class BudgetDecision:
    allowed: bool
    reason: str = ""


async def _load_budgets(pool: asyncpg.Pool, case_id: uuid.UUID) -> dict[str, Any]:
    raw = await pool.fetchval("SELECT budgets FROM cases WHERE id=$1", case_id)
    budgets = dict(DEFAULT_BUDGETS)
    if raw:
        budgets.update(raw)
    return budgets


class BudgetLedger:
    def __init__(self, pool: asyncpg.Pool, redis: aioredis.Redis) -> None:
        self.pool = pool
        self.redis = redis

    async def _ensure_credit(self, case_id: uuid.UUID, total: int) -> None:
        # seed the shared counter exactly once
        await self.redis.set(f"budget:{case_id}:rate", total, nx=True)

    async def reserve_rate_credit(self, case_id: uuid.UUID) -> bool:
        budgets = await _load_budgets(self.pool, case_id)
        await self._ensure_credit(case_id, int(budgets["rate_credits"]))
        remaining = await self.redis.decr(f"budget:{case_id}:rate")
        if remaining < 0:
            await self.redis.incr(f"budget:{case_id}:rate")  # refund overshoot
            return False
        return True

    async def refund_rate_credit(self, case_id: uuid.UUID) -> None:
        await self.redis.incr(f"budget:{case_id}:rate")

    async def reserve_handoff_credit(self, case_id: uuid.UUID) -> bool:
        """Human attention is the scarcest budget — gate handoffs separately."""
        budgets = await _load_budgets(self.pool, case_id)
        await self.redis.set(
            f"budget:{case_id}:handoffs", int(budgets["max_human_handoffs"]), nx=True
        )
        remaining = await self.redis.decr(f"budget:{case_id}:handoffs")
        if remaining < 0:
            await self.redis.incr(f"budget:{case_id}:handoffs")
            return False
        return True

    async def refund_handoff_credit(self, case_id: uuid.UUID) -> None:
        await self.redis.incr(f"budget:{case_id}:handoffs")

    async def check(
        self,
        case_id: uuid.UUID,
        object_id: uuid.UUID,
        *,
        hop_distance: int,
    ) -> BudgetDecision:
        """Non-consuming gates (hop, helpers-per-object). Rate credit is reserved
        separately so it can be refunded on cache hits / no-op routes."""
        budgets = await _load_budgets(self.pool, case_id)
        if hop_distance > int(budgets["max_hop_distance"]):
            return BudgetDecision(False, "max_hop_distance")
        n_runs = await self.pool.fetchval(
            "SELECT count(*) FROM helper_runs WHERE object_id=$1 AND case_id=$2",
            object_id,
            case_id,
        )
        if n_runs >= int(budgets["max_helpers_per_object"]):
            return BudgetDecision(False, "max_helpers_per_object")
        return BudgetDecision(True)
