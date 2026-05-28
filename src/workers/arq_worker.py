"""Arq worker — the production process that drives the cascade.

Tests exercise the cascade coroutines directly against real Postgres + Redis;
this module is the long-running wiring: it builds a CascadeContext once at
startup and drains the outbox on a short cron. Run with:

    uv run arq src.workers.arq_worker.WorkerSettings
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from arq import cron
from arq.connections import RedisSettings

from src.actions.core import Actions
from src.config.settings import get_settings
from src.connectors.registry import CONNECTORS
from src.db.pool import create_pool
from src.db.redis import create_redis
from src.orchestrator.budgets import BudgetLedger
from src.orchestrator.cascade import CascadeContext, run_cascade
from src.orchestrator.manifests import load_manifests
from src.orchestrator.ratelimit import RateLimiter

_HELPERS_DIR = Path(__file__).resolve().parent.parent.parent / "helpers"


async def startup(ctx: dict[str, Any]) -> None:
    settings = get_settings()
    pool = await create_pool(settings.database_url)
    redis = create_redis(settings.redis_url)
    actions = Actions(pool)
    ctx["cascade"] = CascadeContext(
        actions=actions,
        limiter=RateLimiter(redis),
        ledger=BudgetLedger(pool, redis),
        manifests=load_manifests(_HELPERS_DIR),
        connectors=dict(CONNECTORS),
    )
    ctx["pool"] = pool
    ctx["redis"] = redis


async def shutdown(ctx: dict[str, Any]) -> None:
    await ctx["pool"].close()
    await ctx["redis"].aclose()


async def drain_cascade(ctx: dict[str, Any]) -> int:
    return await run_cascade(ctx["cascade"])


class WorkerSettings:
    functions: list[Any] = []
    cron_jobs = [cron(drain_cascade, second=set(range(0, 60, 5)), run_at_startup=True)]
    on_startup = startup
    on_shutdown = shutdown

    @staticmethod
    def redis_settings() -> RedisSettings:
        return RedisSettings.from_dsn(get_settings().redis_url)
