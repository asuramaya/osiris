"""Arq worker — the production process that drives the cascade.

Tests exercise the cascade coroutines directly against real Postgres + Redis;
this module is the long-running wiring: it builds a CascadeContext once at
startup and drains the outbox on a short cron. Run with:

    uv run arq src.workers.arq_worker.WorkerSettings
"""

from __future__ import annotations

import logging
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
from src.orchestrator.monitor import Puller, evaluate_subscriptions, tick
from src.orchestrator.ratelimit import RateLimiter

_HELPERS_DIR = Path(__file__).resolve().parent.parent.parent / "helpers"
_log = logging.getLogger("osiris.worker")

# The watch's source ticks. Source-agnostic by design: a real connector (new filings,
# sanctions deltas, dockets) registers its puller here in a later phase; until then the
# cron iterates an empty set and is a no-op. Keyed by source_id.
SOURCE_TICKS: dict[str, Puller] = {}


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


async def evaluate_watch(ctx: dict[str, Any]) -> int:
    """The tripwire: match new outbox mutations against saved subscriptions."""
    return await evaluate_subscriptions(ctx["pool"])


async def run_source_ticks(ctx: dict[str, Any]) -> int:
    """Pull each registered source's delta. One bad source can't sink the rest."""
    actions: Actions = ctx["cascade"].actions
    total = 0
    for source_id, puller in SOURCE_TICKS.items():
        try:
            total += await tick(actions, source_id, puller)
        except Exception as exc:  # a flaky source must not abort the scheduled run
            _log.warning("source tick %s failed: %r", source_id, exc)
    return total


class WorkerSettings:
    functions: list[Any] = []
    cron_jobs = [
        cron(drain_cascade, second=set(range(0, 60, 5)), run_at_startup=True),
        # the watch: evaluate subscriptions every 5s (offset from the cascade drain),
        # pull source deltas once a minute.
        cron(evaluate_watch, second=set(range(2, 60, 5)), run_at_startup=True),
        cron(run_source_ticks, minute=set(range(0, 60)), second={0}),
    ]
    on_startup = startup
    on_shutdown = shutdown

    @staticmethod
    def redis_settings() -> RedisSettings:
        return RedisSettings.from_dsn(get_settings().redis_url)
