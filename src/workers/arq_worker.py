"""Arq worker — the production process that drives the cascade.

Tests exercise the cascade coroutines directly against real Postgres + Redis;
this module is the long-running wiring: it builds a CascadeContext once at
startup and drains the outbox on a short cron. Run with:

    uv run arq src.workers.arq_worker.WorkerSettings
"""

from __future__ import annotations

import logging
import uuid
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
from src.orchestrator.cascade import CascadeContext, expand_case, run_cascade
from src.orchestrator.manifests import load_manifests
from src.orchestrator.monitor import Puller, evaluate_watches, tick, write_heartbeat
from src.orchestrator.ratelimit import RateLimiter
from src.orchestrator.runner import reap_stale_runs
from src.orchestrator.watchers import make_form_d_watcher

_HELPERS_DIR = Path(__file__).resolve().parent.parent.parent / "helpers"
_log = logging.getLogger("osiris.worker")

# The watch's source ticks, keyed by source_id. Populated at startup from config
# (register_default_watchers); a real connector registers its puller here. Empty =>
# the run_source_ticks cron is a no-op (the watch stays source-agnostic).
SOURCE_TICKS: dict[str, Puller] = {}


def register_default_watchers() -> None:
    """Wire the source watchers named in config into SOURCE_TICKS. Idempotent —
    safe to call on every startup. A Form D watch term 'Neuralink' registers a tick
    keyed 'form_d:Neuralink' that polls SEC for new Form D filings mentioning it."""
    terms = [t.strip() for t in get_settings().osiris_watch_form_d.split(",") if t.strip()]
    for term in terms:
        SOURCE_TICKS[f"form_d:{term}"] = make_form_d_watcher(term)


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
    register_default_watchers()


async def shutdown(ctx: dict[str, Any]) -> None:
    await ctx["pool"].close()
    await ctx["redis"].aclose()


async def drain_cascade(ctx: dict[str, Any]) -> int:
    return await run_cascade(ctx["cascade"])


async def expand_case_job(ctx: dict[str, Any], case_id: str) -> int:
    """The heavy case-expansion the API used to run inline in its own event loop.
    It is ENQUEUED here so a long crawl can never block or crash the console — the
    worker⊥surface cut. The API's SSE stream still surfaces progress, reading the
    same Postgres this writes to."""
    return await expand_case(ctx["cascade"], uuid.UUID(case_id))


async def evaluate_watch(ctx: dict[str, Any]) -> int:
    """The tripwire: match new outbox mutations against active watches."""
    return await evaluate_watches(ctx["pool"])


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


async def reap_runs(ctx: dict[str, Any]) -> int:
    """Self-heal: recover runs orphaned by a crashed worker so a restart re-claims."""
    return await reap_stale_runs(ctx["pool"])


async def heartbeat(ctx: dict[str, Any]) -> int:
    """The dead-man's-switch: touch the heartbeat watermark so a silently-dead worker is
    visible at GET /health/worker (a stale beat) instead of an invisible tripwire gap."""
    await write_heartbeat(ctx["pool"])
    return 1


class WorkerSettings:
    # enqueueable jobs (the API hands heavy work here instead of running it inline)
    functions: list[Any] = [expand_case_job]
    cron_jobs = [
        cron(drain_cascade, second=set(range(0, 60, 5)), run_at_startup=True),
        # the watch: evaluate subscriptions every 5s (offset from the cascade drain),
        # pull source deltas once a minute.
        cron(evaluate_watch, second=set(range(2, 60, 5)), run_at_startup=True),
        cron(run_source_ticks, minute=set(range(0, 60)), second={0}),
        # self-heal orphaned claims every 5 min (the failure-drill recovery path).
        cron(reap_runs, minute=set(range(0, 60, 5)), run_at_startup=True),
        # liveness heartbeat every 30s (the dead-man's-switch /health/worker reads).
        cron(heartbeat, second={0, 30}, run_at_startup=True),
    ]
    on_startup = startup
    on_shutdown = shutdown
    # arq reads this attribute AS the RedisSettings (not a callable) — a staticmethod
    # here makes arq do `.host` on the function object and die at boot. Bind the value.
    redis_settings = RedisSettings.from_dsn(get_settings().redis_url)
