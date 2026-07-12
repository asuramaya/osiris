"""Arq worker — the production process that drives the cascade.

Tests exercise the cascade coroutines directly against real Postgres + Redis;
this module is the long-running wiring: it builds a CascadeContext once at
startup and drains the outbox on a short cron. Run with:

    uv run arq src.workers.arq_worker.WorkerSettings
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import logging
import time
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
from src.ingest.sessions import sense_sessions_tick
from src.orchestrator.budgets import BudgetLedger
from src.orchestrator.cascade import CascadeContext, expand_case, run_cascade
from src.orchestrator.manifests import load_manifests
from src.orchestrator.monitor import (
    Puller,
    evaluate_watches,
    miner_tick_ended,
    miner_tick_started,
    record_job,
    tick,
    write_heartbeat,
)
from src.orchestrator.ratelimit import RateLimiter
from src.orchestrator.runner import reap_stale_runs
from src.orchestrator.trigger import trigger_mail_tick
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


_SENSE_BUDGET = 3  # LLM extract calls per tick — the tick's wall-clock is ~this many calls


async def sense_sessions(ctx: dict[str, Any]) -> int:
    """Sense the session transcripts — the last unsensed source. Distill new dialogue,
    redact, extract, land the yield DERIVED. Off unless OSIRIS_SENSE_SESSIONS names the
    projects root; a failed pass logs and waits for the next tick (cursors only advance
    past what was actually emitted). Every outcome — success, error, even the timeout
    cancel — lands in the miner:ticks telemetry: the heartbeat says the worker breathes,
    this says the tick actually finishes (the onboarding-day outage ran a full day
    behind a green heartbeat, decision 3191e0df)."""
    root = get_settings().osiris_sense_sessions
    if not root:
        return 0
    actions: Actions = ctx["cascade"].actions
    pool = actions.pool
    await miner_tick_started(pool)
    t0 = time.monotonic()
    try:
        report = await sense_sessions_tick(
            actions, Path(root), max_chunks=_SENSE_BUDGET)  # ~ expanded at listing
    except asyncio.CancelledError:
        # arq's timeout cancel: confess the death to telemetry before dying. Shielded —
        # a second cancel must not silence the confession mid-write.
        with contextlib.suppress(Exception):
            await asyncio.shield(miner_tick_ended(
                pool, secs=time.monotonic() - t0, budget=_SENSE_BUDGET, error="timeout"))
        raise
    except Exception as exc:
        # RECORD the rich tick telemetry, then RE-RAISE. It used to swallow here and return 0 —
        # which is what let it fail every tick for ten hours while looking exactly like a clean
        # pass with nothing to mine. The `watched` seam now owns "log it and wait for the next
        # tick" for every cron, and a job that hides its own failure can no longer look green.
        await miner_tick_ended(pool, secs=time.monotonic() - t0,
                               budget=_SENSE_BUDGET, error=repr(exc))
        raise
    await miner_tick_ended(pool, secs=time.monotonic() - t0,
                           budget=_SENSE_BUDGET, report=report)
    if report["chunks"] or report["planted"]:
        _log.info("session sensing: %s", report)
    return report["chunks"]


async def sweep_session(ctx: dict[str, Any], transcript: str) -> int:
    """The DEATH RITE's sweep half (task #22, ruling a882b334): a PreCompact hook posts the
    dying session's transcript here and the miner senses it NOW — anything the mind forgot to
    record deliberately is mined (DERIVED) around the seam instead of up to 10 minutes later,
    so the heir's first orient() already shows it. Same miner, same ownership boundary, just
    summoned to a deathbed instead of walking its rounds."""
    import asyncio

    root = get_settings().osiris_sense_sessions
    path = Path(transcript)
    if not root or not await asyncio.to_thread(path.is_file):
        return 0
    actions: Actions = ctx["cascade"].actions
    try:
        report = await sense_sessions_tick(actions, Path(root), only=path)
    except Exception as exc:  # a deathbed hiccup must not kill the worker
        _log.warning("precompact sweep failed for %s: %r", transcript, exc)
        return 0
    _log.info("precompact sweep %s: %s", path.name, report)
    return report["chunks"]


async def embed_pass(ctx: dict[str, Any]) -> int:
    """The semantic index's backfill walk (max-level ruling a0cfcca1): embed every
    searchable winner text whose hash moved, drop vectors of inactive objects. The hash
    watermark makes a quiet graph a free pass; a missing/unconfigured embedder makes the
    whole cron a no-op (the semantic door simply stays closed). CPU-only by design."""
    from src.orchestrator.semantics import embed_backfill, resolve_embedder

    embedder = resolve_embedder()
    if embedder is None:
        return 0
    actions: Actions = ctx["cascade"].actions
    try:
        report = await embed_backfill(actions.pool, embedder)
    except Exception as exc:  # a model hiccup must not kill the cron
        _log.warning("embed pass failed: %r", exc)
        return 0
    if report["embedded"] or report["dropped"]:
        _log.info("embed pass: %s", report)
    return report["embedded"]


async def neighborhood_pass(ctx: dict[str, Any]) -> int:
    """Rung 4's daily walk (ruling a0cfcca1): fold DERIVED echoes into their deliberate
    captures (mechanical, free), then refresh up to 3 stale neighborhood summaries
    (fingerprint-watermarked — an unchanged repo costs nothing; metered in llm_usage)."""
    from src.ingest.providers import llm_provider
    from src.orchestrator.neighborhoods import consolidate_pass, summarize_neighborhoods

    actions: Actions = ctx["cascade"].actions
    try:
        mech = await consolidate_pass(actions)
        llm = llm_provider()
        summ = await summarize_neighborhoods(actions, llm) if llm else {}
    except Exception as exc:  # a model/DB hiccup must not kill the cron
        _log.warning("neighborhood pass failed: %r", exc)
        return 0
    report = {**mech, **summ}
    if any(report.values()):
        _log.info("neighborhood pass: %s", report)
    return int(report.get("summarized", 0)) + int(report.get("threads_merged", 0))


async def trigger_mail(ctx: dict[str, Any]) -> int:
    """The mailbox alarm clock: wake an agent in a project that has unread mail (bounded by a
    per-project rate cap; OFF unless osiris_trigger_enabled — the kill switch). A spawn failure
    logs, never sinks the cron. Worker-as-tripwire (rule #2); Osiris itself still has no hands."""
    actions: Actions = ctx["cascade"].actions
    try:
        report = await trigger_mail_tick(actions)
    except Exception as exc:  # a spawn/DB hiccup must not kill the cron
        _log.warning("mail trigger failed: %r", exc)
        return 0
    if report["woke"]:
        _log.info("mail trigger: %s", report)
    return report["woke"]


def watched(fn: Any, *, every: int) -> Any:
    """THE SEAM WHERE A JOB CANNOT LIE ABOUT ITS OWN HEALTH.

    The session-miner failed every tick for ten hours and nothing knew, because it CAUGHT its own
    exception, logged a warning nobody reads, and returned 0 — indistinguishable from a clean tick
    with nothing to do. A job that swallows its error looks green.

    So error handling moves OUT of the jobs and into this seam. Every cron reports its outcome
    here — success or failure — and one added next year inherits the watch without its author
    knowing this exists. The "a failed pass logs and waits for the next tick" contract is now kept
    in ONE place, for all of them, instead of being re-implemented (or forgotten) per job.

    `every` is the job's cadence in seconds, stamped WITH the outcome, so the reader can tell
    "late" from "dead" without a table of magic numbers somewhere else.
    """
    @functools.wraps(fn)
    async def run(ctx: dict[str, Any]) -> int:
        pool = ctx["cascade"].actions.pool
        t0 = time.monotonic()
        try:
            n: int = await fn(ctx)
        except asyncio.CancelledError:  # arq's timeout: confess before dying, shielded
            with contextlib.suppress(Exception):
                await asyncio.shield(record_job(
                    pool, fn.__name__, every=every, secs=time.monotonic() - t0, error="timeout"))
            raise
        except Exception as exc:  # a hiccup must not kill the cron — but it MUST be recorded
            _log.warning("%s failed: %r", fn.__name__, exc)
            with contextlib.suppress(Exception):
                await record_job(pool, fn.__name__, every=every,
                                 secs=time.monotonic() - t0, error=repr(exc))
            return 0
        with contextlib.suppress(Exception):  # telemetry must never fail the work it watched
            await record_job(pool, fn.__name__, every=every, secs=time.monotonic() - t0)
        return n
    return run


class WorkerSettings:
    # enqueueable jobs (the API hands heavy work here instead of running it inline)
    functions: list[Any] = [expand_case_job, sweep_session]
    cron_jobs = [
        cron(watched(drain_cascade, every=5), second=set(range(0, 60, 5)), run_at_startup=True),
        # the watch: evaluate subscriptions every 5s (offset from the cascade drain),
        # pull source deltas once a minute.
        cron(watched(evaluate_watch, every=5), second=set(range(2, 60, 5)), run_at_startup=True),
        cron(watched(run_source_ticks, every=60), minute=set(range(0, 60)), second={0}),
        # self-heal orphaned claims every 5 min (the failure-drill recovery path).
        cron(watched(reap_runs, every=300), minute=set(range(0, 60, 5)), run_at_startup=True),
        # liveness heartbeat every 30s (the dead-man's-switch /health/worker reads).
        cron(watched(heartbeat, every=30), second={0, 30}, run_at_startup=True),
        # session-sensing every 10 min (a few LLM calls max per tick; no-op when unset).
        # timeout is EXPLICIT and under the cadence: a saturated tick is 3 extract calls
        # at ~50-90s each — arq's default 300s was one slow call from death, and 540 < 600
        # structurally forbids two ticks mining the same cursors concurrently.
        cron(watched(sense_sessions, every=600), minute=set(range(0, 60, 10)), second={30},
             timeout=540),
        # the semantic index walks behind the miner (offset so they never contend for CPU):
        # fresh text is embedded within ~10 minutes of landing; unchanged graphs cost nothing
        cron(watched(embed_pass, every=600), minute=set(range(5, 60, 10)), second={15},
             timeout=300),
        # rung 4 walks nightly in the quiet hour: echo-folding is free, summaries are
        # budgeted (≤3/pass, stalest-first) and skip-unchanged by fingerprint
        cron(watched(neighborhood_pass, every=86400), hour={9}, minute={10}, timeout=480),
        # the mailbox alarm clock: wake an agent for a project with unread mail — bounded by a
        # per-project rate cap, and a no-op unless osiris_trigger_enabled (the kill switch).
        cron(watched(trigger_mail, every=60), minute=set(range(0, 60)), second={45}),
    ]
    on_startup = startup
    on_shutdown = shutdown
    # arq reads this attribute AS the RedisSettings (not a callable) — a staticmethod
    # here makes arq do `.host` on the function object and die at boot. Bind the value.
    redis_settings = RedisSettings.from_dsn(get_settings().redis_url)
