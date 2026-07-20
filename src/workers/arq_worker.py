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
from src.ingest.orphans import find_orphans, mark_swept
from src.ingest.scope import scope_match, sense_scopes
from src.ingest.sessions import adversary_pass, sense_sessions_tick
from src.ingest.wake_cost import meter_bodies, meter_receipts, meter_wakes
from src.orchestrator.budgets import BudgetLedger
from src.orchestrator.cascade import CascadeContext, expand_case, run_cascade
from src.orchestrator.census import live_bodies, live_bodies_by_cwd
from src.orchestrator.liveness import observe_liveness
from src.orchestrator.manifests import load_manifests
from src.orchestrator.monitor import (
    Puller,
    evaluate_watches,
    miner_tick_ended,
    miner_tick_started,
    reap_decommissioned_jobs,
    record_job,
    tick,
    write_heartbeat,
)
from src.orchestrator.mounts import sweep_ghost_doors, sweep_stale_doors
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
    # THE SCHEDULE IS THE SOURCE OF TRUTH; a watermark is only residue. A cron that is removed
    # leaves its vitals behind, and three separate readers went on reporting "NOT SENSING" forever
    # about the miner's crawl months after we deleted it. Reconcile the DB to the schedule at boot,
    # LOUDLY — an organ that vanishes silently is the exact failure this telemetry exists to catch.
    reaped = await reap_decommissioned_jobs(pool)
    if reaped:
        _log.warning("decommissioned organs reaped from telemetry: %s", ", ".join(reaped))


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


async def sense_liveness(ctx: dict[str, Any]) -> int:
    """IS THAT MIND ALIVE, OR HAS IT MERELY STOPPED TALKING TO US? (456960e5)

    `last_seen` was refreshed only when an agent CALLED Osiris, and every liveness test reads
    `last_seen > now() - 15 minutes` — so a mind heads-down for twenty minutes reads as DEAD. WE
    WERE MEASURING CHATTINESS AND CALLING IT ALIVENESS. The wake trigger reads that same field,
    which is how it woke projects whose agent was alive and working, putting two agents on one tree.

    A `stat()` on the transcripts fixes it: a live session is WRITING, whether or not it is talking
    to us. FREE, deterministic, never wrong — an OBSERVATION, not a guess, which is why it runs on
    its own switch (OSIRIS_TRANSCRIPTS) and NOT the adversary's. Killing the expensive inferrer must
    never blind the free observer.
    """
    root = get_settings().osiris_transcripts
    if not root:
        return 0
    return await observe_liveness(ctx["pool"], Path(root))


async def backfill_transcripts(ctx: dict[str, Any]) -> int:
    """THE STORE STAYS CURRENT ON THE OBSERVER'S SWITCH (task #19, the operator's word
    2026-07-19). The transcript store is Osiris eating sessions from ANY harness; its
    backfill is a free, deterministic sweep — the spend gate (9675fe4) makes an unchanged
    transcript cost a stat and a row lookup, nothing more. So it runs on OSIRIS_TRANSCRIPTS
    (the free observer, always on) and NEVER the miner's licence — killing the expensive
    inferrer must not stale the store (the one-switch-one-cost law, 51000597). Returns
    sessions touched."""
    root = get_settings().osiris_transcripts
    if not root:
        return 0
    from src.actions.core import Actions
    from src.ingest.telemetry import TelemetryStore
    from src.ingest.transcript_store import TranscriptStore
    from src.orchestrator.neighborhoods import census_trees
    out = await TranscriptStore(ctx["pool"]).backfill()
    # the retained-telemetry sweep rides the same observer switch (task #35): the same
    # free-deterministic class, the same spend gate, the same one-switch-one-cost law
    tel = await TelemetryStore(ctx["pool"]).backfill()
    # the disk census too (thread 5e37630b): a free walk that makes 'exists on disk'
    # a graph fact — observation only, never the watch list
    roots = [r for r in get_settings().osiris_census_roots.split(":") if r.strip()]
    cs = await census_trees(Actions(ctx["pool"]), roots=roots)
    if cs["minted"]:
        _log.info("disk census minted %d unmodeled repos: %s",
                  len(cs["minted"]), ", ".join(cs["minted"]))
    return (sum(out.values()) if out else 0) + tel + len(cs["minted"])


async def sweep_doors(ctx: dict[str, Any]) -> int:
    """THE DOOR SWEEP (operator ruling, 2026-07-17: 'chrome shows fleet 5 but there are only
    3 agents up' + 'the 20+ doors on some i also consider a bug'). Two rules, one tick:

    THE GHOST RULE first — a fresh row whose cwd and project hold no claude body is a killed
    tab's leak (terminal kill skips SessionEnd), released now instead of decaying for 15
    minutes. The census runs BEFORE the row fetch (the sweep's grace floor covers the scan
    gap), and a BLIND census (None — pgrep itself failed) skips the rule entirely: 'could not
    look' must never read as 'nobody is home'. THE PILE RULE second — stale rows collapse to
    one last-known address per active agent, none for the demoted or the objectless. Pure
    reconciliation of hot state against OS truth; the graph's record of who lived is
    elsewhere and untouched."""
    pool = ctx["pool"]
    released = 0
    by_cwd = live_bodies_by_cwd()
    if by_cwd is not None:
        released += await sweep_ghost_doors(
            pool, body_cwds=set(by_cwd),
            body_projects=set(live_bodies()))
    released += await sweep_stale_doors(pool)
    return released


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
    # (the transcript-store backfill moved OFF this job to its own observer-keyed cron —
    # backfill_transcripts below — per the one-switch-one-cost law: a free deterministic
    # sweep must never wait on the miner's licence, task #19)
    if report["chunks"] or report["planted"]:
        _log.info("session sensing: %s", report)
    return report["chunks"]


async def meter_the_wakes(ctx: dict[str, Any]) -> int:
    """WHAT DID THE GHOST FARM COST? 818 wakes, ZERO in the ledger (B-final).

    Spawning an entire Claude session is the most expensive thing Osiris can do, and it was the one
    thing nobody could see. I can price the miner to the cent; I cannot tell the operator what the
    farm that minted 463 agents on his abandoned projects cost him. That is the SAME disease I spent
    the week killing in the miner — a producer whose spend nobody counted, and which therefore could
    not be falsified, and which therefore rotted. A HAND YOU CANNOT COST IS A HAND YOU CANNOT
    GOVERN.

    The truth was on the disk the whole time: a wake writes a transcript, and every assistant turn
    carries its real tokens and its model. This reads them. Free, deterministic, once per session —
    an OBSERVATION, so it rides the observer's switch, not the adversary's.
    """
    # the BODY lane first, on its OWN legs: a provider's receipts are the provider's source,
    # not the observer's — they must not ride OSIRIS_TRANSCRIPTS (a switch shared across
    # sources blinded the free observer for a day once; never again). Free, local, idempotent.
    bodies = await meter_bodies(ctx["pool"])
    if bodies["metered"]:
        _log.info("body receipts metered: %s", bodies)
    root = get_settings().osiris_transcripts
    if not root:
        return int(bodies["metered"])
    rep = await meter_wakes(ctx["pool"], Path(root))
    # the receipts pass: RESUME-mode wakes append to transcripts the once-ever watermark has
    # already walked, so their spend lives ONLY in the CLI envelope (found live: wake 819,
    # $0.2559 in a perfect receipt that three ticks walked past). Same cron, same switch.
    rep |= await meter_receipts(ctx["pool"])
    if rep["metered"] or rep.get("receipts_metered"):
        _log.info("wake spend metered: %s", rep)
    return int(rep["metered"]) + int(rep.get("receipts_metered", 0)) + int(bodies["metered"])


async def reap_orphans(ctx: dict[str, Any]) -> int:
    """THE ORPHAN LANE (B7) — a session that died with no rite left NOBODY holding the context.

    Killing the crawl made the PreCompact hook the only bell, so a session that crashes, is killed,
    or simply ENDS without reaching the context ceiling was never read at all. That was a
    regression I shipped knowingly, and this is its fix.

    IT IS NOT A CRAWL. The crawl walked EVERY transcript FOREVER on a clock. This DETECTS — for
    free, with a stat() and a watermark lookup — which sessions have ENDED and were never read, and
    hands each ONCE to the same licence-gated death rite a graceful session gets. Its cost is
    (sessions that actually died un-swept), and it converges to zero.

    And it is the case that proves why the adversary cannot be the agent: a crashed session left
    nobody to ask what it forgot. The mind is gone. Only an outside reader can recover it.
    """
    root = get_settings().osiris_sense_sessions
    if not root:
        return 0                       # the adversary is dark: detect nothing, spend nothing
    pool = ctx["pool"]
    orphans = await find_orphans(pool, Path(root))
    if not orphans:
        return 0
    for path in orphans:
        _log.info("orphan sweep: %s (died with no rite)", path.name)
        await _arq_sweep(ctx, path)
    return len(orphans)


async def _arq_sweep(ctx: dict[str, Any], path: Path) -> None:
    """Sweep one transcript and mark it read — whatever it yielded, INCLUDING NOTHING.

    An empty yield is a COMPLETE answer (most sessions abandon nothing); re-reading a session
    because it had nothing to say would be paying, forever, to be told nothing twice.
    """
    actions: Actions = ctx["cascade"].actions
    try:
        report = await adversary_pass(actions, path)
        _log.info("orphan sweep %s: %s", path.name, report)
    except Exception as exc:  # a dead session's hiccup must not stall the ones behind it
        _log.warning("orphan sweep failed for %s: %r", path.name, exc)
    finally:
        await mark_swept(ctx["pool"], path)


async def sweep_session(ctx: dict[str, Any], transcript: str) -> int:
    """THE DEATH RITE — and now the ONLY way the adversary ever runs (B6, ruling ceae1604).

    A PreCompact/Stop hook posts the dying session's transcript here and the adversary reads it
    WHOLE, once, at the seam. Not a chunk with a cursor: the WHOLE ARC. That is what makes its
    hardest rule possible — "before you return anything, search the rest of the transcript for its
    resolution" — and it is why the crawl could never have obeyed that rule at any prompt quality.
    A reader that cannot remember mints the question and never sees the answer.

    Mining is SUMMONED, never walking: the cost is one call per session that actually ENDS, and
    its output is disposed of at the seam by the mind that still holds the context, instead of
    accumulating on a wall for eight days.
    """
    import asyncio

    st = get_settings()
    root = st.osiris_sense_sessions
    path = Path(transcript)
    if not root or not await asyncio.to_thread(path.is_file):
        return 0
    # THE SCOPE (task #37): a death rite outside the armed projects is DEFERRED, not
    # buried — no spend, and deliberately NO mark_swept, so widening the scope later
    # lets the orphan reaper find this session as ended-and-unread and drain it then.
    if not scope_match(path.parent.name, sense_scopes(st.osiris_sense_projects)):
        _log.info("death-rite sweep deferred (out of scope): %s", path.name)
        return 0
    actions: Actions = ctx["cascade"].actions
    try:
        report = await adversary_pass(actions, path)
    except Exception as exc:  # a deathbed hiccup must not kill the worker
        _log.warning("death-rite sweep failed for %s: %r", transcript, exc)
        return 0
    finally:
        # MARK IT READ EVEN IF IT FAILED. The orphan reaper (B7) looks for transcripts that ended
        # and were never swept — without this mark, every session that dies GRACEFULLY would be
        # swept a second time, 45 minutes later, by the reaper. Two full extractions of the same
        # conversation is exactly the ECHO class we deleted the crawl to be rid of.
        await mark_swept(actions.pool, path)
    _log.info("death-rite sweep %s: %s", path.name, report)
    return int(report.get("proposed", 0))


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
        # THE FREE OBSERVER, every 60s: stat the transcripts so a mind heads-down for 20 minutes
        # is not read as DEAD (456960e5). No model, no money — it may run always, and it does NOT
        # ride the adversary's switch (killing the inferrer must never blind the observer).
        cron(watched(sense_liveness, every=60), minute=set(range(0, 60)), second={15},
             run_at_startup=True),
        # THE STORE'S QUIET MEALS, every 10 min (task #19, operator's word 2026-07-19):
        # eat new transcript turns from every harness into the normalized store. Rides
        # the SAME observer switch as sense_liveness — free and deterministic under the
        # spend gate (unchanged files cost a stat) — never the miner's licence.
        cron(watched(backfill_transcripts, every=600), minute=set(range(8, 60, 10)),
             second={30}, run_at_startup=True, timeout=300),
        # THE DOOR SWEEP, every 60s (operator ruling 2026-07-17): reconcile the mount
        # registry's belief against OS truth — release killed tabs' doors in ~2 minutes
        # instead of 15, and keep the door piles at one last-known address per agent.
        cron(watched(sweep_doors, every=60), minute=set(range(0, 60)), second={45},
             run_at_startup=True),
        # THE ORPHAN LANE (B7): sessions that died with NO rite — a crash, a kill -9, a closed
        # laptop — left nobody holding the context. DETECTION is free (a stat + a watermark); each
        # orphan is then swept ONCE by the same licence-gated death rite. Not a crawl: its cost is
        # (sessions that actually died un-swept) and it converges to zero.
        cron(watched(reap_orphans, every=900), minute={7, 22, 37, 52}, second={0}, timeout=600),
        # THE GHOST FARM'S BILL: 818 wakes, none of them ever in the ledger. Free (a parse of
        # transcripts we already have), deterministic, once per session — so it rides the
        # OBSERVER's switch, never the adversary's.
        cron(watched(meter_the_wakes, every=600), minute=set(range(3, 60, 10)), second={40},
             timeout=300),
        # THE CRAWL IS GONE (B6 of ruling ceae1604). There is no session-sensing cron, and its
        # absence is the design, not an omission.
        #
        # The miner used to walk EVERY transcript in the fleet every ten minutes, forever, paying
        # a `claude -p` per chunk. It read a GROWING file FORWARD, in byte-chunks, with a cursor
        # and NO MEMORY — so it minted a question from an early chunk and NEVER SAW THE ANSWER
        # that landed forty minutes later. That one property produced ECHO (no memory of what it
        # already minted: the worker-wedge was minted THREE TIMES from three chunks of the very
        # conversation diagnosing it) and STALE (never sees the resolution). 3,579 rows, 10.5%
        # ever used, $40, and a wedged worker.
        #
        # MINING IS NOW SUMMONED, NEVER WALKING: `sweep_session` fires at the DEATH RITE
        # (PreCompact / Stop / settle) against ONE transcript — the dying one — read WHOLE, so it
        # can finally see a thing raised and closed. Cost is proportional to sessions that
        # actually END, not to wall-clock times every transcript that has ever existed.
        #
        # A capability nothing schedules used to be a shelf ornament. A capability that schedules
        # ITSELF, against a world that never stops growing, is a leak.
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
