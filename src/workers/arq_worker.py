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

from src import memprofile
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
from src.orchestrator.deploy_guard import (
    alarm_schema_drift,
    alarm_unreviewed_boot,
    check_and_alarm_unreviewed_boot,
    check_and_resolve_clean_boot,
    check_schema_drift,
    check_unreviewed_boot,
    resolve_schema_drift_alarms_on_clean_check,
)
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
from src.orchestrator.resource_lease import reap_stale as reap_stale_leases
from src.orchestrator.runner import reap_stale_runs
from src.orchestrator.trigger import trigger_mail_tick
from src.orchestrator.watchers import make_form_d_watcher

memprofile.maybe_start()  # inert unless OSIRIS_PROFILE_MEMORY is set — thread e6fd3772

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


async def _proc_mem_kb() -> dict[str, int | None]:
    """This process's own RSS/swap off /proc/self/status — mirrors mcp_server.py's own
    helper (thread 4746e7f4/0c03a685); small and process-local enough (stdlib-only) that
    a shared import wasn't worth coupling this daemon's boot path to the other daemon's
    HTTP route module. Fails open to None per field rather than raising."""
    out: dict[str, int | None] = {"rss_kb": None, "swap_kb": None}
    try:
        for line in await asyncio.to_thread(
                lambda: Path("/proc/self/status").read_text().splitlines()):
            if line.startswith("VmRSS:"):
                out["rss_kb"] = int(line.split()[1])
            elif line.startswith("VmSwap:"):
                out["swap_kb"] = int(line.split()[1])
    except (OSError, ValueError, IndexError):
        pass
    return out


_WORKER_MEMTRACE_MAX_FRAMES = 5
_WORKER_MEMTRACE_RSS_REFUSE_KB = 1_500_000
_WORKER_MEMTRACE_CHECK_INTERVAL_S = 5.0


async def _boot_memtrace(window_s: float) -> None:
    """Ports 8c7100c's bounded-window safety rails to the worker's own boot burst (thread
    0c03a685) — the exact incident that redesign exists to prevent (an unbounded
    tracemalloc pinning a live event loop) must never repeat here either. 5 frames, an
    RSS tripwire, a hard duration cap, self-terminating with nobody polling — but unlike
    the mcp route, this daemon has no HTTP surface: it logs the top allocation sites ONCE,
    at the end of the window, since that is the only channel this process has."""
    import tracemalloc

    baseline = await _proc_mem_kb()
    tracemalloc.start(_WORKER_MEMTRACE_MAX_FRAMES)
    deadline = time.monotonic() + window_s
    try:
        while time.monotonic() < deadline:
            await asyncio.sleep(_WORKER_MEMTRACE_CHECK_INTERVAL_S)
            mem = await _proc_mem_kb()
            if mem["rss_kb"] is not None and mem["rss_kb"] > _WORKER_MEMTRACE_RSS_REFUSE_KB:
                break
        snapshot = tracemalloc.take_snapshot()
        top = snapshot.statistics("lineno")[:_WORKER_MEMTRACE_MAX_FRAMES]
        final = await _proc_mem_kb()
        _log.warning(
            "boot memtrace: baseline rss_kb=%s swap_kb=%s -> final rss_kb=%s swap_kb=%s; "
            "top allocation sites: %s", baseline["rss_kb"], baseline["swap_kb"],
            final["rss_kb"], final["swap_kb"],
            [f"{stat.traceback[0]} {round(stat.size / 1024, 1)}kB x{stat.count}"
             for stat in top])
    finally:
        if tracemalloc.is_tracing():
            tracemalloc.stop()


# THE BOOT SERIALIZATION GATE (thread 0c03a685): held only within osiris_worker_boot_
# serialize_s of process start (checked against ctx["boot_serialize_until"], stamped once
# in startup()) — a scheduled tick past that deadline never touches this lock at all.
_boot_lock = asyncio.Lock()


async def startup(ctx: dict[str, Any]) -> None:
    settings = get_settings()
    pool = await create_pool(
        settings.database_url, max_size=settings.osiris_worker_pool_size,
        application_name="osiris-worker")
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
    ctx["boot_serialize_until"] = time.monotonic() + settings.osiris_worker_boot_serialize_s
    if settings.osiris_worker_boot_memtrace_enabled:
        ctx["boot_memtrace_task"] = asyncio.create_task(
            _boot_memtrace(settings.osiris_worker_boot_serialize_s + 30.0))
    register_default_watchers()
    # THE SCHEDULE IS THE SOURCE OF TRUTH; a watermark is only residue. A cron that is removed
    # leaves its vitals behind, and three separate readers went on reporting "NOT SENSING" forever
    # about the miner's crawl months after we deleted it. Reconcile the DB to the schedule at boot,
    # LOUDLY — an organ that vanishes silently is the exact failure this telemetry exists to catch.
    reaped = await reap_decommissioned_jobs(pool)
    if reaped:
        _log.warning("decommissioned organs reaped from telemetry: %s", ", ".join(reaped))
    # THE ONE REAL WORKER GATE (decision 8a830336): unlike mcp_server.main() (which only
    # runs its own boot-time guards under OSIRIS_MCP_TRANSPORT=streamable-http/sse, never
    # the per-session stdio path), this startup() ran BOTH deploy-guard checks below
    # unconditionally, on ANY arq boot from anywhere — including an ad hoc local `arq`
    # invocation (scripts/stranger_test/run.sh's own worker line is a live, documented
    # example) run from a seat's own worktree against the real shared DATABASE_URL. That
    # confessed truthfully but uselessly, 76 times fleet-wide by the time it was measured.
    # `osiris_worker_role` mirrors `osiris_mcp_transport`'s own non-inferred shape exactly:
    # "primary" is set ONLY in the real osiris-worker.service unit (deploy/osiris-worker.
    # service, and the live hand-installed dev unit once updated) — never inferred from
    # cwd or branch ancestry, which is exactly the guessing #113 was refused for tonight.
    if settings.osiris_worker_role == "primary":
        # THE DEPLOY-ORDERING GUARD (thread e6f5556f): LOUD ALARM, never a refusal — see
        # deploy_guard's own module docstring for why. Wrapped defensively here too, on top
        # of check_schema_drift's own internal fail-open: nothing here may ever block a boot.
        try:
            drift = await check_schema_drift(pool)
            if drift:
                await alarm_schema_drift(pool, drift, service="osiris-worker")
            else:
                # THE SCHEMA-DRIFT SUPERSESSION LEG (operator ruling, DM 7035, item 3): a
                # confirmed-clean check closes this service's own older SCHEMA DRIFT alarms.
                with contextlib.suppress(Exception):
                    await resolve_schema_drift_alarms_on_clean_check(
                        pool, service="osiris-worker")
        except Exception as exc:  # noqa: BLE001 — the guard must never become the thing it guards against
            _log.warning("deploy_guard check failed at worker boot: %r", exc)
        # THE REBOOT-IS-A-DEPLOY GUARD (thread 489a39d0): a SEPARATE try/except from the
        # schema check above — a bug in one guard must never suppress the other, same
        # isolation the rest of this module already gives each independent boot-time check.
        try:
            reboot_drift = await check_unreviewed_boot(pool)
            if reboot_drift:
                # THE GRACE WINDOW (thread c27afb62): a ref still unrecorded past 60
                # minutes alarms exactly once, across both services — an ordinary
                # in-flight deploy (this same ref recorded by `osiris deploy` any moment
                # now) alarms nothing at all.
                gated_drift = await check_and_alarm_unreviewed_boot(
                    pool, service="osiris-worker")
                if gated_drift:
                    from src.orchestrator.deploy_guard import _REPO_ROOT, _git_head

                    running_head = _git_head(_REPO_ROOT) or "unknown"
                    src_root = None
                    with contextlib.suppress(Exception):
                        from src.orchestrator.deploy_guard import _resolve_imported_src_root

                        src_root = str(await asyncio.to_thread(_resolve_imported_src_root))
                    await alarm_unreviewed_boot(pool, gated_drift, running_head=running_head,
                                               service="osiris-worker", src_root=src_root)
            else:
                # THE CLEAN-BOOT LEG of the boot-watchdog supersession mechanism (operator
                # ruling, DM 7032): a confirmed-clean boot closes this service's own older
                # alarms. No-ops silently on 'unknown' — the function's own job to decide.
                with contextlib.suppress(Exception):
                    await check_and_resolve_clean_boot(pool, service="osiris-worker")
        except Exception as exc:  # noqa: BLE001 — the guard must never become the thing it guards against
            _log.warning("deploy_guard reboot check failed at worker boot: %r", exc)


async def shutdown(ctx: dict[str, Any]) -> None:
    task = ctx.get("boot_memtrace_task")
    if task is not None:
        task.cancel()
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


async def reap_leases(ctx: dict[str, Any]) -> int:
    """Self-heal: recover resource_leases nobody released (task #103/#119) — a crash, a
    compaction, a dropped session. Mirrors reap_runs exactly; explicit release_lease stays
    the primary path, this is the backstop. Default window matches
    mcp_server.reap_stale_leases' own default (an hour — agent-work-paced, not
    machine-paced like helper_runs' 900s)."""
    return await reap_stale_leases(ctx["pool"])


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
    from pathlib import Path

    from src.actions.core import Actions
    from src.ingest.soul_store import SoulStore
    from src.ingest.telemetry import TelemetryStore
    from src.ingest.transcript_store import TranscriptStore
    from src.orchestrator.neighborhoods import census_trees
    out = await TranscriptStore(ctx["pool"]).backfill()
    # the retained-telemetry sweep rides the same observer switch (task #35): the same
    # free-deterministic class, the same spend gate, the same one-switch-one-cost law
    tel = await TelemetryStore(ctx["pool"]).backfill()
    # the soul store rides the SAME switch too (task #51 piece 1, Lane 1 msg 6527/ruling
    # ba329ccb): measured 89,591 real lines fleet-wide (not the ~20M a linear guess
    # implied), migration 0050's own last_line_idx/last_hash design was built for exactly
    # this resumable-tail shape, and this is the sibling cron this house already trusts
    # for the identical pattern — never SessionEnd/PreCompact, per the ruling's own
    # constraint that nothing may wedge a session's exit
    soul = await SoulStore(ctx["pool"]).backfill(root=Path(root))
    # crush sessions become canonical too (wave 13 item 2, thread 78efd46d): unlike the
    # claude-code half above, crush's own discovery (projects.json + seat offices) never
    # depends on osiris_transcripts, so this runs unconditionally, same free-observer,
    # one-switch-one-cost law — ingest_crush_session resumes from last_line_idx, so a
    # steady-state tick with nothing new costs one SQLite query per known session.
    soul_crush = await SoulStore(ctx["pool"]).backfill_crush()
    # DSH sessions become canonical too (wave 18 item 1, mail 9541 7b8bb398): the same
    # gap crush closed above — DshSessionAdapter feeds harness_turns cleanly via
    # transcript_store's own backfill, but soul_store's own backfill() deliberately
    # excludes it (ingest_path raw-byte-splits on the source file, which would mangle
    # a zstd-compressed DSH session); ingest_dsh_session resumes from last_line_idx
    # the same way, so a steady-state tick with nothing new still pays one decompress
    # per known session (no cheaper stat-gate exists for a compressed file, see
    # backfill_dsh's own docstring)
    soul_dsh = await SoulStore(ctx["pool"]).backfill_dsh()
    # the disk census too (thread 5e37630b): a free walk that makes 'exists on disk'
    # a graph fact — observation only, never the watch list
    roots = [r for r in get_settings().osiris_census_roots.split(":") if r.strip()]
    cs = await census_trees(Actions(ctx["pool"]), roots=roots)
    if cs["minted"]:
        _log.info("disk census minted %d unmodeled repos: %s",
                  len(cs["minted"]), ", ".join(cs["minted"]))
    if cs["refused"]:
        _log.warning("disk census refused %d malformed director%s: %s",
                     len(cs["refused"]), "y" if len(cs["refused"]) == 1 else "ies",
                     ", ".join(r["name"] for r in cs["refused"]))
    return ((sum(out.values()) if out else 0) + tel
            + (sum(soul.values()) if soul else 0)
            + (sum(soul_crush.values()) if soul_crush else 0)
            + (sum(soul_dsh.values()) if soul_dsh else 0) + len(cs["minted"]))


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
    elsewhere and untouched.

    Both rules are reversible and audited (mounts.py, thread 45dd4f3c, Thoth DM 2835) — every
    row either releases writes an `audit_log` snapshot first, undoable via
    `mounts.undrop_dead_project_mount`, the same mechanism #59's own drop already uses."""
    actions = Actions(ctx["pool"])
    released = 0
    by_cwd = live_bodies_by_cwd()
    if by_cwd is not None:
        released += await sweep_ghost_doors(
            actions, body_cwds=set(by_cwd),
            body_projects=set(live_bodies()), actor="cron:sweep_doors")
    released += await sweep_stale_doors(actions, actor="cron:sweep_doors")
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

    pool = ctx["cascade"].actions.pool
    st = get_settings()
    root = st.osiris_sense_sessions
    path = Path(transcript)
    if not root or not await asyncio.to_thread(path.is_file):
        await _mark_ledger_done(pool, transcript)
        return 0
    # THE SCOPE (task #37): a death rite outside the armed projects is DEFERRED, not
    # buried — no spend, and deliberately NO mark_swept, so widening the scope later
    # lets the orphan reaper find this session as ended-and-unread and drain it then.
    if not scope_match(path.parent.name, sense_scopes(st.osiris_sense_projects)):
        _log.info("death-rite sweep deferred (out of scope): %s", path.name)
        await _mark_ledger_done(pool, transcript)
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
        # THE LEDGER'S OWN COMPLETION MARK (Finding A, thread 5177057a): every exit path from
        # this function marks its sweep_ledger row done — whether this call came from the
        # original PreCompact-triggered enqueue or the watchdog's own retry (reap_stuck_sweeps
        # calls this function directly, in-process, same as the orphan reaper's own _arq_sweep
        # precedent). A hiccup here must not leave the row stuck for the watchdog to keep
        # re-driving forever, same "mark done even on failure" logic as mark_swept above.
        await _mark_ledger_done(actions.pool, transcript)
    _log.info("death-rite sweep %s: %s", path.name, report)
    return int(report.get("proposed", 0))


async def _mark_ledger_done(pool: Any, transcript: str) -> None:
    """Every sweep_session exit — success, failure, dark subsystem, or a deliberate out-of-
    scope defer — marks its sweep_ledger row(s) done. An out-of-scope defer or a disabled
    sensing subsystem is a real, stable DECISION, not a stall; retrying either forever would be
    the watchdog nagging (then eventually poison-pill escalating) something that was never
    actually stuck, only ever deferred by design."""
    await pool.execute(
        "UPDATE sweep_ledger SET completed_at = now() "
        "WHERE transcript_path = $1 AND completed_at IS NULL", transcript)


# below this age, a healthy attempt is probably just still running — don't nag it yet
SWEEP_RETRY_SLA = 300
# past this age with no completion, it is not slow — a poison pill (the existing "mark it read
# even if adversary_pass raised" philosophy already absorbs an ordinary content-level failure
# on its FIRST retry, so a row surviving THIS long means the row itself can't reach completion
# at all, not that mining keeps erroring on it). Stop retrying and escalate instead of looping.
SWEEP_RETRY_CEILING = 1800


async def reap_stuck_sweeps(ctx: dict[str, Any]) -> int:
    """Finding A's own watchdog (thread 5177057a, Thoth's design approval DM 1326): sweep_route
    writes one sweep_ledger row per enqueue attempt; this is the only reader that acts on an
    attempt still incomplete past its SLA. Cheap by construction — one indexed query against
    sweep_ledger_pending_idx, no filesystem walk, no spend unless something is actually stuck.

    Catches exactly what B7 (the orphan reaper) structurally cannot: a dropped enqueue on a
    lineage whose first-ever sweep already succeeded. mark_swept's watermark is a one-time-ever
    boolean per transcript file, so the orphan reaper goes permanently blind to that file the
    moment it is swept once — a session compacting every few minutes is exactly this shape.

    Bounded, never loops forever: past SWEEP_RETRY_CEILING with no completion, re-enqueueing
    stops and the row is escalated as a poison pill instead (_escalate_poison_sweep) — a
    durable, fleet-visible Thread plus a logged alarm, the same "confess, don't hide" discipline
    as every other silent-failure class this house has already closed."""
    pool = ctx["cascade"].actions.pool
    stuck = await pool.fetch(
        "SELECT id, transcript_path, session_id, "
        "extract(epoch FROM now() - enqueued_at) AS age_secs "
        "FROM sweep_ledger WHERE completed_at IS NULL "
        "AND enqueued_at < now() - make_interval(secs => $1) "
        "ORDER BY enqueued_at LIMIT 20", float(SWEEP_RETRY_SLA))
    for row in stuck:
        if row["age_secs"] >= SWEEP_RETRY_CEILING:
            await _escalate_poison_sweep(ctx, row)
        else:
            _log.warning("sweep_ledger: retrying stuck enqueue #%s (%.0fs old): %s",
                        row["id"], row["age_secs"], row["transcript_path"])
            # IN-PROCESS, not re-enqueued via arq — same precedent as the orphan reaper's own
            # _arq_sweep, which calls the mining logic directly rather than pushing a nested
            # job onto the queue (this worker's ctx["redis"] is the app's rate-limiter client,
            # not an arq-enqueue-capable connection; sweep_route's own arq pool lives in the
            # MCP server process, a different one).
            await sweep_session(ctx, row["transcript_path"])
    return len(stuck)


async def _escalate_poison_sweep(ctx: dict[str, Any], row: Any) -> None:
    """A sweep that has failed to complete for SWEEP_RETRY_CEILING is not slow, it is STUCK —
    re-enqueueing it forever would burn spend retrying a transcript that will never yield. Stop,
    and confess loudly instead of looping quietly: a durable Thread the fleet can see (open_thread
    dedups on its own summary hash, so a later tick finding the same row doesn't mint a second
    one) plus a logged alarm. Marks the row done either way — this ledger's own job is knowing
    whether to keep retrying, not proving the sweep actually succeeded; the escalation Thread is
    the source of truth for that."""
    from src.orchestrator.capture import open_thread as _open_thread

    _log.warning("sweep_ledger: POISON SWEEP, giving up after %.0fs: id=%s %s",
                row["age_secs"], row["id"], row["transcript_path"])
    actions = ctx["cascade"].actions
    await _open_thread(
        actions,
        f"POISON SWEEP: transcript {row['transcript_path']} (session {row['session_id']}) "
        f"never completed after {SWEEP_RETRY_CEILING // 60} min of retries — the death rite's "
        "mining call is stuck or crashing on this transcript every attempt. Needs a mind to "
        "read it directly and find out why, not another automatic retry.",
        kind="obligation", arc="Compaction-Resilience", source="cron:sweep_watchdog",
    )
    await actions.pool.execute(
        "UPDATE sweep_ledger SET completed_at = now() WHERE id = $1", row["id"])


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
        from src.orchestrator.capture import record_embed_load_failure

        await record_embed_load_failure(actions, cannot_see=f"embed_backfill failed: {exc!r}")
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


async def pit_watch_heartbeat(ctx: dict[str, Any]) -> int:
    """Pit Watch Stage B: alarm on a managed_by pair's ask-graded DM sitting unread while its
    addressee is provably not mid-turn, escalating to the operator's desk after enough
    consecutive sightings (OFF unless osiris_pit_watch_enabled — the kill switch). Never
    dispatches or spawns anything of its own; a DB hiccup logs, never sinks the cron."""
    from src.orchestrator.pit_watch import pit_watch_tick

    actions: Actions = ctx["cascade"].actions
    try:
        report = await pit_watch_tick(actions)
    except Exception as exc:  # a DB hiccup must not kill the cron
        _log.warning("pit watch heartbeat failed: %r", exc)
        return 0
    if report["sighted"] or report["escalated"]:
        _log.info("pit watch heartbeat: %s", report)
    return report["escalated"]


async def fleet_reconcile_heartbeat(ctx: dict[str, Any]) -> int:
    """Task #59 phase 2's scheduled leg — a thin cron shim, same shape as trigger_mail /
    pit_watch_heartbeat: the flag gate (osiris_fleet_reconcile_enabled) and the acting
    logic both live in fleet_reconcile.reconcile_scheduled_tick, never here, so a test can
    exercise the real gate without touching arq. A DB hiccup logs, never sinks the cron."""
    from src.orchestrator.fleet_reconcile import reconcile_scheduled_tick

    actions: Actions = ctx["cascade"].actions
    try:
        report = await reconcile_scheduled_tick(actions)
    except Exception as exc:  # a DB hiccup must not kill the cron
        _log.warning("fleet reconcile heartbeat failed: %r", exc)
        return 0
    acted = len(report.get("folded") or []) + len(report.get("dropped") or [])
    if acted:
        _log.info("fleet reconcile heartbeat: %s", report)
    return acted


async def phantom_heal_heartbeat(ctx: dict[str, Any]) -> int:
    """The phantom-heal sweep's scheduled leg (decision ee012ebc, operator ruling
    7d6815bb) — a no-op unless osiris_phantom_heal_enabled (the kill switch), same shape
    as fleet_reconcile_heartbeat/closure_miner_heartbeat: the flag gate lives here, the
    acting logic (fold_existing_zero_turn_phantoms) lives in agents.py so a test can
    exercise it directly without touching arq. Folds only FRESH zero-turn phantoms never
    before flagged; an already-flagged-but-half-healed row is reported as an obligation
    by the same call, never auto-completed. A DB hiccup logs, never sinks the cron."""
    from src.config.settings import get_settings
    from src.orchestrator.agents import fold_existing_zero_turn_phantoms

    if not get_settings().osiris_phantom_heal_enabled:
        return 0
    actions: Actions = ctx["cascade"].actions
    try:
        folded = await fold_existing_zero_turn_phantoms(actions)
    except Exception as exc:  # a DB hiccup must not kill the cron
        _log.warning("phantom heal heartbeat failed: %r", exc)
        return 0
    if folded:
        _log.info("phantom heal heartbeat: folded %s", folded)
    return len(folded)


async def phantom_fold_reap_heartbeat(ctx: dict[str, Any]) -> int:
    """The phantom/fold backlog reap's scheduled leg (dispatch #185 item (e), ruling
    696d302c, Thoth DM 5464) — a no-op unless osiris_phantom_fold_reap_enabled (the kill
    switch), same shape as fleet_reconcile_heartbeat/phantom_heal_heartbeat: the flag gate
    lives here, the acting logic (phantom_fold_scheduled_tick) lives in
    phantom_fold_reap.py so a test can exercise it directly without touching arq.
    Reinstates a false_mint generation only when registry_census independently confirms a
    live body; invalidates a duplicate works_in edge only when exactly one live target is
    a dead SoftwareProject. A DB hiccup logs, never sinks the cron."""
    from src.config.settings import get_settings
    from src.orchestrator.phantom_fold_reap import phantom_fold_scheduled_tick

    if not get_settings().osiris_phantom_fold_reap_enabled:
        return 0
    actions: Actions = ctx["cascade"].actions
    try:
        report = await phantom_fold_scheduled_tick(actions)
    except Exception as exc:  # a DB hiccup must not kill the cron
        _log.warning("phantom fold reap heartbeat failed: %r", exc)
        return 0
    acted = len(report.get("reinstated") or []) + len(report.get("invalidated") or [])
    if acted:
        _log.info("phantom fold reap heartbeat: %s", report)
    return acted


async def obligation_hygiene_heartbeat(ctx: dict[str, Any]) -> int:
    """The obligation-hygiene no-regrow rule's scheduled leg (dispatch #204 follow-on,
    operator ruling relayed Thoth DM 7161, 2026-09-05): a no-op unless
    osiris_obligation_hygiene_enabled (the kill switch, TRUE BY DEFAULT per the operator's
    own explicit instruction — a named exception to this file's own dark-by-default
    convention), same shape as its seven 15-min siblings — the flag gate lives here, the
    acting logic (obligation_hygiene_scheduled_tick) lives in obligation_hygiene.py so a
    test can exercise it directly without touching arq. N1=7 idle days nudges the
    obligation's own owner (or the operator, per the owner-address fallback); N2=+7 more
    days of silence marks it a STALE-CANDIDATE and briefs the operator's desk. Never
    auto-resolves anything. A DB hiccup logs, never sinks the cron."""
    from src.config.settings import get_settings
    from src.orchestrator.obligation_hygiene import obligation_hygiene_scheduled_tick

    if not get_settings().osiris_obligation_hygiene_enabled:
        return 0
    actions: Actions = ctx["cascade"].actions
    try:
        report = await obligation_hygiene_scheduled_tick(actions)
    except Exception as exc:  # a DB hiccup must not kill the cron
        _log.warning("obligation hygiene heartbeat failed: %r", exc)
        return 0
    acted = len(report.get("nudged") or []) + len(report.get("staled") or [])
    if acted:
        _log.info("obligation hygiene heartbeat: %s", report)
    return acted


async def no_regrow_heartbeat(ctx: dict[str, Any]) -> int:
    """The no-regrow rule's scheduled leg (operator's word via Thoth DM 8606/8618,
    2026-09-09) — a SEPARATE clock from obligation_hygiene_heartbeat above, keyed off
    `stale_after` rather than idle-since-last-touch. A no-op unless
    osiris_no_regrow_enabled (the kill switch, OFF by default). The acting logic
    (no_regrow_scheduled_tick) lives in no_regrow.py so a test can exercise it directly
    without touching arq. A DB hiccup logs, never sinks the cron."""
    from src.config.settings import get_settings
    from src.orchestrator.no_regrow import no_regrow_scheduled_tick

    if not get_settings().osiris_no_regrow_enabled:
        return 0
    actions: Actions = ctx["cascade"].actions
    try:
        report = await no_regrow_scheduled_tick(actions)
    except Exception as exc:  # a DB hiccup must not kill the cron
        _log.warning("no-regrow heartbeat failed: %r", exc)
        return 0
    acted = len(report.get("reclassified") or [])
    if acted:
        _log.info("no-regrow heartbeat: %s", report)
    return acted


async def classification_laws_heartbeat(ctx: dict[str, Any]) -> int:
    """THE STALE-WINDOW SWEEP, INSTALLED ON THE FRESH-INSTALL PATH (thread 28fa9e22,
    operator dispatch wave 3/4, #203's own "ships mechanically, never a coordinator's
    hand pass" mandate): migration 0060 ran ONCE, by hand, at deploy time — but new
    derived threads keep aging past their own 30-day window every day after, and a
    stranger's install has no coordinator to re-run a migration script by hand. This
    cron re-applies the SAME three laws (owner/kind/expiry) on the same 900s cadence as
    its no-regrow siblings, so a fresh `osiris migrate` + boot alone keeps the graph's
    own classification current, forever, with zero hand on it.

    The acting logic (`apply_migration_0060`) lives in migration_0060.py — Khnum's own
    helper, called here verbatim, never a second copy of the closure/owner/kind rules.
    Idempotent by construction (that module's own docstring): a clean re-run over
    already-compliant rows costs nothing. A DB hiccup logs, never sinks the cron.

    ALSO CARRIES THE PROJECT HYGIENE SWEEP (thread 14fae7d3, wave 6 dispatch msg 8063,
    since project_hygiene.py): the same "unclaimed junk expires" reasoning, aimed at
    SoftwareProject stubs instead of Thread rows. Same cadence, same failure discipline —
    a hiccup in either half logs and returns 0 for that half, never sinks the other.

    AND THE GHOST HOUSE-STAMP SWEEP (thread a732e331 clause 3, wave 7 dispatch msg 8079,
    since house_hygiene.py): a Seat's own pre-house-optional ghost stamp, same "unclaimed
    leftover expires mechanically" reasoning a third time — a receipt naming each retired
    seat, same failure discipline as its two siblings above.

    AND THE MECHANICAL FLEET PRUNE (thread 07ca68ca, wave 8, since fleet_prune.py):
    `dead_transcript` (a mount row whose own job_dir anchor directory is gone) and
    `unclaimed_body` (a verified live OS body with no agent_mounts row, bound when
    tree_seat_hint resolves its cwd to a living seat+holder) — deliberately narrower than
    fleet_reconcile's own identity-folding buckets, which stay behind their own
    fleet_reconcile_heartbeat/osiris_fleet_reconcile_enabled kill switch (see fleet_prune.
    py's module docstring for why). PLUS the stacked-header office sub-sweep (thread
    658c2152, folded into 07ca68ca): every active seat's CLAUDE.md checked for a leading
    duplicate header before its compiled marker span, healed through reissue_office
    (adopt=True) — same failure discipline as its three siblings above.

    AND THE PROVENANCE SWEEP HEARTBEAT (wave 15, mail 8840, `apply_provenance_sweep_
    heartbeat` in capture.py): every cardinality-1-mint-or-abstain orphan lane the wave
    built (Agent/Decision/Thread/Reference/Practice/Superstition, each mechanically
    derived, never a guess) re-applied on the same cadence — "so a stranger's install
    self-heals" (the dispatch's own words), never a coordinator re-running a backfill
    script by hand. One lane failing is reported under its own key, never sinks its
    siblings — same discipline this cron already gives its other four sub-sweeps.

    AND THE BOOT DRIFT NUDGE (thread f37aaf1b, v1.1 follow-up piece 1, `apply_boot_
    drift_nudge_sweep` in boot_compiler.py): every active seat whose compiled CLAUDE.md
    carries an older `boot_compiled_version` than the template's current hash gets an
    `open_thread(kind='obligation')` naming `reissue_office(adopt=True)` as the fix —
    nothing read that drift proactively before this; a stale seat only ever got
    recompiled on an explicit reissue call. Idempotent on open_thread's own summary-hash
    dedup, so this never re-nudges the same (stamped, current) gap twice."""
    from src.orchestrator.boot_compiler import (
        apply_boot_drift_nudge_sweep,
        sweep_stacked_office_headers,
    )
    from src.orchestrator.capture import apply_provenance_sweep_heartbeat
    from src.orchestrator.fleet_prune import prune_execute
    from src.orchestrator.house_hygiene import apply_ghost_house_sweep
    from src.orchestrator.migration_0060 import apply_migration_0060
    from src.orchestrator.project_hygiene import apply_project_hygiene_sweep

    actions: Actions = ctx["cascade"].actions
    try:
        report = await apply_migration_0060(actions)
    except Exception as exc:  # a DB hiccup must not kill the cron
        _log.warning("classification laws heartbeat failed: %r", exc)
        report = {}
    acted = int(report.get("owners_resolved", 0)) + int(report.get("kinds_assigned", 0)) \
        + int(report.get("kinds_reclassified", 0)) + int(report.get("expired", 0))
    if acted:
        _log.info("classification laws heartbeat: %s", report)

    try:
        hygiene = await apply_project_hygiene_sweep(actions)
    except Exception as exc:  # a DB hiccup must not kill the cron
        _log.warning("project hygiene sweep failed: %r", exc)
        hygiene = {}
    retired = len(hygiene.get("retired", []))
    if retired or hygiene.get("refused"):
        _log.info("project hygiene sweep: %s", hygiene)

    try:
        ghosts = await apply_ghost_house_sweep(actions)
    except Exception as exc:  # a DB hiccup must not kill the cron
        _log.warning("ghost house-stamp sweep failed: %r", exc)
        ghosts = {}
    ghosts_retired = len(ghosts.get("retired", []))
    if ghosts_retired or ghosts.get("refused"):
        _log.info("ghost house-stamp sweep: %s", ghosts)

    try:
        pruned = await prune_execute(
            actions, actor="cron:classification_laws_heartbeat", execute=True)
    except Exception as exc:  # a DB/census hiccup must not kill the cron
        _log.warning("fleet prune sweep failed: %r", exc)
        pruned = {}
    dropped = len([d for d in pruned.get("dropped_transcripts", []) if "error" not in d])
    bound = len([b for b in pruned.get("bound", []) if b.get("bound")])
    if pruned.get("dropped_transcripts") or pruned.get("bound"):
        _log.info("fleet prune sweep: %s", pruned)

    try:
        headers = await sweep_stacked_office_headers(
            actions, actor="cron:classification_laws_heartbeat")
    except Exception as exc:  # a DB/disk hiccup must not kill the cron
        _log.warning("stacked-header office sub-sweep failed: %r", exc)
        headers = {}
    healed_offices = len(headers.get("healed", []))
    if healed_offices or headers.get("skipped"):
        _log.info("stacked-header office sub-sweep: %s", headers)

    try:
        provenance = await apply_provenance_sweep_heartbeat(
            actions, actor="cron:classification_laws_heartbeat")
    except Exception as exc:  # a DB hiccup must not kill the cron
        _log.warning("provenance sweep heartbeat failed: %r", exc)
        provenance = {}
    provenance_minted = int(provenance.get("total_minted", 0))
    if provenance_minted or any(k.endswith("_error") for k in provenance.get("lanes", {})):
        _log.info("provenance sweep heartbeat: %s", provenance)

    try:
        drift = await apply_boot_drift_nudge_sweep(
            actions, actor="cron:classification_laws_heartbeat")
    except Exception as exc:  # a DB/mail hiccup must not kill the cron
        _log.warning("boot drift nudge sweep failed: %r", exc)
        drift = {}
    drift_nudged = len(drift.get("nudged", []))
    if drift_nudged or drift.get("errors"):
        _log.info("boot drift nudge sweep: %s", drift)

    return (acted + retired + ghosts_retired + dropped + bound + healed_offices
           + provenance_minted + drift_nudged)


async def landing_audit_heartbeat(ctx: dict[str, Any]) -> int:
    """The landing audit's scheduled leg (Thoth DM 5544): task #168 built and tested
    deploy_guard.landing_audit/stale_unmerged_branches — measured live, the mechanism was
    never broken, but its ONLY caller was cmd_deploy, and the deploy cursor had sat at
    e42f7c6 (2026-08-18) for 5 days, blocked by an unrelated gate (Sekhmet's false-mint-live
    lane). Detection was correct; its sole trigger was dark — an adoption gap, not a
    ranking or detection defect. Same thin-shim shape as its five 15-min siblings above:
    the acting logic (deploy_guard.landing_audit) is UNCHANGED, no new surface — a no-op
    unless osiris_landing_audit_enabled (the kill switch). Runs off `_REPO_ROOT`, same as
    check_unreviewed_boot's sibling call above in startup() — correct here because the
    worker daemon always runs from the one canonical deployed checkout, never a seat's own
    worktree (deploy_guard's own worktree-resolution gotcha applies to cmd_deploy, invoked
    by hand from any worktree; it does not apply to this always-primary-service caller). A
    DB or git hiccup logs, never sinks the cron (landing_audit's own NEVER REFUSES law,
    doubled)."""
    from src.config.settings import get_settings
    from src.orchestrator.deploy_guard import _REPO_ROOT, landing_audit

    if not get_settings().osiris_landing_audit_enabled:
        return 0
    actions: Actions = ctx["cascade"].actions
    try:
        audit = await landing_audit(actions, _REPO_ROOT)
    except Exception as exc:  # a DB or git hiccup must not kill the cron
        _log.warning("landing audit heartbeat failed: %r", exc)
        return 0
    acted = len(audit.get("obligations") or [])
    if acted:
        _log.info("landing audit heartbeat: %s", audit)
    return acted


async def closure_miner_heartbeat(ctx: dict[str, Any]) -> int:
    """The closure miner's scheduled leg (Thoth DM 2679, following the deploy that made
    this defensible) — same thin-shim shape as fleet_reconcile_heartbeat: the flag gate
    (osiris_closure_miner_enabled) and the acting logic both live in
    closure.close_by_commits_scheduled_tick, never here. A DB hiccup logs, never sinks
    the cron."""
    from src.ingest.closure import close_by_commits_scheduled_tick

    actions: Actions = ctx["cascade"].actions
    try:
        report = await close_by_commits_scheduled_tick(actions)
    except Exception as exc:  # a DB hiccup must not kill the cron
        _log.warning("closure miner heartbeat failed: %r", exc)
        return 0
    acted = int(report.get("resolved", 0)) + int(report.get("candidates", 0))
    if acted:
        _log.info("closure miner heartbeat: %s", report)
    return acted


async def abstention_miner_heartbeat(ctx: dict[str, Any]) -> int:
    """THE FIRST MINER's own scheduled leg (wave 16, decision 4d622aee, operator
    2026-09-10: "yes, build and wire it") — same thin-shim shape as
    closure_miner_heartbeat: the flag gate (osiris_abstention_miner_enabled) and the
    acting logic both live in abstention_miner.abstention_miner_tick, never here.
    guarded_miner_tick (proposals.py, wave 15 item 4) writes a durable failure receipt
    BEFORE any exception is allowed to propagate; this try/except is the belt to that
    wrapper's own suspenders — a DB hiccup logs, never sinks the cron."""
    from src.orchestrator.abstention_miner import abstention_miner_tick
    from src.orchestrator.proposals import guarded_miner_tick

    actions: Actions = ctx["cascade"].actions
    try:
        report = await guarded_miner_tick(
            actions, "abstention", lambda: abstention_miner_tick(actions))
    except Exception as exc:  # a DB hiccup (or an unrecoverable tick) must not kill the cron
        _log.warning("abstention miner heartbeat failed: %r", exc)
        return 0
    if report.get("action") == "proposed":
        _log.info("abstention miner heartbeat: %s", report)
    return 1 if report.get("action") == "proposed" else 0


async def backfill_decided_in_heartbeat(ctx: dict[str, Any]) -> int:
    """Task #101's own periodic retry (Thoth's grant, DM 2271, riding behind the one-off
    sweep in decisions c6d1598c/e73c1453): the live path (record_decision) only ever
    mints `decided_in` FORWARD, at write time — a decision citing a commit gitlog hasn't
    reached yet is a genuine RACE (ruling c5ab0dcb's Mode B, omission), not an ambiguity,
    and nothing retried it before this. Same idempotent backfill_decided_in the one-off
    sweep used, run on a schedule instead of by hand — most ticks just re-confirm an
    already-near-empty backlog, since the live path already handles the common case.

    DELIBERATELY QUIET ON THE STABLE SKIP: the one-off sweep confirmed 132 commit shas
    cited in this house's own decisions were NEVER ingested at all — a permanent gap this
    cron structurally cannot close (the referent does not exist; no amount of retrying
    changes that). Logging that count every tick would be the exact Stage C disease Thoth
    named: a stable, already-known, unfixable-here fact read back as a fresh alarm 132
    times a day. So only `minted` (a genuinely NEW edge — a real race that just closed)
    triggers a log line, matching this file's own "log only when something happened"
    convention (trigger_mail/pit_watch_heartbeat/fleet_reconcile_heartbeat, above);
    `skipped` still rides in the returned report for record_job's own telemetry, silent
    unless a mind goes looking, never repeated as noise."""
    from src.orchestrator.capture import backfill_decided_in

    actions: Actions = ctx["cascade"].actions
    try:
        report = await backfill_decided_in(actions)
    except Exception as exc:  # a DB hiccup must not kill the cron
        _log.warning("decided_in backfill heartbeat failed: %r", exc)
        return 0
    if report["minted"]:
        _log.info("decided_in backfill heartbeat: minted %d edge(s) (a race just closed)",
                  report["minted"])
    return int(report["minted"])


async def tree_ingest_alarm_heartbeat(ctx: dict[str, Any]) -> int:
    """The tree-ingest census's scheduled leg (thread 5126, operator ruling df646654/
    fe8ec7ff) — a no-op unless osiris_tree_ingest_alarm_enabled (the kill switch), same
    shape as closure_miner_heartbeat/phantom_heal_heartbeat: the flag gate and the acting
    logic both live in tree_ingest.uningested_trees_alarm_tick, never here. It never
    ingests anything itself — it mails the owning Seat a graded 'ask'; ingest_project stays
    that seat's own deliberate second call. A DB hiccup logs, never sinks the cron."""
    from src.orchestrator.tree_ingest import uningested_trees_alarm_tick

    actions: Actions = ctx["cascade"].actions
    try:
        report = await uningested_trees_alarm_tick(actions)
    except Exception as exc:  # a DB hiccup must not kill the cron
        _log.warning("tree ingest alarm heartbeat failed: %r", exc)
        return 0
    alarmed = len(report.get("alarmed") or [])
    if alarmed:
        _log.info("tree ingest alarm heartbeat: %s", report)
    return alarmed


async def retention_heartbeat(ctx: dict[str, Any]) -> int:
    """THE RETENTION HEARTBEAT (wave 12 item 1, operator's word via Thoth DM 8378: "put
    outbox_retention and audit_log_retention ... on the heartbeat"): outbox/audit_log are
    the two highest-churn append-only tables (src.orchestrator.retention's own module
    docstring — measured live 2026-09-08: outbox 803 MB, audit_log 1.2 GB, neither ever
    pruned), so this DELETEs (execute=True, batched — the acting logic lives entirely in
    retention.py, never duplicated here) published outbox rows and audit_log rows older
    than 90 days, once a day (not every-15-min like this file's other siblings — a
    multi-million-row table does not need that granularity, and the operator's own
    acceptance test is "flat over a week", not "flat over 15 minutes"). A no-op unless
    osiris_retention_heartbeat_enabled (TRUE BY DEFAULT, a named exception to this file's
    dark-by-default convention — the operator asked for this to RUN, not merely exist).

    A DESK RECEIPT EVERY RUN (the operator's own explicit acceptance: "the first run's
    counts on the desk"), unlike every sibling above's "log only when something happened"
    convention — a daily cadence means this never floods the desk, and a silent zero-
    delete run is itself useful confirmation the job is alive. `run_at_startup=True` so a
    fresh deploy of this wave gets its first receipt immediately, not at the next 03:30.

    A DB hiccup on either table logs and skips ONLY that table's own retention + receipt
    line — the other table's run is independent, same "one hiccup never sinks a sibling"
    discipline as classification_laws_heartbeat's four sub-sweeps."""
    from src.config.settings import get_settings
    from src.orchestrator.mailbox import send_message
    from src.orchestrator.retention import audit_log_retention, outbox_retention

    if not get_settings().osiris_retention_heartbeat_enabled:
        return 0
    actions: Actions = ctx["cascade"].actions
    pool = actions.pool

    lines: list[str] = []
    deleted = 0
    try:
        outbox = await outbox_retention(pool, days=90, execute=True)
    except Exception as exc:  # a DB hiccup must not kill the cron
        _log.warning("outbox retention heartbeat failed: %r", exc)
    else:
        deleted += outbox["deleted"]
        lines.append(f"outbox: -{outbox['deleted']} rows older than 90d "
                     f"(cutoff {outbox['cutoff']})")

    try:
        audit = await audit_log_retention(pool, days=90, execute=True)
    except Exception as exc:  # a DB hiccup must not kill the cron
        _log.warning("audit_log retention heartbeat failed: %r", exc)
    else:
        deleted += audit["deleted"]
        lines.append(f"audit_log: -{audit['deleted']} rows older than 90d "
                     f"(cutoff {audit['cutoff']})")

    if lines:
        with contextlib.suppress(Exception):  # the desk being unreachable must not sink the cron
            await send_message(
                pool, from_agent="cron:retention_heartbeat", from_project="osiris",
                to_project="operator", body="retention: " + "; ".join(lines),
                desk_kind="fyi", dedup_window_secs=3600)
    if deleted:
        _log.info("retention heartbeat: deleted %d row(s)", deleted)
    return deleted


async def soul_cold_tier_heartbeat(ctx: dict[str, Any]) -> int:
    """THE SOUL STORE'S COLD TIER (wave 12 item 2, thread 78efd46d, operator ruling via
    decision 64ec1905: "memory gets tiers not deletion"): fold a bounded batch of
    sessions untouched for 30+ days (SoulStore.fold_cold_tier_batch — the acting logic
    lives entirely there, never duplicated here) into one compressed soul_lines_cold row
    each. Once a day, same cadence class as retention_heartbeat above (a fold is no more
    urgent than a prune, and both are cheap no-ops on a quiet day) — a no-op unless
    osiris_soul_cold_tier_enabled (TRUE BY DEFAULT, a named exception to this file's
    dark-by-default convention — the operator asked for this to RUN).

    A desk receipt only when something actually folded or errored — unlike retention_
    heartbeat's own "every run" convention: a fold is much rarer per session (30-day
    idle window vs. daily prune eligibility), so most ticks are genuinely nothing to
    report, and a silent-zero receipt every day would be the exact Stage C noise this
    file's other siblings already refuse to produce.

    A DB hiccup on the whole batch logs and returns 0 — SoulStore.fold_cold_tier_batch
    already isolates one bad session's own error into the receipt without aborting its
    siblings, so a hiccup reaching here means something broke before or between
    sessions, not within one."""
    from src.config.settings import get_settings
    from src.ingest.soul_store import SoulStore
    from src.orchestrator.mailbox import send_message

    if not get_settings().osiris_soul_cold_tier_enabled:
        return 0
    actions: Actions = ctx["cascade"].actions
    pool = actions.pool
    try:
        report = await SoulStore(pool).fold_cold_tier_batch(idle_days=30, limit=20)
    except Exception as exc:  # a DB hiccup must not kill the cron
        _log.warning("soul cold tier heartbeat failed: %r", exc)
        return 0
    folded = report["folded"]
    errors = report["errors"]
    if folded or errors:
        saved = sum(f["total_bytes"] - f["compressed_bytes"] for f in folded)
        body = (f"soul cold tier: folded {len(folded)}/{report['candidates']} "
                f"candidate session(s), {saved} byte(s) saved" + (
                    f", {len(errors)} error(s)" if errors else ""))
        with contextlib.suppress(Exception):  # the desk being down must not sink the cron
            await send_message(
                pool, from_agent="cron:soul_cold_tier_heartbeat", from_project="osiris",
                to_project="operator", body=body, desk_kind="fyi", dedup_window_secs=3600)
        _log.info("soul cold tier heartbeat: %s", report)
    return len(folded)


async def harness_backfill_heartbeat(ctx: dict[str, Any]) -> int:
    """THE HARNESS SIGNAL'S OWN CATCH-UP (wave 13 item 3, thread e7f173a6, Thoth's ruling
    msg 8544): mount() stamps a `harness` property going forward, but every mind mounted
    before this wave's own deploy carries no stamp — the fleet render would show every
    one of them as "(box default)" forever, never true, until a coincidental remount.
    This sweeps recently-seen `agent_mounts` rows (last_seen within a day — the live
    fleet's own window, never the whole historical mount table) whose Agent carries no
    `harness` assertion yet, and stamps one from the SAME `_infer_harness` classifier
    mount() itself calls — never a second copy of that shape.

    Same "one bad row must not abort the sweep" law as `backfill`'s own per-session
    try/except, and unconditional (no settings kill switch) — a read-only classification
    off an anchor's own shape, the same class of mechanical, always-on sweep
    classification_laws_heartbeat already is, never a write an operator would want to
    veto."""
    from datetime import UTC, datetime

    from src.mcp_server import _infer_harness

    actions: Actions = ctx["cascade"].actions
    pool = actions.pool
    try:
        rows = await pool.fetch(
            "SELECT DISTINCT ON (m.agent_id) o.id AS object_id, m.agent_id, m.cwd, "
            "m.job_dir FROM agent_mounts m JOIN objects o ON o.canonical = m.agent_id "
            "AND o.type = 'Agent' WHERE m.last_seen > now() - interval '1 day' "
            "AND NOT EXISTS (SELECT 1 FROM current_assertions a "
            "                WHERE a.object_id = o.id AND a.name = 'harness') "
            "ORDER BY m.agent_id, m.last_seen DESC")
    except Exception as exc:  # a DB hiccup must not kill the cron
        _log.warning("harness backfill heartbeat failed: %r", exc)
        return 0
    stamped = 0
    for row in rows:
        try:
            harness = _infer_harness(row["cwd"], row["job_dir"])
            await actions.assert_property(
                row["object_id"], "harness", harness, source_id="cron:harness_backfill",
                observed_at=datetime.now(UTC), confidence=0.7,
                actor="cron:harness_backfill_heartbeat")
            stamped += 1
        except Exception as exc:  # noqa: BLE001 — one bad row must not abort the sweep
            _log.warning("harness backfill failed for %s: %r", row["agent_id"], exc)
            continue
    if stamped:
        _log.info("harness backfill heartbeat: stamped %d agent(s)", stamped)
    return stamped


async def graph_layout_heartbeat(ctx: dict[str, Any]) -> int:
    """THE GRAPH VISUALIZER, wave B item 1 (thread 8839): positions the graph
    INCREMENTALLY, one bounded batch per tick, off graph_layout.layout_batch -- see that
    module's own docstring for why this is a LOCAL relaxation anchored on already-placed
    neighbors, never a global recompute. Read-mostly (the only writes are graph_x/graph_y
    assertions on objects that had none); one bad batch must not kill the cron, same "log
    and wait for the next tick" law every other heartbeat here already holds itself to."""
    from src.orchestrator.graph_layout import layout_batch

    actions: Actions = ctx["cascade"].actions
    try:
        placed = await layout_batch(actions)
    except Exception as exc:  # noqa: BLE001 — a DB hiccup or a bad batch must not kill the cron
        _log.warning("graph layout heartbeat failed: %r", exc)
        return 0
    if placed:
        _log.info("graph layout heartbeat: positioned %d object(s)", placed)
    return placed


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

    BOOT SERIALIZATION (thread 0c03a685): within `ctx["boot_serialize_until"]` (stamped
    once in startup()) this acquires `_boot_lock` before running `fn` — the fifteen
    run_at_startup jobs that used to fire concurrently in the same ~3s window now queue
    one at a time instead. Past that deadline the check is a cheap monotonic comparison
    and the lock is never touched — a scheduled tick's steady-state cost is unchanged.
    """
    @functools.wraps(fn)
    async def run(ctx: dict[str, Any]) -> int:
        deadline = ctx.get("boot_serialize_until")
        if deadline is not None and time.monotonic() < deadline:
            async with _boot_lock:
                return await _run_watched(fn, ctx, every)
        return await _run_watched(fn, ctx, every)
    return run


async def _run_watched(fn: Any, ctx: dict[str, Any], every: int) -> int:
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


class WorkerSettings:
    # enqueueable jobs (the API hands heavy work here instead of running it inline)
    functions: list[Any] = [expand_case_job, sweep_session]
    # RUN_AT_STARTUP HARDENING (Thoth's diagnosis + go-ahead, DM 1338/1350): a cron job WITHOUT
    # run_at_startup only SETS next_run on the worker's first heartbeat rather than firing
    # immediately (arq's own run_cron) — it needs the process to survive uninterrupted to its
    # next real wall-clock tick to ever fire once. If the worker crash-loops around a restart,
    # every flagless job can silently miss its whole window while heartbeat (which HAS the
    # flag) keeps firing and masks the outage — exactly "registered, zero runs, no errors",
    # the reap_orphans symptom this closes. Every cron job below now carries it.
    cron_jobs = [
        cron(watched(drain_cascade, every=5), second=set(range(0, 60, 5)), run_at_startup=True),
        # the watch: evaluate subscriptions every 5s (offset from the cascade drain),
        # pull source deltas once a minute.
        cron(watched(evaluate_watch, every=5), second=set(range(2, 60, 5)), run_at_startup=True),
        cron(watched(run_source_ticks, every=60), minute=set(range(0, 60)), second={0},
             run_at_startup=True),
        # self-heal orphaned claims every 5 min (the failure-drill recovery path).
        cron(watched(reap_runs, every=300), minute=set(range(0, 60, 5)), run_at_startup=True),
        # self-heal orphaned resource_leases every 5 min, same shape (task #103/#119).
        cron(watched(reap_leases, every=300), minute=set(range(1, 60, 5)), run_at_startup=True),
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
        cron(watched(reap_orphans, every=900), minute={7, 22, 37, 52}, second={0}, timeout=600,
             run_at_startup=True),
        # THE SWEEP LEDGER'S WATCHDOG (Finding A, thread 5177057a): B7 above only catches a
        # transcript that never got ANY successful sweep, ever — its watermark is a one-time-
        # ever boolean per file, so it goes permanently blind to a dropped enqueue on a
        # lineage's 2nd/3rd/Nth compaction once the 1st has already succeeded. This is the
        # narrower, faster net: one indexed query (no filesystem walk), retries a stuck
        # enqueue in-process, and escalates (never loops forever) past SWEEP_RETRY_CEILING.
        cron(watched(reap_stuck_sweeps, every=120), minute=set(range(0, 60, 2)), second={20},
             run_at_startup=True),
        # THE GHOST FARM'S BILL: 818 wakes, none of them ever in the ledger. Free (a parse of
        # transcripts we already have), deterministic, once per session — so it rides the
        # OBSERVER's switch, never the adversary's.
        cron(watched(meter_the_wakes, every=600), minute=set(range(3, 60, 10)), second={40},
             timeout=300, run_at_startup=True),
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
             timeout=300, run_at_startup=True),
        # rung 4 walks nightly in the quiet hour: echo-folding is free, summaries are
        # budgeted (≤3/pass, stalest-first) and skip-unchanged by fingerprint
        cron(watched(neighborhood_pass, every=86400), hour={9}, minute={10}, timeout=480,
             run_at_startup=True),
        # the mailbox alarm clock: wake an agent for a project with unread mail — bounded by a
        # per-project rate cap, and a no-op unless osiris_trigger_enabled (the kill switch).
        cron(watched(trigger_mail, every=60), minute=set(range(0, 60)), second={45},
             run_at_startup=True),
        # the pair heartbeat (Pit Watch Stage B): a managed_by pair's ask-graded DM sitting
        # unread while the addressee is not mid-turn escalates to the operator's desk after
        # enough consecutive sightings — a no-op unless osiris_pit_watch_enabled.
        cron(watched(pit_watch_heartbeat, every=300), minute=set(range(0, 60, 5)),
             second={30}, run_at_startup=True),
        # task #59's reaper: bulk-fold/roll-up/drop the reconcile tray's high-confidence
        # rows on a schedule — a no-op unless osiris_fleet_reconcile_enabled (the kill
        # switch), same cadence class as reap_orphans (every 15 min, not urgent cleanup).
        cron(watched(fleet_reconcile_heartbeat, every=900), minute={4, 19, 34, 49},
             second={0}, timeout=600, run_at_startup=True),
        # the closure miner's scheduled leg (Thoth DM 2679): find the commit that witnesses
        # each untouched open thread, on a 15-min cadence — a no-op unless
        # osiris_closure_miner_enabled (the kill switch). Same cadence class as reap_orphans/
        # fleet_reconcile_heartbeat; offset from both (and from embed_pass's :05/:15/... grid)
        # so none of them contend for CPU at the same wall-clock second.
        cron(watched(closure_miner_heartbeat, every=900), minute={10, 25, 40, 55},
             second={5}, timeout=600, run_at_startup=True),
        # THE FIRST MINER's own scheduled leg (wave 16, decision 4d622aee) —
        # osiris_abstention_miner_enabled (the kill switch). Same 15-min cadence class as
        # closure_miner_heartbeat, offset from it and from every other 900s job's own
        # minute/second grid so none of them contend for CPU at the same wall-clock tick.
        cron(watched(abstention_miner_heartbeat, every=900), minute={12, 27, 42, 57},
             second={35}, run_at_startup=True),
        # the phantom-heal sweep's scheduled leg (decision ee012ebc, operator ruling
        # 7d6815bb): fold FRESH zero-turn phantoms fleet-wide, on the same 15-min cadence
        # class as reap_orphans/fleet_reconcile_heartbeat/closure_miner_heartbeat — a
        # no-op unless osiris_phantom_heal_enabled (the kill switch). Offset from all
        # three so none contend for CPU at the same wall-clock second.
        cron(watched(phantom_heal_heartbeat, every=900), minute={13, 28, 43, 58},
             second={10}, timeout=600, run_at_startup=True),
        # task #101's backfill, on a schedule (Thoth's grant DM 2271): closes the RACE
        # where a decision cites a commit before gitlog has ingested it — a cheap,
        # SQL-only scan (no LLM/embedding cost), offset from the other 10-minute jobs
        # (backfill_transcripts@8, embed_pass@5, meter_the_wakes@3) to avoid CPU
        # contention with them.
        cron(watched(backfill_decided_in_heartbeat, every=600), minute=set(range(1, 60, 10)),
             second={50}, timeout=300, run_at_startup=True),
        # the tree-ingest census's scheduled leg (thread 5126, operator ruling df646654/
        # fe8ec7ff): find un-ingested trees and mail the OWNING SEAT, on the same 15-min
        # cadence class as reap_orphans/fleet_reconcile_heartbeat/closure_miner_heartbeat/
        # phantom_heal_heartbeat — a no-op unless osiris_tree_ingest_alarm_enabled (the kill
        # switch). It never ingests; it only alarms. Offset from all four siblings so none
        # contend for CPU at the same wall-clock second.
        cron(watched(tree_ingest_alarm_heartbeat, every=900), minute={1, 16, 31, 46},
             second={15}, timeout=600, run_at_startup=True),
        # the phantom/fold backlog reap (dispatch #185 item (e), ruling 696d302c): reinstate
        # a mis-folded live generation or clean a dead-project works_in duplicate, on the
        # same 15-min cadence class as its five siblings above — a no-op unless
        # osiris_phantom_fold_reap_enabled (the kill switch). Offset from all five so none
        # contend for CPU at the same wall-clock second.
        cron(watched(phantom_fold_reap_heartbeat, every=900), minute={7, 22, 37, 52},
             second={20}, timeout=600, run_at_startup=True),
        # the landing audit's scheduled leg (Thoth DM 5544): mint an obligation for a
        # branch stale-unmerged into main, or graph text whose cited merge is provably not
        # an ancestor of main — the exact detection cmd_deploy already runs, now firing
        # independent of whether a deploy has recently succeeded. Same 15-min cadence class
        # as its six siblings above — a no-op unless osiris_landing_audit_enabled (the kill
        # switch). Offset from all six so none contend for CPU at the same wall-clock second.
        cron(watched(landing_audit_heartbeat, every=900), minute={2, 17, 32, 47},
             second={35}, timeout=600, run_at_startup=True),
        # the obligation-hygiene no-regrow rule (dispatch #204 follow-on, operator ruling
        # relayed Thoth DM 7161, 2026-09-05): N1=7 idle days -> a DM nudge to the
        # obligation's own owner; N2=+7 more days of silence -> a STALE-CANDIDATE marker
        # plus a desk brief, never auto-resolved. Same 15-min cadence class as its seven
        # siblings above — TRUE BY DEFAULT on osiris_obligation_hygiene_enabled, per the
        # operator's own explicit instruction ("land it with the flag ON"), a named
        # exception to every sibling's dark-by-default switch. Offset from all seven so
        # none contend for CPU at the same wall-clock second.
        cron(watched(obligation_hygiene_heartbeat, every=900), minute={5, 20, 35, 50},
             second={40}, timeout=600, run_at_startup=True),
        # the no-regrow rule (operator's word via Thoth DM 8606/8618, 2026-09-09): a
        # SEPARATE clock from obligation_hygiene_heartbeat above, keyed off `stale_after`
        # rather than idle-since-last-touch — 21+ days past stale_after with no touch
        # reclassifies kind='task', with a receipt on the owner's mail, never resolves.
        # Same 15-min cadence class as its eight siblings above — a no-op unless
        # osiris_no_regrow_enabled (the kill switch, OFF by default — no explicit "ship
        # it ON" instruction accompanied this dispatch). Offset from all eight so none
        # contend for CPU at the same wall-clock second.
        cron(watched(no_regrow_heartbeat, every=900), minute={3, 18, 33, 48},
             second={5}, timeout=600, run_at_startup=True),
        # thread 28fa9e22: migration 0060's own three classification laws (owner/kind/
        # expiry), re-applied on a fresh install with no coordinator's hand — same
        # 15-min cadence class, offset from all eight siblings above so none contend for
        # CPU at the same wall-clock second. run_at_startup=True so a stranger's very
        # first boot already carries current classification, not just after 15 minutes.
        cron(watched(classification_laws_heartbeat, every=900), minute={6, 21, 36, 51},
             second={45}, timeout=600, run_at_startup=True),
        # wave 12 item 1: outbox/audit_log retention, once a day (not the 15-min cadence
        # class above — a multi-million-row prune has no need of that granularity). TRUE
        # BY DEFAULT on osiris_retention_heartbeat_enabled, per the operator's own explicit
        # dispatch (Thoth DM 8378) asking for this to run. run_at_startup=True so this
        # wave's own deploy gets its first receipt immediately.
        cron(watched(retention_heartbeat, every=86400), hour={3}, minute={30}, second={0},
             timeout=600, run_at_startup=True),
        # wave 12 item 2: the soul store's cold tier, once a day (offset from wave 12
        # item 1's own retention_heartbeat slot at 03:30 so the two never contend for
        # CPU). TRUE BY DEFAULT on osiris_soul_cold_tier_enabled, per the operator's own
        # explicit dispatch (Thoth DM 8378) asking for this to run. run_at_startup=True
        # so this wave's own deploy gets its first fold pass immediately.
        cron(watched(soul_cold_tier_heartbeat, every=86400), hour={3}, minute={45},
             second={0}, timeout=600, run_at_startup=True),
        # wave 13 item 3: the harness signal's own catch-up sweep — same 15-min cadence
        # class as classification_laws_heartbeat/obligation_hygiene_heartbeat, offset
        # from all nine so none contend for CPU at the same wall-clock second.
        # run_at_startup=True so this wave's own deploy backfills the live fleet
        # immediately, not after 15 minutes of every remaining body reading "(box
        # default)". Unconditional — read-only classification, no kill switch.
        cron(watched(harness_backfill_heartbeat, every=900), minute={8, 23, 38, 53},
             second={50}, timeout=600, run_at_startup=True),
        # WAVE B item 1 (thread 8839): positions the whole graph incrementally, one
        # bounded batch (1000 objects, local relaxation only) every 5 minutes -- a fresh
        # graph reaches full coverage in ~41 ticks (~3.5h) without ever paying for a
        # global recompute; a settled graph's steady-state cost converges toward zero
        # (unpositioned_batch finds nothing left to do). run_at_startup=True so a
        # restart doesn't cost this wave's own first batch a 5-minute wait.
        cron(watched(graph_layout_heartbeat, every=300), minute=set(range(0, 60, 5)),
             second={5}, timeout=300, run_at_startup=True),
    ]
    on_startup = startup
    on_shutdown = shutdown
    # arq reads this attribute AS the RedisSettings (not a callable) — a staticmethod
    # here makes arq do `.host` on the function object and die at boot. Bind the value.
    redis_settings = RedisSettings.from_dsn(get_settings().redis_url)
