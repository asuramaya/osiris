"""ORGAN HEALTH — can Osiris tell you it has stopped sensing?

The session-miner died at 08:50 on 2026-07-12 and stayed dead for ten hours. Every ten-minute
tick failed, the graph stopped forming memory, and nothing told anyone. Two things that look
like fixes and are not, both under test here:

  * a WATCHDOG CRON would have lived inside the very worker that died — so health is DERIVED at
    read time by whoever asks, never written by a process that can die with the thing it watches.
  * the WORKER HEARTBEAT was green the entire ten hours. The process was alive and healthy; the
    JOB inside it was failing. So vitals are per-JOB, never per-process.

And the reason nobody noticed: the miner CAUGHT its own exception and returned 0 — a failure
that looks exactly like a clean tick with nothing to do. A job that swallows its error looks green.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import asyncpg
from src.actions.core import Actions
from src.orchestrator.monitor import (
    health_banner,
    organ_health,
    record_job,
    write_heartbeat,
)


async def _stamp(pool: asyncpg.Pool, job: str, *, every: int, last_ok: datetime | None,
                 error: str | None = None) -> None:
    """Forge a job's watermark directly — the shape record_job writes, aged to taste."""
    blob: dict[str, object] = {"every": every, "last_run": datetime.now(UTC).isoformat()}
    if last_ok is not None:
        blob["last_ok"] = last_ok.isoformat()
    if error:
        blob["last_error"] = error
        blob["fails"] = 3
    await pool.execute(
        "INSERT INTO watermarks (key, cursor, updated_at) VALUES ($1,$2,now()) "
        "ON CONFLICT (key) DO UPDATE SET cursor=EXCLUDED.cursor", f"job:{job}", json.dumps(blob))


async def test_a_healthy_body_says_nothing(actions) -> None:  # type: ignore[no-untyped-def]
    """Silence when well is the whole design: an alarm that is always lit is wallpaper."""
    await record_job(actions.pool, "embed_pass", every=600, secs=12.0)
    organs = await organ_health(actions.pool)
    assert [o["verdict"] for o in organs] == ["ok"]
    assert health_banner(organs) is None


async def test_a_job_down_for_hours_is_named_and_timed(actions) -> None:  # type: ignore[no-untyped-def]
    """The exact outage: ten hours of failing ten-minute ticks."""
    now = datetime.now(UTC)
    await _stamp(actions.pool, "embed_pass", every=600, last_ok=now - timedelta(hours=10),
                 error="RuntimeError('no LLM provider for session-sensing')")
    organs = await organ_health(actions.pool)
    sick = [o for o in organs if o["down"]]
    assert [o["job"] for o in sick] == ["embed_pass"]
    banner = health_banner(organs)
    assert banner is not None
    assert "embed_pass" in banner and "10h ago" in banner
    # THE BANNER MUST NOT OVERSTATE. It used to end "the graph is not forming memory" —
    # true only of the miner's crawl, which no longer exists. Deliberate capture rides
    # no cron and cannot be broken by one; claiming otherwise is the same crime as
    # everything else we killed this week.
    assert "not forming memory" not in banner
    assert "Deliberate capture" in banner and "still works" in banner
    assert "no hands" in banner       # it SURFACES; it never restarts anything


async def test_a_live_worker_does_not_vouch_for_a_dead_job(actions) -> None:  # type: ignore[no-untyped-def]
    """THE TRAP. The worker heartbeat was GREEN for all ten hours — it was beating happily while
    the job inside it failed every tick. Per-process health would have reported all-clear while
    the graph went blind, which is why vitals are per-JOB."""
    await write_heartbeat(actions.pool)                                  # worker: alive, healthy
    await record_job(actions.pool, "heartbeat", every=30, secs=0.01)     # its own cron: fine
    await _stamp(actions.pool, "embed_pass", every=600,
                 last_ok=datetime.now(UTC) - timedelta(hours=10), error="boom")
    organs = await organ_health(actions.pool)
    by_job = {o["job"]: o for o in organs}
    assert by_job["heartbeat"]["verdict"] == "ok"       # the process is beating...
    assert by_job["embed_pass"]["down"]             # ...and an organ has still stopped
    assert health_banner(organs) is not None


async def test_a_job_that_never_ran_is_not_mistaken_for_a_healthy_one(actions) -> None:  # type: ignore[no-untyped-def]
    """Absence of evidence is not evidence of health — a job with no success EVER is not 'ok'."""
    await _stamp(actions.pool, "trigger_mail", every=60, last_ok=None, error="never started")
    organs = await organ_health(actions.pool)
    assert organs[0]["verdict"] == "never" and organs[0]["down"]
    assert "never ran" in (health_banner(organs) or "")


async def test_late_is_not_dead(actions) -> None:  # type: ignore[no-untyped-def]
    """One missed cadence is a blip; three is an outage. A tripwire that fires on jitter gets
    muted, and a muted tripwire is the thing we are trying to fix."""
    now = datetime.now(UTC)
    await _stamp(actions.pool, "embed_pass", every=600, last_ok=now - timedelta(minutes=17))
    organs = await organ_health(actions.pool)
    assert organs[0]["verdict"] == "stale" and not organs[0]["down"]
    assert health_banner(organs) is None        # stale does not cry wolf


async def test_a_fast_cadence_job_survives_a_deploy_restart_under_the_floor(
    actions: Actions,
) -> None:
    """drain_cascade/evaluate_watch run every=5s — 3x that is 15s, which a routine deploy
    restart's cancel-and-resume cost (measured up to ~44s, 2026-09-01) blows past on its own.
    `sick_after_secs` floors the DOWN threshold (never the 'stale' one — 1.5x cadence still
    means late) so THIS job stays merely 'stale', never 'down', through that cost, while a
    job genuinely dead past the floor still reads 'down' — the same rule surface.py's
    `_sensing` now imports from here instead of hand-copying (Thoth msg 6327)."""
    from src.orchestrator.monitor import _SICK_FLOOR_SECS

    now = datetime.now(UTC)
    await _stamp(actions.pool, "drain_cascade", every=5,
                 last_ok=now - timedelta(seconds=_SICK_FLOOR_SECS - 5))
    organs = await organ_health(actions.pool)
    assert organs[0]["verdict"] == "stale" and not organs[0]["down"]

    await _stamp(actions.pool, "drain_cascade", every=5,
                 last_ok=now - timedelta(seconds=_SICK_FLOOR_SECS + 5))
    organs = await organ_health(actions.pool)
    assert organs[0]["verdict"] == "down" and organs[0]["down"]


async def test_a_failure_does_not_erase_when_it_last_worked(actions) -> None:  # type: ignore[no-untyped-def]
    """'Down 4 minutes' and 'down ten hours' are different emergencies — a failure records the
    error WITHOUT clearing last_ok, or the reader cannot tell them apart."""
    await record_job(actions.pool, "embed_pass", every=600, secs=9.0)
    await record_job(actions.pool, "embed_pass", every=600, secs=1.0, error="claude CLI exit 1")
    organs = await organ_health(actions.pool)
    o = organs[0]
    assert o["last_ok"] is not None            # we still know when it last truly worked
    assert o["fails"] == 1
    assert o["last_error"] == "claude CLI exit 1"
    assert o["verdict"] == "ok"                # one bad tick after a good one is not an outage


async def test_the_seam_records_a_failure_the_job_tried_to_hide(actions) -> None:  # type: ignore[no-untyped-def]
    """THE ROOT CAUSE, guarded. The SESSION-MINER used to catch its own exception, log a warning
    nobody reads, and `return 0` — indistinguishable from a clean tick with nothing to mine. It
    looked green for ten hours. (Its cron is gone now, killed in ceae1604, so the specimen below
    is a synthetic job; the disease it carries is the miner's.)

    Error handling now lives in the `watched` seam, not in the jobs, so a job CANNOT swallow its
    own failure and still be counted alive. A cron added next year inherits this without its
    author knowing the seam exists.
    """
    from src.workers.arq_worker import watched

    async def a_job_that_hides_its_error(ctx: dict[str, object]) -> int:
        raise RuntimeError("no LLM provider for session-sensing")

    ctx = {"cascade": type("C", (), {"actions": actions})()}
    out = await watched(a_job_that_hides_its_error, every=600)(ctx)

    assert out == 0                            # the cron survives — a hiccup never kills it
    # a SYNTHETIC job is not in the live schedule, so we name it explicitly — the filter that
    # hides decommissioned crons must not also hide a job a test deliberately invented.
    organs = await organ_health(actions.pool, scheduled={"a_job_that_hides_its_error"})
    o = next(x for x in organs if x["job"] == "a_job_that_hides_its_error")
    assert o["verdict"] == "never"             # it has never once succeeded
    assert o["down"] and "no LLM provider" in o["last_error"]


async def test_the_seam_records_success_without_touching_the_result(actions) -> None:  # type: ignore[no-untyped-def]
    """Telemetry must never alter, delay, or fail the work it watches."""
    from src.workers.arq_worker import watched

    async def a_good_job(ctx: dict[str, object]) -> int:
        return 7

    ctx = {"cascade": type("C", (), {"actions": actions})()}
    assert await watched(a_good_job, every=60)(ctx) == 7
    organs = await organ_health(actions.pool, scheduled={"a_good_job"})   # synthetic: named
    assert next(x for x in organs if x["job"] == "a_good_job")["verdict"] == "ok"


async def test_a_DECOMMISSIONED_organ_stops_nagging_forever(actions: Actions) -> None:
    """AN ORGAN THAT IS NO LONGER SCHEDULED IS NOT AN ORGAN.

    A watermark row OUTLIVES the job that wrote it. When we killed the session-miner's crawl
    (ceae1604), its `job:sense_sessions` row stayed behind — last_ok frozen at the moment it
    died — so organ_health would have read it as DOWN forever and nagged the operator at every
    single prompt about a capability we deliberately removed.

    The schedule is the source of truth; the watermark is only residue. An alarm that is always
    on is an alarm nobody reads.
    """
    from src.orchestrator import monitor

    await record_job(actions.pool, "heartbeat", every=30, secs=0.01)
    await record_job(actions.pool, "sense_sessions", every=600, secs=1.0)   # the ghost

    organs = await organ_health(actions.pool)
    jobs = {o["job"] for o in organs}
    assert "heartbeat" in jobs
    assert "sense_sessions" not in jobs, "a job we deleted must not haunt the health banner"

    # ...and if we CANNOT ask the schedule, we trust the watermarks rather than hide a sick organ
    saved = monitor.scheduled_jobs
    monitor.scheduled_jobs = lambda: set()          # type: ignore[assignment]
    try:
        organs = await organ_health(actions.pool)
    finally:
        monitor.scheduled_jobs = saved              # type: ignore[assignment]
    assert "sense_sessions" in {o["job"] for o in organs}, "fail LOUD, never quiet"


async def test_a_DECOMMISSIONED_organ_is_REAPED_not_merely_hidden(actions: Actions) -> None:
    """THE OPERATOR CAUGHT THIS ONE: "sense_sessions not sensing though."

    He was right, and it is the bug I have committed all week. I DID fix it — in ONE of the three
    places that ask "is Osiris sensing?". organ_health got a filter. The STATUSLINE re-implements
    the same check inline (it is standalone on purpose and imports nothing). PREFLIGHT has its own
    copy again. A correction that lands at one site and not at the others that READ is not a
    correction.

    So the fix is not a third filter — it is to STOP LYING IN THE DB. The schedule is the source of
    truth; the watermark is only residue; and the worker, which knows its own schedule, reconciles
    the two at boot. Every reader is corrected for free, including one written next year by someone
    who never learns this happened.
    """
    from src.orchestrator.monitor import reap_decommissioned_jobs

    await record_job(actions.pool, "heartbeat", every=30, secs=0.01)     # a real, scheduled cron
    await record_job(actions.pool, "sense_sessions", every=600, secs=1.0)  # the ghost of the crawl

    reaped = await reap_decommissioned_jobs(actions.pool)
    assert reaped == ["sense_sessions"], "the ghost must be named as it goes — never silently"

    left = {r["key"] for r in await actions.pool.fetch(
        "SELECT key FROM watermarks WHERE key LIKE 'job:%'")}
    assert left == {"job:heartbeat"}, "the residue is GONE from the DB, not merely hidden at a lens"

    # ...and it is idempotent: a second boot reaps nothing and says nothing
    assert await reap_decommissioned_jobs(actions.pool) == []
