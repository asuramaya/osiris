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
    await record_job(actions.pool, "sense_sessions", every=600, secs=12.0)
    organs = await organ_health(actions.pool)
    assert [o["verdict"] for o in organs] == ["ok"]
    assert health_banner(organs) is None


async def test_a_job_down_for_hours_is_named_and_timed(actions) -> None:  # type: ignore[no-untyped-def]
    """The exact outage: ten hours of failing ten-minute ticks."""
    now = datetime.now(UTC)
    await _stamp(actions.pool, "sense_sessions", every=600, last_ok=now - timedelta(hours=10),
                 error="RuntimeError('no LLM provider for session-sensing')")
    organs = await organ_health(actions.pool)
    sick = [o for o in organs if o["down"]]
    assert [o["job"] for o in sick] == ["sense_sessions"]
    banner = health_banner(organs)
    assert banner is not None
    assert "sense_sessions" in banner and "10h ago" in banner
    assert "not forming memory" in banner
    assert "no hands" in banner       # it SURFACES; it never restarts anything


async def test_a_live_worker_does_not_vouch_for_a_dead_job(actions) -> None:  # type: ignore[no-untyped-def]
    """THE TRAP. The worker heartbeat was GREEN for all ten hours — it was beating happily while
    the job inside it failed every tick. Per-process health would have reported all-clear while
    the graph went blind, which is why vitals are per-JOB."""
    await write_heartbeat(actions.pool)                                  # worker: alive, healthy
    await record_job(actions.pool, "heartbeat", every=30, secs=0.01)     # its own cron: fine
    await _stamp(actions.pool, "sense_sessions", every=600,
                 last_ok=datetime.now(UTC) - timedelta(hours=10), error="boom")
    organs = await organ_health(actions.pool)
    by_job = {o["job"]: o for o in organs}
    assert by_job["heartbeat"]["verdict"] == "ok"       # the process is beating...
    assert by_job["sense_sessions"]["down"]             # ...and the memory has still stopped
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


async def test_a_failure_does_not_erase_when_it_last_worked(actions) -> None:  # type: ignore[no-untyped-def]
    """'Down 4 minutes' and 'down ten hours' are different emergencies — a failure records the
    error WITHOUT clearing last_ok, or the reader cannot tell them apart."""
    await record_job(actions.pool, "sense_sessions", every=600, secs=9.0)
    await record_job(actions.pool, "sense_sessions", every=600, secs=1.0, error="claude CLI exit 1")
    organs = await organ_health(actions.pool)
    o = organs[0]
    assert o["last_ok"] is not None            # we still know when it last truly worked
    assert o["fails"] == 1
    assert o["last_error"] == "claude CLI exit 1"
    assert o["verdict"] == "ok"                # one bad tick after a good one is not an outage


async def test_the_seam_records_a_failure_the_job_tried_to_hide(actions) -> None:  # type: ignore[no-untyped-def]
    """THE ROOT CAUSE, guarded. sense_sessions used to catch its own exception, log a warning
    nobody reads, and `return 0` — which is indistinguishable from a clean tick with nothing to
    mine. It looked green for ten hours.

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
    organs = await organ_health(actions.pool)  # ...but the failure is ON THE RECORD
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
    organs = await organ_health(actions.pool)
    assert next(x for x in organs if x["job"] == "a_good_job")["verdict"] == "ok"
