"""LIVENESS — is that mind ALIVE, or has it merely stopped talking to us?

The operator, 2026-07-12: "there is a bug where agents are getting staled while they are still
working." He was right. `last_seen` was refreshed ONLY when an agent CALLED Osiris, and every
liveness test in the system reads `last_seen > now() - 15 minutes` — so a mind heads-down for
twenty minutes, writing code and running tests, was marked DEAD while it was very much alive.

WE WERE MEASURING CHATTINESS AND CALLING IT ALIVENESS.
"""

from __future__ import annotations

import os
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import asyncpg
from src.actions.core import Actions
from src.orchestrator import mounts
from src.orchestrator.liveness import observe_liveness


async def _live(pool: asyncpg.Pool, within_secs: int = 900) -> set[str]:
    """THE question all eight readers ask, verbatim: last_seen > now() - 15 minutes."""
    rows = await pool.fetch(
        "SELECT agent_id FROM agent_mounts "
        "WHERE last_seen > now() - make_interval(secs => $1)", within_secs)
    return {r["agent_id"] for r in rows}


def _transcript(root: Path, project: str, sid: str, *, age_secs: float = 0) -> Path:
    d = root / project
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{sid}.jsonl"
    p.write_text('{"type":"user","message":{"content":"working"}}\n')
    if age_secs:
        old = time.time() - age_secs
        os.utime(p, (old, old))
    return p


async def _mount(pool: asyncpg.Pool, agent: str, sid: str, *, quiet_for: timedelta) -> None:
    """A mounted agent that last SPOKE to Osiris `quiet_for` ago."""
    await mounts.save_mount(pool, job_dir=f"/home/x/.claude/jobs/{sid[:8]}", agent_id=agent,
                            project="osiris", cwd="/repo", model=None, session_key=None)
    await pool.execute("UPDATE agent_mounts SET last_seen = $2 WHERE agent_id = $1",
                       agent, datetime.now(UTC) - quiet_for)


async def test_a_mind_HEADS_DOWN_is_not_DEAD(actions: Actions, tmp_path: Path) -> None:
    """THE BUG, exactly. An agent that has not called Osiris in twenty minutes — because it has
    been writing code and running a test suite — reads as dead to all eight liveness readers,
    including the WAKE TRIGGER, which would then spawn a second agent onto its shared tree.

    But it has been WRITING TO ITS TRANSCRIPT the whole time, and that is a `stat()` away.
    """
    await _mount(actions.pool, "agent:deep-work", "aaaa1111", quiet_for=timedelta(minutes=20))
    _transcript(tmp_path, "-repo", "aaaa1111")          # ...but the transcript moved just now

    stale_before = await _live(actions.pool)
    assert "agent:deep-work" not in stale_before, "the bug: silent == dead"

    touched = await observe_liveness(actions.pool, tmp_path)
    assert touched == 1

    live_after = await _live(actions.pool)
    assert "agent:deep-work" in live_after, "it was alive the whole time; we were measuring talk"


async def test_liveness_can_only_ever_ADD_life_never_take_it(
    actions: Actions, tmp_path: Path,
) -> None:
    """A liveness fix that could mark a WORKING mind dead would be worse than the bug it replaces.

    GREATEST(last_seen, mtime): an Osiris call still makes you alive, a moving transcript makes
    you alive, and a stale file cannot un-alive you. The two signals are a UNION, never a swap.
    """
    await _mount(actions.pool, "agent:chatty", "bbbb2222", quiet_for=timedelta(seconds=5))
    _transcript(tmp_path, "-repo", "bbbb2222", age_secs=3 * 3600)   # transcript untouched for 3h

    before = await actions.pool.fetchval(
        "SELECT last_seen FROM agent_mounts WHERE agent_id='agent:chatty'")
    await observe_liveness(actions.pool, tmp_path)
    after = await actions.pool.fetchval(
        "SELECT last_seen FROM agent_mounts WHERE agent_id='agent:chatty'")

    assert after == before, "a stale transcript must never drag a talking agent backwards"
    assert "agent:chatty" in await _live(actions.pool)


async def test_a_genuinely_dead_session_STAYS_dead(actions: Actions, tmp_path: Path) -> None:
    """The fix must not trade false-dead for false-alive — which is exactly what simply widening
    the 15-minute window would have done. A session whose transcript has not moved in hours has
    not been writing, and it is not working. It is gone."""
    await _mount(actions.pool, "agent:ghost", "cccc3333", quiet_for=timedelta(hours=4))
    _transcript(tmp_path, "-repo", "cccc3333", age_secs=4 * 3600)

    await observe_liveness(actions.pool, tmp_path)
    assert "agent:ghost" not in await _live(actions.pool)


async def test_the_extractor_s_own_scratch_sessions_are_not_MINDS(
    actions: Actions, tmp_path: Path,
) -> None:
    """The adversary's `claude -p` calls write transcripts too, under a recognizable slug. An
    instrument reading itself is the loop-pathology class, and it would have counted the miner's
    own chatter as a live fleet member."""
    await _mount(actions.pool, "agent:notreal", "dddd4444", quiet_for=timedelta(hours=3))
    _transcript(tmp_path, "-tmp-osiris-extract", "dddd4444")

    assert await observe_liveness(actions.pool, tmp_path) == 0
    assert "agent:notreal" not in await _live(actions.pool)


def test_the_OBSERVER_and_the_INFERRER_do_not_share_a_switch() -> None:
    """THE CHARTER, in one assertion. Observing is FREE, deterministic and always right; inferring
    costs money and is gated on a licence. If they shared a switch, killing the expensive miner
    would silently blind the free liveness signal — and one day someone would pull the wrong one
    and never know what else went dark with it.

    (This is not hypothetical: the honest liveness signal USED to be stamped by the miner's crawl,
    so when the miner was killed on 2026-07-12 the fleet lost its only truthful liveness source and
    nobody noticed, because the two were the same switch.)
    """
    from src.config.settings import Settings

    s = Settings(osiris_sense_sessions="", osiris_transcripts="/home/x/.claude/projects")
    assert not s.osiris_sense_sessions          # the adversary is DARK...
    assert s.osiris_transcripts                 # ...and the observer still sees

    from src.workers.arq_worker import WorkerSettings
    crons = {c.coroutine.__name__ for c in WorkerSettings.cron_jobs}
    assert "sense_liveness" in crons            # free: it may always run
    assert "sense_sessions" not in crons        # costly: summoned only, never walking
