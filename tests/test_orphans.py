"""THE ORPHAN LANE — a session that died with no rite left NOBODY holding the context.

A REGRESSION I SHIPPED KNOWINGLY. Killing the crawl (B6) made the PreCompact hook the only bell, so
a session that crashes, is kill -9'd, has its laptop closed, or SIMPLY ENDS without reaching the
context ceiling was never read at all. Before, the crawl would have caught it within ten minutes.
That was not a decision — it was a consequence.

AND IT IS THE CASE THAT PROVES THE WHOLE THESIS. A crashed session left nobody to ask what it
forgot. The mind is GONE. Only an outside reader can recover it:

    "if the live agent could settle on its own we would not need osiris or a miner, but the whole
     point is that the adversary checks and we both forget."          — the operator, 2026-07-13

What is tested hardest here is that this is NOT THE CRAWL COMING BACK.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

from src.actions.core import Actions
from src.ingest.orphans import QUIET_SECS, find_orphans, mark_swept, swept_key


def _transcript(root: Path, sid: str, *, quiet_secs: float = 0) -> Path:
    d = root / "-repo"
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{sid}.jsonl"
    p.write_text('{"type":"user","message":{"content":"hello"}}\n')
    if quiet_secs:
        old = time.time() - quiet_secs
        os.utime(p, (old, old))
    return p


async def test_a_session_that_died_with_NO_RITE_is_found(
    actions: Actions, tmp_path: Path,
) -> None:
    """The crash, the kill -9, the closed laptop. Nobody is holding the context any more."""
    crashed = _transcript(tmp_path, "dead0001", quiet_secs=QUIET_SECS + 60)
    found = await find_orphans(actions.pool, tmp_path)
    assert found == [crashed]


async def test_a_LIVE_session_is_NEVER_swept(actions: Actions, tmp_path: Path) -> None:
    """THE MOST IMPORTANT GUARD HERE. Sweeping a live session would mine a conversation MID-THOUGHT
    — minting the question and never seeing the answer, which is the crawl's entire disease and the
    reason 54 of my 264 hand-sorted rows were garbage.

    And we JUST spent a day learning that quiet is not dead (456960e5): a mind heads-down for
    twenty minutes is very much alive. So the quiet window is generous ON PURPOSE. We would rather
    be LATE than WRONG.
    """
    _transcript(tmp_path, "alive001")                              # written just now
    _transcript(tmp_path, "thinking", quiet_secs=20 * 60)          # heads-down for 20 minutes
    assert await find_orphans(actions.pool, tmp_path) == []


async def test_a_transcript_is_read_ONCE_and_NEVER_AGAIN(actions: Actions, tmp_path: Path) -> None:
    """NOT A CRAWL. The crawl re-read every transcript forever, on a clock — its cost was
    (wall-clock × every transcript that ever existed). This reads each dead session ONCE. The
    backlog is finite, it shrinks by construction, and it converges to zero."""
    p = _transcript(tmp_path, "dead0002", quiet_secs=QUIET_SECS + 60)
    assert await find_orphans(actions.pool, tmp_path) == [p]

    await mark_swept(actions.pool, p)
    assert await find_orphans(actions.pool, tmp_path) == [], "it read the same session twice"


async def test_an_EMPTY_yield_still_marks_the_session_READ(
    actions: Actions, tmp_path: Path,
) -> None:
    """AN EMPTY YIELD IS A COMPLETE ANSWER. Most sessions abandon nothing — the adversary's prompt
    says so in as many words. Re-reading a session because it had nothing to say would be paying,
    forever, to be told nothing twice."""
    p = _transcript(tmp_path, "quiet001", quiet_secs=QUIET_SECS + 60)
    await mark_swept(actions.pool, p)          # the sweep ran and found nothing at all
    assert await find_orphans(actions.pool, tmp_path) == []
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM watermarks WHERE key=$1", swept_key(p)) == 1


async def test_the_adversary_s_OWN_scratch_sessions_are_not_orphans(
    actions: Actions, tmp_path: Path,
) -> None:
    """Every `claude -p` the adversary makes writes a transcript of its own. Reaping those would be
    the instrument reading itself — the loop-pathology class, and the exact shape of the bug where
    Osiris mined its own alarm clock."""
    d = tmp_path / "-tmp-osiris-extract"
    d.mkdir(parents=True)
    p = d / "extract1.jsonl"
    p.write_text("{}\n")
    old = time.time() - (QUIET_SECS + 600)
    os.utime(p, (old, old))
    assert await find_orphans(actions.pool, tmp_path) == []


async def test_the_batch_is_BOUNDED_and_the_oldest_go_first(
    actions: Actions, tmp_path: Path,
) -> None:
    """Each sweep is a real model call the licence has to justify, so a backlog is drained, never
    stampeded. Oldest first: the deepest orphan is the one most likely to be lost for good."""
    for i in range(6):
        _transcript(actions_root := tmp_path, f"dead{i:04d}",
                    quiet_secs=QUIET_SECS + 600 - i * 60)   # dead0000 is the oldest
    assert actions_root == tmp_path
    found = await find_orphans(actions.pool, tmp_path, limit=3)
    assert [p.stem for p in found] == ["dead0000", "dead0001", "dead0002"]


def test_the_reaper_is_SCHEDULED_but_the_MINER_still_is_not() -> None:
    """The distinction this whole redesign rests on. DETECTION is an OBSERVATION — a stat() and a
    watermark lookup, free, deterministic, never wrong — so it may run on a clock. The SWEEP is an
    INFERENCE, so it costs money, holds a licence, and runs ONCE per dead session.

    A cron that detects is not a crawl. A cron that mines is.
    """
    from src.workers.arq_worker import WorkerSettings

    crons = {c.coroutine.__name__ for c in WorkerSettings.cron_jobs}
    assert "reap_orphans" in crons          # the free detector may walk
    assert "sense_sessions" not in crons    # the paid inferrer may not

async def test_the_scope_defers_the_reaper_and_never_buries(
    actions: Actions, tmp_path: Path,
) -> None:
    """The adversary's scope, the reaper's half (task #37): a scoped-out ended session is
    not detected — the per-tick BATCH is never spent outside the armed projects — and
    because nothing ever marked it swept, WIDENING the scope hands it straight back to
    this detector. Scope DEFERS reading; it never buries a session."""
    d = tmp_path / "-home-x-code-mono"
    d.mkdir()
    p = d / "dead0003.jsonl"
    p.write_text('{"type":"user","message":{"content":"hello"}}\n')
    old = time.time() - (QUIET_SECS + 60)
    os.utime(p, (old, old))

    assert await find_orphans(actions.pool, tmp_path, scopes=["pokex"]) == []
    assert await find_orphans(actions.pool, tmp_path, scopes=["pokex", "mono"]) == [p]
    assert await find_orphans(actions.pool, tmp_path, scopes=[]) == [p]
