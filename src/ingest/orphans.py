"""THE ORPHAN LANE — a session that died with no rite left nobody holding the context.

THIS IS A REGRESSION I SHIPPED, KNOWINGLY, AND THIS IS THE FIX FOR IT.

Killing the crawl (B6, ceae1604) made `sweep_session` the ONLY way the adversary ever runs — and it
fires on the PreCompact hook. So a session that crashes, is kill -9'd, has its laptop closed, or
SIMPLY ENDS WITHOUT REACHING THE CONTEXT CEILING is never read at all. Before, the crawl would have
picked it up within ten minutes. That was not a decision. It was a consequence, and I owe it a fix.

AND IT IS THE ONE CASE THAT PROVES WHY THE ADVERSARY CANNOT BE THE AGENT.

    "if the live agent could settle on its own we would not need osiris or a miner, but the whole
     point is that the adversary checks and we both forget."          — the operator, 2026-07-13

A crashed session left NOBODY holding the context. There is no mind to ask what it forgot; the mind
is gone. Only an outside reader can recover it — which is the entire thesis, in its purest form.

THIS IS NOT A CRAWL, AND THE DIFFERENCE IS THE WHOLE POINT:

  THE CRAWL     walked EVERY transcript, FOREVER, on a clock — re-reading live conversations in
                byte-chunks with no memory, minting the question and never seeing the answer. Its
                cost was (wall-clock × every transcript that has ever existed).

  THE REAPER    DETECTS, for free, which sessions have ENDED and were never read — a stat() and a
                watermark lookup, no model, no money, no guessing. Each one is then swept ONCE, as
                a whole, by the same licence-gated, semaphore-bounded death rite that a graceful
                session gets. Its cost is (sessions that actually died un-swept), and it converges
                to zero. A transcript is swept once and never again.

The detection is an OBSERVATION. Only the sweep INFERS, and the sweep must hold a licence.
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

import asyncpg

from src.ingest.scope import scope_match, sense_scopes

# A session is ENDED when its transcript has been still this long. Generous on purpose: a mind
# heads-down for twenty minutes is ALIVE (456960e5 — we just spent a day learning that), and
# sweeping a live session would mine a conversation mid-thought, which is the crawl's whole disease.
# We would rather be late than wrong.
QUIET_SECS = 45 * 60

# Per tick. The backlog is finite and shrinking by construction, so it does not have to be cleared
# in one pass — and each sweep is a real model call the licence has to justify.
BATCH = 3

_SWEPT = "swept:"   # watermark: this transcript has been read, once, forever


def swept_key(path: Path) -> str:
    return f"{_SWEPT}{path.stem}"


def _ended(root: Path, quiet_secs: int, scopes: list[str] | None = None) -> list[Path]:
    """Transcripts that have STOPPED MOVING — sorted oldest-first, so the deepest orphan is
    recovered before the freshest. A stat(), nothing more. `scopes` (the adversary's scope,
    task #37) skips scoped-out projects at detection so the per-tick BATCH is never spent
    on transcripts the licence doesn't cover — and since a scoped-out orphan is never
    marked swept, widening the scope later hands it to this very detector."""
    now = time.time()
    out: list[tuple[float, Path]] = []
    try:
        paths = list(root.expanduser().rglob("*.jsonl"))
    except OSError:
        return []
    for p in paths:
        if p.parent.name.endswith("-osiris-extract"):
            continue  # the adversary's own scratch sessions: an instrument reading itself
        if not scope_match(p.parent.name, scopes or []):
            continue  # the licence is armed for other projects: defer, never bury
        try:
            mtime = p.stat().st_mtime
        except OSError:
            continue
        if now - mtime >= quiet_secs:
            out.append((mtime, p))
    out.sort()
    return [p for _, p in out]


async def find_orphans(
    pool: asyncpg.Pool, root: Path, *, quiet_secs: int = QUIET_SECS, limit: int = BATCH,
    scopes: list[str] | None = None,
) -> list[Path]:
    """Sessions that ENDED and were never read. Free: a stat() and a watermark lookup.

    NOT "sessions that look interesting" and NOT "sessions that are stale" — ENDED AND UNREAD, both
    of which are FACTS. A reaper that acts on a suspicion is the janitor with opinions, and we have
    already ruled that out (69d64899).

    `scopes` = the adversary's project scope: None reads OSIRIS_SENSE_PROJECTS, [] is
    explicitly unscoped.
    """
    if scopes is None:
        from src.config.settings import get_settings
        scopes = sense_scopes(get_settings().osiris_sense_projects)
    ended = await asyncio.to_thread(_ended, root, quiet_secs, scopes)
    if not ended:
        return []
    known = {r["key"] for r in await pool.fetch(
        "SELECT key FROM watermarks WHERE key LIKE $1", f"{_SWEPT}%")}
    return [p for p in ended if swept_key(p) not in known][:limit]


async def mark_swept(pool: asyncpg.Pool, path: Path) -> None:
    """This transcript has been READ. Once, and never again.

    Written whatever the sweep returned — including nothing at all. AN EMPTY YIELD IS A COMPLETE
    ANSWER (most sessions abandon nothing), and re-reading a session because it had nothing to say
    would be paying, forever, to be told nothing twice.
    """
    await pool.execute(
        "INSERT INTO watermarks (key, cursor, updated_at) VALUES ($1,'1',now()) "
        "ON CONFLICT (key) DO UPDATE SET updated_at=now()", swept_key(path))
