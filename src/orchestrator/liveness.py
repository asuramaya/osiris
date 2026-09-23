"""LIVENESS: is that agent actually alive, or has it merely stopped talking to us?

The underlying bug: agents were getting marked stale while they were still working.
`agent_mounts.last_seen` was refreshed only when an agent called Osiris, and every
liveness test in the system reads `last_seen > now() - 15 minutes`. So an agent heads-down
for twenty minutes, writing code, running a suite, reading files, is marked dead while it
is very much alive. The column was measuring chattiness and calling it aliveness.

This is not cosmetic. Eight readers key on that column:
  * the wake trigger (used twice), so it can wake a project whose agent is alive and
    working, putting two agents on one shared tree. That collision has actually happened.
  * the co-agent collision warning at mount, which flickers on and off as a working agent
    crosses the 15-minute line (reported as an apparent race, but it is not a race, it is
    the wrong measurement).
  * DM routing, the mailbox lease, the fleet roster, send()'s listener flag.

The honest signal costs nothing and is already sitting on disk. A session that is alive is
writing to its transcript, whether or not it is talking to Osiris. So liveness is the
freshest of (last osiris call, last transcript write), and the second half is a `stat()`.
No model, no inference, no cost.

    It is an observation, not a guess, which is why it gets its own switch.

`OSIRIS_TRANSCRIPTS` (this, free, deterministic, always on) is deliberately separate from
`OSIRIS_SENSE_SESSIONS` (the license to read those same files with a model, which costs
money and is gated). Disabling the expensive inferrer must never blind the free observer.
That is the whole design for a background process: it may observe for nothing, and it may
only infer on a license, and the two must not share a switch, or one day someone disables
the wrong one.

We write into `last_seen` itself rather than a new column, so all eight readers are
corrected at the writer and none of them has to be found and changed individually.
GREATEST() means an Osiris call still counts: this can only ever make an agent look more
alive, never less.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path

import asyncpg


def _sessions(root: Path) -> list[tuple[str, datetime]]:
    """(session id, last write) for every transcript on disk. A stat(), nothing more.

    Runs in a thread: under heavy load this is a few thousand stats, and the event loop that
    serves the fleet must not stall on the filesystem.
    """
    out: list[tuple[str, datetime]] = []
    try:
        paths = list(root.expanduser().rglob("*.jsonl"))
    except OSError:
        return out
    for p in paths:
        if p.parent.name.endswith("-osiris-extract"):
            continue  # the extractor's own scratch sessions are not agents
        try:
            mtime = p.stat().st_mtime
        except OSError:
            continue
        out.append((p.stem, datetime.fromtimestamp(mtime, UTC)))
    return out


async def observe_liveness(pool: asyncpg.Pool, root: Path) -> int:
    """Stamp every mounted agent's liveness from its transcript's mtime. Returns rows touched.

    Matching is by the job-dir anchor: a mount's `job_dir` ends with the session's short id,
    which is the transcript's stem prefix. That is the same join `_writers_for` uses, and it is
    why the durable anchor matters: a session with no anchor cannot be found on disk and keeps
    the old, chatty signal (degraded, never wrong).

    Second key, the same self-evident derivation `find_session_row`'s own lane 3 uses: a
    `-p --resume` wake's mount row carries a job_dir keyed by the wake's own fresh job anchor,
    unrelated to the resumed transcript's own sid. The job-dir join above misses that case even
    though the sid genuinely is that agent's own generation-derived identity. Matching also on
    `agent_id` directly (self-evident, no ledger read needed; record_session_anchor deliberately
    never writes an entry for this exact case) closes that gap in the same bulk UPDATE, with no
    per-row Python loop. Verified live: an agent's session read cold while it was still working,
    because its transcript's own mtime never promoted its row when the job_dir didn't match the
    session id directly. (The anchor_sid ledger lane, find_session_row's lane 2, for a truly
    rebound identity the sid alone cannot re-derive, is not added here: it needs `_generation`'s
    own roman-numeral suffix stripping, which isn't safely expressible as raw SQL; left as a
    follow-up if it's ever measured to matter for liveness specifically, not built
    speculatively.)

    GREATEST(last_seen, mtime): a transcript that moved makes you alive; an Osiris call still
    makes you alive. This can only ever add life, never take it away: a liveness fix that could
    mark a working agent dead would be worse than the bug it replaces.

    This is also the promotion path for a provisional seat (save_mount(alive=False)): a session
    can be seated without a confirmed pulse, and the first thing its transcript writes is what
    certifies the process as a real agent. A spare that never writes is never promoted, which is
    the entire point, and it costs a stat() we were already doing.

    Earned-pulse column: this is one of exactly two writers ever allowed to stamp
    `earned_pulse_at` (the other is save_mount's own `alive=True` path); a transcript genuinely
    growing is an earned act, per this module's own reasoning above. First-earn only
    (`COALESCE(m.earned_pulse_at, v.moved)`), the same discipline save_mount's stamp uses, so the
    column answers "did this row ever earn a pulse", never "when was it last promoted".
    """
    seen = await asyncio.to_thread(_sessions, root)
    if not seen:
        return 0
    # one round trip: (short_id, mtime) pairs joined against the mounts by their anchor.
    # `last_seen IS NULL` is not the same as `v.moved > m.last_seen`: the latter is NULL, which
    # is not TRUE, so a provisional seat would never be promoted and a real agent could work all
    # day while the fleet read it as dead. The whole provisional design hangs on this one clause.
    rows = await pool.fetch(
        "UPDATE agent_mounts m SET last_seen = GREATEST(m.last_seen, v.moved), "
        "                          earned_pulse_at = COALESCE(m.earned_pulse_at, v.moved) "
        "FROM (SELECT * FROM unnest($1::text[], $2::timestamptz[]) AS t(sid, moved)) v "
        "WHERE (m.job_dir LIKE '%' || v.sid "
        "       OR m.agent_id = 'agent:' || v.sid "
        "       OR m.agent_id LIKE 'agent:' || v.sid || '-%') "
        "AND (m.last_seen IS NULL OR v.moved > m.last_seen) "
        "RETURNING m.job_dir, m.agent_id",
        [s[:8] for s, _ in seen], [t for _, t in seen])
    # A promoted row must follow its lineage head. Promotion is by mtime, and mtime knows
    # nothing of succession: a row still naming a superseded generation reads as a live
    # co-agent of its own descendant (e.g. "Agent IV" beside "Agent V", the same generation
    # naming convention every agent onboarded under this scheme was warned about with respect
    # to its own ancestor). The resolution layer already re-walks every call to the head; the
    # registry was the one reader left behind. The guard on the old agent_id makes the
    # re-point lose gracefully to any concurrent mount that already rewrote the row.
    from src.orchestrator.agents import lineage_head
    heads: dict[str, str] = {}
    for r in rows:
        cur = r["agent_id"]
        if cur not in heads:
            heads[cur] = await lineage_head(pool, cur)
        if heads[cur] != cur:
            await pool.execute(
                "UPDATE agent_mounts SET agent_id=$2 WHERE job_dir=$1 AND agent_id=$3",
                r["job_dir"], heads[cur], cur)
    return len(rows)
