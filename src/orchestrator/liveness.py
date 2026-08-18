"""LIVENESS — is that mind ALIVE, or has it merely stopped talking to us?

THE BUG (456960e5; the operator: "there is a bug where agents are getting staled while they are
still working"). `agent_mounts.last_seen` was refreshed ONLY when an agent CALLED Osiris, and every
liveness test in the system reads `last_seen > now() - 15 minutes`. So a mind heads-down for twenty
minutes — writing code, running a suite, reading files — is marked DEAD while it is very much
alive. WE WERE MEASURING CHATTINESS AND CALLING IT ALIVENESS.

IT IS NOT COSMETIC. Eight readers key on that column:
  * the WAKE TRIGGER (×2) — so it can wake a project whose agent is alive and working, putting TWO
    agents on ONE shared tree. That collision has actually happened.
  * the co-agent collision warning at mount — it FLICKERS on and off as a working agent crosses the
    15-minute line (Khepri III reported exactly this and reasonably assumed a race; it is not a
    race, it is the wrong measurement).
  * DM routing, the mailbox lease, the fleet roster, send()'s listener flag.

THE HONEST SIGNAL COSTS NOTHING AND IS SITTING ON THE DISK. A session that is alive is WRITING TO
ITS TRANSCRIPT — whether or not it is talking to Osiris. So liveness is the freshest of (last osiris
call, last transcript write), and the second half is a `stat()`. No model, no inference, no money.

    IT IS AN OBSERVATION, NOT A GUESS — which is why it gets its own switch.

`OSIRIS_TRANSCRIPTS` (this, free, deterministic, always on) is DELIBERATELY SEPARATE from
`OSIRIS_SENSE_SESSIONS` (the adversary's licence to read those same files WITH A MODEL, which costs
money and is gated). Killing the expensive inferrer must never blind the free observer. That is the
whole charter for a background critter: it may OBSERVE for nothing, and it may only INFER on a
licence — and the two must not share a switch, or one day someone pulls the wrong one.

We write into `last_seen` itself rather than a new column, so all eight readers are corrected at the
WRITER and none of them has to be found and changed. GREATEST() means an Osiris call still counts —
this only ever makes a mind look MORE alive, never less.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path

import asyncpg


def _sessions(root: Path) -> list[tuple[str, datetime]]:
    """(session id, last write) for every transcript on disk. A stat(), nothing more.

    Runs in a thread: on a busy box this is a few thousand stats, and the event loop that serves
    the fleet must not stall on the filesystem.
    """
    out: list[tuple[str, datetime]] = []
    try:
        paths = list(root.expanduser().rglob("*.jsonl"))
    except OSError:
        return out
    for p in paths:
        if p.parent.name.endswith("-osiris-extract"):
            continue  # the extractor's own scratch sessions are not minds
        try:
            mtime = p.stat().st_mtime
        except OSError:
            continue
        out.append((p.stem, datetime.fromtimestamp(mtime, UTC)))
    return out


async def observe_liveness(pool: asyncpg.Pool, root: Path) -> int:
    """Stamp every mounted agent's liveness from its transcript's mtime. Returns rows touched.

    Matching is by the job-dir anchor: a mount's `job_dir` ends with the session's short id, which
    is the transcript's stem prefix. That is the same join `_writers_for` uses, and it is why the
    durable anchor matters — a session with no anchor cannot be found on disk and keeps the old,
    chatty signal (degraded, never wrong).

    SECOND KEY (thread #174, Ptah's gap, 2026-08-17), the SAME self-evident derivation
    `find_session_row`'s own lane 3 uses: a `-p --resume` wake's mount row carries a job_dir
    keyed by the WAKE's own fresh job anchor, unrelated to the resumed transcript's own sid —
    the job-dir join above misses it even though the sid genuinely IS that agent's own
    generation-derived identity. Matching also on `agent_id` directly (self-evident, no ledger
    read needed — record_session_anchor deliberately never writes an entry for this exact case)
    closes that gap in the SAME bulk UPDATE, no per-row Python loop. Verified live: Ptah
    (agent:02eaaa7a-iii) read cold at 23:53 while working; his transcript's own mtime never
    promoted his row because job_dir=jobs/c8a22a05 never matched sid 02eaaa7a. (The anchor_sid
    LEDGER lane — find_session_row's lane 2, for a truly-rebound identity the sid alone cannot
    re-derive — is NOT added here: it needs `_generation`'s own roman-numeral suffix stripping,
    which isn't safely expressible as raw SQL; left as a follow-up if it's ever measured to
    matter for liveness specifically, not built speculatively.)

    GREATEST(last_seen, mtime): a transcript that moved makes you alive; an Osiris call still makes
    you alive. This can only ever ADD life, never take it away — a liveness fix that could mark a
    working mind dead would be worse than the bug it replaces.

    THIS IS ALSO THE PROMOTION PATH for a PROVISIONAL seat (save_mount(alive=False)): the whisper
    seats a session without a pulse, and the first thing that transcript writes is what certifies
    the process as a mind. A spare never writes, so it is never promoted — which is the entire
    point, and it costs a stat() we were already doing.
    """
    seen = await asyncio.to_thread(_sessions, root)
    if not seen:
        return 0
    # one round trip: (short_id, mtime) pairs joined against the mounts by their anchor.
    # `last_seen IS NULL` is NOT the same as `v.moved > m.last_seen` — the latter is NULL, which
    # is not TRUE, so a provisional seat would NEVER BE PROMOTED and a real agent could work all
    # day while the fleet read it as dead. The whole provisional design hangs on this one clause.
    rows = await pool.fetch(
        "UPDATE agent_mounts m SET last_seen = GREATEST(m.last_seen, v.moved) "
        "FROM (SELECT * FROM unnest($1::text[], $2::timestamptz[]) AS t(sid, moved)) v "
        "WHERE (m.job_dir LIKE '%' || v.sid "
        "       OR m.agent_id = 'agent:' || v.sid "
        "       OR m.agent_id LIKE 'agent:' || v.sid || '-%') "
        "AND (m.last_seen IS NULL OR v.moved > m.last_seen) "
        "RETURNING m.job_dir, m.agent_id",
        [s[:8] for s, _ in seen], [t for _, t in seen])
    # A PROMOTED ROW MUST FOLLOW ITS LINEAGE HEAD (thread cb2b0a09). Promotion is by mtime,
    # and mtime knows nothing of succession — a row still naming a superseded generation reads
    # as a live CO-AGENT of its own descendant (Ferryman IV beside Ferryman V, Anubis XII
    # beside XIII: every mind onboarded on 2026-07-14 was warned about its own ancestor). The
    # resolution layer already re-walks every call to the head; the registry was the one
    # reader left behind. The guard on the old agent_id makes the re-point lose gracefully to
    # any concurrent mount that already rewrote the row.
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
