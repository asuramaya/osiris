"""THE GRAPH WATERMARK (ruling cf9286b2) — auto-refresh's whole mechanism: poll a cheap
change-marker, never the composition itself. "Re-run the composition every N seconds" fails
in both directions (idle, it burns real work for nothing — the roadmap composition measured
61K chars unbounded, ruling ad19a779/#64; under real write load it re-runs the full spec on
every tick per open tab) and collides with Imhotep's census finding (thread 0d7a4d3c):
census.live_bodies() is a synchronous pgrep + /proc read, so anything touching it on a fast
loop blocks the event loop, fleet-wide, per tab. A watermark that only tells the client
"something moved" costs nothing when nothing did and never runs an expensive Function on a
timer it doesn't need.

FOUR INDEPENDENT MARKERS, not one combined scalar — deliberately. `GREATEST()`-ing raw
sequence values from different tables is WRONG: audit_log's bigserial reaches the millions
while agent_wakes' sits in the low thousands, so a burst of new wakes could still read as
"no change" once audit_log's own id has permanently outgrown it. Comparability only holds
WITHIN a table's own sequence, so each table gets its own scalar and the client compares
them field-by-field ("did ANY key change since I last looked"), never combined into one
number that could silently mask a smaller table's own movement.

WHY THESE FOUR, and why nothing else (yet — this is deliberately not exhaustive; it is the
set that makes the two "seconds" compositions ruling cf9286b2 names — mail, fleet-strip —
and the general knowledge graph actually live):
  - audit_log.id (bigserial, PK-indexed) — EVERY graph write (objects/links/assertions)
    goes through Actions, which writes an audit_log row atomically with the domain write
    (src/actions/core.py's own docstring: "the domain write, its audit_log row, and any
    [outbox event]"). This alone covers threads, decisions, practices, roadmap, docs, lint,
    family, portfolio — most of DEFAULT_COMPOSITIONS.
  - fleet_messages.id (bigserial, PK-indexed) — new mail. audit_log does NOT cover this:
    send_message is a direct INSERT, outside the Actions/audit_log gate (mail is
    operational state, not graph knowledge).
  - agent_mounts.mounted_at (timestamptz, set once at INSERT and never touched again —
    verified against save_mount's own ON CONFLICT clause, which updates agent_id/project/
    cwd/model/session_key/last_seen but never mounted_at) — a NEW agent joining the fleet,
    not a heartbeat. agent_mounts has no serial PK (job_dir text is the natural key) and
    last_seen updates on every heartbeat ping from every live session — using last_seen
    here would make the marker move constantly regardless of whether anything a human
    would call "a change" actually happened.
  - agent_wakes.id (bigserial, PK-indexed) — the wake ledger fleet-strip's own pulse line
    (mounts.fleet_pulse, wired through _fn_fleet_pulse_line) reflects.

MEASURED (2026-07-29, against the live house DB — audit_log 1.77M rows, fleet_messages
~1.9K, agent_mounts 25, agent_wakes ~1.2K): 0.071ms server-side execution time (EXPLAIN
ANALYZE — three index-only backward scans plus one 25-row seq scan), 0.247ms average full
round trip over 200 sequential calls. The query is not the constraint on tick rate; nothing
here is. See compositions.py's _COMP_REFRESH_SECS for the chosen default and why."""
from __future__ import annotations

from typing import Any

import asyncpg

_WATERMARK_SQL = (
    "SELECT "
    " (SELECT max(id) FROM audit_log) AS audit_log, "
    " (SELECT max(id) FROM fleet_messages) AS fleet_messages, "
    " (SELECT max(mounted_at) FROM agent_mounts) AS agent_mounts, "
    " (SELECT max(id) FROM agent_wakes) AS agent_wakes"
)


async def graph_watermark(pool: asyncpg.Pool) -> dict[str, Any]:
    """{table: its own latest marker}, one per table above — None for a genuinely empty
    table (never a false '0 == 0, nothing changed' against a table that later gets its
    first row). The client's whole comparison is "did any of these four values change
    since I last looked", never a cross-table comparison — see the module docstring for
    why combining them into one scalar is actively wrong, not just less clean."""
    row = await pool.fetchrow(_WATERMARK_SQL)
    return {
        "audit_log": row["audit_log"],
        "fleet_messages": row["fleet_messages"],
        "agent_mounts": row["agent_mounts"].isoformat() if row["agent_mounts"] else None,
        "agent_wakes": row["agent_wakes"],
    }
