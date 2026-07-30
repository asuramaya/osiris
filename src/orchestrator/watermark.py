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

TASK #109 (Thoth DM 2133, 2026-07-30): auto-refresh covered the LENS — the composition
CONTENT on screen — but nothing told the client the CATALOG had moved: the room switcher's
own `cases`/`compositions` counts (index.html's loadRooms) and the composer sidebar's shelf
of lenses (loadComps) are fetched once at boot/room-switch and never revisited. The
operator's cited case: a compositions backfill landed (verified, deployed), and his sidebar
kept reading a stale count against a room that had actually grown. THREE MORE MARKERS, same
discipline as the four above — never folded into them, since `rooms`/`compositions`/`cases`
are work-artifact tables outside the Actions/audit_log gate entirely (same reason
fleet_messages needed its own field: a direct INSERT, not a domain write):
  - rooms.created_at (uuid PK, no serial column — same shape as agent_mounts) — a new room.
  - compositions.created_at — a new saved composition (lens or watch); this is exactly the
    backfill shape the operator hit. KNOWN GAP, accepted to keep this v1 small (same spirit
    as the lens-poller's own accepted composer-clobber gap above): save_composition's
    re-save path can reassign an EXISTING composition's room_id (or section) without
    inserting a new row, which moves a room's visible count without moving this marker.
    Rare in practice (nothing in the UI re-parents a composition today; only a direct
    MCP/API re-save with an explicit new room_id does) — flagged rather than silently
    assumed away, not chased without a live report the way cf9286b2 itself insists on.
  - cases.created_at OR cases.archived_at, whichever is newer — a case's own room-count
    (list_rooms' `cases` field) drops on archival, not just grows on creation, and
    archived_at is an EXISTING column on the exact rows that already carry created_at, so
    catching both costs nothing extra: no migration, one more GREATEST() in the same query.

MEASURED (2026-07-29, against the live house DB — audit_log 1.77M rows, fleet_messages
~1.9K, agent_mounts 25, agent_wakes ~1.2K): 0.071ms server-side execution time (EXPLAIN
ANALYZE — three index-only backward scans plus one 25-row seq scan), 0.247ms average full
round trip over 200 sequential calls. The query is not the constraint on tick rate; nothing
here is. See compositions.py's _COMP_REFRESH_SECS for the chosen default and why. The three
task #109 additions scan rooms/compositions/cases — tables smaller than agent_mounts' own
25 rows in every environment seen so far — so they ride the same non-factor budget rather
than re-measured from scratch."""
from __future__ import annotations

from typing import Any

import asyncpg

_WATERMARK_SQL = (
    "SELECT "
    " (SELECT max(id) FROM audit_log) AS audit_log, "
    " (SELECT max(id) FROM fleet_messages) AS fleet_messages, "
    " (SELECT max(mounted_at) FROM agent_mounts) AS agent_mounts, "
    " (SELECT max(id) FROM agent_wakes) AS agent_wakes, "
    " (SELECT max(created_at) FROM rooms) AS rooms, "
    " (SELECT max(created_at) FROM compositions) AS compositions, "
    " (SELECT GREATEST(max(created_at), max(archived_at)) FROM cases) AS cases"
)


async def graph_watermark(pool: asyncpg.Pool) -> dict[str, Any]:
    """{table: its own latest marker}, one per table above — None for a genuinely empty
    table (never a false '0 == 0, nothing changed' against a table that later gets its
    first row). The client's whole comparison is "did any of these values change since I
    last looked", never a cross-table comparison — see the module docstring for why
    combining them into one scalar is actively wrong, not just less clean.

    task #109's three additions (rooms/compositions/cases) are the CATALOG half — the
    client polls them separately from the four above (the LENS half, cf9286b2) and, on
    a move, re-fetches /rooms + /compositions rather than re-running whatever composition
    happens to be on screen. Same field, same endpoint, two independent client-side
    watchers reading different keys out of it — not two endpoints, since the query itself
    is one cheap round trip either way."""
    row = await pool.fetchrow(_WATERMARK_SQL)
    return {
        "audit_log": row["audit_log"],
        "fleet_messages": row["fleet_messages"],
        "agent_mounts": row["agent_mounts"].isoformat() if row["agent_mounts"] else None,
        "agent_wakes": row["agent_wakes"],
        "rooms": row["rooms"].isoformat() if row["rooms"] else None,
        "compositions": row["compositions"].isoformat() if row["compositions"] else None,
        "cases": row["cases"].isoformat() if row["cases"] else None,
    }
