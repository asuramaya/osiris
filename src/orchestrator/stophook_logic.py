"""stophook_logic — the PURE HALVES of scripts/osiris_stophook.py's two DB reads (task
#180 piece 2 (b), msg 5253), extracted verbatim so the `/stop` route and the hook's own
direct-connect fallback share ONE implementation instead of drifting copies.

Every real stop-hook invocation used to open its OWN `asyncpg.connect()` — up to two per
call (`_deliverable` always, `_offload_boxes` conditionally) — the same per-process-fork
pattern Thoth measured on the statusline before `/heartbeat` (thread #180): a Stop hook
fires on every turn boundary, fleet-wide, so this was the SAME cost repeated on a
different trigger. `compute_stop_deliverable`/`compute_stop_offload` take an already-open
`conn`/`pool` (the MCP server's shared pool, warm) instead of opening one; the hook script
tries the new `/stop` route first and only falls back to its own direct connect (now
calling these same functions against a throwaway connection) on any failure — a route
outage costs exactly what today already costs, never more, same law `_heartbeat_via_http`
established."""
from __future__ import annotations

from typing import Any

import asyncpg

# The HOOK's patience window (osiris_stophook.py's own STOP_GRACE_SECS) — duplicated here
# rather than imported, because the hook script inserts the repo root onto sys.path itself
# (arbitrary cwd) and this module must not gain a reverse import back onto a scripts/ file.
STOP_GRACE_SECS = 3600


async def compute_stop_deliverable(
    conn: asyncpg.Pool | asyncpg.Connection, *, cwd: str, session_id: str,
) -> dict[str, Any]:
    """Verbatim extraction of osiris_stophook.py's own `_deliverable` body — see that
    function's docstring for the full rationale (the project resolution, the self-echo
    guard, the lineage rollup). Returns a JSON-shaped dict instead of a tuple so the /stop
    route can hand it back unchanged; the hook's own `_deliverable` wrapper unpacks it."""
    from src.orchestrator.mounts import find_session_row
    from src.orchestrator.seats import resolve_project

    row = await find_session_row(conn, session_id or "")
    if row is None or not row["agent_id"]:
        return {"n": 0, "senders": [], "window": None, "bands": {}, "project": None}
    project = await resolve_project(conn, str(row["agent_id"]), cwd)
    me = str(row["agent_id"])
    root, sep, suffix = me.rpartition("-")
    base = root if sep and root and suffix and set(suffix) <= set("ivxlcdm") else me
    n_row = await conn.fetchrow(
        "SELECT count(*) AS n, array_agg(DISTINCT m.from_agent) AS senders, "
        " count(*) FILTER (WHERE m.grade='ask') AS asks, "
        " count(*) FILTER (WHERE m.grade='fyi') AS fyis "
        "FROM fleet_messages m "
        "LEFT JOIN message_recipients r ON r.message_id=m.id AND r.agent_id=$1 "
        "WHERE ((m.to_agent=$1) "
        "   OR (m.to_agent = $4 OR m.to_agent LIKE $4 || '-%') "
        "   OR (m.to_project=$2 AND m.to_agent IS NULL AND m.from_agent <> $1)) "
        "AND m.read_at IS NULL "
        "AND NOT EXISTS (SELECT 1 FROM message_recipients r3 WHERE r3.message_id=m.id "
        "  AND (r3.agent_id=$1 OR r3.agent_id=$4 OR r3.agent_id LIKE $4 || '-%') "
        "  AND r3.read_at IS NOT NULL) "
        "AND (r.delivered_at IS NULL OR r.delivered_at < now() - make_interval(secs => $3))",
        row["agent_id"], project, STOP_GRACE_SECS, base)
    n = int(n_row["n"]) if n_row else 0
    senders = [s for s in (n_row["senders"] or []) if s] if n_row else []
    bands = ({"ask": int(n_row["asks"] or 0), "fyi": int(n_row["fyis"] or 0)}
             if n_row else {})
    return {
        "n": n, "senders": senders, "window": row["context_window_size"],
        "bands": bands, "project": project,
    }


async def compute_stop_offload(
    conn: asyncpg.Pool | asyncpg.Connection, *, session_id: str, cwd: str,
) -> dict[str, bool | None] | None:
    """Verbatim extraction of osiris_stophook.py's own `_offload_boxes` body — see that
    function's docstring for the full rationale (the seat-office cwd resolution, the
    shared `settle_boxes` delegation)."""
    from src.orchestrator.mounts import find_session_row
    from src.orchestrator.offices import _DEFAULT_OFFICE_ROOT
    from src.orchestrator.seats import held_seat
    from src.orchestrator.settle import settle_boxes

    row = await find_session_row(conn, session_id or "")
    if row is None or not row["agent_id"] or not row["mounted_at"]:
        return None
    charter_cwd = cwd
    seat = await held_seat(conn, str(row["agent_id"]))
    if seat and seat.get("handle"):
        charter_cwd = str(_DEFAULT_OFFICE_ROOT / seat["handle"].lower())
    return await settle_boxes(conn, agent_id=str(row["agent_id"]),
                              mounted_at=row["mounted_at"], cwd=charter_cwd,
                              seat_id=seat["seat_id"] if seat else None)
