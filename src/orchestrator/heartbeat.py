"""THE STATUSLINE'S SHARED HEARTBEAT (thread #180, 2026-08-18) — the counts/bump logic
`scripts/osiris_statusline.py::_counts` used to own outright, now callable against EITHER
a fresh per-process connection (the script's own fallback path) or the MCP server's own
warm shared pool (the new `/heartbeat` HTTP route) — ONE INGRESS instead of every rendering
tab forking a cold `asyncpg.connect()`.

Thoth's own measurement (msg 5205): 138 tx/s and 23 backends against an idle fleet of 16 —
"at 1000 workers that is 20 backend forks/s from statusline alone against max_connections=100".
`/succession` already proved the pattern (a hook POSTs, the server does the write on its own
pool); this extends it to the FAR heavier statusline read/bump path.

`conn` is deliberately `Any`, not `asyncpg.Connection` — every callee here (`find_session_row`,
`held_seat`, `seat_facts`, `surface.fetch`) already accepts either a Pool or a Connection (both
expose the same fetch/fetchrow/fetchval surface), so the SAME function serves a single warm
connection (the script's own fallback, one connection, one query budget) and a shared pool
(the route, where each sub-query may land on a different pooled connection — safe, since
Postgres read-committed visibility does not depend on connection identity once a write has
committed)."""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, NamedTuple

OnSuccession = Callable[[str, str], Awaitable[str | None]]


class HeartbeatResult(NamedTuple):
    briefs: int
    mail: int
    dm: int
    flight: int
    souls: int
    wakes: int
    owed: int
    owed_here: int
    sick: list[str]
    spend: tuple[float, float, int]
    resolved_project: str | None
    resolved_intent: str | None
    resolved_seat_handle: str | None


def _seat_owns_cwd(cwd: str, *, handle: str, anchor_cwd: str | None) -> bool:
    """Is `cwd` one of THIS seat's own mechanical pin copies — the office, the anchor_cwd
    courtesy copy, or the `~/code/<handle>` scratch-workspace convention (Thoth's d8331496:
    "THREE PIN COPIES AND NO WRITER REACHES ALL THREE") — rather than a genuinely separate
    governed checkout? Containment, not exact match: `cwd` may be a subdirectory of any of
    these roots and still be answered by that root's own `.osiris` (read_project_label's own
    climb-to-repo-root behavior). The scratch-workspace root is best-effort, same convention
    sweep_seat_workspace's own default leans on (mintseat.py's `workspace = Path.home() /
    "code" / handle.lower()` when no custom `path=` was given at mint) — a seat minted with
    an explicit custom path is not covered by this guess, same known gap that verb accepts.

    Pure and cheap: no filesystem I/O beyond what Path.resolve() needs, no DB query — this
    runs on every statusline paint (Thoth's own hard constraint)."""
    from src.orchestrator.offices import _default_office_root

    try:
        target = Path(cwd).resolve()
    except OSError:
        return False
    roots = [_default_office_root() / handle.lower(), Path.home() / "code" / handle.lower()]
    if anchor_cwd:
        roots.append(Path(anchor_cwd))
    for root in roots:
        try:
            root = root.resolve()
        except OSError:
            continue
        if target == root or root in target.parents:
            return True
    return False


async def compute_heartbeat(
    conn: Any, *, project_hint: str, session_id: str, model_id: str = "", model_raw: str = "",
    window_size: int | None = None, intent_hint: str | None = None, lease_secs: int,
    on_succession: OnSuccession | None = None, cwd: str = "",
) -> HeartbeatResult:
    """Verbatim extraction of `_counts`'s own body (task #33's `find_session_row`, ruling
    a882b334's succession-owned model stamp, Thoth's msg 3949/3951 seat fallback, ruling
    e9ef7373's `surface.fetch` single authority) — see that function's own long-standing
    comments for the WHY of each step; only the connection source and the succession call
    moved, nothing about the resolution order changed.

    `cwd` (added: thread 6483/6487/6492, Thoth's own dispatch and ruling) feeds the (A)/(B)
    split his "mark, never pick a winner" prior was missing the discriminator for: at the
    seat's OWN mechanical pin copies (case A — `_seat_owns_cwd` above), nobody ever
    DECLARED the value there, mint wrote it once and no verb kept it synced, so a
    divergence from the graph is fossil, not testimony — the graph wins outright. Anywhere
    else (case B), a `.osiris` pin is a genuinely separate governed checkout speaking for
    itself (577988ed) and keeps winning, unchanged. `model`'s file-wins precedent (ruling
    1874ad35) does NOT transfer here: the pin is a genuine operator INPUT for `model` (a
    deliberate /model swap); `project` is never operator-input the same way, so there is no
    parallel input to protect."""
    from src.orchestrator.mounts import find_session_row

    agent = None
    if session_id:
        bare = model_id.split("[", 1)[0].strip()
        found = await find_session_row(conn, session_id)
        row0 = None
        if found is not None:
            row0 = await conn.fetchrow(
                "UPDATE agent_mounts SET last_seen=now(), "
                "model=COALESCE(model, NULLIF($2,'')), model_raw=NULLIF($3,''), "
                "context_window_size=COALESCE($4, context_window_size) "
                "WHERE job_dir = $1 RETURNING agent_id, model",
                found["job_dir"], bare, model_raw, window_size)
        agent = row0["agent_id"] if row0 else None
        stored = row0["model"] if row0 else None
        if agent and bare and stored and stored.split("[", 1)[0].strip() != bare and on_succession:
            heir = await on_succession(session_id, bare)
            agent = heir or agent
    agent = agent or ""

    resolved_project = project_hint or None
    resolved_intent = intent_hint
    resolved_seat_handle: str | None = None
    if agent:
        from src.orchestrator.seats import held_seat, seat_facts

        seat = await held_seat(conn, agent)
        if seat:
            resolved_seat_handle = seat.get("handle")
            anchor = None
            if seat.get("seat_id"):
                facts = await seat_facts(conn, seat["seat_id"])
                anchor = facts.get("anchor_cwd")
                if resolved_intent is None and anchor:
                    from src.orchestrator.agents import read_project_model
                    resolved_intent = read_project_model(anchor)
            if resolved_project is None:
                resolved_project = seat.get("house")
            elif (cwd and resolved_seat_handle
                  and _seat_owns_cwd(cwd, handle=resolved_seat_handle, anchor_cwd=anchor)):
                # CASE (A): the pin answered from one of THIS seat's own mechanical
                # copies — nobody ever declared it there, so a divergence from the
                # graph is a fossil, not a second witness. Graph wins outright.
                resolved_project = seat.get("house") or resolved_project

    from src.orchestrator import surface

    seg = await surface.fetch(conn, project=resolved_project or "", agent=agent or None,
                              lease_secs=lease_secs)
    return HeartbeatResult(
        seg.briefs_mine.data["briefs"], seg.mail.data["mail"], seg.mail.data["dm"],
        seg.mail.data["flight"], seg.live.data["souls"], seg.wakes.data["wakes"],
        seg.owed.data["owed"], seg.owed_here.data["owed_here"], seg.sensing.data["sick"],
        (seg.spend.data.get("spent", 0.0), seg.spend.data.get("cap", 0.0),
         seg.spend.data.get("blind", 0)),
        resolved_project, resolved_intent, resolved_seat_handle)
