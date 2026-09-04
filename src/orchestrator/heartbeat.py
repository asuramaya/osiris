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

    `cwd` (added: thread 6483/6487/6492) used to feed an (A)/(B) split that let a seat's
    own `house` override an already-resolved pin at the seat's own mechanical pin copies
    (office/anchor/workspace) — on the premise that nobody ever DECLARES a value there, so
    a divergence from the graph must be a mint fossil. THAT PREMISE BROKE the day
    found_seat/mint_seat stopped fabricating `project` from the handle (decision
    24e0b761/commit cf201a9): the office pin is now exactly where a seat's project gets
    DELIBERATELY declared, so overriding it with `house` reintroduced the same class of
    fabrication one hop over — the live specimen (operator bug, msg 6934, thread
    19d6bdcb7fa9): Chad's pin correctly says `cdking`, but the statusline rendered
    `Chad·Chad` because `house` (itself fabricated at mint — a seat founded via
    found_seat/mint_seat gets `house=handle` unconditionally, never a project) won over it.

    RESOLUTION ORDER NOW, PLAINLY: (1) the PIN — `project_hint`, however it resolved —
    wins outright the instant it resolves to anything; no cwd-based override, ever. (2)
    Absent a pin, the seat's own DECLARED `charter` — if it names exactly one repo, that
    repo is the project; more than one is genuine ambiguity, not this function's call to
    break. (3) Absent both, the agent's own LINEAGE `works_in` (`lineage_works_in`,
    merge-normalized through `_normalize_project_label_through_merge`) — the same
    ABSTAIN law that lookup already enforces (only when the whole lineage agrees). `house`
    NEVER stands in for `project` anywhere in this order — it answers a different
    question (which house a seat belongs to), and conflating the two is the exact bug this
    fix closes. `model`'s file-wins precedent (ruling 1874ad35) does NOT transfer here:
    the pin is a genuine operator INPUT for `model` (a deliberate /model swap); `project`
    is never operator-input the same way, so there is no parallel input to protect."""
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
            # PIN WINS OUTRIGHT (see this function's own docstring for the full law and
            # the live specimen that caught its absence): once `resolved_project` answers
            # from the pin, nothing below ever touches it again — no cwd-based override,
            # `house` least of all. Absent a pin, `project_of` (agents.py) carries the
            # SAME charter -> lineage_works_in fallback this function used to inline —
            # one implementation, not two copies drifting apart.
            if resolved_project is None:
                from src.orchestrator.agents import project_of
                resolved_project = await project_of(conn, agent, cwd=cwd or None)

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
