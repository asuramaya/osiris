"""Shared heartbeat/counts logic used by the statusline.

`scripts/osiris_statusline.py::_counts` used to own this logic outright; it is now
callable against either a fresh per-process connection (the script's own fallback path)
or the MCP server's own warm shared pool (the `/heartbeat` HTTP route), so there is one
ingress instead of every rendering tab forking a new `asyncpg.connect()`.

A measurement against an idle fleet of 16 agents showed 138 tx/s and 23 backends from
statusline traffic alone; at 1000 workers that would be roughly 20 backend connections
per second against a max_connections=100 database, which motivated consolidating onto a
shared pool. The `/succession` route already established the pattern of a hook POSTing
and the server doing the write on its own pool; this extends that pattern to the heavier
statusline read/bump path.

`conn` is deliberately typed `Any`, not `asyncpg.Connection`: every callee here
(`find_session_row`, `held_seat`, `seat_facts`, `surface.fetch`) already accepts either a
Pool or a Connection, since both expose the same fetch/fetchrow/fetchval surface. That
lets the same function serve a single warm connection (the script's own fallback, one
connection, one query budget) or a shared pool (the route, where each sub-query may land
on a different pooled connection) safely, since Postgres read-committed visibility does
not depend on connection identity once a write has committed."""
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
    # Live sessions holding seats managed_by this seat. 0 for a seat that manages nobody;
    # the UI then shows no fleet cell at all, since the bar is scoped to the agent's own
    # scope of responsibility.
    team: int = 0
    # ...over the seats it manages, rendered as "team 3/4" (an n/x count, informational only).
    team_of: int = 0
    # Unread mail that asks something of this reader: direct mail of any grade, plus room
    # broadcasts not graded fyi. See mailbox.unread_split.
    needs: int = 0
    # Open obligations owned by this seat/lineage/handle, and how many are past their
    # stale window. (owed_here, elsewhere, counts the operator's own debts.)
    owed_mine: int = 0
    stale_mine: int = 0
    # A third owner category: open obligations in a project this seat governs whose owner
    # is the bare project name itself or empty, which owed_mine's individual-spelling
    # match does not catch. Renders as the bar's `+M project` suffix, never folded into
    # owed_mine.
    owed_mine_project: int = 0


async def _team_live(conn: Any, seat_id: str, *, live_secs: int) -> tuple[int, int]:
    """(live, managed): seats managed_by `seat_id` that currently have a live body, over
    all active seats managed_by it. (0, 0) for a seat that manages nobody."""
    row = await conn.fetchrow(
        "WITH managed AS ("
        "  SELECT s.id, s.canonical FROM links mb JOIN objects s ON s.id = mb.from_id "
        "  JOIN objects mgr ON mgr.id = mb.to_id "
        "  WHERE mgr.canonical = $1 AND mb.type = 'managed_by' AND s.status = 'active' "
        "    AND (mb.valid_until IS NULL OR mb.valid_until > now())) "
        "SELECT (SELECT count(*) FROM managed) AS managed, "
        "       (SELECT count(DISTINCT managed.id) FROM managed "
        "        JOIN links h ON h.to_id = managed.id AND h.type = 'holds' "
        "          AND (h.valid_until IS NULL OR h.valid_until > now()) "
        "        JOIN objects a ON a.id = h.from_id "
        "        JOIN agent_mounts m ON m.agent_id = a.canonical "
        "          AND m.last_seen > now() - make_interval(secs => $2)) AS live",
        seat_id, float(live_secs))
    if row is None:
        return 0, 0
    return int(row["live"] or 0), int(row["managed"] or 0)


def _seat_owns_cwd(cwd: str, *, handle: str, anchor_cwd: str | None) -> bool:
    """Is `cwd` one of this seat's own mechanical pin copies (the office, the anchor_cwd
    courtesy copy, or the `~/code/<handle>` scratch-workspace convention), rather than a
    genuinely separate governed checkout? There are three pin copies and no single writer
    reaches all three, so this check has to cover each one.

    This checks containment, not exact match: `cwd` may be a subdirectory of any of these
    roots and still be answered by that root's own `.osiris` (read_project_label's own
    climb-to-repo-root behavior). The scratch-workspace root is best-effort, following the
    same convention sweep_seat_workspace's own default leans on (mintseat.py's
    `workspace = Path.home() / "code" / handle.lower()` when no custom `path=` was given at
    mint); a seat minted with an explicit custom path is not covered by this guess, a known
    gap that convention accepts.

    Pure and cheap: no filesystem I/O beyond what Path.resolve() needs, no DB query, since
    this runs on every statusline render."""
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
    """A near-verbatim extraction of the original combined counts function's body: see
    that history for the reasoning behind each step. Only the connection source and the
    succession call moved here; the resolution order below is unchanged.

    `cwd` used to feed a two-branch split that let a seat's own `house` field override an
    already-resolved pin at the seat's own mechanical pin copies (office/anchor/workspace),
    on the premise that nobody ever declares a value there, so a divergence from the graph
    must be a leftover from minting. That premise broke once seat creation stopped
    fabricating `project` from the handle: the office pin is now exactly where a seat's
    project gets deliberately declared, so overriding it with `house` reintroduced the same
    class of fabrication one step removed. The live bug this caused: a seat's pin correctly
    named its real project, but the statusline rendered the handle twice, because `house`
    (itself fabricated at mint, since a seat founded through the standard path gets
    `house=handle` unconditionally, never a real project) won over the correct pin.

    Resolution order now, plainly: (1) the pin, `project_hint`, however it resolved, wins
    outright the instant it resolves to anything; there is no cwd-based override, ever. (2)
    Absent a pin, the seat's own declared `charter`: if it names exactly one repo, that repo
    is the project; more than one is genuine ambiguity, not this function's call to break.
    (3) Absent both, the agent's own lineage `works_in` (`lineage_works_in`,
    merge-normalized through `_normalize_project_label_through_merge`), following the same
    abstain rule that lookup already enforces (only when the whole lineage agrees). `house`
    never stands in for `project` anywhere in this order: it answers a different question
    (which house a seat belongs to), and conflating the two was the bug this fix closes.
    The file-wins precedent used for `model` does not transfer here: the pin is a genuine
    external input for `model` (a deliberate model swap), while `project` is never
    externally supplied the same way, so there is no parallel input to protect."""
    from src.orchestrator.mounts import find_session_row

    agent = None
    if session_id:
        bare = model_id.split("[", 1)[0].strip()
        found = await find_session_row(conn, session_id)
        row0 = None
        if found is not None:
            # A statusline render is a read, not an earned act: this bump may only refresh
            # a pulse the row already earned (earned_pulse_at IS NOT NULL), never grant one
            # to a row that never has. The metadata fields (model/model_raw/
            # context_window_size) are not a liveness grant and still update
            # unconditionally.
            row0 = await conn.fetchrow(
                "UPDATE agent_mounts SET "
                "last_seen=CASE WHEN earned_pulse_at IS NOT NULL THEN now() ELSE last_seen END, "
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
    team, team_of = 0, 0
    owed_mine, stale_mine, owed_mine_project = 0, 0, 0
    if agent:
        from src.orchestrator.seats import held_seat, seat_facts
        from src.orchestrator.stophook_logic import owned_obligations

        mine = await owned_obligations(conn, agent)
        owed_mine, stale_mine = mine["owned"], mine["stale"]
        owed_mine_project = mine["project"]
        seat = await held_seat(conn, agent)
        if seat:
            resolved_seat_handle = seat.get("handle")
            anchor = None
            if seat.get("seat_id"):
                team, team_of = await _team_live(conn, seat["seat_id"],
                                                 live_secs=lease_secs)
                facts = await seat_facts(conn, seat["seat_id"])
                anchor = facts.get("anchor_cwd")
                if resolved_intent is None and anchor:
                    from src.orchestrator.agents import read_project_model
                    resolved_intent = read_project_model(anchor)
            # The pin wins outright (see this function's docstring for the full
            # resolution order and the bug that caught its absence): once
            # `resolved_project` answers from the pin, nothing below ever touches it
            # again, no cwd-based override, `house` least of all. Absent a pin,
            # `project_of` (agents.py) carries the same charter -> lineage_works_in
            # fallback this function used to inline, so there is one implementation
            # rather than two copies drifting apart.
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
        resolved_project, resolved_intent, resolved_seat_handle, team, team_of,
        int(seg.mail.data.get("needs", seg.mail.data["mail"] + seg.mail.data["dm"])),
        owed_mine, stale_mine, owed_mine_project)
