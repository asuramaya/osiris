"""THE STUB CULL (msg 1675, Thoth's dispatch) — a sanctioned verb to retire a dead
SoftwareProject, shaped after seats.py's retire_seat/vacate_holder: a single self-contained
function that gathers its own refusal evidence and, only once every check clears, performs
the write. Unlike vacate_dead_seat's seat-liveness check, every signal a project needs
(commits, open threads, a recent mount) is already a graph/table read — no external process
or transcript evidence to gather, so this needed no trigger.py-style split.

THE STATUS FLIP IS A COMPENSATING EVENT, not a bare property assertion: `Actions.set_status`
writes an append-only `object_events` row (event_type='status_change') alongside the
`objects.status` column flip, so a retirement is auditable and reversible (re-`set_status`
back to 'active') exactly like every other lifecycle transition in this codebase — never a
DELETE.

REFUSES LOUDLY on: a blank `because`; a `project` ref that doesn't resolve to a
SoftwareProject (scoped to that type ONLY — never resolves through to a Seat or Agent of the
same name, the exact ambiguity Thoth flagged for 'seshat'/'ra', which are stub PROJECTS
distinct from the seats of the same names); a project already non-active; any commit
recorded against it (`in_repo` from a Commit); any open Thread pointing in (`in_repo`,
status='active'); or a mount seen against it within the live window (15 minutes, the same
threshold `mounts.agent_liveness` already uses for a mind). The receipt always names the
project by its CANONICAL (`repo:<name>`), never a bare name, so it can never be misread as a
seat's receipt."""
from __future__ import annotations

import re
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg

from src.actions.core import Actions

_LIVE_MOUNT_WINDOW = timedelta(minutes=15)


async def _resolve_software_project(pool: asyncpg.Pool, ref: str) -> asyncpg.Record | None:
    """A SoftwareProject ONLY — a full UUID, an 8-char short id, an exact canonical
    (`repo:<name>` accepted with or without the prefix), or its `name` property. Never
    widens to another object type: this is the structural half of the seshat/ra
    disambiguation, not just wording in the receipt."""
    ref = ref.strip()
    try:
        oid = uuid.UUID(ref)
    except ValueError:
        oid = None
    if oid is not None:
        row = await pool.fetchrow(
            "SELECT id, canonical, status FROM objects WHERE id=$1 AND type='SoftwareProject'",
            oid)
        if row is not None:
            return row
    short = ref.lower()
    if re.fullmatch(r"[0-9a-f]{8}[0-9a-f-]*", short):
        row = await pool.fetchrow(
            "SELECT id, canonical, status FROM objects "
            "WHERE type='SoftwareProject' AND id::text LIKE $1 || '%' LIMIT 1", short)
        if row is not None:
            return row
    canon = ref if ref.startswith("repo:") else f"repo:{ref}"
    row = await pool.fetchrow(
        "SELECT id, canonical, status FROM objects WHERE type='SoftwareProject' AND "
        "canonical=$1", canon)
    if row is not None:
        return row
    return await pool.fetchrow(
        "SELECT o.id, o.canonical, o.status FROM objects o "
        "JOIN current_assertions a ON a.object_id=o.id "
        "WHERE o.type='SoftwareProject' AND a.name='name' AND a.value #>> '{}' = $1 LIMIT 1",
        ref)


async def retire_project(
    actions: Actions, *, project: str, actor: str, because: str,
) -> dict[str, Any]:
    """Retire a dead SoftwareProject stub — status flip to 'retired' via a compensating
    event (`Actions.set_status`). Refuses on: blank `because`; an unresolved or non-active
    project; any commit, any open thread pointing in, or any mount seen within the last 15
    minutes against it (live signal — this verb never evicts a project that's actually in
    use)."""
    because = (because or "").strip()
    if not because:
        return {"error": "because is required — retiring a project is a deliberate act on "
                         "the record"}
    project = (project or "").strip()
    if not project:
        return {"error": "project is required"}
    row = await _resolve_software_project(actions.pool, project)
    if row is None:
        return {"error": f"no such SoftwareProject: {project!r}"}
    pid, canonical, status = row["id"], row["canonical"], row["status"]
    if status != "active":
        return {"error": f"{canonical} is already {status} — nothing to retire"}
    commits = await actions.pool.fetchval(
        "SELECT count(*) FROM links l JOIN objects c ON c.id=l.from_id "
        "WHERE l.to_id=$1 AND l.type='in_repo' AND c.type='Commit'", pid)
    if commits:
        return {"error": f"{canonical} has {commits} commit(s) — live signal, "
                         "retire_project refuses"}
    open_threads = await actions.pool.fetchval(
        "SELECT count(*) FROM links l JOIN objects t ON t.id=l.from_id "
        "WHERE l.to_id=$1 AND l.type='in_repo' AND t.type='Thread' AND t.status='active'", pid)
    if open_threads:
        return {"error": f"{canonical} has {open_threads} open thread(s) pointing in — "
                         "live signal, retire_project refuses"}
    bare_name = canonical.removeprefix("repo:")
    mount_seen = await actions.pool.fetchval(
        "SELECT max(last_seen) FROM agent_mounts WHERE project=$1", bare_name)
    if mount_seen is not None and (datetime.now(UTC) - mount_seen) < _LIVE_MOUNT_WINDOW:
        return {"error": f"{canonical} has a live mount (seen {mount_seen.isoformat()}) — "
                         "live signal, retire_project refuses"}
    await actions.set_status(pid, "retired", because, actor)
    return {"retired_project": canonical, "id": str(pid)[:8], "because": because}
