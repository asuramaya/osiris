"""The durable half of the mount registry: identity that survives a server restart.

The MCP server's in-memory registry dies with the process, and the process dies routinely
(deploy restarts, an out-of-memory kill): every restart wiped the whole fleet's mounts at
once, and each agent rediscovered it by a hard "mount(cwd) first" failure mid-work. This
table is the memory the in-memory dict doesn't have: mount() upserts here, and any later
call that misses the dict can re-attach by the client's job_dir (presented per-request via
the X-Osiris-Job header) instead of failing until the agent notices.

Keyed by job_dir: the one durable handle the client re-presents. A mount without a job_dir
has nothing to re-attach by, so it stays memory-only (exactly the old behavior, degraded
gracefully). session_key is informational (which MCP session last touched the mount), never
a lookup key: a reconnecting client gets a fresh session id, so it can't be one.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
import time
import tomllib
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

_EC = EvidenceClass.SELF_DECLARED.value
_CONF = confidence_for(EvidenceClass.SELF_DECLARED)

# THE GREETING LEDGER (a resume race observed in 2026-07): a window resume fires the
# predecessor's SessionEnd and the successor's SessionStart concurrently, and when the
# end lands second (observed live: automount 20:03:03, session-end 20:03:04) it deleted
# the mount row the greeting had just registered. The seat then answered every probe
# {live:false, last_seen:NULL} until the next human act, and the poke lane read the dead
# registry and skipped the window while a build order sat unread. Both handlers live in
# one process, so the discriminator is this module-level stamp: automount notes each
# greeting; session-end yields when the same sid was greeted within the grace period.
# Yielding is the safe side of the asymmetry: a wrongly-kept row is reaped by the census
# sweep within about 2 minutes; a wrongly-killed row blinds probes and pokes until a
# human types into the window.
_GREETS: dict[str, float] = {}
_GREET_GRACE_SECS = 10.0


def note_greeting(session_id: str) -> None:
    """Stamp a session's greeting (automount's server half) for the resume-race yield."""
    sid8 = (session_id or "").strip().lower()[:8]
    if len(sid8) < 8:
        return
    now = time.monotonic()
    _GREETS[sid8] = now
    if len(_GREETS) > 256:  # bounded: anything past the grace period is dead weight
        for k, t in list(_GREETS.items()):
            if now - t > _GREET_GRACE_SECS:
                _GREETS.pop(k, None)


def greeted_within_grace(session_id: str, *, grace: float = _GREET_GRACE_SECS) -> bool:
    """Did a greeting for this sid land within the grace period? Session-end's yield check."""
    sid8 = (session_id or "").strip().lower()[:8]
    t = _GREETS.get(sid8)
    return t is not None and (time.monotonic() - t) < grace


@dataclass(frozen=True)
class MountRecord:
    """What re-attachment needs: the arguments to re-run identity resolution with."""

    job_dir: str
    agent_id: str
    project: str | None
    cwd: str
    model: str | None


async def save_mount(
    pool: asyncpg.Pool, *, job_dir: str, agent_id: str, project: str | None, cwd: str,
    model: str | None, session_key: str | None, alive: bool = True,
) -> datetime | None:
    """Upsert the durable mount row. Called at mount() and again at every re-attach (bumping
    last_seen: the fleet's liveness signal for the listener probe). Returns the PREVIOUS
    last_seen (None on first mount): the anchor for the while-you-were-away fold, everything
    that happened in this lineage's name between its last sign of life and this re-entry.

    `alive=False` registers a mount without a liveness pulse: a provisional mount, needed
    because of a ghost-process problem. Claude Code fires SessionStart for processes that are
    not real agent sessions: background pre-warm processes, pty hosts, claim-socket daemons.
    Each has a real session id and a real cwd, so the startup handshake registers it, and
    `last_seen=now()` handed it a heartbeat, which made it read as live by every test the
    fleet has. It inflated the roster, it made the co-agent warning cry wolf on an
    uncontended tree, and it could take delivery of a message into a process that will never
    read anything.

        A HEARTBEAT MUST BE EARNED BY AN ACT, NEVER GRANTED BY A GREETING.

    So the startup handshake registers the caller (its identity is ready the moment it
    exists) but does not certify it as living. A real session proves itself within seconds:
    its first Osiris call bumps this row, or its transcript grows and observe_liveness stamps
    it. A spare process never does either, and lies there with a null pulse, costing nothing
    and fooling no one.

    EARNED PULSE: this `alive=True` path is one of exactly two writers ever allowed to stamp
    `earned_pulse_at` (the other is liveness.py's `observe_liveness`, a real transcript
    growing): first-earn only (`COALESCE(agent_mounts.earned_pulse_at, now())` on conflict),
    never re-stamped by a later touch, so the column answers "did this row ever earn a
    pulse", not "when was it last seen". Every other writer that bumps `last_seen` must read
    this column first and refuse to grant a pulse a row never earned: this is the one column
    that says whether they may.
    """
    return await pool.fetchval(  # type: ignore[no-any-return]
        "WITH old AS (SELECT last_seen FROM agent_mounts WHERE job_dir=$1) "
        "INSERT INTO agent_mounts (job_dir, agent_id, project, cwd, model, session_key, "
        "                          last_seen, earned_pulse_at) "
        "VALUES ($1,$2,$3,$4,$5,$6, CASE WHEN $7 THEN now() END, "
        "                          CASE WHEN $7 THEN now() END) "
        "ON CONFLICT (job_dir) DO UPDATE SET agent_id=$2, project=$3, cwd=$4, model=$5, "
        "session_key=$6, "
        # A greeting must never revoke a pulse either: a re-registered session that is
        # already proven alive keeps what it earned.
        "last_seen=CASE WHEN $7 THEN now() ELSE agent_mounts.last_seen END, "
        "earned_pulse_at=CASE WHEN $7 "
        "  THEN COALESCE(agent_mounts.earned_pulse_at, now()) "
        "  ELSE agent_mounts.earned_pulse_at END "
        "RETURNING (SELECT last_seen FROM old)",
        job_dir, agent_id, project, cwd, model, session_key, alive,
    )


async def release_mounts(pool: asyncpg.Pool, agent_id: str) -> int:
    """Close every durable mount row naming `agent_id`: retire()'s seat release (an earlier
    incident had an agent hold a live seat after its farewell, still appearing in the fleet
    display and liveness counts). The registry is hot state, not the event-sourced kernel:
    the retirement itself is stamped on the Agent object, so dropping the seat loses no
    record. Exact-id only: a successor re-mounted on the same job_dir has already overwritten
    the row with its own agent_id and is never touched. Returns rows released."""
    tag = await pool.execute("DELETE FROM agent_mounts WHERE agent_id=$1", agent_id)
    return int(tag.rsplit(" ", 1)[-1])


# The "suspended, no genuine sighting" sentinel: a real datetime, not a Postgres literal
# string, because asyncpg binds params by Python type before any server-side cast runs, so
# a bare 'epoch' string fails to bind against a timestamptz column.
SUSPENDED_AT = datetime.fromtimestamp(0, tz=UTC)


async def release_session_mounts(
    pool: asyncpg.Pool, *, job_dir: str, session_id: str,
) -> int:
    """THE SESSION-SCOPED RELEASE (a false-succession incident): SessionEnd releases the
    ending session's rows (its own anchor row plus any row its binding rode elsewhere, since
    a resume anchors at its ancestor's job_dir, marked session_key='sid:<its own id>') and
    never the whole seat. release_mounts(agent_id) once let one closing tab-view delete a
    living session's anchor row; the wrongly emptied registry then read as the seat's death,
    and downstream logic, correct on its own evidence, minted a false successor at the
    original seat's office (a title-generator stub nearly took over). A row is an address:
    only the addressed row's death may release it. Seat-wide release stays retire()'s own: an
    agent's deliberate farewell.

    NEVER A DELETE: this used to delete the row, correct for liveness (the row must stop
    answering any probe immediately, same effect as before) but wrong for the registry,
    because SessionEnd firing does not always mean the process is actually gone (the exact
    resume-race class this function's own docstring already guards elsewhere:
    `greeted_within_grace` catches the ordering race, this catches the survival case, a
    daemon re-adopt or a process the harness still lists). Suspends instead: `last_seen`
    flips to the epoch sentinel (heal with compensating events, never delete), which reads
    exactly as dead to `is_live()` (same immediate liveness effect the delete always had)
    while the row itself survives, findable by `find_mount`. A genuine re-adopt or resume
    then promotes it back the ordinary way: `save_mount`'s own `ON CONFLICT (job_dir) DO
    UPDATE` refreshes `last_seen=now()` on the same row, no different from any other
    re-mount. Idempotent: a row already suspended is excluded from the result (re-suspending
    nothing is not a release).

    EARNED PULSE: clears `earned_pulse_at` alongside `last_seen`: a suspended address's
    earned pulse dies with it, same as the address itself; a future occupant of the same
    job_dir/session_key re-earns its own pulse via a genuine act rather than silently
    inheriting whatever the address earned before it went quiet."""
    sid32 = (session_id or "").replace("-", "").strip().lower()
    n = await pool.fetchval(
        "WITH gone AS (UPDATE agent_mounts SET last_seen=$3, earned_pulse_at=NULL "
        "WHERE (job_dir=$1 OR session_key=$2) AND last_seen IS DISTINCT FROM $3 "
        "RETURNING 1) SELECT count(*) FROM gone",
        job_dir, f"sid:{sid32}", SUSPENDED_AT)
    return int(n or 0)


async def suspend_mounts_for_agents(pool: asyncpg.Pool, agent_ids: list[str]) -> int:
    """THE KILL PATH'S OWN AGENT-SCOPED SUSPEND: `release_session_mounts`'s sibling, keyed
    by exact `agent_id` membership rather than `job_dir`/`session_key`, for a caller
    (`stop_seat`) that already knows which agents to release but not, in advance, which row
    each one mounted at. A `--fork-session` child (`agent_type='fork'`, spawned_by-linked, no
    seat of its own) mounts its own row under its own agent_id at its own job_dir:
    stop_seat's own release step never reached it (neither the seat's stable anchor nor any
    succession_chain generation's own derived job_dir matches a fork's row), so a live fork
    process under a stopped seat stayed falsely "live" indefinitely. Same rule as
    release_session_mounts: suspend, never delete: `last_seen` flips to the epoch sentinel,
    the row itself survives. Idempotent: a row already suspended is excluded from the result.
    Empty `agent_ids` is a no-op, never a wildcard match."""
    if not agent_ids:
        return 0
    n = await pool.fetchval(
        "WITH gone AS (UPDATE agent_mounts SET last_seen=$2 "
        "WHERE agent_id = ANY($1::text[]) AND last_seen IS DISTINCT FROM $2 "
        "RETURNING 1) SELECT count(*) FROM gone",
        agent_ids, SUSPENDED_AT)
    return int(n or 0)


AgentsJsonFn = Callable[[], Awaitable[list[dict[str, Any]]]]
ProcReadFn = Callable[[int], str | None]


async def registry_census(
    pool: asyncpg.Pool, *, agents_json: AgentsJsonFn | None = None,
    read_exe: ProcReadFn | None = None, read_cwd: ProcReadFn | None = None,
) -> dict[str, Any]:
    """THE REGISTRY+/PROC CENSUS: the harness's own live-process list (`claude agents
    --json`, trigger.py's `_claude_agents_json`, reused verbatim rather than reinvented),
    each row verified against `/proc` (census.py's `_proc_exe`/`_proc_cwd`, a harness row is
    trusted only once `/proc` confirms the pid is really a claude process): this is what "is
    a process live right now" answers going forward. `agent_mounts` is the cache this
    reconciles against, never a second source of truth: `matched` names rows the census
    confirms are real; `rowless` names live, verified processes with no agent_mounts row at
    all (session_id prefix matches nothing), the exact population this reconciliation exists
    to close to zero.

    OCCUPANCY, NOT IDENTITY: the boundary this function must never cross. This answers "is a
    process running, and what does the harness/OS say about it"; it never resolves which
    agent lineage holds a seat, never touches `holds` links or `claim_name`'s own
    arbitration. A caller wanting identity reads the graph (seats.py/agents.py); a caller
    wanting occupancy reads this. Conflating the two is exactly the class of bug the
    duplicate-authority ruling and this codebase's own "never let one answer for the other"
    rule both guard against.

    Injectable (`agents_json`/`read_exe`/`read_cwd`) so tests drive this with fakes, same
    test-injection discipline as census.py's own pgrep/proc functions: the real defaults (harness
    subprocess + real /proc) are imported lazily to avoid a module-load-time cycle between
    mounts.py (imported early, by agents.py among others) and trigger.py/census.py
    (themselves importing agents.py). Fails open on a harness read failure: a census that
    could not run reports `verified: []`, `blind: true`, never a false-empty population read
    as "nothing is live" (same rule as census.py's own pgrep=None handling).

    PULSE-LIVE, A DISTINCT POPULATION: the Claude-only harness registry above can never
    confirm a non-Claude process by construction. `pulse_live` names the agents a non-Claude
    harness's own self-reported freshness (`pulse_mount`, below) currently backs, kept
    separate from `matched` on purpose (a census-confirmed process and a self-reported-fresh
    one are different grades of evidence; conflating them into one list would erase exactly
    the visibility this guard exists for). Scoped to agents whose own stamped `harness`
    property (mount()'s own `assert_property(..., "harness", ...)`) reads anything but
    'claude-code': a Claude session's liveness is answered by the census above alone,
    unchanged. A 5-minute freshness window, stricter than the 15-minute mount-staleness
    window the census path rides on elsewhere (resolve_seat), since a self-report is weaker
    evidence than a verified census match and earns a tighter leash."""
    from src.orchestrator import census as _census
    from src.orchestrator.trigger import _claude_agents_json

    agents_json = agents_json or _claude_agents_json
    read_exe = read_exe or _census._proc_exe
    read_cwd = read_cwd or _census._proc_cwd
    pulse_rows = await pool.fetch(
        "SELECT o.canonical AS agent_id, m.job_dir, m.last_seen FROM agent_mounts m "
        "JOIN objects o ON o.type='Agent' AND o.canonical=m.agent_id AND o.status='active' "
        "WHERE COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='harness' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1), '') "
        "  NOT IN ('', 'claude-code') "
        "AND m.last_seen > now() - interval '5 minutes'")
    pulse_live = [{"agent_id": r["agent_id"], "job_dir": r["job_dir"],
                  "last_seen": r["last_seen"].isoformat()} for r in pulse_rows]
    try:
        rows = await agents_json()
    except (OSError, TimeoutError, ValueError):
        return {"blind": True, "verified": [], "matched": [], "rowless": [],
                "pulse_live": pulse_live, "pulse_live_count": len(pulse_live),
                "note": "the harness registry read failed, cannot census, not empty"}
    verified: list[dict[str, Any]] = []
    for r in rows:
        sid = str(r.get("sessionId") or "")
        if not sid:
            continue
        pid = r.get("pid")
        exe = read_exe(int(pid)) if isinstance(pid, int) else None
        if not _census._is_claude_body(exe):
            continue  # the harness claims a process; /proc does not confirm it, not counted
        verified.append({
            "session_id": sid, "job_dir_key": sid[:8], "pid": pid,
            "harness_cwd": r.get("cwd"), "harness_name": r.get("name"),
            "proc_cwd": read_cwd(int(pid)) if isinstance(pid, int) else None,
        })
    db_rows = await pool.fetch("SELECT job_dir, agent_id, project, last_seen FROM agent_mounts")
    by_key: dict[str, list[asyncpg.Record]] = defaultdict(list)
    for row in db_rows:
        by_key[Path(row["job_dir"]).name].append(row)
    matched: list[dict[str, Any]] = []
    rowless: list[dict[str, Any]] = []
    for v in verified:
        candidates = by_key.get(v["job_dir_key"], [])
        if candidates:
            matched.append({**v, "agent_id": candidates[0]["agent_id"],
                            "project": candidates[0]["project"],
                            "job_dir": candidates[0]["job_dir"]})
        else:
            rowless.append(v)
    return {"blind": False, "verified": verified, "matched": matched, "rowless": rowless,
            "verified_count": len(verified), "matched_count": len(matched),
            "rowless_count": len(rowless), "pulse_live": pulse_live,
            "pulse_live_count": len(pulse_live)}


async def apply_boot_time_fleet_pass(
    actions: Actions, *, census_fn: Any = None, actor: str = "boot-fleet-pass",
) -> dict[str, Any]:
    """REBOOT SURVIVAL, the fleet half: a live specimen from a real reboot showed the
    harness daemon resumed every seat's process within five minutes, but the osiris side did
    nothing to notice. `agent_mounts.last_seen` for a resumed process stayed frozen at its
    pre-reboot value until that process's own next MCP call: every liveness reader
    (agent_liveness, seat_occupancy, fleet()) read every resumed seat as cold for however
    long it took that process to next speak, even though the harness itself already
    confirmed the process alive again.

    Runs `registry_census` (the same harness+/proc cross-check `fleet_prune`'s own
    `_unclaimed_bodies` already trusts) and, for every `matched` row (a live process the
    census already ties to an existing agent_mounts row), refreshes that row's `last_seen` to
    now() and stamps a durable `boot_resumed_at` property on the Agent: evidence this
    specific generation was independently confirmed live at this moment, never overwritten,
    so a later investigation can see exactly when a resumed process was first noticed rather
    than inferring it from a mount row's own bumped timestamp alone.

    `rowless` processes (a verified live process with no agent_mounts row at all) are named
    here, never bound: `fleet_prune`'s own `unclaimed_body` bucket already resolves and binds
    these via `tree_seat_hint` on its own 15-minute cadence; duplicating that resolve-then-
    bind logic here would be a second copy of the same mechanism for a population the
    existing sweep already reaches within 15 minutes regardless. A blind census (the harness
    registry read itself failed) reports nothing rather than guessing, the same "could not
    look" rule every reader of this census already holds to.

    Called both at worker startup (arq_worker.py's own `startup()`, once per process boot,
    the automatic fleet-wide side) and on demand via `osiris boot-status --fleet` (an
    operator wanting to check right now, without waiting for the next worker restart): the
    same function, never two copies."""
    census_fn = census_fn or registry_census
    census = await census_fn(actions.pool)
    if census.get("blind"):
        return {"blind": True, "refreshed": [], "rowless": [],
                "note": "the harness registry read failed, cannot census, not empty"}
    stamped_at = datetime.now(UTC)
    refreshed: list[dict[str, Any]] = []
    for m in census["matched"]:
        agent_id, job_dir = m["agent_id"], m["job_dir"]
        await actions.pool.execute(
            "UPDATE agent_mounts SET last_seen=now() WHERE job_dir=$1", job_dir)
        agent_obj = await actions.create_or_find_object("Agent", agent_id, actor)
        await actions.assert_property(
            agent_obj, "boot_resumed_at", stamped_at.isoformat(), actor, stamped_at,
            _CONF, evidence_class=_EC)
        refreshed.append({"agent_id": agent_id, "job_dir": job_dir, "pid": m.get("pid")})
    rowless = [
        {"session_id": r["session_id"], "pid": r.get("pid"),
         "cwd": r.get("proc_cwd") or r.get("harness_cwd")}
        for r in census["rowless"]
    ]
    return {
        "blind": False, "refreshed": refreshed, "rowless": rowless,
        "refreshed_count": len(refreshed), "rowless_count": len(rowless),
        "note": "REBOOT SURVIVAL, the fleet half: every registry_census-verified process "
                "already tied to a mount row had its last_seen refreshed and a "
                "boot_resumed_at property stamped; a process with no mount row at all is "
                "named here, not bound (fleet_prune's own unclaimed_body bucket does "
                "that, on its own cadence).",
    }


async def pulse_mount(pool: asyncpg.Pool, *, agent_id: str) -> dict[str, Any]:
    """THE HARNESS-NEUTRAL PULSE: a lightweight, self-scoped liveness refresh any MCP
    client can call directly, no startup hook or statusline required. Touches only
    `agent_id`'s own `agent_mounts` rows: there is no `target` parameter, by construction, so
    a caller can never refresh another agent's row; the caller's own resolved identity is the
    entire address space. A well-behaved non-Claude client that simply calls mount()
    periodically already gets this for free (`save_mount`'s own upsert already bumps
    `last_seen`); this exists for the cheaper, more frequent case, a client that wants to
    stay fresh without paying mount()'s full re-attach cost every time.

    Feeds `registry_census`'s own `pulse_live` population above, the only thing that changes
    for a non-Claude harness's own liveness reading (`is_occupied_by_a_live_body`); the
    Claude census path is entirely untouched. Zero rows touched (an agent_id with no durable
    mount row at all) is a legal, reportable no-op, never an error."""
    rows = await pool.fetch(
        "UPDATE agent_mounts SET last_seen=now() WHERE agent_id=$1 "
        "RETURNING job_dir, last_seen", agent_id)
    newest = max((r["last_seen"] for r in rows), default=None)
    return {"agent": agent_id, "touched": len(rows),
            "last_seen": newest.isoformat() if newest else None}


_MOUNT_COLS = (
    "job_dir", "agent_id", "project", "cwd", "model", "model_raw", "session_key",
    "mounted_at", "last_seen", "context_window_size", "seat_id",
)


def _mount_snapshot(row: asyncpg.Record) -> dict[str, Any]:
    """An `agent_mounts` row -> a JSON-safe snapshot. `audit_log.payload` is jsonb and the
    pool's codec is plain `json.dumps` with no datetime default (src/db/pool.py): the two
    timestamptz columns must be stringified before they ever reach it, or the INSERT below
    raises. The inverse of `undrop_dead_project_mount`'s own restore-from-snapshot read."""
    out: dict[str, Any] = {}
    for k in _MOUNT_COLS:
        v = row[k]
        out[k] = v.isoformat() if isinstance(v, datetime) else v
    return out


async def drop_dead_project_mount(
    actions: Actions, *, job_dir: str, project: str, actor: str,
) -> dict[str, Any]:
    """Release ONE mount row that is residue against an already-retired project
    (fleet_reconcile.py's drop_ephemeral_test_cwd bucket). Row-scoped by `job_dir`, the
    durable per-row key (`agent_mounts.job_dir` is the table's own ON CONFLICT target),
    never agent-id-wide: `release_mounts`' own lesson from a past false-succession incident,
    where an agent-id-wide DELETE killed a live sibling session's row. The same rule
    `release_session_mounts` already keeps: a row is an address, and only the addressed
    row's own death may release it.

    REVERSIBLE AND AUDITED: this was once a bare `pool.execute`, no audit_log, no
    object_events; not merely irreversible but unwitnessed, worse than "no undo" because
    after it ran there was nothing to reconstruct from, by anyone. The row is snapshotted
    whole into an `audit_log` row (action='drop_dead_project_mount') before the delete, in
    the same transaction. `audit_log`, not `object_events`, deliberately: `object_events.
    object_id` is NOT NULL, and attaching the record to a freshly-minted Agent object would
    violate an existing, deliberate invariant this same reaper's own test enforces
    (test_fleet_reconcile.py: "a drop releases the residue row only, no Agent object was
    ever minted here"): a drop must promote nothing, including its own record. Pass the
    returned `audit_id` to `undrop_dead_project_mount` to replay the row back exactly.

    Re-checks `project` at delete time rather than trusting the caller's earlier read: a
    row whose project changed between a sweep's report and this call (re-mounted into a
    live project in the interim) no longer matches and is left untouched, and nothing is
    written, audit row included: a record for a delete that didn't happen would itself be
    a false record. Returns {"dropped": 0|1, "audit_id": int|None} (job_dir is unique, so
    at most one row can ever match)."""
    async with actions.pool.acquire() as conn, conn.transaction():
        row = await conn.fetchrow(
            f"SELECT {', '.join(_MOUNT_COLS)} FROM agent_mounts "
            "WHERE job_dir=$1 AND project=$2 FOR UPDATE", job_dir, project)
        if row is None:
            return {"dropped": 0, "audit_id": None}
        snapshot = _mount_snapshot(row)
        audit_id = await conn.fetchval(
            "INSERT INTO audit_log (action, actor, payload) VALUES ($1,$2,$3) "
            "RETURNING id", "drop_dead_project_mount", actor, snapshot)
        await conn.execute(
            "DELETE FROM agent_mounts WHERE job_dir=$1 AND project=$2", job_dir, project)
        return {"dropped": 1, "audit_id": audit_id}


_MOUNT_DROP_ACTIONS = frozenset({
    "drop_dead_project_mount", "sweep_ghost_doors", "sweep_stale_doors",
    "drop_dead_transcript_mount", "retire_seatless_mount_claim",
})  # every action that snapshots a row via `_mount_snapshot` before deleting it: same
    # payload shape regardless of which one wrote it, so one inverse undoes all of them
    # (do not invent a second shape for the same problem); also the exact set
    # `rescue_seat_holder_mount` walks: `retire_seatless_mount_claim` is deliberately
    # included so that walk sees past its own compensating entry to an older one naming
    # the real seat holder, rather than stopping cold on a drop that (by construction)
    # never names one


async def drop_dead_transcript_mount(
    actions: Actions, *, job_dir: str, actor: str,
) -> dict[str, Any]:
    """Release ONE mount row whose own anchor directory (`job_dir`) no longer exists on
    disk (a "dead transcript" class of cleanup): the same reversible, audited, row-scoped
    shape `drop_dead_project_mount` already proves, keyed on `job_dir` alone rather than
    (job_dir, project) since a gone directory has no project to re-check. Re-checks
    existence at delete time under the row lock, same discipline as `drop_dead_project_
    mount`'s own re-check of `project`: a directory recreated between a sweep's report and
    this call (a rare but real race: a job_dir reused, or a slow NFS mount) is left
    untouched, and nothing is written, audit row included."""
    async with actions.pool.acquire() as conn, conn.transaction():
        row = await conn.fetchrow(
            f"SELECT {', '.join(_MOUNT_COLS)} FROM agent_mounts "
            "WHERE job_dir=$1 FOR UPDATE", job_dir)
        if row is None or await asyncio.to_thread(Path(job_dir).exists):
            return {"dropped": 0, "audit_id": None}
        snapshot = _mount_snapshot(row)
        audit_id = await conn.fetchval(
            "INSERT INTO audit_log (action, actor, payload) VALUES ($1,$2,$3) "
            "RETURNING id", "drop_dead_transcript_mount", actor, snapshot)
        await conn.execute("DELETE FROM agent_mounts WHERE job_dir=$1", job_dir)
        return {"dropped": 1, "audit_id": audit_id}


async def undrop_dead_project_mount(
    actions: Actions, *, audit_id: int, actor: str,
) -> dict[str, Any]:
    """The compensating inverse, later widened to cover the row sweeps too: replays the
    exact row a `_MOUNT_DROP_ACTIONS` audit_log row recorded, back into `agent_mounts`
    unchanged. `audit_id`, not `job_dir`, pins exactly which drop to undo: the same job_dir
    can be dropped, re-mounted, and dropped again, and a replay must never reach for the
    wrong generation of that history.

    Refuses loudly (an error dict, nothing written) when: no such audit_log row, or its
    `action` isn't one of `_MOUNT_DROP_ACTIONS` (an undrop never invents a snapshot from an
    unrelated audit entry); the job_dir is already occupied, a live session re-mounted there
    since the drop, so the newer row is the truth and reviving the old one underneath it
    would fork identity, never overwrite.

    PROOF, NOT ASSERTION: the restored row round-trips through the same `_mount_snapshot`
    the drop used, so `undrop(...); read; _mount_snapshot(...) == original_snapshot` is a
    literal equality a caller (or test) can check, not an eyeballed match."""
    row = await actions.pool.fetchrow(
        "SELECT action, payload FROM audit_log WHERE id=$1", audit_id)
    if row is None:
        return {"error": f"no audit_log row #{audit_id}, an undrop never invents one"}
    if row["action"] not in _MOUNT_DROP_ACTIONS:
        return {"error": f"audit_log #{audit_id} is a {row['action']!r} record, not one of "
                         f"{sorted(_MOUNT_DROP_ACTIONS)}, nothing to undrop"}
    snap = row["payload"]
    async with actions.pool.acquire() as conn, conn.transaction():
        occupied = await conn.fetchval(
            "SELECT 1 FROM agent_mounts WHERE job_dir=$1", snap["job_dir"])
        if occupied:
            return {"error": f"{snap['job_dir']} is already occupied by a live mount, "
                             "undrop refuses to overwrite it; the newer row is the truth"}
        cols = ", ".join(_MOUNT_COLS)
        placeholders = ", ".join(f"${i + 1}" for i in range(len(_MOUNT_COLS)))
        values = [
            datetime.fromisoformat(snap[k])
            if k in ("mounted_at", "last_seen") and snap[k] else snap[k]
            for k in _MOUNT_COLS
        ]
        await conn.execute(
            f"INSERT INTO agent_mounts ({cols}) VALUES ({placeholders})", *values)
        undrop_audit_id = await conn.fetchval(
            "INSERT INTO audit_log (action, actor, payload) VALUES ($1,$2,$3) "
            "RETURNING id", "undrop_dead_project_mount", actor,
            {"job_dir": snap["job_dir"], "restored_from_audit_id": audit_id})
    return {"restored": 1, "job_dir": snap["job_dir"], "undrop_audit_id": undrop_audit_id}


async def find_session_row(
    db: Any, session_id: str,
) -> Any | None:
    """THE ONE LOOKUP from a harness session id to its mount row: every per-window surface
    (statusline, stop hook, live_succession) resolves a window's identity through this,
    never its own copy (a copy is a fork that forgets it is one). Returns the row (job_dir,
    agent_id, project, model, context_window_size, mounted_at) or None.

    Three lanes, strongest evidence first:
      1. THE ANCHOR NAMED FOR THE SID: the startup handshake derives ~/.claude/jobs/<sid8>
         for every session it registers, so this covers first lives, resumes, and forks
         alike. Guarded: an 8-char prefix collides across real sessions often enough that
         this rung refuses a suspended row (SUSPENDED_AT, release_session_mounts' own
         sentinel) or one naming a retired Agent: compute_heartbeat trusts whatever this
         returns enough to update it unconditionally, so a dead/retired match here would
         resurrect a corpse's last_seen under a live session's own heartbeat.
      2. THE SESSION LEDGER: anchor_sid:<sid8> assertions (handshake.record_session_anchor
         files sid to agent at every registration for identities the sid alone could not
         re-derive); the owner's lineage's freshest row answers for a window whose durable
         anchor wears another name (a mount(job_dir=<inherited anchor>) session).
      3. SELF-EVIDENT DERIVATION: record_session_anchor deliberately never writes an
         anchor_sid entry when the sid already is the agent's own generation-derived
         identity ("the ledger holds only what a wiped registry could not reconstruct"),
         but that optimization assumed a reader could re-derive it directly, and until this
         lane existed, none could: lane 1 needs job_dir to itself carry the sid (false for a
         `-p --resume` wake, whose job_dir is a fresh per-wake anchor unrelated to the
         resumed transcript's own sid), and lane 2 finds nothing because nothing was ever
         written to find. Verified live in one incident: an agent's mount row lived under
         job_dir=jobs/c8a22a05 (the wake's own job anchor) while its transcript file (and
         its own identity) was a different uuid entirely: both prior lanes missed it, and
         a fleet status read it cold while it was actively working. This lane performs the
         same re-derivation record_session_anchor's skip already assumed was possible,
         closing the loop without any new write.
    DEAD END, recorded so nobody re-walks it: agent_mounts.session_key's 'sid:<hex>' is the
    MCP connection id (Mcp-Session-Id), never the harness sid: it cannot serve this lookup.
    `db` is any fetchrow-capable handle (pool or connection), so the hook scripts can pass
    their own single connection."""
    sid = (session_id or "").strip().lower()
    if len(sid) < 8:
        return None
    cols = "job_dir, agent_id, project, model, context_window_size, mounted_at"
    # DSH session ids come in two shapes: depth-0 ids carry a `session-` prefix, spawned
    # subagent ids are a bare uuid. Either way the mount row keys on the full session dir
    # path (job_dir ends with the id), and the sid8 anchor the ledger and self-evident
    # lanes speak is the uuid's first 8, never `session-` (which would collide across
    # every DSH session on the box). Normalized once, here, so every lane below speaks
    # the same grammar.
    # _UUID_RE is reused, never re-declared: sessions.py:301 and handshake.py:185 already
    # each carry a copy, and a third would be a duplicate implementation this codebase
    # keeps paying for. Function-local because handshake/sessions both import mounts;
    # module-level would be circular; sessions' own import of mounts is function-local too.
    from src.ingest.sessions import _UUID_RE
    dsh_uuid = None
    if sid.startswith("session-") and len(sid) == len("session-") + 36:
        dsh_uuid = sid[len("session-"):]
    elif len(sid) == 36 and _UUID_RE.match(sid) is not None:
        dsh_uuid = sid
    if dsh_uuid is not None:
        row = await db.fetchrow(
            f"SELECT {cols} FROM agent_mounts "
            "WHERE job_dir LIKE '%/' || $1 OR job_dir LIKE '%/session-' || $1 "
            "ORDER BY last_seen DESC NULLS LAST LIMIT 1", dsh_uuid)
        if row is not None:
            return row
    sid8 = dsh_uuid[:8] if dsh_uuid else sid[:8]
    # LIVENESS + RETIREMENT: an 8-char prefix is not a strong key: a genuinely dead row
    # (SUSPENDED_AT, this module's own release_session_mounts sentinel) or a row still
    # naming a retired Agent (never deleted, heal with compensating events) can share a
    # sid8 with a real live session, and compute_heartbeat blindly updates whatever this
    # rung returns. Same predicate lane 2's owner-agent check already carries
    # (`o.status='active'`), never a new rule, just applied here too, plus the dead-row
    # exclusion that lane never needed (it resolves through a still-current assertion,
    # this rung matches raw rows).
    row = await db.fetchrow(
        f"SELECT {cols} FROM agent_mounts m WHERE m.job_dir LIKE '%/jobs/' || $1 "
        "AND m.last_seen IS DISTINCT FROM $2 "
        "AND EXISTS (SELECT 1 FROM objects o WHERE o.canonical=m.agent_id "
        "  AND o.type='Agent' AND o.status='active') "
        "ORDER BY m.last_seen DESC NULLS LAST LIMIT 1", sid8, SUSPENDED_AT)
    if row is not None:
        return row
    owner = await db.fetchval(
        "SELECT o.canonical FROM current_assertions a "
        "JOIN objects o ON o.id=a.object_id AND o.type='Agent' AND o.status='active' "
        "WHERE a.name = 'anchor_sid:' || $1 "
        "ORDER BY a.observed_at DESC LIMIT 1", sid8)
    if owner is None:
        owner = f"agent:{sid8}"  # lane 3: self-evident. Falls through to the same
        # base-lineage query below, which naturally returns None if no such lineage ever
        # mounted (a made-up sid matches nothing real; no extra existence check needed).
    from src.orchestrator.agents import _generation
    base = _generation(str(owner))[0]
    return await db.fetchrow(
        f"SELECT {cols} FROM agent_mounts "
        "WHERE agent_id = $1 OR agent_id LIKE $1 || '-%' "
        "ORDER BY last_seen DESC NULLS LAST LIMIT 1", base)


_DOOR_WINDOW_SECS = 900  # the same 15-minute decay every liveness read in the fleet uses
_GHOST_GRACE_SECS = 120  # a row pulsed this recently is too new for the ghost rule to judge


def _normed(cwd: str | None) -> str:
    """Pure-string path normalization (no IO, safe beside an event loop): process cwds
    arrive kernel-resolved from /proc, and a symlinked row cwd that misses the cwd match
    still matches the project match (both labels come from the same .osiris walk), so a
    living row never hangs on symlink luck."""
    return os.path.normpath(cwd) if cwd else ""


async def sweep_stale_doors(actions: Actions, *, actor: str) -> int:
    """THE PILE RULE: a row is an address, and an agent has one last-known address, not a
    pile of stale rows back to the first week (an earlier incident found seats accumulating
    20+ rows apiece, ruled a bug). SessionEnd deletes a row when it fires; every kill, crash,
    and reboot skips the hook and leaks its row forever, which is where those piled-up seats
    came from. This sweep is the standing cleanup: among rows past the liveness window, keep
    exactly the freshest per agent as its last-known address, and keep even that only for an
    agent the graph still calls active (a fresh row elsewhere already is the address; a
    demoted claimant, a retired seat, or an objectless unrecognized identity holds no address
    at all). Pure SQL to find the doomed rows, no OS read: the ghost rule
    (`sweep_ghost_doors`) is the one that needs a live process check.

    REVERSIBLE AND AUDITED: this was a bare bulk DELETE, the same unwitnessed-irreversible
    defect class `drop_dead_project_mount` had before its own fix, higher volume here since
    this fires every 60s in production. Each doomed row is re-fetched `FOR UPDATE` inside its
    own transaction immediately before it is deleted, that lock is what makes the audit_log
    snapshot and the delete see the identical row, not two reads racing a third writer, and
    snapshotted into `audit_log` (action='sweep_stale_doors') via the same
    `_mount_snapshot`/`_MOUNT_COLS` shape `drop_dead_project_mount` uses, undoable through the
    same `undrop_dead_project_mount`. A row gone by the time its lock is taken (already
    released some other way) is simply skipped, never counted, never recorded, since nothing
    was actually done to it here. Returns rows released."""
    doomed = await actions.pool.fetch(
        "WITH aged AS ("
        "  SELECT job_dir, agent_id, row_number() OVER ("
        "    PARTITION BY agent_id ORDER BY COALESCE(last_seen, mounted_at) DESC) AS rn"
        "  FROM agent_mounts"
        "  WHERE COALESCE(last_seen, mounted_at) < now() - make_interval(secs => $1)), "
        "doomed AS ("
        "  SELECT a.job_dir FROM aged a"
        "  WHERE a.rn > 1"
        "     OR EXISTS (SELECT 1 FROM agent_mounts f WHERE f.agent_id = a.agent_id"
        "          AND COALESCE(f.last_seen, f.mounted_at) >= now() - make_interval(secs => $1))"
        "     OR NOT EXISTS (SELECT 1 FROM objects o WHERE o.canonical = a.agent_id"
        "          AND o.status = 'active')) "
        "SELECT job_dir FROM doomed", float(_DOOR_WINDOW_SECS))
    released = 0
    for d in doomed:
        async with actions.pool.acquire() as conn, conn.transaction():
            row = await conn.fetchrow(
                f"SELECT {', '.join(_MOUNT_COLS)} FROM agent_mounts "
                "WHERE job_dir=$1 FOR UPDATE", d["job_dir"])
            if row is None:
                continue  # already gone since the scan, nothing here to record or delete
            await conn.execute(
                "INSERT INTO audit_log (action, actor, payload) VALUES ($1,$2,$3)",
                "sweep_stale_doors", actor, _mount_snapshot(row))
            await conn.execute("DELETE FROM agent_mounts WHERE job_dir=$1", d["job_dir"])
            released += 1
    return released


async def sweep_ghost_doors(
    actions: Actions, *, body_cwds: set[str], body_projects: set[str], actor: str,
) -> int:
    """THE GHOST RULE: the late-SessionEnd problem, automated (ruled a bug after a fleet
    display once showed 5 agents when only 3 were actually up). A terminal kill skips the
    SessionEnd hook, so the dead tab's row stays "live" for the full decay window and the
    panel lies for fifteen minutes. The OS knows better right now: a fresh row whose cwd
    holds no claude process and whose project holds none anywhere (the double check, since
    an office and its governed repo can share a label, and a label quirk must never cost a
    living session its row) is a ghost, released on the spot.

    THE CALLER OWNS THE BLINDNESS CHECK: `census.live_bodies_by_cwd()` returning None means
    "could not look" and this function must simply not be called that tick. Two race guards:
    a grace floor (rows pulsed within the last two minutes are too new to judge, a session
    born after the /proc scan must never be read as bodyless), and the delete re-checks
    `last_seen` unchanged, so a row re-pulsed after the fetch survives untouched. A killed
    tab's row thus releases in about 2 minutes instead of decaying for 15.

    REVERSIBLE AND AUDITED: same defect class `drop_dead_project_mount` had before its own
    fix, unfixed here until now. The `last_seen` re-check now runs as a `FOR UPDATE` fetch
    inside the same transaction as the snapshot and the delete: one lock, one record, one
    write, so what `audit_log` (action='sweep_ghost_doors') records is provably the row that
    was actually removed, not a belief about it from a moment earlier. Undoable through
    `undrop_dead_project_mount`. Returns rows released."""
    from src.orchestrator.offices import is_bare_office_root

    rows = await actions.pool.fetch(
        f"SELECT {', '.join(_MOUNT_COLS)} FROM agent_mounts "
        "WHERE COALESCE(last_seen, mounted_at) >= now() - make_interval(secs => $1) "
        "  AND COALESCE(last_seen, mounted_at) < now() - make_interval(secs => $2)",
        float(_DOOR_WINDOW_SECS), float(_GHOST_GRACE_SECS))
    released = 0
    for r in rows:
        cwd = _normed(r["cwd"])
        # THE BARE-CONTAINER EXEMPTION: a live coordinator tab mounted at the seats
        # container itself (~/.osiris/seats, never a seat's own office subdirectory) is
        # never released by the ghost rule: traced against six weeks of audit_log for one
        # seat's own job_dir, dozens of sweeps, every single one at this exact cwd, none
        # at any other cwd that lineage ever carried. `_normed`'s own docstring already
        # concedes the cwd match can miss (its documented fallback is the project match),
        # but `live_bodies()`'s own bare-root skip (avoiding a phantom "seats" project)
        # means that fallback is structurally unreachable for exactly this population, so
        # a cwd-match miss here has no safety net at all, unlike every other row. The
        # ghost rule (fast, census-only release) simply does not apply to a
        # bare-container cwd; `sweep_stale_doors`'s own pile rule (staleness and the
        # graph's own belief the lineage is still active) is the correct, slower
        # mechanism for this population: it already protects a fresh row for an agent the
        # graph still calls active, exactly what a live coordinator tab is.
        if is_bare_office_root(cwd):
            continue
        project = r["project"] or (Path(cwd).name if cwd else "")
        if cwd in body_cwds or (project and project in body_projects):
            continue
        async with actions.pool.acquire() as conn, conn.transaction():
            row = await conn.fetchrow(
                f"SELECT {', '.join(_MOUNT_COLS)} FROM agent_mounts "
                "WHERE job_dir=$1 AND last_seen IS NOT DISTINCT FROM $2 FOR UPDATE",
                r["job_dir"], r["last_seen"])
            if row is None:
                continue  # re-pulsed (or already gone) since the scan, leave it untouched
            await conn.execute(
                "INSERT INTO audit_log (action, actor, payload) VALUES ($1,$2,$3)",
                "sweep_ghost_doors", actor, _mount_snapshot(row))
            await conn.execute("DELETE FROM agent_mounts WHERE job_dir=$1", r["job_dir"])
            released += 1
    return released


async def find_mount(pool: asyncpg.Pool, *, job_dir: str) -> MountRecord | None:
    """The durable mount for a job_dir, or None (never mounted / no durable handle)."""
    r = await pool.fetchrow(
        "SELECT job_dir, agent_id, project, cwd, model FROM agent_mounts WHERE job_dir=$1",
        job_dir,
    )
    if r is None:
        return None
    return MountRecord(job_dir=r["job_dir"], agent_id=r["agent_id"], project=r["project"],
                       cwd=r["cwd"], model=r["model"])


async def borrowed_job_dir_owner(pool: asyncpg.Pool, agent_id: str) -> str | None:
    """Is `agent_id` actually a different agent's own live job_dir slug, borrowed rather
    than minted (a past incident where two agents' identities crossed this way)? The
    fingerprint, not a guess: a live `agent_mounts` row exists whose `job_dir` basename is
    this exact bare id, and whose own `agent_id` is a different canonical, proof this string
    was never minted as an identity of its own, it is borrowed from a real, currently-mounted
    other agent. Returns that other agent's own canonical (the real owner, for a caller's own
    error/finding text) or None (not borrowed, a real identity, however sparsely
    provenanced, or a genuinely dead job_dir nobody currently mounts).

    Shared by `trigger._trustworthy_fallback_ancestor` (bind-before-spawn's own fallback
    guard), `seats.rehold_seat` (the third-party correction path, the same shape can reach a
    seat's holds edge through an explicit rehold, not just a launch), and
    `compositions.seat_holder_census` (the `osiris lint --check seat-holders` report): one
    fingerprint, never three independently-drifting copies of the same query."""
    bare = agent_id.removeprefix("agent:")
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT agent_id FROM agent_mounts WHERE job_dir LIKE '%/' || $1 AND agent_id <> $2 "
        "LIMIT 1", bare, agent_id)


async def _active_seat_for_lineage_base(pool: asyncpg.Pool, base: str) -> dict[str, Any] | None:
    """Does `base` (a lineage root, e.g. `agent:ad1a1cb0`) hold an active seat right
    now? Returns `{holder, seat_id, project, cwd}` (the seat's current generation, its
    own durable `anchor_cwd`/`house`) or None (no live `holds` link for this base at
    all, never held a seat, or genuinely released it since). The single query both
    `rescue_seat_holder_mount` and `demote_seatless_mount_if_outranked` share, so the
    two rules can never independently drift on what "holds a seat" means."""
    row = await pool.fetchrow(
        "SELECT hf.canonical AS holder, ht.canonical AS seat_id "
        "FROM links hl JOIN objects hf ON hf.id=hl.from_id "
        "  AND hf.type='Agent' AND hf.status='active' "
        "JOIN objects ht ON ht.id=hl.to_id AND ht.type='Seat' "
        "WHERE hl.type='holds' AND (hf.canonical=$1 OR hf.canonical LIKE $1 || '-%') "
        "AND (hl.valid_until IS NULL OR hl.valid_until > now()) "
        "ORDER BY hl.created_at DESC LIMIT 1", base)
    if row is None:
        return None
    seat_props = await pool.fetch(
        "SELECT a.name, a.value #>> '{}' AS val FROM current_assertions a "
        "JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name IN ('anchor_cwd','house')", row["seat_id"])
    props = {p["name"]: p["val"] for p in seat_props}
    return {"holder": row["holder"], "seat_id": row["seat_id"],
            "project": props.get("house"), "cwd": props.get("anchor_cwd") or ""}


async def rescue_seat_holder_mount(pool: asyncpg.Pool, *, job_dir: str) -> MountRecord | None:
    """THE STARTUP SEAT RESCUE: `find_mount`'s own row can vanish for reasons that have
    nothing to do with the lineage dying, a ghost/stale-row sweep (this module's own
    `_MOUNT_DROP_ACTIONS`) fires on staleness or a bodyless `/proc` census, never on
    whether the lineage still holds a seat. Measured live in one incident: the ghost sweep
    released a particular seat's own job_dir row repeatedly across six weeks, every prior
    time absorbed silently by some other re-attach path, until the one restart where nothing
    else caught it, and the startup handshake fell all the way through to a job_dir-derived
    unrecognized identity that happened to already exist in the graph (an artifact of this
    exact same chronic gap), minting its heir instead of the real seat holder's.

    Every sweep drop is already reversible and audited: the same `_mount_snapshot` shape
    `undrop_dead_project_mount` replays verbatim. Walks this job_dir's own audit history
    newest first (never just the single most recent entry, `_retire_seatless_mount_claim`
    can itself be the newest entry, naming the very unrecognized identity this rescue exists
    to see past) and, for the first entry whose `agent_id`'s lineage still holds an active
    seat right now, re-adopts that lineage's current generation, never a fresh mint, never
    the dropped generation itself (a lineage moves on; the seat's present holder is the fact
    that matters). Never fires when no entry in this job_dir's history ever named a seat
    holder: a real unrecognized session or a real retirement, not the ghost-sweep-over-a-
    live-holder this exists to catch.

    A synthetic MountRecord, built from the seat's own durable `anchor_cwd`/`house` (facts
    the seat carries regardless of any one mount row's own history): this never writes
    anything itself; the caller's own registration path does that, exactly as it would for
    a genuine `find_mount` hit."""
    from src.orchestrator.agents import _generation

    rows = await pool.fetch(
        "SELECT payload FROM audit_log WHERE action = ANY($1::text[]) "
        "AND payload->>'job_dir' = $2 ORDER BY created_at DESC LIMIT 20",
        list(_MOUNT_DROP_ACTIONS), job_dir)
    for r in rows:
        dropped_agent = (r["payload"] or {}).get("agent_id")
        if not dropped_agent:
            continue
        seat = await _active_seat_for_lineage_base(pool, _generation(dropped_agent)[0])
        if seat is not None:
            return MountRecord(job_dir=job_dir, agent_id=seat["holder"],
                               project=seat["project"], cwd=seat["cwd"], model=None)
    return None


async def _retire_seatless_mount_claim(
    pool: asyncpg.Pool, *, job_dir: str, actor: str,
) -> None:
    """THE COMPENSATING HALF: once a seat holder is found to outrank a job_dir's own
    current (still-present, not merely dropped) row, a seatless unrecognized identity that
    already minted itself once and now owns a live `agent_mounts` row of its own,
    perpetuating on every later restart exactly as a past incident showed, that row is
    released the same audited, reversible way every other drop in this module is, so the
    next restart's `find_mount` stops finding the unrecognized identity too. Its own Agent
    object is left untouched (never deleted, never retired), only its claim on this job_dir.
    A row gone by the time its lock is taken (already released some other way) is simply
    skipped, same discipline as `sweep_stale_doors`/`sweep_ghost_doors`."""
    async with pool.acquire() as conn, conn.transaction():
        row = await conn.fetchrow(
            f"SELECT {', '.join(_MOUNT_COLS)} FROM agent_mounts WHERE job_dir=$1 FOR UPDATE",
            job_dir)
        if row is None:
            return
        await conn.execute(
            "INSERT INTO audit_log (action, actor, payload) VALUES ($1,$2,$3)",
            "retire_seatless_mount_claim", actor, _mount_snapshot(row))
        await conn.execute("DELETE FROM agent_mounts WHERE job_dir=$1", job_dir)


async def demote_seatless_mount_if_outranked(
    pool: asyncpg.Pool, *, job_dir: str, actor: str,
) -> MountRecord | None:
    """The rescue above only fires when `find_mount` comes back empty, but once a
    wrongly-minted unrecognized identity has already registered its own `agent_mounts` row
    for this job_dir (exactly what happens the very next time a wrong mint from that gap
    actually lands), `find_mount` keeps finding that row forever after, self-reinforcing.
    This checks whether the job_dir's current row belongs to a seatless lineage while this
    job_dir's own audit history names a different lineage that holds an active seat right
    now: if so, the seat holder wins, and the seatless row's claim is retired (an audited,
    undoable compensating event; the unrecognized identity's Agent object itself is never
    touched, the operator's own `/merge` remains the one action that folds it away for
    good).

    Returns the seat holder's MountRecord when it outranks the current row, else None
    (either the current row's own lineage genuinely holds a seat, or nothing in this
    job_dir's history ever claimed one, a real unrecognized session, left exactly as
    `find_mount` found it)."""
    from src.orchestrator.agents import _generation

    bound = await find_mount(pool, job_dir=job_dir)
    if bound is None:
        return None
    if await _active_seat_for_lineage_base(pool, _generation(bound.agent_id)[0]) is not None:
        return None  # the row's own lineage genuinely holds a seat, nothing outranks it
    rescue = await rescue_seat_holder_mount(pool, job_dir=job_dir)
    if rescue is None:
        return None  # no seat holder ever claimed this job_dir, a real, seatless session
    await _retire_seatless_mount_claim(pool, job_dir=job_dir, actor=actor)
    return rescue


async def resolve_dirty_tree_owner(
    pool: asyncpg.Pool, repo_dir: str | None,
) -> dict[str, Any] | None:
    """WHO OWNS an uncommitted hunk at `repo_dir` (mapping an uncommitted hunk to its owner
    by the worktree and mount that touched it). The join nobody had built:
    `settle.py`'s own `uncommitted_git_work` (the dirty-check) against `agent_mounts.cwd` (no
    shared function resolved "who is mounted at this cwd" before this, every prior read site
    did its own ad-hoc inline SELECT). Exact cwd match only, never fuzzy path containment: a
    worktree's own root is a mount's own cwd by this codebase's own convention (measured
    live: 60+ worktrees, one per task branch, cwd == the worktree root every time), so a
    substring/prefix match would risk matching a sibling worktree that merely shares a path
    segment.

    Most-recently-active mount at that exact cwd wins (`ORDER BY last_seen DESC LIMIT 1`),
    including a vacated seat's own stale row, deliberately: this function never decides a
    mount is "too old to count," it returns `last_seen` so the caller can judge staleness
    itself (a settle() reader is in a much better position to weigh "this mount is 6 hours
    old" than a blind cutoff baked in here would ever be).

    Returns None in the two honest "cannot answer" cases: `repo_dir` has no uncommitted work
    at all (nothing to attribute), or no mount row's cwd matches exactly (never a guess, the
    caller keeps its own disclaimer verbatim, same fail-open rule `uncommitted_git_work`
    itself already holds for `repo_dir` outside a git worktree entirely). Otherwise
    `{"agent_id", "last_seen"}`, `last_seen` as a live `datetime`, the caller's own concern
    to format or age-check."""
    from src.orchestrator.settle import uncommitted_git_work

    dirty = await uncommitted_git_work(repo_dir)
    if not dirty:
        return None
    row = await pool.fetchrow(
        "SELECT agent_id, last_seen FROM agent_mounts WHERE cwd=$1 "
        "ORDER BY last_seen DESC LIMIT 1", repo_dir)
    if row is None:
        return None
    return {"agent_id": row["agent_id"], "last_seen": row["last_seen"]}


LIVENESS_WINDOW_MINUTES = 15


def freshest_liveness_ts(
    mount_seen: datetime | None, transcript_mtime: datetime | None = None,
) -> datetime | None:
    """The one liveness timestamp every reader of fleet identity must agree on (superseding
    an earlier design after a live specimen exposed its flaw).

    `transcript_mtime` used to be `current_assertions.last_active`, replaced, not merely
    renamed: that property was meant to carry a transcript's own mtime (session-miner's
    `_stamp_alive`, src/ingest/sessions.py: "a transcript grows when an agent works, whether
    or not it calls Osiris"), but the miner only ever stamps it once, when a transcript is
    first mined; nothing re-stamps it as the same lineage's later generations keep working,
    so on any lineage the miner touched once and never again, it freezes at whatever date
    that pass ran and sits there forever, silently correct-shaped (a real ISO timestamp, a
    real source_id) and silently wrong. Verified live on one lineage: `current_assertions.
    last_active` on the base generation read a date over two months stale, source_id=
    'session-miner', the only last_active row that lineage had ever carried. `max()`-ing a
    live signal against that is correct in isolation, but the property's mere presence as an
    eligible signal is the defect: a live agent reads dead the instant its own mount row goes
    missing for any reason, because the fallback is a two-month-old stale value instead of a
    live read.

    `transcript_mtime` is now populated only by a live filesystem stat, at read time
    (`_lineage_transcript_mtime`, called by `agent_liveness`/`agent_liveness_exact` only when
    `mount_seen` doesn't already prove liveness, the common case, a fresh mount row, costs
    nothing extra). This still answers the earlier specimen (an agent live and working with
    no agent_mounts row at all) via the same durable `anchor_sid` ledger
    record_session_anchor already keeps, never a graph property that can go stale forever
    once written."""
    stamps = [t for t in (mount_seen, transcript_mtime) if t is not None]
    return max(stamps) if stamps else None


def is_live(ts: datetime | None, *, now: datetime | None = None) -> bool:
    """True iff `ts` (from freshest_liveness_ts) falls within the shared liveness window."""
    now = now or datetime.now(UTC)
    return ts is not None and now - ts < timedelta(minutes=LIVENESS_WINDOW_MINUTES)


# A PAST PERFORMANCE REGRESSION: the first version of this function did one
# `root.rglob(f"{sid}.jsonl")` per session id, a full recursive walk of ~/.claude/projects
# (7,518 directories measured live) for every candidate sid. A lineage with a large
# `anchor_sid` ledger (one base carried 4,478 rows, one generation alone 3,548) turned one
# liveness check into thousands of tree walks, about 1s each on the box, so a single call
# could run for the better part of an hour, holding `arq_worker._boot_lock` for its whole
# run and starving every sibling cron behind it. First fix: walk the tree once per call
# regardless of sid count. Follow-up: a sub-sweep asking about many agents in one tick
# still pays one walk per agent even at that reduced cost, the tree does not change
# between two liveness checks a few seconds apart, so `_transcript_index` below caches
# the whole sid->path mapping at module level for `_TRANSCRIPT_INDEX_TTL_SECS`, and every
# call in that window reuses it: the walk itself now happens at most once per TTL window,
# not once per call.
_TRANSCRIPT_INDEX_TTL_SECS = 60.0
_transcript_index_cache: dict[str, tuple[float, dict[str, Path]]] = {}


def _transcript_index(root: Path) -> dict[str, Path]:
    """sid (file stem) -> path, for every `*.jsonl` under `root`: one walk, cached at
    module level keyed by `root` for `_TRANSCRIPT_INDEX_TTL_SECS`. A stale index only ever
    costs a liveness read a slightly-out-of-date mtime within the TTL window, never a wrong
    answer: a transcript that appeared in the last `_TRANSCRIPT_INDEX_TTL_SECS` is found on
    the next rebuild at worst, and `is_live`'s own 15-minute window easily absorbs a 60s
    staleness margin."""
    key = str(root)
    now = time.monotonic()
    cached = _transcript_index_cache.get(key)
    if cached is not None and now - cached[0] < _TRANSCRIPT_INDEX_TTL_SECS:
        return cached[1]
    index: dict[str, Path] = {}
    try:
        for p in root.expanduser().rglob("*.jsonl"):
            index[p.stem] = p
    except OSError:
        pass
    _transcript_index_cache[key] = (now, index)
    return index


def _freshest_transcript_mtime(root: Path, session_ids: list[str]) -> datetime | None:
    """A stat(), nothing more: mirrors liveness.py's `_sessions()` own observation, just
    scoped to a handful of named session ids instead of a full-tree walk (this only ever
    runs from the rare fallback branch below, never the primary path). The tree walk itself
    is `_transcript_index`'s own job (cached, at most once per TTL window); this only stats
    the handful of paths the index resolves for the wanted sids."""
    wanted = {sid for sid in session_ids if sid}
    if not wanted:
        return None
    index = _transcript_index(root)
    freshest: datetime | None = None
    for sid in wanted:
        p = index.get(sid)
        if p is None:
            continue
        try:
            mtime = datetime.fromtimestamp(p.stat().st_mtime, UTC)
        except OSError:
            continue
        if freshest is None or mtime > freshest:
            freshest = mtime
    return freshest


# A lineage's `anchor_sid` ledger only ever grows (record_session_anchor never retires a
# row): a long-lived lineage can carry thousands (one specimen: 4,478 on one base, 3,548 on
# a single generation). Liveness only ever cares whether any recent session is still live,
# so consulting more than a handful of the freshest is pure waste: every extra sid is one
# more entry `_freshest_transcript_mtime`'s single walk must still find before it can
# early-exit. Capped, never unbounded, regardless of how large the ledger grows.
MAX_ANCHOR_SIDS_FOR_LIVENESS_CHECK = 25


async def _lineage_transcript_mtime(
    pool: asyncpg.Pool, agent_id: str, base: str | None = None,
) -> datetime | None:
    """LIVE FALLBACK for a lineage with no usable `agent_mounts` row (an earlier specimen:
    an agent live and working, writing to its own transcript, with nothing in the mount
    registry to say so, a wiped or never-written cache row, not evidence of absence). Reads
    this lineage's own `anchor_sid:*` ledger (`record_session_anchor`, durable, never swept,
    "a real session bound here at least once") for candidate session ids, then stats each
    one's own transcript file directly: the same free, deterministic, always-fresh
    observation `liveness.py`'s periodic sweep performs, done live and scoped to one lineage
    on demand instead of a full-tree walk on a timer. `base=None` means an exact check (no
    lineage widening), `agent_liveness_exact`'s own contract; a base widens it the way
    `agent_liveness` already does for its own mount query.

    Consults at most `MAX_ANCHOR_SIDS_FOR_LIVENESS_CHECK` sids, the freshest by
    `observed_at` (a past performance regression, see above): a lineage's ledger only
    grows, and liveness only needs to know whether any recent session is still live, never
    whether the lineage's very first session ever was."""
    from src.config.settings import get_settings

    root = get_settings().osiris_transcripts
    if not root:
        return None
    if base is not None:
        rows = await pool.fetch(
            "SELECT a.value #>> '{}' AS sid FROM current_assertions a "
            "JOIN objects o ON o.id=a.object_id "
            "WHERE o.type='Agent' AND a.name LIKE 'anchor_sid:%' "
            "AND (o.canonical=$1 OR o.canonical=$2 OR o.canonical LIKE $2 || '-%') "
            "ORDER BY a.observed_at DESC LIMIT $3",
            agent_id, base, MAX_ANCHOR_SIDS_FOR_LIVENESS_CHECK)
    else:
        rows = await pool.fetch(
            "SELECT a.value #>> '{}' AS sid FROM current_assertions a "
            "JOIN objects o ON o.id=a.object_id "
            "WHERE o.type='Agent' AND a.name LIKE 'anchor_sid:%' AND o.canonical=$1 "
            "ORDER BY a.observed_at DESC LIMIT $2",
            agent_id, MAX_ANCHOR_SIDS_FOR_LIVENESS_CHECK)
    if not rows:
        return None
    return await asyncio.to_thread(
        _freshest_transcript_mtime, Path(root), [r["sid"] for r in rows])


async def agent_liveness(pool: asyncpg.Pool, agent_id: str) -> dict[str, Any]:
    """Is this AGENT live right now (for a message's send result)? Lineage-aware: a mount
    row is an address, and machinery legitimately re-points addresses between generations
    (the liveness promotion follows the lineage head; registrations rewrite agent_id), so a
    probe for one generation must not read dead because the row momentarily wears another
    generation's numeral of the same lineage. THE LINEAGE ANSWERS: `agent_mounts.last_seen`,
    widened across every generation of the base, no longer blended against
    `current_assertions.last_active` (a stale-forever miner property, see
    `freshest_liveness_ts`'s own docstring). When that alone doesn't already prove liveness,
    a live transcript-mtime check (`_lineage_transcript_mtime`) covers the case the property
    used to (an earlier specimen: no mount row, genuinely live), never paid when a fresh
    mount row
    already settles it. live = within 15 min."""
    from src.orchestrator.agents import _generation
    base = _generation(agent_id)[0]
    mount_seen = await pool.fetchval(
        "SELECT max(last_seen) FROM agent_mounts "
        "WHERE agent_id=$1 OR agent_id=$2 OR agent_id LIKE $2 || '-%'", agent_id, base)
    transcript_mtime = (None if is_live(mount_seen)
                        else await _lineage_transcript_mtime(pool, agent_id, base))
    ts = freshest_liveness_ts(mount_seen, transcript_mtime)
    # `ever_mounted`: distinct from `live` on purpose, and deliberately not keyed on
    # `mount_seen` alone, agent_mounts is a cache (the same distinction an earlier tenure
    # fix draws: a durable registry row, but one the sweep (mounts.py's own doomed-row
    # deletion) or a server reboot can leave with no row at all for a lineage that
    # genuinely mounted and sent messages hours earlier). `anchor_sid:*`
    # (record_session_anchor, stamped once per real session at handshake time, the
    # anonymous-canonical case excepted) is never swept, the durable, permanent proof "a
    # real session bound to this identity at least once," the same class of signal the
    # tenure fix's graph-assertion leg trusts over the mount cache for the identical
    # reason.
    ever_mounted = mount_seen is not None or bool(await pool.fetchval(
        "SELECT 1 FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.type='Agent' AND (o.canonical=$1 OR o.canonical=$2 "
        "  OR o.canonical LIKE $2 || '-%') AND a.name LIKE 'anchor_sid:%' LIMIT 1",
        agent_id, base))
    return {"live": is_live(ts), "last_seen": ts.isoformat() if ts is not None else None,
            "ever_mounted": ever_mounted}


async def agent_liveness_exact(pool: asyncpg.Pool, agent_id: str) -> dict[str, Any]:
    """`agent_liveness`'s exact-canonical counterpart, no lineage-base widening at all.
    Built for `seats.follow_binding`'s own live-sibling guard and reused by
    `agents.correct_succession`'s liveness guard (a live specimen caught mid-batch:
    `agent_liveness`'s widened check read a purely historical ancestor generation as "live"
    the instant its lineage's current, unrelated-in-this-context generation had a fresh
    mount row, exactly the false positive `agent_liveness`'s own docstring warns any caller
    reading a specific generation's identity, not the lineage's, must avoid). Any caller
    asking "is this exact id itself active", not "is this id's lineage active", wants this,
    never the widened check. Same live transcript-mtime fallback as `agent_liveness`,
    exact-scoped: only this one canonical's own `anchor_sid:*` ledger, never a sibling
    generation's."""
    mount_seen = await pool.fetchval(
        "SELECT max(last_seen) FROM agent_mounts WHERE agent_id=$1", agent_id)
    transcript_mtime = (None if is_live(mount_seen)
                        else await _lineage_transcript_mtime(pool, agent_id))
    ts = freshest_liveness_ts(mount_seen, transcript_mtime)
    return {"live": is_live(ts), "last_seen": ts.isoformat() if ts is not None else None}


async def project_last_seen(pool: asyncpg.Pool, project: str) -> str | None:
    """The freshest mount activity for a project (ISO), for the send() listener probe."""
    v = await pool.fetchval(
        "SELECT max(last_seen) FROM agent_mounts WHERE project=$1", project)
    return v.isoformat() if v is not None else None


async def project_prev_seen(
    pool: asyncpg.Pool, project: str | None, *, exclude_job_dir: str
) -> datetime | None:
    """The lineage's last sign of life, excluding the caller's own (just-upserted) row: the
    while-you-were-away anchor for a fresh session. A new session id has no past of its own,
    but its project does, and that past is exactly what it must not wake blind to (a
    sibling's tab reopened as a new session and got no fold-in of updates while a duplicate
    session had already resolved its threads)."""
    if not project:
        return None
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT max(last_seen) FROM agent_mounts WHERE project=$1 AND job_dir <> $2",
        project, exclude_job_dir)


async def live_co_agents(
    pool: asyncpg.Pool, *, project: str, exclude_job_dir: str | None = None,
    exclude_lineage_base: str | None = None, within_secs: int = 900,
) -> list[dict[str, Any]]:
    """One query for "which OTHER agent_mounts rows are live on this project right now",
    shared by mcp_server.py's `_co_agents` (mount()/orient()'s co-agents block) and
    handshake.py's `automount()` (the startup routine's own co-agents block). These used to
    be two independent copies of this same query, free to drift from each other: the point
    of consolidating them into one function is that two drifting copies of one query is
    precisely the class of bug this was built to end, except automount()'s own copy was
    apparently never consolidated into it before now.

    Cache-based, confessed, never a gate: this uses `agent_mounts.last_seen` freshness only,
    and is never cross-checked against `registry_census`/`is_occupied_by_a_live_body` (the
    real harness+/proc authority used everywhere this codebase actually refuses or mints on
    liveness). Both callers use this purely for awareness ("a sibling might be touching this
    tree"), never to block anything, so the cheap read is the right one; it just must stop
    being silently mistaken for a verified fact, the same cache-freshness discipline this
    house's other liveness fixes already apply.

    Returns every matching row, freshest first, with no built-in cap. Without this, a
    truncated top-N can silently under-report a live sibling process to a caller who trusts
    it (an earlier version of `_co_agents` had its own `LIMIT 8`, which under-reported on a
    project with more than 8 fresh rows). Each caller picks its own display count from the
    full list and can therefore say "N more not shown" instead of just dropping them.

    Two different exclusion needs, since the two callers know different things about
    "myself" at the point they call this: `exclude_job_dir` (automount()'s startup routine
    fires before an agent_id may even be resolved, so job_dir is the one thing always
    known); `exclude_lineage_base` (mount()/orient() know their own resolved agent_id and
    want to exclude every generation of their own lineage, not just their own exact row)."""
    where = ["m.project = $1", "m.last_seen > now() - make_interval(secs => $2)"]
    args: list[Any] = [project, float(within_secs)]
    if exclude_job_dir is not None:
        args.append(exclude_job_dir)
        where.append(f"m.job_dir <> ${len(args)}")
    rows = await pool.fetch(
        f"SELECT DISTINCT ON (m.agent_id) m.agent_id, m.cwd, m.job_dir, m.last_seen "
        f"FROM agent_mounts m WHERE {' AND '.join(where)} "
        f"ORDER BY m.agent_id, m.last_seen DESC", *args)
    out = [dict(r) for r in rows]
    out.sort(key=lambda r: r["last_seen"], reverse=True)
    if exclude_lineage_base is not None:
        from src.orchestrator.agents import _generation

        out = [r for r in out if _generation(str(r["agent_id"]))[0] != exclude_lineage_base]
    return out


async def live_claimed_sids(
    pool: asyncpg.Pool, *, exclude_session_key: str | None, within_secs: int = 900
) -> set[str]:
    """Session handles currently held by a live mount on a different client session: the
    claimed set the cwd-guess must refuse (two anchorless same-project sessions grabbing the
    hottest transcript would otherwise merge). Lineage-aware: a minted heir (agent:x-ii)
    claims its base handle x too.

    A seated session id is claimed until released: a mount row bound to a Seat stays claimed
    regardless of pulse, since its holder dying must never make its identity guessable by an
    unrecognized session reading the hottest transcript. The liveness window guards the
    living; the binding guards the seated dead (session_end releases the row, so a
    deliberately-closed seat frees its session id the honest way)."""
    rows = await pool.fetch(
        "SELECT agent_id, session_key FROM agent_mounts "
        "WHERE last_seen > now() - make_interval(secs => $1) OR seat_id IS NOT NULL",
        within_secs)
    from src.orchestrator.agents import _generation

    out: set[str] = set()
    for r in rows:
        if exclude_session_key and r["session_key"] == exclude_session_key:
            continue
        root, _ = _generation(r["agent_id"])
        out.add(root.removeprefix("agent:"))
    return out


async def live_mount_sid_prefixes(
    pool: asyncpg.Pool, *, within_secs: int = 900,
) -> set[str]:
    """The session-id prefixes (first 8 chars) of mounts with a live pulse: the transcript
    heal's do-not-touch set (the job_dir anchor is ~/.claude/jobs/<sid[:8]>, so its basename
    is the session prefix). Deliberately pulse-only, unlike `live_claimed_sids`: the
    seated-forever claim there guards against identity guessing, while the heal needs
    process-liveness; a seated but closed session is exactly the one whose transcript must
    stay healable."""
    rows = await pool.fetch(
        "SELECT job_dir FROM agent_mounts "
        "WHERE last_seen > now() - make_interval(secs => $1)", within_secs)
    return {Path(r["job_dir"]).name for r in rows if r["job_dir"]}


async def fleet_pulse(
    pool: asyncpg.Pool, *, lease_secs: int = 900, live_secs: int = 900
) -> str:
    """One glance line for orient: 'N live · owed X · briefs Y · wakes K/h'. The caller
    omits the key on any failure, since the pulse must never slow or crash orient.

    A thin view over the shared segment authority in surface.py: `live`/`owed`/
    `briefs_total`/`wakes`/`spend` are the fleet-wide-scoped variants (this line has no
    project of its own to narrow to; orient() calls it unscoped). All thresholds and
    presentation rules live in surface.py now; this function only picks its five segments
    and formats them the way this line has always read.

    One deliberate exception, disclosed rather than silently unified: this pulse has never
    gated spend on a threshold, it shows the day's figure whenever spend is metered, full
    stop (unlike the statusline's dark-until-60%-of-cap convention). So this reads
    `seg.spend.data['metered']` directly rather than `seg.spend.show`."""
    from src.orchestrator import surface

    seg = await surface.fetch(pool, lease_secs=lease_secs, live_secs=live_secs)
    vis = f"+{seg.live.data['visitors']} " if seg.live.data["visitors"] else ""
    spend_tail = ""
    if seg.spend.data.get("metered"):
        d = seg.spend.data
        spend = "$∞" if d["unlimited"] else f"${d['spent']:.2f}/${d['cap']:.0f}"
        blind = f" ⚠{d['blind']} unpriced" if d["blind"] else ""
        spend_tail = f" · {spend} day{blind}"
    return (f"{seg.live.data['souls']} live {vis}· owed {seg.owed.data['owed']} "
            f"· briefs {seg.briefs_total.data['briefs']} · wakes {seg.wakes.data['wakes']}/h"
            f"{spend_tail}")


async def while_away(
    pool: asyncpg.Pool, project: str | None, agent_id: str, since: datetime | None
) -> dict[str, Any] | None:
    """What happened in this project's name between the lineage's last sign of life and now:
    a returning agent must not have to guess where it stands. It reports who acted as its
    project (wakes by lane; other agent ids that sent mail under its name) and how its
    conversations moved (per-thread last word plus settled state). Returns None when there
    is no anchor (first mount) or nothing happened; the quiet case stays quiet."""
    if since is None or not project:
        return None
    wakes = await pool.fetch(
        "SELECT mode, count(*) AS n FROM agent_wakes "
        "WHERE to_project=$1 AND woke_at > $2 GROUP BY mode", project, since)
    wearers = [r["from_agent"] for r in await pool.fetch(
        "SELECT DISTINCT from_agent FROM fleet_messages "
        "WHERE from_project=$1 AND from_agent <> $2 AND created_at > $3",
        project, agent_id, since)]
    threads = await pool.fetch(
        "SELECT DISTINCT ON (COALESCE(thread_id, id)) COALESCE(thread_id, id) AS thread, "
        " from_agent, from_project, to_project, left(body, 200) AS body, created_at, "
        " read_at IS NOT NULL AS settled "
        "FROM fleet_messages WHERE (to_project=$1 OR from_project=$1) AND created_at > $2 "
        "ORDER BY COALESCE(thread_id, id), created_at DESC LIMIT 8", project, since)
    # your spawns: children your lineage delegated to (spawned_by -> any generation of your
    # base) since your last sign of life. The parent is told, never surprised. Registered
    # live by the spawn hooks, or by the miner's disk round; same keying, so they show here
    # either way.
    from src.orchestrator.agents import _generation
    base = _generation(agent_id)[0]  # generation-aware: agent:x-xvii → agent:x; uuids intact
    spawns = await pool.fetch(
        "SELECT c.canonical, l.first_seen, "
        " (SELECT value #>> '{}' FROM current_assertions WHERE object_id=c.id "
        "   AND name='agent_type' ORDER BY confidence DESC, observed_at DESC LIMIT 1) AS kind, "
        " (SELECT value #>> '{}' FROM current_assertions WHERE object_id=c.id "
        "   AND name='source_model' ORDER BY confidence DESC, observed_at DESC LIMIT 1) AS model, "
        " (SELECT value #>> '{}' FROM current_assertions WHERE object_id=c.id "
        "   AND name='spawn_witnessed' "
        "   ORDER BY confidence DESC, observed_at DESC LIMIT 1) AS witnessed "
        "FROM links l JOIN objects c ON c.id=l.from_id JOIN objects p ON p.id=l.to_id "
        "WHERE l.type='spawned_by' AND (p.canonical = $1 OR p.canonical LIKE $1 || '-%') "
        "  AND l.first_seen > $2 ORDER BY l.first_seen DESC LIMIT 8", base, since)
    if not wakes and not wearers and not threads and not spawns:
        return None
    # the ghost-spawn rule: a spawn whose only evidence is the harness's announcement, with
    # no transcript ever materialized and no act ever seen, must not carry the "another
    # actor" warning. It gets named (the record forgets nothing) but rendered as the harness
    # machinery it almost certainly is; the warning is reserved for witnessed hands.
    all_ghost = bool(spawns) and not wakes and not wearers and not threads \
        and all(s["witnessed"] == "false" for s in spawns)
    return {
        "since": since.isoformat(),
        "wakes": {r["mode"]: r["n"] for r in wakes},
        "acted_in_your_name": wearers,
        "threads": [
            {"thread": t["thread"], "last_from": t["from_agent"],
             "between": f"{t['from_project']} → {t['to_project']}",
             "settled": t["settled"], "at": t["created_at"].isoformat(), "last": t["body"]}
            for t in threads],
        **({"spawns": [
            {"agent": s["canonical"], "type": s["kind"], "model": s["model"],
             "at": s["first_seen"].isoformat(),
             **({"unwitnessed": "ephemeral harness sidechain, announced but never "
                                "observed; likely internal, not another actor"}
                if s["witnessed"] == "false" else {})}
            for s in spawns]} if spawns else {}),
        "note": ("only unwitnessed harness sidechains appeared: announced by the harness, "
                 "but no transcript or act was ever observed; likely internal machinery. "
                 "Nothing else moved in your name." if all_ghost else
                 "another actor may have acted in your name here, read this before assuming "
                 "you know where you stand; the graph, not your memory, records these turns"),
    }


# ── SEAT REBIND ──────────────────────────────────────────────────────────────────────────
# treating `path = project = identity` orphaned a seat when its folder was moved. A seat's
# identity, lineage, attribution, and mail all key on its durable project label (the
# `project` assertion; house_of reads it), never on cwd. Moving the folder should be a
# non-event; without this fix it silently detaches the .osiris pin and strands every durable
# mount row at the old path. Piloted on a pure office seat with no code in it.


def _write_osiris_file(new_cwd: str, project_label: str) -> str:
    """Write/refresh `new_cwd/.osiris`, pinning `project_label`: the existing durable
    mechanism (`agents.read_project_label`) that makes a folder rename stop mattering. Reads
    only the file already at `new_cwd` (never walks up; a parent repo's `.osiris` is not this
    seat's business). Every top-level scalar key already declared there survives untouched
    (`model =`, or anything added later; a rebind must never silently drop a key it doesn't
    recognize); `project =` is always overwritten to the label being pinned. Honest limit:
    TOML comments do not survive a rewrite, since the parser never sees them."""
    d = Path(new_cwd)
    d.mkdir(parents=True, exist_ok=True)
    f = d / ".osiris"
    kept: dict[str, Any] = {}
    if f.is_file():
        try:
            kept = {k: v for k, v in tomllib.loads(f.read_text()).items()
                    if k != "project" and isinstance(v, (str, int, float, bool))}
        except (OSError, tomllib.TOMLDecodeError, ValueError):
            kept = {}
    lines = [f"project = {json.dumps(project_label)}"]
    for k, v in sorted(kept.items()):
        # json string/number/bool literals are valid TOML values for these scalar types
        lines.append(f"{k} = {json.dumps(v)}")
    f.write_text("\n".join(lines) + "\n")
    return str(f)


def _harness_slug(cwd: str) -> str:
    """The harness's transcript-directory name for a cwd. The current harness (witnessed
    live, 2026-07-16, v2.1.211: ~/.osiris/seats/<name> → -home-asuramaya--osiris-seats-
    <name>) replaces both '/' and '.' with '-'; an earlier scheme kept the dot, and some
    sessions parked directories under dot-form slugs the new harness cannot see.
    `_legacy_slug` + `converge_legacy_slug` fold those back home."""
    return cwd.replace("/", "-").replace(".", "-")


def _legacy_slug(cwd: str) -> str:
    """The old scheme ('/' → '-', dots kept): read-side only, for convergence."""
    return cwd.replace("/", "-")


def converge_legacy_slug(cwd: str, *, projects_root: Path | None = None) -> int:
    """Fold a cwd's legacy dot-form slug dir into its canonical one (never clobbering): the
    split-brain fix that makes content moved under the old scheme visible to the harness
    again. Idempotent; returns entries moved; 0 when the schemes agree for this cwd or there
    is nothing legacy."""
    root = projects_root or (Path.home() / ".claude" / "projects")
    old_name, new_name = _legacy_slug(cwd), _harness_slug(cwd)
    if old_name == new_name:
        return 0
    old_dir, new_dir = root / old_name, root / new_name
    if not old_dir.is_dir():
        return 0
    moved, _left, _landed = _merge_dir(old_dir, new_dir)
    with contextlib.suppress(OSError):
        old_dir.rmdir()
    return moved


def _merge_dir(old: Path, new: Path) -> tuple[int, int, list[Path]]:
    """Move every entry old→new, never clobbering (a transcript that exists on both sides
    stays where it is; losing either would falsify the record); one level of recursion
    merges subdirectories (the subagents/ tree). Returns (moved, left_behind, landed).
    `landed` is every .jsonl this merge itself moved, so the caller can re-address exactly
    what it relocated and nothing co-resident."""
    moved = left = 0
    landed: list[Path] = []
    new.mkdir(parents=True, exist_ok=True)
    for entry in sorted(old.iterdir()):
        target = new / entry.name
        if not target.exists():
            entry.rename(target)
            moved += 1
            if target.is_file() and target.suffix == ".jsonl":
                landed.append(target)
        elif entry.is_dir() and target.is_dir():
            m, s, sub = _merge_dir(entry, target)
            moved += m
            left += s
            landed.extend(sub)
            with contextlib.suppress(OSError):
                entry.rmdir()
        else:
            left += 1
    return moved, left, landed


_HEAL_QUIET_SECS = 300


def _transcript_cwd_probe(path: Path, *, max_lines: int = 50) -> str | None:
    """The first top-level `cwd` a transcript carries: the field the harness's resume
    validator reads (witnessed live: /resume refused a moved transcript naming its first
    recorded cwd, not the slug directory it was listed under). Returns None when the head
    carries no cwd at all (a summary-only stub, or unreadable)."""
    try:
        with path.open(encoding="utf-8", errors="replace") as f:
            for _ in range(max_lines):
                line = f.readline()
                if not line:
                    break
                try:
                    obj = json.loads(line)
                except ValueError:
                    continue
                if isinstance(obj, dict) and isinstance(obj.get("cwd"), str):
                    return str(obj["cwd"])
    except OSError:
        return None
    return None


def lineage_cwd_evidence(
    sids: set[str], *, projects_root: Path | None = None,
) -> str | None:
    """Where a lineage's transcripts actually live: the anchor authority when the mount
    registry holds no row (a lineage older than the registry, or one that only ever
    registered through per-session startup rows). Sweeps the projects root for the
    lineage's session-id-prefixed transcripts and returns the freshest one's internal cwd,
    the same rule as the recollection guard: transcript evidence decides an address, never
    a remembered or inferred one. Returns None when no session id has a transcript anywhere
    (a lineage with no running process at all: nothing to carry, and nothing is guessed)."""
    root = projects_root or (Path.home() / ".claude" / "projects")
    best: tuple[float, Path] | None = None
    if not root.is_dir():
        return None
    for slug in root.iterdir():
        if not slug.is_dir():
            continue
        for sid in sids:
            if not sid:
                continue
            for f in slug.glob(sid + "*.jsonl"):
                try:
                    mtime = f.stat().st_mtime
                except OSError:
                    continue
                if best is None or mtime > best[0]:
                    best = (mtime, f)
    return _transcript_cwd_probe(best[1]) if best else None


def _rewrite_transcript_cwd(
    path: Path, new_cwd: str, *, expect: tuple[int, int] | None = None,
) -> int:
    """Re-address a transcript: point every line's top-level `cwd` at `new_cwd`, atomically
    (tmp + rename, so a crash mid-write never leaves a half-file). Only the routing field
    moves; content, trackingPath, and every other byte pass through verbatim. The graph is
    the history; the transcript's cwd is an address, and a moved session's address is
    wherever it now lives. Returns the number of lines rewritten.

    `expect` = (st_size, st_mtime_ns) from when the caller last looked: re-checked at the
    last instant before the replace, and a file that changed since is left untouched
    (OSError, tmp removed). A still-active writer appending mid-rewrite must lose nothing
    (the torn-write window shrinks from the whole rewrite to one syscall).

    The mtime is preserved: re-addressing is machinery, not activity, but a fresh mtime
    reads as a growing transcript to the liveness observer, which previously stamped a
    just-relocated seat live and then refused its own office as occupied for the decay
    window. The rule that a pulse must be earned, never granted, extends to files: a pulse
    is earned by words, never granted by a rewrite."""
    tmp = path.with_name("." + path.name + ".heal-tmp")
    rewritten = 0
    try:
        orig_stat = path.stat()
        with path.open(encoding="utf-8", errors="replace") as src, \
                tmp.open("w", encoding="utf-8") as dst:
            for line in src:
                stripped = line.strip()
                if stripped:
                    try:
                        obj = json.loads(stripped)
                    except ValueError:
                        obj = None
                    if (isinstance(obj, dict) and isinstance(obj.get("cwd"), str)
                            and obj["cwd"] != new_cwd):
                        obj["cwd"] = new_cwd
                        dst.write(json.dumps(obj, ensure_ascii=False,
                                             separators=(",", ":")) + "\n")
                        rewritten += 1
                        continue
                dst.write(line)
        if not rewritten:
            tmp.unlink()
            return 0
        if expect is not None:
            st = path.stat()
            if (st.st_size, st.st_mtime_ns) != expect:
                tmp.unlink()
                raise OSError(f"{path.name} changed while being re-addressed, "
                              "aborted; the live file's contents are untouched")
        os.utime(tmp, ns=(orig_stat.st_atime_ns, orig_stat.st_mtime_ns))
        tmp.replace(path)
    except OSError:
        with contextlib.suppress(OSError):
            tmp.unlink()
        raise
    return rewritten


def heal_slug_transcripts(
    cwd: str, *, projects_root: Path | None = None,
    skip_sids: set[str] | frozenset[str] = frozenset(),
    skip_sid_prefixes: set[str] | frozenset[str] = frozenset(),
    quiet_secs: int = _HEAL_QUIET_SECS,
) -> dict[str, Any]:
    """Self-healing resume (caught during a seat transition test): the harness lists a
    session under whichever slug directory its .jsonl sits in, but validates resume against
    the `cwd` recorded inside its lines, so a transcript moved between slugs (rebind,
    extraction, any hand) stays listed but refuses to resume ('This conversation is from a
    different directory'). Runs at every automount: any transcript listed under this cwd
    whose internal address disagrees is re-addressed to point here. Drift converges at the
    next launch in the directory, however the drift happened: this is part of the system,
    never a one-time patch.

    Guards: a live session's transcript is its harness process's own file, never write under
    it. The mounting session itself (`skip_sids`, full session ids) and every session id a
    live-pulse mount anchors (`skip_sid_prefixes`, the jobs/<sid[:8]> basenames) are skipped;
    so is anything written within `quiet_secs` (an open tab appends, so silence is the window
    a heal may use; deferred files converge on a later launch). Fail-soft per file: one
    unreadable transcript lands in the result, never blocks the rest."""
    root = projects_root or (Path.home() / ".claude" / "projects")
    # the split-brain fix rides every launch: anything a legacy-scheme slug still holds
    # for this cwd folds into the canonical dir before the sweep reads it
    with contextlib.suppress(OSError):
        converge_legacy_slug(cwd, projects_root=root)
    slug_dir = root / _harness_slug(cwd)
    if not slug_dir.is_dir():
        return {}
    healed: dict[str, int] = {}
    healed_paths: dict[str, str] = {}
    skipped_live = deferred_fresh = 0
    errors: list[str] = []
    now = time.time()
    for entry in sorted(slug_dir.glob("*.jsonl")):
        if not entry.is_file():
            continue
        sid = entry.name[: -len(".jsonl")]
        try:
            probe = _transcript_cwd_probe(entry)
            if probe is None or probe == cwd:
                continue  # converged (or addressless): the common case, probe-cheap
            if sid in skip_sids or any(sid.startswith(p) for p in skip_sid_prefixes if p):
                skipped_live += 1
                continue
            st = entry.stat()
            if now - st.st_mtime < quiet_secs:
                deferred_fresh += 1
                continue
            n = _rewrite_transcript_cwd(entry, cwd,
                                        expect=(st.st_size, st.st_mtime_ns))
            if n:
                healed[sid[:8]] = n
                healed_paths[sid] = str(entry)
        except OSError as e:
            errors.append(f"{entry.name}: {str(e)[:120]}")
    out: dict[str, Any] = {}
    if healed:
        out["healed"] = healed
        # full anchor session id -> resolved path, beside `healed`'s own 8-char-prefix keys.
        # `healed` itself stays exactly as every existing caller/test already reads it
        # (tests/test_rebind.py's own result-shape assertions); a caller that needs to
        # target the registered-agent store's own (harness, anchor_sid) key, never a prefix,
        # and the file to re-ingest, reads this instead, never re-deriving `_harness_slug`'s
        # own path convention a second time.
        out["healed_paths"] = healed_paths
    if skipped_live:
        out["skipped_live"] = skipped_live
    if deferred_fresh:
        out["deferred_fresh"] = deferred_fresh
    if errors:
        out["errors"] = errors
    return out


def resumed_anchor(job_dir: str, *, jobs_home: Path | None = None) -> str | None:
    """The durable anchor a bridged resume continues: read from the harness's own result.
    The session-picker / daemon-backend resume (ctrl+a) mints a new job for the continued
    conversation (state.json: sessionId=<new>, resumeSessionId=<old>, backend=daemon) and
    presents the new anchor on every hook-stamped call, while the registry only ever knew
    the old one, so each per-request re-attach from the resumed tab bounced with a terminal
    error (witnessed live: a session presented a new job directory over a registry that knew
    the old one). jobs/<sid8>/state.json is the one place the harness records the pair.
    Returns the resumed session's job_dir, or None (not a resume job, or nothing legible;
    this is a hint, never a verdict)."""
    p = Path(job_dir)
    home = jobs_home or p.parent
    try:
        data = json.loads((home / p.name / "state.json").read_text())
    except (OSError, ValueError):
        return None
    resumed = data.get("resumeSessionId")
    if not isinstance(resumed, str) or len(resumed.strip()) < 8:
        return None
    return str(home / resumed.strip().lower()[:8])


def stale_recollection(
    job_dir: str, declared_cwd: str, row_cwd: str, *, projects_root: Path | None = None,
) -> bool:
    """Is a re-mount's declared cwd a stale memory of home? A resumed agent re-mounting
    after a bounce quotes its own conversation history, and an address is exactly what a
    move makes stale (one session re-mounted itself at a demolished former directory this
    way). Transcript evidence decides, not the clock: the harness writes a session's
    transcript under the slug of the directory it actually runs in (and the resume heal
    re-addresses moved ones), so when the registry row's cwd holds this session's transcript
    and the declared cwd's slug does not, the declaration is a recollection of a former
    home; the harness's observation outranks the agent's memory. Conservative: every other
    combination (both, neither, unreadable) returns False and the declaration stands."""
    root = projects_root or (Path.home() / ".claude" / "projects")
    sid8 = Path(job_dir).name
    if not sid8 or not declared_cwd or not row_cwd:
        return False
    try:
        declared_has = any((root / _harness_slug(declared_cwd)).glob(sid8 + "*.jsonl"))
        row_has = any((root / _harness_slug(row_cwd)).glob(sid8 + "*.jsonl"))
    except OSError:
        return False
    return row_has and not declared_has


def _readdress(landed: list[Path], new_cwd: str) -> dict[str, Any]:
    """Re-address the transcripts a move just landed (per-file fail-soft, result honest)."""
    files = lines = 0
    errors: list[str] = []
    for p in landed:
        try:
            st = p.stat()
            n = _rewrite_transcript_cwd(p, new_cwd,
                                        expect=(st.st_size, st.st_mtime_ns))
        except OSError as e:
            errors.append(f"{p.name}: {str(e)[:120]}")
            continue
        if n:
            files += 1
            lines += n
    out: dict[str, Any] = {}
    if files:
        out["cwd_readdressed"] = {"files": files, "lines": lines}
    if errors:
        out["cwd_readdress_errors"] = errors
    return out


def migrate_harness_metadata(
    old_cwd: str, new_cwd: str, *, projects_root: Path | None = None,
    claude_json: Path | None = None, only_sids: set[str] | None = None,
) -> dict[str, Any]:
    """The Claude Code adapter half of a rebind (arbitrary directory moves must be a
    non-event: the metadata and pointers have to move too). The harness keys two stores on
    the absolute cwd, and a folder move orphans both:

      * `~/.claude/projects/<slug>`, the transcripts. Orphaned, they break resume, the
        startup routine's cwd-locate, the fork archaeology, and the miner: the whole
        harness-side memory strands at the dead path. Merged old→new, never clobbering
        (both sides of a fracture may hold real sessions).
      * `~/.claude.json`'s `projects` map: trust, allowedTools, MCP approvals. Re-keyed
        old→new only when the new path has no entry of its own (never overwrite state the
        new path already earned); written atomically (tmp+rename) because the live harness
        rewrites this file too.

    Best-effort by design: every failure lands in the result as a string, never an
    exception, since the graph half of a rebind must not unwind because the harness half
    stumbled. `projects_root`/`claude_json` are test injection points.

    `only_sids` is extraction mode: moving a seat out of a shared cwd (into its Osiris
    office) must take only that seat's own lineage transcripts; a wholesale slug move would
    steal the co-resident repo sessions' history and break their resume mid-tab. Top-level
    entries (files and directories; the session directory holds subagents/ + tool-results/,
    session state as much as the .jsonl) whose name starts with one of the session ids move,
    and the slug's memory/ follows the seat (the auto-memory was the seat's knowledge, and
    an office booting blind defeats the office; a repo slug regrows repo-scoped memory if it
    ever needs its own); everything else stays; the old dir is never removed (it is still a
    living project's slug); the ~/.claude.json entry is not re-keyed (the old path remains a
    real working project; the office earns its own entry at first launch).

    Every transcript either mode moves gets its per-line `cwd` re-addressed to `new_cwd`
    (the harness validates resume against that field, so a moved file that keeps its old
    address stays listed but refuses to resume): exactly the files this move itself
    relocated, never a co-resident's."""
    out: dict[str, Any] = {}
    root = projects_root or (Path.home() / ".claude" / "projects")
    # both sides converge from any legacy-scheme dir first, so a move reads whole truth
    with contextlib.suppress(OSError):
        converge_legacy_slug(old_cwd, projects_root=root)
        converge_legacy_slug(new_cwd, projects_root=root)
    old_dir = root / _harness_slug(old_cwd)
    new_dir = root / _harness_slug(new_cwd)
    if only_sids is not None:
        out["mode"] = "extraction: the shared slug stays; only the seat's own moved"
        out["transcripts_moved"] = 0
        try:
            if old_dir.is_dir() and old_dir != new_dir:
                moved = left = 0
                landed: list[Path] = []
                new_dir.mkdir(parents=True, exist_ok=True)
                for entry in sorted(old_dir.iterdir()):
                    if not any(entry.name.startswith(s) for s in only_sids):
                        continue
                    target = new_dir / entry.name
                    if target.exists():
                        left += 1
                    else:
                        entry.rename(target)
                        moved += 1
                        if target.is_file() and target.suffix == ".jsonl":
                            landed.append(target)
                mem_old, mem_new = old_dir / "memory", new_dir / "memory"
                if mem_old.is_dir() and not mem_new.exists():
                    mem_old.rename(mem_new)
                    out["memory"] = "moved with the seat"
                elif mem_old.is_dir():
                    out["memory"] = "left in place, the destination has its own"
                out["transcripts_moved"] = moved
                if left:
                    out["transcripts_left_behind"] = left
                out.update(_readdress(landed, new_cwd))
        except OSError as e:
            out["transcripts_error"] = str(e)[:200]
        return out
    try:
        if old_dir.is_dir() and old_dir != new_dir:
            moved, left, landed = _merge_dir(old_dir, new_dir)
            with contextlib.suppress(OSError):
                old_dir.rmdir()  # only an EMPTIED husk is removed
            out["transcripts_moved"] = moved
            if left:
                out["transcripts_left_behind"] = left
            out.update(_readdress(landed, new_cwd))
        else:
            out["transcripts_moved"] = 0
    except OSError as e:
        out["transcripts_error"] = str(e)[:200]
    cj = claude_json or (Path.home() / ".claude.json")
    try:
        if cj.is_file():
            data = json.loads(cj.read_text())
            projects = data.get("projects")
            if isinstance(projects, dict) and old_cwd in projects:
                if new_cwd in projects:
                    out["project_state"] = ("left in place, the new path already has its "
                                            "own entry")
                else:
                    projects[new_cwd] = projects.pop(old_cwd)
                    tmp = cj.parent / (cj.name + ".osiris-rebind-tmp")
                    tmp.write_text(json.dumps(data, indent=2))
                    tmp.replace(cj)
                    out["project_state"] = "re-keyed to the new path"
            else:
                out["project_state"] = "no entry for the old path"
    except (OSError, ValueError) as e:
        out["project_state_error"] = str(e)[:200]
    return out


async def rebind_seat(
    actions: Actions, *, seat_or_agent: str, new_cwd: str, actor: str | None = None,
    projects_root: Path | None = None, claude_json: Path | None = None,
    extract: bool = False, force: bool = False, because: str | None = None,
    agents_json: Any = None, read_exe: Any = None, read_cwd: Any = None,
    office_root: Path | None = None,
) -> dict[str, Any]:
    """Move a seat's whole footprint: mount rows, harness metadata, the `.osiris` pin,
    preserving identity, lineage, attribution, and mail. Mints nothing: no new Agent, no
    handle or lineage edge is touched here.

    The anchor invariant: `anchor_cwd` is identity, always `<office_root>/<handle>`,
    derived, never a caller-supplied path. Relocating where a seat's work happens is
    `bind_seat_tree`'s job (`tree_cwd`), not this action's. Root-caused live: two seats each
    broke their own anchor by calling this action on themselves with their own just-observed
    cwd (evidence_class=self_declared, source_id = each seat's own live generation, not a
    daemon, not an outside caller) at the exact millisecond their session's cwd moved. The
    action itself was the trap, not a misuse. So `new_cwd` outside the office root
    (`offices._default_office_root()`) moves everything this call still owns (mounts,
    harness metadata, the pin) but the `anchor_cwd` assertion is skipped, not written; the
    result says so and names `bind_seat_tree` as the action for a genuine tree move. A
    `new_cwd` inside the office root (establish_office's own setup, always `str(office)`) is
    unaffected: this is a no-op distinction for every existing correct caller.

    This also carries the harness half (`migrate_harness_metadata`): the transcripts
    directory and the ~/.claude.json project entry follow the move, so `mv` + `rebind_seat`
    together make a folder move a complete non-event for mounts/harness/mail; graph, mail,
    attribution, resume, and the startup routine's archaeology all keep working from the new
    path, even when it lands outside the office root and therefore never touches
    `anchor_cwd`.

    (a) resolve `seat_or_agent`: a claimed name (`resolve_handle`) or a raw agent id (an
        explicit id is intent, so a dead or unclaimed seat can still be moved).
    (b) read the seat's durable project label (`project_of`: pin→charter→lineage works_in,
        never house). This used to be `house_of`'s raw Agent.project stamp, carried forward
        verbatim, including a fabricated one: a rebind of a seat whose project equaled its
        handle at mint time would perpetuate that fabrication
        into the new location's pin forever, exactly the class this fix closes. `project_of`
        resolves the seat's real project instead (or homeless, a legal answer) rather than
        copying whatever the mint-time stamp happened to say.
    (c) write/refresh `new_cwd/.osiris` pinning that label.
    (d) re-point the whole lineage's durable `agent_mounts` rows (the `cwd` column) at
        `new_cwd`, not just the live holder's, or an earlier generation's row resurrects at
        the old path the instant anything reads it by job_dir.
    (e) stamp a self-declared `anchor_moved` assertion on the Agent: the move is on the
        record, not only in the filesystem, and in the mover's name (`actor`, the mounted
        caller). A rebind is one agent's act on another seat, so the record must say whose
        hand moved it, never pretend the seat moved itself.

    The seat-direct path: a seat nobody has ever claim_name'd resolves to no agent at all
    (an unclaimed handle matches neither a claimed handle nor an Agent canonical), so (a)'s
    existing resolution used to refuse outright, and even when some agent id did resolve (a
    dead one-off session with no `holds` link), the seat-anchor write at the end silently
    no-op'd behind `held_seat`'s graph-link lookup while the result still read like success.
    An explicit seat identifier (its own canonical, or its `handle` property) is checked
    directly whenever agent resolution can't supply one, and a hit there covers a seat
    instead of an agent: (b)-(e) are all agent-lineage concepts (mounts, harness, an
    `anchor_moved` stamp) with nothing to act on for a seat that was never occupied, so this
    path writes only `.osiris` + the Seat's own `anchor_cwd`, using the seat's own derived
    house rather than an agent's.

    Refuses loudly (an error dict, nothing written) when `seat_or_agent` resolves to nobody
    at all, neither an agent nor a seat: an unknown seat is never a silent no-op.

    The liveness guard: self (the caller's own lineage matches the target's) stays exactly
    as open as before this guard existed, since establish_office's own onboarding step
    depends on this, its target being definitionally live at that instant. Third-party (a
    different lineage) acting on a target a harness-confirmed live session currently
    occupies refuses by default; `force=True` (requires `because`, the same rule every
    repair action here follows) is the deliberate override. A cold or never-yet-claimed
    target is unaffected either way: this guard fires only on genuine live occupancy, never
    on bare seat status."""
    seat_or_agent = (seat_or_agent or "").strip()
    from src.orchestrator.offices import _default_office_root

    root = office_root or _default_office_root()
    try:
        # PurePath only, no .resolve()/stat: this must not touch disk on an async primary
        # code path (ASYNC240). A pure string/segment comparison is exactly what the
        # invariant needs: anchor_cwd's own correctness is about the declared path, not a
        # symlink-resolved one.
        anchor_ok = Path(new_cwd) == root or root in Path(new_cwd).parents
    except (OSError, ValueError):
        anchor_ok = False
    anchor_skip_note = (
        None if anchor_ok else
        f"anchor_cwd left untouched, {new_cwd!r} is outside the office root ({root}); "
        "anchor_cwd is identity, always <office_root>/<handle>, never a caller-supplied "
        "path. Mounts/harness metadata still moved to new_cwd below. "
        "Use bind_seat_tree if this is a work-tree relocation, not an identity one."
    )
    from src.orchestrator.agents import _generation, project_of, resolve_handle
    from src.orchestrator.seats import derive_house

    agent_id = await resolve_handle(actions, seat_or_agent) if seat_or_agent else None
    if agent_id is None and seat_or_agent:
        # not a claimed name (or nobody holds it): a raw id is its own intent, so accept it
        # only if the Agent object genuinely exists (never invent one; that is mint territory).
        exists = await actions.pool.fetchval(
            "SELECT 1 FROM objects WHERE canonical=$1 AND type='Agent' AND status='active'",
            seat_or_agent)
        agent_id = seat_or_agent if exists else None
    direct_seat_id: str | None = None
    if seat_or_agent:
        direct_seat_id = await actions.pool.fetchval(
            "SELECT o.canonical FROM objects o WHERE o.type='Seat' AND o.status='active' "
            "AND (o.canonical=$1 OR EXISTS (SELECT 1 FROM current_assertions a "
            "WHERE a.object_id=o.id AND a.name='handle' AND a.value #>> '{}' = $1))",
            seat_or_agent)
    if agent_id is None and direct_seat_id is None:
        return {"error": f"no such seat or agent: {seat_or_agent!r}, unknown to the graph; "
                         "a rebind never silently no-ops on a name nobody holds"}
    if agent_id is None:
        # pure seat path: no claimed occupant, so no agent lineage to repoint at all; the
        # seat's own record is the whole ask. direct_seat_id is guaranteed set here (the
        # refusal above already ruled out both being None).
        assert direct_seat_id is not None
        label = await derive_house(actions.pool, direct_seat_id)
        if not label:
            return {"error": f"{direct_seat_id} has no derivable house to preserve, nothing "
                             "to anchor a project label against"}
        osiris_path = _write_osiris_file(new_cwd, label)
        now = datetime.now(UTC)
        soid = await actions.create_or_find_object("Seat", direct_seat_id,
                                                   actor or direct_seat_id)
        if anchor_ok:
            await actions.assert_singular_property(
                soid, "anchor_cwd", new_cwd, actor or direct_seat_id, now, _CONF,
                evidence_class=_EC)
        note = (
            f"{direct_seat_id}'s anchor moved to {new_cwd}, no claimed agent for "
            "this seat, so only the seat's own record was written (nothing to "
            "repoint in agent_mounts/harness for a lineage that never existed)"
            if anchor_ok else anchor_skip_note)
        return {
            "seat": direct_seat_id, "project": label, "new_cwd": new_cwd,
            "osiris_written": osiris_path, "note": note,
        }
    label = await project_of(actions.pool, agent_id)
    if not label:
        return {"error": f"{agent_id} has no durable project label to preserve, it has never "
                         "been mounted in a project, so there is no anchor to move"}
    base = _generation(agent_id)[0]
    if force and not (because or "").strip():
        return {"error": "force=True requires because, a forced rebind of a live seat is "
                         "not self-justifying"}
    if not force:
        from src.orchestrator.agents import is_occupied_by_a_live_body
        if await is_occupied_by_a_live_body(
            actions.pool, agent_id, agents_json=agents_json, read_exe=read_exe,
            read_cwd=read_cwd,
        ):
            actor_base = _generation(actor)[0] if actor else None
            if actor_base != base:
                return {"error": f"{agent_id} is occupied by a live process right now, and the "
                                 f"caller ({actor!r}) is not that same lineage, refusing a "
                                 "THIRD-PARTY rebind of a live seat (self stays open, "
                                 "third-party-on-live refuses by default). Pass force=True "
                                 "with a because to override, self-rebind (the caller IS "
                                 "this lineage) never needs this",
                        "occupied": True, "target_lineage": base,
                        "caller_lineage": actor_base}
    old_cwd = await actions.pool.fetchval(
        "SELECT cwd FROM agent_mounts WHERE agent_id=$1 OR agent_id LIKE $1 || '-%' "
        "ORDER BY last_seen DESC NULLS LAST LIMIT 1", base)
    osiris_path = _write_osiris_file(new_cwd, label)
    tag = await actions.pool.execute(
        "UPDATE agent_mounts SET cwd=$2 WHERE agent_id=$1 OR agent_id LIKE $1 || '-%'",
        base, new_cwd)
    rows_updated = int(tag.rsplit(" ", 1)[-1])
    # a wholesale move moves everyone: when the directory itself has moved or died, every
    # row anchored there is stale, whatever its seat. Re-pointing only the target lineage
    # left co-residents' rows aimed at a dead path, and a housemate's co-agent panel
    # rendered another agent as "working from" the demolished directory. Extraction never
    # does this: the seat leaves, the co-residents genuinely stay.
    co_repointed = 0
    if not extract and old_cwd and old_cwd != new_cwd:
        tag2 = await actions.pool.execute(
            "UPDATE agent_mounts SET cwd=$2 WHERE cwd=$1", old_cwd, new_cwd)
        co_repointed = int(tag2.rsplit(" ", 1)[-1])
    # extraction: moving a seat out of a shared cwd takes only its own lineage's sessions,
    # session ids from the lineage's session assertions plus its mount-row anchors (either
    # alone can miss a member).
    only_sids: set[str] | None = None
    if extract:
        srows = await actions.pool.fetch(
            "SELECT DISTINCT a.value #>> '{}' AS sid FROM current_assertions a "
            "JOIN objects o ON o.id=a.object_id WHERE a.name='session' AND o.type='Agent' "
            "AND (o.canonical=$1 OR o.canonical LIKE $1 || '-%')", base)
        jrows = await actions.pool.fetch(
            "SELECT job_dir FROM agent_mounts WHERE agent_id=$1 OR agent_id LIKE $1 || '-%'",
            base)
        only_sids = ({str(r["sid"]) for r in srows if r["sid"]}
                     | {Path(r["job_dir"]).name for r in jrows})
    # a registry-less lineage still has a home: a seat whose generations all predate the
    # mount registry, or only ever registered as anonymous startup rows, reads old_cwd=None
    # here, and the harness half below would silently skip: an office minted while the
    # whole agent stayed in the old slug. The transcripts themselves are the address
    # authority: derive the anchor from where the lineage's session ids actually live.
    old_cwd_evidence = None
    if extract and not old_cwd and only_sids:
        old_cwd = lineage_cwd_evidence(only_sids, projects_root=projects_root)
        if old_cwd:
            old_cwd_evidence = "transcript-location (no mount row for the lineage)"
    # the harness half: transcripts + the .claude.json project entry follow the move
    # (best-effort: its failures land in the result, never unwind the graph half above)
    harness = (migrate_harness_metadata(old_cwd, new_cwd, projects_root=projects_root,
                                        claude_json=claude_json, only_sids=only_sids)
               if old_cwd and old_cwd != new_cwd else {})
    now = datetime.now(UTC)
    # the Seat object's anchor follows: the daemon launches at the office. A live `holds`
    # link is the common case, but it is not the only source of truth: `direct_seat_id`
    # (computed above from `seat_or_agent` itself) covers an agent that resolved fine yet
    # holds no seat at all, a case where the result used to just skip in silence.
    from src.orchestrator.seats import held_seat
    bound = await held_seat(actions.pool, agent_id)
    seat_to_anchor = bound["seat_id"] if bound else direct_seat_id
    if seat_to_anchor and anchor_ok:
        soid = await actions.create_or_find_object("Seat", seat_to_anchor,
                                                   actor or agent_id)
        await actions.assert_singular_property(
            soid, "anchor_cwd", new_cwd, actor or agent_id, now, _CONF,
            evidence_class=_EC)
    a = await actions.create_or_find_object("Agent", agent_id, agent_id)
    await actions.assert_property(
        a, "anchor_moved", f"{old_cwd or '?'} → {new_cwd}", actor or agent_id, now, _CONF,
        evidence_class=_EC)
    return {
        "agent": agent_id, "project": label, "old_cwd": old_cwd, "new_cwd": new_cwd,
        "mount_rows_updated": rows_updated, "osiris_written": osiris_path,
        "seat": seat_to_anchor,
        **({"seat_anchor_skipped": True} if not seat_to_anchor else {}),
        **({"anchor_cwd_skipped": anchor_skip_note} if seat_to_anchor and not anchor_ok else {}),
        **({"old_cwd_evidence": old_cwd_evidence} if old_cwd_evidence else {}),
        **({"co_resident_rows_repointed": co_repointed} if co_repointed else {}),
        **({"harness": harness} if harness else {}),
        "note": (
            f"{label}'s anchor moved to {new_cwd}, identity, lineage, attribution, and "
            "mail all key on the label, untouched by this move; the harness metadata "
            "(transcripts, project state) moved with it"
            if anchor_ok else
            f"{label}'s mount/harness footprint moved to {new_cwd}, but its anchor_cwd was "
            "NOT touched, see anchor_cwd_skipped")
            + ("" if seat_to_anchor else ", NO SEAT ANCHORED: this agent holds no seat "
               "and seat_or_agent didn't name one directly either, so the graph still "
               "has no office on record for it"),
    }
