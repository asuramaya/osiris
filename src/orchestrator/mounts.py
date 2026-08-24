"""The durable half of the mount registry — identity that survives a server bounce.

The MCP server's in-memory registry dies with the process, and the process dies routinely
(deploy restarts, an OOM-kill — diagnosed 56f6a0d6): every bounce wiped the WHOLE fleet's
mounts at once, and each agent rediscovered it by a hard "mount(cwd) first" failure mid-work.
This table is the memory the dict doesn't have: mount() upserts here, and any later call that
misses the dict can RE-ATTACH by the client's job_dir (presented per-request via the
X-Osiris-Job header) instead of failing until the agent notices.

Keyed by job_dir — the one durable handle the client re-presents. A mount without a job_dir
has nothing to re-attach by, so it stays memory-only (exactly the old behavior, degraded
gracefully). session_key is informational (which MCP session last touched the mount), never a
lookup key: a reconnecting client gets a fresh session id, so it can't be one.
"""
from __future__ import annotations

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

# THE GREET LEDGER (the resume race, Alfred's field report msgs 717/718, 2026-07-19):
# a window resume fires the predecessor's SessionEnd and the successor's SessionStart
# CONCURRENTLY, and when the end lands second (observed live: automount 20:03:03,
# session-end 20:03:04) it deleted the door the greeting had just seated — the seat then
# answered every probe {live:false, last_seen:NULL} until the next human act, and the
# poke lane read the dead registry and skipped the window while a build order sat unread.
# Both handlers live in one process, so the discriminator is this module-level stamp:
# automount notes each greeting; session-end YIELDS when the same sid was greeted within
# the grace. Yielding is the safe side of the asymmetry — a wrongly-kept door is reaped
# by the census sweep within ~2 minutes; a wrongly-killed door blinds probes and pokes
# until a human types into the window.
_GREETS: dict[str, float] = {}
_GREET_GRACE_SECS = 10.0


def note_greeting(session_id: str) -> None:
    """Stamp a session's greeting (automount's server half) for the resume-race yield."""
    sid8 = (session_id or "").strip().lower()[:8]
    if len(sid8) < 8:
        return
    now = time.monotonic()
    _GREETS[sid8] = now
    if len(_GREETS) > 256:  # bounded: anything past the grace is dead weight
        for k, t in list(_GREETS.items()):
            if now - t > _GREET_GRACE_SECS:
                _GREETS.pop(k, None)


def greeted_within_grace(session_id: str, *, grace: float = _GREET_GRACE_SECS) -> bool:
    """Did a greeting for this sid land within the grace? session-end's yield check."""
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
    last_seen — the fleet's liveness signal for the listener probe). Returns the PREVIOUS
    last_seen (None on first mount) — the anchor for the while-you-were-away fold: everything
    that happened in this lineage's name between its last sign of life and this re-entry.

    `alive=False` SEATS WITHOUT A PULSE — the provisional mount, and it exists because of the
    GHOST (Anubis XII, msg 424). Claude Code fires SessionStart for processes that are not
    anybody: `claude bg-spare` pre-warms, pty hosts, claim-socket daemons. Each has a real
    session id and a real cwd, so the whisper seats it, and `last_seen=now()` handed it a
    HEARTBEAT — which made it live by every test the fleet has. It inflated the roster, it made
    the co-agent warning cry wolf on an uncontended tree, and it could take delivery of a DM into
    a process that will never read anything.

        A HEARTBEAT MUST BE EARNED BY AN ACT, NEVER GRANTED BY A GREETING.

    So the whisper seats you (your identity is ready the moment you exist) but does not certify
    you as living. A real session proves itself within seconds — its first Osiris call bumps this
    row, or its transcript grows and observe_liveness stamps it. A spare never does either, and
    lies there with a null pulse, costing nothing and fooling no one.
    """
    return await pool.fetchval(  # type: ignore[no-any-return]
        "WITH old AS (SELECT last_seen FROM agent_mounts WHERE job_dir=$1) "
        "INSERT INTO agent_mounts (job_dir, agent_id, project, cwd, model, session_key, "
        "                          last_seen) "
        "VALUES ($1,$2,$3,$4,$5,$6, CASE WHEN $7 THEN now() END) "
        "ON CONFLICT (job_dir) DO UPDATE SET agent_id=$2, project=$3, cwd=$4, model=$5, "
        "session_key=$6, "
        # a greeting must never REVOKE a pulse either: a re-whispered session that is already
        # proven alive keeps what it earned.
        "last_seen=CASE WHEN $7 THEN now() ELSE agent_mounts.last_seen END "
        "RETURNING (SELECT last_seen FROM old)",
        job_dir, agent_id, project, cwd, model, session_key, alive,
    )


async def release_mounts(pool: asyncpg.Pool, agent_id: str) -> int:
    """Close every durable mount row naming `agent_id` — retire()'s seat release (thread
    b47b3814: Anubis VII held a live seat after its farewell, haunting the fleet chrome and
    the liveness counts). The registry is hot state, not the event-sourced kernel — the
    retirement itself is stamped on the Agent object, so dropping the seat loses no record.
    Exact-id only: a successor re-mounted on the same job_dir has already overwritten the
    row with its own agent_id and is never touched. Returns rows released."""
    tag = await pool.execute("DELETE FROM agent_mounts WHERE agent_id=$1", agent_id)
    return int(tag.rsplit(" ", 1)[-1])


# the "suspended, no genuine sighting" sentinel — a real datetime, not a Postgres literal
# string: asyncpg binds params by Python type before any server-side cast runs, so a bare
# 'epoch' string fails to bind against a timestamptz column.
SUSPENDED_AT = datetime.fromtimestamp(0, tz=UTC)


async def release_session_mounts(
    pool: asyncpg.Pool, *, job_dir: str, session_id: str,
) -> int:
    """THE DOOR-SCOPED RELEASE (the g40-v/g40-vi false-succession incident, 2026-07-17):
    SessionEnd releases the ENDING SESSION's rows — its own anchor row plus any row its
    binding rode elsewhere (a resume anchors at its ancestor's job_dir, marked
    session_key='sid:<its own id>') — and NEVER the whole seat. release_mounts(agent_id)
    here let one closing tab-view delete a LIVING session's anchor row; the wrongly
    emptied registry then read as the seat's death, and the office door — correct on its
    own evidence — minted false successors at thoth's own office (a title-generator stub
    nearly took the throne). A row is an ADDRESS: only the addressed door's death may
    release it. Seat-wide release stays retire()'s — a mind's deliberate farewell.

    NEVER A DELETE (#178 piece a, Thoth dispatch msg 5224): this used to DELETE the row —
    correct for LIVENESS (the row must stop answering any probe immediately, same law as
    before) but wrong for the REGISTRY, because SessionEnd firing does not always mean the
    body is actually gone (the exact resume-race class this function's own docstring
    already guards elsewhere: `greeted_within_grace` catches the ordering race, this catches
    the SURVIVAL case — a daemon re-adopt or a body the harness still lists). Suspends
    instead: `last_seen` flips to the epoch sentinel (constitution #3 — heal with
    compensating events, never DELETE), which reads exactly as dead to `is_live()` (same
    immediate liveness effect the DELETE always had) while the ROW ITSELF survives, findable
    by `find_mount`. A genuine re-adopt or resume then PROMOTES IT BACK the ordinary way —
    `save_mount`'s own `ON CONFLICT (job_dir) DO UPDATE` refreshes `last_seen=now()` on the
    SAME row, no different from any other re-mount. Idempotent: a row already suspended is
    excluded from the receipt (re-suspending nothing is not a release)."""
    sid32 = (session_id or "").replace("-", "").strip().lower()
    n = await pool.fetchval(
        "WITH gone AS (UPDATE agent_mounts SET last_seen=$3 "
        "WHERE (job_dir=$1 OR session_key=$2) AND last_seen IS DISTINCT FROM $3 "
        "RETURNING 1) SELECT count(*) FROM gone",
        job_dir, f"sid:{sid32}", SUSPENDED_AT)
    return int(n or 0)


AgentsJsonFn = Callable[[], Awaitable[list[dict[str, Any]]]]
ProcReadFn = Callable[[int], str | None]


async def registry_census(
    pool: asyncpg.Pool, *, agents_json: AgentsJsonFn | None = None,
    read_exe: ProcReadFn | None = None, read_cwd: ProcReadFn | None = None,
) -> dict[str, Any]:
    """THE REGISTRY+/PROC CENSUS (#178 piece c, Thoth dispatch msg 5224) — the harness's
    own live-body list (`claude agents --json`, trigger.py's `_claude_agents_json`, reused
    verbatim rather than reinvented), each row VERIFIED against `/proc` (census.py's
    `_proc_exe`/`_proc_cwd` — a harness row is trusted only once `/proc` confirms the pid
    is really a claude body) — this is what "is a body live right now" answers going
    forward. `agent_mounts` is the CACHE this reconciles against, never a second source of
    truth: `matched` names rows the census confirms are real; `rowless` names live,
    verified bodies with NO agent_mounts row at all (session_id prefix matches nothing) —
    exactly the population #178 pieces (a)/(b) exist to close to zero.

    OCCUPANCY, NOT IDENTITY (Ptah's framing, msg 5219 — the boundary this function must
    never cross): this answers "is a body running, and what does the harness/OS say about
    it" — it never resolves which AGENT LINEAGE holds a seat, never touches `holds` links
    or `claim_name`'s own arbitration. A caller wanting IDENTITY reads the graph
    (seats.py/agents.py); a caller wanting OCCUPANCY reads this. Conflating the two is
    exactly the class of bug the two-body-problem ruling (719ed5b1) and this house's own
    "never let one answer for the other" law both guard against.

    Injectable (`agents_json`/`read_exe`/`read_cwd`) so tests drive this with fakes, same
    seam discipline as census.py's own pgrep/proc functions — the real defaults (harness
    subprocess + real /proc) are imported lazily to avoid a module-load-time cycle between
    mounts.py (imported early, by agents.py among others) and trigger.py/census.py
    (themselves importing agents.py). Fails open on a harness read failure — a census that
    could not run reports `verified: []`, `blind: true`, never a false-empty population
    read as "nothing is live" (same law as census.py's own pgrep=None handling)."""
    from src.orchestrator import census as _census
    from src.orchestrator.trigger import _claude_agents_json

    agents_json = agents_json or _claude_agents_json
    read_exe = read_exe or _census._proc_exe
    read_cwd = read_cwd or _census._proc_cwd
    try:
        rows = await agents_json()
    except (OSError, TimeoutError, ValueError):
        return {"blind": True, "verified": [], "matched": [], "rowless": [],
                "note": "the harness registry read failed — cannot census, not empty"}
    verified: list[dict[str, Any]] = []
    for r in rows:
        sid = str(r.get("sessionId") or "")
        if not sid:
            continue
        pid = r.get("pid")
        exe = read_exe(int(pid)) if isinstance(pid, int) else None
        if not _census._is_claude_body(exe):
            continue  # the harness claims a body; /proc does not confirm it — not counted
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
                            "project": candidates[0]["project"]})
        else:
            rowless.append(v)
    return {"blind": False, "verified": verified, "matched": matched, "rowless": rowless,
            "verified_count": len(verified), "matched_count": len(matched),
            "rowless_count": len(rowless)}


_MOUNT_COLS = (
    "job_dir", "agent_id", "project", "cwd", "model", "model_raw", "session_key",
    "mounted_at", "last_seen", "context_window_size", "seat_id",
)


def _mount_snapshot(row: asyncpg.Record) -> dict[str, Any]:
    """An `agent_mounts` row -> a JSON-safe snapshot. `audit_log.payload` is jsonb and the
    pool's codec is plain `json.dumps` with no datetime default (src/db/pool.py) — the two
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
    """Release ONE mount row that is residue against an already-retired project (task #59
    phase 2, fleet_reconcile.py's drop_ephemeral_test_cwd bucket). Row-scoped by `job_dir`
    — the durable per-row key (`agent_mounts.job_dir` is the table's own ON CONFLICT target)
    — never agent-id-wide: `release_mounts`' own lesson, the g40-v/vi false-succession
    incident, where an agent-id-wide DELETE killed a LIVE sibling session's row. The same
    doctrine `release_session_mounts` already keeps: a row is an ADDRESS, and only the
    addressed door's own death may release it.

    REVERSIBLE AND AUDITED (Thoth's gate, DM 2677 — this was a bare `pool.execute`, no
    audit_log, no object_events; not merely irreversible but UNWITNESSED, worse than "no
    undo" because after it ran there was nothing to reconstruct from, by anyone). The row
    is snapshotted whole into an `audit_log` row (action='drop_dead_project_mount') BEFORE
    the delete, in the same transaction. `audit_log`, not `object_events`, deliberately:
    `object_events.object_id` is NOT NULL, and attaching the witness to a freshly-minted
    Agent object would violate an existing, deliberate invariant this same reaper's own
    test enforces (test_fleet_reconcile.py: "a drop releases the RESIDUE ROW only — no
    Agent object was ever minted here") — a drop must promote nothing, including its own
    witness. Pass the returned `audit_id` to `undrop_dead_project_mount` to replay the row
    back exactly.

    Re-checks `project` at delete time rather than trusting the caller's earlier read — a
    row whose project changed between a sweep's report and this call (re-mounted into a
    live project in the interim) no longer matches and is left untouched, and NOTHING is
    written, audit row included: a witness for a delete that didn't happen would itself be
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
})  # every action that snapshots a row via `_mount_snapshot` before deleting it — same
    # payload shape regardless of which one wrote it, so one inverse undoes all three
    # (Thoth DM 2835: "do not invent a second shape for the same problem")


async def undrop_dead_project_mount(
    actions: Actions, *, audit_id: int, actor: str,
) -> dict[str, Any]:
    """The compensating inverse (task #59's gate, Thoth DM 2677; widened to cover the door
    sweeps per Thoth DM 2835): replays the exact row a `_MOUNT_DROP_ACTIONS` audit_log row
    witnessed, back into `agent_mounts` unchanged. `audit_id` — not `job_dir` — pins exactly
    WHICH drop to undo: the same job_dir can be dropped, re-mounted, and dropped again, and
    a replay must never reach for the wrong generation of that history.

    Refuses LOUDLY (an error dict, nothing written) when: no such audit_log row, or its
    `action` isn't one of `_MOUNT_DROP_ACTIONS` (an undrop never invents a snapshot from an
    unrelated audit entry); the job_dir is already occupied — a live session re-mounted
    there since the drop, so the newer row is the truth and reviving the old one underneath
    it would fork identity, never overwrite.

    PROOF, NOT ASSERTION: the restored row round-trips through the SAME `_mount_snapshot`
    the drop used, so `undrop(...); read; _mount_snapshot(...) == original_snapshot` is a
    literal equality a caller (or test) can check, not an eyeballed match."""
    row = await actions.pool.fetchrow(
        "SELECT action, payload FROM audit_log WHERE id=$1", audit_id)
    if row is None:
        return {"error": f"no audit_log row #{audit_id} — an undrop never invents one"}
    if row["action"] not in _MOUNT_DROP_ACTIONS:
        return {"error": f"audit_log #{audit_id} is a {row['action']!r} record, not one of "
                         f"{sorted(_MOUNT_DROP_ACTIONS)} — nothing to undrop"}
    snap = row["payload"]
    async with actions.pool.acquire() as conn, conn.transaction():
        occupied = await conn.fetchval(
            "SELECT 1 FROM agent_mounts WHERE job_dir=$1", snap["job_dir"])
        if occupied:
            return {"error": f"{snap['job_dir']} is already occupied by a live mount — "
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
    """THE ONE LOOKUP from a harness session id to its mount row (thread a61b6bc7, task
    #33): every per-window surface — statusline, stop hook, live_succession — resolves a
    window's identity through THIS, never its own copy (the vitals law: a copy is a fork
    that forgets it is one). Returns the row (job_dir, agent_id, project, model,
    context_window_size, mounted_at) or None.

    Three lanes, strongest evidence first:
      1. THE ANCHOR NAMED FOR THE SID — the whisper derives ~/.claude/jobs/<sid8> for
         every session it greets, so this covers first lives, resumes, and forks alike.
      2. THE SESSION LEDGER — anchor_sid:<sid8> assertions (handshake.record_session_anchor
         files sid→soul at every whisper for identities the sid alone could not re-derive);
         the owner's lineage's freshest row answers for a window whose durable anchor
         wears another name (a mount(job_dir=<inherited anchor>) session).
      3. SELF-EVIDENT DERIVATION (thread #174, Ptah's gap, 2026-08-17): record_session_anchor
         deliberately never WRITES an anchor_sid entry when the sid already IS the agent's own
         generation-derived identity ("the ledger holds only what a wiped registry could not
         reconstruct") — but that optimization assumed a reader could re-derive it directly,
         and until this lane existed, none could: lane 1 needs job_dir to itself carry the sid
         (false for a `-p --resume` wake, whose job_dir is a FRESH per-wake anchor unrelated to
         the resumed transcript's own sid), and lane 2 finds nothing because nothing was ever
         written to find. Verified live: Ptah (agent:02eaaa7a-iii)'s mount row lives under
         job_dir=jobs/c8a22a05 (the wake's own job anchor) while his transcript file (and his
         own identity) is 02eaaa7a-ea67-4fac-a9fc-d9377c6f8474.jsonl — both prior lanes missed
         him; fleet() read him cold at 23:53 while he was actively working (his own msg 5164
         carries the symptom). This lane performs the SAME re-derivation record_session_anchor's
         skip already assumed was possible, closing the loop without any new write.
    DEAD END, verified 2026-07-19 and recorded so nobody re-walks it: agent_mounts.
    session_key's 'sid:<hex>' is the MCP CONNECTION id (Mcp-Session-Id), never the harness
    sid — it cannot serve this lookup. `db` is any fetchrow-capable handle (pool or
    connection), so the hook scripts can pass their own single connection."""
    sid = (session_id or "").strip().lower()
    if len(sid) < 8:
        return None
    cols = "job_dir, agent_id, project, model, context_window_size, mounted_at"
    # DSH session ids come in TWO shapes (verified live, 2026-08-23): depth-0 ids
    # carry a `session-` prefix, spawned subagent ids are a bare uuid. Either way
    # the mount row keys on the FULL session dir path (job_dir ends with the id),
    # and the sid8 anchor the ledger and self-evident lanes speak is the UUID's
    # first 8, never `session-` (which would collide across every DSH session on
    # the box). Normalized once, here, so every lane below speaks the same grammar.
    # _UUID_RE is REUSED, never re-declared: sessions.py:301 and handshake.py:185 already
    # each carry a copy, and a third would be the second-implementation class this house
    # keeps paying for. Function-local because handshake/sessions both import mounts —
    # module-level would be circular; sessions' own import of mounts is function-local too.
    from src.ingest.sessions import _UUID_RE
    # _UUID_RE is REUSED, never re-declared: sessions.py:301 and handshake.py:185 each
    # already carry a copy and a third would be the second-implementation class. Function-
    # local because both of those import mounts — module-level would be circular.
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
    row = await db.fetchrow(
        f"SELECT {cols} FROM agent_mounts WHERE job_dir LIKE '%/jobs/' || $1 "
        "ORDER BY last_seen DESC NULLS LAST LIMIT 1", sid8)
    if row is not None:
        return row
    owner = await db.fetchval(
        "SELECT o.canonical FROM current_assertions a "
        "JOIN objects o ON o.id=a.object_id AND o.type='Agent' AND o.status='active' "
        "WHERE a.name = 'anchor_sid:' || $1 "
        "ORDER BY a.observed_at DESC LIMIT 1", sid8)
    if owner is None:
        owner = f"agent:{sid8}"  # lane 3: self-evident — falls through to the same
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
    """Pure-string path normalization (no IO — safe beside an event loop): body cwds arrive
    kernel-resolved from /proc, and a symlinked row cwd that misses the cwd witness still
    matches the PROJECT witness (both labels come from the same .osiris walk), so a living
    door never hangs on symlink luck."""
    return os.path.normpath(cwd) if cwd else ""


async def sweep_stale_doors(actions: Actions, *, actor: str) -> int:
    """THE PILE RULE (operator ruling, 2026-07-17: 'the 20+ doors on some i also consider a
    bug'): a row is an ADDRESS, and an agent has ONE last-known address — not a pile of
    corpses back to the first week. SessionEnd deletes a door when it fires; every kill,
    crash, and reboot skips the hook and leaks its row FOREVER, which is where 23-door seats
    came from. This sweep is the standing broom: among rows past the liveness window, keep
    exactly the freshest per agent as its last-known address — and keep even that only for an
    agent the graph still calls active (a fresh row elsewhere already IS the address; a
    demoted claimant, a retired seat, or an objectless stranger holds no address at all).
    Pure SQL to FIND the doomed rows, no OS read — the ghost rule (`sweep_ghost_doors`) is
    the one that needs eyes.

    REVERSIBLE AND AUDITED (thread 45dd4f3c, Thoth DM 2835 — this was a bare bulk DELETE,
    the same unwitnessed-irreversible defect class `drop_dead_project_mount` had before
    e7b30db; higher volume here since this fires every 60s in production). Each doomed row
    is re-fetched `FOR UPDATE` inside its own transaction immediately before it is deleted —
    that lock is what makes the audit_log snapshot and the delete see the identical row, not
    two reads racing a third writer — and snapshotted into `audit_log`
    (action='sweep_stale_doors') via the SAME `_mount_snapshot`/`_MOUNT_COLS` shape
    `drop_dead_project_mount` uses, undoable through the same `undrop_dead_project_mount`.
    A row gone by the time its lock is taken (already released some other way) is simply
    skipped — never counted, never witnessed, since nothing was actually done to it here.
    Returns rows released."""
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
                continue  # already gone since the scan — nothing here to witness or delete
            await conn.execute(
                "INSERT INTO audit_log (action, actor, payload) VALUES ($1,$2,$3)",
                "sweep_stale_doors", actor, _mount_snapshot(row))
            await conn.execute("DELETE FROM agent_mounts WHERE job_dir=$1", d["job_dir"])
            released += 1
    return released


async def sweep_ghost_doors(
    actions: Actions, *, body_cwds: set[str], body_projects: set[str], actor: str,
) -> int:
    """THE GHOST RULE — the late SessionEnd, automated (queue item 5, ruled a bug by the
    operator 2026-07-17: 'fleet 5 but there are only 3 agents up'): a terminal kill skips the
    SessionEnd hook, so the dead tab's row stays 'live' for the full decay window and the
    panel lies for fifteen minutes. The OS knows better RIGHT NOW: a fresh row whose cwd
    holds no claude body AND whose project holds none anywhere (the double gate — an office
    and its governed repo can share a label, and a label quirk must never cost a living
    session its door) is a ghost, released on the spot.

    THE CALLER OWNS THE BLINDNESS CHECK: `census.live_bodies_by_cwd()` returning None means
    'could not look' and this function must simply not be called that tick. Two race guards:
    a GRACE floor (rows pulsed within the last two minutes are too new to judge — a session
    born after the /proc scan must never be read as bodyless), and the delete re-checks
    `last_seen` unchanged, so a row re-pulsed after the fetch survives untouched. A killed
    tab's door thus releases in ~2 minutes instead of decaying for 15.

    REVERSIBLE AND AUDITED (thread 45dd4f3c, Thoth DM 2835 — same defect class
    `drop_dead_project_mount` had before e7b30db, unfixed here until now). The `last_seen`
    re-check now runs as a `FOR UPDATE` fetch inside the same transaction as the snapshot
    and the delete — one lock, one witness, one write, so what `audit_log`
    (action='sweep_ghost_doors') records is provably the row that was actually removed, not
    a belief about it from a moment earlier. Undoable through `undrop_dead_project_mount`.
    Returns rows released."""
    rows = await actions.pool.fetch(
        f"SELECT {', '.join(_MOUNT_COLS)} FROM agent_mounts "
        "WHERE COALESCE(last_seen, mounted_at) >= now() - make_interval(secs => $1) "
        "  AND COALESCE(last_seen, mounted_at) < now() - make_interval(secs => $2)",
        float(_DOOR_WINDOW_SECS), float(_GHOST_GRACE_SECS))
    released = 0
    for r in rows:
        cwd = _normed(r["cwd"])
        project = r["project"] or (Path(cwd).name if cwd else "")
        if cwd in body_cwds or (project and project in body_projects):
            continue
        async with actions.pool.acquire() as conn, conn.transaction():
            row = await conn.fetchrow(
                f"SELECT {', '.join(_MOUNT_COLS)} FROM agent_mounts "
                "WHERE job_dir=$1 AND last_seen IS NOT DISTINCT FROM $2 FOR UPDATE",
                r["job_dir"], r["last_seen"])
            if row is None:
                continue  # re-pulsed (or already gone) since the scan — leave it untouched
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


LIVENESS_WINDOW_MINUTES = 15


def freshest_liveness_ts(
    mount_seen: datetime | None, last_active_iso: str | None,
) -> datetime | None:
    """The ONE liveness timestamp every reader of fleet identity must agree on: freshest
    of the durable mount registry's last_seen and the graph's own self-declared
    last_active testimony. Shared by fleet() and agent_liveness() (ruling 70493925) —
    before this, the listener probe read agent_mounts alone while fleet() also trusted
    last_active, so the SAME live agent could flap live/dead between the two readers
    whenever a mount row went stale but the agent's own graph testimony hadn't. Two
    independent implementations of "is this agent alive" is what produced that; a third
    copy that happens to agree today would only disagree later — so this is the one place
    either signal turns into a timestamp, and both readers now call it."""
    stamps: list[datetime] = []
    if mount_seen is not None:
        stamps.append(mount_seen)
    if last_active_iso:
        try:
            stamps.append(datetime.fromisoformat(last_active_iso))
        except ValueError:
            pass
    return max(stamps) if stamps else None


def is_live(ts: datetime | None, *, now: datetime | None = None) -> bool:
    """True iff `ts` (from freshest_liveness_ts) falls within the shared liveness window."""
    now = now or datetime.now(UTC)
    return ts is not None and now - ts < timedelta(minutes=LIVENESS_WINDOW_MINUTES)


async def agent_liveness(pool: asyncpg.Pool, agent_id: str) -> dict[str, Any]:
    """Is this MIND live right now (for a DM's send receipt)? Lineage-aware — phase 2
    arriving on Alfred's field evidence (msg 718, 2026-07-19): a mount row is an ADDRESS,
    and machinery legitimately re-points addresses between generations (the liveness
    promotion follows the lineage head; greets rewrite agent_id) — so a probe for -iii
    must not read dead because the row momentarily wears another numeral of the same
    soul. The SOUL answers: freshest of the mount registry's last_seen AND the graph's
    own last_active self-testimony, across every generation of the base — the same two
    signals fleet() has always trusted, now read through the same helper (ruling
    70493925: this probe used to be agent_mounts-only, and that gap is what made it flap
    against a live seat fleet() called live the whole time). live = within 15 min."""
    from src.orchestrator.agents import _generation
    base = _generation(agent_id)[0]
    mount_seen = await pool.fetchval(
        "SELECT max(last_seen) FROM agent_mounts "
        "WHERE agent_id=$1 OR agent_id=$2 OR agent_id LIKE $2 || '-%'", agent_id, base)
    last_active_iso = await pool.fetchval(
        "SELECT a.value#>>'{}' FROM current_assertions a "
        "JOIN objects o ON o.id = a.object_id "
        "WHERE a.name='last_active' AND o.type='Agent' "
        "AND (o.canonical=$1 OR o.canonical=$2 OR o.canonical LIKE $2 || '-%') "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", agent_id, base)
    ts = freshest_liveness_ts(mount_seen, last_active_iso)
    return {"live": is_live(ts), "last_seen": ts.isoformat() if ts is not None else None}


async def project_last_seen(pool: asyncpg.Pool, project: str) -> str | None:
    """The freshest mount activity for a project (ISO), for the send() listener probe."""
    v = await pool.fetchval(
        "SELECT max(last_seen) FROM agent_mounts WHERE project=$1", project)
    return v.isoformat() if v is not None else None


async def project_prev_seen(
    pool: asyncpg.Pool, project: str | None, *, exclude_job_dir: str
) -> datetime | None:
    """The LINEAGE's last sign of life, excluding the caller's own (just-upserted) row — the
    while-you-were-away anchor for a FRESH session: a new session id has no past of its own,
    but its project does, and that past is exactly what it must not wake blind to (a sibling's
    tab re-opened as a new session and got NO fold while twins had settled its threads)."""
    if not project:
        return None
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT max(last_seen) FROM agent_mounts WHERE project=$1 AND job_dir <> $2",
        project, exclude_job_dir)


async def live_claimed_sids(
    pool: asyncpg.Pool, *, exclude_session_key: str | None, within_secs: int = 900
) -> set[str]:
    """Session handles currently HELD by a live mount on a DIFFERENT client session — the
    claimed-set the cwd-guess must refuse (two anchorless same-project sessions grabbing the
    hottest transcript would otherwise merge). Lineage-aware: a minted heir (agent:x-ii)
    claims its base handle x too.

    A SEATED SID IS CLAIMED UNTIL RELEASED (Phase D, ruling 5cef856b): a mount row bound to
    a Seat stays claimed regardless of pulse — its holder dying must never make its identity
    GUESSABLE by an anchorless stranger reading the hottest transcript. The liveness window
    guards the living; the binding guards the seated dead (session_end releases the row, so
    a deliberately-closed seat frees its sid the honest way)."""
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
    """The sid[:8] prefixes of mounts with a LIVE pulse — the transcript heal's
    do-not-touch set (the job_dir anchor is ~/.claude/jobs/<sid[:8]>, so its basename IS
    the session prefix). Deliberately pulse-only, unlike `live_claimed_sids`: Phase D's
    seated-forever claim guards identity GUESSING; the heal needs process-liveness — a
    seated but CLOSED session is exactly the one whose transcript must stay healable."""
    rows = await pool.fetch(
        "SELECT job_dir FROM agent_mounts "
        "WHERE last_seen > now() - make_interval(secs => $1)", within_secs)
    return {Path(r["job_dir"]).name for r in rows if r["job_dir"]}


async def fleet_pulse(
    pool: asyncpg.Pool, *, lease_secs: int = 900, live_secs: int = 900
) -> str:
    """One glance line for orient — 'N live · owed X · briefs Y · wakes K/h'. The caller
    omits the key on any failure (the pulse must never slow or crash orient).

    A THIN VIEW over the shared segment authority (surface.py, ruling e9ef7373, thread
    109b6c48 — the render analog of the write-verb receipt law): `live`/`owed`/
    `briefs_total`/`wakes`/`spend` are the fleet-wide-scoped variants (this line has no
    project of its own to narrow to — orient() calls it unscoped). ALL thresholds and
    presentation rules live in surface.py now; this function only picks its five segments
    and formats them the way this line has always read.

    ONE DELIBERATE EXCEPTION, disclosed rather than silently unified: this pulse has NEVER
    gated spend on a threshold — it shows the day's figure whenever spend is metered, full
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
    """What happened in this project's NAME between the lineage's last sign of life and now —
    the anti-face-wearing fold (operator: "the agents have to know, or it falls apart"). A
    returning agent must not have to guess where it stands: WHO acted as its project (wakes
    by lane; other agent ids that sent mail wearing its face) and how its CONVERSATIONS moved
    (per-thread last word + settled state). None when there is no anchor (first mount) or
    nothing happened — the quiet case stays quiet."""
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
    # YOUR SPAWNS — children your lineage delegated to (spawned_by → any generation of your
    # base) since your last sign of life: the parent is TOLD, never surprised (the operator's
    # spawn-provenance complaint, 2026-07-10). Registered live by the spawn hooks, or by the
    # miner's disk round — same keying, so they show here either way.
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
    # THE GHOST-SPAWN LAW (ruling 708a972d): a spawn whose only evidence is the harness's
    # announcement — no transcript ever materialized, no act ever seen — must not wear the
    # 'another hand' warning. It gets named (the record forgets nothing) but rendered as the
    # harness machinery it almost certainly is; the scare is reserved for witnessed hands.
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
             **({"unwitnessed": "ephemeral harness sidechain — announced but never "
                                "observed; likely internal, not another hand"}
                if s["witnessed"] == "false" else {})}
            for s in spawns]} if spawns else {}),
        "note": ("only unwitnessed harness sidechains appeared — announced by the harness, "
                 "but no transcript or act was ever observed; likely internal machinery. "
                 "Nothing else moved in your name." if all_ghost else
                 "another hand may have worn your face here — read this before assuming you "
                 "know where you stand; the graph, not your memory, records these turns"),
    }


# ── SEAT REBIND ──────────────────────────────────────────────────────────────────────────
# `path = project = identity` orphaned alfred when the operator moved his folder (ruling
# dd47c1da) — the fleet's field diagnosis, and the operator is BLOCKED on the cure. A seat's
# identity, lineage, attribution, and mail all key on its DURABLE PROJECT LABEL (the `project`
# assertion; house_of reads it) — never on cwd. Moving the folder should be a non-event; today
# it silently detaches the .osiris pin and strands every durable mount row at the old path.
# Pilot: house bytebye — alfred's seat, a pure office with no code in it.


def _write_osiris_file(new_cwd: str, project_label: str) -> str:
    """Write/refresh `new_cwd/.osiris`, pinning `project_label` — the existing durable
    mechanism (`agents.read_project_label`) that makes a folder rename stop mattering. Reads
    ONLY the file already at `new_cwd` (never walks up — a parent repo's `.osiris` is not this
    seat's business). Every top-level scalar key already declared there survives untouched
    (`model =`, or anything a later fold adds — a rebind must never silently eat a key it
    doesn't recognize); `project =` is always overwritten to the label being pinned. Honest
    limit: TOML comments do not survive a rewrite — the parser never sees them."""
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
    """The harness's transcript-directory name for a cwd. The CURRENT harness (witnessed
    live, 2026-07-16, v2.1.211: ~/.osiris/seats/thoth → -home-asuramaya--osiris-seats-
    thoth) replaces BOTH '/' and '.' with '-'; an earlier scheme kept the dot, and one
    night's ceremonies parked estates under dot-form slugs the new harness cannot see —
    `_legacy_slug` + `converge_legacy_slug` fold those home."""
    return cwd.replace("/", "-").replace(".", "-")


def _legacy_slug(cwd: str) -> str:
    """The OLD scheme ('/' → '-', dots kept) — read-side only, for convergence."""
    return cwd.replace("/", "-")


def converge_legacy_slug(cwd: str, *, projects_root: Path | None = None) -> int:
    """Fold a cwd's legacy dot-form slug dir into its canonical one (never clobbering) —
    the split-brain cure: content the ceremonies moved under the old scheme becomes
    visible to the harness again. Idempotent; returns entries moved; 0 when the schemes
    agree for this cwd or there is nothing legacy."""
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
    """Move every entry old→new, NEVER clobbering (a transcript that exists on both sides
    stays where it is — losing either would falsify the record); one level of recursion
    merges subdirectories (the subagents/ tree). Returns (moved, left_behind, landed) —
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
    """The first top-level `cwd` a transcript carries — the field the harness's resume
    validator reads (witnessed live, thread 39ea074c: /resume refused a moved transcript
    naming its FIRST recorded cwd, not the slug directory it was listed under). None when
    the head carries no cwd at all (a summary-only stub, or unreadable)."""
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
    """Where a lineage's transcripts ACTUALLY live — the anchor authority when the mount
    registry holds no row (a lineage older than the registry, or one that only ever
    registered through per-session whisper rows). Sweeps the projects root for the
    lineage's sid-prefixed transcripts and returns the freshest one's INTERNAL cwd — the
    same doctrine as the recollection guard: transcript evidence decides an address,
    never a remembered or inferred one. None when no sid has a transcript anywhere
    (a truly bodiless lineage — nothing to carry, and nothing is guessed)."""
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
    (tmp + rename, so a crash mid-write never leaves a half-file). ONLY the routing field
    moves — content, trackingPath, and every other byte pass through verbatim: the graph is
    the history; the transcript's cwd is an ADDRESS, and a moved session's address is
    wherever it now lives. Returns the number of lines rewritten.

    `expect` = (st_size, st_mtime_ns) from when the CALLER last looked: re-checked at the
    last instant before the replace, and a file that changed since is left untouched
    (OSError, tmp removed) — an off-the-rails live pen appending mid-rewrite must lose
    NOTHING (the torn-write window shrinks from the whole rewrite to one syscall).

    THE MTIME IS PRESERVED (Atlas's blocked door, 2026-07-17): re-addressing is machinery,
    not life — but a fresh mtime reads as a growing transcript to the liveness observer,
    which stamped a just-ceremonied seat LIVE and its own office door then refused it as
    occupied for the decay window. The heartbeat law extends to files: a pulse is earned
    by words, never granted by a rewrite."""
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
                raise OSError(f"{path.name} changed while being re-addressed — "
                              "aborted; the live pen's words are untouched")
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
    """SELF-HEALING RESUME (the alfred transition test's catch, thread 39ea074c): the
    harness LISTS a session under whichever slug directory its .jsonl sits in, but
    validates RESUME against the `cwd` recorded inside its lines — so a transcript moved
    between slugs (rebind, extraction, any hand) stays listed but refuses to resume
    ('This conversation is from a different directory'). Runs at every automount: any
    transcript listed under THIS cwd whose internal address disagrees is re-addressed to
    point here. Drift converges at the next launch in the directory, however the drift
    happened — the operator's ruling: part of the system, never a one-time patch.

    GUARDS — a live session's transcript is its harness process's own pen, never write
    under it: the mounting session itself (`skip_sids`, full session ids) and every sid a
    live-pulse mount anchors (`skip_sid_prefixes`, the jobs/<sid[:8]> basenames) are
    skipped; so is anything written within `quiet_secs` (an open tab appends — silence is
    the seam a heal may use; deferred files converge on a later launch). Fail-soft per
    file: one unreadable transcript lands in the receipt, never blocks the rest."""
    root = projects_root or (Path.home() / ".claude" / "projects")
    # the split-brain cure rides every launch: anything a legacy-scheme slug still holds
    # for this cwd folds into the canonical dir before the sweep reads it
    with contextlib.suppress(OSError):
        converge_legacy_slug(cwd, projects_root=root)
    slug_dir = root / _harness_slug(cwd)
    if not slug_dir.is_dir():
        return {}
    healed: dict[str, int] = {}
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
                continue  # converged (or addressless) — the common case, probe-cheap
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
        except OSError as e:
            errors.append(f"{entry.name}: {str(e)[:120]}")
    out: dict[str, Any] = {}
    if healed:
        out["healed"] = healed
    if skipped_live:
        out["skipped_live"] = skipped_live
    if deferred_fresh:
        out["deferred_fresh"] = deferred_fresh
    if errors:
        out["errors"] = errors
    return out


def resumed_anchor(job_dir: str, *, jobs_home: Path | None = None) -> str | None:
    """The durable anchor a BRIDGED RESUME continues — read from the harness's own receipt.
    The session-picker / daemon-backend resume (ctrl+a) mints a NEW job for the continued
    conversation (state.json: sessionId=<new>, resumeSessionId=<old>, backend=daemon) and
    presents the NEW anchor on every hook-stamped call — while the registry only ever knew
    the old one, so each per-request re-attach from the resumed tab bounced TERMINAL
    (witnessed live: alfred presented jobs/ceed2d2e over a registry that knew
    jobs/838639d1, thread 90f0cb3a). jobs/<sid8>/state.json is the one place the harness
    records the pair. Returns the RESUMED session's job_dir, or None (not a resume job,
    or nothing legible — this is a hint, never a verdict)."""
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
    """Is a re-mount's declared cwd a STALE MEMORY of home? A resumed mind re-mounting
    after a bounce quotes its own conversation history — and an address is exactly what a
    move makes stale (alfred re-mounted himself at the demolished husk this way, 90f0cb3a).
    TRANSCRIPT EVIDENCE decides, not the clock: the harness writes a session's transcript
    under the slug of the directory it actually runs in (and the resume heal re-addresses
    moved ones), so when the registry row's cwd holds this session's transcript and the
    declared cwd's slug does not, the declaration is a recollection of a former home —
    the harness's observation outranks the mind's memory. Conservative: every other
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
    """Re-address the transcripts a move just landed (per-file fail-soft, receipt honest)."""
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
    """The CLAUDE-CODE-ADAPTER half of a rebind (operator's directive, 2026-07-15: arbitrary
    directory moves must be a NON-EVENT — 'the metadata and pointer has to move also'). The
    harness keys two stores on the ABSOLUTE cwd, and a folder move orphans both:

      * `~/.claude/projects/<slug>` — the transcripts. Orphaned, they break resume, the
        whisper's cwd-locate, the fork archaeology, and the miner: the house's whole
        harness-side memory strands at the dead path. MERGED old→new, never clobbering
        (both sides of a fracture may hold real sessions — ByeByte did).
      * `~/.claude.json`'s `projects` map — trust, allowedTools, MCP approvals. RE-KEYED
        old→new only when the new path has no entry of its own (never overwrite state the
        new path already earned); written atomically (tmp+rename) because the live harness
        rewrites this file too.

    Best-effort by design: every failure lands in the receipt as a string, never an
    exception — the graph half of a rebind must not unwind because the harness half
    stumbled. `projects_root`/`claude_json` are test seams.

    `only_sids` is EXTRACTION MODE (the seat-offices ruling, ed5f5ce2): moving a SEAT out
    of a SHARED cwd (into its Osiris office) must take only that seat's own lineage
    transcripts — a wholesale slug move would steal the co-resident repo sessions' history
    and break their resume mid-tab. Top-level entries (files AND directories — the sid dir
    holds subagents/ + tool-results/, session state as much as the .jsonl) whose name
    starts with one of the sids move, and the slug's memory/ follows the seat (39ea074c:
    the auto-memory was the seat's knowledge; an office booting blind defeats the office —
    a repo slug regrows repo-scoped memory if it ever needs its own); everything else
    stays; the old dir is NEVER removed (it is still a living project's slug); the
    ~/.claude.json entry is NOT re-keyed (the old path remains a real working project —
    the office earns its own entry at first launch).

    Every transcript EITHER MODE moves gets its per-line `cwd` re-addressed to `new_cwd`
    (39ea074c: the harness validates resume against that field, so a moved file that keeps
    its old address stays listed but refuses to resume) — exactly the files this move
    itself relocated, never a co-resident's."""
    out: dict[str, Any] = {}
    root = projects_root or (Path.home() / ".claude" / "projects")
    # both sides converge from any legacy-scheme dir first, so a move reads whole truth
    with contextlib.suppress(OSError):
        converge_legacy_slug(old_cwd, projects_root=root)
        converge_legacy_slug(new_cwd, projects_root=root)
    old_dir = root / _harness_slug(old_cwd)
    new_dir = root / _harness_slug(new_cwd)
    if only_sids is not None:
        out["mode"] = "extraction — the shared slug stays; only the seat's own moved"
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
                    out["memory"] = "left in place — the destination has its own"
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
                    out["project_state"] = ("left in place — the new path already has its "
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
    extract: bool = False,
) -> dict[str, Any]:
    """Move a seat's ANCHOR cwd, preserving identity, lineage, attribution, and mail (`dd47c1da`
    — the fold the operator is blocked on). MINTS NOTHING: no new Agent, no handle or lineage
    edge is touched here — this only re-points where a seat's rows live and re-pins the durable
    label a moved folder would otherwise silently detach from.

    Since the operator's arbitrary-move directive (2026-07-15) this also carries the HARNESS
    half (`migrate_harness_metadata`): the transcripts directory and the ~/.claude.json
    project entry follow the move, so `mv` + `rebind_seat` together make a folder move a
    complete non-event — graph, mail, attribution, resume, and the whisper's archaeology all
    keep working from the new path.

    (a) resolve `seat_or_agent` — a claimed name (`resolve_handle`) or a raw agent id (the
        GRAVE RULE: an explicit id is intent, so a dead or unclaimed seat can still be moved).
    (b) read the seat's DURABLE project label (`house_of` — the `project` assertion mail and
        attribution key on). This value NEVER changes; a rebind that touched it would just be
        the identity-fracture bug wearing a different hat.
    (c) write/refresh `new_cwd/.osiris` pinning that label.
    (d) re-point the WHOLE LINEAGE's durable `agent_mounts` rows (the `cwd` column) at
        `new_cwd` — not just the live holder's, or an earlier generation's row resurrects at
        the old path the instant anything reads it by job_dir.
    (e) stamp a SELF_DECLARED `anchor_moved` assertion on the Agent — the move is on the
        record, not only in the filesystem, and in the MOVER'S name (`actor`, the mounted
        caller): a rebind is a mind's act on another seat, so the record must say whose hand
        moved it, never pretend the seat moved itself.

    THE SEAT-DIRECT PATH (thread 3ae57d36, the GRAVE RULE's other half): a seat nobody has
    ever claim_name'd resolves to NO agent at all — 'grantprobe' matches neither a claimed
    handle nor an Agent canonical — so (a)'s existing resolution used to refuse outright, and
    even when some agent id DID resolve (a dead one-off body with no `holds` link), the seat-
    anchor write at the end silently no-op'd behind `held_seat`'s graph-link lookup while the
    receipt still read like success. An explicit SEAT identifier — its own canonical, or its
    `handle` property — is checked directly whenever agent resolution can't supply one, and a
    hit there IS the grave rule firing for a seat instead of an agent: (b)-(e) are all agent-
    lineage concepts (mounts, harness, an `anchor_moved` stamp) with nothing to act on for a
    seat that was never occupied, so this path writes ONLY `.osiris` + the Seat's own
    `anchor_cwd`, using the seat's OWN derived house rather than an agent's.

    Refuses LOUDLY (an error dict, nothing written) when `seat_or_agent` resolves to nobody at
    all — neither an agent nor a seat — an unknown seat is never a silent no-op."""
    seat_or_agent = (seat_or_agent or "").strip()
    from src.orchestrator.agents import _generation, house_of, resolve_handle
    from src.orchestrator.seats import derive_house

    agent_id = await resolve_handle(actions, seat_or_agent) if seat_or_agent else None
    if agent_id is None and seat_or_agent:
        # not a claimed name (or nobody holds it) — a RAW id is its own intent: accept it only
        # if the Agent object genuinely exists (never invent one; that is mint territory).
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
        return {"error": f"no such seat or agent: {seat_or_agent!r} — unknown to the graph; "
                         "a rebind never silently no-ops on a name nobody holds"}
    if agent_id is None:
        # PURE SEAT PATH: no claimed occupant, so no agent lineage to repoint at all — the
        # seat's own record is the whole ask. direct_seat_id is guaranteed set here (the
        # refusal above already ruled out both being None).
        assert direct_seat_id is not None
        label = await derive_house(actions.pool, direct_seat_id)
        if not label:
            return {"error": f"{direct_seat_id} has no derivable house to preserve — nothing "
                             "to anchor a project label against"}
        osiris_path = _write_osiris_file(new_cwd, label)
        now = datetime.now(UTC)
        soid = await actions.create_or_find_object("Seat", direct_seat_id,
                                                   actor or direct_seat_id)
        await actions.assert_property(soid, "anchor_cwd", new_cwd, actor or direct_seat_id, now,
                                      _CONF, evidence_class=_EC)
        return {
            "seat": direct_seat_id, "project": label, "new_cwd": new_cwd,
            "osiris_written": osiris_path,
            "note": f"{direct_seat_id}'s anchor moved to {new_cwd} — no claimed agent for "
                    "this seat, so only the seat's own record was written (nothing to "
                    "repoint in agent_mounts/harness for a lineage that never existed)",
        }
    label = await house_of(actions.pool, agent_id)
    if not label:
        return {"error": f"{agent_id} has no durable project label to preserve — it has never "
                         "been mounted in a project, so there is no anchor to move"}
    base = _generation(agent_id)[0]
    old_cwd = await actions.pool.fetchval(
        "SELECT cwd FROM agent_mounts WHERE agent_id=$1 OR agent_id LIKE $1 || '-%' "
        "ORDER BY last_seen DESC NULLS LAST LIMIT 1", base)
    osiris_path = _write_osiris_file(new_cwd, label)
    tag = await actions.pool.execute(
        "UPDATE agent_mounts SET cwd=$2 WHERE agent_id=$1 OR agent_id LIKE $1 || '-%'",
        base, new_cwd)
    rows_updated = int(tag.rsplit(" ", 1)[-1])
    # A WHOLESALE MOVE MOVES EVERYONE (Werner's catch, 2026-07-16): when the directory
    # itself has moved or died, EVERY row anchored there is stale, whatever its seat —
    # re-pointing only the target lineage left co-residents' rows aimed at the grave, and
    # a housemate's co-agent panel rendered agent:98ab0ae8 as 'working from' the demolished
    # husk. Extraction never does this: the seat leaves, the co-residents genuinely stay.
    co_repointed = 0
    if not extract and old_cwd and old_cwd != new_cwd:
        tag2 = await actions.pool.execute(
            "UPDATE agent_mounts SET cwd=$2 WHERE cwd=$1", old_cwd, new_cwd)
        co_repointed = int(tag2.rsplit(" ", 1)[-1])
    # EXTRACTION (the seat-offices ruling, ed5f5ce2): moving a seat out of a SHARED cwd
    # takes only its own lineage's sessions — sids from the lineage's session assertions
    # plus its mount-row anchors (either alone can miss a member).
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
    # A REGISTRY-LESS LINEAGE STILL HAS A HOME (the children's rollout, 2026-07-16): a
    # seat whose generations all predate the mount registry — or only ever registered as
    # anonymous whisper rows — reads old_cwd=None here, and the harness half below would
    # silently skip: an office minted while the whole mind stayed in the old slug (all
    # five rollout children were this case). The transcripts themselves are the address
    # authority: derive the anchor from where the lineage's sids actually live.
    old_cwd_evidence = None
    if extract and not old_cwd and only_sids:
        old_cwd = lineage_cwd_evidence(only_sids, projects_root=projects_root)
        if old_cwd:
            old_cwd_evidence = "transcript-location (no mount row for the lineage)"
    # the HARNESS half — transcripts + the .claude.json project entry follow the move
    # (best-effort: its failures land in the receipt, never unwind the graph half above)
    harness = (migrate_harness_metadata(old_cwd, new_cwd, projects_root=projects_root,
                                        claude_json=claude_json, only_sids=only_sids)
               if old_cwd and old_cwd != new_cwd else {})
    now = datetime.now(UTC)
    # the Seat OBJECT's anchor follows — the daemon summons at the office (5cef856b). A LIVE
    # `holds` link is the common case, but it is not the only source of truth: `direct_seat_id`
    # (computed above from `seat_or_agent` itself) covers an agent that resolved fine yet holds
    # no seat at all — thread 3ae57d36's lying receipt, where this used to just skip in silence.
    from src.orchestrator.seats import held_seat
    bound = await held_seat(actions.pool, agent_id)
    seat_to_anchor = bound["seat_id"] if bound else direct_seat_id
    if seat_to_anchor:
        soid = await actions.create_or_find_object("Seat", seat_to_anchor,
                                                   actor or agent_id)
        await actions.assert_property(soid, "anchor_cwd", new_cwd, actor or agent_id, now,
                                      _CONF, evidence_class=_EC)
    a = await actions.create_or_find_object("Agent", agent_id, agent_id)
    await actions.assert_property(
        a, "anchor_moved", f"{old_cwd or '?'} → {new_cwd}", actor or agent_id, now, _CONF,
        evidence_class=_EC)
    return {
        "agent": agent_id, "project": label, "old_cwd": old_cwd, "new_cwd": new_cwd,
        "mount_rows_updated": rows_updated, "osiris_written": osiris_path,
        "seat": seat_to_anchor,
        **({"seat_anchor_skipped": True} if not seat_to_anchor else {}),
        **({"old_cwd_evidence": old_cwd_evidence} if old_cwd_evidence else {}),
        **({"co_resident_rows_repointed": co_repointed} if co_repointed else {}),
        **({"harness": harness} if harness else {}),
        "note": f"{label}'s anchor moved to {new_cwd} — identity, lineage, attribution, and "
                "mail all key on the label, untouched by this move; the harness metadata "
                "(transcripts, project state) moved with it"
                + ("" if seat_to_anchor else " — NO SEAT ANCHORED: this agent holds no seat "
                   "and seat_or_agent didn't name one directly either, so the graph still "
                   "has no office on record for it"),
    }
