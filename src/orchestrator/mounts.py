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

import json
import tomllib
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

_EC = EvidenceClass.SELF_DECLARED.value
_CONF = confidence_for(EvidenceClass.SELF_DECLARED)


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


async def agent_liveness(pool: asyncpg.Pool, agent_id: str) -> dict[str, Any]:
    """Is a SPECIFIC agent live right now (for a DM's send receipt)? Freshest mount by agent_id;
    live = seen within 15 min. Lineage-aware falls to phase 2 — a bare agent_id match for now."""
    v = await pool.fetchval(
        "SELECT max(last_seen) FROM agent_mounts WHERE agent_id=$1", agent_id)
    iso = v.isoformat() if v is not None else None
    from datetime import UTC, datetime, timedelta
    live = bool(v and datetime.now(UTC) - v < timedelta(minutes=15))
    return {"live": live, "last_seen": iso}


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
    claims its base handle x too."""
    rows = await pool.fetch(
        "SELECT agent_id, session_key FROM agent_mounts "
        "WHERE last_seen > now() - make_interval(secs => $1)", within_secs)
    from src.orchestrator.agents import _generation

    out: set[str] = set()
    for r in rows:
        if exclude_session_key and r["session_key"] == exclude_session_key:
            continue
        root, _ = _generation(r["agent_id"])
        out.add(root.removeprefix("agent:"))
    return out


async def fleet_pulse(
    pool: asyncpg.Pool, *, lease_secs: int = 900, live_secs: int = 900
) -> str:
    """One glance line for orient — 'N live · desk M · wakes K/h'. One round trip; the
    caller omits the key on any failure (the pulse must never slow or crash orient). The
    statusline shows the same numbers per-render; this is the same glance for agents whose
    chrome the operator can't see."""
    r = await pool.fetchrow(
        "SELECT "
        " (SELECT count(*) FROM agent_mounts "
        "   WHERE last_seen > now() - make_interval(secs => $2)) AS live, "
        " (SELECT count(*) FROM fleet_messages m WHERE m.to_project='operator' "
        "   AND m.to_agent IS NULL AND m.read_at IS NULL "
        "   AND NOT EXISTS (SELECT 1 FROM message_recipients r "
        "     WHERE r.message_id=m.id AND r.agent_id='operator' AND r.read_at IS NOT NULL) "
        "   AND NOT EXISTS (SELECT 1 FROM message_recipients r WHERE r.message_id=m.id "
        "     AND r.agent_id='operator' AND r.delivered_at >= now() "
        "       - make_interval(secs => $1))) AS desk, "
        " (SELECT count(*) FROM agent_wakes "
        "   WHERE woke_at > now() - interval '1 hour') AS wakes",
        lease_secs, live_secs)
    return f"{r['live']} live · desk {r['desk']} · wakes {r['wakes']}/h"


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
    seat's business) so a `model =` line already declared there survives untouched; `project =`
    is always overwritten to the label being pinned."""
    d = Path(new_cwd)
    d.mkdir(parents=True, exist_ok=True)
    f = d / ".osiris"
    model: str | None = None
    if f.is_file():
        try:
            value = tomllib.loads(f.read_text()).get("model")
            model = str(value).strip() if value else None
        except (OSError, tomllib.TOMLDecodeError, ValueError):
            model = None
    lines = [f"project = {json.dumps(project_label)}"]
    if model:
        lines.append(f"model = {json.dumps(model)}")
    f.write_text("\n".join(lines) + "\n")
    return str(f)


async def rebind_seat(actions: Actions, *, seat_or_agent: str, new_cwd: str) -> dict[str, Any]:
    """Move a seat's ANCHOR cwd, preserving identity, lineage, attribution, and mail (`dd47c1da`
    — the fold the operator is blocked on). MINTS NOTHING: no new Agent, no handle or lineage
    edge is touched here — this only re-points where a seat's rows live and re-pins the durable
    label a moved folder would otherwise silently detach from.

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
        record, not only in the filesystem.

    Refuses LOUDLY (an error dict, nothing written) when `seat_or_agent` resolves to nobody —
    an unknown seat is never a silent no-op."""
    seat_or_agent = (seat_or_agent or "").strip()
    from src.orchestrator.agents import _generation, house_of, resolve_handle

    agent_id = await resolve_handle(actions, seat_or_agent) if seat_or_agent else None
    if agent_id is None and seat_or_agent:
        # not a claimed name (or nobody holds it) — a RAW id is its own intent: accept it only
        # if the Agent object genuinely exists (never invent one; that is mint territory).
        exists = await actions.pool.fetchval(
            "SELECT 1 FROM objects WHERE canonical=$1 AND type='Agent' AND status='active'",
            seat_or_agent)
        agent_id = seat_or_agent if exists else None
    if agent_id is None:
        return {"error": f"no such seat or agent: {seat_or_agent!r} — unknown to the graph; "
                         "a rebind never silently no-ops on a name nobody holds"}
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
    now = datetime.now(UTC)
    a = await actions.create_or_find_object("Agent", agent_id, agent_id)
    await actions.assert_property(
        a, "anchor_moved", f"{old_cwd or '?'} → {new_cwd}", agent_id, now, _CONF,
        evidence_class=_EC)
    return {
        "agent": agent_id, "project": label, "old_cwd": old_cwd, "new_cwd": new_cwd,
        "mount_rows_updated": rows_updated, "osiris_written": osiris_path,
        "note": f"{label}'s anchor moved to {new_cwd} — identity, lineage, attribution, and "
                "mail all key on the label, untouched by this move",
    }
