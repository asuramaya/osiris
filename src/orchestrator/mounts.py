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
import time
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
    """The harness's transcript-directory name for a cwd — mirrors the read side
    (`src/ingest/sessions.py`'s cwd-locate): '/' → '-', nothing else."""
    return cwd.replace("/", "-")


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
    NOTHING (the torn-write window shrinks from the whole rewrite to one syscall)."""
    tmp = path.with_name("." + path.name + ".heal-tmp")
    rewritten = 0
    try:
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
    # the Seat OBJECT's anchor follows — the daemon summons at the office (5cef856b)
    from src.orchestrator.seats import held_seat
    bound = await held_seat(actions.pool, agent_id)
    if bound:
        soid = await actions.create_or_find_object("Seat", bound["seat_id"],
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
        **({"old_cwd_evidence": old_cwd_evidence} if old_cwd_evidence else {}),
        **({"co_resident_rows_repointed": co_repointed} if co_repointed else {}),
        **({"harness": harness} if harness else {}),
        "note": f"{label}'s anchor moved to {new_cwd} — identity, lineage, attribution, and "
                "mail all key on the label, untouched by this move; the harness metadata "
                "(transcripts, project state) moved with it",
    }
