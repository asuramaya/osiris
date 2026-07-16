"""The whisper — automatic onboarding at session start (operator's blessing, 2026-07-08).

"It just knows, or it gets whispered, without me even telling it." The SessionStart hook
(scripts/osiris_whisper.py, user scope — EVERY session on the box) posts here before the
agent's first token; the server mounts it and hands back the one paragraph that makes a
stranger a fleet member: its name, its project, its mail, what happened while its lineage
slept. The agent wakes up already remembering Osiris — the hive-mind assumption made flesh:
every agent writes to the graph because every agent arrives already mounted.

Reuses the whole tested mount path (resolve_identity → register_agent → save_mount): the
hook-derived job_dir (~/.claude/jobs/<sid[:8]> — the harness's own scheme, verified live)
makes the registration DURABLE and the identity ANCHORED, so the trigger's liveness probe
sees the tab and mail takes the deliver lane, never a twin-minting wake. Fail-open by
design: the hook prints a manual-mount hint when this endpoint is unreachable, and a
session that never got whispered can always mount by hand.
"""
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from src.actions.core import Actions
from src.ingest.sessions import locate_current_transcript
from src.orchestrator import forks, mounts
from src.orchestrator.agents import register_agent, resolve_identity
from src.orchestrator.mailbox import desk_briefs_from, settle_history_at_join, unread_count


async def fork_seat(
    actions: Actions, *, job_dir: str | None, root: Path | None = None,
) -> str | None:
    """The seat this session ALREADY HAS under another name — or None if it is truly new.

    `claude --fork-session --resume` continues one mind under a NEW session id: new id, new
    whisper, new automount, A SECOND SEAT (see forks.py for the autopsy). The fork is not a
    stranger; it is the same conversation, still running. Ask the transcript who it really is
    before minting anybody.

    Called ONLY when the registry has no row for this anchor — i.e. once, at a session's birth
    — and memoized from there. A session that is nobody's child pays one ~1s disk sweep, ever.
    """
    if not job_dir:
        return None
    base = root or (Path.home() / ".claude/projects")
    path = locate_current_transcript(base, job_dir, anchored_only=True)
    if path is None:
        # THE BRIDGED RESUME (90f0cb3a): the session-picker resume mints a new sid that
        # writes NO transcript of its own (appends continue in the resumed session's file),
        # so the archaeology above finds nothing and this mind would be minted a twin. The
        # harness's own job state names who it continues — adopt that seat.
        prior = mounts.resumed_anchor(job_dir)
        rec = await mounts.find_mount(actions.pool, job_dir=prior) if prior else None
        return rec.agent_id if rec else None
    try:
        return await forks.seat_of_fork(actions.pool, path, root=base)
    except Exception:  # noqa: BLE001 — identity may degrade, but the whisper must never die
        return None


async def view_seat(
    actions: Actions, *, transcript_path: str, session_id: str,
    jobs_home: Path | None = None,
) -> str | None:
    """The seat a VIEW continues — the alias-clone cure (2026-07-16). A live session
    attached through a new tab fires a whisper under the TAB's sid: no state.json receipt
    (that is the daemon's artifact), no transcript of its own (appends continue in the
    real session's file) — every archaeology in fork_seat finds nobody and a clone row is
    minted beside the living original. But the hook itself names the conversation it
    continues: transcript_path. When that file belongs to ANOTHER session's sid and that
    session holds a mount row, this tab is a window onto that mind — adopt it, mint
    nothing. Returns None for a session appending its own transcript (genuinely fresh)."""
    name = Path(transcript_path or "").name
    if not name.endswith(".jsonl"):
        return None
    tsid = name[: -len(".jsonl")]
    if len(tsid) < 8 or tsid[:8] == (session_id or "")[:8]:
        return None
    prior = _derive_job_dir(tsid, jobs_home=jobs_home)
    rec = await mounts.find_mount(actions.pool, job_dir=prior) if prior else None
    return rec.agent_id if rec else None


def _derive_job_dir(session_id: str, *, jobs_home: Path | None = None) -> str | None:
    """~/.claude/jobs/<first 8 of the session id> — the harness's scheme (verified against
    live job dirs). None when the id is too short to trust. `jobs_home` is a test seam."""
    sid = (session_id or "").strip().lower()
    if len(sid) < 8:
        return None
    return str((jobs_home or Path.home() / ".claude" / "jobs") / sid[:8])


async def automount(
    actions: Actions, *, session_id: str, cwd: str, actor: str,
    expected_model: str | None = None, lease_secs: int = 900,
    root: Path | None = None, jobs_home: Path | None = None,
    project_label: str | None = None, source: str | None = None,
    seat_id: str | None = None, attach_token: str | None = None,
    transcript_path: str | None = None,
) -> dict[str, Any]:
    """Mount a just-started session and return its whisper payload. Identical semantics to
    the mount() tool (same resolution, same registration, same durable row — idempotent on
    re-fire), plus the glance the whisper prints: mail, desk, pulse, the away fold, and the
    agent's SEAT (its human name, or None if still anonymous — the whisper offers a claim).

    `source` is the SessionStart trigger (startup|resume|clear|compact). Under the mind ruling
    (a882b334) a compaction or /clear is a DEATH — the weights survive but the memory the
    operator was talking to does not — so those sources mint the lineage's next generation.
    Gated on a prior durable mount: you can only die if you lived (a stranger whose first-ever
    whisper arrives at a compact boundary just mounts fresh, no phantom ancestor).

    THE BINDING (Soundwave V's complaint, thread 33838160): a session whose mind deliberately
    wears a SEAT (a mount with a foreign anchor — a new tab claiming its lineage's old anchor)
    leaves its session row pointing at that seat. The whisper must NEVER override a binding by
    re-deriving from the session id — it asserted a hash twin over a claimed seat in
    authoritative voice, and fleet writes landed on the wrong soul. When the session row's
    agent is a different lineage than the session would derive, the row IS the identity."""
    job_dir = _derive_job_dir(session_id, jobs_home=jobs_home)
    mint_reason = None
    bound = await mounts.find_mount(actions.pool, job_dir=job_dir) if job_dir else None
    # THE FORK (7cbc2f98): no row for this anchor does NOT mean a new mind. `--fork-session
    # --resume` gives one running conversation a brand-new session id, and the transcript it
    # carries REWRITES every record's sessionId to the new one, so the fork swears it is newborn.
    # Ask its record uuids who its parent is before we mint it a second identity.
    forked = await fork_seat(actions, job_dir=job_dir, root=root) if bound is None else None
    # THE TAB VIEW (the alias-clone class): neither a row nor a fork, but the hook's own
    # transcript_path names the session this tab continues — adopt, never clone.
    viewed = (await view_seat(actions, transcript_path=transcript_path,
                              session_id=session_id, jobs_home=jobs_home)
              if bound is None and forked is None and transcript_path else None)
    # you can only DIE if you LIVED — and a fork has lived, under its ancestor's name.
    if source in ("compact", "clear") and (bound is not None or forked is not None):
        mint_reason = "compaction" if source == "compact" else "context-clear"
    ident = resolve_identity(cwd=cwd, job_dir=job_dir, root=root, project_label=project_label)
    if bound is not None:
        from src.orchestrator.agents import _generation
        if _generation(bound.agent_id)[0] != _generation(ident.agent_id)[0]:
            # the deliberate binding wins: seams (swap/compaction) run on the SEAT's lineage
            ident.agent_id = bound.agent_id
    elif forked is not None:
        # the same mind, wearing a new session id. Adopt the ancestor's SEAT — never the
        # transcript's root sid, which would invent a third identity while curing a second.
        ident.agent_id = forked
    elif viewed is not None:
        # a tab-view of a living session: the window registers as the soul it shows
        ident.agent_id = viewed
    await register_agent(actions, ident, actor=actor, expected_model=expected_model,
                         mint_reason=mint_reason)
    prev = None
    if job_dir:
        # PROVISIONAL — seated, but with NO PULSE. The whisper fires for processes that are not
        # anybody (`claude bg-spare`, pty hosts, claim-socket daemons: a real session id, a real
        # cwd, and no conversation ever). Granting them a heartbeat made them LIVE by every test
        # the fleet has — inflating the roster, crying wolf about contended trees, and taking
        # delivery of mail into a process that will never read it (Anubis XII, msg 424).
        # A HEARTBEAT MUST BE EARNED BY AN ACT, NEVER GRANTED BY A GREETING. A real session
        # certifies itself within seconds: its first Osiris call bumps this row, or its transcript
        # grows and observe_liveness stamps it. A spare does neither, forever.
        prev = await mounts.save_mount(
            actions.pool, job_dir=job_dir, agent_id=ident.agent_id, project=ident.project,
            cwd=cwd, model=ident.model, session_key=f"whisper:{session_id[:8]}", alive=False)
        if prev is None:  # a fresh session: anchor the fold on the lineage's last life
            # ...and a JOINER inherits the room's collective settle-state (sibling-settled
            # broadcasts are not a newcomer's unread; truly-open mail still greets it)
            await settle_history_at_join(actions.pool, ident.project, ident.agent_id)
            prev = await mounts.project_prev_seen(
                actions.pool, ident.project, exclude_job_dir=job_dir)
    # THE ATTACH CEREMONY (identity core, 5cef856b): a spawner exported OSIRIS_SEAT_ID +
    # OSIRIS_ATTACH_TOKEN into this session's environment before its first breath; the
    # whisper carried them here. Verify and BIND — refusals are LOUD (an error the whisper
    # prints) but the whisper itself never dies of one: the mount above stands either way,
    # so a refused attach degrades to exactly today's inferred identity, plus a confession.
    attach: dict[str, Any] | None = None
    binding: str | None = None
    if seat_id and attach_token and job_dir:
        from src.orchestrator.seats import attach_session
        try:
            attach = await attach_session(actions, seat_id=seat_id, token=attach_token,
                                          job_dir=job_dir, agent_id=ident.agent_id)
            binding = attach.get("attached")
        except Exception as e:  # noqa: BLE001 — fail-open, loud in the payload
            attach = {"error": f"ATTACH FAILED — {str(e)[:200]}; the mount stands, unbound"}
    elif job_dir:
        # THE HAND-RESUME FOLLOWS THE SEAT (Phase B4): no spawner env here, but if this mind
        # actively HOLDS a seat, its fresh mount row re-earns the binding from the durable
        # holds link — session_end deleted the hot half, never the graph's memory of it.
        from src.orchestrator.seats import reseed_binding, seat_of_mount
        try:
            binding = (await seat_of_mount(actions.pool, job_dir=job_dir)
                       or await reseed_binding(actions.pool, agent_id=ident.agent_id,
                                               job_dir=job_dir))
        except Exception:  # noqa: BLE001 — the binding is a bonus; the whisper never dies
            binding = None
    # SELF-HEALING RESUME (thread 39ea074c, the operator's ruling: part of the system,
    # never a one-time patch): any transcript LISTED under this cwd whose internal address
    # (the per-line `cwd` the harness validates resume against) still names a former home
    # is re-addressed to point here — a moved/extracted session resumes at the next launch
    # with no hand on it. Guarded inside: never the mounting session's own file, never a
    # live-pulse sid, never a file still warm from an open tab's pen. Fail-open loud.
    heal: dict[str, Any] | None = None
    try:
        prefixes = await mounts.live_mount_sid_prefixes(actions.pool)
        heal = await asyncio.to_thread(
            mounts.heal_slug_transcripts, cwd, projects_root=root,
            skip_sids={session_id}, skip_sid_prefixes=prefixes)
    except Exception as e:  # noqa: BLE001 — the whisper never dies of a heal
        heal = {"error": f"TRANSCRIPT HEAL FAILED — {str(e)[:200]}; the mount stands; "
                         "moved sessions may still refuse to resume here"}
    mail = await unread_count(actions.pool, ident.project, reader_agent=ident.agent_id,
                              lease_secs=lease_secs) if ident.project else 0
    # graded asks travel beside the total (f9449d8d) so the whisper can lead with what is
    # actionable; ungraded mail keeps the plain count — never guessed into a band
    mail_asks = (await unread_count(actions.pool, ident.project, reader_agent=ident.agent_id,
                                    lease_secs=lease_secs, grade="ask")
                 if ident.project and mail else 0)
    # the desk, SCOPED (operator ruling, 2026-07-16): this seat's own unanswered briefs,
    # never the fleet-wide backlog — a number identical in every chrome informs nobody
    desk = await desk_briefs_from(actions.pool, ident.agent_id)
    away = await mounts.while_away(actions.pool, ident.project, ident.agent_id, prev)
    try:
        pulse: str | None = await mounts.fleet_pulse(actions.pool, lease_secs=lease_secs)
    except Exception:  # noqa: BLE001 — the pulse must never break the whisper
        pulse = None
    # the THIN-PROJECT flag (field report msg 124): an agent auto-mounted to a young/empty
    # project reads its own orient()'s silence as an empty GRAPH — a lie of omission. Cheap
    # check: does the project have any recorded decisions/threads at all?
    thin = False
    if ident.project:
        try:
            thin = not bool(await actions.pool.fetchval(
                "SELECT 1 FROM links l JOIN objects p ON p.id = l.to_id "
                "JOIN objects s ON s.id = l.from_id "
                "WHERE p.canonical = 'repo:' || $1 AND s.type IN ('Decision','Thread') "
                "LIMIT 1", ident.project))
        except Exception:  # noqa: BLE001 — the flag must never break the whisper
            thin = False
    return {
        "agent": ident.agent_id,
        "project": ident.project,
        "model": ident.model,
        "resolved": ident.resolved,
        "minted": ident.succeeded_from,
        "swap": ident.model_succession,
        "mail": mail,
        "mail_asks": mail_asks,
        "desk": desk,
        "pulse": pulse,
        "away": away,
        # the durable anchor for THIS session (derived from its id, not $CLAUDE_JOB_DIR which is
        # empty in plain sessions). The whisper hands it to the agent so any later mount() — even
        # a reconnect re-mount — carries the real anchor and RE-ATTACHES instead of minting a twin
        # (thread 883a24f4). Distinct per session, so co-located agents (one repo's cloud+engine
        # on one dir) never collide: each has its own session id → its own job_dir.
        "job_dir": job_dir,
        # the SEAT: the agent's claimed human name + generation ('Thoth', 'Anna II'), or None
        # if still anonymous — the whisper offers a claim in that case.
        "seat": await _seat_of(actions, ident.agent_id),
        # thin=True → the whisper says plainly: YOUR project is young; the GRAPH is not.
        "thin": thin,
        # the attach ceremony's verdict (None: no spawner-exported seat in this environment)
        **({"attach": attach} if attach is not None else {}),
        # the durable binding this session sits in, however it got there (attach or reseed)
        **({"seat_binding": binding} if binding else {}),
        # the resume heal's receipt (empty = nothing listed here needed re-addressing)
        **({"transcripts_healed": heal} if heal else {}),
        # the tab-view adoption's confession: this whisper fired for a WINDOW onto the
        # named session, and the window registered as that soul — no clone was minted
        **({"view_of": Path(transcript_path or "").name[:8]} if viewed else {}),
    }


async def session_end(
    actions: Actions, *, session_id: str, jobs_home: Path | None = None,
) -> dict[str, Any]:
    """SessionEnd's server half — the ghost-seat fix (heinrich's filing, thread 1fe6811c): Stop
    fires PER-TURN and cannot mean "closed"; SessionEnd is the harness's actual close signal.
    Releases the ending session's durable mount row(s) THE SAME WAY retire()'s tool releases a
    seat (`mounts.release_mounts`, exact `agent_id` — a successor that already overwrote the row
    is never touched): the row stops answering `agent_liveness` / `project_last_seen` /
    the trigger's `_owner_live` freshness probe THE INSTANT the tab closes, instead of lingering
    live for up to `last_seen`'s 15-minute decay (the fleet carrying 277 stale ghosts this way).

    Deliberately NOT retire(): no `retired=true` certificate is stamped, and no undisposed-pile
    warning fires. retire() is a MIND's own deliberate, permanent farewell — it gates the RESUME
    lane forever and warns of reanimation if the name is worn again. SessionEnd is only the
    HARNESS observing that a process exited; the SAME session id can resume later
    (`claude --resume`) and its automount re-earns the row from scratch, exactly as if this had
    never fired. Only the SEAT (the durable mount row) is released — identity, lineage, and mail
    are untouched.

    Same anchor derivation as `automount` (`_derive_job_dir`: the harness's own
    ~/.claude/jobs/<sid[:8]> scheme) — a session that was never mounted (no row: a phantom/spare
    that never earned a pulse, or a session id too short to trust) is a silent, honest no-op,
    never an error."""
    job_dir = _derive_job_dir(session_id, jobs_home=jobs_home)
    if job_dir is None:
        return {"released": 0, "note": "session id too short to derive an anchor"}
    row = await mounts.find_mount(actions.pool, job_dir=job_dir)
    if row is None:
        return {"released": 0, "note": "no durable mount for this session — nothing to release"}
    released = await mounts.release_mounts(actions.pool, row.agent_id)
    return {"agent": row.agent_id, "project": row.project, "released": released}


async def _seat_of(actions: Actions, agent_id: str) -> str | None:
    from src.orchestrator.agents import seat_label
    row = await actions.pool.fetchrow(
        "SELECT max(value#>>'{}') FILTER (WHERE a.name='handle') AS handle, "
        "       max(value#>>'{}') FILTER (WHERE a.name='seat_generation') AS gen "
        "FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name IN ('handle','seat_generation')", agent_id)
    if not row or not row["handle"]:
        return None
    return seat_label(agent_id, row["handle"], int(row["gen"]) if row["gen"] else None)
