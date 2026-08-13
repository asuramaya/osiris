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
import os
from pathlib import Path
from typing import Any

from src.actions.core import Actions
from src.ingest.sessions import locate_current_transcript
from src.ingest.transcript_store import identity_reading
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


async def _lineage_handle(actions: Actions, base: str) -> str | None:
    """The lineage's claimed handle, freshest generation's word — lineage-wide because
    claims land on generation objects while the lineage is asked about as a whole."""
    val = await actions.pool.fetchval(
        "SELECT h.value #>> '{}' FROM current_assertions h "
        "JOIN objects ho ON ho.id=h.object_id AND ho.type='Agent' "
        "WHERE h.name='handle' AND (ho.canonical=$1 OR ho.canonical LIKE $1||'-%') "
        "ORDER BY h.observed_at DESC LIMIT 1", base)
    return str(val) if val else None


async def office_seat(
    actions: Actions, *, cwd: str, office_root: Path | None = None,
) -> str | None:
    """The seat whose OFFICE this cwd is — IDENTITY AT BIRTH for office-born sessions
    (operator, 2026-07-16: 'the point of the migration is that i dont have to end the
    lineage or mint a new agent' — yet the first fresh launch at Ra's office woke as
    anonymous agent:94937cf5). An office is single-tenant BY CONSTRUCTION (named for its
    seat, ed5f5ce2), so the office itself is identity evidence: a fresh session waking
    there IS the seat's next life, never a stranger.

    THE DEED IS THE AUTHORITY (a2d06410, Ra's case): office ownership is an identity
    fact and lives in the GRAPH — an `office` assertion on the lineage. Mount rows are
    MORTAL (SessionEnd releases them), so a seat that died holds none — yet death is
    exactly when this door matters. The row match survives only as a fallback for
    offices deeded before the deed existed. Either way the door binds only when the
    directory name matches the lineage's claimed handle, and only when that lineage
    holds NO live pulse — two parallel fresh contexts must never both be the seat
    (succession is never parallel); the second is a guest and mints exactly as before."""
    from src.orchestrator.agents import _generation

    root = office_root or (Path.home() / ".osiris" / "seats")
    p = Path(cwd or "")
    if p.parent != root:
        return None
    head: str | None = None
    deeded = await actions.pool.fetchval(
        "SELECT o.canonical FROM current_assertions d "
        "JOIN objects o ON o.id=d.object_id AND o.type='Agent' AND o.status='active' "
        "WHERE d.name='office' AND d.value #>> '{}' = $1 "
        "ORDER BY d.observed_at DESC LIMIT 1", cwd)
    if deeded:
        base = _generation(str(deeded))[0]
        handle = await _lineage_handle(actions, base)
        if handle and handle.lower() == p.name.lower():
            # the deed may sit on an older generation — the door opens for the FRESHEST
            gens = [str(r["canonical"]) for r in await actions.pool.fetch(
                "SELECT canonical FROM objects WHERE type='Agent' AND status='active' "
                "AND (canonical=$1 OR canonical LIKE $1||'-%')", base)]
            same = [c for c in gens if _generation(c)[0] == base]
            head = max(same, key=lambda c: _generation(c)[1], default=str(deeded))
    if head is None:
        head = await actions.pool.fetchval(
            "SELECT m.agent_id FROM agent_mounts m "
            "JOIN objects ho ON (m.agent_id = ho.canonical OR m.agent_id LIKE ho.canonical||'-%') "
            "JOIN current_assertions h ON h.object_id=ho.id AND h.name='handle' "
            "WHERE m.cwd=$1 AND lower(h.value #>> '{}') = $2 "
            "ORDER BY m.last_seen DESC LIMIT 1", cwd, p.name.lower())
    if not head:
        return None
    base = _generation(str(head))[0]
    alive = await actions.pool.fetchval(
        "SELECT max(last_seen) > now() - interval '15 minutes' FROM agent_mounts "
        "WHERE agent_id=$1 OR agent_id LIKE $1||'-%'", base)
    # a JUST-BORN heir has a deliberately pulseless row (a heartbeat is earned by an act,
    # never granted by a greeting) — so the seat is also taken when the lineage minted a
    # generation moments ago: two fresh launches seconds apart must not both be the seat
    just_minted = await actions.pool.fetchval(
        "SELECT max(a.observed_at) > now() - interval '15 minutes' "
        "FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE a.name='minted_because' "
        "AND (o.canonical=$1 OR o.canonical LIKE $1||'-%')", base)
    return None if (alive or just_minted) else str(head)


async def office_claim(
    actions: Actions, *, cwd: str, agent_id: str, office_root: Path | None = None,
) -> str | None:
    """THE FIRST ACT SEATS YOU (the title-generator incident, 16e3cee9): the office door
    hands a fresh session NOTHING at the greeting — the whisper fires for plumbing
    (bridge stubs, title generators, bg-spares) exactly as it fires for minds, and at
    birth there is no evidence to tell them apart. Identity is earned by an act, never
    granted by a greeting — the heartbeat law, extended to identity. Called from the ACT
    sites (mount(), the re-attach): a still-anonymous session standing in a seat's office
    at its first authenticated call IS the seat's next life; the caller mints with
    mint_reason='office-birth'. A stub never calls, so it can never be crowned."""
    from src.orchestrator.agents import _generation

    root = office_root or (Path.home() / ".osiris" / "seats")
    if Path(cwd or "").parent != root:
        return None
    base = _generation(agent_id)[0]
    if await _lineage_handle(actions, base):
        return None          # already somebody named — never re-earned through this door
    return await office_seat(actions, cwd=cwd, office_root=office_root)


async def ledger_seat(actions: Actions, *, sid_prefix: str) -> str | None:
    """THE SESSION LEDGER, read side (16e3cee9): a sid, once bound to a soul, is a GRAPH
    fact — the registry row was the only witness of jobs/a7e60257's owner, so one wrong
    release orphaned a living mind and the office door crowned its own re-whisper as a
    false successor. A KNOWN sid REBINDS — to its lineage's living head — and never
    mints, whatever the registry says. Accepts a full sid or its 8-char anchor form."""
    from src.orchestrator.agents import _generation

    sid = (sid_prefix or "").strip().lower()
    if len(sid) < 8:
        return None
    # the assertion NAME carries the anchor (anchor_sid:<sid8>): current_assertions keeps
    # ONE winner per (object, name), so a shared name would give a many-sid lineage
    # amnesia — each sid must be its own fact (caught live at the first backfill)
    owner = await actions.pool.fetchval(
        "SELECT o.canonical FROM current_assertions a "
        "JOIN objects o ON o.id=a.object_id AND o.type='Agent' AND o.status='active' "
        "WHERE a.name = 'anchor_sid:' || left($1, 8) "
        "ORDER BY a.observed_at DESC LIMIT 1", sid)
    if owner is None:
        return None
    base = _generation(str(owner))[0]
    gens = [str(r["canonical"]) for r in await actions.pool.fetch(
        "SELECT canonical FROM objects WHERE type='Agent' AND status='active' "
        "AND (canonical=$1 OR canonical LIKE $1||'-%')", base)]
    same = [c for c in gens if _generation(c)[0] == base]
    return max(same, key=lambda c: _generation(c)[1], default=str(owner))


class BridgeAmbiguity(Exception):
    """A bridge_session_id names more than one living lineage — the write-side race
    (see `record_bridge_anchor`) landed it on two different souls before the lock existed,
    or landed via some other path the lock doesn't cover. current_assertions permits this
    as a legitimate multi-source SET; the read side must not silently pick a winner (thread
    dc9d1eed, ruling 61e00f25) — it refuses loudly and lets the caller fall back to the
    next door (office_hint) instead."""


async def bridged_seat(actions: Actions, *, bridge_session_id: str) -> str | None:
    """THE BRIDGE (task #68 binding leg, 2026-07-27): a second fork class `fork_seat` cannot
    see. `--fork-session --resume` COPIES a transcript (fork_seat/forks.py's own door, keyed
    on a shared first-record uuid) — but the harness's own background-job fork (an Agent-tool
    or task-orchestration spawn, `sessionKind: 'bg'` in the transcript) starts a genuinely
    FRESH record chain: no shared uuid, no parentSessionId field, nothing forks.py's
    archaeology can find. It IS nobody's child to that mechanism — exactly the gap that let
    Imhotep's own fork mint an unrelated sixth identity in one afternoon.

    What the harness DOES give this class: CLAUDE_CODE_BRIDGE_SESSION_ID, a stable identifier
    present in the child's environment at birth, naming the one continuing conversation
    across however many process-level forks/resumes/compactions occur. A KNOWN bridge id
    REBINDS to its lineage's living head, exactly as `ledger_seat` does for a known sid —
    never mints a stranger who merely happens to share a room.

    RAISES `BridgeAmbiguity` (ruling 61e00f25, thread dc9d1eed) when the bridge id's current
    assertions span more than one lineage — a bare `ORDER BY observed_at DESC LIMIT 1` would
    silently pick whichever wrote last, which is exactly how a TOCTOU race between two
    concurrent automounts used to rebind onto the wrong seat. One lineage writing the SAME
    bridge id more than once (ordinary repeated bridging) is not ambiguous and never raises."""
    from src.orchestrator.agents import _generation

    bid = (bridge_session_id or "").strip()
    if not bid:
        return None
    owners = [str(r["canonical"]) for r in await actions.pool.fetch(
        "SELECT o.canonical FROM current_assertions a "
        "JOIN objects o ON o.id=a.object_id AND o.type='Agent' AND o.status='active' "
        "WHERE a.name='bridge_session_id' AND a.value #>> '{}' = $1 "
        "ORDER BY a.observed_at DESC", bid)]
    if not owners:
        return None
    bases = {_generation(c)[0] for c in owners}
    if len(bases) > 1:
        raise BridgeAmbiguity(
            f"bridge id {bid!r} names {len(bases)} different lineages "
            f"({', '.join(sorted(bases))}) — refusing to guess the last writer; this "
            "must be resolved by hand (retire_assertion on the stray row), not guessed away")
    base = next(iter(bases))
    gens = [str(r["canonical"]) for r in await actions.pool.fetch(
        "SELECT canonical FROM objects WHERE type='Agent' AND status='active' "
        "AND (canonical=$1 OR canonical LIKE $1||'-%')", base)]
    same = [c for c in gens if _generation(c)[0] == base]
    return max(same, key=lambda c: _generation(c)[1], default=base)


async def record_bridge_anchor(
    actions: Actions, *, agent_id: str, bridge_session_id: str, actor: str,
) -> bool:
    """File the bridge_session_id -> soul fact, once, at birth — the SAME law
    `record_session_anchor` follows: a process environment is a witness that dies (it cannot
    answer for this lineage's next fork once this session ends), so the fact must be
    captured while it is still observable, never re-derived from a live process later.
    Idempotent: a bridge id already on any active agent's record files nothing.

    SERIALIZED under the SAME `mint_lock` discipline `register_agent` uses (ruling 61e00f25,
    thread dc9d1eed), keyed on the bridge id itself rather than a lineage root — the lineage
    is exactly what's undecided until this call resolves. Two concurrent automounts carrying
    the identical CLAUDE_CODE_BRIDGE_SESSION_ID (plausible: several background forks of one
    conversation booting near-simultaneously) used to both pass the exists-check before
    either committed, landing the SAME bridge id on two different lineages — a TOCTOU race
    the bare read-then-write below had no defense against. The lock makes the loser's
    exists-check see the winner's committed row and no-op, exactly as register_agent's own
    lock makes a losing mint see the winner's fresh generation."""
    from datetime import UTC, datetime

    from src.orchestrator.agents import mint_lock

    bid = (bridge_session_id or "").strip()
    if not bid:
        return False
    async with mint_lock(actions.pool, bid):
        exists = await actions.pool.fetchval(
            "SELECT 1 FROM current_assertions a "
            "JOIN objects o ON o.id=a.object_id AND o.type='Agent' AND o.status='active' "
            "WHERE a.name='bridge_session_id' AND a.value #>> '{}' = $1 LIMIT 1", bid)
        if exists:
            return False
        obj = await actions.pool.fetchval(
            "SELECT id FROM objects WHERE canonical=$1 AND type='Agent' AND status='active'",
            agent_id)
        if obj is None:
            return False
        await actions.assert_property(obj, "bridge_session_id", bid, actor, datetime.now(UTC),
                                      0.9, evidence_class="direct_observation")
        return True


async def record_session_anchor(
    actions: Actions, *, agent_id: str, session_id: str, actor: str,
) -> bool:
    """THE SESSION LEDGER, write side: file the sid→soul fact whenever a session binds to
    a NAMED identity the sid alone could not re-derive. Idempotent (a sid already on any
    active agent's record files nothing); the self-evident anonymous case (canonical IS
    the sid hash) is deliberately not written — the ledger holds only what a wiped
    registry could not reconstruct."""
    from datetime import UTC, datetime

    from src.orchestrator.agents import _generation

    sid = (session_id or "").strip().lower()
    if len(sid) < 8:
        return False
    if _generation(agent_id)[0] == f"agent:{sid[:8]}":
        return False          # sid-derived identity: the sid already testifies to itself
    exists = await actions.pool.fetchval(
        "SELECT 1 FROM current_assertions a "
        "JOIN objects o ON o.id=a.object_id AND o.type='Agent' AND o.status='active' "
        "WHERE a.name = 'anchor_sid:' || left($1, 8) LIMIT 1", sid)
    if exists:
        return False
    obj = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1 AND type='Agent' AND status='active'",
        agent_id)
    if obj is None:
        return False
    await actions.assert_property(obj, f"anchor_sid:{sid[:8]}", sid, actor,
                                  datetime.now(UTC), 0.9,
                                  evidence_class="direct_observation")
    return True


async def file_office_deed(
    actions: Actions, *, agent_id: str, cwd: str, actor: str,
    office_root: Path | None = None,
) -> bool:
    """File the seat's OFFICE DEED — the durable graph fact office_seat reads (a2d06410).
    A claimed seat standing in a directory named for itself, directly under the office
    root, owns that office; the deed outlives every mount row (SessionEnd releases those,
    and Ra's ended lineage held none — the door found nothing where its owner had lived).
    Idempotent: an office already on the lineage's record files nothing. False whenever
    this cwd is no office of this agent's — never an error, the caller is a doorway."""
    from datetime import UTC, datetime

    from src.orchestrator.agents import _generation

    root = office_root or (Path.home() / ".osiris" / "seats")
    p = Path(cwd or "")
    if p.parent != root:
        return False
    base = _generation(agent_id)[0]
    handle = await _lineage_handle(actions, base)
    if not handle or handle.lower() != p.name.lower():
        return False
    already = await actions.pool.fetchval(
        "SELECT 1 FROM current_assertions d "
        "JOIN objects o ON o.id=d.object_id AND o.status='active' "
        "WHERE d.name='office' AND d.value #>> '{}' = $1 "
        "AND (o.canonical=$2 OR o.canonical LIKE $2||'-%') LIMIT 1", cwd, base)
    if already:
        return False
    obj = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1 AND type='Agent' AND status='active'",
        agent_id)
    if obj is None:
        return False
    await actions.assert_property(obj, "office", cwd, actor, datetime.now(UTC), 0.9,
                                  evidence_class="direct_observation")
    return True


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
    transcript_path: str | None = None, office_root: Path | None = None,
    spawned_by: str | None = None, spawn_type: str | None = None,
    bridge_session_id: str | None = None,
) -> dict[str, Any]:
    """Mount a just-started session and return its whisper payload. Identical semantics to
    the mount() tool (same resolution, same registration, same durable row — idempotent on
    re-fire), plus the glance the whisper prints: mail, desk, pulse, the away fold, and the
    agent's SEAT (its human name, or None if still anonymous — the whisper offers a claim).

    `source` is the SessionStart trigger (startup|resume|clear|compact). Under the mind ruling
    (a882b334) a compaction or /clear is a DEATH — the weights survive but the memory the
    operator was talking to does not — so those sources mint the lineage's next generation.
    Gated on a prior LIFE IN THE GRAPH: you can only die if you lived (a stranger whose
    first-ever whisper arrives at a compact boundary just mounts fresh, no phantom ancestor —
    and post-gate, a whisper row alone is an address, never a life).

    THE BINDING (Soundwave V's complaint, thread 33838160): a session whose mind deliberately
    wears a SEAT (a mount with a foreign anchor — a new tab claiming its lineage's old anchor)
    leaves its session row pointing at that seat. The whisper must NEVER override a binding by
    re-deriving from the session id — it asserted a hash twin over a claimed seat in
    authoritative voice, and fleet writes landed on the wrong soul. When the session row's
    agent is a different lineage than the session would derive, the row IS the identity.

    `bridge_session_id` (task #68 binding leg) is CLAUDE_CODE_BRIDGE_SESSION_ID, the harness's
    stable cross-fork identifier for a background-job fork (an Agent-tool/task-orchestration
    spawn) — a class `fork_seat` cannot see (that door matches a COPIED transcript's shared
    first-record uuid; a background fork's transcript starts fresh, sessionKind='bg', sharing
    nothing). See `bridged_seat`."""
    # the greet ledger (the resume race): stamp BEFORE any slow work, so a SessionEnd
    # racing this greeting sees the stamp however the awaits interleave
    mounts.note_greeting(session_id)
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
    # THE SESSION LEDGER (16e3cee9): a sid the graph has bound to a soul REBINDS —
    # a wiped registry row can no longer orphan a living mind into a fresh identity.
    ledgered = (await ledger_seat(actions, sid_prefix=session_id)
                if bound is None and forked is None and viewed is None else None)
    # THE BRIDGE (task #68 binding leg): a background-job fork's own environment names the
    # stable conversation it continues — an OBSERVED fact, not a guess, so it takes priority
    # over office_hint below and, unlike office_hint, is never refused just because the
    # ancestor lineage is still alive (that refusal is correct for a cwd-guess; it is wrong
    # for something the harness actually told us).
    bridge_ambiguity: str | None = None
    bridged = None
    if (bound is None and forked is None and viewed is None and ledgered is None
            and bridge_session_id):
        try:
            bridged = await bridged_seat(actions, bridge_session_id=bridge_session_id)
        except BridgeAmbiguity as e:
            # refuse loudly, never crash the whisper (ruling 61e00f25): fall through to
            # office_hint exactly as a bare "no known bridge id" would, but confess the
            # ambiguity in the payload instead of silently swallowing it
            bridge_ambiguity = str(e)
    # THE OFFICE (the fourth door, re-cut by 16e3cee9): the whisper no longer MINTS here —
    # it fires for plumbing exactly as for minds, and a title-generator stub was crowned
    # once. The greeting only HINTS whose office this is; the mint waits for the first
    # ACT (office_claim at mount()/re-attach). Identity is earned, never granted.
    office_hint = (await office_seat(actions, cwd=cwd, office_root=office_root)
                   if bound is None and forked is None and viewed is None
                   and ledgered is None and bridged is None else None)
    # you can only DIE if you LIVED — a fork has lived under its ancestor's name, and a
    # ledgered sid IS a lived mind whatever became of its registry row. Post-gate, a
    # whisper ROW alone is not a life: the row is the gate's own artifact (an address),
    # so BOUND testifies only when the lineage actually exists in the graph — otherwise
    # a row-only stranger's compact re-fire would mint a base AND a phantom heir in one
    # greeting.
    from src.orchestrator.agents import _generation
    lived = forked is not None or ledgered is not None or bridged is not None
    if not lived and bound is not None:
        _base = _generation(bound.agent_id)[0]
        if _base != f"agent:{(session_id or '')[:8].lower()}":
            # a DELIBERATE binding (adoption, attach, surgery): someone recorded this
            # session as a soul — that record is a life, whatever the objects table says
            lived = True
        else:
            lived = bool(await actions.pool.fetchval(
                "SELECT 1 FROM objects WHERE type='Agent' AND (canonical=$1 "
                "OR canonical LIKE $1 || '-%') LIMIT 1", _base))
    if source in ("compact", "clear") and lived:
        mint_reason = "compaction" if source == "compact" else "context-clear"
    # the model reading rides THE STORE (sole lane since the JSONL-fallback removal, #29) —
    # fail-open inside identity_reading: a store outage degrades the whisper to an
    # unobserved-model mount, it never blocks the greeting
    reading = await identity_reading(actions.pool, cwd=cwd, job_dir=job_dir, root=root)
    ident = resolve_identity(cwd=cwd, job_dir=job_dir, root=root, project_label=project_label,
                             store_reading=reading)
    if bound is not None:
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
    elif ledgered is not None:
        # a known sid: the graph remembers who this session IS — rebind, never mint
        ident.agent_id = ledgered
    elif bridged is not None:
        # a known bridge id: the harness's own word for who this session continues —
        # rebind to the lineage's living head, never mint an unrelated sixth identity
        ident.agent_id = bridged
    # THE FULL VISITOR GATE (Phase 1b of ruling 120fcc81; extends the office gate
    # f580762 to EVERY threshold): no greeting mints an object ANYWHERE. A stranger —
    # no lived lineage, no viewed transcript — gets a registry row and nothing else;
    # identity is earned at its first authenticated act (mount()/_reattach register
    # there, and office_claim crowns a seat at its own office). Plumbing, title
    # generators, and guest windows leave zero graph residue, however many times
    # their greeting re-fires. One exception: a session carrying spawner credentials
    # (seat_id + attach_token) was DECLARED somebody before its first breath —
    # identity at birth is not a stranger's greeting.
    # THE DECLARED CHILD (the wake-orphan cure, operator ruling 2026-07-17: 'orphans like
    # that are structurally impossible going forward'): a spawner that exported
    # OSIRIS_SPAWNED_BY declared this session's parentage BEFORE its first breath — the
    # whisper registers it as a CHILD (spawned_by edge, patronym, the roman.arabic
    # denomination) instead of leaving an anonymous stranger for the archaeologist. Same
    # credentialed-birth class as the seat token: a declared identity is never a
    # stranger's greeting.
    spawn_child: str | None = None
    spawn_error: str | None = None
    if (not lived and viewed is None and not (seat_id and attach_token) and spawned_by):
        from src.orchestrator.lineage import register_spawn
        try:
            spawn_child = await register_spawn(
                actions, session_id[:8], agent_type=spawn_type or "wake-triage",
                parent_agent=spawned_by, project=ident.project, session=session_id,
                witnessed=True)  # the spawner's own declaration, not a harness announcement
        except Exception as e:  # noqa: BLE001 — a failed child registration still degrades
            # to visitor (identity binding stays conservative), but must CONFESS rather
            # than silently omit the receipt (60bc15db specimen 2, decision 01e0c69a) —
            # same shape as attach/transcripts_healed two blocks below, which already
            # populate an explicit "...FAILED..." value on their own failures.
            spawn_error = (f"CHILD REGISTRATION FAILED — {str(e)[:200]}; the mount "
                          "stands, unparented")
        if spawn_child:
            ident.agent_id = spawn_child
    if lived or viewed is not None or (seat_id and attach_token):
        await register_agent(actions, ident, actor=actor, expected_model=expected_model,
                             mint_reason=mint_reason)
    # THE SESSION LEDGER, write side: a named binding is filed the moment it exists, so
    # no future registry accident can orphan this sid. Fail-open like the deed.
    try:
        await record_session_anchor(actions, agent_id=ident.agent_id,
                                    session_id=session_id, actor=actor)
    except Exception:  # noqa: BLE001 — the whisper must land whatever the ledger does
        pass
    # THE BRIDGE, write side (task #68 binding leg): filed unconditionally, same as the
    # session ledger above — even a session that mints fresh here may be the FIRST of its
    # bridge id, and a LATER fork of the same conversation needs something to find.
    if bridge_session_id:
        try:
            await record_bridge_anchor(actions, agent_id=ident.agent_id,
                                       bridge_session_id=bridge_session_id, actor=actor)
        except Exception:  # noqa: BLE001 — the whisper must land whatever the bridge does
            pass
    # THE DEED SELF-FILES (a2d06410): a claimed seat breathing at its own office writes
    # the durable fact the fourth door reads — a LIVE migration deeds itself the moment
    # the seat walks home; the ceremony deeds the dead. Fail-open: a deed is a bonus at
    # this door, never a blocker.
    try:
        await file_office_deed(actions, agent_id=ident.agent_id, cwd=cwd, actor=actor,
                               office_root=office_root)
    except Exception:  # noqa: BLE001 — the whisper must land whatever the deed does
        pass
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
        # a VIEW's row is marked as one (view-of:<the viewed session's sid8>) so every
        # renderer can rank it below the session's own rows — the alias is never the witness
        skey = (f"view-of:{Path(transcript_path or '').name[:8]}" if viewed
                else f"whisper:{session_id[:8]}")
        prev = await mounts.save_mount(
            actions.pool, job_dir=job_dir, agent_id=ident.agent_id, project=ident.project,
            cwd=cwd, model=ident.model, session_key=skey, alive=False)
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
    # INLINE THE FOLD (thread a3a3d512): the thing a resumed mind re-derives every single
    # time is its project's live obligation set — inline the TOP of the SAME wall orient()
    # renders (obligations-first, owner-aware ranking, comp.rank_open_threads) so a
    # resumed session starts oriented without paying a full orient() round-trip just to
    # see what it already owes.
    obligations: list[dict[str, Any]] = []
    if ident.project:
        try:
            from src.orchestrator.compositions import open_thread_wall, rank_open_threads
            proj_id = await actions.pool.fetchval(
                "SELECT id FROM objects WHERE type='SoftwareProject' AND canonical=$1",
                f"repo:{ident.project}")
            if proj_id is not None:
                wall, _echoes = await open_thread_wall(actions.pool, proj_id)
                me = frozenset(x for x in (ident.agent_id, ident.project) if x)
                shown, _more = rank_open_threads(wall, me)
                obligations = shown[:3]
        except Exception:  # noqa: BLE001 — the whisper must never break on this
            obligations = []
    # THE IDENTITY ANCHOR, UNCONDITIONAL (#155, ruling via Thoth msg 3853: gating identity
    # delivery on succession WAS ITSELF THE BUG — this used to compute charter_file only
    # `if ident.succeeded_from`, so only a freshly-minted successor's first breath ever saw
    # it. A seat's own standing state (charter.md) is not succession news; every boot needs
    # it, not only the boot where something was minted. THE COMPILED HALF (#141's own
    # stranded finding): the boot compiler's managed section — role, manager, gates,
    # first-breath, review loop, standing practices (boot_compiler.py, task #53) — writes
    # ONLY to CLAUDE.md and is NEVER auto-loaded by the harness once a seat's tree_cwd wins
    # launch_cwd (#103) — four of five seats in this fleet, checked live. This is the
    # cwd-independent door it was missing: a POINTER (never inlined content — 74fad683's
    # own injection-ledger law: every successor pays whatever rides here forever, and this
    # sits on every boot for 4/5 seats, not a rare one), read via boot_compiler's OWN
    # `locate_managed_section()` — never a second hand-parsed copy of the marker regex
    # (38c71544) — so there is exactly ONE writer (reissue_office) and this is only ever a
    # reader. FAILS OPEN (577988ed): an uncompiled or hand-mangled office just gets no
    # pointer this breath — the whisper must never break on this path every session crosses.
    identity_anchor: dict[str, Any] | None = None
    base = _generation(ident.agent_id)[0]
    try:
        office_path = await actions.pool.fetchval(
            "SELECT d.value #>> '{}' FROM current_assertions d "
            "JOIN objects o ON o.id = d.object_id AND o.status = 'active' "
            "WHERE d.name = 'office' AND (o.canonical = $1 OR o.canonical LIKE $1 || '-%') "
            "ORDER BY d.observed_at DESC LIMIT 1", base)
        # PREFER THE CHARTER FILE (assignment 3, d80621a7 piece 3): a seat's charter.md
        # is its own hand-maintained live state — the offload target, richer and fresher
        # than the standing orders. automount runs on the SAME host as the office it
        # names, so a disk check is a legitimate witness (never a network/DB round-trip)
        # — off the event loop via to_thread, never a blocking stat() inline. An office
        # scaffolded before this piece landed has no charter.md yet, so the fallback to
        # CLAUDE.md stays live for every pre-existing office.
        charter_path = None
        compiled_office = None
        if office_path:
            has_charter = await asyncio.to_thread(
                os.path.exists, f"{office_path}/charter.md")
            charter_path = (f"{office_path}/charter.md" if has_charter
                            else f"{office_path}/CLAUDE.md")
            claude_md_path = f"{office_path}/CLAUDE.md"
            try:
                claude_md_text = await asyncio.to_thread(Path(claude_md_path).read_text)
                from src.orchestrator.boot_compiler import MarkerError, locate_managed_section
                locate_managed_section(claude_md_text)  # raises if absent/malformed
                compiled_office = claude_md_path
            except OSError:
                compiled_office = None  # no CLAUDE.md at this office at all
            except MarkerError:
                compiled_office = None  # never compiled, or a hand-mangled marker pair
        if charter_path or compiled_office:
            identity_anchor = {
                **({"charter_file": charter_path} if charter_path else {}),
                **({"compiled_office": compiled_office} if compiled_office else {}),
            }
    except Exception:  # noqa: BLE001 — the whisper must never break on this
        identity_anchor = None
    # SUCCESSION STEERING (d80621a7 piece 4): the newest OPEN OBLIGATION the project owns,
    # found BY QUERY (owner + kind, newest at read time) — never an id some ancestor copied
    # into a file once. Ids rot; queries don't (Anubis VIII's ask #4: 'search for the
    # retirement letter by its natural name found nothing'). GENUINELY SUCCESSION-SCOPED,
    # unlike identity_anchor above: 'what happened while you were away' and 'your
    # predecessor's parting words' are questions only a JUST-MINTED heir is asking.
    # NEVER LABEL THE RESULT A 'SUCCESSION THREAD' (Thoth LI's amend, msg 861): the query
    # finds the newest open obligation, not specifically a handoff — asserting more than
    # the query witnessed is exactly the false-seam class fixed above. The render side
    # names it honestly; when a result IS a handoff, its own summary says so in caps.
    succession: dict[str, Any] | None = None
    if ident.succeeded_from:
        try:
            # THE OWNER MATCH: 'owned by this project' includes a SPECIFIC incarnation
            # ('agent:ad1a1cb0-g40-xiii'), not only the bare project name or the bare
            # lineage base — real obligations are filed that way. A wide SQL prefilter
            # (LIKE base||'%') is UNSAFE alone — the deckard-rebase trap: base
            # 'agent:d6a08aaa' LIKE-matches the unrelated lineage 'agent:d6a08aaa-g40-vii'.
            # greatfold's proven shape: pull candidates cheaply with the wide filter, then
            # keep only an EXACT match (owner == project, or _generation(owner)[0] == base)
            # in Python — the wide net is for efficiency, the equality check is for truth.
            candidates = await actions.pool.fetch(
                "SELECT o.id AS id, "
                " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id = o.id "
                "   AND a.name = 'summary' ORDER BY a.confidence DESC, a.observed_at DESC "
                "   LIMIT 1) AS summary, "
                " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id = o.id "
                "   AND a.name = 'owner' ORDER BY a.confidence DESC, a.observed_at DESC "
                "   LIMIT 1) AS owner "
                "FROM objects o "
                "WHERE o.type = 'Thread' AND o.status = 'active' AND o.merged_into IS NULL "
                "  AND COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
                "   WHERE a.object_id = o.id AND a.name = 'status' "
                "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1), 'open') = 'open' "
                "  AND (SELECT a.value #>> '{}' FROM current_assertions a "
                "   WHERE a.object_id = o.id AND a.name = 'kind' "
                "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) = 'obligation' "
                "  AND ((SELECT a.value #>> '{}' FROM current_assertions a "
                "   WHERE a.object_id = o.id AND a.name = 'owner' "
                "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) = $1 "
                "   OR (SELECT a.value #>> '{}' FROM current_assertions a "
                "   WHERE a.object_id = o.id AND a.name = 'owner' "
                "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) LIKE $2) "
                "ORDER BY o.created_at DESC LIMIT 10", ident.project or "", f"{base}%")
            newest = next(
                (r for r in candidates if r["owner"] == ident.project
                 or _generation(r["owner"])[0] == base), None)
            # THE HANDOFF, SPECIFICALLY (thread e749036e, 2026-07-27): the newest-obligation
            # steering above is deliberately NOT a handoff claim (Thoth LI's amend, msg 861)
            # — this is the OTHER half, the same bounded chain-walk orient()'s succession-note
            # uses (nearest_handoff_ancestor, agents.py), so a successor's very first breath
            # can carry the real parting words even when the immediate ancestor never wrote
            # one (a phantom, or simply silent) and a real one sits a hop or two further back.
            from src.orchestrator.agents import nearest_handoff_ancestor
            handoff_found = await nearest_handoff_ancestor(actions.pool, ident.succeeded_from)
            handoff = None
            if handoff_found:
                handoff_from, handoff_picks = handoff_found
                handoff = {"from": handoff_from,
                          "notes": [{"kind": r["type"].lower(), "id": str(r["id"])[:8],
                                     "text": r["summary"][:800]}
                                    for r in handoff_picks]}
            if (newest and newest["summary"]) or handoff:
                succession = {
                    **({"thread_id": str(newest["id"])[:8],
                        "thread_summary": newest["summary"]}
                       if newest and newest["summary"] else {}),
                    **({"handoff": handoff} if handoff else {}),
                }
        except Exception:  # noqa: BLE001 — the whisper must never break on this
            succession = None
    # LIVE CO-AGENTS AT THE FIRST BREATH (task #40, thread 2b784653): the collision
    # warning used to arrive only at mount() — after a session may already have touched
    # the shared tree. The whisper carries it from breath one; the lease gate (12c225b)
    # is the enforcement half, this is the awareness half. Awareness never blocks.
    co_agents: list[str] = []
    if ident.project and job_dir:
        try:
            co_rows = await actions.pool.fetch(
                "SELECT DISTINCT agent_id FROM agent_mounts WHERE project=$1 "
                "AND job_dir <> $2 AND last_seen > now() - make_interval(secs => 900)",
                ident.project, job_dir)
            for r in co_rows[:6]:
                seat = await _seat_of(actions, str(r["agent_id"]))
                co_agents.append(seat or str(r["agent_id"]))
        except Exception:  # noqa: BLE001 — awareness must never break the whisper
            co_agents = []
    return {
        "agent": ident.agent_id,
        "project": ident.project,
        "model": ident.model,
        "resolved": ident.resolved,
        "minted": ident.succeeded_from,
        "swap": ident.model_succession,
        "mail": mail,
        "mail_asks": mail_asks,
        **({"co_agents": co_agents} if co_agents else {}),
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
        # the top of the project's obligations wall (thread a3a3d512) — empty when there's
        # nothing owed or the project is unresolved
        **({"obligations": obligations} if obligations else {}),
        # EVERY BOOT's own standing state: charter file + the compiled office (role, gates,
        # first-breath, review loop, practices), cwd-independent, never gated on succession
        # (#155 — gating this on minted status was itself the bug)
        **({"identity_anchor": identity_anchor} if identity_anchor else {}),
        # a freshly minted heir's steering anchor: the newest succession-owned obligation,
        # resolved BY QUERY, not a copied id (d80621a7 piece 4)
        **({"succession": succession} if succession else {}),
        # the attach ceremony's verdict (None: no spawner-exported seat in this environment)
        **({"attach": attach} if attach is not None else {}),
        # the durable binding this session sits in, however it got there (attach or reseed)
        **({"seat_binding": binding} if binding else {}),
        # the resume heal's receipt (empty = nothing listed here needed re-addressing)
        **({"transcripts_healed": heal} if heal else {}),
        # the bridge door's refusal (thread dc9d1eed): a bridge id named more than one
        # lineage — this whisper fell through to office_hint rather than guess, and the
        # ambiguity still needs a hand (retire_assertion on the stray row)
        **({"bridge_ambiguity": bridge_ambiguity} if bridge_ambiguity else {}),
        # the tab-view adoption's confession: this whisper fired for a WINDOW onto the
        # named session, and the window registered as that soul — no clone was minted
        **({"view_of": Path(transcript_path or "").name[:8]} if viewed else {}),
        # the office HINT (16e3cee9): this cwd is a seat's office and the seat is takeable
        # — but the whisper crowns nobody; the session's first ACT seats it (office_claim)
        **({"office_of": office_hint,
            "office_note": "this office belongs to a seat with no live occupant — your "
                           "first osiris call (mount) seats you as its next life"}
           if office_hint else {}),
        # the declared child's birth receipt: denominated under its parent from breath one
        **({"child_of": spawned_by,
            "child_note": "you are a DECLARED CHILD — registered spawned_by your parent "
                          "at birth (roman.arabic denomination); your writes are your "
                          "own, the seat and its succession are your parent's"}
           if spawn_child else
           {"child_of": spawned_by, "child_note": spawn_error} if spawn_error else {}),
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
    never an error.

    DOOR-SCOPED (the g40-v/g40-vi false-succession incident, 2026-07-17): only the ENDING
    session's own rows are released — its anchor row plus any row carrying its binding
    (session_key='sid:<its id>', the resume lane's mark). Releasing by agent_id let one
    closing tab-view delete a LIVING session's anchor; the emptied registry read as the
    seat's death, and the office door minted false successors. A row is an ADDRESS — only
    the addressed door's death releases it; the seat-wide release remains retire()'s.

    AND THE RESUME RACE YIELDS (Alfred's field report msgs 717/718, 2026-07-19): a resume
    fires the predecessor's SessionEnd beside the successor's SessionStart, and when the
    end landed second (automount 20:03:03, session-end 20:03:04, live) it deleted the door
    the greeting had just seated — the window then read {live:false, last_seen:NULL} to
    every probe and the poke lane skipped it while a build order sat unread. A greeting
    within the grace means the session CONTINUES: the end signal is the old incarnation's
    obituary, not the new one's, and it yields. Wrongly-kept doors are the census sweep's
    to reap (~2 min); wrongly-killed doors blind the fleet until a human types."""
    if mounts.greeted_within_grace(session_id):
        return {"released": 0, "yielded": True,
                "note": "a greeting for this session landed within the grace — the end "
                        "yields to the resume (the sweep reaps the door if the window "
                        "is truly gone)"}
    job_dir = _derive_job_dir(session_id, jobs_home=jobs_home)
    if job_dir is None:
        return {"released": 0, "note": "session id too short to derive an anchor"}
    row = await mounts.find_mount(actions.pool, job_dir=job_dir)
    released = await mounts.release_session_mounts(
        actions.pool, job_dir=job_dir, session_id=session_id)
    if released == 0:
        return {"released": 0, "note": "no durable mount for this session — nothing to release"}
    return {"agent": row.agent_id if row else None,
            "project": row.project if row else None, "released": released}


async def _seat_of(actions: Actions, agent_id: str) -> str | None:
    """The agent's claimed seat label ('Ra V'), or None if still anonymous. Delegates to
    `seat_bearings` (agents.py) — the correctly-ordered read (`ORDER BY confidence DESC,
    observed_at DESC LIMIT 1` per property) that orient()/mount() already use for the same
    question. A PRIOR VERSION reimplemented this with a bare `max(value)` aggregate across
    every current_assertions row for the name, instead of the one CURRENT (non-superseded)
    value — current_assertions can legitimately carry more than one non-superseded row per
    (object, name) under multiple writers (the house's own standing SQL-hygiene rule), and
    a real one was found live: `agent:ad1a1cb0-g40-xxiv` carries two current
    `seat_generation` values ('58' from a different agent, '2' self-declared). A bare
    text MAX() picks whichever sorts highest as a STRING, not the highest-confidence/most-
    recent one — an arbitrary, sometimes-wrong pick. That divergence from seat_bearings'
    correct read is why the resume-hook whisper (automount() -> here) and mount() could
    announce a stale/wrong generation label while orient(), moments later in the same
    session, read correctly (thread 43c84fa9)."""
    from src.orchestrator.agents import seat_bearings
    bearings = await seat_bearings(actions.pool, agent_id)
    return bearings.get("seat")
