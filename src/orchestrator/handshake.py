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

from pathlib import Path
from typing import Any

from src.actions.core import Actions
from src.orchestrator import mounts
from src.orchestrator.agents import register_agent, resolve_identity
from src.orchestrator.mailbox import OPERATOR_ADDR, settle_history_at_join, unread_count


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
    if source in ("compact", "clear") and bound is not None:
        mint_reason = "compaction" if source == "compact" else "context-clear"
    ident = resolve_identity(cwd=cwd, job_dir=job_dir, root=root, project_label=project_label)
    if bound is not None:
        from src.orchestrator.agents import _generation
        if _generation(bound.agent_id)[0] != _generation(ident.agent_id)[0]:
            # the deliberate binding wins: seams (swap/compaction) run on the SEAT's lineage
            ident.agent_id = bound.agent_id
    await register_agent(actions, ident, actor=actor, expected_model=expected_model,
                         mint_reason=mint_reason)
    prev = None
    if job_dir:
        prev = await mounts.save_mount(
            actions.pool, job_dir=job_dir, agent_id=ident.agent_id, project=ident.project,
            cwd=cwd, model=ident.model, session_key=f"whisper:{session_id[:8]}")
        if prev is None:  # a fresh session: anchor the fold on the lineage's last life
            # ...and a JOINER inherits the room's collective settle-state (sibling-settled
            # broadcasts are not a newcomer's unread; truly-open mail still greets it)
            await settle_history_at_join(actions.pool, ident.project, ident.agent_id)
            prev = await mounts.project_prev_seen(
                actions.pool, ident.project, exclude_job_dir=job_dir)
    mail = await unread_count(actions.pool, ident.project, reader_agent=ident.agent_id,
                              lease_secs=lease_secs) if ident.project else 0
    desk = await unread_count(actions.pool, OPERATOR_ADDR, reader_agent=OPERATOR_ADDR,
                              lease_secs=lease_secs)
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
        "desk": desk,
        "pulse": pulse,
        "away": away,
        # the durable anchor for THIS session (derived from its id, not $CLAUDE_JOB_DIR which is
        # empty in plain sessions). The whisper hands it to the agent so any later mount() — even
        # a reconnect re-mount — carries the real anchor and RE-ATTACHES instead of minting a twin
        # (thread 883a24f4). Distinct per session, so co-located agents (monsterhouse cloud+engine
        # on one dir) never collide: each has its own session id → its own job_dir.
        "job_dir": job_dir,
        # the SEAT: the agent's claimed human name + generation ('Thoth', 'Anna II'), or None
        # if still anonymous — the whisper offers a claim in that case.
        "seat": await _seat_of(actions, ident.agent_id),
        # thin=True → the whisper says plainly: YOUR project is young; the GRAPH is not.
        "thin": thin,
    }


async def _seat_of(actions: Actions, agent_id: str) -> str | None:
    from src.orchestrator.agents import seat_label
    handle = await actions.pool.fetchval(
        "SELECT value#>>'{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='handle'", agent_id)
    return seat_label(agent_id, handle)
