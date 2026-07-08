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
from src.orchestrator.mailbox import OPERATOR_ADDR, unread_count


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
) -> dict[str, Any]:
    """Mount a just-started session and return its whisper payload. Identical semantics to
    the mount() tool (same resolution, same registration, same durable row — idempotent on
    re-fire), plus the glance the whisper prints: mail, desk, pulse, and the away fold."""
    job_dir = _derive_job_dir(session_id, jobs_home=jobs_home)
    ident = resolve_identity(cwd=cwd, job_dir=job_dir, root=root)
    await register_agent(actions, ident, actor=actor, expected_model=expected_model)
    prev = None
    if job_dir:
        prev = await mounts.save_mount(
            actions.pool, job_dir=job_dir, agent_id=ident.agent_id, project=ident.project,
            cwd=cwd, model=ident.model, session_key=f"whisper:{session_id[:8]}")
        if prev is None:  # a fresh session: anchor the fold on the lineage's last life
            prev = await mounts.project_prev_seen(
                actions.pool, ident.project, exclude_job_dir=job_dir)
    mail = await unread_count(actions.pool, ident.project, lease_secs=lease_secs) \
        if ident.project else 0
    desk = await unread_count(actions.pool, OPERATOR_ADDR, lease_secs=lease_secs)
    away = await mounts.while_away(actions.pool, ident.project, ident.agent_id, prev)
    try:
        pulse: str | None = await mounts.fleet_pulse(actions.pool, lease_secs=lease_secs)
    except Exception:  # noqa: BLE001 — the pulse must never break the whisper
        pulse = None
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
    }
