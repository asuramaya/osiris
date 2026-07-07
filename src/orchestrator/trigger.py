"""The fleet trigger-hook — the mailbox's alarm clock.

The mailbox is PULL-based (Osiris has no hands): an agent perceives mail only when it takes a
turn, so coordination waits for the operator to hand-trigger the recipient. This closes that gap
WITHOUT giving Osiris hands: the WORKER (the sanctioned alarm clock / tripwire, rule #2) spawns
`claude -p` in a recipient project's repo when it has unread mail; Claude — the intelligence —
then mounts, reads its inbox, and decides. The membrane (rule #6): the loop may close, but never
silently and never irreversibly.

The named danger is the A↔B ping-pong (recursion). It is bounded by a per-project RATE CAP: at
most `rate_cap` wakes per project per `window` — each side of a loop hits its own cap and halts,
no cross-process depth-propagation needed. Every wake is recorded in agent_wakes (the visible
chain), and the whole thing is OFF unless osiris_trigger_enabled (the kill switch). Conservative
by construction: bounded, visible, killable.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.config.settings import Settings, get_settings

_log = logging.getLogger("osiris.trigger")

_WAKE_PROMPT = (
    'You have unread Osiris mail. Call mount(cwd="{repo}", job_dir=$CLAUDE_JOB_DIR), then '
    "inbox(), then act on what it asks. Write back as you go (record_decision / open_thread / "
    "resolve_thread). Reply via send(to=...) ONLY if a reply carries NEW information — never an "
    "acknowledgement-only message (that would just wake the sender again). If nothing needs "
    "doing, do nothing."
)


def should_wake(*, enabled: bool, recent_wakes: int, rate_cap: int) -> str | None:
    """The bounded decision (pure). Returns a SKIP REASON, or None to WAKE. The kill switch and
    the per-project rate cap are the whole safety story — a ping-pong hits the cap and halts."""
    if not enabled:
        return "disabled"
    if recent_wakes >= rate_cap:
        return "rate-capped"
    return None


async def _projects_with_unread(pool: asyncpg.Pool) -> list[tuple[str, int, str | None]]:
    """(project, oldest_unread_message_id, its_sender) for every project with unread mail."""
    rows = await pool.fetch(
        "SELECT DISTINCT ON (to_project) to_project, id, from_agent FROM fleet_messages "
        "WHERE read_at IS NULL ORDER BY to_project, created_at")
    return [(r["to_project"], r["id"], r["from_agent"]) for r in rows]


async def _recent_wakes(pool: asyncpg.Pool, project: str, window_secs: int) -> int:
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT count(*) FROM agent_wakes WHERE to_project=$1 "
        "AND woke_at > now() - make_interval(secs => $2)", project, window_secs)


async def _repo_path(pool: asyncpg.Pool, project: str) -> str | None:
    """The recipient project's on-disk repo — the cwd of a registered Agent that works there
    (stored at mount). None if unknown (then we can't spawn; the mail stays pull-only)."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT cw.value #>> '{}' FROM objects o "
        "JOIN current_assertions pr ON pr.object_id=o.id AND pr.name='project' "
        "JOIN current_assertions cw ON cw.object_id=o.id AND cw.name='cwd' "
        "WHERE o.type='Agent' AND pr.value #>> '{}' = $1 "
        "ORDER BY cw.observed_at DESC LIMIT 1", project)


async def _spawn_claude(repo: str, prompt: str) -> None:
    """Wake an agent: a detached `claude -p` in the repo (the same CLI the extractor uses). The
    worker rings the bell; Claude decides. Fire-and-forget — the woken agent runs on its own."""
    proc = await asyncio.create_subprocess_exec(
        "claude", "-p", prompt, cwd=repo,
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
    )
    _log.info("trigger: woke an agent in %s (pid %s)", repo, proc.pid)


async def trigger_mail_tick(
    actions: Actions, *, settings: Settings | None = None, spawn: Any = _spawn_claude
) -> dict[str, int]:
    """One trigger pass: for each project with unread mail, apply the bounded decision, then wake
    (or skip). `spawn` is injected so tests assert the DECISION without launching a process. The
    wake is RECORDED before the spawn — the ledger is both the rate limiter and the chain."""
    st = settings or get_settings()
    pool = actions.pool
    report = {"woke": 0, "skipped": 0}
    for project, msg_id, sender in await _projects_with_unread(pool):
        recent = await _recent_wakes(pool, project, st.osiris_trigger_window_secs)
        if should_wake(enabled=st.osiris_trigger_enabled, recent_wakes=recent,
                       rate_cap=st.osiris_trigger_rate_cap) is not None:
            report["skipped"] += 1
            continue
        repo = await _repo_path(pool, project)
        if repo is None:  # no known repo → can't spawn; the mail stays pull-only
            report["skipped"] += 1
            continue
        await pool.execute(
            "INSERT INTO agent_wakes (to_project, from_agent, message_id) VALUES ($1,$2,$3)",
            project, sender, msg_id)
        await spawn(repo, _WAKE_PROMPT.format(repo=repo))
        report["woke"] += 1
    return report
