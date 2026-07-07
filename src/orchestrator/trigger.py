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
import os
import tempfile
from pathlib import Path
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.config.settings import Settings, get_settings
from src.orchestrator.mailbox import OPERATOR_ADDR

_log = logging.getLogger("osiris.trigger")

# Where a spawned wake's synthesized CLAUDE_JOB_DIR lives. A triggered `claude -p` inherits no
# job dir from any harness, so the woken agent has no durable identity anchor and mounts by
# GUESSING off the box's hottest transcript (a co-tenant's). We hand it one: `<base>/jobs/wake-<id>`
# — the literal 'jobs' segment is what _job_id parses, so mount(job_dir=$CLAUDE_JOB_DIR) resolves a
# stable, distinct agent:wake-<id> instead. Under the system temp: ephemeral, no cleanup owed.
_WAKE_JOB_ROOT = Path(tempfile.gettempdir()) / "osiris-wakes"

_WAKE_PROMPT = (
    'You have unread Osiris mail. Call mount(cwd="{repo}", job_dir=$CLAUDE_JOB_DIR), then '
    "inbox(), then act on what it asks. Write back as you go (record_decision / open_thread / "
    "resolve_thread). SETTLE each message you have handled — reply with send(reply_to=<id>) "
    "or ack with inbox(ack=[ids]); unsettled mail redelivers and re-wakes you. Reply ONLY if "
    "it carries NEW information — never an acknowledgement-only message (that would just wake "
    "the sender again). REPORT UP (the operator must see the loop close): when this exchange "
    "CONCLUDES — a finding established, work divided, a decision made — record_decision the "
    "outcome AND send(to='operator') a three-line brief. If nothing needs doing, do nothing."
)


def should_wake(
    *, enabled: bool, recent_wakes: int, rate_cap: int, within_grace: bool = False
) -> str | None:
    """The bounded decision (pure). Returns a SKIP REASON, or None to WAKE. The kill switch and
    the per-project rate cap are the safety — a ping-pong hits the cap and halts. `within_grace`
    is the double-wake guard: a project woken moments ago is still spawning/mounting (~100s+),
    so its mail only LOOKS unhandled — skip as 'wake-grace', distinct from the 'rate-capped' bound
    (the cap wins when both apply — the harder signal). grace expiry re-arms the wake."""
    if not enabled:
        return "disabled"
    if recent_wakes >= rate_cap:
        return "rate-capped"
    if within_grace:
        return "wake-grace"
    return None


async def _projects_with_unread(
    pool: asyncpg.Pool, lease_secs: int
) -> list[tuple[str, int, str | None]]:
    """(project, oldest_deliverable_message_id, its_sender) for every project with deliverable
    mail. DELIVERABLE, not merely unsettled: mail under a live lease is being processed right
    now — re-waking on it would double-spawn; if the processing died, lease expiry re-arms the
    wake. The operator address is skipped — it is a desk, not a repo (never woken)."""
    rows = await pool.fetch(
        "SELECT DISTINCT ON (to_project) to_project, id, from_agent FROM fleet_messages "
        "WHERE to_project <> $1 AND read_at IS NULL AND (delivered_at IS NULL "
        "OR delivered_at < now() - make_interval(secs => $2)) "
        "ORDER BY to_project, created_at", OPERATOR_ADDR, lease_secs)
    return [(r["to_project"], r["id"], r["from_agent"]) for r in rows]


async def _recent_wakes(pool: asyncpg.Pool, project: str, window_secs: int) -> int:
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT count(*) FROM agent_wakes WHERE to_project=$1 "
        "AND woke_at > now() - make_interval(secs => $2)", project, window_secs)


async def _woken_within(pool: asyncpg.Pool, project: str, grace_secs: int) -> bool:
    """True if this project was woken within the last `grace_secs` — a wake still in flight (the
    agent is spawning/mounting/leasing, ~100s+). grace_secs<=0 disables the grace (only the rate
    cap bounds then). Reads the same ledger as the cap, on a shorter, per-message-latency window."""
    if grace_secs <= 0:
        return False
    return bool(await pool.fetchval(
        "SELECT 1 FROM agent_wakes WHERE to_project=$1 "
        "AND woke_at > now() - make_interval(secs => $2) LIMIT 1", project, grace_secs))


def _wake_job_dir(wake_id: int) -> str:
    """A durable per-wake CLAUDE_JOB_DIR (a real created dir). The token 'wake-<row id>' is stable
    and unique — derived from the ledger row just inserted, never Date-random, so the woken agent
    resolves to the same agent:wake-<id> across a re-attach and tests stay deterministic. The
    literal 'jobs' segment is exactly what _job_id parses to that token."""
    d = _WAKE_JOB_ROOT / "jobs" / f"wake-{wake_id}"
    d.mkdir(parents=True, exist_ok=True)
    return str(d)


async def wake_status(pool: asyncpg.Pool, project: str, st: Settings) -> str:
    """What the trigger would do for this project right now — the sender-visible signal
    (send() surfaces it so 'busy listener' is distinguishable from 'feature off'). The
    operator address is a desk, not a repo: 'operator (read at the desk, never woken)'."""
    if project == OPERATOR_ADDR:
        return "operator (read at the desk, never woken)"
    reason = should_wake(
        enabled=st.osiris_trigger_enabled,
        recent_wakes=await _recent_wakes(pool, project, st.osiris_trigger_window_secs),
        rate_cap=st.osiris_trigger_rate_cap,
        within_grace=await _woken_within(pool, project, st.osiris_trigger_grace_secs))
    return reason if reason is not None else "armed"


async def _repo_path(pool: asyncpg.Pool, project: str) -> str | None:
    """The recipient project's on-disk repo — the cwd of a registered Agent that works there
    (stored at mount). None if unknown (then we can't spawn; the mail stays pull-only)."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT cw.value #>> '{}' FROM objects o "
        "JOIN current_assertions pr ON pr.object_id=o.id AND pr.name='project' "
        "JOIN current_assertions cw ON cw.object_id=o.id AND cw.name='cwd' "
        "WHERE o.type='Agent' AND pr.value #>> '{}' = $1 "
        "ORDER BY cw.observed_at DESC LIMIT 1", project)


async def _spawn_claude(repo: str, prompt: str, job_dir: str) -> None:
    """Wake an agent: a detached `claude -p` in the repo (the same CLI the extractor uses). The
    worker rings the bell; Claude decides. The child inherits our environment PLUS a synthesized
    CLAUDE_JOB_DIR — the durable identity anchor a triggered `claude -p` gets from no harness — so
    the woken agent mounts as a distinct agent:wake-<id>, not a guess off a co-tenant's transcript.
    Fire-and-forget — the woken agent runs on its own."""
    env = {**os.environ, "CLAUDE_JOB_DIR": job_dir}
    proc = await asyncio.create_subprocess_exec(
        "claude", "-p", prompt, cwd=repo, env=env,
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
    )
    _log.info("trigger: woke an agent in %s (pid %s, job_dir %s)", repo, proc.pid, job_dir)


async def trigger_mail_tick(
    actions: Actions, *, settings: Settings | None = None, spawn: Any = _spawn_claude
) -> dict[str, int]:
    """One trigger pass: for each project with unread mail, apply the bounded decision, then wake
    (or skip). `spawn` is injected so tests assert the DECISION without launching a process. The
    wake is RECORDED before the spawn — the ledger is both the rate limiter and the chain."""
    st = settings or get_settings()
    pool = actions.pool
    report = {"woke": 0, "skipped": 0}
    for project, msg_id, sender in await _projects_with_unread(pool, st.osiris_mail_lease_secs):
        recent = await _recent_wakes(pool, project, st.osiris_trigger_window_secs)
        within_grace = await _woken_within(pool, project, st.osiris_trigger_grace_secs)
        if should_wake(enabled=st.osiris_trigger_enabled, recent_wakes=recent,
                       rate_cap=st.osiris_trigger_rate_cap,
                       within_grace=within_grace) is not None:
            report["skipped"] += 1
            continue
        repo = await _repo_path(pool, project)
        if repo is None:  # no known repo → can't spawn; the mail stays pull-only
            report["skipped"] += 1
            continue
        wake_id = await pool.fetchval(
            "INSERT INTO agent_wakes (to_project, from_agent, message_id) VALUES ($1,$2,$3) "
            "RETURNING id", project, sender, msg_id)
        await spawn(repo, _WAKE_PROMPT.format(repo=repo), _wake_job_dir(wake_id))
        report["woke"] += 1
    return report
