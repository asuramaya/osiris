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
from src.ingest.sessions import locate_current_transcript
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
    "outcome AND send(to='operator') a three-line brief. If nothing needs doing, do nothing. "
    "ECONOMY: you are a TRIAGE wake — if the mail demands real work (analysis, building, long "
    "reads), do NOT grind it here: open_thread(kind='obligation') describing it, reply with "
    "that pointer (which settles the mail), and let a full session take it."
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


_RESUME_PROMPT = (
    "New Osiris mail for your project — you are RESUMED (your context is your own: do NOT "
    "re-mount or re-orient unless your connection demands it; you already know this world). "
    "inbox(), act on what it asks, SETTLE each handled message — reply with send(reply_to=<id>) "
    "or ack with inbox(ack=[ids]). Reply ONLY with NEW information — never an "
    "acknowledgement-only message. When the exchange CONCLUDES, record_decision the outcome "
    "AND send(to='operator') a three-line brief. If nothing needs doing, do nothing."
)


async def _owner_live(pool: asyncpg.Pool, project: str, within_secs: int) -> bool:
    """A mount fresher than the liveness window = an awake owner. DELIVER, don't spawn: its
    own chrome/orient shows the mail; waking a twin beside a live owner is the fragmentation
    heinrich reported (thread 9f2ddb44 — 'strangers worked in my name')."""
    return bool(await pool.fetchval(
        "SELECT 1 FROM agent_mounts WHERE project=$1 "
        "AND last_seen > now() - make_interval(secs => $2) LIMIT 1", project, within_secs))


async def _retired(pool: asyncpg.Pool, agent_canonical: str) -> bool:
    """True if the agent carries a winning retired=true — a deliberate close (operator's or its
    own farewell). The trigger must never reanimate what was deliberately closed."""
    v = await pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='retired' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", agent_canonical)
    return bool(v == "true")


def _pick_resumable_sync(
    cands: list[tuple[str, str]], root: Path, ceiling_bytes: int
) -> tuple[str, str] | None:
    """The disk half of resume-resolution (sync — called via to_thread): for each candidate
    (job_dir, cwd), anchor its transcript and check the context ceiling. Returns
    (full_session_id, cwd) for the first resumable owner. The transcript stem IS the session
    id `claude --resume` takes; a transcript at the ceiling is retirement-by-compaction
    territory — resuming it would replay heinrich's 21:30 case, which was LEGITIMATE
    succession."""
    for job_dir, cwd in cands:
        t = locate_current_transcript(root, job_dir, anchored_only=True)
        if t is None:
            continue
        try:
            if t.stat().st_size > ceiling_bytes:
                continue
        except OSError:
            continue
        return t.stem, cwd
    return None


async def _resumable_owner(
    pool: asyncpg.Pool, project: str, st: Settings
) -> tuple[str, str] | None:
    """(session_id, repo_cwd) of the project's freshest RESUMABLE owner, else None: not
    retired (graph check), transcript anchored on its own job_dir (never a co-tenant's), and
    below the context ceiling."""
    rows = await pool.fetch(
        "SELECT agent_id, job_dir, cwd FROM agent_mounts WHERE project=$1 "
        "ORDER BY last_seen DESC LIMIT 5", project)
    cands: list[tuple[str, str]] = []
    for r in rows:
        if not await _retired(pool, r["agent_id"]):
            cands.append((r["job_dir"], r["cwd"]))
    if not cands:
        return None
    root = Path(st.osiris_sense_sessions) if st.osiris_sense_sessions \
        else Path.home() / ".claude" / "projects"
    return await asyncio.to_thread(
        _pick_resumable_sync, cands, root, st.osiris_resume_ceiling_bytes)


async def _last_wake_mode(pool: asyncpg.Pool, project: str, message_id: int) -> str | None:
    """How the LAST wake for this exact message dispatched — the alternation guard's input:
    a resume that never leased its mail (still deliverable past grace) is not retried; the
    next wake mints. Never two consecutive resume attempts on one undelivered message."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT mode FROM agent_wakes WHERE to_project=$1 AND message_id=$2 "
        "ORDER BY woke_at DESC LIMIT 1", project, message_id)


async def _spawn_claude(
    repo: str, prompt: str, *, job_dir: str | None = None, resume_session: str | None = None,
    model: str | None = None,
) -> None:
    """Wake an agent: a detached `claude -p` in the repo. RESUME lane: `--resume <session>`
    continues the owner's own session — it pays only for the new mail, not a fresh cosmology
    (thread 9f2ddb44). MINT lane: a fresh process with a synthesized CLAUDE_JOB_DIR — the
    durable identity anchor a triggered `claude -p` gets from no harness. Fire-and-forget."""
    env = os.environ.copy()
    cmd = ["claude", "-p"]
    if model:  # wake economics: triage wakes on a cheaper model; the prompt escalates real work
        cmd += ["--model", model]
    if resume_session:
        cmd += ["--resume", resume_session]
    if job_dir:
        env["CLAUDE_JOB_DIR"] = job_dir
    cmd.append(prompt)
    proc = await asyncio.create_subprocess_exec(
        *cmd, cwd=repo, env=env,
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
    )
    _log.info("trigger: woke %s in %s (pid %s)",
              f"resume:{resume_session}" if resume_session else f"mint:{job_dir}",
              repo, proc.pid)


async def trigger_mail_tick(
    actions: Actions, *, settings: Settings | None = None, spawn: Any = _spawn_claude
) -> dict[str, int]:
    """One trigger pass — the dispatch order is DELIVER → RESUME → MINT (thread 9f2ddb44):
    a live owner just gets its mail (no spawn); a resumable owner is CONTINUED via its own
    session (cheap — no re-ingestion, no twin, no succession seam); only otherwise is a fresh
    twin minted (succession-stamped at mount, 88ca0a1). `spawn` is injected so tests assert
    the DECISION without launching a process. The wake is RECORDED (with its mode) before the
    spawn — the ledger is the rate limiter, the chain, and the alternation guard."""
    st = settings or get_settings()
    pool = actions.pool
    report = {"woke": 0, "resumed": 0, "skipped": 0, "owner_live": 0}
    for project, msg_id, sender in await _projects_with_unread(pool, st.osiris_mail_lease_secs):
        if not st.osiris_trigger_enabled:
            report["skipped"] += 1
            continue
        if await _owner_live(pool, project, st.osiris_owner_live_secs):
            report["owner_live"] += 1  # deliver: the awake owner reads its own box
            continue
        recent = await _recent_wakes(pool, project, st.osiris_trigger_window_secs)
        within_grace = await _woken_within(pool, project, st.osiris_trigger_grace_secs)
        if should_wake(enabled=True, recent_wakes=recent,
                       rate_cap=st.osiris_trigger_rate_cap,
                       within_grace=within_grace) is not None:
            report["skipped"] += 1
            continue
        resume = None
        if await _last_wake_mode(pool, project, msg_id) != "resume":  # alternation guard
            resume = await _resumable_owner(pool, project, st)
        if resume is not None:
            session_id, repo = resume
            await pool.execute(
                "INSERT INTO agent_wakes (to_project, from_agent, message_id, mode) "
                "VALUES ($1,$2,$3,'resume')", project, sender, msg_id)
            await spawn(repo, _RESUME_PROMPT, resume_session=session_id,
                        model=st.osiris_wake_model or None)
            report["resumed"] += 1
            report["woke"] += 1
            continue
        repo_path = await _repo_path(pool, project)
        if repo_path is None:  # no known repo → can't spawn; the mail stays pull-only
            report["skipped"] += 1
            continue
        wake_id = await pool.fetchval(
            "INSERT INTO agent_wakes (to_project, from_agent, message_id, mode) "
            "VALUES ($1,$2,$3,'mint') RETURNING id", project, sender, msg_id)
        await spawn(repo_path, _WAKE_PROMPT.format(repo=repo_path),
                    job_dir=_wake_job_dir(wake_id), model=st.osiris_wake_model or None)
        report["woke"] += 1
    return report
