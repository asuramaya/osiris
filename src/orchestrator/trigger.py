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
import contextlib
import logging
import os
import tempfile
from pathlib import Path
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.config.settings import Settings, get_settings
from src.ingest.sessions import locate_current_transcript
from src.orchestrator.bodies import BodyProvider, LocalProvider
from src.orchestrator.ceiling import may_spend
from src.orchestrator.mailbox import OPERATOR_ADDR, send_message

# Where a wake drops the CLI's own cost envelope (`--output-format json` -> total_cost_usd).
# Outside the transcript tree ON PURPOSE: everything under ~/.claude/projects is read by the
# liveness observer, the orphan reaper and the adversary, and a receipt landing in there would
# be Osiris sensing its own exhaust — the loop-pathology class, and the exact shape of the bug
# where the miner mined its own alarm clock.
RECEIPTS = Path.home() / ".osiris" / "wake-receipts"

_log = logging.getLogger("osiris.trigger")

# the trigger speaks in its own name when it gives up — never in an agent's
_TRIGGER_AGENT = "agent:osiris-trigger"

# Where a spawned wake's synthesized CLAUDE_JOB_DIR lives. A triggered `claude -p` inherits no
# job dir from any harness, so the woken agent has no durable identity anchor and mounts by
# GUESSING off the box's hottest transcript (a co-tenant's). We hand it one: `<base>/jobs/wake-<id>`
# — the literal 'jobs' segment is what _job_id parses, so mount(job_dir=$CLAUDE_JOB_DIR) resolves a
# stable, distinct agent:wake-<id> instead. Under the system temp: ephemeral, no cleanup owed.
_WAKE_JOB_ROOT = Path(tempfile.gettempdir()) / "osiris-wakes"

_WAKE_PROMPT = (
    'You have unread Osiris mail. Call mount(cwd="{repo}", job_dir="{job_dir}"), then '
    "inbox(peek=true) — a peek leases nothing, and you can settle straight from it: "
    "inbox(ack=[the ids you rendered]) for FYI/no-action mail; only lease (non-peek) what "
    "needs deeper handling. Act on what it asks. Write back as you go (record_decision / "
    "open_thread / resolve_thread). SETTLE each message you have handled — reply with "
    "send(reply_to=<id>) "
    "or ack with inbox(ack=[ids]); unsettled mail redelivers and re-wakes you. Reply ONLY if "
    "it carries NEW information — never an acknowledgement-only message (that would just wake "
    "the sender again). REPORT UP (the operator must see the loop close): when this exchange "
    "CONCLUDES — a finding established, work divided, a decision made — record_decision the "
    "outcome AND send(to='operator', desk=<your triage: 'decision' if only the human can "
    "call it, 'hands' if blocked on their physical/authorization act, 'fyi' for loop-closed "
    "status>) a three-line brief — and if you have briefed the desk "
    "on this SAME topic before, send(reply_to=<your prior brief id>) instead of a fresh "
    "send: the desk folds superseded briefs under your newest one. If nothing needs doing, "
    "do nothing. "
    "ECONOMY: you are a TRIAGE wake — if the mail demands real work (analysis, building, long "
    "reads), do NOT grind it here: open_thread(kind='obligation') describing it, reply with "
    "that pointer (which settles the mail), and let a full session take it. "
    "YOU MUST NOT LEAVE MAIL UNSETTLED. This is the one rule with teeth. A letter you leave "
    "untouched does not wait politely for a better reader — it RE-ARMS THIS WAKE and spawns "
    "another session exactly like you, forever. On 2026-07-12 one letter addressed 'to whoever "
    "mounts this project next' spawned 79 of us over 18 hours on a project its owner had not "
    "opened in two days: every wake read it, correctly judged it was not theirs to ack, left it "
    "politely alone, and thereby summoned its replacement. If mail is not yours to act on, that "
    "is not a reason to leave it — it is the THIRD DOOR: open_thread(kind='obligation') carrying "
    "what it asks, then ack it. The letter is not lost (nothing is ever deleted; it stays "
    "readable in the graph and the thread now carries its duty) — it simply stops ringing. "
    "ONE-SHOT (wake hygiene, thread fc2071f8): before your final word, call retire() — a "
    "triage wake's face closes cleanly (no zombie card, no reanimation target); everything "
    "you settled outlives you in the graph."
)


# WAKE ECONOMICS (obligation 4e52af7e): defer non-urgent wakes when the fleet-wide hourly
# spend nears its ceiling. The scheduler now reads the SAME ledger the chrome displays
# ('wakes N/h') instead of ignoring it. Urgent mail (the operator's word, or mail old
# enough that deferral would become starvation) rides through until the hard ceiling.
_BUDGET_DEFER_AT = 0.8      # the soft ceiling: past this share, only urgent mail wakes
_URGENT_AGE_SECS = 3600     # deferred this long, mail is urgent by starvation


def should_wake(
    *, enabled: bool, recent_wakes: int, rate_cap: int, within_grace: bool = False,
    hourly_wakes: int = 0, hourly_budget: int = 0, urgent: bool = False,
    attempts: int = 0, attempt_limit: int = 0,
) -> str | None:
    """The bounded decision (pure). Returns a SKIP REASON, or None to WAKE.

    A RATE IS NOT A BOUND. Every other guard here — the per-project cap, the hourly budget, the
    grace — measures wakes over a SLIDING WINDOW, so every one of them RESETS and the wake fires
    again. On 2026-07-12 that let ONE unread letter ("to whoever mounts <project> next") spawn
    79 `claude -p` sessions over 18 hours on a project the operator had not opened in two days,
    minting a fresh agent every ~32 minutes, at exactly the cap. The cap was working perfectly.
    THAT WAS THE BUG: it capped the RATE and nothing capped the TOTAL, so a message that could
    never be settled became a permanent alarm clock ticking at the legal limit.

    `attempts` is the lifetime count of wakes on THIS MESSAGE and `attempt_limit` bounds it — a
    TOTAL, which never resets. It is checked FIRST and `urgent` cannot override it: a message that
    has failed three times is still failing, and urgency is not a reason to keep failing louder.
    A retry that has failed 79 times is not a retry, it is a leak. When the limit is hit the
    caller must ESCALATE to the human and stop (the membrane: a loop may close, but never
    silently — and this one never closed at all).

    (The DM lane already knew this — "one resume attempt per message: a resume that didn't settle
    it is not looped". The broadcast lane simply never learned it.)
    """
    if not enabled:
        return "disabled"
    if attempt_limit > 0 and attempts >= attempt_limit:
        return "unsettleable"
    if recent_wakes >= rate_cap:
        return "rate-capped"
    if hourly_budget > 0 and hourly_wakes >= hourly_budget:
        return "budget-exhausted"
    if hourly_budget > 0 and not urgent and hourly_wakes >= _BUDGET_DEFER_AT * hourly_budget:
        return "budget-deferred"
    if within_grace:
        return "wake-grace"
    return None


async def _projects_with_unread(
    pool: asyncpg.Pool, lease_secs: int
) -> list[tuple[str, int, str | None, float]]:
    """(project, oldest_deliverable_message_id, its_sender, age_secs) for every project with
    deliverable
    BROADCAST mail. DELIVERABLE = no recipient has settled it AND none holds a live lease (mail
    being processed right now would double-spawn if re-woken; lease expiry re-arms). Broadcasts
    only: the wake ensures SOMEONE in the project looks, and a broadcast read by one agent still
    shows in the others' inboxes. DMs rely on pull until agent-precise waking (a later phase).
    The operator desk is skipped — never woken."""
    rows = await pool.fetch(
        "SELECT DISTINCT ON (m.to_project) m.to_project, m.id, m.from_agent, "
        " extract(epoch FROM (now() - m.created_at)) AS age_secs FROM fleet_messages m "
        "WHERE m.to_project <> $1 AND m.to_agent IS NULL AND m.read_at IS NULL "
        "AND NOT EXISTS (SELECT 1 FROM message_recipients r WHERE r.message_id=m.id "
        "  AND r.read_at IS NOT NULL) "
        "AND NOT EXISTS (SELECT 1 FROM message_recipients r WHERE r.message_id=m.id "
        "  AND r.delivered_at >= now() - make_interval(secs => $2)) "
        "ORDER BY m.to_project, m.created_at", OPERATOR_ADDR, lease_secs)
    return [(r["to_project"], r["id"], r["from_agent"], float(r["age_secs"] or 0))
            for r in rows]


async def _recent_wakes(pool: asyncpg.Pool, project: str, window_secs: int) -> int:
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT count(*) FROM agent_wakes WHERE to_project=$1 "
        "AND woke_at > now() - make_interval(secs => $2)", project, window_secs)


async def _attempts_on(pool: asyncpg.Pool, message_id: int) -> int:
    """How many times we have woken ANYONE for THIS message, ever. No window: this is the total
    the rate-limiters never kept, and its absence is what let one letter spawn 79 sessions."""
    n: int | None = await pool.fetchval(
        "SELECT count(*) FROM agent_wakes WHERE message_id=$1 AND mode <> 'abandoned'",
        message_id)
    return n or 0


async def _abandon(
    pool: asyncpg.Pool, project: str, message_id: int, sender: str | None, attempts: int,
) -> bool:
    """GIVE UP, AND SAY SO. The loop may close, but never silently — and a wake loop that simply
    kept firing never closed at all.

    Some mail CANNOT be settled by a triage wake, and no number of retries will change that. The
    letter that caused the 2026-07-12 storm was addressed "to whoever mounts <project> next" —
    a message for a future full session. Every wake read it, correctly judged it was not FYI it
    could ack, left it unsettled exactly as instructed... and thereby summoned its replacement.
    THE LETTER'S OWN POLITENESS WAS THE FUEL. Every agent in that chain behaved perfectly and the
    machine still burned for 18 hours.

    So after `attempt_limit` tries we stop waking on this message FOREVER (the 'abandoned' row is
    the tombstone, and it is excluded from the attempt count so it cannot re-arm anything) and we
    hand it to the one reader who can actually act: the human. Idempotent — one brief, not one per
    tick. Returns True when this call is the one that abandoned it."""
    already = await pool.fetchval(
        "SELECT 1 FROM agent_wakes WHERE message_id=$1 AND mode='abandoned'", message_id)
    if already:
        return False
    await pool.execute(
        "INSERT INTO agent_wakes (to_project, from_agent, message_id, mode) "
        "VALUES ($1,$2,$3,'abandoned')", project, sender, message_id)
    body = (
        f"UNSETTLEABLE MAIL — I woke {project} {attempts} time(s) for message {message_id} and "
        f"no agent ever settled it, so I have STOPPED waking on it. It is not a retry any more; "
        f"it is a leak.\n\n"
        f"A triage wake can only ack mail it can dispose of. Mail addressed to a future session "
        f"(\"to whoever mounts {project} next\"), or asking for a judgement only you can make, "
        f"will never be settled by a machine reading its inbox — it will only summon another one. "
        f"That is the loop this stop exists to end.\n\n"
        f"It needs a real session on {project}, or your word. Read it at /mail?box={project} "
        f"(message {message_id}); nothing is lost and nothing was deleted. No further wake will "
        f"fire for it."
    )
    with contextlib.suppress(Exception):  # a failed brief must never re-arm the wake
        await send_message(pool, from_agent=_TRIGGER_AGENT, from_project="osiris",
                           to_project=OPERATOR_ADDR, body=body, desk_kind="decision")
    _log.warning("wake ABANDONED: project=%s message=%s after %d attempts",
                 project, message_id, attempts)
    return True


async def _woken_within(pool: asyncpg.Pool, project: str, grace_secs: int) -> bool:
    """True if this project was woken within the last `grace_secs` — a wake still in flight (the
    agent is spawning/mounting/leasing, ~100s+). grace_secs<=0 disables the grace (only the rate
    cap bounds then). Reads the same ledger as the cap, on a shorter, per-message-latency window."""
    if grace_secs <= 0:
        return False
    return bool(await pool.fetchval(
        "SELECT 1 FROM agent_wakes WHERE to_project=$1 "
        "AND woke_at > now() - make_interval(secs => $2) LIMIT 1", project, grace_secs))


def _wake_job_dir(project: str) -> str:
    """ONE GHOST PER PROJECT, reused forever — not one per wake.

    This was keyed on the WAKE ROW ID, so every wake resolved to a brand-new agent:wake-<id>: the
    trigger minted 463 identities over the fleet's life and the roster filled with strangers the
    operator never started (48 on ONE project alone, which he had not opened in two days).
    A wake is not a new MIND, it is the same errand run again — so it gets one stable name per
    house, `agent:wake-<project>`, and re-wearing it is the whole point.

    Worse, none of it ever ran: the anchor was handed to the agent as the literal text
    `$CLAUDE_JOB_DIR` inside a PROMPT, and a woken agent has no shell to expand it with (its
    tools are `mcp__osiris` only). The mount-anchor hook rightly refuses a `$`-bearing path and
    fell back to deriving one from the SESSION id — fresh for every `claude -p` — so all 463
    mints got a new identity and the stable anchor was never used once. Ruling 40faa5e6 had
    already fixed this exact bug for the SessionStart whisper ("tell the agent the literal path,
    never $CLAUDE_JOB_DIR") and nobody carried it here. The prompt now carries the real path.

    The literal 'jobs' segment is exactly what _job_id parses to that token.
    """
    slug = "".join(c if (c.isalnum() or c in "-_") else "-" for c in project)[:40] or "unknown"
    d = _WAKE_JOB_ROOT / "jobs" / f"wake-{slug}"
    d.mkdir(parents=True, exist_ok=True)
    return str(d)


async def wake_status(pool: asyncpg.Pool, project: str, st: Settings) -> str:
    """What the trigger would do for this project right now — the sender-visible signal
    (send() surfaces it so 'busy listener' is distinguishable from 'feature off'). The
    operator address is a desk, not a repo: 'operator (read at the desk, never woken)'."""
    if project == OPERATOR_ADDR:
        return "operator (read at the desk, never woken)"
    allow = {p.strip() for p in st.osiris_trigger_projects.split(",") if p.strip()}
    if st.osiris_trigger_enabled and allow and project not in allow:
        # the sender must see the SCOPED truth: "armed" for a project outside a scoped
        # re-arm would be the same false witness the mcp-unit mirror was built to kill
        return f"scoped-out (this re-arm names only: {', '.join(sorted(allow))})"
    hourly = await pool.fetchval(
        "SELECT count(*) FROM agent_wakes WHERE woke_at > now() - interval '1 hour'")
    reason = should_wake(
        enabled=st.osiris_trigger_enabled,
        recent_wakes=await _recent_wakes(pool, project, st.osiris_trigger_window_secs),
        rate_cap=st.osiris_trigger_rate_cap,
        within_grace=await _woken_within(pool, project, st.osiris_trigger_grace_secs),
        hourly_wakes=int(hourly or 0), hourly_budget=st.osiris_wake_hourly_budget)
    if reason == "budget-deferred":
        return "budget-deferred (non-urgent near the hourly ceiling — urgent mail and " \
               "aged mail still wake)"
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
    "AND send(to='operator') a three-line brief (reply_to your prior brief on the same "
    "topic — the desk folds supersessions). If nothing needs doing, do nothing."
)


_DM_RESUME_PROMPT = (
    "A private Osiris DM is waiting for YOU — addressed to your seat; no sibling will read it "
    "for you. You are RESUMED (your context is your own: do NOT re-mount or re-orient unless "
    "your connection demands it). inbox(), act on what it asks, SETTLE it — reply with "
    "send(reply_to=<id>) or ack with inbox(ack=[ids]). Reply ONLY with NEW information — "
    "never an acknowledgement-only message. When the exchange CONCLUDES, record_decision the "
    "outcome AND send(to='operator') a three-line brief (reply_to your prior brief on the "
    "same topic — the desk folds supersessions). If nothing needs doing, ack and stop."
)


# The POKE prompts are the resume prompts' cheaper cousins: the mind's context is LIVE in
# its own window (nothing was resumed, nothing re-ingested) — the message only has to say
# "check your box and settle", then hand the window back to whatever it was doing.
_POKE_PROMPT = (
    "[osiris] New mail for your project, delivered to your OPEN window — your context is "
    "live, nothing was resumed. inbox(), act on what it asks, SETTLE each handled message "
    "(send(reply_to=<id>) or inbox(ack=[ids])), then continue what you were doing. If "
    "nothing needs doing, ack and continue."
)

_DM_POKE_PROMPT = (
    "[osiris] A private DM is waiting for YOU, delivered to your OPEN window — your context "
    "is live, nothing was resumed. inbox(), act on what it asks, SETTLE it "
    "(send(reply_to=<id>) or inbox(ack=[ids])), then continue what you were doing."
)


async def _manager_windows() -> list[dict[str, Any]]:
    """The manager's window roster ([{name, alive, idle_seconds, job_dir?, seat_id?}, ...]) —
    [] when the daemon is down, absent, or slow: the poke lane fails OPEN into the resume
    lane; the trigger never dies of the manager."""
    from src.manager.client import manager_call
    try:
        out = await manager_call({"op": "pty_list"})
    except (OSError, TimeoutError, ValueError):
        return []
    rows = out.get("sessions")
    return [r for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []


async def _poke_window(name: str, text: str, *, dedup: str, min_idle: int) -> dict[str, Any]:
    """One pty_poke through the control socket — the daemon owns the idle gate and the
    dedup memory; failures come back as {'error': ...} instead of raising (the caller's
    ladder falls through on anything that is not a clean 'poked')."""
    from src.manager.client import manager_call
    try:
        return await manager_call({"op": "pty_poke", "name": name, "text": text,
                                   "dedup": dedup, "min_idle_secs": min_idle})
    except (OSError, TimeoutError, ValueError) as exc:
        return {"error": f"manager unreachable: {exc}"}


def _window_for(windows: list[dict[str, Any]], anchor_sids: set[str]) -> str | None:
    """The first live window whose recorded anchor (job_dir, stamped at pty_spawn) belongs
    to one of `anchor_sids` (8-char forms). None = no manager-hosted window for this
    addressee; the ladder proceeds to resume."""
    for w in windows:
        job_dir = w.get("job_dir") or ""
        name = w.get("name")
        if (w.get("alive") and job_dir and isinstance(name, str)
                and Path(job_dir).name[:8] in anchor_sids):
            return name
    return None


async def _dms_with_unread(
    pool: asyncpg.Pool, lease_secs: int
) -> list[tuple[str, int, str | None]]:
    """(addressee, oldest deliverable DM id, sender) for every agent with unsettled DM mail —
    fleet mail phase 3 (task #61). A DM's wake ladder is DELIVER → RESUME → NOTHING: a live
    addressee reads its own box; a stale addressee is resumed via ITS OWN session; and there
    is NO mint lane — a fresh twin is not the addressee, and a private message must never be
    delivered to a stranger. Undeliverable DMs stay pull-only (and follow the seat at the
    next mint — the estate)."""
    rows = await pool.fetch(
        "SELECT DISTINCT ON (m.to_agent) m.to_agent, m.id, m.from_agent FROM fleet_messages m "
        "WHERE m.to_agent IS NOT NULL AND m.to_agent <> $1 AND m.read_at IS NULL "
        "AND NOT EXISTS (SELECT 1 FROM message_recipients r WHERE r.message_id=m.id "
        "  AND r.agent_id=m.to_agent AND r.read_at IS NOT NULL) "
        "AND NOT EXISTS (SELECT 1 FROM message_recipients r WHERE r.message_id=m.id "
        "  AND r.agent_id=m.to_agent AND r.delivered_at >= now() - make_interval(secs => $2)) "
        "ORDER BY m.to_agent, m.created_at", OPERATOR_ADDR, lease_secs)
    return [(r["to_agent"], r["id"], r["from_agent"]) for r in rows]


async def _agent_live(pool: asyncpg.Pool, agent_id: str, within_secs: int) -> bool:
    """Is the ADDRESSEE itself awake (not just some project sibling)? Its chrome/stop-hook
    already surface the DM — waking beside a live addressee would be noise."""
    return bool(await pool.fetchval(
        "SELECT 1 FROM agent_mounts WHERE agent_id=$1 "
        "AND last_seen > now() - make_interval(secs => $2) LIMIT 1", agent_id, within_secs))


async def _agent_resumable(
    pool: asyncpg.Pool, agent_id: str, st: Settings
) -> tuple[str, str] | None:
    """(session_id, repo_cwd) to resume the ADDRESSEE's own session, else None — the same
    checks as the project ladder (not retired, own anchored transcript, below the context
    ceiling) scoped to one agent's mounts."""
    if await _retired(pool, agent_id):
        return None
    rows = await pool.fetch(
        "SELECT job_dir, cwd FROM agent_mounts WHERE agent_id=$1 "
        "ORDER BY last_seen DESC LIMIT 5", agent_id)
    cands = [(r["job_dir"], r["cwd"]) for r in rows]
    if not cands:
        return None
    root = Path(st.osiris_sense_sessions) if st.osiris_sense_sessions \
        else Path.home() / ".claude" / "projects"
    return await asyncio.to_thread(
        _pick_resumable_sync, cands, root, st.osiris_resume_ceiling_bytes)


async def _owner_live(pool: asyncpg.Pool, project: str, within_secs: int) -> bool:
    """A mount fresher than the liveness window = an awake owner. DELIVER, don't spawn: its
    own chrome/orient shows the mail; waking a twin beside a live owner is the fragmentation
    a sibling project reported (thread 9f2ddb44 — 'strangers worked in my name')."""
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
    territory — resuming it would replay a sibling project's 21:30 case, which was LEGITIMATE
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


def _receipt_path(job_dir: str | None, resume_session: str | None) -> Path | None:
    """Where this wake drops the CLI's cost envelope. None when we have nowhere to put it — and
    a wake with nowhere to put its receipt is a wake nobody can ever cost, which the log says
    out loud rather than letting it pass for free."""
    stem = None
    if job_dir:
        stem = Path(job_dir).name
    elif resume_session:
        stem = resume_session[:8]
    if not stem:
        return None
    return RECEIPTS / f"{stem}.json"


async def _room_parent(pool: asyncpg.Pool, project: str) -> str | None:
    """The seat a wake-child belongs to (operator ruling 2026-07-17: wake mints are
    CHILDREN, denominated roman.arabic — 'orphans like that are structurally impossible
    going forward'): the project's freshest NAMED lineage, resolved to its living head.
    None in a seatless room — such a mint stays anonymous, and the archaeologist's
    `seatless` key names the room for the visitor sweep."""
    named = await pool.fetchval(
        "SELECT m.agent_id FROM agent_mounts m WHERE m.project=$1 AND EXISTS ("
        "  SELECT 1 FROM current_assertions h JOIN objects ho ON ho.id=h.object_id "
        "  WHERE h.name='handle' AND (ho.canonical=m.agent_id "
        "        OR m.agent_id LIKE ho.canonical||'-%')) "
        "ORDER BY m.last_seen DESC NULLS LAST LIMIT 1", project)
    if named is None:
        return None
    from src.orchestrator.folds import living_head
    return await living_head(pool, str(named))


async def _spawn_claude(
    repo: str, prompt: str, *, job_dir: str | None = None, resume_session: str | None = None,
    model: str | None = None, allowed_tools: str | None = None,
    spawn_parent: str | None = None,
) -> None:
    """Wake an agent: a detached `claude -p` in the repo. RESUME lane: `--resume <session>`
    continues the owner's own session — it pays only for the new mail, not a fresh cosmology
    (thread 9f2ddb44). MINT lane: a fresh process with a synthesized CLAUDE_JOB_DIR — the
    durable identity anchor a triggered `claude -p` gets from no harness. Fire-and-forget.
    `allowed_tools` (thread ba73c0c8): headless -p cannot answer permission prompts — the
    spawner pre-authorizes the graph tools, or the wake is born with its hands tied."""
    env = os.environ.copy()
    # the spawner's own anchor must never leak into a child: an inherited CLAUDE_JOB_DIR
    # hands the wake the SPAWNER'S identity (the anchor-collision class, 2294e95d) — a
    # child's anchor is the one minted for it below, or none at all.
    env.pop("CLAUDE_JOB_DIR", None)
    cmd = ["claude", "-p", "--output-format", "json"]
    if model:  # wake economics: triage wakes on a cheaper model; the prompt escalates real work
        cmd += ["--model", model]
    if allowed_tools:
        cmd += ["--allowedTools", allowed_tools]
    if resume_session:
        cmd += ["--resume", resume_session]
    if job_dir:
        env["CLAUDE_JOB_DIR"] = job_dir
    # THE DECLARED CHILD (the wake-orphan cure, operator ruling 2026-07-17): a MINT is
    # born already claimed — the whisper reads this env and registers a denominated child
    # (spawned_by the room's seat, roman.arabic), never an anonymous stranger the
    # archaeologist must dig up later. A resume is not a birth; it exports nothing.
    if spawn_parent and not resume_session:
        env["OSIRIS_SPAWNED_BY"] = spawn_parent
        env["OSIRIS_SPAWN_TYPE"] = "wake-triage"
    cmd.append(prompt)

    # THE RECEIPT (21a99136). This was `stdout=DEVNULL`, and so Osiris's single most expensive act
    # — an entire Claude session, with tools, in a repo, on the operator's card — threw away the
    # vendor's own price for itself on every one of 463 spawns. `--output-format json` makes the
    # CLI print an envelope carrying `total_cost_usd`: authoritative, free, volunteered. It is
    # exactly where the miner's $40.49-to-the-cent comes from. We were binning it.
    #
    # STILL FIRE-AND-FORGET: the receipt goes to a FILE, and NOTHING HERE AWAITS THE PROCESS. That
    # is not laziness, it is B1's scar — an arq timeout that abandoned a live billing `claude -p`
    # is precisely how the worker wedged itself with ten 290MB children against a 2G cap. A
    # separate free observer (ingest/wake_cost.meter_wakes) reads these envelopes after the fact.
    #
    #   A HAND YOU CANNOT COST IS A HAND YOU CANNOT GOVERN — and the cost was being handed to us.
    receipt = _receipt_path(job_dir, resume_session)
    out: Any = asyncio.subprocess.DEVNULL
    if receipt is not None:
        try:
            receipt.parent.mkdir(parents=True, exist_ok=True)
            out = receipt.open("wb")
        except OSError:  # an unwritable receipt must never stop the wake — degrade, don't die
            out = asyncio.subprocess.DEVNULL
            receipt = None
    proc = await asyncio.create_subprocess_exec(
        *cmd, cwd=repo, env=env, stdout=out, stderr=asyncio.subprocess.DEVNULL,
    )
    if receipt is not None and out is not asyncio.subprocess.DEVNULL:
        out.close()  # the child holds the fd; the parent must not, or the file never closes
    _log.info("trigger: woke %s in %s (pid %s, receipt %s)",
              f"resume:{resume_session}" if resume_session else f"mint:{job_dir}",
              repo, proc.pid, receipt or "NONE — this wake will be invisible in the ledger")


# The body lane's default envelope (doctrine 2's ceiling knob, no metering-policy attached
# yet — §0.2 wires the daily ceiling into `budget_usd`). 2GiB: the same cap providers.py's
# extractor already lives inside, so one wake body costs no more headroom than one extractor.
_BODY_DEFAULT_RAM_BYTES = 2 * 2**30


async def _spawn_in_body(
    repo: str, prompt: str, *, job_dir: str | None = None, resume_session: str | None = None,
    model: str | None = None, allowed_tools: str | None = None,
    provider: BodyProvider | None = None, ram_bytes: int = _BODY_DEFAULT_RAM_BYTES,
    cores: int | None = None, budget_usd: float | None = None,
) -> str:
    """The body-lane twin of `_spawn_claude` (doctrine 2, ruling `7ff54707`: LOCAL is the
    default product tier, not a test double). SAME env/anchor discipline as `_spawn_claude` —
    CLAUDE_JOB_DIR, --model, --allowedTools, --resume — only the substrate changes: the process
    runs inside a metered `BodyProvider` body instead of a bare child of this one.

    THE WAKE PROTOCOL ITSELF IS UNCHANGED: nothing calls this yet, and it does not replace
    `_spawn_claude` — it sits beside it, dark, until Phase 1's daemon routes real wakes through
    a body. Returns the body's HANDLE (not a pid): unlike `_spawn_claude`'s fire-and-forget, the
    caller owns dissolve(handle) — that is where the receipt is minted, not here.

    The CLI's own per-call dollar receipt (`_spawn_claude`'s `total_cost_usd` capture) is a
    SEPARATE concern from the body's resource receipt (core/ram-seconds, minted by the provider
    at dissolve) — folding the two into one ledger is §0.2's job, not this one's.
    """
    provider = provider or LocalProvider()
    env = os.environ.copy()
    env.pop("CLAUDE_JOB_DIR", None)  # same anchor discipline as _spawn_claude: never inherited
    cmd = ["claude", "-p", "--output-format", "json"]
    if model:
        cmd += ["--model", model]
    if allowed_tools:
        cmd += ["--allowedTools", allowed_tools]
    if resume_session:
        cmd += ["--resume", resume_session]
    if job_dir:
        env["CLAUDE_JOB_DIR"] = job_dir
    cmd.append(prompt)

    # The seat's durable anchor (§2's "identity at birth"): job_dir when minting, the resumed
    # session's own id otherwise. NEVER the bare repo path — path=identity is the root bug this
    # whole spec exists to kill (dd47c1da), and two bodies in one repo would share an accounting
    # identity. A summon with no anchor is a caller error, refused loudly.
    seat_anchor = job_dir or resume_session
    if seat_anchor is None:
        raise ValueError("_spawn_in_body needs an anchor: job_dir or resume_session")
    handle = await provider.summon(
        "claude", cores, ram_bytes, repo, seat_anchor, budget_usd, command=cmd, env=env)
    _log.info("trigger: bodied %s in %s (handle %s)",
              f"resume:{resume_session}" if resume_session else f"mint:{job_dir}", repo, handle)
    return handle


async def trigger_mail_tick(
    actions: Actions, *, settings: Settings | None = None, spawn: Any = _spawn_claude,
    windows: Any = None, poke: Any = None,
) -> dict[str, int]:
    """One trigger pass — the dispatch order is DELIVER → POKE → RESUME → MINT (thread
    9f2ddb44 + the wake law): a live owner just gets its mail (no spawn); a manager-hosted
    window already holding the addressee's session gets the mail TYPED INTO IT as its next
    turn (the wake law — no second process ever resumes a session whose window is still
    open); a resumable owner is CONTINUED via its own session (cheap — no re-ingestion, no
    twin, no succession seam); only otherwise is a fresh twin minted (succession-stamped at
    mount, 88ca0a1). `spawn`/`windows`/`poke` are injected so tests assert the DECISION
    without launching a process or a daemon. The wake is RECORDED (with its mode) before the
    spawn — the ledger is the rate limiter, the chain, and the alternation guard."""
    st = settings or get_settings()
    pool = actions.pool
    report = {"woke": 0, "resumed": 0, "poked": 0, "window_busy": 0, "skipped": 0,
              "owner_live": 0, "abandoned": 0, "scoped_out": 0, "poke_only_held": 0}
    # the re-arm scope: a non-empty allowlist names the ONLY projects this trigger may touch
    allow = {p.strip() for p in st.osiris_trigger_projects.split(",") if p.strip()}

    # THE CEILING — and this is the producer it was built for. A wake is not a token, it is an
    # entire Claude session with tools, in a repo, on the operator's card. 463 of them were minted
    # on projects he had not opened in days, and NOT ONE was ever in the ledger, because the
    # spawner throws the vendor's own receipt at /dev/null (21a99136). Every other guard here is a
    # RATE (wakes per hour, attempts per message) — and a rate is not a bound: the storm ran for
    # days at a perfectly legal 5/hr. THIS is the bound.
    ok, why = await may_spend(pool, cap=st.osiris_daily_usd)
    if not ok:
        report["refused"] = 1
        report["why"] = why  # type: ignore[assignment]
        _log.warning("the trigger is refusing to spend: %s", why)
        return report

    # the fleet-wide hourly spend — the SAME number the chrome renders as 'wakes N/h'
    hourly = await pool.fetchval(
        "SELECT count(*) FROM agent_wakes WHERE woke_at > now() - interval '1 hour'")
    # the manager's window roster, once per tick ([] when the daemon is dark — fail-open).
    # None defaults resolve LATE (module attribute, not a bound default) so tests can
    # darken the manager for the whole module with one monkeypatch.
    windows = windows or _manager_windows
    poke = poke or _poke_window
    wins = await windows() if st.osiris_trigger_enabled else []
    for project, msg_id, sender, age in await _projects_with_unread(
            pool, st.osiris_mail_lease_secs):
        if not st.osiris_trigger_enabled:
            report["skipped"] += 1
            continue
        if allow and project not in allow:
            # a scoped re-arm touches ONLY its named subjects — unread mail elsewhere waits
            # for its own re-arm (or a live reader), it is never a licence to wake
            report["scoped_out"] += 1
            continue
        if await _owner_live(pool, project, st.osiris_owner_live_secs):
            report["owner_live"] += 1  # deliver: the awake owner reads its own box
            continue
        recent = await _recent_wakes(pool, project, st.osiris_trigger_window_secs)
        within_grace = await _woken_within(pool, project, st.osiris_trigger_grace_secs)
        # urgent = the operator's own word, or mail deferred long enough that another
        # deferral is starvation (the economics never silently orphan a message)
        urgent = (sender or "").startswith("operator") or age >= _URGENT_AGE_SECS
        # THE TOTAL, not the rate — the bound every other guard here forgot to keep.
        attempts = await _attempts_on(pool, msg_id)
        reason = should_wake(enabled=True, recent_wakes=recent,
                             rate_cap=st.osiris_trigger_rate_cap,
                             within_grace=within_grace,
                             hourly_wakes=int(hourly or 0) + report["woke"],
                             hourly_budget=st.osiris_wake_hourly_budget,
                             urgent=urgent,
                             attempts=attempts,
                             attempt_limit=st.osiris_wake_message_attempts)
        if reason == "unsettleable":
            # we have tried and failed enough times to know that trying again is not a plan.
            # Stop forever, and tell the human — the only reader who can actually act.
            if await _abandon(pool, project, msg_id, sender, attempts):
                report["abandoned"] += 1
            continue
        if reason is not None:
            report["skipped"] += 1
            continue
        # THE POKE LANE (the wake law, Phase 2): a manager-hosted window already holding a
        # session of this project gets the mail AS A TURN — typed into the open window —
        # instead of a second process resuming a session whose window is still live. The
        # daemon owns the idle gate (never type into a streaming turn or over the
        # operator's shoulder) and the dedup (one cause types at most once per window).
        if wins:
            sids = {Path(r["job_dir"]).name[:8] for r in await pool.fetch(
                "SELECT job_dir FROM agent_mounts WHERE project=$1 "
                "AND job_dir IS NOT NULL", project)}
            wname = _window_for(wins, sids)
            if wname is not None:
                res = await poke(wname, _POKE_PROMPT, dedup=f"msg:{msg_id}",
                                 min_idle=st.osiris_poke_min_idle_secs)
                if res.get("poked") and not res.get("deduped"):
                    await pool.execute(
                        "INSERT INTO agent_wakes (to_project, from_agent, message_id, mode) "
                        "VALUES ($1,$2,$3,'poke')", project, sender, msg_id)
                    report["poked"] += 1
                    report["woke"] += 1
                    continue
                if res.get("busy"):
                    # an ACTIVE window: its mail waits for the next tick (or its own next
                    # osiris call) — never a wake recorded, because nothing was spent
                    report["window_busy"] += 1
                    continue
                if res.get("deduped"):
                    if await _last_wake_mode(pool, project, msg_id) != "poke":
                        # the poke typed once but a crash lost its record — heal the
                        # ledger instead of double-typing or double-spending
                        await pool.execute(
                            "INSERT INTO agent_wakes (to_project, from_agent, message_id, "
                            "mode) VALUES ($1,$2,$3,'poke')", project, sender, msg_id)
                        report["poked"] += 1
                        continue
                    # already poked for this cause and still unsettled — escalate past the
                    # window (fall through to resume/mint, the pre-poke ladder)
        if st.osiris_trigger_poke_only:
            # THE POKE-ONLY ARM (operator, 2026-07-19): the ladder ends here — no resume,
            # no mint, no new process. Held mail stays pull-only; the counter says so.
            report["poke_only_held"] += 1
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
                        model=st.osiris_wake_model or None,
                        allowed_tools=st.osiris_wake_allowed_tools or None)
            report["resumed"] += 1
            report["woke"] += 1
            continue
        repo_path = await _repo_path(pool, project)
        if repo_path is None:  # no known repo → can't spawn; the mail stays pull-only
            report["skipped"] += 1
            continue
        await pool.execute(  # the wake is RECORDED before it is fired — a spawn we can't count
            "INSERT INTO agent_wakes (to_project, from_agent, message_id, mode) "
            "VALUES ($1,$2,$3,'mint')", project, sender, msg_id)
        # the anchor goes into the PROMPT as a literal path — a woken agent has no shell to
        # expand $CLAUDE_JOB_DIR with, which is why 463 mints never once used the stable anchor
        wake_anchor = _wake_job_dir(project)
        await spawn(repo_path, _WAKE_PROMPT.format(repo=repo_path, job_dir=wake_anchor),
                    job_dir=wake_anchor, model=st.osiris_wake_model or None,
                    allowed_tools=st.osiris_wake_allowed_tools or None,
                    spawn_parent=await _room_parent(pool, project))
        report["woke"] += 1

    # THE DM LANE (fleet mail phase 3, task #61): DELIVER → RESUME → nothing. No mint, ever —
    # a private message is never handed to a stranger; an unresumable addressee's DM stays
    # pull-only and follows the seat at the next mint (the estate). One resume attempt per
    # message (the alternation guard's DM half): a resume that didn't settle it is not looped.
    for agent_id, msg_id, sender in await _dms_with_unread(pool, st.osiris_mail_lease_secs):
        if not st.osiris_trigger_enabled:
            report["skipped"] += 1
            continue
        if await _agent_live(pool, agent_id, st.osiris_owner_live_secs):
            report["owner_live"] += 1  # the addressee is awake: its own chrome shows the DM
            continue
        row = await pool.fetchrow(
            "SELECT project FROM agent_mounts WHERE agent_id=$1 "
            "ORDER BY last_seen DESC LIMIT 1", agent_id)
        dm_project: str | None = row["project"] if row else None
        if not dm_project:  # an unmounted addressee has no session to resume: pull-only
            report["skipped"] += 1
            continue
        project = dm_project
        recent = await _recent_wakes(pool, project, st.osiris_trigger_window_secs)
        within_grace = await _woken_within(pool, project, st.osiris_trigger_grace_secs)
        if should_wake(enabled=True, recent_wakes=recent,
                       rate_cap=st.osiris_trigger_rate_cap,
                       within_grace=within_grace) is not None:
            report["skipped"] += 1
            continue
        if await _last_wake_mode(pool, project, msg_id) == "dm-resume":
            report["skipped"] += 1  # one attempt per DM — never a resume loop
            continue
        # THE DM POKE (the wake law's private half): the addressee's OWN window — matched
        # lineage-wide, any generation's anchor — gets the DM typed in as its next turn.
        # Same discipline as the project lane; a deduped-and-still-unsettled DM falls
        # through to the resume rung (its one attempt).
        if wins:
            from src.orchestrator.agents import _generation
            base = _generation(agent_id)[0]
            sids = {Path(r["job_dir"]).name[:8] for r in await pool.fetch(
                "SELECT job_dir FROM agent_mounts WHERE (agent_id=$1 "
                "OR agent_id LIKE $1 || '-%') AND job_dir IS NOT NULL", base)}
            wname = _window_for(wins, sids)
            if wname is not None:
                res = await poke(wname, _DM_POKE_PROMPT, dedup=f"dm:{msg_id}",
                                 min_idle=st.osiris_poke_min_idle_secs)
                if res.get("poked") and not res.get("deduped"):
                    await pool.execute(
                        "INSERT INTO agent_wakes (to_project, from_agent, message_id, mode) "
                        "VALUES ($1,$2,$3,'dm-poke')", project, sender, msg_id)
                    report["poked"] += 1
                    report["woke"] += 1
                    continue
                if res.get("busy"):
                    report["window_busy"] += 1
                    continue
        if st.osiris_trigger_poke_only:
            report["poke_only_held"] += 1  # the DM's resume rung is a spawn — held too
            continue
        resume = await _agent_resumable(pool, agent_id, st)
        if resume is None:
            report["skipped"] += 1
            continue
        session_id, repo = resume
        await pool.execute(
            "INSERT INTO agent_wakes (to_project, from_agent, message_id, mode) "
            "VALUES ($1,$2,$3,'dm-resume')", project, sender, msg_id)
        await spawn(repo, _DM_RESUME_PROMPT, resume_session=session_id,
                    model=st.osiris_wake_model or None,
                    allowed_tools=st.osiris_wake_allowed_tools or None)
        report["resumed"] += 1
        report["woke"] += 1
    return report
