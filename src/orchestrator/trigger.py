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
import json
import logging
import os
import re
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import asyncpg

from src.actions.core import Actions
from src.config.settings import Settings, get_settings
from src.ingest.providers import spend_is_metered
from src.ingest.sessions import locate_current_transcript, resume_verdict
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
    shows in the others' inboxes. DMs dispatch per-message through dispatch_dm (the
    background-session adapter). The operator desk is skipped — never woken."""
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


async def _recent_wakes_for_pair(
    pool: asyncpg.Pool, *, base_a: str, seat_a: str | None, base_b: str,
    seat_b: str | None, window_secs: int,
) -> int:
    """Wakes in EITHER direction between two seats within the window — the ping-pong hazard
    the rate cap exists to bound (should_wake's own docstring: 'the A↔B ping-pong') is a
    property of the PAIR, not the project it happens to share (msg 984, 2026-07-21: two
    capped nudges in ten minutes, on the two most important messages of the day, both
    rescued by the operator by hand — a project-wide cap makes ordinary house growth
    indistinguishable from a runaway loop). Matched by current lineage base (a wake's
    from_agent may wear an older generation's suffix — the same LIKE-prefix shape
    _seat_wakes already uses for to_agent) OR a seat address, whichever the message wore."""
    n = await pool.fetchval(
        "SELECT count(*) FROM agent_wakes aw JOIN fleet_messages fm ON fm.id=aw.message_id "
        "WHERE aw.woke_at > now() - make_interval(secs => $1) AND ("
        "  ((aw.from_agent = $2 OR aw.from_agent LIKE $2 || '-%') "
        "    AND (fm.to_agent = $3 OR fm.to_agent LIKE $3 || '-%' "
        "         OR ($4::text IS NOT NULL AND fm.to_agent = $4::text)))"
        "  OR"
        "  ((aw.from_agent = $3 OR aw.from_agent LIKE $3 || '-%') "
        "    AND (fm.to_agent = $2 OR fm.to_agent LIKE $2 || '-%' "
        "         OR ($5::text IS NOT NULL AND fm.to_agent = $5::text)))"
        ")",
        window_secs, base_a, base_b, seat_b, seat_a)
    return int(n or 0)


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
    operator address is a desk, not a repo: 'operator (read at the desk, never woken)'.

    THE POKE-ONLY HONESTY FIX (thread aa58c1e4): a BROADCAST has NO daemon-reply lane at
    all (that hop is dispatch_dm-only, targeted DMs exclusively) — its only push path is
    owner-live -> poke -> resume/mint, and osiris_trigger_poke_only=1 (a live, standing
    config) makes the ladder terminate at a permanent 'held' for any project with no open
    manager-hosted window: no resume, no mint, ever, no matter how long the sweep keeps
    retrying every ~60s. This used to fall through to the same generic 'armed' every DM
    gets — the exact lying-receipt shape a live incident exposed (the operator watched 23
    minutes of apparent silence and concluded the messages didn't land; measured diagnosis:
    decision 636c8abd — the daemon hop itself is near-instant, 0.32s in the one case
    checked; the broadcast simply had no push mechanism available under this config at
    all). Checked ONLY when it would otherwise say 'armed' (an extra manager round-trip on
    every other verdict would be spent on an answer nobody needed)."""
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
    if reason == "disabled":
        # NOT a transient brake — the kill switch itself is off; no autonomous retry ever
        # fires until a human re-enables osiris_trigger_enabled, so no cadence applies
        return "disabled"
    if reason is not None:
        # rate-capped / budget-exhausted / wake-grace: genuinely transient sliding-window
        # brakes the sweep re-checks and clears on its own — attempts/attempt_limit are
        # never passed here, so should_wake can't return 'unsettleable' from this call site
        return f"{reason} (the sweep runs every ~60s and retries once it clears)"
    if st.osiris_trigger_poke_only:
        sids = {Path(r["job_dir"]).name[:8] for r in await pool.fetch(
            "SELECT job_dir FROM agent_mounts WHERE project=$1 "
            "AND job_dir IS NOT NULL", project)}
        wins = await _manager_windows()
        if _window_for(wins, sids) is None:
            return ("poke-only, no open manager window for this project — a broadcast has "
                    "NO daemon-reply lane (that hop is DM-only) and poke-only mode never "
                    "resumes or mints, so this will NOT be pushed to anyone; it reaches a "
                    "reader only when one is already live or next opens this project on "
                    "their own")
    return "armed"


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
    """(addressee, oldest deliverable DM id, sender) for every agent with unsettled DM mail.
    Each row feeds dispatch_dm (the background-session adapter, 6c4d0b62): deliver a mid-turn
    addressee, poke an open manager window, RESUME the backgrounded session — and there is NO
    mint lane — a fresh twin is not the addressee, and a private message must never be
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


async def _agent_resumable(
    pool: asyncpg.Pool, agent_id: str, st: Settings
) -> tuple[str, str, float, str] | None:
    """(session_id, repo_cwd, mtime, job_dir) to resume the ADDRESSEE's own session, else
    None — the same checks as the project ladder (not retired, own anchored transcript,
    below the context ceiling) scoped to one agent's mounts. `job_dir` (thread 25943031,
    the resident-guard corroboration fallback) is the SPECIFIC registry row this hit came
    from — existing callers all index resume[0]/resume[1], never destructure the full
    tuple, so appending it here is additive, not a breaking change."""
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
        _pick_resumable_sync, cands, root, st.osiris_resume_ceiling_bytes,
        st.osiris_resume_min_tail_bytes)


async def _agent_resume_miss_reason(pool: asyncpg.Pool, agent_id: str, st: Settings) -> str:
    """Only called after _agent_resumable already returned None for this SAME agent_id —
    re-walks its own three early-exit shapes (retired, no mounts at all, no anchored
    transcript among its mounts) plus _resume_miss_reason's ceiling distinction, to name
    WHICH one fired. A second pass rather than threading a reason through
    _agent_resumable's own return, so every existing caller's tuple-or-None contract stays
    exactly as it was (dispatch_dm's mid-turn check, _transcript_activity) — this only
    runs on the refusal path dispatch_dm's message needed distinguished (Thoth's ruling,
    2026-08-03, #135/#136), never the hot path."""
    if await _retired(pool, agent_id):
        return "retired — a deliberate close, never reanimated"
    rows = await pool.fetch(
        "SELECT job_dir, cwd FROM agent_mounts WHERE agent_id=$1 "
        "ORDER BY last_seen DESC LIMIT 5", agent_id)
    cands = [(r["job_dir"], r["cwd"]) for r in rows]
    if not cands:
        return "no anchored transcript at all"
    root = Path(st.osiris_sense_sessions) if st.osiris_sense_sessions \
        else Path.home() / ".claude" / "projects"
    return await asyncio.to_thread(
        _resume_miss_reason, cands, root, st.osiris_resume_ceiling_bytes,
        st.osiris_resume_min_tail_bytes)


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


def _resume_candidate_verdict(t: Path, ceiling_bytes: int, min_tail_bytes: int) -> str | None:
    """The dispatch-layer's thin wrapper over `sessions.resume_verdict` — kept here (not
    just calling that function directly at every site) because THIS is where Settings
    already lives and every caller in this file already imports from here. The verdict
    ITSELF (#156's rebuild, 2026-08-09, the operator's own correction: "closed at exactly
    the compaction seam is a rare special case" — a minimum-tail-bytes floor replaces the
    old compaction-COUNT gate; see `resume_diagnostics`'s own docstring in
    src/ingest/sessions.py for the full finding, including sekhmet's live specimen the old
    gate got wrong) lives in ONE place so `_pick_resumable_sync` (the hot path) and
    `_resume_miss_reason` (the refusal-message path) can never independently drift —
    closed by construction. That drift already happened once: eebeb1f (Sekhmet's #136)
    shipped a byte-for-byte duplicate of this function's OLD raw-size check (Thoth's
    ruling, 2026-08-03 — "that is 38c71544 exactly"). Raises OSError on an unreadable
    transcript; both callers handle it the same way (skip this candidate).

    BOTH GATES MUST PASS, CHECKED INDEPENDENTLY: don't reanimate a generation that already
    retired via legitimate succession is ALREADY `_retired()`'s job, checked first by
    every caller here, authoritatively — not this function's concern. This function
    answers a narrower question: would a resume return something worth having."""
    return resume_verdict(t, ceiling_bytes=ceiling_bytes, min_tail_bytes=min_tail_bytes)


def _pick_resumable_sync(
    cands: list[tuple[str, str]], root: Path, ceiling_bytes: int, min_tail_bytes: int = 0,
) -> tuple[str, str, float, str] | None:
    """The disk half of resume-resolution (sync — called via to_thread): for each candidate
    (job_dir, cwd), anchor its transcript and run `_resume_candidate_verdict`. Returns
    (full_session_id, cwd, transcript_mtime, job_dir) for the first resumable owner. The
    transcript stem IS the session id `claude --resume` takes. The mtime rides along as
    the ONE honest mid-turn signal: a turn writes the transcript; nothing else does (the
    statusline-heartbeat superstition, killed 2026-07-20 — see dispatch_dm). `job_dir`
    (thread 25943031) rides along too, additive — see _agent_resumable's own docstring."""
    for job_dir, cwd in cands:
        t = locate_current_transcript(root, job_dir, anchored_only=True)
        if t is None:
            continue
        try:
            st = t.stat()
            if _resume_candidate_verdict(t, ceiling_bytes, min_tail_bytes) is not None:
                continue
        except OSError:
            continue
        return t.stem, cwd, st.st_mtime, job_dir
    return None


def _resume_miss_reason(
    cands: list[tuple[str, str]], root: Path, ceiling_bytes: int, min_tail_bytes: int = 0,
) -> str:
    """Why _pick_resumable_sync came back empty for these SAME candidates — the two
    opposite shapes dispatch_dm's refusal message used to collapse into one identical
    sentence (Thoth's ruling, 2026-08-03, #135/#136): 'no anchored transcript at all'
    (nothing ever mounted here, or never wrote one) is not the same situation as 'found
    a real session, disqualified by ceiling or compaction depth' — the first means there
    is genuinely nothing to wait for; the second means a mind exists but a resume of it
    would not be worth having. Never called on the hot path — only after resume-
    resolution already returned None, to name which of the two it was. Reads the SAME
    `_resume_candidate_verdict` `_pick_resumable_sync` does, so the two can never
    disagree on why a candidate was skipped; an unreadable transcript (OSError) counts
    as not-anchored here too, the same as it does there."""
    for job_dir, _cwd in cands:
        t = locate_current_transcript(root, job_dir, anchored_only=True)
        if t is None:
            continue
        try:
            reason = _resume_candidate_verdict(t, ceiling_bytes, min_tail_bytes)
        except OSError:
            continue
        if reason is not None:
            return reason
    return "no anchored transcript at all"


async def _resumable_owner(
    pool: asyncpg.Pool, project: str, st: Settings
) -> tuple[str, str, float, str] | None:
    """(session_id, repo_cwd, mtime, job_dir) of the project's freshest RESUMABLE owner,
    else None: not retired (graph check), transcript anchored on its own job_dir (never a
    co-tenant's), and below the context ceiling."""
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
        _pick_resumable_sync, cands, root, st.osiris_resume_ceiling_bytes,
        st.osiris_resume_min_tail_bytes)


async def _last_wake_mode(pool: asyncpg.Pool, project: str, message_id: int) -> str | None:
    """How the LAST wake for this exact message dispatched — the alternation guard's input:
    a resume that never leased its mail (still deliverable past grace) is not retried; the
    next wake mints. Never two consecutive resume attempts on one undelivered message."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT mode FROM agent_wakes WHERE to_project=$1 AND message_id=$2 "
        "ORDER BY woke_at DESC LIMIT 1", project, message_id)


# ══════════════════════ THE BACKGROUND-SESSION ADAPTER (ruling 6c4d0b62) ══════════════════════
# The fleet runs as harness-backgrounded sessions under ONE spawner pty: no pty fd to poke,
# no turn in flight for a stop-hook to decorate. RESUME is therefore the DM lane's PRIMARY
# push — and it dispatches PER MESSAGE, on arrival (send() calls dispatch_dm directly; the
# worker tick calls the same function as the backstop that drains queues). The four walls:
# immediate (an event, never a clock), gated (needs-input / explicit pause — mail queues, it
# never interrupts a mind that asked the human), FLAT (every seat equal; hierarchy is
# convention in who-mails-whom, never privilege in this code), braked (per-message dedup +
# per-seat rate + fleet budget + the daily dollar ceiling). Every dispatch returns a RECEIPT
# {mode, detail} the sender sees in send()'s echo — a hop is visible or it did not happen.

_ASK_SLACK_SECS = 900  # a turn may run this long past its own desk brief before going quiet


async def _last_wake_mode_msg(pool: asyncpg.Pool, message_id: int) -> str | None:
    """The DM alternation guard's input, message-scoped (a DM's ledger rows may span the
    addressee's projects): one dm-resume per message, ever — a resume that did not settle
    its mail is not looped."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT mode FROM agent_wakes WHERE message_id=$1 ORDER BY woke_at DESC LIMIT 1",
        message_id)


async def _paused(pool: asyncpg.Pool, canonicals: list[str]) -> str | None:
    """The explicit per-seat PAUSE wall (6c4d0b62 wall #2): the newest `paused` assertion
    per object wins (a control lever is latest-word, not highest-grade); returns the id
    that carries a winning paused=true, else None. Checked across the addressee's faces —
    the seat outlives successions, the agent covers the unbound."""
    rows = await pool.fetch(
        "SELECT DISTINCT ON (o.canonical) o.canonical, a.value #>> '{}' AS v "
        "FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical = ANY($1::text[]) AND a.name='paused' "
        "ORDER BY o.canonical, a.observed_at DESC", canonicals)
    for r in rows:
        if str(r["v"]).lower() == "true":
            return str(r["canonical"])
    return None


async def _awaiting_operator(pool: asyncpg.Pool, agent_id: str) -> dict[str, Any] | None:
    """The NEEDS-INPUT wall (6c4d0b62 wall #2): a seat whose last act was asking the human —
    an undismissed lead desk brief of kind decision/hands, with the seat QUIET since — is
    not peer-resumable; its mail queues until the operator's word. The quiet check is what
    keeps this honest: a seat that briefed the desk and kept working was never halted by its
    own ask (most briefs are filed mid-stride), so only ask-then-silence gates. Release is
    the human's act by construction: dismissing the brief empties the predicate, and
    answering by hand gives the seat a turn that moves its last_seen past the slack."""
    from src.orchestrator.agents import _generation
    from src.orchestrator.mailbox import _DESK_BRIEF_ROW
    base = _generation(agent_id)[0]
    q = ("SELECT m.desk_kind, m.created_at FROM fleet_messages m WHERE "
         + _DESK_BRIEF_ROW.replace("$op", "$1")
         + " AND m.desk_kind IN ('decision','hands') "
         "AND (m.from_agent = $2 OR m.from_agent LIKE $2 || '-%') "
         "ORDER BY m.created_at DESC LIMIT 1")
    row = await pool.fetchrow(q, OPERATOR_ADDR, base)
    if row is None:
        return None
    last = await pool.fetchval(
        "SELECT max(last_seen) FROM agent_mounts WHERE agent_id=$1 "
        "OR agent_id LIKE $1 || '-%'", base)
    if last is not None and (last - row["created_at"]).total_seconds() > _ASK_SLACK_SECS:
        return None  # it kept working well past the ask — the brief did not halt it
    return {"desk": str(row["desk_kind"]), "since": str(row["created_at"])}


async def _seat_wakes(
    pool: asyncpg.Pool, agent_id: str, seat_id: str | None, window_secs: int
) -> int:
    """DM wakes landed on THIS addressee (lineage-wide, seat address included) within the
    window — the per-seat rate brake's numerator (wall #4). Distinct from _recent_wakes,
    which counts a PROJECT's wakes: an A<->B ping-pong inside one project would sail past
    the project cap while each seat burns."""
    from src.orchestrator.agents import _generation
    base = _generation(agent_id)[0]
    n: int | None = await pool.fetchval(
        "SELECT count(*) FROM agent_wakes aw JOIN fleet_messages fm ON fm.id=aw.message_id "
        "WHERE aw.mode IN ('dm-reply','dm-resume','dm-poke') "
        "AND aw.woke_at > now() - make_interval(secs => $1) "
        "AND (fm.to_agent = $2 OR fm.to_agent LIKE $2 || '-%' "
        "     OR ($3::text IS NOT NULL AND fm.to_agent = $3::text))",
        window_secs, base, seat_id)
    return n or 0


# THE RESIDENT'S SIGNATURE — who actually lives in a session (the leak fix, operator's
# question 2026-07-20: 'how can it resolve the current agent I'm speaking with?').
# NOT by registry timestamp: the statusline pump bumps last_seen by job_dir, so a stale
# claimant row on a live job is kept eternally fresh by the RESIDENT's own chrome, and the
# one-row-per-door upsert erases history. The honest witness is the transcript: every
# osiris act a session performs leaves a SIGNED receipt in its own append-only JSONL —
# a send's {"sent":N,"from":"agent:…"}, a mount's {"agent":"agent:…","project":…}, the
# whisper's "knows you as agent:…" at first breath. The newest signature in the tail IS
# the resident. The chrome cannot pollute this file; only turns write it.
# receipts appear JSON-escaped inside transcript lines (\"from\":\"agent:…\") and
# occasionally plain — the optional backslashes cover both encodings
_SIGNED = [
    re.compile(r'\\?"sent\\?":\s*\d+,\s*\\?"from\\?":\s*\\?"(agent:[A-Za-z0-9._-]+)'),
    re.compile(r'\\?"agent\\?":\s*\\?"(agent:[A-Za-z0-9._-]+)\\?",\s*\\?"project\\?"'),
    re.compile(r"knows you as (agent:[A-Za-z0-9._-]+)"),
]
_RESIDENT_TAIL_BYTES = 400_000
# THE CORROBORATION FALLBACK (thread 25943031, halcyon's own stranding, design approved
# Thoth DM 1825): how many further 400KB windows the deeper scan reads BEHIND the tail
# already checked, on a tail miss only. Bounds total scan cost regardless of file size —
# 4 extra windows is ~1.6MB beyond the tail's own 400KB, ~2MB worst case per dispatch.
_RESIDENT_DEEP_WINDOWS = 4


def _resident_of_sync(root: Path, sid: str) -> str | None:
    """Sync (runs via to_thread): the agent id of the NEWEST signed osiris act in session
    `sid`'s transcript, or None when the session has never signed anything (no whisper, no
    mount, no send — a stranger this dispatcher must not address)."""
    if not sid:
        return None
    t = next(iter(root.expanduser().glob(f"*/{sid}.jsonl")), None)
    if t is None:
        return None
    try:
        size = t.stat().st_size
        with t.open("rb") as f:
            f.seek(max(0, size - _RESIDENT_TAIL_BYTES))
            tail = f.read().decode("utf-8", errors="replace")
    except OSError:
        return None
    for line in reversed(tail.splitlines()):
        for pat in _SIGNED:
            m = pat.search(line)
            if m:
                return m.group(1)
    return None


def _resident_of_deeper_sync(
    root: Path, sid: str, *, extra_windows: int = _RESIDENT_DEEP_WINDOWS,
) -> tuple[str | None, Path | None]:
    """Sync (runs via to_thread): called ONLY after `_resident_of_sync`'s own tail check
    already returned None (thread 25943031 — halcyon's last 400KB was all unsigned harness
    noise — away summaries, chrome — even though every signature further back in the SAME
    file was its own lineage). Reads up to `extra_windows` further 400KB windows strictly
    BEHIND the tail already checked (never re-reading it), stopping at the first signed
    act found or the front of the file. Returns (resident_agent_id, transcript_path) — the
    path rides along even on a signature MISS, so `_resident_disagrees` can reason about
    the file for the registry corroboration step without a second glob. Bounded: total
    cost never exceeds `extra_windows` chunks regardless of how large the file is."""
    if not sid:
        return None, None
    t = next(iter(root.expanduser().glob(f"*/{sid}.jsonl")), None)
    if t is None:
        return None, None
    try:
        size = t.stat().st_size
    except OSError:
        return None, t
    end = max(0, size - _RESIDENT_TAIL_BYTES)  # the tail's own start — never overlap it
    for _ in range(extra_windows):
        if end <= 0:
            break
        start = max(0, end - _RESIDENT_TAIL_BYTES)
        try:
            with t.open("rb") as f:
                f.seek(start)
                chunk = f.read(end - start).decode("utf-8", errors="replace")
        except OSError:
            break
        for line in reversed(chunk.splitlines()):
            for pat in _SIGNED:
                m = pat.search(line)
                if m:
                    return m.group(1), t
        end = start
    return None, t


async def _registry_corroborates(
    pool: asyncpg.Pool, job_dir_hint: str, transcript: Path, base: str, *,
    seat_id: str | None,
) -> bool:
    """A FRESH registry read (thread 25943031 — never reused state from candidate
    selection) for the SPECIFIC job_dir this candidate came from: never a cwd-wide search
    (a cwd is not unique across job_dirs by design — two generations of one lineage
    legitimately share one, which a slug-first lookup would misread as ambiguous). Three
    checks, ALL required: (1) the row's agent_id is this lineage's base or a later
    generation; (2) the row's own cwd, SLUGIFIED (never the transcript's directory name
    DEcoded — slugification is lossy to invert, lossless to apply; Thoth's own instruction,
    DM 1825), matches the directory the transcript ACTUALLY sits in — a registry row whose
    cwd field has drifted from disk reality fails here rather than passing on trust alone;
    (3) when the addressee holds a seat, the row's seat_id agrees (a null seat_id is not
    itself suspicious — not every agent holds one). `job_dir_hint` may be a full job_dir
    (the resume lane already knows it exactly) or a bare basename (the daemon lane's own
    job['short']) — matched either way against agent_mounts.job_dir, its own primary key.

    A SLUG COLLISION — some OTHER live door's cwd ALSO slugifying to this exact directory
    name (dashes AND dots both fold to '-' under _harness_slug, so this is a real, not
    hypothetical, case) — is corroboration FAILURE, the guard's existing conservative
    bias: a coincidental string match is ambiguous evidence, never proof."""
    from src.orchestrator.agents import _generation
    from src.orchestrator.mounts import _harness_slug, _legacy_slug

    row = await pool.fetchrow(
        "SELECT job_dir, agent_id, cwd, seat_id FROM agent_mounts "
        "WHERE job_dir = $1 OR job_dir LIKE '%/' || $1", job_dir_hint)
    if row is None:
        return False
    if _generation(row["agent_id"])[0] != base:
        return False
    slug = transcript.parent.name
    if _harness_slug(row["cwd"]) != slug and _legacy_slug(row["cwd"]) != slug:
        return False
    # #184 Leg 1 follow-on, live-reproduced (metron): a collision candidate must be a
    # DIFFERENT lineage — one of THIS lineage's own earlier, stale rows (a durable
    # per-seat anchor from a prior generation, never swept) shares this exact cwd by
    # construction, every time a seat re-mounts. That is corroborating evidence, not
    # ambiguity; only another LINEAGE'S row landing on the same slug is a real collision.
    others = await pool.fetch(
        "SELECT cwd, agent_id FROM agent_mounts WHERE job_dir != $1", row["job_dir"])
    if any((_harness_slug(r["cwd"]) == slug or _legacy_slug(r["cwd"]) == slug)
           and _generation(r["agent_id"])[0] != base for r in others):
        return False  # a slug collision — refuse rather than trust a coincidental match
    return seat_id is None or row["seat_id"] is None or row["seat_id"] == seat_id


def _turn_fresh_sync(root: Path, sid: str, active_secs: int) -> bool:
    """Sync (runs via to_thread): is a TURN genuinely in flight in session `sid` — by the
    transcript's own newest timestamped line, never the inode. AWAKE and ASLEEP are
    different states and must not be confounded (operator, 2026-07-21, the Aegis phantom:
    a session 13 hours dead wore a seconds-old mtime — something in the chrome/daemon
    touches the file without writing). A turn appends timestamped records; a toucher
    cannot. No timestamp in the tail = not moving."""
    if not sid:
        return False
    t = next(iter(root.expanduser().glob(f"*/{sid}.jsonl")), None)
    if t is None:
        return False
    try:
        size = t.stat().st_size
        with t.open("rb") as f:
            f.seek(max(0, size - 65_536))
            tail = f.read().decode("utf-8", errors="replace")
    except OSError:
        return False
    for line in reversed(tail.splitlines()):
        try:
            ts = json.loads(line).get("timestamp")
        except (ValueError, AttributeError):
            continue
        if not ts:
            continue
        try:
            at = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        except ValueError:
            continue
        return (datetime.now(UTC) - at).total_seconds() < active_secs
    return False


ResidentVerdict = Literal["match", "mismatch", "unknown"]


async def _resident_verdict(
    pool: asyncpg.Pool, root: Path, sid: str, base: str, *,
    job_dir_hint: str = "", seat_id: str | None = None,
) -> ResidentVerdict:
    """Does the session's own signed testimony agree with the addressee? Three answers,
    not two (ruling f624d114 — the 18th specimen of 60bc15db: a function that could not
    say "I don't know" and reported "no" instead). "mismatch" is a POSITIVE finding — a
    signed act was found and it names a different lineage than `base`, the crossed-
    registry class (thread 0100a35e, the Ra misdelivery). "unknown" is an ABSENCE of
    evidence — nothing signed was found anywhere in the scan, or there was nothing left
    to corroborate against — and must never be rendered with the same words as a found
    disagreement: an ignorance dressed as a finding is exactly what cost the fleet its
    coordination lane (Cupid's measurement, obligation 94dc4aae). Both non-"match"
    verdicts still refuse a nudge/resume the same way (the addressee's mail must never
    land in a foreign or unverified window) — only the CALLER'S WORDING differs now.

    THE DIFFERENT-MIND ARM IS UNCONDITIONAL (thread 25943031, halcyon's own stranding —
    the fix for the OTHER arm never touches this one): any signed act found, whether in
    the tail or the deeper fallback scan below, is compared directly against `base`; no
    corroboration check ever runs on a found disagreement, let alone overrides one.

    THE UNSIGNED-TAIL ARM alone gets the fallback: on a total tail MISS (nothing signed in
    the last 400KB), a lineage-correct body whose last activity happened to be unsigned
    harness noise (away summaries, chrome) must not read as a stranger just because it
    hasn't SAID anything recently. Before refusing, scan further back in the SAME file
    (bounded, `_resident_of_deeper_sync`) for the newest signed act; accept the session as
    the addressee's ONLY when that act's lineage matches AND a fresh registry read
    corroborates (`_registry_corroborates`) — either alone is insufficient. Still nothing
    found anywhere within the scan cap, or the deeper act names someone else, or the
    registry doesn't corroborate: "unknown" (or "mismatch" if a lineage was positively
    named), exactly the refusal shape from before this fix — only the label is honest now.

    A TOTAL MISS (no signed act ANYWHERE, tail or deep scan both empty) gets the SAME
    registry-corroboration fallback (#184 Leg 1 follow-on, live-reproduced: metron/deckard,
    both genuinely-live seats refused for lack of RECENT osiris tool chatter — busy coding
    sessions that simply hadn't called mount/send/whisper within the ~2MB scanned) — but
    ONLY when `job_dir_hint`'s own registry row is the FRESHEST mount anywhere in this
    lineage (no other row for the same lineage carries a later `last_seen`). Without this,
    the fallback would corroborate a STALE ancestor whose own successor has since taken
    over — the registry row genuinely IS the ancestor's, but a fresher successor's mount
    means resuming/nudging the old one is a different question the registry alone cannot
    answer (`test_an_absent_transcript_refuses_as_unknown_never_as_a_found_mismatch`'s own
    shape: a declared-but-never-mounted successor `abcd1234-ii` still mounts its OWN row —
    same "most recently mounted wins" comparison `wakeable_identity` itself already uses —
    with a fresher `last_seen` than `abcd1234`'s, and must still win over trusting
    `abcd1234`'s own genuinely-corroborated but superseded door). A `last_seen`
    comparison, not a generation-number one, because this fallback is ALSO reachable from
    `dispatch_dm`'s own daemon-reply/nudge lane (`_resident_disagrees`) with no hop or
    target-generation context threaded at all — freshness is the one signal every caller
    already has (it is `wakeable_identity`'s own tiebreak)."""
    from src.orchestrator.agents import _generation
    resident = await asyncio.to_thread(_resident_of_sync, root, sid)
    if resident is not None:
        return "mismatch" if _generation(resident)[0] != base else "match"
    deep_resident, transcript = await asyncio.to_thread(_resident_of_deeper_sync, root, sid)
    if transcript is None:
        return "unknown"  # the transcript itself is unreadable — nothing left to check
    if deep_resident is None:
        # #184 Leg 1 follow-on, live-reproduced (metron/deckard, both currently-live
        # seats refused for lack of RECENT osiris tool chatter): nothing signed in ~2MB
        # is genuine silence for a session doing other work, not evidence of a stranger —
        # try the SAME registry corroboration the deeper-signed-act branch below already
        # uses, rather than giving up the instant no signature exists to re-check.
        if job_dir_hint:
            fresher = await pool.fetchval(
                "SELECT 1 FROM agent_mounts a WHERE (a.agent_id=$1 OR a.agent_id LIKE $1 || '-%') "
                "AND a.last_seen > COALESCE((SELECT last_seen FROM agent_mounts "
                "  WHERE job_dir=$2 OR job_dir LIKE '%/' || $2), 'epoch'::timestamptz) LIMIT 1",
                base, job_dir_hint)
            if fresher is None:
                corroborates = await _registry_corroborates(
                    pool, job_dir_hint, transcript, base, seat_id=seat_id)
                if corroborates:
                    return "match"
        return "unknown"  # nothing signed found anywhere in the scan — ignorance, not a finding
    if _generation(deep_resident)[0] != base:
        return "mismatch"
    if not job_dir_hint:
        return "unknown"  # no candidate to corroborate against — refuse, never guess one
    corroborates = await _registry_corroborates(
        pool, job_dir_hint, transcript, base, seat_id=seat_id)
    return "match" if corroborates else "unknown"


async def _resident_disagrees(
    pool: asyncpg.Pool, root: Path, sid: str, base: str, *,
    job_dir_hint: str = "", seat_id: str | None = None,
) -> bool:
    """True when the door should NOT be trusted — either a positive mismatch or an
    unresolved unknown (both refuse a nudge/resume the same way). Thin bool wrapper over
    `_resident_verdict` for callers that only ever silently drop a candidate and never
    render the reason as prose (a silent drop cannot misrepresent an unknown as a
    finding, so collapsing the two here is safe) — callers that DO render a reason to a
    human must call `_resident_verdict` directly instead, so "mismatch" and "unknown"
    stay distinguishable at the point the words get written."""
    return await _resident_verdict(
        pool, root, sid, base, job_dir_hint=job_dir_hint, seat_id=seat_id) != "match"


def _zero_hop_graph_corroborates(
    resume: tuple[str, str, float, str], *, hop: int | None, launch_cwd: str | None,
) -> bool:
    """A NAMED third verdict alongside `_resident_verdict`'s match/mismatch/unknown (#173a,
    the ferryman incident 2026-08-18 00:41Z: `osiris launch` FOUND d8727352 — gen 8, 0
    hops, resumable — and the resident-unknown guard REFUSED it, because no signed osiris
    act had been written into that transcript yet; the operator resumed it by hand anyway,
    correctly, since it plainly WAS ferryman's own session). NEVER a loosening of
    `_resident_verdict` itself (that function, and its three verdicts, are untouched) —
    this is a wholly separate, narrower door that opens ONLY for `_lineage_resume_
    candidate`'s own first hop (`hop == 0`, i.e. the lineage HEAD's own current session,
    never an ancestor's): the session id found there came straight off the GRAPH's own
    `session` property (succession_chain's fresh read of this exact lineage head's own
    mount-time stamp), not from re-scanning a transcript for testimony it may not have
    written yet.

    REVIEW NOTE (Sekhmet XXI, reviewing this branch as a stranger's per Thoth's own
    instruction): `resume[1]` is NOT independent evidence — trace `_lineage_resume_
    candidate`'s own return (`(session, repo, mtime, "")`, `repo` being that function's
    OWN `repo` KEYWORD ARGUMENT), and `launch_seat`'s one call site (`repo=launch_cwd`,
    then `_resume_guard(..., launch_cwd=launch_cwd)`): `resume[1]` and `launch_cwd` are
    the SAME value threaded through, so `resume[1] == launch_cwd` is true by construction
    for every existing caller — never a real second signal, whatever this docstring used
    to claim. Left in (rather than silently dropped) as a documented invariant a future
    caller could violate by wiring `resume`/`launch_cwd` from different sources; the
    ACTUAL gate here is just `hop == 0` — trusting the graph's own `session` pointer for
    the lineage head's own current generation, once a launch location is known.
    A GENUINE second signal (e.g. a fresh `agent_mounts` row for this exact holder whose
    own `cwd` agrees) is possible and may be worth adding — flagged to Thoth rather than
    redesigned here, mid-incident, in a branch under active review. Callers outside the
    lineage-walk lane never pass `hop`/`launch_cwd` — this always returns False for them,
    so `_resume_guard` behaves exactly as before wherever this new context is
    unavailable."""
    return hop == 0 and launch_cwd is not None and resume[1] == launch_cwd


async def _resume_guard(
    pool: asyncpg.Pool, resume: tuple[str, str, float, str], base: str, *,
    seat_id: str | None, st: Settings, hop: int | None = None,
    launch_cwd: str | None = None,
) -> tuple[str | None, str | None]:
    """The resident-identity gate (0100a35e) an already-found `_agent_resumable` result
    must clear before ANY caller continues someone else's session — shared by
    dispatch_dm's DM-resume branch, launch_seat's launch-resume branch, and
    wake_gate_preflight so the check cannot drift between them the way eebeb1f's byte-
    for-byte copy of the OLD ceiling check did (Thoth's ruling, 2026-08-04, decision
    a829a15d: "reuse it, do not reimplement it").

    Returns `(gate, detail)`: `(None, None)` means the resume may proceed. Otherwise
    `gate` is a stable short token the caller uses DIRECTLY for its mode/status string —
    "crossed-registry" for a POSITIVELY found different mind, "resident-unknown" for an
    absence of evidence either way (ruling f624d114: these used to be the same bool and
    the same rendered sentence; a caller that string-matched `detail` to recover the
    distinction would just reinvent that bug one layer up, so this returns the token
    itself instead of prose to be re-parsed).

    `hop`/`launch_cwd` (#173a): only `launch_seat`'s own lineage-walk branch has this
    context (`_lineage_resume_candidate`'s hop count and the seat's own launch location);
    every other caller omits both and gets identical behavior to before this addition —
    see `_zero_hop_graph_corroborates`'s own docstring for what this narrow extra door is
    and, just as importantly, is NOT (it never runs when the testimony arm found a
    positive mismatch — that arm stays unconditional)."""
    root = Path(st.osiris_sense_sessions) if st.osiris_sense_sessions \
        else Path.home() / ".claude" / "projects"
    verdict = await _resident_verdict(pool, root, resume[0], base, job_dir_hint=resume[3],
                                      seat_id=seat_id)
    if verdict == "match":
        return None, None
    if verdict == "mismatch":
        return "crossed-registry", (
            "the registry's door for this addressee leads to a session whose own "
            "signed testimony names a different mind (the crossed-registry class, "
            "0100a35e)")
    if _zero_hop_graph_corroborates(resume, hop=hop, launch_cwd=launch_cwd):
        return None, None
    return "resident-unknown", (
        "no signed testimony could be found anywhere in the addressee's scanned "
        "session history to confirm this door — an absence of evidence, not "
        "evidence of a different mind (thread 0100a35e's unknown arm)")


def _gate_name(detail: str) -> str:
    """A stable, short token for WHICH named gate refused a resume (#156.2, Thoth's own
    ask: 'refused-by-<named-gate>', not a bare 'refused'). Reads the SAME prose
    `_resume_candidate_verdict` / `_resume_miss_reason` already produce for humans —
    never a second source of truth to drift from. A string it doesn't recognize names
    itself 'unknown' rather than guessing: the whole point of this function is to never
    relabel a refusal as something it wasn't.

    NEVER FED `_resume_guard`'s output (ruling f624d114): that gate now returns its
    token directly — "crossed-registry" or "resident-unknown" — precisely so nothing
    downstream has to re-derive a mismatch/unknown distinction by string-matching
    rendered prose, which is the same shape of bug this function's own docstring above
    already warns against for every OTHER gate it names."""
    if "seam itself" in detail:
        return "compaction"
    if "context ceiling" in detail:
        return "ceiling"
    if "no anchored transcript" in detail:
        return "no-anchor"
    # `_lineage_resume_candidate`'s OWN log wording (task #178) — the same "nothing to
    # resume at this hop" fact, phrased differently because it comes from a lineage-walk
    # entry, never from `_resume_miss_reason`'s single-mount-row prose.
    if "no transcript found on disk" in detail:
        return "no-anchor"
    if "never mounted, no session to check" in detail:
        return "no-anchor"
    if detail.startswith("retired"):
        return "retired"
    return "unknown"


async def _unresolved_wake_receipt(pool: asyncpg.Pool, target: str) -> dict[str, str]:
    """THE LOOKUP-BEFORE-CLASSIFIER FIX (threads 94dc4aae + 27917f1f, Ra XXXV's specimen
    msg 4901): `wakeable_identity` returning None means agent_mounts has no row matching
    `target`'s lineage — it does NOT mean the mind never mounted. Before Ra's specimen,
    that None fell straight into mode="never-mounted", a POSITIVE claim of absence
    manufactured from a lookup miss, even while fleet()/job_for's own registry (agent_
    liveness, the SAME freshest-of-mount-and-last_active signal fleet() renders as
    live:true) showed the addressee live seconds ago. Consulted here, BEFORE the
    classifier speaks: live → the honest word is 'could not resolve a session for a live
    mind', an outcome (mail queues, reads at its own next turn), never a failure; dead →
    'never mounted' remains true and is said plainly, unchanged from before this fix."""
    from src.orchestrator import mounts

    liveness = await mounts.agent_liveness(pool, target)
    if liveness["live"]:
        return {"mode": "queued-live-unresolved",
                "detail": f"{target} is live (registry last seen {liveness['last_seen']}) "
                          "but no resumable OS session could be resolved for it — "
                          "could not resolve recipient session; the mail queues in the "
                          "box and reads at its own next natural turn"}
    return {"mode": "never-mounted",
            "detail": f"{target} has never mounted — no session to resume"}


async def wake_gate_preflight(
    pool: asyncpg.Pool, target: str, *, seat_id: str | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    """Answer the FOUR RESUME GATES (compaction/ceiling/no-anchor/crossed-registry)
    BEFORE an attempt, not discovered as a wall after (#156.4, Thoth msg 3823: '#153
    fixed the rendering; this is the query'). Reuses the EXACT functions dispatch_dm's
    own refusal path already calls (`_agent_resumable`, `_lineage_resume_candidate`,
    `_resume_guard`, `_gate_name`) — never a second copy of the gate logic, only a
    read-only entry point into it.

    THE LINEAGE WALK RUNS UNCONDITIONALLY WHEN THE MOUNT-KEYED LOOKUP MISSES (fixed
    live-fire defect, Alfred's fulcrum specimen, Thoth msg 5852): a prior version gated
    the `_lineage_resume_candidate` attempt on the mount-keyed miss reason naming
    "compaction" specifically, which meant a genuinely EMPTY `agent_mounts` row for a
    `--bg`-launched seat's current generation (the shared-anchor collapse
    `_lineage_resume_candidate`'s own docstring documents) reported a confident
    `resume-refused-no-anchor` even when the lineage walk — the SAME primitive `launch()`
    always tries — would have found a real, resumable session. Fixed by matching
    dispatch_dm's actual precedence exactly: the walk is attempted for every mount-keyed
    miss, and only the FRESH-HEIR MINT decision (never the walk attempt itself) stays
    scoped to a compaction-specific miss. This is now genuinely faithful to what a real
    wake would find, not merely documented as such.

    SCOPED TO THE RESUME GATES ON PURPOSE, not dispatch_dm's full mail-routing sequence
    (vacant/paused/human-attended/awaiting-operator) — those are each a one-line graph
    read already cheap enough to check directly (fleet(), dossier(), seat_receipt()).
    The four gates here are the expensive ones: they walk a lineage and read real
    transcripts off disk, which is the actual cost worth answering up front.

    `target` must already be a RESOLVED agent id (a lineage head — e.g. from
    succession_chain, dossier, or wakeable_identity) — this does not itself walk
    seat -> holder -> living head the way dispatch_dm/wake_worker do; a caller that
    only has a seat or handle should resolve first. `seat_id`, if known, sharpens the
    crossed-registry check's unsigned-tail corroboration fallback (`_resume_guard`'s own
    docstring) — omitting it narrows that one fallback path, never the other three
    gates, and is disclosed here rather than silently changing the answer.

    Returns the SAME vocabulary #156.2 built for wake() (no-live-body / refused-<gate>),
    plus a state dispatch_dm itself never needs to say aloud: 'resumable' — every gate
    clears, a real wake would resume this addressee now."""
    from src.orchestrator.folds import wakeable_identity

    st = settings or get_settings()
    wake_target = await wakeable_identity(pool, target)
    if wake_target is None:
        receipt = await _unresolved_wake_receipt(pool, target)
        return {**receipt, "status": _WAKE_STATUS.get(receipt["mode"], "no-live-body")}
    if await _retired(pool, target):
        return {"mode": "retired", "status": "no-live-body",
                "detail": f"{target} is retired — the trigger never reanimates a "
                          "deliberate close; the estate carries the mail to the next mint"}
    resume = await _agent_resumable(pool, wake_target, st)
    if resume is None:
        from src.orchestrator.agents import _generation
        from src.orchestrator.seats import seat_facts

        # THE LINEAGE WALK RUNS UNCONDITIONALLY NOW — live defect, Alfred's fulcrum
        # specimen (Thoth msg 5852): `_agent_resumable` is agent_mounts-keyed, the exact
        # shared-anchor blind spot `_lineage_resume_candidate`'s own docstring documents
        # for a `--bg`-launched seat (one durable per-seat mount row, overwritten by
        # whichever generation mounts most recently — a genuinely EMPTY row for the
        # current generation reads as "no anchored transcript at all", not compaction).
        # The OLD code here gated the walk ATTEMPT itself on `gate == "compaction"`,
        # conflating "which gate permits minting a fresh heir" (dispatch_dm's own
        # compaction-only scoping, preserved below) with "whether the walk is worth
        # trying at all" — dispatch_dm never makes that second conflation: it tries the
        # walk for EVERY mount-keyed miss reason, unconditionally, and only asks which
        # gate AFTER the walk itself has also failed. fulcrum's own case (gen 10, a real
        # 5MB resumable session, 0 hops back) was gate="no-anchor" from the narrow mount
        # check, so the walk never ran and preflight reported a confident false absence
        # while `launch()` — which always tries the walk — resumed it correctly.
        facts = await seat_facts(pool, seat_id) if seat_id is not None else None
        launch_cwd = (facts["tree_cwd"] or facts["anchor_cwd"]) if facts else None
        if launch_cwd:
            graph_outcome = await _lineage_resume_candidate(
                pool, wake_target, st, repo=launch_cwd)
            graph_resume = (graph_outcome[0]
                            if isinstance(graph_outcome, tuple) else None)
            graph_log = (graph_outcome[1]
                        if isinstance(graph_outcome, tuple) else graph_outcome)
            if graph_resume is not None:
                g_gate, g_refusal = await _resume_guard(
                    pool, graph_resume, _generation(target)[0], seat_id=seat_id,
                    st=st, hop=len(graph_log) - 1, launch_cwd=launch_cwd)
                if g_gate is None:
                    return {"mode": "resumable", "status": "resumable",
                            "detail": f"resumable now via the lineage walk — session "
                                      f"{graph_resume[0][:8]}, no gate refuses it "
                                      "(the shared-anchor mount row missed it; the "
                                      "graph did not)"}
                return {"mode": f"resume-refused-{g_gate}", "status": f"refused-{g_gate}",
                        "detail": g_refusal}
            # THE WALK ALSO FOUND NOTHING — the FINAL reported reason is sourced from
            # its OWN log tail (dispatch_dm's identical precedence), never re-derived
            # from the earlier, narrower mount-keyed reason: the two must report the
            # SAME failure for the SAME lineage, or this function is lying about which
            # primitive it actually consulted last.
            miss_reason = graph_log[-1] if graph_log else "no resumable generation found"
        else:
            # no seat/office known at all — the walk was never attemptable; the mount-
            # keyed reason is the only one there is to report.
            miss_reason = await _agent_resume_miss_reason(pool, wake_target, st)
        gate = _gate_name(miss_reason)
        # FRESH-HEIR, SCOPED TO COMPACTION ONLY (ruling 94c2e7e8) — every other gate
        # (ceiling/no-anchor/crossed-registry/resident-unknown) is a genuine "who this
        # even is" uncertainty; minting on top of that repeats the stranger-over-a-live-
        # head class dispatch_dm's own docstring names.
        if (gate == "compaction" and seat_id is not None and launch_cwd
                and facts and facts.get("handle") and facts.get("anchor_cwd")):
            return {"mode": "fresh-heir-available", "status": "fresh-heir-available",
                    "detail": f"{miss_reason} — a real wake would boot a fresh "
                              f"successor at {facts['anchor_cwd']} instead of "
                              "refusing (its graph identity and office both resolve)"}
        return {"mode": f"resume-refused-{gate}", "status": f"refused-{gate}",
                "detail": miss_reason}
    from src.orchestrator.agents import _generation
    # a DISTINCT name from `gate` above (never the same variable retyped): that one is a
    # refusal gate `_gate_name` guarantees is always `str`; this one is a guard verdict
    # that is legitimately `str | None` — mypy caught the two uses colliding on one name
    # at the merge point (Thoth's finding), and widening `gate`'s type to fix it would
    # have let a genuine absence leak into a path that currently guarantees a name.
    guard_gate, refusal = await _resume_guard(
        pool, resume, _generation(target)[0], seat_id=seat_id, st=st)
    if guard_gate is not None:
        return {"mode": f"resume-refused-{guard_gate}",
                "status": f"refused-{guard_gate}", "detail": refusal}
    return {"mode": "resumable", "status": "resumable",
            "detail": f"resumable now — session {resume[0][:8]}, no gate refuses it"}


async def _lineage_resume_candidate(
    pool: asyncpg.Pool, holder: str, st: Settings, *, repo: str,
) -> tuple[tuple[str, str, float, str], list[str]] | list[str]:
    """THE STRUCTURAL FIX for a live-fire defect (Thoth, 2026-08-04, msg 3691 — Sekhmet):
    `_agent_resumable`/`wakeable_identity` resolve a resume candidate through
    `agent_mounts.job_dir`, which works for a `-p --resume`-triggered wake (a fresh,
    session-specific job_dir every time) but NOT for a `--bg`-launched seat: `_launch_anchor`
    gives EVERY generation of one seat the SAME durable per-seat anchor
    (".../jobs/<seat-id>"), set once in CLAUDE_JOB_DIR at process spawn and unchanged for
    that process's entire life — so agent_mounts' one shared row for that anchor NEVER
    encodes a real session id, only whichever generation most recently mounted (confirmed
    live against Sekhmet's own data: her 32MB, 12-compaction gen-11 session, d80350e5, has
    NO agent_mounts row at all — it was overwritten the instant gen 12 mounted, even though
    gen 12's own body never ran a single turn). This is not a zero-turn-generation edge
    case alone; it is the whole `--bg`-anchor scheme colliding with a job-dir-keyed lookup,
    for EVERY generation in a self-compacting lineage, not only a stillborn one.

    THE FIX: walk `succession_chain` (reused, not reimplemented — it already returns the
    one thing agent_mounts cannot: `session`, a GRAPH property register_agent stamps on
    every mount, per Agent object, immune to the shared-anchor collapse) from `holder`
    backward, and for each hop with a session, apply the IDENTICAL gate
    `_agent_resumable`'s own hot path uses (`_resume_candidate_verdict` — so the compaction
    +ceiling gates can never drift between the two lookup strategies). Bounded by
    succession_chain's own MAX_SUCCESSION_HOPS, the same "a walk never widens unbounded"
    discipline as every other resume check in this file.

    Returns ((session_id, repo, mtime, job_dir=""), log) on the first hop that clears both
    gates — `repo` is the CALLER's own `launch_cwd`, since every generation of one seat
    works in the same place by construction, not a per-hop agent_mounts.cwd (which suffers
    the identical anchor-collapse this function exists to route around); `job_dir=""`
    because no per-hop anchor is available for `_resume_guard`'s corroboration fallback —
    a known, disclosed narrowing (see the caller's own receipt/report), not a silent one.
    Returns the bare `log` (no candidate) when NOTHING in the walk clears both gates — each
    entry names the generation, its session's transcript size, and the SPECIFIC gate that
    refused it (numbers, not adjectives — Thoth's own explicit requirement), so a caller's
    receipt can read one line instead of a human re-running succession_chain by hand."""
    from src.orchestrator.succession import succession_chain

    chain = await succession_chain(pool, holder)
    root = Path(st.osiris_sense_sessions) if st.osiris_sense_sessions \
        else Path.home() / ".claude" / "projects"
    log: list[str] = []
    for hop in chain:
        gen, session = hop["generation"], hop["session"]
        if not session:
            log.append(f"gen {gen}: minted but never mounted, no session to check")
            continue
        t = await asyncio.to_thread(
            locate_current_transcript, root, f"jobs/{session}", anchored_only=True)
        if t is None:
            log.append(f"gen {gen} (session {session[:8]}): mounted but no transcript "
                       "found on disk")
            continue
        try:
            size_mb = t.stat().st_size / 1_000_000
        except OSError:
            log.append(f"gen {gen} (session {session[:8]}): transcript unreadable")
            continue
        verdict = await asyncio.to_thread(
            _resume_candidate_verdict, t, st.osiris_resume_ceiling_bytes,
            st.osiris_resume_min_tail_bytes)
        if verdict is not None:
            log.append(f"gen {gen} (session {session[:8]}, {size_mb:.0f}MB): {verdict}")
            continue
        log.append(f"gen {gen} (session {session[:8]}, {size_mb:.0f}MB): resumable, "
                   f"{len(log)} hop(s) back")
        return (session, repo, t.stat().st_mtime, ""), log
    return log


def _mail_envelope(msg_id: int, *, sender_label: str, addressee_label: str,
                   grade: str | None, preview: str) -> str:
    """The nudge as a CUTE LITTLE MAIL (operator, 2026-07-20: 'formatting is important so
    the logs know who did what at the transcript level'). The injected turn IS a transcript
    record — it carries full attribution (who, to whom, which message, what grade) so a log
    reader, the miner, or the operator scrolling a window sees the hop's provenance without
    leaving the line. The envelope is the KNOCK, never the letter's authority: the addressee
    still reads and settles through the box."""
    g = {"ask": "ask — needs your reply or act",
         "fyi": "fyi — an ack settles it"}.get(grade or "", grade or "ungraded")
    return (
        f"┌─ osiris mail · DM #{msg_id} ─────────────────────\n"
        f"│ from : {sender_label}\n"
        f"│ to   : {addressee_label}\n"
        f"│ grade: {g}\n"
        f"│ « {preview} »\n"
        f"└─ inbox() reads it whole; settle with send(reply_to={msg_id}) or "
        f"inbox(ack=[{msg_id}]). Reply only with NEW information."
    )


async def _resolve_wake_address(
    pool: asyncpg.Pool, addressee: str,
) -> tuple[str, str | None] | dict[str, str]:
    """The address resolution dispatch_dm's own first hop shares with wake_gate_preflight's
    MCP surface (#156.4): seat -> current holder -> living head (folds). Returns
    (resolved_agent_id, seat_id) on success, or a terminal {mode, detail} dict on a vacant
    seat — the one early-exit this resolution step can produce on its own, before any gate
    runs. Extracted so the two callers can never independently drift on HOW an address
    becomes a mind (38c71544's own class of bug, applied to itself)."""
    from src.orchestrator.folds import canonical_agent, living_head
    from src.orchestrator.seats import held_seat, seat_receipt

    target = addressee
    seat_id: str | None = None
    if target.startswith("seat:"):
        seat_id = target
        sr = await seat_receipt(pool, target)
        holder = (sr or {}).get("holder")
        if not holder:
            return {"mode": "seat-vacant",
                    "detail": f"{target} is vacant — the mail waits for its next holder"}
        target = str(holder)
    target = await living_head(pool, await canonical_agent(pool, target))
    if seat_id is None:
        seat_id = ((await held_seat(pool, target)) or {}).get("seat_id")
    return target, seat_id


async def _confirm_listener(job: dict[str, Any], agents_json: Any) -> bool:
    """A session-shaped body actually backs `job`, confirmed against `claude agents --json`
    — the harness's own front-end view, which shows every live body BY CONSTRUCTION
    (`_claude_agents_json`'s own docstring). The daemon's {ok:true} on `nudge` means it
    ACCEPTED the envelope into its own queue, never that a live reader is there to receive
    it: a job the daemon still lists after its own body already exited, or one whose body
    outlived the daemon's own GENERATION (a respawned daemon does not always inherit every
    prior job cleanly), both accept without anyone home. Matched the same way
    `claude_daemon.job_for` matches a job to an identity (short id, full session id, or its
    8-char prefix) so the two never drift. Fails open to False (never raises) — an
    `agents_json` read failure reads as 'cannot confirm', never as a false positive."""
    sid = str(job.get("sessionId") or "")
    short = str(job.get("short") or "")
    ids = {i for i in (short, sid, sid[:8] if sid else "") if i}
    if not ids:
        return False
    try:
        rows = await agents_json()
    except (OSError, TimeoutError, ValueError):
        return False
    for r in rows:
        if not isinstance(r, dict):
            continue
        rsid = str(r.get("sessionId") or "")
        rid = str(r.get("id") or "")
        if rid in ids or rsid in ids or (rsid and rsid[:8] in ids):
            return True
    return False


async def _resume_occupancy_gate(
    resume: tuple[str, str, float, str], *, agents_json: Any,
) -> str | None:
    """IDENTITY vs OCCUPANCY (task #178, the ferryman/sekhmet wave, operator's tonight-law:
    mechanisms not patches, 7d6815bb/df646654): `_lineage_resume_candidate` (and
    `_resume_guard` beside it) answer IDENTITY — whose session, graph-truth, via
    `succession_chain`. Neither answers OCCUPANCY — whether a body is ACTUALLY SITTING
    there right now. A `-p --resume` fired beside a live body forks the mind into two
    processes both claiming the same identity; nothing upstream of this call catches
    that, because nothing upstream asks the occupancy question at all. This is the ask,
    right before ANY resume-spawn — two independent signals, matched differently on
    purpose (a live body can be found by either without being findable by both):

    `_confirm_listener` (task #176's own primitive, reused verbatim, never a second
    matcher) checks BY SESSION ID against `claude agents --json` — catches a body the
    graph's own candidate session id already names, wherever `_lineage_resume_candidate`
    found it, at any hop. `census.live_bodies_by_cwd` checks BY OFFICE DIRECTORY via
    /proc — catches a live claude process sitting in the exact office this resume would
    land in, whatever session id it thinks it has (a manually-run `claude` that hasn't
    self-mounted yet is invisible to the first signal, visible to this one).

    Either signal firing refuses. Returns a short reason naming WHICH, or None when
    neither finds anybody home (safe to resume) — never a silent block; the caller's own
    receipt names exactly why."""
    session_id, repo = resume[0], resume[1]
    if await _confirm_listener({"sessionId": session_id, "short": ""}, agents_json):
        return (f"a live body is already listed in `claude agents --json` for session "
                f"{session_id[:8]}")
    from src.orchestrator.census import live_bodies_by_cwd
    bodies = await asyncio.to_thread(live_bodies_by_cwd)
    if bodies is not None and bodies.get(repo):
        pids = ", ".join(str(p) for p in bodies[repo])
        return f"a live claude process (pid {pids}) is already sitting at {repo!r}"
    return None


async def dispatch_dm(
    pool: asyncpg.Pool, *, addressee: str, msg_id: int, sender: str | None,
    settings: Settings | None = None, spawn: Any = None, windows: Any = None,
    poke: Any = None, jobs: Any = None, nudge: Any = None, agents_json: Any = None,
    fresh_spawn: Any = None,
) -> dict[str, str]:
    """Dispatch ONE DM — the adapter's whole grammar in one function, shared verbatim by
    send()'s immediate leg and the worker tick's backstop sweep (two callers, one law: the
    lanes must never drift). Returns the per-hop RECEIPT {mode, detail}:

      nudged             — the mail envelope was injected into the addressee's live
                           backgrounded session via the HARNESS DAEMON (the visible hop:
                           the operator's front renders daemon-owned turns natively —
                           thread 4261a0d8, the ghost problem's fix) AND a matching
                           session-shaped body is confirmed live in `claude agents --json`
      queued-no-listener — the daemon ACCEPTED the envelope ({ok:true}) but no
                           session-shaped body confirms it in `claude agents --json` — a
                           job the daemon still lists after its body exited, or one that
                           outlived the daemon's own generation, can both accept without
                           anyone home (task #176, practice 2c45d78e: UNKNOWN, never
                           smoothed into either 'nudged' or a resolved failure). Queue
                           semantics are UNCHANGED — the backstop sweep still retries
                           at-least-once, exactly as before this verdict existed
      resumed            — the addressee's own session was continued with the mail as its
                           next turn (the fallback push when the daemon doesn't hold it)
      poked              — typed into the addressee's manager-hosted OPEN window (rare now)
      delivered          — the addressee is MID-TURN; its own turn's end surfaces the DM
      queued-fyi         — grade='fyi' never wakes: the grammar's loop terminator (an ack at
                           the addressee's next natural turn settles it, no turn minted)
      queued-paused      — an explicit pause holds the seat; mail waits in the box
      queued-needs-input — the addressee asked the operator and went quiet; the human's word
                           is the release, peer mail must not preempt it
      queued-wake-in-flight / braked / skipped-* / held / pull-only / refused — the brakes,
                           each detail saying exactly which one and why.

    A wake carries ONE nudge and no spawn authority (--allowedTools mcp__osiris); the
    ledger row is written under an advisory lock BEFORE the spend, so two dispatchers (the
    send leg and a concurrent tick) can never double-fire one message."""
    st = settings or get_settings()
    spawn = spawn or _spawn_claude
    fresh_spawn = fresh_spawn or _spawn_claude_bg
    windows = windows or _manager_windows
    poke = poke or _poke_window
    agents_json = agents_json or _claude_agents_json
    if jobs is None or nudge is None:
        # THE INJECT SEAM (ruling 85fba696, superseding 482c3d0f): `nudge` defaults to
        # claude_daemon.reply — the daemon turn-injection channel, disclosed to Anthropic and
        # deemed INTENDED DESIGN, so the old "do not build new machinery on the default" bar is
        # WITHDRAWN. `nudge` stays a parameter anyway: the lane is an undocumented internal free
        # to break without notice, so this is the swap seam for whatever replaces it — kept as
        # operational insurance now rather than legal cover. `jobs` (job_for) is a benign READ
        # of daemon liveness (the authoritative state Ra's reachability read consults).
        from src.ingest.harness import claude_daemon
        jobs = jobs or claude_daemon.job_for
        nudge = nudge or claude_daemon.reply
    if not st.osiris_trigger_enabled:
        return {"mode": "trigger-dark", "detail": "the trigger is dark (osiris_trigger_enabled=0)"}
    mrow = await pool.fetchrow(
        "SELECT grade, left(body, 160) AS preview FROM fleet_messages WHERE id=$1", msg_id)
    grade = mrow["grade"] if mrow else None
    preview = " ".join(str((mrow["preview"] if mrow else "") or "").split())
    if grade == "fyi":
        return {"mode": "queued-fyi",
                "detail": "an fyi never wakes — the addressee acks it at its own next turn "
                          "(the loop terminator)"}
    # resolve the address to a living mind: seat → current holder → living head (folds).
    # THE SEAT GAP this closes: name-addressed mail stores the SEAT id (B2), and the old DM
    # lane matched it against agent_mounts verbatim — so every seat-bound addressee (the
    # whole charter pattern) was silently pull-only.
    from src.orchestrator.folds import wakeable_identity
    from src.orchestrator.seats import held_seat
    resolved = await _resolve_wake_address(pool, addressee)
    if isinstance(resolved, dict):
        return resolved
    target, seat_id = resolved
    # THE HUMAN-ATTENDED GUARD (Thoth LIII 2026-07-21, ruling d8a77f80; the proxy REPLACED by
    # thread 96f62338). The daemon reply lane FORGES a human turn (the confirmed RCE), so a
    # nudge/poke into a session the operator drives lands in the operator's OWN input turn —
    # caught live. The ORIGINAL guard read 'seat manages someone' as 'a human drives this
    # session' — true only while Thoth was the sole manager, and broken the day workers started
    # minting sub-workers and test seats of their own (Imhotep's own flip-test mints made him a
    # manager; alfred's #50-pilot workers did too — both silently lost their push lane forever).
    # THE REAL SIGNAL now: `_is_human_attended` reads the seat's own explicit `attended`
    # property (set_seat_attended, operator-approved to change) — never infers it from the org
    # chart. This yields the same asymmetry the guard always wanted (manager→worker injects,
    # worker→manager waits in the box) but keyed on WHO IS ATTENDED, not who manages. It runs
    # EVEN WITH the trigger on — a human-attended seat is never a nudge target, period.
    if seat_id and await _is_human_attended(pool, seat_id):
        return {"mode": "queued-human",
                "detail": f"{target} is a human-attended seat — the daemon never injects a "
                          "human's live turn; the mail waits in its box and surfaces on its "
                          "next turn (perceived by pull, not a forged injection)"}
    faces = [c for c in {addressee, target, seat_id} if c]
    # wall #2 — the gate: an explicit pause, or needs-input (ask-then-silence)
    paused_on = await _paused(pool, faces)
    if paused_on:
        return {"mode": "queued-paused",
                "detail": f"{paused_on} is explicitly paused — mail queues in the box; "
                          "pause_seat(paused=false) releases it"}
    ask = await _awaiting_operator(pool, target)
    if ask:
        return {"mode": "queued-needs-input",
                "detail": f"the addressee briefed the operator (desk={ask['desk']}, "
                          f"{ask['since']}) and has been quiet since — awaiting the human's "
                          "word; peer mail queues rather than preempting it"}
    if await _retired(pool, target):
        return {"mode": "retired",
                "detail": f"{target} is retired — the trigger never reanimates a deliberate "
                          "close; the estate carries the mail to the next mint"}
    # ALREADY SETTLED BY THE LINEAGE (the per-agent-id read-state class, bug 00378259 —
    # its third bite of the day, this one on the trigger's own deliverable query: it keys
    # settlement on the EXACT addressed id, so mail acked by a successor generation reads
    # deliverable forever and earns a phantom nudge. Caught live when the lane's first
    # unsolicited delivery knocked on THIS builder's window with mail its own lineage
    # settled days earlier.)
    from src.orchestrator.agents import _generation
    base = _generation(target)[0]
    if await pool.fetchval(
            "SELECT 1 FROM message_recipients r WHERE r.message_id=$1 "
            "AND r.read_at IS NOT NULL AND (r.agent_id = $2 OR r.agent_id = $3 "
            "OR r.agent_id LIKE $3 || '-%') LIMIT 1", msg_id, addressee, base):
        return {"mode": "settled",
                "detail": "already settled by the addressee's lineage — the deliverable "
                          "query lags on cross-generation read-state (00378259); "
                          "nothing to wake"}
    # wall #4 — the brakes, cheapest first
    if await _last_wake_mode_msg(pool, msg_id) in ("dm-reply", "dm-resume", "dm-poke"):
        return {"mode": "skipped-once-per-message",
                "detail": "this message already got its one wake and did not settle — "
                          "never looped; it stays readable in the box"}
    grace = st.osiris_trigger_grace_secs
    if grace > 0 and await _seat_wakes(pool, target, seat_id, grace):
        return {"mode": "queued-wake-in-flight",
                "detail": "a wake for this addressee is already in flight — the resumed "
                          "session reads its WHOLE box, this message rides along"}
    cap = st.osiris_seat_wake_hourly_cap
    if cap > 0 and await _seat_wakes(pool, target, seat_id, 3600) >= cap:
        return {"mode": "braked",
                "detail": f"the per-seat rate brake: {cap} wakes/h already landed on this "
                          "addressee — the backstop sweep runs every ~60s and retries once "
                          "the hourly window clears"}
    # WAKE'S OWN QUESTION (thread 28842543): `target` above is DELIVERY's answer — the
    # declared lineage head, trusted even past a successor that never actually mounted.
    # A bare mount lookup on `target` alone reproduces the exact defect (a live agent
    # under an earlier generation reported "has never mounted" beside its own fresh
    # last_seen, in the SAME receipt). wake_target is the most recently mounted id
    # anywhere in `target`'s lineage — None only when NOTHING there has ever mounted, in
    # which case `target` (the declared name) is still the honest thing to name below.
    wake_target = await wakeable_identity(pool, target)
    if wake_target is None:
        return await _unresolved_wake_receipt(pool, target)
    project = str(await pool.fetchval(
        "SELECT project FROM agent_mounts WHERE agent_id=$1 "
        "ORDER BY last_seen DESC LIMIT 1", wake_target))
    hourly = await pool.fetchval(
        "SELECT count(*) FROM agent_wakes WHERE woke_at > now() - interval '1 hour'")
    # THE RATE CAP'S UNIT (msg 984, 2026-07-21): the ping-pong hazard this brake bounds is a
    # property of a PAIR, not a project — a project-wide cap makes ordinary house growth
    # indistinguishable from a runaway loop (measured live: two capped nudges in ten
    # minutes, on the two most important messages of the day, both rescued by the operator
    # by hand). When sender and target share an active managed_by edge, scope the cap to
    # that pair; anything else (a peer DM, an unseated party) keeps the original project
    # brake — the aggregate/cost hazard already has its own dedicated guard (may_spend,
    # below), so the rate cap does not need to double as one and can stay narrow.
    sender_seat = ((await held_seat(pool, sender)) or {}).get("seat_id") if sender else None
    if sender and seat_id and sender_seat and await _managed_edge(pool, seat_id, sender_seat):
        recent = await _recent_wakes_for_pair(
            pool, base_a=_generation(sender)[0], seat_a=sender_seat, base_b=base,
            seat_b=seat_id, window_secs=st.osiris_trigger_window_secs)
    else:
        recent = await _recent_wakes(pool, project, st.osiris_trigger_window_secs)
    reason = should_wake(
        enabled=True, recent_wakes=recent,
        rate_cap=st.osiris_trigger_rate_cap, within_grace=False,
        hourly_wakes=int(hourly or 0), hourly_budget=st.osiris_wake_hourly_budget,
        urgent=(sender or "").startswith("operator"))
    if reason is not None:
        return {"mode": "skipped-" + reason,
                "detail": f"the fleet's own brakes held it ({reason}) — the backstop sweep "
                          "runs every ~60s and retries once the brake clears"}
    # the dollar wall: a resume is a real turn — but only a BILLED one. On a subscription the
    # CLI's cost is notional and this gate is inert (spend_is_metered=False); it bites only when
    # Osiris runs on a keyed API backend.
    ok, why = await may_spend(pool, cap=st.osiris_daily_usd, metered=spend_is_metered(st))
    if not ok:
        return {"mode": "refused", "detail": str(why)}
    # MID-TURN MEANS THE TRANSCRIPT IS MOVING — nothing else (the statusline-heartbeat
    # superstition, killed 2026-07-20 at the operator's first live round-trip ask: the
    # statusline bumps agent_mounts.last_seen every few seconds FOR BACKGROUNDED SESSIONS
    # TOO, so by that field every seated idle agent read as permanently working and this
    # gate could never open. A turn WRITES the transcript; a chrome render does not. The
    # heartbeat keeps its own job — the minting guard — it just no longer testifies here.)
    # AND THE INODE IS NOT THE TRANSCRIPT (the Aegis phantom, 2026-07-21: a session 13h
    # dead on a 401 — size unchanged the whole time — wore an mtime seconds old, bumped
    # by something in the chrome/daemon, and this gate read the corpse as a working mind.
    # The operator's ruling: AWAKE and ASLEEP must never be confounded — 'waking them up
    # to run jobs is one thing, waiting for them to awake is another'.) mtime is only the
    # cheap pre-filter; the WITNESS is the newest timestamped line — a TURN writes those,
    # a toucher cannot.
    resume = await _agent_resumable(pool, wake_target, st)
    if resume is not None and time.time() - resume[2] < st.osiris_dm_active_secs:
        root = Path(st.osiris_sense_sessions) if st.osiris_sense_sessions \
            else Path.home() / ".claude" / "projects"
        if await asyncio.to_thread(
                _turn_fresh_sync, root, resume[0], st.osiris_dm_active_secs):
            return {"mode": "delivered",
                    "detail": "the addressee's transcript is moving right now (genuinely "
                              "mid-turn) — its own turn's end surfaces the DM; no second "
                              "process beside a working mind"}
        # a fresh inode with no fresh TURN is an ASLEEP addressee — fall through and wake
    doors = {Path(r["job_dir"]).name for r in await pool.fetch(
        "SELECT job_dir FROM agent_mounts WHERE (agent_id=$1 "
        "OR agent_id LIKE $1 || '-%') AND job_dir IS NOT NULL", base)}
    # THE DAEMON-REPLY RUNG LEADS — the VISIBLE hop (thread 4261a0d8; the ghost problem,
    # operator-confirmed solved 2026-07-20): the front renders the harness DAEMON's job
    # stream, so a daemon-owned turn is the one push the operator actually SEES. The
    # daemon's own list is also the address book for LIVE jobs (no door-row dependency —
    # bug b6a64207 shrinks to the daemon-dark case). Every failure falls OPEN into
    # poke/resume: undocumented internals never get to strand a message.
    ids = doors | {d[:8] for d in doors}
    if resume is not None:
        ids |= {resume[0], resume[0][:8]}
    root = Path(st.osiris_sense_sessions) if st.osiris_sense_sessions \
        else Path.home() / ".claude" / "projects"
    job = await jobs(ids)
    if job is not None and await _resident_disagrees(
            pool, root, str(job.get("sessionId") or ""), base,
            job_dir_hint=str(job.get("short") or ""), seat_id=seat_id):
        # the crossed-registry class (0100a35e): the registry pointed here, the session's
        # own signed testimony names someone else (or nobody) — never leak the envelope
        job = None
    if job is not None:
        s_handle = ((await held_seat(pool, sender)) or {}).get("handle") if sender else None
        a_handle = ((await held_seat(pool, target)) or {}).get("handle")
        env = _mail_envelope(
            msg_id, grade=grade, preview=preview,
            sender_label=(f"{s_handle} ({sender})" if s_handle else (sender or "the fleet")),
            addressee_label=(f"you — {a_handle} ({target})" if a_handle
                             else f"you ({target})"))
        nudged = False
        async with pool.acquire() as conn, conn.transaction():
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended('osiris-dm-' || $1, 7445))",
                str(msg_id))
            prior = await conn.fetchval(
                "SELECT 1 FROM agent_wakes WHERE message_id=$1 "
                "AND mode IN ('dm-reply','dm-resume','dm-poke')", msg_id)
            if prior:
                return {"mode": "skipped-once-per-message",
                        "detail": "another dispatcher already woke for this message"}
            nudged = bool(await nudge(job, env))
            if nudged:
                await conn.execute(
                    "INSERT INTO agent_wakes (to_project, from_agent, message_id, mode) "
                    "VALUES ($1,$2,$3,'dm-reply')", project, sender, msg_id)
        if nudged:
            # RECEIPT HONESTY (thread aa58c1e4): 'nudged' means the daemon ACCEPTED the
            # injection ({ok:true} on its own control socket) — the confirmed hop, not an
            # observed one (ruling 986b12f0's own distinction: reply() returning True means
            # QUEUED, not SEEN). Measured empirically (decision 636c8abd): this hop lands as
            # an actually-submitted turn in ~0.3s in the one case checked — fast enough that
            # 'nudged' stays the honest label for it — but the wording no longer claims the
            # turn already happened, only that the daemon took it.
            #
            # THE THIRD STATE (task #176, 2026-08-18, practice 2c45d78e): {ok:true} still
            # cannot distinguish "injected into a live reader" from "handed to a daemon with
            # nobody home" — a job the daemon still lists after its own body exited, or one
            # that outlived the daemon's own GENERATION, both accept without anyone there to
            # receive it. `_confirm_listener` cross-checks `claude agents --json` (the
            # harness's own front-end view, which shows every live body BY CONSTRUCTION) for
            # a session-shaped body actually matching this job. QUEUE SEMANTICS UNCHANGED:
            # the agent_wakes row above is written the SAME WAY regardless of this check
            # (at-least-once across successions stays correct) — this only decides what the
            # RECEIPT honestly claims, never whether or how the message redelivers.
            shown = job.get("name") or job.get("short") or "the job"
            if not await _confirm_listener(job, agents_json):
                return {"mode": "queued-no-listener",
                        "detail": f"the harness daemon ACCEPTED the mail envelope for "
                                  f"{shown}, but no session-shaped body confirms it in "
                                  "`claude agents --json` — the daemon queued it; whether "
                                  "or when it is actually read is UNKNOWN, never a resolved "
                                  "failure (practice 2c45d78e); the backstop sweep retries, "
                                  "unchanged"}
            return {"mode": "nudged",
                    "detail": f"the harness daemon ACCEPTED the mail envelope as {shown}'s "
                              "next turn AND a matching session-shaped body is confirmed "
                              "live in `claude agents --json` (typically lands within a "
                              "second or two, per measurement — visible live in the agents "
                              "view once it does)"}
        # the daemon refused (version seam, dead socket, missing key) — fall open
    # the poke lane: a manager-hosted OPEN window holding this lineage's session gets
    # the mail typed in as a turn — never a second process beside an open window
    wins = await windows()
    if wins:
        wname = _window_for(wins, {d[:8] for d in doors})
        if wname is not None:
            res = await poke(wname, _DM_POKE_PROMPT, dedup=f"dm:{msg_id}",
                             min_idle=st.osiris_poke_min_idle_secs)
            if res.get("poked") and not res.get("deduped"):
                await pool.execute(
                    "INSERT INTO agent_wakes (to_project, from_agent, message_id, mode) "
                    "VALUES ($1,$2,$3,'dm-poke')", project, sender, msg_id)
                return {"mode": "poked",
                        "detail": f"typed into the open window {wname} as its next turn"}
            if res.get("busy"):
                return {"mode": "window-busy",
                        "detail": "the addressee's window is streaming a turn — the mail "
                                  "waits; nothing spent"}
    if not st.osiris_dm_resume:
        return {"mode": "held",
                "detail": "the DM resume arm is dark (osiris_dm_resume=0) — pull-only"}
    # THE SELECTION SWAP (task #178, Seshat's own specimen tonight — an old generation's
    # 103MB transcript picked over her real, current session): `resume` above (from
    # `_agent_resumable`) is `agent_mounts`-keyed, freshest-mounted-row-wins — the exact
    # shared-anchor defect `_lineage_resume_candidate`'s own docstring documents for a
    # `--bg`-launched seat (one durable per-seat mount row, overwritten by whichever
    # generation mounts most recently, blind to which one is the TRUE current lineage
    # head). It stays correct for the MID-TURN check above (an activity probe, not an
    # identity claim) but must never pick WHICH session `-p --resume` actually targets.
    # From here down, `_lineage_resume_candidate` — the SAME graph-truth primitive
    # launch_seat already uses — walks `succession_chain`'s own `session` property
    # instead: identity from the graph, never the registry's own amnesia. Started from
    # `wake_target`, NOT `target`: `target` is living_head's DECLARED successor, which
    # can be a real, active, but never-mounted Agent object with no `succeeded_from`
    # backward link for `succession_chain` to walk at all (thread 28842543's own shape,
    # the reason `wake_target` exists) — `wake_target` is wake's own already-resolved
    # answer to "which identity can actually be resumed", the correct root for this walk
    # exactly as `_agent_resumable` above was rooted on it too.
    from src.orchestrator.seats import seat_facts
    launch_cwd = None
    facts: dict[str, Any] | None = None
    if seat_id is not None:
        facts = await seat_facts(pool, seat_id)
        launch_cwd = facts["tree_cwd"] or facts["anchor_cwd"]
    graph_outcome = await _lineage_resume_candidate(
        pool, wake_target, st, repo=launch_cwd) if launch_cwd else [
            "no seat/office known for this addressee — a resume needs a launch location"]
    graph_log = graph_outcome[1] if isinstance(graph_outcome, tuple) else graph_outcome
    graph_resume = graph_outcome[0] if isinstance(graph_outcome, tuple) else None
    if graph_resume is None:
        who = target if wake_target == target else (
            f"{target} (its own live mount, {wake_target}, checked too)")
        miss_reason = graph_log[-1] if graph_log else "no resumable generation found"
        miss_gate = _gate_name(miss_reason)
        # THE FRESH-HEIR FALLBACK (ruling 94c2e7e8, dispatch 5398 leg 1 — "the zero-
        # tolerance compaction gate is the bug, not the seats"): resume / nudge /
        # fresh-heir are the three outcomes every seat must land in, never a fourth
        # silent wall. A generation whose OWN transcript sits too close to its own
        # compaction seam to resume (resume_verdict's honest "nothing real to resume
        # into") still has a real graph identity and a real office — SCOPED TO
        # COMPACTION ONLY: the other gates (ceiling/no-anchor/crossed-registry/
        # resident-unknown) are each a genuine "who this even is" uncertainty, and
        # minting on top of THAT would repeat the exact stranger-over-a-live-head class
        # Leg 3 just closed. Boots the SUCCESSOR at the seat's own launch location with
        # the SAME fresh-mint boot prompt launch_seat's own fallthrough already uses —
        # no separate carry-the-message prompt needed, `_bg_boot_prompt` already tells
        # a fresh body to inbox() for its opening brief, and this very mail sits there
        # addressed to the seat's own name.
        handle = facts.get("handle") if facts else None
        office = facts.get("anchor_cwd") if facts else None
        house = facts.get("house") if facts else None
        if (miss_gate == "compaction" and seat_id is not None and launch_cwd
                and handle and office):
            anchor = _launch_anchor(seat_id)
            boot_prompt = _bg_boot_prompt(office=office, anchor=anchor, handle=handle)
            async with pool.acquire() as conn, conn.transaction():
                await conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended('osiris-dm-' || $1, "
                    "7445))", str(msg_id))
                prior = await conn.fetchval(
                    "SELECT 1 FROM agent_wakes WHERE message_id=$1 "
                    "AND mode IN ('dm-reply','dm-resume','dm-poke','dm-fresh-heir')",
                    msg_id)
                if prior:
                    return {"mode": "skipped-once-per-message",
                            "detail": "another dispatcher already woke for this message"}
                await conn.execute(
                    "INSERT INTO agent_wakes (to_project, from_agent, message_id, mode) "
                    "VALUES ($1,$2,$3,'dm-fresh-heir')", project, sender, msg_id)
            try:
                await fresh_spawn(launch_cwd, name=f"[{_house_tag(house)}] {handle}",
                                  model=st.osiris_wake_model or None, prompt=boot_prompt)
            except OSError as exc:
                return {"mode": "refused-spawn",
                        "detail": f"fresh-heir spawn failed ({exc}) — {who}'s compacted "
                                  "generation still holds the mail; nothing else spent"}
            return {"mode": "fresh-heir",
                    "detail": f"{who}'s own transcript sits past its resumable seam "
                              f"({miss_reason}) — booted a fresh successor at {office} "
                              "instead of refusing; its own first turn's inbox() finds "
                              "this same mail"}
        return {"mode": f"resume-refused-{miss_gate}",
                "detail": f"{who} has no resumable session ({miss_reason}) — a private "
                          "message is never handed to a fresh twin"}
    session_id, repo = graph_resume[0], graph_resume[1]
    hop = len(graph_log) - 1
    gate, refusal = await _resume_guard(
        pool, graph_resume, base, seat_id=seat_id, st=st, hop=hop, launch_cwd=launch_cwd)
    if gate is not None:
        return {"mode": f"resume-refused-{gate}",
                "detail": f"{refusal} — refusing both nudge and resume; the mail "
                          "stays pull-only for now"}
    # OCCUPANCY, THE OTHER HALF (task #178): identity above says WHOSE session this is;
    # this asks whether anybody is ALREADY SITTING there. A `-p --resume` beside a live
    # body forks the mind — refuse it here, before the spend, never after.
    occupied = await _resume_occupancy_gate(graph_resume, agents_json=agents_json)
    if occupied is not None:
        return {"mode": "resume-refused-occupied",
                "detail": f"{occupied} — refusing to fork a second mind beside it; the "
                          "mail stays pull-only for now"}
    # the ledger row goes in UNDER AN ADVISORY LOCK, before the spawn: two dispatchers
    # (send's immediate leg + a concurrent tick) can both reach here for one message —
    # exactly one of them may spend
    async with pool.acquire() as conn, conn.transaction():
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended('osiris-dm-' || $1, 7445))",
            str(msg_id))
        prior = await conn.fetchval(
            "SELECT 1 FROM agent_wakes WHERE message_id=$1 "
            "AND mode IN ('dm-reply','dm-resume','dm-poke')", msg_id)
        if prior:
            return {"mode": "skipped-once-per-message",
                    "detail": "another dispatcher already woke for this message"}
        await conn.execute(
            "INSERT INTO agent_wakes (to_project, from_agent, message_id, mode) "
            "VALUES ($1,$2,$3,'dm-resume')", project, sender, msg_id)
    await spawn(repo, _DM_RESUME_PROMPT, resume_session=session_id,
                model=st.osiris_dm_resume_model or None,
                allowed_tools=st.osiris_wake_allowed_tools or None)
    return {"mode": "resumed",
            "detail": f"the addressee's own session ({session_id[:8]}) is continued with "
                      "this mail as its next turn — watch the hop in the agents view"}


# ═══ THE KNOCK — wake(), thread 9f566244 piece D, ruling 16722273 ═══════════════════════
# A manager or worker rousing the OTHER half of its own managed_by pair — never a peer.
# Gated on the seat graph alone: dispatch_dm above already IS the resolver (handle → seat →
# living holder → daemon short, with the crossed-registry guard, the real mid-turn check,
# and every rate brake this file owns) — duplicating it would drift the instant either copy
# was touched. wake() adds only the authority gate in front of it and an honest vocabulary
# behind it (dispatch_dm's own "delivered" mode actually means mid-turn and unread — the
# lying receipt, thread 5af93c89 — this layer never repeats it).


async def _seat_for_target(actions: Actions, target: str) -> str | None:
    """The Seat addressed by `target` — a raw seat id, a raw agent id, or a claimed handle —
    or None (a legacy unbound agent, or nobody). managed_by is Seat-to-Seat; a target with no
    seat of its own cannot be arbitrated by it, gated or not."""
    target = (target or "").strip()
    if target.startswith("seat:"):
        exists = await actions.pool.fetchval(
            "SELECT 1 FROM objects WHERE canonical=$1 AND type='Seat' AND status='active'",
            target)
        return target if exists else None
    from src.orchestrator.seats import held_seat, seats_by_handle
    if target.startswith("agent:"):
        held = await held_seat(actions.pool, target)
        return held["seat_id"] if held else None
    # THE VACANT-SEAT FIX (task #68): resolve_seat below is AGENT-centric — it walks Agent
    # objects that claimed a handle, so a freshly minted, never-launched seat (no Agent has
    # ever attached to it) was unresolvable by its own handle, and launch() could never body
    # it. The Seat object itself already carries the handle mint_seat/ensure_seat stamped;
    # try that FIRST. An unambiguous hit is the same seat_id the occupied-case fallback below
    # would find anyway (one seat, one handle), so this changes nothing for the already-
    # working case — it only adds the vacant one. A twin handle (2+ matches) falls through to
    # the legacy resolver rather than guessing which one the caller meant.
    by_handle = await seats_by_handle(actions.pool, target)
    if len(by_handle) == 1:
        return by_handle[0]
    from src.orchestrator.agents import resolve_seat
    resolved = await resolve_seat(actions, target)
    return resolved.get("seat_id")


async def _manages_someone(pool: asyncpg.Pool, seat_id: str) -> bool:
    """True if this seat is the MANAGER side of an active managed_by edge — managed_by is
    minted worker→manager (mintseat.py), so a manager is the TO-side: workers point up to it.

    NO LONGER dispatch_dm's human-attended proxy (thread 96f62338 replaced it — 'manages
    someone' stopped meaning 'a human drives this session' the day workers started minting
    their own sub-workers and test seats; see `_is_human_attended`). Still a correct, plain
    org-chart predicate in its own right — kept for whatever else asks "does X manage Y"."""
    return bool(await pool.fetchval(
        "SELECT 1 FROM links l JOIN objects t ON t.id=l.to_id "
        "WHERE t.canonical=$1 AND l.type='managed_by' "
        "AND (l.valid_until IS NULL OR l.valid_until > now()) LIMIT 1", seat_id))


async def _seat_attendance(pool: asyncpg.Pool, seat_id: str) -> str | None:
    """A seat's own explicit `attended` stamp ('human' or 'worker'), or None if it has never
    been stamped — the common case today, since rollout is deliberate and per-seat
    (set_seat_attended, thread 96f62338)."""
    value = await pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND o.type='Seat' AND o.status='active' AND a.name='attended' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", seat_id)
    return str(value) if value is not None else None


async def _is_human_attended(pool: asyncpg.Pool, seat_id: str) -> bool:
    """THE HUMAN-ATTENDED GUARD'S REAL SIGNAL (thread 96f62338, replacing ruling d8a77f80's
    broken `_manages_someone` proxy). Reads the seat's own explicit `attended` property
    first — no more inference from the org chart, so a worker that mints a test seat or a
    sub-worker of its own no longer silently loses its push lane forever (Imhotep's own
    flip-test mints, alfred's #50-pilot workers — both hit exactly this).

    NO PROPERTY YET (the rollout is deliberate, most seats are never stamped) falls back
    CLOSED — human-attended — ONLY for thoth's own seat (belt while the rollout is
    incomplete: never silently start injecting into the one seat that IS actually
    operator-driven just because nobody has stamped it yet) and OPEN for everyone else —
    the actual fix: a seat that merely manages a sub-worker or a test seat is no longer
    misclassified."""
    attended = await _seat_attendance(pool, seat_id)
    if attended is not None:
        return attended == "human"
    from src.orchestrator.seats import seat_facts
    facts = await seat_facts(pool, seat_id)
    return (facts.get("handle") or "").strip().lower() == "thoth"


async def _managed_edge(pool: asyncpg.Pool, seat_a: str, seat_b: str) -> bool:
    """An ACTIVE managed_by edge between two seats, in EITHER direction (ruling 16722273: a
    wake is a knock, not a killing — gating it downward-only like compaction disenfranchises
    the worker, who holds the freshest information and the blocking question). Peers and
    cross-house traffic still refuse; only a live manager<->worker pair may knock on each
    other."""
    return bool(await pool.fetchval(
        "SELECT 1 FROM links l JOIN objects f ON f.id=l.from_id JOIN objects t ON t.id=l.to_id "
        "WHERE l.type='managed_by' AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "AND ((f.canonical=$1 AND t.canonical=$2) OR (f.canonical=$2 AND t.canonical=$1))",
        seat_a, seat_b))


# THE PROVENANCE MARKER (ruling 986b12f0): the harness stamps EVERY arrival in a session's
# input buffer origin.kind='human', regardless of who actually wrote it — an agent's
# injection and a person's typing share one socket and one key, with no technical mark
# distinguishing them. wake() is the choke point every future knock in this house passes
# through, so the marker is minted HERE, once, rather than trusted to each caller.
_WAKE_MARKER_FMT = "[osiris-wake from={who} seat={seat}]"


def _wake_marker(caller: str, caller_seat: str, caller_handle: str | None) -> str:
    """A single, stable, machine-parseable first line naming who actually wrote this turn —
    never the harness's own label, which cannot tell an agent's hand from a person's."""
    who = f"{caller_handle} ({caller})" if caller_handle else caller
    return _WAKE_MARKER_FMT.format(who=who, seat=caller_seat)


_MARKER_TAIL_BYTES = 65_536


def _marker_landed_sync(root: Path, sid_prefix: str, marker: str) -> bool:
    """Sync (runs via to_thread): has `marker` landed as a SUBMITTED user turn anywhere
    under `sid_prefix`'s transcript(s) — never assumed from a queue op (ruling 986b12f0:
    reply() returning True means QUEUED, not SEEN; the operator has watched an injected
    turn erased by a keystroke before it ever submitted). Only a "type":"user" line whose
    OWN content carries the marker counts."""
    if not sid_prefix or not marker:
        return False
    for t in root.expanduser().glob(f"*/{sid_prefix}*.jsonl"):
        try:
            size = t.stat().st_size
            with t.open("rb") as f:
                f.seek(max(0, size - _MARKER_TAIL_BYTES))
                tail = f.read().decode("utf-8", errors="replace")
        except OSError:
            continue
        for line in reversed(tail.splitlines()):
            try:
                obj = json.loads(line)
            except ValueError:
                continue
            if not isinstance(obj, dict) or obj.get("type") != "user":
                continue
            msg = obj.get("message")
            content = msg.get("content") if isinstance(msg, dict) else None
            text = content if isinstance(content, str) else json.dumps(content)
            if marker in text:
                return True
    return False


async def _verify_landed(
    pool: asyncpg.Pool, target_seat: str, marker: str, settings: Settings | None,
) -> bool:
    """Best-effort OUTCOME-READ (ruling 986b12f0: never let a receipt claim an effect nobody
    observed). A narrow, READ-ONLY re-check of the already-resolved holder's newest
    transcript — not a second resolver: dispatch_dm's own resolution walk (rate brakes, the
    crossed-registry guard, the daemon job listing) is never repeated here, only the marker's
    actual arrival is confirmed."""
    from src.orchestrator.seats import seat_receipt

    st = settings or get_settings()
    holder = (await seat_receipt(pool, target_seat) or {}).get("holder")
    if not holder:
        return False
    row = await pool.fetchrow(
        "SELECT job_dir FROM agent_mounts WHERE agent_id=$1 "
        "ORDER BY last_seen DESC NULLS LAST LIMIT 1", holder)
    if row is None or not row["job_dir"]:
        return False
    sid_prefix = Path(row["job_dir"]).name
    root = Path(st.osiris_sense_sessions) if st.osiris_sense_sessions \
        else Path.home() / ".claude" / "projects"
    return await asyncio.to_thread(_marker_landed_sync, root, sid_prefix, marker)


# dispatch_dm's mode → wake()'s honest vocabulary. THREE BUCKETS (#156.2, Thoth msg 3823):
# not-injectable (push is unavailable for a SYSTEM/CONFIG reason — nothing wrong with the
# addressee, the mail is queued and WILL be pulled), no-live-body (there is definitively
# nobody to wake — vacant, retired, or never mounted), refused-<gate> (a body/session DOES
# exist but a NAMED gate refused to use it). The OLD single "pull-only" -> "no-live-body"
# mapping was a LIE for two of its six call sites (trigger-dark, held): mail filed there
# WILL be pulled, nothing is missing — Alfred hit this live, concluded a reachable seat was
# unreachable, escalated, then retracted after testing delivery instead of trusting the
# string (60bc15db). Anything still not named here (queued-*, braked, skipped-*, settled,
# window-busy) is a rate brake, a pause, or an in-flight wake already covering it — all
# genuinely "queued" via the dict's own default — and `detail`/`raw_mode` carry the specific
# reason so nothing is lost to the bucket.
_WAKE_STATUS = {
    "nudged": "delivered", "resumed": "delivered", "poked": "delivered",
    # a seat past its own compaction seam wakes as its successor rather than staying
    # unreachable (ruling 94c2e7e8, dispatch 5398 leg 1) — the fresh body's first turn
    # is its own inbox() read, the same "next turn" framing "resumed" already carries.
    "fresh-heir": "delivered",
    "delivered": "mid-turn",  # dispatch_dm's own word for this means mid-turn, never delivered
    "trigger-dark": "not-injectable", "held": "not-injectable",
    "seat-vacant": "no-live-body", "retired": "no-live-body",
    "never-mounted": "no-live-body",
    # live by the SAME registry fleet() trusts, but no OS session could be resolved for
    # it — an outcome (mail queues, reads at its own next turn), never a failure; must
    # never collapse into "no-live-body", which is the false-absence class this fixes.
    "queued-live-unresolved": "queued",
    "resume-refused-compaction": "refused-compaction",
    "resume-refused-ceiling": "refused-ceiling",
    "resume-refused-no-anchor": "refused-no-anchor",
    "resume-refused-crossed-registry": "refused-crossed-registry",
    # a POSITIVE mismatch (found a different mind) and an ABSENCE of evidence either way
    # are no longer the same bucket (ruling f624d114) — an ignorance must never wear the
    # same status as a finding, even at the bucket level.
    "resume-refused-resident-unknown": "refused-resident-unknown",
    "resume-refused-unknown": "refused-unknown",
    # a manager is a LIVE human body, not a missing one — the human-attended guard queues the
    # knock in its box (perceived by pull), it does not forge the human's live turn (d8a77f80).
    "queued-human": "queued-human-attended",
    "refused": "refused-budget",  # the dollar wall, distinct from the authority refusal below
}


async def wake_worker(
    actions: Actions, *, caller: str, target: str, message: str,
    settings: Settings | None = None, spawn: Any = None, windows: Any = None,
    poke: Any = None, jobs: Any = None, nudge: Any = None,
) -> dict[str, Any]:
    """Knock on the other half of `caller`'s OWN managed_by pair. Refuses LOUDLY (nothing
    sent) when no active managed_by edge exists between the two seats in either direction —
    peers and cross-house calls route through a manager or the operator's desk instead.
    THE OPERATOR NEVER CALLS THIS: there is no operator parameter and none should ever be
    added — an override a caller can assert in an argument is an override that can be forged;
    the operator's real override stays out-of-band, their own hand in the window (Thoth,
    msg 989).

    On authorization, this posts `message` — prefixed with a self-identifying provenance
    marker (ruling 986b12f0: the harness cannot tell an agent's injection from a person's
    typing, so this refuses to hide behind that label) — as a graded ask (a wake IS a
    request for attention) addressed to the target's SEAT, and dispatches it through
    dispatch_dm, the exact path send() uses for every DM. A "delivered" status is only ever
    returned once the marker is CONFIRMED landed as a submitted turn in the target's own
    transcript — a queued injection that hasn't (yet, or ever) been seen reports honestly
    as "queued", never as an effect nobody observed."""
    from src.orchestrator.agents import house_of
    from src.orchestrator.seats import held_seat

    pool = actions.pool
    caller_held = await held_seat(pool, caller)
    caller_seat = (caller_held or {}).get("seat_id")
    if caller_seat is None:
        return {"mode": "refused-not-your-worker",
                "detail": f"{caller} holds no seat — wake() is a seat-to-seat act; an "
                          "unseated caller has no managed_by relationship to invoke it with"}
    target_seat = await _seat_for_target(actions, target)
    if target_seat is None:
        return {"mode": "refused-not-your-worker",
                "detail": f"'{target}' resolves to no living Seat — managed_by is Seat-to-"
                          "Seat and cannot arbitrate a target with none"}
    if not await _managed_edge(pool, caller_seat, target_seat):
        return {"mode": "refused-not-your-worker",
                "detail": f"no active managed_by edge between {caller_seat} and {target_seat} "
                          "in either direction — wake() only knocks within a manager<->worker "
                          "pair; peers and cross-house traffic route through a manager or the "
                          "operator's desk"}
    # THE LANE SWITCH (osiris_wake_enabled, default True since ruling 85fba696 — Anthropic
    # reviewed the disclosure and deemed the daemon reply lane INTENDED DESIGN, withdrawing
    # 482c3d0f's quarantine). It stays a SWITCH rather than becoming unconditional: the lane is
    # an undocumented internal that can break without notice, and an operator may want it dark
    # for a run. The check sits here, PAST authorization, so a refusal says honestly "you're
    # allowed, the lane is off" rather than a blanket "verb off" — and so no marker/DM is ever
    # minted when it is. Do not move it above the managed_by gate.
    st = settings or get_settings()
    if not st.osiris_wake_enabled:
        return {"mode": "refused-wake-frozen",
                "detail": "wake() is switched off (osiris_wake_enabled=False) — the pair is "
                          "authorized and the lane itself is sanctioned (ruling 85fba696), but "
                          "this deployment has it disabled, so no knock was sent"}
    marker = _wake_marker(caller, caller_seat, (caller_held or {}).get("handle"))
    body = f"{marker}\n\n{message}"
    res = await send_message(pool, from_agent=caller, from_project=await house_of(pool, caller),
                             to_agent=target_seat, body=body, grade="ask")
    d = await dispatch_dm(pool, addressee=res["to_agent"], msg_id=res["id"], sender=caller,
                          settings=settings, spawn=spawn, windows=windows, poke=poke,
                          jobs=jobs, nudge=nudge)
    mode = d.get("mode", "")
    status = _WAKE_STATUS.get(mode, "queued")
    out: dict[str, Any] = {"message_id": res["id"], "seat": target_seat, "raw_mode": mode,
                           "status": status, "detail": d.get("detail", mode)}
    if status == "delivered":
        observed = await _verify_landed(pool, target_seat, marker, settings)
        out["observed"] = observed
        if not observed:
            out["status"] = "queued"
            out["detail"] = (f"{d.get('detail', mode)} — injected but NOT YET CONFIRMED as a "
                             "submitted turn (a queued injection is not a seen one); it may "
                             "still land, or may never have been submitted at all")
    return out


# ═══ launch() — THE BODY VERB (thread 9f566244 piece D; ruling 43b84c5e) ══════════════════════
# wake() speaks to a body that already EXISTS; launch() CREATES one. It is a thin authority
# layer over the manager daemon's pty_spawn (src/manager/daemon.py), which already does the hard
# parts: identity-at-birth (the seat's one-time attach token in the child env, so it self-binds
# before its first breath — §4.2/5cef856b), the cgroup envelope (born bounded or not at all),
# window metadata for mail-routing, the lease gate, and the room-busy advisory. This verb adds
# only what pty_spawn deliberately lacks: (1) a managed_by authority gate, DOWNWARD-ONLY — a
# manager bodies a seat it manages; a worker may wake its manager (16722273) but never spawn one
# a body (78e3734e); (2) the [TAG] Handle naming; (3) idempotency (a live body is RETURNED, never
# twinned); (4) Ra's honest receipt (53ae1a87): body_exists and can_receive are SEPARATE states,
# each an independent READ, never one collapsed boolean. Creating a body is NOT the frozen reply
# lane — pty_spawn spawns a fresh `claude`, the same act the MINT lane already performs; it
# injects no turn into anyone's live session.


async def _manages(pool: asyncpg.Pool, manager_seat: str, worker_seat: str) -> bool:
    """DOWNWARD authority (78e3734e): an active managed_by edge FROM worker TO manager
    (managed_by is minted worker→manager, mintseat.py). launch() is downward-only — a birth is
    not a knock: a worker may wake its manager but may never spawn it a body."""
    return bool(await pool.fetchval(
        "SELECT 1 FROM links l JOIN objects f ON f.id=l.from_id JOIN objects t ON t.id=l.to_id "
        "WHERE l.type='managed_by' AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "AND f.canonical=$1 AND t.canonical=$2", worker_seat, manager_seat))


async def _manager_control(req: dict[str, Any]) -> dict[str, Any]:
    """One manager control op (the injectable default; tests supply their own). Raises on a dark
    daemon — launch_seat catches it and reports 'manager-cold' honestly, never a false success."""
    from src.manager.client import manager_call
    return await manager_call(req)


def _house_tag(house: str | None) -> str:
    """The window's [TAG] prefix — the operator's front-door naming ('[OS] Thoth', c8da5a52). A
    simple house-derived short code for now (osiris→OS); a real house→tag map is a later
    refinement, flagged in the launch() build."""
    h = (house or "").strip()
    return h[:2].upper() if h else "OS"


def _tree_exists(tree_cwd: str) -> bool:
    """A plain sync helper (ASYNC240: file I/O stays out of async function bodies, same
    convention src/ingest/reference.py documents) — launch_seat's one filesystem check that
    a bound tree_cwd is actually there, never provisioned by osiris itself (ff3bdc37)."""
    return Path(tree_cwd).is_dir()


def _launch_anchor(seat_id: str) -> str:
    """A STABLE durable anchor per SEAT (not per launch) — a re-launched seat re-wears its own
    anchor, the same 'one ghost, re-worn' discipline _wake_job_dir uses per project. The seat
    (via its attach token) is the true identity; this is the session anchor the trigger keys
    its mail-window routing on, so it must match the window's own job_dir metadata."""
    return str(Path.home() / ".claude" / "jobs" / seat_id.replace(":", "-"))


async def _launch_twin_check(
    pool: asyncpg.Pool, agents_json: Any, launch_cwd: str,
) -> dict[str, Any] | None:
    """THE SHARED TWIN GUARD, reused by BOTH launch doors (ruling 983ec87a, "two doors, one
    receipt") — the harness-native launch lane's own idempotency check used to consult ONLY
    `claude agents --json`, the harness's own `--bg` roster, which is INVISIBLE TO A RESUMED
    (`-p --resume`) BODY BY CONSTRUCTION (this file's own launch_seat docstring, decision
    536de12f). A live resumed session sitting at `launch_cwd` was therefore invisible to this
    guard — not an audit gap, a real double-mint collision risk (task #148's contested seam
    4). Reads BOTH halves of "what is running" and refuses on EITHER: `claude agents --json`
    (the harness's own, known-incomplete) AND `agent_mounts` (osiris's own registry, which a
    resumed body's mid-turn mount() call DOES reach). NEVER FIXES the harness roster's own
    incompleteness (that is THEIRS, per #148's ruling) — only stops trusting it ALONE.

    Returns None when neither source sees a live body at `launch_cwd` (safe to proceed).
    Otherwise a dict naming EXACTLY which source(s) fired (577988ed: a guard that wrongly
    blocks a legitimate launch is worse than the disease it prevents — a caller must be able
    to see and judge WHY this refused, never just that it did) — `harness` (the matching
    `claude agents --json` row, or None) and `mounts` (the matching agent_mounts row, or
    None). Both present is not treated as more ambiguous than either alone: two live-looking
    signals for the same cwd both mean the same thing (don't mint), so both are reported and
    both refuse the same way — there is no genuinely ambiguous case here to invent a third
    verdict for, only ONE OR THE OTHER OR NEITHER, and this reports exactly which."""
    try:
        roster = await agents_json(cwd=launch_cwd)
    except (OSError, TimeoutError, ValueError):
        roster = []
    from_harness = next((r for r in roster
                         if isinstance(r, dict) and r.get("cwd") == launch_cwd), None)
    from_mounts_row = await pool.fetchrow(
        "SELECT agent_id, last_seen FROM agent_mounts WHERE cwd=$1 "
        "ORDER BY last_seen DESC NULLS LAST LIMIT 1", launch_cwd)
    from_mounts = None
    if from_mounts_row is not None and from_mounts_row["last_seen"] is not None:
        from src.orchestrator.mounts import is_live
        if is_live(from_mounts_row["last_seen"]):
            from_mounts = {"agent_id": from_mounts_row["agent_id"],
                           "last_seen": from_mounts_row["last_seen"].isoformat()}
    if from_harness is None and from_mounts is None:
        return None
    return {"harness": from_harness, "mounts": from_mounts}


async def launch_seat(
    actions: Actions, *, caller: str, target: str, message: str = "",
    model: str | None = None, settings: Settings | None = None,
    manager: Any = None, windows: Any = None, substrate: str | None = None,
    spawn: Any = None, agents_json: Any = None, cost_reader: Any = None,
    resume_spawn: Any = None,
) -> dict[str, Any]:
    """Give a seat a BODY. Downward-only, managed_by-gated (a manager bodies a seat it manages).
    Idempotent: a live window for the seat is RETURNED, never twinned. The receipt reports
    body_exists and can_receive SEPARATELY, each from an independent read — Ra's requirement
    (53ae1a87): a launch that returns success must mean a body exists AND can receive, and where
    those are separable states the verb must not collapse them into one lying boolean.

    THE OPERATOR NEVER CALLS THIS (no operator param, exactly like wake): an override a caller can
    assert in an argument is an override that can be forged; the operator's hand stays out-of-band.

    SUBSTRATE (the default flip, task #68 wave, rulings 0fe36e59 + 33d6a2eb clause 3): `substrate`
    picks the spawn lane — an explicit argument wins, then `osiris_launch_substrate`
    (default 'harness'). 'harness' bodies the seat as a `claude --bg` background session
    (visible in `claude agents --json` BY CONSTRUCTION); 'pty' keeps the original osiris
    PTY-broker lane alive as an explicit, vendor-neutral fallback. `manager`/`windows` are
    injected for the PTY lane's tests; `spawn`/`agents_json`/`cost_reader` for the harness
    lane's — either way, without a live daemon or a live `claude` binary.

    THE RESUME LANE (operator's own order, 2026-08-04, ruling via decision a829a15d + msg
    3639 — reverses the "not built, deliberately" call in 315c3181): the harness lane, before
    minting fresh, now checks whether the seat's own lineage left a resumable session
    (`_lineage_resume_candidate` — walks past a zero-turn generation to find one, the SAME
    compaction+ceiling gates dispatch_dm's mail-wake already honors — resumable never means
    resume-everything) and, if so, CONTINUES it instead — reusing dispatch_dm's own resume
    machinery (`_resume_guard`, `_DM_RESUME_PROMPT`, `_spawn_claude`'s `--resume` lane it
    already runs in production) rather than reimplementing it, so the two can never drift
    apart the way a hand-copied gate check already has once this reign (eebeb1f).

    WHY A LINEAGE WALK, NOT PLAIN `_agent_resumable` (live-fire correction, 2026-08-04,
    Thoth msg 3691 — Sekhmet): a `--bg`-launched seat's every generation shares ONE durable
    per-seat mount anchor (`_launch_anchor`), fixed in CLAUDE_JOB_DIR for that OS process's
    entire life — so `agent_mounts`' one shared row for it NEVER encodes a real session id,
    only whichever generation most recently mounted, and a plain `agent_mounts`-keyed
    lookup is structurally blind to every earlier generation's own resumable transcript,
    not merely a stillborn one. `_lineage_resume_candidate` reads `session` instead — a
    GRAPH property, immune to the shared-anchor collapse — and the receipt now NAMES the
    decision every time (which generation, how many hops back, the actual transcript size
    and gate numbers), never a silent correct-by-accident refusal (4ef68cfe).

    ONE-SHOT, DELIBERATELY, not a standing window: a
    `-p --resume` body runs its one turn and exits (confirmed live: `claude agents --json
    --all` cannot retain it even when the body itself calls mount() mid-turn — a harness
    fact, not ours to fix, decision a829a15d). This is not a downgrade from `--bg`'s
    persistent window — it is RE-SUMMONABLE, not unreachable: dispatch_dm's own resume lane
    wakes the same session again on the next mail, so a standing window here would be a
    SECOND mechanism doing a job dispatch_dm's resume lane already does (38c71544's shape).
    Whether `osiris launch` should ALSO offer a persistent resumed window was once an open
    fork (thread 65d8846a) — RESOLVED, not left dangling: decision 696d302c named "the
    launch window is a property of launch, not a per-launch question" as the general rule
    for this whole class of recurring coordinator decisions, and this branch already lives
    that rule (window shape is DERIVED from `_lineage_resume_candidate`, never asked of a
    human per call). No persistent-resumed-window lane is being added on top of it; asking
    would reintroduce exactly the per-launch human judgment the ruling retired.
    `resume_spawn` injects `_spawn_claude` (the `-p --resume` lane) for this branch's tests,
    parallel to `spawn`/`agents_json`/`cost_reader` above.

    THE UNKNOWN ARM NEVER MINTS A STRANGER (thread ef88e2bb, operator, 2026-08-17, ruling
    7d6815bb — mirrored in `_cmd_launch_harness`, ruling 983ec87a's "two doors, one
    receipt"): a `resident-unknown` gate is an ABSENCE of signed testimony, not a positive
    finding of a different mind — it used to fall through to the same fresh mint as a real
    `crossed-registry` finding, minting strangers over ferryman's and halcyon's actually-
    resumable heads. It now refuses the WHOLE launch (`status: refused-resume-unknown`),
    spawning nothing, naming the exact `claude -p --resume <sid>` a human can run by hand.
    `crossed-registry` still falls through to fresh — that session was never this seat's."""
    pool = actions.pool
    from src.orchestrator.agents import _generation, house_of
    from src.orchestrator.seats import held_seat, seat_receipt
    manager = manager or _manager_control
    windows = windows or _manager_windows
    spawn = spawn or _spawn_claude_bg
    resume_spawn = resume_spawn or _spawn_claude
    agents_json = agents_json or _claude_agents_json
    cost_reader = cost_reader or _bg_session_cost

    caller_held = await held_seat(pool, caller)
    caller_seat = (caller_held or {}).get("seat_id")
    if caller_seat is None:
        return {"status": "refused-not-your-worker",
                "detail": f"{caller} holds no seat — launch is a seat-to-seat act; an unseated "
                          "caller has no managed_by relationship to invoke it with"}
    target_seat = await _seat_for_target(actions, target)
    if target_seat is None:
        return {"status": "refused-not-your-worker",
                "detail": f"'{target}' resolves to no living Seat — launch bodies a seat, and "
                          "managed_by is Seat-to-Seat"}
    if not await _manages(pool, caller_seat, target_seat):
        return {"status": "refused-not-your-worker",
                "detail": f"no active managed_by edge from {target_seat} up to {caller_seat} — "
                          "launch is DOWNWARD-ONLY (78e3734e): you may only body a seat you "
                          "manage; a worker cannot spawn its manager a body"}

    from src.orchestrator.seats import seat_facts
    facts = await seat_facts(pool, target_seat)
    handle, house, office = facts["handle"], facts["house"], facts["anchor_cwd"]
    if not handle:
        return {"status": "refused-no-handle",
                "detail": f"{target_seat} carries no handle assertion — a body cannot be named "
                          "for a nameless seat"}
    if not office:
        return {"status": "refused-no-office",
                "detail": f"{handle} ({target_seat}) has no anchor_cwd — establish_office first; "
                          "a body needs a room to be born in"}

    # TREE_CWD (task #103's re-scope, ff3bdc37, Thoth DM 2794): the seat's OFFICE (identity —
    # unchanged above, `office`) and its CODE CHECKOUT are distinct properties on purpose;
    # collapsing them is John's own catastrophe (#128) repeated. OSIRIS NEVER PROVISIONS a
    # tree (harness owns isolation) — a bound-but-missing tree_cwd is refused, mirroring the
    # refused-no-office shape exactly, one line up; an UNSET tree_cwd falls back to `office`
    # silently, the unchanged behavior every seat not doing isolated tree work keeps getting.
    # `launch_cwd` — never `office` alone — is what the body actually spawns into and what
    # idempotency below matches against, or a tree-bound seat would twin on every relaunch
    # (its live process sits at tree_cwd; a check still reading `office` would never find it).
    tree_cwd = facts["tree_cwd"]
    launch_cwd = office
    if tree_cwd:
        if not _tree_exists(tree_cwd):
            return {"status": "refused-no-tree",
                    "detail": f"{handle} ({target_seat}) names tree_cwd={tree_cwd!r} but it "
                              "does not exist on disk — osiris expects the harness (or a "
                              "human, via EnterWorktree) to have created it before launch; "
                              "it never provisions one itself"}
        launch_cwd = tree_cwd

    # THE ATTACH LINE (ruling 0fe36e59, thread c171a3de finding #6): spawn location must never
    # matter, so REACHING the body can never depend on which harness project slug its cwd
    # happened to register under — every receipt below hands the operator everything needed
    # to get there directly, independent of that. `command` uses the plumbing that's actually
    # live today (src/manager/attach.py); `osiris attach <handle>` is the same door once that
    # CLI build (thread 16a0c76b) lands.
    anchor = _launch_anchor(target_seat)
    attach = {"office": office, "tree_cwd": tree_cwd, "session_anchor": anchor,
             "command": f'python -m src.manager.attach "[{_house_tag(house)}] {handle}"'}

    # ONE SEAT, ONE LIVE LINEAGE HEAD (ruling 921eabcf item 1, obligation 164fc26c —
    # the halcyon specimen: xxi job 39ece19d + xxiii job db9ff657, both heartbeating on
    # the same seat). Consult the SAME occupancy authority the fold/reanimation/send()
    # doors already share (`is_occupied_by_a_live_body`, built at 3014910) rather than a
    # second notion of occupancy — checked centrally, before either spawn lane, so a
    # launch can never fork a second eligible head regardless of substrate.
    from src.orchestrator.agents import is_occupied_by_a_live_body
    current_holder = ((await seat_receipt(pool, target_seat)) or {}).get("holder")
    if current_holder and await is_occupied_by_a_live_body(
        pool, current_holder, agents_json=agents_json,
    ):
        return {"status": "refused-occupied", "seat": target_seat, "holder": current_holder,
                "body_exists": True, "can_receive": True, "attach": attach,
                "detail": f"{handle} ({target_seat}) is already occupied by a live body "
                          f"({current_holder}, confirmed via registry_census) — one seat, "
                          "one live lineage head; refusing rather than forking a second "
                          "eligible head. If this is stale, vacate_seat first."}

    st = settings or get_settings()
    lane = (substrate or st.osiris_launch_substrate or "harness").strip().lower()
    # MODEL PRECEDENCE (task #68, finding #7, thread 20e4feb6): an explicit caller param wins,
    # then the SEAT'S OWN stamped intended_model (mint_seat's pin — the whole point of naming
    # a model per worker), and only then the trigger's own global default. The old order
    # skipped the stamp entirely, which is why a seat pinned to sonnet-5 could spawn on
    # whatever osiris_wake_model happened to be that day, silently.
    argv_model = model or facts.get("intended_model") or st.osiris_wake_model or None
    name = f"[{_house_tag(house)}] {handle}"

    out: dict[str, Any]
    if lane == "pty":
        # ═══ THE OSIRIS PTY-BROKER LANE — the ORIGINAL substrate, kept alive as an explicit,
        # vendor-neutral FALLBACK (rulings 0fe36e59 + 33d6a2eb clause 3) for an incident, or a
        # harness build with no --bg. No longer the default; a caller (or the settings knob)
        # must ask for it by name.
        #
        # IDEMPOTENCY — a live window already holding this seat is RETURNED, never twinned
        # (the one-body-at-a-time discipline; Ra's stale-liveness collision, b3a86a7d).
        # Fail-open on a dark daemon: an empty roster means nothing to collide with, and the
        # spawn below reports its own state.
        try:
            roster = await windows()
        except (OSError, TimeoutError, ValueError):
            roster = []
        for w in roster:
            if isinstance(w, dict) and w.get("seat_id") == target_seat and w.get("alive"):
                return {"status": "already-live", "window": w.get("name"), "seat": target_seat,
                        "body_exists": True, "can_receive": True, "attach": attach,
                        "detail": f"a live body already holds {handle} — not minting a twin"}

        argv = ["claude"]
        if argv_model:
            argv += ["--model", argv_model]
        # A FULL env, minus the launcher's own CLAUDE_JOB_DIR (never inherit an anchor — the
        # collision class, 2294e95d), plus the body's own stable anchor so it matches the
        # window's own job_dir metadata. pty_spawn adds OSIRIS_SEAT_ID + OSIRIS_ATTACH_TOKEN
        # on top (identity at birth); a child with only those two vars has no PATH, so the
        # real environ is the base.
        child_env = {k: v for k, v in os.environ.items() if k != "CLAUDE_JOB_DIR"}
        child_env["CLAUDE_JOB_DIR"] = anchor

        try:
            res = await manager({
                "op": "pty_spawn", "name": name, "argv": argv, "cwd": launch_cwd,
                "seat": {"handle": handle, "house": house}, "job_dir": anchor,
                "env": child_env})
        except (OSError, TimeoutError, ValueError) as exc:
            return {"status": "manager-cold", "seat": target_seat,
                    "detail": f"the manager daemon is unreachable ({exc}) — ask the operator "
                              "to start osiris-manager; NOTHING was spawned"}
        if not isinstance(res, dict) or res.get("error"):
            return {"status": "refused-spawn", "seat": target_seat,
                    "detail": (res.get("error") if isinstance(res, dict) else str(res))
                    or "the manager refused the spawn"}

        spawned = res.get("spawned")
        # RA'S RECEIPT (53ae1a87): body_exists is the spawn's own word; can_receive is a
        # SEPARATE, independent READ. A fresh claude takes seconds to boot and self-mount, so
        # at THIS instant the window exists but is almost never live yet.
        alive = False
        try:
            for w in await windows():
                if isinstance(w, dict) and w.get("name") == spawned and w.get("alive"):
                    alive = True
                    break
        except (OSError, TimeoutError, ValueError):
            pass
        out = {
            "status": "launched", "window": spawned, "seat": res.get("seat_id", target_seat),
            "body_exists": bool(spawned), "can_receive": alive, "spawned_model": argv_model,
            "detail": ("body created and live" if alive else
                       "body created; mount NOT yet confirmed — the claude is booting and "
                       "will self-bind via its attach token; confirm with pty_list / "
                       "occupancy"),
            "attach": attach,
        }
        if res.get("room_busy"):
            out["room_busy"] = res["room_busy"]
            if res.get("room_busy_note"):
                out["room_busy_note"] = res["room_busy_note"]
    else:
        # ═══ THE HARNESS-NATIVE LANE (the default flip, task #68 wave) — `claude --bg` +
        # `claude agents --json` instead of the manager daemon's PTY broker + claim-socket.
        # Every body this creates is visible in the operator's own `claude agents` list BY
        # CONSTRUCTION (clause 3, "front end wide open", made mechanical instead of patched
        # around) — see the spike verdict f2dc98549521 and _spawn_claude_bg's own docstring.
        #
        # IDEMPOTENCY MATCHES ON THE SEAT'S OWN LAUNCH CWD, NOT A SESSION ID (live finding,
        # 2026-07-27, replacing the original design): `claude --bg` MANAGES ITS OWN SESSION ID
        # and silently ignores an explicit `--session-id` ("warning: --bg manages the session
        # id; ignoring --session-id" on stderr, which a fire-and-forget spawn never reads) —
        # confirmed against a real spawn, not assumed. A seat's launch location (office, or
        # tree_cwd when bound — #103) is 1:1 with the seat by construction, so any live
        # process already sitting there IS its body, whatever session id the harness gave it.
        # MUST be `launch_cwd`, never bare `office`: a tree-bound seat's live process sits at
        # tree_cwd, and matching on `office` alone would never find it, twinning on relaunch.
        twin = await _launch_twin_check(pool, agents_json, launch_cwd)
        if twin is not None:
            seen_via = [s for s in (
                f"claude agents --json ({twin['harness'].get('name')!r})"
                if twin["harness"] else None,
                f"agent_mounts ({twin['mounts']['agent_id']}, last_seen "
                f"{twin['mounts']['last_seen']})" if twin["mounts"] else None,
            ) if s]
            return {"status": "already-live",
                    "window": (twin["harness"] or {}).get("name"), "seat": target_seat,
                    "body_exists": True, "can_receive": True, "attach": attach,
                    "seen_via": seen_via,
                    "detail": f"a live body already holds {handle} — not minting a twin "
                              f"(seen via {', '.join(seen_via)})"}

        # THE RESUME LANE (this docstring's own "THE RESUME LANE" section explains the
        # policy; this is just the mechanism). Checked BEFORE minting fresh. `holder` (not
        # `target_seat`) is the identity `_lineage_resume_candidate`/`_resume_guard` need —
        # a Seat is never itself an Agent lineage. WALKS THE LINEAGE (`_lineage_resume_
        # candidate`, not the plain agent_mounts-keyed `_agent_resumable`/`wakeable_
        # identity` dispatch_dm's DM lane uses) — a `--bg`-launched seat's every generation
        # shares ONE durable per-seat mount anchor, so the job-dir-keyed lookup is
        # structurally blind to any of them; the lineage's own `session` graph property
        # survives where agent_mounts cannot (see that function's own docstring; live-fire
        # finding, Thoth msg 3691, Sekhmet).
        holder = ((await seat_receipt(pool, target_seat)) or {}).get("holder")
        resume_outcome = await _lineage_resume_candidate(
            pool, holder, st, repo=launch_cwd) if holder else ["no seat holder on record"]
        resume_log = resume_outcome[1] if isinstance(resume_outcome, tuple) else resume_outcome
        resume = resume_outcome[0] if isinstance(resume_outcome, tuple) else None
        if resume is not None:
            # holder is truthy whenever resume is set — resume_outcome only comes from
            # _lineage_resume_candidate(holder, ...), never the bare-string branch, when
            # holder was falsy. Asserted, not silently narrowed: a violated invariant here
            # should be loud, never a quiet skip of the identity gate.
            assert holder is not None
            # hop count (#173a): the SAME arithmetic `_lineage_resume_candidate`'s own
            # success line renders ("...resumable, N hop(s) back") — its log always ends
            # with exactly one success entry when `resume` is set, so the count of entries
            # BEFORE it (this list minus that one) is N. `launch_cwd` is this seat's own
            # launch location (office, or tree_cwd when tree-bound), the 1:1 identity the
            # zero-hop graph door corroborates against.
            gate, refusal = await _resume_guard(
                pool, resume, _generation(holder)[0], seat_id=target_seat, st=st,
                hop=len(resume_log) - 1, launch_cwd=launch_cwd)
            if gate == "resident-unknown":
                # THE FIX FOR ef88e2bb (operator, 2026-08-17, ruling 7d6815bb — self-healing
                # over manual bandaids): an ABSENCE of signed testimony is not evidence this
                # head belongs to someone else — it is exactly the case a resumable session
                # (ferryman gen 8, 2MB, 0 hops back) was refused for and then a stranger was
                # minted OVER it anyway. "crossed-registry" (a POSITIVE finding of a
                # different mind) still falls through to a fresh mint below — that session
                # genuinely isn't this seat's, so a fresh body under this seat's own name is
                # legitimate. "resident-unknown" gets no such pass: refuse the WHOLE launch,
                # spawn nothing, name the exact command a human can run to confirm it
                # themselves — never silently degrade into "mint a stranger." NOTE (#173a):
                # this branch is only reached when the zero-hop graph door (passed via
                # hop/launch_cwd above) did NOT already clear the gate — a zero-hop,
                # graph-corroborated candidate resolves gate=None before this check ever runs.
                return {"status": "refused-resume-unknown", "seat": target_seat,
                        "session": resume[0], "body_exists": False, "can_receive": False,
                        "detail": f"{refusal} — refusing rather than minting a stranger "
                                  f"over a possibly-resumable head; run `claude -p --resume "
                                  f"{resume[0]}` by hand to confirm it yourself, or clear "
                                  "the seat's stale session pointer if it's truly dead"}
            if gate is not None:
                resume_log = [*resume_log, f"{gate} guard refused it: {refusal}"]
                resume = None
        if resume is not None:
            session_id, repo = resume[0], resume[1]
            # THE MESSAGE LANDS BEFORE THE SPAWN, deliberately unlike the fresh-mint lane
            # below (which sends its brief AFTER spawning): a fresh `--bg` body takes
            # seconds to boot, mount, and claim_name before its first inbox() call, so send-
            # after-spawn there is safely ordered by the boot lag alone. A RESUMED body has
            # no such lag — its first turn IS the inbox() check (_DM_RESUME_PROMPT) — so the
            # mail row must exist first, the same "ledger before spawn" discipline
            # dispatch_dm's own resume branch already follows for its wake ledger.
            resume_brief_id: int | None = None
            if message.strip():
                sent = await send_message(
                    pool, from_agent=caller, from_project=await house_of(pool, caller),
                    to_agent=target_seat, body=message, grade="ask")
                resume_brief_id = sent.get("id")
            await resume_spawn(repo, _DM_RESUME_PROMPT, resume_session=session_id,
                               model=argv_model, allowed_tools=st.osiris_wake_allowed_tools
                               or None)
            out = {
                "status": "launched", "mode": "resumed", "seat": target_seat,
                "session": session_id, "body_exists": True, "can_receive": True,
                "spawned_model": argv_model, "attach": attach,
                "resume_check": resume_log,
                "detail": f"resumed session {session_id[:8]} as a ONE-SHOT turn — walked "
                          f"{len(resume_log)} generation(s) back to find it "
                          f"({'; '.join(resume_log)}); it runs the brief and exits; "
                          "`claude agents --json` shows it only WHILE it runs, never after "
                          "(a harness fact, not a bug: a further mail wake continues it, "
                          "exactly like any other dormant addressee)",
            }
            if resume_brief_id is not None:
                out["brief_message_id"] = resume_brief_id
            stamped_model = facts.get("intended_model")
            if stamped_model and argv_model != stamped_model:
                out["model_mismatch"] = (
                    f"spawned on {argv_model!r} but the seat's own stamped intended_model "
                    f"is {stamped_model!r} — never silent (thread 20e4feb6)")
            return out

        # IDENTITY, VIA THE SESSION'S OWN FIRST TURN, NOT ENV STAMPING (live finding,
        # 2026-07-27, replacing the original design): `--bg` claims a PRE-FORKED spare
        # process off a claim-socket (confirmed via `ps`/`/proc/<pid>/environ` on a real
        # spawn — the spare's env is fixed at fork time, long before this call), so NEITHER
        # CLAUDE_JOB_DIR NOR any OSIRIS_*-prefixed var this call sets ever reaches the
        # claimed session — a real launch mounted anonymous (agent:<sid8>, seat=null) with
        # every one of them stamped. `claude [options] [prompt]` DOES deliver a trailing
        # positional prompt as the session's genuine first turn (confirmed live) — so the
        # boot instruction rides THAT, telling the session to bind itself via mount() +
        # claim_name(handle), the same proven, independently-tested adoption path a human
        # follows into a fresh office (claim_name: a name matching an already-vacant seat is
        # ADOPTED, never twinned — see mint_seat's own receipt: '...or start a session in
        # the office and have it claim_name itself').
        # THE BOOT PROMPT ANCHORS mount() AT THE OFFICE, ALWAYS — never `launch_cwd`. Identity
        # lives at the office regardless of where the process's own cwd happens to sit (#103's
        # whole point); a tree-bound seat's session boots WITH its shell cwd at tree_cwd (the
        # `spawn(launch_cwd, ...)` below) but is told to mount(cwd=office) all the same, the
        # identical pattern every seat in this house already follows by hand.
        boot_prompt = _bg_boot_prompt(office=office, anchor=anchor, handle=handle)

        # THE DORMANT-HISTORY CONFESSION (thread fc69b9b4, Ooblek specimen 2026-08-02): a
        # fresh mind can land on a transcript full of history it cannot read — `--bg` picks
        # its own session id (the finding two comments up), so this side of the call cannot
        # know or prevent a reuse, only NAME what's already sitting at launch_cwd before it
        # happens. Checked pre-spawn on purpose, same "tell the truth at launch time" framing
        # as the rest of this lane's receipt.
        #
        # BOTH SLUGS, ALWAYS (task #135/#136, 2026-08-03): office and tree_cwd are two
        # DIFFERENT slugs by design (#103) — checking only `launch_cwd` missed whichever one
        # this particular launch was NOT spawning into. A dormant transcript can sit under
        # either, so both are checked regardless of which one launch_cwd resolved to; the
        # freshest match is confessed.
        from src.ingest.sessions import dormant_history_confession
        dormant = dormant_history_confession(
            office, *([tree_cwd] if tree_cwd else []),
            ceiling_bytes=st.osiris_resume_ceiling_bytes,
            min_tail_bytes=st.osiris_resume_min_tail_bytes)

        try:
            await spawn(launch_cwd, name=name, model=argv_model, prompt=boot_prompt)
        except OSError as exc:
            return {"status": "refused-spawn", "seat": target_seat,
                    "detail": f"claude --bg failed to start ({exc}); NOTHING was spawned"}

        # RA'S RECEIPT (53ae1a87), same law as the PTY lane: body_exists is the spawn's own
        # word; can_receive is a SEPARATE, independent read — `claude --bg` returns as soon as
        # the harness's own background-agent daemon takes the session over, which can be
        # seconds before it necessarily shows up in `claude agents --json`.
        alive_row: dict[str, Any] | None = None
        try:
            alive_row = next((r for r in await agents_json(cwd=launch_cwd)
                              if isinstance(r, dict) and r.get("cwd") == launch_cwd), None)
        except (OSError, TimeoutError, ValueError):
            pass
        # THE RECEIPT NAMES THE RESUME DECISION EVERY TIME (Thoth's explicit condition,
        # msg 3691: "a correct decision made silently is indistinguishable from a broken
        # one"). `resume_log` carries WHY this booted fresh instead — numbers, not
        # adjectives (transcript size, compaction count, the gate's own threshold), one
        # line a human reads instead of re-running succession_chain by hand.
        booted_why = (f"booted fresh — {'; '.join(resume_log)} (gate: min_tail_bytes="
                      f"{st.osiris_resume_min_tail_bytes}, ceiling="
                      f"{st.osiris_resume_ceiling_bytes}b)")
        out = {
            "status": "launched", "window": name, "seat": target_seat,
            "body_exists": True, "can_receive": alive_row is not None,
            "spawned_model": argv_model, "resume_check": resume_log,
            "detail": (f"{booted_why}; live" if alive_row is not None else
                       f"{booted_why}; mount NOT yet confirmed — the claude is booting and "
                       "will self-bind via its own boot prompt; confirm with `claude agents "
                       "--json`"),
            "attach": attach,
        }
        if dormant is not None:
            out["dormant_history"] = dormant
        # THE CEILING'S READ PATH (task #8, unblocked by the binding leg): a --bg body is a
        # real billed session on a metered backend, same as any other one — recorded into
        # llm_usage or the ceiling never learns it happened (the ghost-farm disease,
        # wake_cost's own doctrine: "a hand you cannot cost is a hand you cannot govern").
        # _bg_session_cost never fabricates: a real cost_usd if the harness ever grows one,
        # the honest UNPRICED/blind class today (confirmed live, 2026-07-27 — no cost field
        # exists on this surface at all) — never folded into 0, which the ceiling would
        # silently read as a free call. Fail-open: a metering failure must never cost a launch.
        # Needs the REAL session id the harness assigned (the live roster row, just read for
        # can_receive) — a launch not yet visible there has nothing to look up YET; it is
        # simply not metered this cycle rather than metered on a guessed id.
        real_session_id = (alive_row or {}).get("sessionId")
        if real_session_id:
            try:
                priced = await cost_reader(str(real_session_id), cwd=launch_cwd)
                from src.ingest.providers import Usage
                from src.ingest.usage import record_usage
                await record_usage(
                    pool, purpose="launch",
                    usage=Usage(model=argv_model or "unknown",
                               cost_usd=(priced.get("cost_usd")
                                         if priced.get("priced") else None)))
            except Exception:  # noqa: BLE001 — a metering failure must never cost a launch
                _log.debug("launch cost-metering failed for %s", real_session_id,
                          exc_info=True)

    # THE OPENING BRIEF rides the ordinary mail→poke lane, never a hand-forged turn: sent as a
    # graded ask to the new seat, the trigger types it into the fresh window once it is alive.
    brief_id: int | None = None
    if message.strip():
        sent = await send_message(
            pool, from_agent=caller, from_project=await house_of(pool, caller),
            to_agent=target_seat, body=message, grade="ask")
        brief_id = sent.get("id")

    stamped_model = facts.get("intended_model")
    if stamped_model and argv_model != stamped_model:
        out["model_mismatch"] = (
            f"spawned on {argv_model!r} but the seat's own stamped intended_model is "
            f"{stamped_model!r} — never silent (thread 20e4feb6)")
    if brief_id is not None:
        out["brief_message_id"] = brief_id
    return out


async def _real_kill_pid(pid: int) -> None:
    """SIGTERM, the real default — a graceful ask, not SIGKILL: a live turn gets the chance
    to unwind (flush a partial write, let the harness mark the session cleanly ended)
    rather than vanishing mid-syscall. Injectable so no test ever touches a real process."""
    import signal
    os.kill(pid, signal.SIGTERM)


async def stop_seat(
    actions: Actions, *, caller: str, target: str | None, reason: str = "",
    kill: Any = None, agents_json: Any = None, read_exe: Any = None, read_cwd: Any = None,
) -> dict[str, Any]:
    """STOP — the process-lifecycle inverse of launch_seat (#156's held half, ruling
    94c2e7e8 leg 2: "reachable and stoppable are the same lane's two directions"). Ends a
    LIVE body's own OS process (SIGTERM) — nothing more. Deliberately NOT a symmetric
    'sleep' (the operator's own naming ruling, b3ccd3f6: no promised thaw-where-you-left-
    off — a `stop`/`wake` pair would promise a resume nobody can guarantee across an
    arbitrary gap). NEITHER does this gate future reachability on some new flag of its
    own: the SAME occupancy authority (`is_occupied_by_a_live_body` / `registry_census`)
    that already governs launch/fold/reanimation/send() reads the real OS state, so the
    instant this process actually exits, that authority sees it gone — a later wake() or
    launch() boots a fresh successor with no separate 'unstop' step to remember, ONE
    occupancy authority for every door, never a second notion of it (ruling 921eabcf).

    DOWNWARD-ONLY, mirroring launch_seat's own authority (a manager stops a seat it
    manages) — a worker may never stop its own manager's body. `target=None` always
    allowed (going quiet on purpose, pause_seat's own precedent) — the one case
    authority never needs to arbitrate.

    Refuses (nothing signaled) on: no seat, no managed_by edge (`refused-not-your-
    worker`), or no harness/proc-CONFIRMED live body for the seat's current holder
    (`no-live-body` — a stale mount row, or an already-dead process, is not something to
    signal). `stopped_at`/`stopped_reason` land on the seat object (survives succession —
    it names an EVENT, not a gate on the chair) as a plain, append-only assertion, never a
    boolean flag a later caller has to remember to clear."""
    pool = actions.pool
    from src.orchestrator.mounts import registry_census
    from src.orchestrator.seats import held_seat, seat_receipt
    kill = kill or _real_kill_pid
    self_stop = target is None
    if self_stop:
        target_seat = ((await held_seat(pool, caller)) or {}).get("seat_id")
        if target_seat is None:
            return {"status": "refused-no-seat",
                    "detail": f"{caller} holds no seat — nothing to stop"}
    else:
        assert target is not None  # self_stop is False only when target was given
        caller_held = await held_seat(pool, caller)
        caller_seat = (caller_held or {}).get("seat_id")
        if caller_seat is None:
            return {"status": "refused-not-your-worker",
                    "detail": f"{caller} holds no seat — stop is a seat-to-seat act; an "
                              "unseated caller has no managed_by relationship to invoke "
                              "it with"}
        target_seat = await _seat_for_target(actions, target)
        if target_seat is None:
            return {"status": "refused-not-your-worker",
                    "detail": f"'{target}' resolves to no living Seat"}
        if not await _manages(pool, caller_seat, target_seat):
            return {"status": "refused-not-your-worker",
                    "detail": f"no active managed_by edge from {target_seat} up to "
                              f"{caller_seat} — stop is DOWNWARD-ONLY, mirroring launch "
                              "(78e3734e): a worker may never stop its own manager's body"}
    holder = ((await seat_receipt(pool, target_seat)) or {}).get("holder")
    if not holder:
        return {"status": "no-live-body", "seat": target_seat,
                "detail": "the seat has no current holder — nothing to stop"}
    census = await registry_census(
        pool, agents_json=agents_json, read_exe=read_exe, read_cwd=read_cwd)
    match = next((m for m in census.get("matched", []) if m.get("agent_id") == holder), None)
    pid = match.get("pid") if match else None
    if not isinstance(pid, int):
        return {"status": "no-live-body", "seat": target_seat, "holder": holder,
                "detail": f"{holder} carries no harness/proc-confirmed live body right "
                          "now — nothing to signal (already dead, or never really live)"}
    try:
        await kill(pid)
    except ProcessLookupError:
        return {"status": "no-live-body", "seat": target_seat, "holder": holder, "pid": pid,
                "detail": "the process was already gone by the time the signal landed"}
    except OSError as exc:
        return {"status": "refused-signal", "seat": target_seat, "holder": holder, "pid": pid,
                "detail": f"the stop signal itself failed ({exc}) — nothing else changed"}
    now = datetime.now(UTC)
    oid = await actions.create_or_find_object("Seat", target_seat, caller)
    await actions.assert_property(oid, "stopped_at", now.isoformat(), caller, now, 0.9,
                                  evidence_class="self_declared")
    if reason:
        await actions.assert_property(oid, "stopped_reason", reason[:500], caller, now, 0.9,
                                      evidence_class="self_declared")
    return {"status": "stopped", "seat": target_seat, "holder": holder, "pid": pid,
            "by": caller, **({"reason": reason} if reason else {}),
            "detail": f"SIGTERM sent to {holder}'s live body (pid {pid}) — reachability "
                      "afterward is governed entirely by the SAME occupancy authority "
                      "launch()/wake() already consult; no separate release step exists"}


async def _transcript_activity(
    pool: asyncpg.Pool, holder: str, st: Settings,
) -> tuple[bool, bool]:
    """(transcript_checked, fresh) for `holder`'s own resumable session, by the transcript's
    newest TIMESTAMPED line — never mtime (the Aegis phantom: a session 13h dead wore a
    seconds-old mtime, bumped by something in the chrome/daemon that is not a turn). The
    same `_turn_fresh_sync` reads dispatch_dm's own mid-turn gate already trusts. No
    findable transcript at all → (False, False): not evidence of life, just nothing more
    to check beyond the roster."""
    resume = await _agent_resumable(pool, holder, st)
    if resume is None:
        return False, False
    root = Path(st.osiris_sense_sessions) if st.osiris_sense_sessions \
        else Path.home() / ".claude" / "projects"
    fresh = await asyncio.to_thread(_turn_fresh_sync, root, resume[0], st.osiris_dm_active_secs)
    return True, fresh


async def vacate_dead_seat(
    actions: Actions, *, seat_id: str, actor: str, because: str,
    agents_json: Any = None, settings: Settings | None = None,
    transcript_activity: Any = None,
) -> dict[str, Any]:
    """THE VACATE-DEAD-HOLDER VERB (thread 445a7356, Thoth's ruling msg 1611) — the
    evidence-gathering complement to seats.vacate_holder's bare write, and to
    seats.retire_seat's stale-holder refusal (never its bypass: that refusal stays
    exactly as-is, correctly declining to evict a live mind — this exists for the one
    case it correctly can't resolve alone, a holder whose PROCESS actually died without
    ever calling retire() on itself; found live during task #68's acceptance demo).

    GATED ON REAL LIVENESS EVIDENCE, CONJUNCTIVELY — refuses loudly, never guesses, if
    EITHER signal disagrees with the other:
      (1) the harness roster (`claude agents --json`) shows NO live session at the
          seat's own office cwd — the substrate-agnostic front door (works for a
          harness-native body same as a PTY one, since both register under the office);
      (2) the holder's own transcript's newest TIMESTAMPED LINE is stale — NOT mtime
          (the Aegis phantom, 2026-07-21: a session 13h dead wore a seconds-old mtime,
          bumped by something in the chrome/daemon that is not a turn) — the same
          `_turn_fresh_sync` reads dispatch_dm's own mid-turn gate already trusts.
    Neither signal alone is enough: a fresh mtime with no roster entry could be a
    just-exited process the roster hasn't dropped yet; a roster miss with a genuinely
    fresh transcript line could be a body whose live process sits outside this office's
    own cwd tracking. Both agreeing is the bar. A holder with NO findable transcript at
    all is not treated as ambiguous — the roster (a direct, present-tense signal) already
    settles it, and being unable to find MORE evidence of life is not evidence of life.

    AUTO-INVOCATION IS OUT OF SCOPE (reaper #59, stays operator-gated per Thoth's
    ruling) — this is for a deliberate hand, called once on a specific seat, never a
    sweep."""
    st = settings or get_settings()
    agents_json = agents_json or _claude_agents_json
    transcript_activity = transcript_activity or _transcript_activity
    pool = actions.pool
    from src.orchestrator.seats import seat_facts, seat_receipt, vacate_holder

    receipt = await seat_receipt(pool, seat_id)
    holder = (receipt or {}).get("holder") if receipt else None
    if not holder:
        return {"status": "refused-vacant", "seat": seat_id,
                "detail": f"{seat_id} is already vacant — nothing to vacate"}
    facts = await seat_facts(pool, seat_id)
    office = facts.get("anchor_cwd")
    if not office:
        return {"status": "refused-no-office", "seat": seat_id,
                "detail": f"{seat_id} has no anchor_cwd on record — a liveness check with "
                          "nowhere to look is not evidence of death"}

    # SIGNAL 1 — the harness roster, the substrate-agnostic front door.
    try:
        roster = await agents_json(cwd=office)
    except (OSError, TimeoutError, ValueError):
        return {"status": "refused-ambiguous", "seat": seat_id,
                "detail": "the harness roster could not be read — a liveness check that "
                          "cannot see is not evidence of death; refusing rather than "
                          "guessing"}
    live_row = next((r for r in roster if isinstance(r, dict) and r.get("cwd") == office),
                    None)
    if live_row is not None:
        return {"status": "refused-live", "seat": seat_id,
                "detail": f"a live session ({live_row.get('name', 'unnamed')!r}) still "
                          f"sits at {office} — never evicts a live mind"}

    # SIGNAL 2 — the holder's own transcript, by its newest TIMESTAMPED line (never mtime).
    transcript_checked, fresh = await transcript_activity(pool, holder, st)
    if fresh:
        return {"status": "refused-live", "seat": seat_id,
                "detail": f"{holder}'s own transcript shows a turn within the last "
                          f"{st.osiris_dm_active_secs}s — mtime alone lies (the Aegis "
                          "phantom); this reads the transcript's own timestamped "
                          "content and it disagrees with the roster"}

    out = await vacate_holder(actions, seat_id=seat_id, actor=actor, because=because)
    if "error" in out:
        return {"status": "refused", "seat": seat_id, "detail": out["error"]}
    return {"status": "vacated", "seat": seat_id, "was_held_by": out["was_held_by"],
            "evidence": {"roster_checked": True, "transcript_checked": transcript_checked},
            "detail": f"no live process at {office} and no fresh transcript activity from "
                      f"{holder} — vacated"}


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


# --- THE HARNESS-NATIVE SUBSTRATE (task #68 item 9, ruling 33d6a2eb clause 3; spike verdict
# f2dc98549521) --------------------------------------------------------------------------------
#
# A third sibling to _spawn_claude/_spawn_in_body: `claude --bg` + `claude agents --json`
# instead of a bare -p child, a metered body, or the manager daemon's PTY broker + claim-socket.
# The spike verified live (claude --help + a real `claude agents --json` sample against the
# fleet's own running sessions) that these documented, sanctioned flags already cover both
# halves launch() needs — documented flags beat a hand-rolled client against the undocumented
# daemon spare-pool claim-socket protocol, and that preference SURVIVES ruling 85fba696 (which
# withdrew 482c3d0f's do-not-build bar): the lane being sanctioned makes it permitted, not
# preferable — a documented flag still can't break under us silently. Every spawned
# session is visible in `claude agents --json` BY CONSTRUCTION — clause 3 ("front end wide
# open") made mechanical, not patched around (contrast the attach-line receipt, part 2 of this
# task, which is an interim fix for the OLD substrate's blind spot, not a replacement for this).

def _bg_boot_prompt(*, office: str, anchor: str, handle: str) -> str:
    """The harness-native lane's boot instruction (see `_spawn_claude_bg`'s own docstring for
    why this replaces env-var identity stamping): a `--bg` session's genuine first turn tells
    it to bind itself via the same proven mount()+claim_name() path a human follows into a
    fresh office. Shared by `launch_seat` and `osiris launch` (task #72, thread 842aa184) —
    one wording, never two copies to drift apart."""
    return (
        f'You have just been launched into your own seat\'s office. Call '
        f'mount(cwd="{office}", job_dir="{anchor}"), then claim_name("{handle}") — that '
        f"exact name — to bind to the seat already waiting for you. Then inbox() for "
        f"your opening brief. No one is watching this window: work the brief to "
        f"completion, a real blocker, or a seam — never park on a question typed into "
        f"this empty room; a genuine ask goes out as mail (grade='ask')."
    )


async def _spawn_claude_bg(
    repo: str, *, name: str | None = None, model: str | None = None,
    prompt: str | None = None, allowed_tools: str | None = None,
) -> None:
    """`claude --bg` in `repo` — the harness's own documented background-session surface.
    SAME fire-and-forget discipline as `_spawn_claude`/`_spawn_in_body` (B1's scar: an arq
    timeout that awaited a live billing `claude -p` once wedged the whole worker) — `--bg`
    itself returns almost immediately once the harness's background-agent daemon has taken
    the session over, so this call confirms only that the command was ISSUED, never that the
    session completed; poll `_claude_agents_json` for that, the same surface the operator's
    own front end reads.

    NO ENV-VAR IDENTITY CHANNEL (the default-flip's live correction, task #68 wave,
    2026-07-27): an earlier build of this primitive (commit 33b3984) set CLAUDE_JOB_DIR /
    OSIRIS_SEAT_ID / OSIRIS_ATTACH_TOKEN here and presented a deterministic
    `--session-id`, on the theory that a spawned child inherits this call's env like any
    other subprocess. A real spawn proved that wrong on BOTH counts: `--bg` claims a
    PRE-FORKED spare off a claim-socket (confirmed via `ps`/a real spare's own
    `/proc/<pid>/environ` — its env was fixed at fork time, long before this call, and NONE
    of those three vars ever reached it) and separately ignores an explicit `--session-id`
    outright ('warning: --bg manages the session id; ignoring --session-id' on stderr,
    which this fire-and-forget spawn never reads). Neither env vars nor a chosen session id
    are a working channel into a `--bg` session's identity.

    `prompt` IS a working channel — `claude [options] [prompt]` delivers a trailing
    positional prompt as the session's genuine first turn (confirmed live) — so
    launch_seat now hands the session its own boot instruction there (mount + claim_name,
    with cwd/job_dir as literal strings in the text, needing no env passthrough at all) and
    idempotency matches on the seat's own office cwd instead of a session id (see
    launch_seat's harness-lane comment).

    OWN PROCESS GROUP, ALWAYS (#156, Thoth's own ruling: 'ship it independently, it is
    already a bug today'): `start_new_session=True` makes the spawned body its own
    session/group leader (POSIX `setsid()`, `pgid == pid`) — the same discipline
    `pty_broker.py`'s own children already carry, this lane just never had it. Without it,
    a seat's Bash-tool children share OSIRIS'S OWN process group and can outlive their
    parent with nobody owning them — the exact shape of the two leaked spares found and
    killed by hand this afternoon (#156.5's own investigation; live proof, not a theory).
    THE NAMED LIMIT: this only isolates bodies spawned AFTER this lands — every session
    already alive when this code deploys was started without it and stays in the shared
    group until it naturally cycles. A future group-based kill must check for this rather
    than assume it; a kill verb that silently behaves differently for old and new bodies
    is worse than one that refuses cleanly on the old ones."""
    env = os.environ.copy()
    # same anchor discipline as _spawn_claude: a spawner's own anchor must never leak into
    # the child (the anchor-collision class, 2294e95d) — inert for --bg today (env vars
    # don't reach the claimed spare either way) but cheap and harmless to keep scrubbed.
    env.pop("CLAUDE_JOB_DIR", None)
    cmd = ["claude", "--bg"]
    if name:
        cmd += ["-n", name]
    if model:
        cmd += ["--model", model]
    if allowed_tools:
        cmd += ["--allowedTools", allowed_tools]
    if prompt:
        cmd.append(prompt)
    proc = await asyncio.create_subprocess_exec(
        *cmd, cwd=repo, env=env, start_new_session=True,
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
    _log.info("trigger: bg-spawned %s in %s (pid %s)", name or "(unnamed)", repo, proc.pid)


async def _claude_agents_json(
    *, cwd: str | None = None, include_completed: bool = False,
) -> list[dict[str, Any]]:
    """`claude agents --json` — the harness's own front-end view (clause 3: a body this
    cannot show is an orphan by definition). Fails open to `[]` on any error, the same
    discipline `_manager_windows` uses: a status read must never break a caller that only
    wants a roster."""
    cmd = ["claude", "agents", "--json"]
    if cwd:
        cmd += ["--cwd", cwd]
    if include_completed:
        cmd.append("--all")
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        out, _ = await proc.communicate()
    except OSError:
        return []
    try:
        rows = json.loads(out.decode() or "[]")
    except ValueError:
        return []
    return [r for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []


async def _bg_session_cost(
    session_id: str, *, cwd: str | None = None,
) -> dict[str, Any]:
    """The spike's own open question, answered honestly: `claude agents --json` carries no
    cost/usage field for any session — confirmed live, 2026-07-27, across busy/idle/done
    states alike. A `--bg`-spawned session's spend is therefore structurally UNPRICED from
    this surface, same doctrine as the subscription-lane blind spot (osiris cannot recover
    the marginal joule from outside the vendor's own dashboard). Never fabricates a number:
    reports `{'priced': False, ...}` rather than a guess — if a future harness version adds
    a cost field, this starts reporting it (`cost_usd`/`total_cost_usd`, checked first)."""
    for row in await _claude_agents_json(cwd=cwd, include_completed=True):
        if row.get("sessionId") == session_id or row.get("id") == session_id[:8]:
            cost = row.get("cost_usd") or row.get("total_cost_usd")
            if cost is not None:
                return {"priced": True, "cost_usd": cost}
            return {"priced": False,
                    "reason": "claude agents --json carries no cost field for this session",
                    "session_row": row}
    return {"priced": False, "reason": "session not found in claude agents --json"}


async def dispatch_broadcast(
    pool: asyncpg.Pool, *, project: str, msg_id: int, sender: str | None,
    settings: Settings | None = None, spawn: Any = None, windows: Any = None,
    poke: Any = None, hourly_wakes: int | None = None,
) -> dict[str, str]:
    """Dispatch ONE broadcast to its project — dispatch_dm's own grammar, finally applied to
    the surface it was missing from (task #151; ruling 60bc15db, the mail layer's own
    specimen). Before this, send(to=<project>) filed a message and computed wake_status (a
    STATUS STRING, no dispatch) — the only push a broadcast ever got was the worker tick's
    sweep, up to ~60s later, and under the standing osiris_trigger_poke_only arm a project
    with no open manager window got NONE, ever. That is how Thoth LXXII's commit-freeze
    broadcast reached nobody the night of the second history rewrite (Imhotep, msg 3707:
    "sent individual DMs since the freeze broadcast never reached anyone") — four agents
    held live worktrees and the channel built for reaching all of them at once was silent.

    Shared verbatim by send()'s immediate leg and the worker tick's backstop sweep — two
    callers, one law, no drift (dispatch_dm's own principle: "(The DM lane already knew
    this... The broadcast lane simply never learned it.)"). Returns the per-hop RECEIPT
    {mode, detail}:

      queued-fyi      — grade='fyi' never wakes: the DM lane's own loop terminator (line
                        ~1204 above), extended here rather than re-invented. CONFIRMED, not
                        assumed, that grade previously had zero effect on broadcast dispatch
                        before this — it only ever reached mount()/orient()'s unread count.
      delivered       — the project's own owner is live right now; reads its own box
      poked           — typed into a manager-hosted OPEN window holding this project
      window-busy     — that window is mid-turn; the mail waits, nothing spent
      resumed         — an owner's own session was continued with the mail as its next turn
      woke            — no live/resumable owner; a fresh session was minted
      poke-only-held  — the ladder ends at poke (osiris_trigger_poke_only=1, the operator's
                        standing arm): no resume, no mint, ever. Held mail stays pull-only.
      skipped-*       — the fleet's own brakes (rate-capped / budget-* / wake-grace)
      abandoned       — failed its lifetime attempt limit; escalated to the human, the
                        trigger stops trying rather than loop forever
      no-repo         — no known repo to spawn into; stays pull-only
      trigger-dark / scoped-out — the kill switch, or a scoped re-arm naming other projects

    `hourly_wakes` lets the sweep pass its OWN running total (hourly DB count + wakes already
    fired earlier in the SAME tick) so a burst of messages in one tick can't each read a
    stale pre-burst count and collectively blow past the hourly budget — the exact correction
    the sweep's inline loop already made before this was extracted. `None` (send()'s
    immediate leg, a single isolated call) reads a fresh count.

    THE RACE THIS FUNCTION MUST NOT LOSE: with two callers, a message arriving at the same
    moment the sweep is mid-tick could be dispatched by both. The poke step trusts `poke`'s
    own dedup (`dedup=f"msg:{msg_id}"`, the same guarantee the old sweep-only code already
    relied on); resume/mint claim the ledger row under an advisory transaction lock BEFORE
    spawning, exactly as dispatch_dm's own resume arm does — the second caller finds the row
    already there and stops rather than double-spawning."""
    st = settings or get_settings()
    spawn = spawn or _spawn_claude
    windows = windows or _manager_windows
    poke = poke or _poke_window
    mrow = await pool.fetchrow(
        "SELECT grade, extract(epoch FROM (now() - created_at)) AS age_secs "
        "FROM fleet_messages WHERE id=$1", msg_id)
    grade = mrow["grade"] if mrow else None
    age = float(mrow["age_secs"] or 0) if mrow else 0.0
    if grade == "fyi":
        return {"mode": "queued-fyi",
                "detail": "an fyi broadcast never wakes — it settles at each reader's own "
                          "next turn (the DM lane's own loop terminator, extended here)"}
    if not st.osiris_trigger_enabled:
        return {"mode": "trigger-dark",
                "detail": "the trigger is dark (osiris_trigger_enabled=0)"}
    allow = {p.strip() for p in st.osiris_trigger_projects.split(",") if p.strip()}
    if allow and project not in allow:
        return {"mode": "scoped-out",
                "detail": f"this re-arm names only: {', '.join(sorted(allow))}"}
    if await _owner_live(pool, project, st.osiris_owner_live_secs):
        return {"mode": "delivered",
                "detail": "the project's own owner is live right now — reads its own box"}
    urgent = (sender or "").startswith("operator") or age >= _URGENT_AGE_SECS
    hourly = hourly_wakes if hourly_wakes is not None else int(await pool.fetchval(
        "SELECT count(*) FROM agent_wakes WHERE woke_at > now() - interval '1 hour'") or 0)
    attempts = await _attempts_on(pool, msg_id)
    reason = should_wake(
        enabled=True,
        recent_wakes=await _recent_wakes(pool, project, st.osiris_trigger_window_secs),
        rate_cap=st.osiris_trigger_rate_cap,
        within_grace=await _woken_within(pool, project, st.osiris_trigger_grace_secs),
        hourly_wakes=hourly, hourly_budget=st.osiris_wake_hourly_budget,
        urgent=urgent, attempts=attempts, attempt_limit=st.osiris_wake_message_attempts)
    if reason == "unsettleable":
        await _abandon(pool, project, msg_id, sender, attempts)
        return {"mode": "abandoned",
                "detail": f"failed its lifetime attempt limit ({attempts}) — escalated to "
                          "the human; the trigger stops trying rather than loop forever"}
    if reason is not None:
        return {"mode": "skipped-" + reason,
                "detail": f"the fleet's own brakes held it ({reason}) — the backstop sweep "
                          "runs every ~60s and retries once it clears"}
    wins = await windows()
    if wins:
        sids = {Path(r["job_dir"]).name[:8] for r in await pool.fetch(
            "SELECT job_dir FROM agent_mounts WHERE project=$1 AND job_dir IS NOT NULL",
            project)}
        wname = _window_for(wins, sids)
        if wname is not None:
            res = await poke(wname, _POKE_PROMPT, dedup=f"msg:{msg_id}",
                             min_idle=st.osiris_poke_min_idle_secs)
            if res.get("poked") and not res.get("deduped"):
                await pool.execute(
                    "INSERT INTO agent_wakes (to_project, from_agent, message_id, mode) "
                    "VALUES ($1,$2,$3,'poke')", project, sender, msg_id)
                return {"mode": "poked",
                        "detail": f"typed into the open window {wname} as its next turn"}
            if res.get("busy"):
                return {"mode": "window-busy",
                        "detail": "the window is streaming a turn — mail waits, nothing "
                                  "spent"}
            if res.get("deduped") and await _last_wake_mode(pool, project, msg_id) != "poke":
                await pool.execute(
                    "INSERT INTO agent_wakes (to_project, from_agent, message_id, mode) "
                    "VALUES ($1,$2,$3,'poke')", project, sender, msg_id)
                return {"mode": "poked",
                        "detail": "already poked (ledger healed after a lost record)"}
    if st.osiris_trigger_poke_only:
        return {"mode": "poke-only-held",
                "detail": "the ladder ends at poke (osiris_trigger_poke_only=1, the "
                          "operator's standing arm) — no resume, no mint, ever; held mail "
                          "stays pull-only"}
    # THE LOCK IS FOR THE RACE, NOT FOR MEMORY: unlike a DM (one resume attempt EVER — a
    # resume that didn't settle it is not looped), a broadcast RETRIES across ticks up to
    # `attempt_limit`, already enforced above via _attempts_on/should_wake. This lock only
    # keeps two near-simultaneous dispatchers (send()'s immediate leg + a sweep tick landing
    # in the same instant) from both reading "not yet resumed this tick" and both spawning —
    # it must never become a second, broader "already woke, ever" gate, or attempt_limit's
    # own retry ladder breaks (measured live: an earlier draft of this check did exactly
    # that and silently capped every retry at one, killing the abandon-after-N-attempts path
    # the ghost-farm fix depends on).
    async with pool.acquire() as conn, conn.transaction():
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended('osiris-bc-' || $1, 7445))",
            str(msg_id))
        resume = None
        mint_repo_path: str | None = None
        if await _last_wake_mode(pool, project, msg_id) != "resume":
            resume = await _resumable_owner(pool, project, st)
        if resume is not None:
            await conn.execute(
                "INSERT INTO agent_wakes (to_project, from_agent, message_id, mode) "
                "VALUES ($1,$2,$3,'resume')", project, sender, msg_id)
        else:
            mint_repo_path = await _repo_path(pool, project)
            if mint_repo_path is None:
                return {"mode": "no-repo",
                        "detail": "no known repo to spawn into — stays pull-only"}
            await conn.execute(
                "INSERT INTO agent_wakes (to_project, from_agent, message_id, mode) "
                "VALUES ($1,$2,$3,'mint')", project, sender, msg_id)
    if resume is not None:
        session_id, repo = resume[0], resume[1]
        await spawn(repo, _RESUME_PROMPT, resume_session=session_id,
                    model=st.osiris_wake_model or None,
                    allowed_tools=st.osiris_wake_allowed_tools or None)
        return {"mode": "resumed",
                "detail": f"continued the owner's own session ({session_id[:8]})"}
    assert mint_repo_path is not None  # the no-repo branch above already returned otherwise
    repo_path = mint_repo_path
    wake_anchor = _wake_job_dir(project)
    await spawn(repo_path, _WAKE_PROMPT.format(repo=repo_path, job_dir=wake_anchor),
                job_dir=wake_anchor, model=st.osiris_wake_model or None,
                allowed_tools=st.osiris_wake_allowed_tools or None,
                spawn_parent=await _room_parent(pool, project))
    return {"mode": "woke", "detail": f"minted a fresh session in {repo_path}"}


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
              "owner_live": 0, "abandoned": 0, "scoped_out": 0, "poke_only_held": 0,
              "fyi_queued": 0}

    # THE CEILING — and this is the producer it was built for. A wake is not a token, it is an
    # entire Claude session with tools, in a repo, on the operator's card. 463 of them were minted
    # on projects he had not opened in days, and NOT ONE was ever in the ledger, because the
    # spawner throws the vendor's own receipt at /dev/null (21a99136). Every other guard here is a
    # RATE (wakes per hour, attempts per message) — and a rate is not a bound: the storm ran for
    # days at a perfectly legal 5/hr. THIS is the bound.
    ok, why = await may_spend(pool, cap=st.osiris_daily_usd, metered=spend_is_metered(st))
    if not ok:
        report["refused"] = 1
        report["why"] = why  # type: ignore[assignment]
        _log.warning("the trigger is refusing to spend: %s", why)
        return report

    # the fleet-wide hourly spend — the SAME number the chrome renders as 'wakes N/h'
    hourly = await pool.fetchval(
        "SELECT count(*) FROM agent_wakes WHERE woke_at > now() - interval '1 hour'")
    # None defaults resolve LATE (module attribute, not a bound default) so tests can
    # darken the manager for the whole module with one monkeypatch.
    windows = windows or _manager_windows
    poke = poke or _poke_window
    # THE BROADCAST LANE (task #151, dispatch_broadcast's own docstring has the full ladder
    # and the incident that demanded it): this loop is now the BACKSTOP sweep, the exact
    # role the DM lane below already had — send()'s immediate leg dispatches on arrival
    # through the very same dispatch_broadcast, this drains what arrived gated or ran before
    # the trigger was armed. Two callers, one law, no drift.
    for project, msg_id, sender, _age in await _projects_with_unread(
            pool, st.osiris_mail_lease_secs):
        if not st.osiris_trigger_enabled:
            report["skipped"] += 1
            continue
        d = await dispatch_broadcast(
            pool, project=project, msg_id=msg_id, sender=sender, settings=st,
            spawn=spawn, windows=windows, poke=poke,
            hourly_wakes=int(hourly or 0) + report["woke"])
        mode = d["mode"]
        if mode == "queued-fyi":
            report["fyi_queued"] += 1
        elif mode == "delivered":
            report["owner_live"] += 1
        elif mode == "poked":
            report["poked"] += 1
            report["woke"] += 1
        elif mode == "window-busy":
            report["window_busy"] += 1
        elif mode == "resumed":
            report["resumed"] += 1
            report["woke"] += 1
        elif mode == "woke":
            report["woke"] += 1
        elif mode == "poke-only-held":
            report["poke_only_held"] += 1
        elif mode == "abandoned":
            report["abandoned"] += 1
        elif mode == "scoped-out":
            report["scoped_out"] += 1
        else:  # trigger-dark / skipped-* / no-repo / skipped-once-per-message
            report["skipped"] += 1

    # THE DM LANE — the background-session adapter's BACKSTOP sweep (ruling 6c4d0b62):
    # send() dispatches each DM on arrival through the very same dispatch_dm; this loop
    # exists to DRAIN THE QUEUES — mail that arrived gated (paused / needs-input / braked /
    # mid-turn) retries here once the gate lifts. Two callers, one grammar, no drift. No
    # mint, ever — a private message is never handed to a stranger; an unresumable
    # addressee's DM stays pull-only and follows the seat at the next mint (the estate).
    for agent_id, msg_id, sender in await _dms_with_unread(pool, st.osiris_mail_lease_secs):
        if not st.osiris_trigger_enabled:
            report["skipped"] += 1
            continue
        d = await dispatch_dm(pool, addressee=agent_id, msg_id=msg_id, sender=sender,
                              settings=st, spawn=spawn, windows=windows, poke=poke)
        mode = d["mode"]
        if mode == "resumed":
            report["resumed"] += 1
            report["woke"] += 1
        elif mode == "nudged":
            report["nudged"] = report.get("nudged", 0) + 1
            report["woke"] += 1
        elif mode == "poked":
            report["poked"] += 1
            report["woke"] += 1
        elif mode == "delivered":
            report["owner_live"] += 1
        elif mode == "window-busy":
            report["window_busy"] += 1
        elif mode.startswith("queued"):
            report["dm_queued"] = report.get("dm_queued", 0) + 1
        elif mode == "held":
            report["poke_only_held"] += 1  # the resume arm is dark: held, same book
        else:  # skipped-*, braked, pull-only, refused
            report["skipped"] += 1
    return report
