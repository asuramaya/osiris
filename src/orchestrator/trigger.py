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
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.config.settings import Settings, get_settings
from src.ingest.providers import spend_is_metered
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
) -> tuple[str, str, float, str] | None:
    """The disk half of resume-resolution (sync — called via to_thread): for each candidate
    (job_dir, cwd), anchor its transcript and check the context ceiling. Returns
    (full_session_id, cwd, transcript_mtime, job_dir) for the first resumable owner. The
    transcript stem IS the session id `claude --resume` takes; a transcript at the ceiling
    is retirement-by-compaction territory — resuming it would replay a sibling project's
    21:30 case, which was LEGITIMATE succession. The mtime rides along as the ONE honest
    mid-turn signal: a turn writes the transcript; nothing else does (the statusline-
    heartbeat superstition, killed 2026-07-20 — see dispatch_dm). `job_dir` (thread
    25943031) rides along too, additive — see _agent_resumable's own docstring."""
    for job_dir, cwd in cands:
        t = locate_current_transcript(root, job_dir, anchored_only=True)
        if t is None:
            continue
        try:
            st = t.stat()
            if st.st_size > ceiling_bytes:
                continue
        except OSError:
            continue
        return t.stem, cwd, st.st_mtime, job_dir
    return None


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
        _pick_resumable_sync, cands, root, st.osiris_resume_ceiling_bytes)


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
    others = await pool.fetch(
        "SELECT cwd FROM agent_mounts WHERE job_dir != $1", row["job_dir"])
    if any(_harness_slug(r["cwd"]) == slug or _legacy_slug(r["cwd"]) == slug for r in others):
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


async def _resident_disagrees(
    pool: asyncpg.Pool, root: Path, sid: str, base: str, *,
    job_dir_hint: str = "", seat_id: str | None = None,
) -> bool:
    """True when the session's own signed testimony names a DIFFERENT lineage than the
    addressee — the crossed-registry class (thread 0100a35e, the Ra misdelivery): the
    registry said the addressee lived there; the transcript says someone else does. Both
    the nudge AND the resume must refuse on this — each would put the addressee's mail
    into a foreign window.

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
    registry doesn't corroborate: refuse, exactly as before this fix."""
    from src.orchestrator.agents import _generation
    resident = await asyncio.to_thread(_resident_of_sync, root, sid)
    if resident is not None:
        return _generation(resident)[0] != base
    deep_resident, transcript = await asyncio.to_thread(_resident_of_deeper_sync, root, sid)
    if deep_resident is None or transcript is None:
        return True
    if _generation(deep_resident)[0] != base:
        return True
    if not job_dir_hint:
        return True  # no candidate to corroborate against — refuse, never guess one
    return not await _registry_corroborates(
        pool, job_dir_hint, transcript, base, seat_id=seat_id)


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


async def dispatch_dm(
    pool: asyncpg.Pool, *, addressee: str, msg_id: int, sender: str | None,
    settings: Settings | None = None, spawn: Any = None, windows: Any = None,
    poke: Any = None, jobs: Any = None, nudge: Any = None,
) -> dict[str, str]:
    """Dispatch ONE DM — the adapter's whole grammar in one function, shared verbatim by
    send()'s immediate leg and the worker tick's backstop sweep (two callers, one law: the
    lanes must never drift). Returns the per-hop RECEIPT {mode, detail}:

      nudged             — the mail envelope was injected into the addressee's live
                           backgrounded session via the HARNESS DAEMON (the visible hop:
                           the operator's front renders daemon-owned turns natively —
                           thread 4261a0d8, the ghost problem's fix)
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
    windows = windows or _manager_windows
    poke = poke or _poke_window
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
        return {"mode": "pull-only", "detail": "the trigger is dark (osiris_trigger_enabled=0)"}
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
    from src.orchestrator.folds import canonical_agent, living_head, wakeable_identity
    from src.orchestrator.seats import held_seat, seat_receipt
    target = addressee
    seat_id: str | None = None
    if target.startswith("seat:"):
        seat_id = target
        sr = await seat_receipt(pool, target)
        holder = (sr or {}).get("holder")
        if not holder:
            return {"mode": "pull-only",
                    "detail": f"{target} is vacant — the mail waits for its next holder"}
        target = str(holder)
    target = await living_head(pool, await canonical_agent(pool, target))
    if seat_id is None:
        seat_id = ((await held_seat(pool, target)) or {}).get("seat_id")
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
        return {"mode": "pull-only",
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
        return {"mode": "pull-only",
                "detail": f"{target} has never mounted — no session to resume"}
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
            shown = job.get("name") or job.get("short") or "the job"
            return {"mode": "nudged",
                    "detail": f"the harness daemon ACCEPTED the mail envelope as {shown}'s "
                              "next turn (typically lands within a second or two, per "
                              "measurement — visible live in the agents view once it does)"}
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
    if resume is None:
        who = target if wake_target == target else (
            f"{target} (its own live mount, {wake_target}, checked too)")
        return {"mode": "pull-only",
                "detail": f"{who} has no resumable session (no anchored transcript, or "
                          "at the context ceiling) — a private message is never handed to "
                          "a fresh twin"}
    session_id, repo = resume[0], resume[1]
    if await _resident_disagrees(pool, root, session_id, base,
                                 job_dir_hint=resume[3], seat_id=seat_id):
        return {"mode": "pull-only",
                "detail": "the registry's door for this addressee leads to a session whose "
                          "own signed testimony names a different mind (the crossed-registry "
                          "class, 0100a35e) — refusing both nudge and resume; the mail "
                          "stays pull-only until the identity is healed"}
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


# dispatch_dm's mode → wake()'s honest vocabulary. Anything not named here (queued-*, braked,
# skipped-*, settled, held, window-busy) is a rate brake, a pause, or an in-flight wake already
# covering it — all genuinely "queued", and `detail`/`raw_mode` carry the specific reason so
# nothing is lost to the bucket.
_WAKE_STATUS = {
    "nudged": "delivered", "resumed": "delivered", "poked": "delivered",
    "delivered": "mid-turn",  # dispatch_dm's own word for this means mid-turn, never delivered
    "pull-only": "no-live-body",
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


async def launch_seat(
    actions: Actions, *, caller: str, target: str, message: str = "",
    model: str | None = None, settings: Settings | None = None,
    manager: Any = None, windows: Any = None, substrate: str | None = None,
    spawn: Any = None, agents_json: Any = None, cost_reader: Any = None,
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
    lane's — either way, without a live daemon or a live `claude` binary."""
    pool = actions.pool
    from src.orchestrator.agents import house_of
    from src.orchestrator.seats import held_seat
    manager = manager or _manager_control
    windows = windows or _manager_windows
    spawn = spawn or _spawn_claude_bg
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
        try:
            roster = await agents_json(cwd=launch_cwd)
        except (OSError, TimeoutError, ValueError):
            roster = []
        live = next((r for r in roster
                    if isinstance(r, dict) and r.get("cwd") == launch_cwd), None)
        if live is not None:
            return {"status": "already-live", "window": live.get("name"), "seat": target_seat,
                    "body_exists": True, "can_receive": True, "attach": attach,
                    "detail": f"a live body already holds {handle} — not minting a twin"}

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
        dormant = dormant_history_confession(office, *([tree_cwd] if tree_cwd else []))

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
        out = {
            "status": "launched", "window": name, "seat": target_seat,
            "body_exists": True, "can_receive": alive_row is not None,
            "spawned_model": argv_model,
            "detail": ("body created and live" if alive_row is not None else
                       "body created; mount NOT yet confirmed — the claude is booting and "
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
    launch_seat's harness-lane comment)."""
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
        *cmd, cwd=repo, env=env,
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
    ok, why = await may_spend(pool, cap=st.osiris_daily_usd, metered=spend_is_metered(st))
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
            session_id, repo = resume[0], resume[1]
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
