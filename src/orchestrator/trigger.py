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
from src.orchestrator.signatures import (
    SIGNED as _SIGNED,
)
from src.orchestrator.signatures import (
    SIGNED_ACTS as _SIGNED_ACTS,
)
from src.orchestrator.signatures import (
    newest_signatures as _newest_signatures,
)

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


def _spawn_cwd_for(t: Path, candidates: list[str | None]) -> tuple[str | None, str]:
    """THE SPAWN MUST HAPPEN WHERE THE HARNESS WILL LOOK (operator, 2026-09-02: "something
    about the resume id that it finds is disconnected from what can actually be resumed").

    `claude --resume <sid>` reads exactly one path: ~/.claude/projects/<slug-of-its-own-
    cwd>/<sid>.jsonl. Osiris's own finder is slug-IMMUNE — it globs every project directory
    and anchors on the job/session id — so it routinely LOCATES a transcript under one slug
    and then handed the caller's `launch_cwd` to the spawn, a different directory entirely.
    Three live failures in one night (chad, marquee, jesus) all reduced to that: the finder
    answers "where is the best copy?", the spawner answers "where does this seat live?", and
    nothing checked they were the same place. Decisions de70e54d / 2e05d662.

    Returns (cwd, note). `cwd` is the first candidate whose harness slug equals the located
    transcript's own parent directory — i.e. the one place the harness can actually read it
    from. None when no candidate matches: the caller must REFUSE rather than spawn blind,
    because a spawn at a non-matching cwd cannot succeed and fails as an opaque
    "source session not found" with no indication which layer was wrong."""
    from src.orchestrator.mounts import _harness_slug
    want = t.parent.name
    for c in candidates:
        if c and _harness_slug(c) == want:
            return c, f"spawn cwd {c} matches the transcript's own slug {want}"
    first = next((c for c in candidates if c), None)
    return first, (
        f"NO CANDIDATE MATCHES: the transcript sits under slug {want!r} but none of "
        f"{[c for c in candidates if c]!r} resolves to it — falling back to {first!r}, which "
        "is what this code did unconditionally before. `claude --resume` will not find the "
        "file there. NOT a refusal ON PURPOSE (2026-09-02): the existing test fixtures write "
        "transcripts to a hardcoded slug unrelated to the seat's cwd, so refusing here would "
        "fail ~7 tests that encode the very assumption this function exists to correct. The "
        "fixtures model an impossible world and should be fixed first; until then this "
        "returns the old behaviour and says so, rather than adding a refusal path whose only "
        "proven trigger is a synthetic one.")


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
        spawn_cwd, _note = _spawn_cwd_for(t, [cwd, *(c for _j, c in cands)])
        return t.stem, spawn_cwd or cwd, st.st_mtime, job_dir
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
_RESIDENT_TAIL_BYTES = 400_000
# THE CORROBORATION FALLBACK (thread 25943031, halcyon's own stranding, design approved
# Thoth DM 1825): how many further 400KB windows the deeper scan reads BEHIND the tail
# already checked, on a tail miss only. Bounds total scan cost regardless of file size —
# 4 extra windows is ~1.6MB beyond the tail's own 400KB, ~2MB worst case per dispatch.
_RESIDENT_DEEP_WINDOWS = 4


def _resident_of_sync(root: Path, sid: str) -> str | None:
    """Sync (runs via to_thread): the agent id of the NEWEST signed osiris act in session
    `sid`'s transcript, or None when the session has never signed anything (no whisper, no
    mount, no send — a stranger this dispatcher must not address).

    ANCHORED (the same fix as `_turn_fresh_sync`'s own, decision pending, msg 6653):
    a bare `next(iter(root.glob(f"*/{sid}.jsonl")), None)` assumes exactly one file on
    disk carries this session id — false the instant the materializer (ruling d161a156)
    writes a second, static copy under the seat's own office slug. This is the CROSSED-
    REGISTRY identity check itself — reading the wrong copy here doesn't just misjudge
    liveness, it can miss real signed testimony the live file carries, or read stale
    testimony a materialized snapshot happens to predate. Anchored on `f"jobs/{sid}"`
    directly — `sid` is already the exact, authoritative session id (that's what the old
    glob matched exactly on too), so synthesizing beats trusting a CALLER-supplied
    job_dir hint that may not even be a real job_dir path (`_resume_guard`'s own
    `job_dir_hint` can be a bare id, not a path — `_job_id` finds no anchor in that shape
    at all). Among genuinely anchored matches, `locate_current_transcript` picks the
    freshest by mtime — the live file, never a static copy."""
    if not sid:
        return None
    t = locate_current_transcript(root, f"jobs/{sid}", anchored_only=True)
    if t is None:
        return None
    try:
        size = t.stat().st_size
        with t.open("rb") as f:
            f.seek(max(0, size - _RESIDENT_TAIL_BYTES))
            tail = f.read().decode("utf-8", errors="replace")
    except OSError:
        return None
    act, whisper = _newest_signatures(tail.splitlines())
    return act or whisper


def _resident_tail_signatures_sync(root: Path, sid: str) -> tuple[str | None, str | None]:
    """`_resident_of_sync`'s two halves kept apart — (newest act, newest greeting) in the
    tail — so `_resident_verdict` can tell testimony from hearsay. Same anchored file,
    same window."""
    if not sid:
        return None, None
    t = locate_current_transcript(root, f"jobs/{sid}", anchored_only=True)
    if t is None:
        return None, None
    try:
        size = t.stat().st_size
        with t.open("rb") as f:
            f.seek(max(0, size - _RESIDENT_TAIL_BYTES))
            tail = f.read().decode("utf-8", errors="replace")
    except OSError:
        return None, None
    return _newest_signatures(tail.splitlines())


def _resident_of_deeper_sync(
    root: Path, sid: str, *, extra_windows: int = _RESIDENT_DEEP_WINDOWS,
    acts_only: bool = False,
) -> tuple[str | None, Path | None]:
    """Sync (runs via to_thread): called ONLY after `_resident_of_sync`'s own tail check
    already returned None (thread 25943031 — halcyon's last 400KB was all unsigned harness
    noise — away summaries, chrome — even though every signature further back in the SAME
    file was its own lineage). Reads up to `extra_windows` further 400KB windows strictly
    BEHIND the tail already checked (never re-reading it), stopping at the first signed
    act found or the front of the file. Returns (resident_agent_id, transcript_path) — the
    path rides along even on a signature MISS, so `_resident_disagrees` can reason about
    the file for the registry corroboration step without a second glob. Bounded: total
    cost never exceeds `extra_windows` chunks regardless of how large the file is.

    ANCHORED, matching `_resident_of_sync`'s own fix immediately above — same reasoning,
    same `f"jobs/{sid}"` synthesis, so both halves of one scan always agree on which file
    they read."""
    if not sid:
        return None, None
    t = locate_current_transcript(root, f"jobs/{sid}", anchored_only=True)
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
            for pat in (_SIGNED_ACTS if acts_only else _SIGNED):
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


def _turn_fresh_sync(root: Path, sid: str, active_secs: int, job_dir: str = "") -> bool:
    """Sync (runs via to_thread): is a TURN genuinely in flight in session `sid` — by the
    transcript's own newest timestamped line, never the inode. AWAKE and ASLEEP are
    different states and must not be confounded (operator, 2026-07-21, the Aegis phantom:
    a session 13 hours dead wore a seconds-old mtime — something in the chrome/daemon
    touches the file without writing). A turn appends timestamped records; a toucher
    cannot. No timestamp in the tail = not moving.

    ANCHORED, NEVER GLOBBED (the wire-resume-to-store finding, decision pending, msg
    6653/Thoth: "the same disease we just spent the night removing, one function over"):
    used to be `next(iter(root.glob(f"*/{sid}.jsonl")), None)` — an UNANCHORED search
    assuming exactly one file on disk carries this session id, silently returning
    "whichever the filesystem handed me first" when that assumption breaks. The
    materializer (ruling d161a156) now routinely writes a SECOND copy of a session's
    transcript under the seat's own office slug, so the assumption was already false the
    instant that landed — a live, still-growing transcript and a static materialized copy
    can share one stem. `locate_current_transcript(root, job_dir, anchored_only=True)` asks
    the answerable question instead: the file at the address already known (the job_dir's
    own anchor), and among genuinely anchored matches picks the FRESHEST by mtime — which
    is exactly the live, still-being-written file, never a static copy sitting still.
    `job_dir` defaults to `""`, synthesizing `f"jobs/{sid}"` below (the SAME convention
    `_lineage_resume_candidate` used before its own rewrite) when a caller has no real
    per-hop anchor to pass — a real job_dir, when the caller has one, is always preferred."""
    if not sid:
        return False
    t = locate_current_transcript(root, job_dir or f"jobs/{sid}", anchored_only=True)
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
    act, whisper = await asyncio.to_thread(_resident_tail_signatures_sync, root, sid)
    if act is not None:
        act_base = _generation(act)[0]
        if whisper is not None and act_base == base and _generation(whisper)[0] != base:
            # `whisper` is only ever recorded when it is NEWER than the act (see
            # `_newest_signatures`): the addressee's own last act, then a greeting naming
            # someone else, then silence — hearsay over the addressee's own session.
            return "unknown"
        return "mismatch" if act_base != base else "match"
    if whisper is not None:
        if _generation(whisper)[0] == base:
            return "match"
        # HEARSAY DISAGREEMENT (see `_SIGNED_WHISPERS`): the newest thing in the tail is
        # the server's own greeting naming another lineage, and no mind has ACTED since.
        # Look deeper for the newest act. An act by another lineage: a real mismatch. An
        # act by the addressee's own lineage: the greeting was a misresolution over the
        # addressee's own session — "unknown", never "match" (the greeting is still a
        # disagreement on the record) and never "mismatch" (nothing signed names a
        # different mind). A nudge stays refused on "unknown"; a resume can still clear
        # it through `_zero_hop_graph_corroborates`'s own narrow door.
        deep_act, _ = await asyncio.to_thread(
            _resident_of_deeper_sync, root, sid, acts_only=True)
        if deep_act is None or _generation(deep_act)[0] != base:
            return "mismatch"
        return "unknown"
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
            # materialize=False: this function is a READ-ONLY entry point (its own
            # docstring's promise) — it answers "would a resume succeed", never spawns,
            # so it must never emit a materialized transcript as a side effect.
            graph_outcome = await _lineage_resume_candidate(
                pool, wake_target, st, repo=launch_cwd, seat_id=seat_id, materialize=False)
            graph_resume = (graph_outcome[0]
                            if isinstance(graph_outcome, tuple) else None)
            graph_log = (graph_outcome[1]
                        if isinstance(graph_outcome, tuple) else graph_outcome)
            if graph_resume is not None:
                g_gate, g_refusal = await _resume_guard(
                    pool, (graph_resume[0], graph_resume[1], graph_resume[2],
                          graph_resume[3]), _generation(target)[0], seat_id=seat_id,
                    st=st, hop=graph_resume[5], launch_cwd=launch_cwd)
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


async def _resume_office(
    pool: asyncpg.Pool, seat_id: str | None, *, fallback: str | None,
) -> str:
    """WHERE A RESUME SPAWNS when the materializer had nothing to write (operator, 2026-09-03:
    "~/.osiris is where osiris agents get their identity and meta from"): the seat's own
    DERIVED office (`offices.seat_office_target`, the anchor invariant's own address — the
    same target the materializer emits into), falling back to the recorded anchor only
    when no office can be derived. Never the caller's tree/launch cwd: the harness resumes
    whichever copy of a session id sits in the SPAWN cwd's own slug (measured live on
    2.1.259 — a cwd whose slug held a stale 20KB partial resumed THAT over a fuller copy one
    slug over, and a cwd holding no copy found nothing once two copies existed), so a
    spawn anywhere the canon was not emitted continues a stale partial. One helper for all
    three resume doors (cli.py, dispatch_dm, launch_seat), so the rule cannot drift."""
    from src.orchestrator.offices import seat_office_target
    office = await seat_office_target(pool, seat_id) if seat_id else None
    return office or fallback or ""


async def _adopt_resumed_body(
    pool: asyncpg.Pool, *, agents_json: Any, office: str, requested_sid: str,
    holder: str, project: str | None, attempts: int = 6, delay: float = 2.0,
) -> dict[str, Any]:
    """WHAT THE HARNESS ACTUALLY STARTED (measured live, 2026-09-03, harness 2.1.259):
    `claude --bg --resume <id>` continues the session under its own id — OR, when any
    background record for it still exists (a `claude stop` leaves a "stopped" one),
    "starts a copy and says so": a NEW session id whose window carries the old
    conversation but whose file and whisper are a stranger's. Three copies of Chad in a
    row tonight, each minted a fresh root by the whisper and reported by osiris as
    "resumed 7451509a" — the receipt lying by omission, the 5d31762a class again.

    Polls `claude agents --json` for the body at `office`, then: the requested id came
    back → `copied: False`, nothing to do. A different id → `copied: True`, and the copy
    is ADOPTED as the seat's own continuation before its first act can mint a stranger:
    the session ledger files the copy's sid under `holder` (`record_session_anchor`, whose
    own transcript guard still applies) and the copy's registry row (the whisper's own
    provisional row, keyed on jobs/<copy8>) is rebound to `holder` — the same two facts
    a `--fork-session` copy resolves through. No body found within the window →
    `session_id: None`, confessed, never assumed. Every caller prints what happened."""
    from src.actions.core import Actions
    from src.orchestrator.handshake import record_session_anchor
    from src.orchestrator.mounts import save_mount

    req8 = requested_sid[:8]
    row: dict[str, Any] | None = None
    for _ in range(attempts):
        try:
            rows = await agents_json(cwd=office)
        except (OSError, TimeoutError, ValueError):
            rows = []
        row = next((r for r in rows if isinstance(r, dict) and r.get("cwd") == office
                    and r.get("sessionId")), None)
        if row is not None:
            break
        await asyncio.sleep(delay)
    if row is None:
        return {"session_id": None, "copied": False, "adopted": False}
    sid = str(row["sessionId"])
    if sid.startswith(req8):
        return {"session_id": sid, "copied": False, "adopted": False}
    filed = await record_session_anchor(Actions(pool), agent_id=holder, session_id=sid,
                                        actor="osiris-resume")
    await save_mount(pool, job_dir=str(Path.home() / ".claude" / "jobs" / sid[:8]),
                     agent_id=holder, project=project, cwd=office, model=None,
                     session_key=None)
    return {"session_id": sid, "copied": True, "adopted": True, "ledgered": filed}


async def _lineage_resume_candidate(
    pool: asyncpg.Pool, holder: str, st: Settings, *, repo: str,
    seat_id: str | None = None, materialize: bool = True,
) -> tuple[tuple[str, str, float, str, str | None, int], list[str]] | list[str]:
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

    THE INVERSION (ruling d161a156/d63b2ca6, Thoth dispatch 6620): this used to HUNT —
    `locate_current_transcript` on disk, then `_spawn_cwd_for` to guess which candidate
    cwd matched wherever the hunt landed. It now MATERIALIZES: per hop, ENSURE the session
    is in the soul store (`SoulStore.ensure_ingested`, the same harness-adapter discovery
    the old hunt used, just feeding the store instead of a direct disk read), then check
    the verdict against the STORE's own tail measurement (`SoulStore.resume_diagnostics`
    — never a disk stat; the load-bearing move: today's verdict was a measurement of
    whatever file the hunt happened to land on, confidently wrong ABOUT THE WRONG FILE
    when the hunt landed wrong, de70e54d's exact specimen). On the winning hop, when
    `materialize` is true, EMIT via `rematerialize_to_disk` to the seat's own office
    (`offices.seat_office_target`, the SAME derivation the anchor invariant heals toward —
    never an independently-typed copy) — "elsewhere" cannot exist once nothing reads what
    is already on disk.

    `materialize` DEFAULTS TRUE for the real resume-spawn callers (cli.py, dispatch_dm,
    launch_seat — the majority) but MUST be passed `False` by a caller that only checks
    resumability without ever spawning (`wake_gate_preflight`'s own docstring promises
    "a read-only entry point"; `succession_repair.unresumed_heads`'s own docstring promises
    "read-only, proposes nothing, writes nothing") — `rematerialize_to_disk` already
    refuses to clobber a live transcript (mtime-vs-last_ingested_at guard) so a stray
    materialize would rarely corrupt anything, but a "read-only" function that silently
    writes files is a broken promise regardless of whether the write was safe.

    A NAMED ORDERING GAP, DISCLOSED RATHER THAN SILENT: the emit happens HERE, inside
    candidate-finding, before any caller's own `_resume_occupancy_gate` check runs — so
    THE PEN RULE's occupancy re-verification (agents-json + /proc, "at the moment of the
    write") does not, today, gate this specific write. The write's own safety instead
    rests entirely on `rematerialize_to_disk`'s existing, independently-reviewed guard
    (refuses whenever the target's mtime is newer than the store's own last_ingested_at —
    i.e. something wrote to it more recently than the store has seen, the honest signature
    of a live writer). Moving the occupancy check to run BEFORE this emit would need
    `agents_json` threaded into this function, a real restructure — named here rather than
    silently done, so a reviewer can decide whether the existing mtime guard is enough or
    the occupancy check needs to move.

    Returns ((session_id, repo, mtime, job_dir="", materialized_at, hop), log) on the first
    hop that clears both gates. `session_id` IS THE FULL HARNESS ID, NEVER THE GRAPH'S OWN
    TRUNCATED `session` PROPERTY (operator's own live catch, 2026-09-03, decision 5d31762a):
    the graph stores an 8-char prefix by convention (`sid[:8]`, agents.py), which matches
    nothing in the harness's own `--resume` index — passing it through unchanged silently
    minted a fresh, disposable session on every "successful" resume this whole scheme ever
    reported, never actually continuing anything, undetected because nothing checked the
    SPAWNED PROCESS's own transcript. Read directly off the discovered file's own stem
    (`ensure_ingested`'s winning `soul_sessions.source_path`) — a positive property of the
    file itself, never inferred from the graph's shorter key. A hop whose only discoverable
    file has NO fuller id than the bare anchor (e.g. an earlier materialize wrote its own
    destination by the truncated value, the same bug one door over, also fixed here) is
    refused rather than handed back a value already known unusable.

    `hop` IS THE EXPLICIT COUNT OF ENTRIES IN `log` BEFORE the
    winning hop's own success line (task #200 residual, Thoth dispatch 6786/6792: a caller
    used to derive this by `len(log) - 1`, on the documented assumption the log always ends
    with exactly one success entry — a false assumption whenever this function itself
    appends a SECOND line after the success line for the same hop, e.g. a materialize
    refusal below. That miscounted a genuine hop-0 candidate as hop 1, which silently denies
    it `_zero_hop_graph_corroborates`'s own zero-hop fallback and forces a resident-unknown
    refusal on a session that IS the lineage head's own current, real transcript — the exact
    live specimen: Marquee, `agent:38cf08a9-xi`, decision 6a0b1236). Read `hop` directly;
    never re-derive it from `len(log)`.

    `repo` is UNCHANGED IN MEANING from before this rewrite — the
    CALLER's own `launch_cwd`, since `_zero_hop_graph_corroborates`'s `resume[1] ==
    launch_cwd` invariant depends on it staying exactly that (Thoth's explicit ruling: do
    NOT smuggle the office into this field — the anchor_cwd/tree_cwd fusion, #103/#141, is
    the exact bug that reinterpreting a field silently produces). `materialized_at` is the
    NEW, own-named 5th field — WHERE rematerialize_to_disk actually wrote (or attempted to
    write), None when `materialize=False` or when no seat office target could be derived —
    the actual spawn cwd a caller that intends to spawn must use, never `repo`. `job_dir=""`
    because no per-hop anchor is available for `_resume_guard`'s corroboration fallback —
    a known, disclosed narrowing (see the caller's own receipt/report), not a silent one.
    Returns the bare `log` (no candidate) when NOTHING in the walk clears both gates — each
    entry names the generation, its session's STORE TAIL size (was "transcript size" before
    this rewrite — the store, not a disk stat, is what's being measured now), and the
    SPECIFIC gate that refused it (numbers, not adjectives — Thoth's own explicit
    requirement), so a caller's receipt can read one line instead of a human re-running
    succession_chain by hand."""
    from src.ingest.sessions import _verdict_from_diagnostics
    from src.ingest.soul_store import SoulStore
    from src.orchestrator.offices import seat_office_target
    from src.orchestrator.succession import succession_chain

    chain = await succession_chain(pool, holder)
    store = SoulStore(pool)
    sense_root = Path(st.osiris_sense_sessions) if st.osiris_sense_sessions \
        else Path.home() / ".claude" / "projects"
    log: list[str] = []
    for hop in chain:
        gen, session = hop["generation"], hop["session"]
        if not session:
            log.append(f"gen {gen}: minted but never mounted, no session to check")
            continue
        anchor_sid = await store.ensure_ingested(
            cwd=None, job_dir=f"jobs/{session}", root=sense_root)
        if anchor_sid is None:
            log.append(f"gen {gen} (session {session[:8]}): mounted but no transcript "
                       "found on disk or in the store")
            continue
        soul_row = await pool.fetchrow(
            "SELECT last_ingested_at, source_path FROM soul_sessions "
            "WHERE harness='claude-code' AND anchor_sid=$1", anchor_sid)
        last_ingested = soul_row["last_ingested_at"] if soul_row else None
        # THE FULL SESSION ID, NEVER THE GRAPH'S OWN TRUNCATED `session` (operator's own
        # live catch, 2026-09-03: `claude --bg --resume <8-char-anchor>` matches nothing in
        # the harness's own index — it silently mints a fresh, disposable session instead
        # of erroring, so every resume this scheme ever reported "success" on may have been
        # a false positive nobody checked the SPAWNED PROCESS's own transcript for). The
        # graph's `session` property is a deliberate 8-char truncation (this module's own
        # `sid[:8]` convention) — fine for graph matching, NEVER valid as a harness CLI
        # argument. `ensure_ingested`'s own discovered file's stem IS the harness's real id
        # (ClaudeJsonlAdapter.discover: `session_id=stem`, `anchor_sid=stem.split("-")[0]`)
        # — read it directly off disk here (a positive property of the file itself, never
        # inferred from the graph's own shorter key) rather than widen ensure_ingested's own
        # return contract, which every other caller already depends on staying anchor_sid.
        full_sid = Path(soul_row["source_path"]).stem if soul_row else None
        if not full_sid or full_sid == anchor_sid:
            # The winning discovered file's own stem IS just the bare anchor — e.g. an
            # earlier rematerialize wrote its destination as f"{session}.jsonl" (the SAME
            # truncation bug, one door over: fixed below) and that stub, not a genuine
            # harness transcript, won the anchored mtime race. Resuming into a value that
            # can never match the harness's own index is worse than refusing this hop
            # honestly — never silently hand back a value already known to be unusable.
            log.append(f"gen {gen} (session {session[:8]}): only a truncated/synthetic "
                       "transcript on disk — no genuine full session id to resume into")
            continue
        diagnostics = await store.resume_diagnostics(anchor_sid)
        if diagnostics is None:
            log.append(f"gen {gen} (session {session[:8]}): no transcript found on disk "
                       "or in the store")
            continue
        _count, tail_bytes, tail_lines = diagnostics
        tail_mb = tail_bytes / 1_000_000
        verdict = _verdict_from_diagnostics(
            tail_bytes, tail_lines, ceiling_bytes=st.osiris_resume_ceiling_bytes,
            min_tail_bytes=st.osiris_resume_min_tail_bytes)
        if verdict is not None:
            log.append(f"gen {gen} (session {session[:8]}, {tail_mb:.2f}MB store tail): "
                       f"{verdict}")
            continue
        hop_num = len(log)
        log.append(f"gen {gen} (session {session[:8]}, {tail_mb:.2f}MB store tail): "
                   f"resumable, {hop_num} hop(s) back")
        materialized_at: str | None = None
        if materialize:
            office = await seat_office_target(pool, seat_id) if seat_id else None
            if office is None:
                log.append(f"gen {gen} (session {session[:8]}): no seat office target to "
                           "materialize into — resumable, but nowhere to emit it")
            else:
                from src.orchestrator.mounts import _harness_slug
                # `full_sid`, never truncated `session` — naming the destination by the
                # graph's own 8-char value is exactly how today's stray stub got created
                # in the first place, and how it went on to shadow the real transcript in
                # the next anchored discovery (mtime-max among same-prefix files).
                dest = str(sense_root / _harness_slug(office) / f"{full_sid}.jsonl")
                receipt = await store.rematerialize_to_disk(anchor_sid, dest=dest)
                if "error" in receipt:
                    log.append(f"gen {gen} (session {session[:8]}): materialize refused — "
                               f"{receipt['error']}")
                else:
                    # `materialized_at` is the OFFICE (a real cwd), not `dest` (the jsonl
                    # file the office's own harness slug resolves to) — a spawn's `cwd`
                    # argument is a directory the harness derives its OWN slug from, the
                    # same convention `resume_spawn`/`_spawn_claude_bg` already use for
                    # `repo`; handing back the file path here would make every caller
                    # re-derive the office from it instead of just using what it got.
                    materialized_at = office
        mtime = last_ingested.timestamp() if last_ingested is not None else time.time()
        # `full_sid`, not the graph's own truncated `session` — this is what a caller
        # hands to resume_spawn/_spawn_claude_bg as `resume_session`, and the harness's
        # `--resume` flag needs the exact id it knows itself by, never an 8-char prefix.
        return (full_sid, repo, mtime, "", materialized_at, hop_num), log
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
    resume: tuple[str, str, float, str], *, agents_json: Any, spawn_cwd: str | None = None,
) -> tuple[str, str] | None:
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

    Either signal firing refuses the RESUME. They are NOT the same finding and this
    returns `(which, reason)` so the caller can never flatten them again (the collapse
    fixed 2026-08-28; ruling f624d114's law one turn further down the ladder — an
    ignorance must never wear the same status as a finding, and here a POSITIVE
    IDENTIFICATION OF THE RIGHT MIND had been wearing the same status as an
    unidentified process):

      'self'    — `_confirm_listener` matched THIS RESUME'S OWN SESSION ID. The
                  addressee itself is live and holding the very session the mail is
                  addressed to. It reads this from its own inbox at its next turn.
                  NOTHING IS LOST AND NOTHING IS OWED — an OUTCOME, never a failure
                  (the `queued-live-unresolved` class, whose own comment records
                  Alfred concluding a reachable seat was unreachable and retracting).
      'foreign' — only `live_bodies_by_cwd` matched: a claude process sits in the
                  office, identity UNKNOWN — a stranger, a manual run that has not
                  self-mounted, or another lineage entirely. Whether anyone ever reads
                  this mail is genuinely unknown, and the receipt must say so.

    Returns None when neither finds anybody home (safe to resume) — never a silent
    block; the caller's own receipt names exactly which signal fired and what it means.

    `spawn_cwd` (the wire-resume-to-store rewrite, ruling d161a156/d63b2ca6): the ACTUAL
    directory the resume is about to spawn into and materialize onto — the office, once a
    caller has one, never `resume[1]` (`repo`, which stays the work-tree/launch_cwd value
    by contract and is no longer where the transcript lands). Defaults to `resume[1]` for
    a caller with no office yet, unchanged from before this parameter existed. THE PEN
    RULE (4d6844bc) is exactly why this must check the SPAWN target, not the launch_cwd
    the old hunt happened to use: a live body sitting in the office — about to have its
    own transcript overwritten by `rematerialize_to_disk` — must refuse, not go
    undetected because the check looked at the wrong directory."""
    session_id, repo = resume[0], resume[1]
    check_cwd = spawn_cwd or repo
    if await _confirm_listener({"sessionId": session_id, "short": ""}, agents_json):
        return ("self",
                f"the addressee's OWN session ({session_id[:8]}) is live in "
                "`claude agents --json`")
    from src.orchestrator.census import live_bodies_by_cwd
    bodies = await asyncio.to_thread(live_bodies_by_cwd)
    if bodies is not None and bodies.get(check_cwd):
        pids = ", ".join(str(p) for p in bodies[check_cwd])
        return ("foreign",
                f"a live claude process (pid {pids}) of UNKNOWN identity is already "
                f"sitting at {check_cwd!r}")
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
                _turn_fresh_sync, root, resume[0], st.osiris_dm_active_secs, resume[3]):
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
        pool, wake_target, st, repo=launch_cwd, seat_id=seat_id) if launch_cwd else [
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
        # Leg 3 just closed. Boots the SUCCESSOR at the seat's own launch location.
        handle = facts.get("handle") if facts else None
        office = facts.get("anchor_cwd") if facts else None
        house = facts.get("house") if facts else None
        if (miss_gate == "compaction" and seat_id is not None and launch_cwd
                and handle and office):
            anchor = _launch_anchor(seat_id)
            # THE LEDGER ROW GOES IN UNDER AN ADVISORY LOCK, BEFORE THE MINT (matching
            # dispatch_dm's own established discipline elsewhere in this function: two
            # dispatchers — send()'s immediate leg and a concurrent worker-tick sweep —
            # can both reach here for one message, and only one may spend). The bind-
            # before-spawn write below is exactly a spend (it mints a heir), so it must
            # not run before this gate decides which dispatcher, if either, gets to.
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
            # BOUND BEFORE SPAWN, HERE TOO (Thoth dispatch 6713, closing the hole above
            # Khnum's own claim_name backstop, 2c65c6d): this fallback used to hand a
            # fresh body `_bg_boot_prompt`'s INSTRUCTION-shaped text ("claim_name
            # yourself") — Sekhmet's Piece 1 (msg 6692/6694) fixed launch_seat's own
            # fresh-mint fallthrough the same way this fixes dispatch_dm's: identity
            # is a fact the server already holds (office/handle/house all came off
            # `seat_facts` above), not a re-derivation to hand a fresh session through
            # a fallible tool call it might refuse and then route around (the Marquee
            # specimen, one path over). `current_holder=target`: `target` IS this
            # seat's own declared living head — the identity whose compaction seam
            # triggered this fallback in the first place — used only as
            # `_bind_before_spawn`'s OWN fallback (its real ancestor resolution reads
            # the seat's lineage first, never the `holds` edge, same as launch_seat's
            # own call).
            bound = await _bind_before_spawn(
                Actions(pool), target_seat=seat_id, handle=handle, house=house,
                current_holder=target, office=office, anchor=anchor,
                source=sender or "session")
            boot_prompt = _bg_boot_prompt_bound(
                office=office, anchor=anchor, handle=handle, agent=bound["agent"],
                generation=bound["generation"])
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
    session_id, repo, materialized_at = graph_resume[0], graph_resume[1], graph_resume[4]
    # THE SPAWN CWD IS THE OFFICE, ALWAYS (operator, 2026-09-03: "~/.osiris is where osiris
    # agents get their identity and meta from, and cannot drift from the db state"): the
    # materializer emits the store's canon into the OFFICE slug, and the harness resumes
    # whichever copy of a session id sits in the SPAWN cwd's own slug (measured live,
    # 2.1.259: a cwd whose slug held a stale 20KB partial resumed THAT, not the fuller copy
    # one slug over; cross-slug lookup found nothing at all once two copies existed). The
    # old `or repo` fallback therefore spawned at a stale copy whenever the emit was
    # declined — Chad's exact 2026-09-03 shape. `repo` stays in the tuple untouched for
    # `_zero_hop_graph_corroborates`'s own invariant; it is the spawn location ONLY as
    # the last-resort fallback for a seatless lineage whose office cannot be derived.
    spawn_cwd = materialized_at or await _resume_office(pool, seat_id, fallback=repo)
    hop = graph_resume[5]
    gate, refusal = await _resume_guard(
        pool, (graph_resume[0], graph_resume[1], graph_resume[2], graph_resume[3]), base,
        seat_id=seat_id, st=st, hop=hop, launch_cwd=launch_cwd)
    if gate is not None:
        return {"mode": f"resume-refused-{gate}",
                "detail": f"{refusal} — refusing both nudge and resume; the mail "
                          "stays pull-only for now"}
    # OCCUPANCY, THE OTHER HALF (task #178): identity above says WHOSE session this is;
    # this asks whether anybody is ALREADY SITTING there. A `-p --resume` beside a live
    # body forks the mind — refuse it here, before the spend, never after.
    occupied = await _resume_occupancy_gate(
        (graph_resume[0], graph_resume[1], graph_resume[2], graph_resume[3]),
        agents_json=agents_json, spawn_cwd=spawn_cwd)
    if occupied is not None:
        which, reason = occupied
        if which == "self":
            # NOT A FAILURE. The addressee is live and holds this exact session; the mail
            # is in its box and its next turn's inbox() finds it. Resuming would fork the
            # mind for no gain, so we don't — but saying "refused, pull-only" here reads
            # as unreachable and is how a reader (Alfred on 60bc15db, Thoth on 2026-08-28)
            # concludes a reachable seat is lost and escalates. Name the outcome instead.
            return {"mode": "queued-live-holder",
                    "detail": f"{reason} — it reads this from its own inbox at its next "
                              "turn; a resume would only fork the mind, so none was "
                              "spent. Nothing is lost and nothing is owed"}
        return {"mode": "resume-refused-occupied-foreign",
                "detail": f"{reason} — refusing to fork a second mind beside it; whether "
                          "that body ever reads this mail is UNKNOWN, never a resolved "
                          "delivery (practice 2c45d78e)"}
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
    await spawn(spawn_cwd, _DM_RESUME_PROMPT, resume_session=session_id,
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
    OWN content carries the marker counts.

    RE-DERIVED, NOT INHERITED (Thoth dispatch 6715): Khnum's own survey found this glob's
    shape (`*/{sid_prefix}*.jsonl`, a substring match) but had previously cleared it as
    safe-by-construction because it scans EVERY match rather than trusting the first —
    true when he said it, and it STAYS true even now that the materializer writes second
    copies: scanning more physical copies of the SAME session's real content can only ever
    ADD a confirmation, never suppress one (an `or` across matches has no failure mode
    where MORE true matches makes the answer wrong). What duplicates change is the
    UNRELATED risk this function shares with the old `forks._find` (also fixed this
    dispatch): an unanchored substring match can catch a LOOK-ALIKE filename that merely
    CONTAINS `sid_prefix`, never at the very start — and more files on disk (duplicates
    included) is more surface for that pre-existing look-alike risk to fire. The fix here
    is narrower than `_find`'s: this function's whole SAFETY PROPERTY depends on scanning
    every genuine copy, so collapsing to `locate_current_transcript`'s single newest-
    anchored file (as `_find` now does) would be a REAL regression — the marker landing in
    a copy other than the newest would then read as a false negative, and `wake()`'s own
    caller re-injects a turn that already landed. So: anchor the MATCH (stem-prefix, same
    rule `locate_current_transcript` uses) while keeping the loop over every match."""
    if not sid_prefix or not marker:
        return False
    for t in root.expanduser().glob("*/*.jsonl"):
        if not t.stem.startswith(sid_prefix):
            continue
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
    # OCCUPANCY, SPLIT (2026-08-28). #178 added `resume-refused-occupied` and never added
    # it here, so it rode an unnamed default written for rate-brakes and pauses. Worse, it
    # answered two different findings with one word. Both arms are named now:
    # the addressee's OWN live session is a DELIVERY OUTCOME (it reads at its next turn,
    # exactly what `queued-live-unresolved` above exists to stop mislabelling)...
    "queued-live-holder": "queued",
    # ...while an unidentified body in the office is a real refusal with an unknown reader.
    "resume-refused-occupied-foreign": "refused-occupied-foreign",
    # the pre-split mode, kept mapped so stored receipts never fall to a default.
    "resume-refused-occupied": "queued",
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


def _is_git_tree(path: str) -> bool:
    """Sync helper, same ASYNC240 reasoning as `_tree_exists`: does `path` hold a git
    checkout (a `.git` directory or worktree file)? A bound tree that does NOT is not
    proof of anything by itself — but paired with a charter that governs a real tree
    elsewhere, it is the #199 fabrication's exact signature."""
    return (Path(path) / ".git").exists()


async def fabricated_tree_verdict(
    pool: asyncpg.Pool, seat_id: str, tree_cwd: str,
) -> tuple[str, str] | None:
    """THE #199 MINT-TIME FABRICATION, caught at the launch door (operator, 2026-09-03:
    "launch has a bug that lands the agent in the wrong cwd" — Jesus and Chad were both
    bound at mint to a convention-derived ~/code/<handle> holding nothing but a `.osiris`
    pin, while their charters governed the real trees at ~/code/REPOS/Godel and
    ~/code/cdking, where every real session had actually worked). Returns
    `(repo, real_path)` when `tree_cwd` holds no git tree AND the seat's charter governs a
    project whose recorded on_disk_path IS a git tree somewhere else — the one shape where
    "spawn where the graph says" is knowably wrong. None otherwise: a tree with a `.git`
    is trusted as bound (a non-git workspace with no chartered tree elsewhere is a
    legitimate state, never refused). Shared by BOTH launch doors (cli.py and
    launch_seat), one implementation — the guard-symmetry rule (983ec87a)."""
    from src.orchestrator.charter import governed_trees
    if _is_git_tree(tree_cwd):
        return None
    for repo, path in await governed_trees(pool, seat_id):
        if path != tree_cwd and _tree_exists(path) and _is_git_tree(path):
            return repo, path
    return None


def _launch_anchor(seat_id: str) -> str:
    """A STABLE durable anchor per SEAT (not per launch) — a re-launched seat re-wears its own
    anchor, the same 'one ghost, re-worn' discipline _wake_job_dir uses per project. The seat
    (via its attach token) is the true identity; this is the session anchor the trigger keys
    its mail-window routing on, so it must match the window's own job_dir metadata."""
    return str(Path.home() / ".claude" / "jobs" / seat_id.replace(":", "-"))


async def _bind_before_spawn(
    actions: Actions, *, target_seat: str, handle: str, house: str | None,
    current_holder: str | None, office: str, anchor: str, source: str,
) -> dict[str, Any]:
    """PIECE 1 (Thoth dispatch, msg 6692, d161a156 one layer up): mint and bind the seat's
    next generation SERVER-SIDE, before the body exists — the identity `launch_seat` already
    holds at prompt-construction time (office, handle, house, and everything generation math
    needs) written as a fact instead of serialized into English and handed to a fresh
    session to re-derive through a fallible tool call. THE LIVE SPECIMEN: Marquee's own
    `claim_name` refused (the name was held by an unrelated lineage, seat:bdbe031e's actual
    recorded holder) and the session's own autonomy clause — "never park on a question typed
    into this empty room" — converted that refusal into a phantom mint: a stranger Agent
    ("Awning"), a stray Seat, an orphan SoftwareProject. The agent behaved correctly; the
    machinery asked it to do something that could fail and gave it no way back once it did.

    ANCESTOR CASE (the ordinary one): reuses `mint_heir` outright rather than duplicating
    its own hard-won mechanics — grave-avoidance, the succeeded_by/succeeded_from chain,
    handle inheritance. The ancestor is resolved from THE SEAT'S OWN LINEAGE, never from
    `current_holder` (the `holds` edge) directly (Thoth's own correction, msg 6694, measured
    against Marquee: 11 generations of the seat's own substantive properties — handle,
    house, intended_model — all sourced from lineage agent:38cf08a9, and exactly ONE
    disagreeing edge, `holds`, naming an unrelated agent that built none of the rest). A
    COLD seat's `holds` edge can go stale without the seat itself being wrong — the defect
    is resolving generation math FROM that edge instead of walking the lineage that actually
    built the seat. `_seat_lineage_ancestor` reads the seat's own `handle` assertion's
    SOURCE (the property least likely to ever be written by a lineage that isn't genuinely
    this seat's own) and walks it forward via `lineage_head` to the TRUE current generation
    — the same succeeded_by walk fork resolution and mailbox routing already trust. Falls
    back to `current_holder` only when the seat has no handle assertion at all (nothing
    lineage-side to read yet — a seat still using the pre-Seat-object binding, or a launch
    racing its own first-ever claim). Either way, `bind_holder` is called EXPLICITLY below,
    never left to mint_heir's own `follow_binding` alone — a corrected-lineage ancestor may
    not itself hold `target_seat` (Marquee's own 38cf08a9-xi doesn't; 226a2695-ii does), so
    follow_binding would move nothing. This is deliberately the SAME act that corrects a
    stale `holds` edge, sequenced to happen only when a real launch actually runs, through
    this named verb, with the reasoning recorded on the heir — never a hand-write.

    NO-ANCESTOR CASE (a seat truly never described, no handle assertion, no holder): mints
    a bare first generation directly, under the same `agent:seat-<id>` root every pure-seat-
    office lineage already carries (my own root, `agent:seat-af50a33e`, is one) — a
    deterministic, collision-free identity derived from the seat itself, never invented from
    a transcript that does not exist yet.

    HOUSE, NOT GOVERNS (Thoth's own flag, msg 6694, a separate axis, not this dispatch's to
    chase): a seat's `house` property can be stale label residue after a project fold
    (Marquee's own case: `house`='dealer-to-fb', `governs`->'dtfb', the merged live name) —
    `house` here is `launch_seat`'s own caller-supplied param, unchanged from before this
    dispatch; this function does not re-derive it, on purpose, so it carries forward exactly
    whatever staleness already existed rather than silently fixing a different bug in passing.

    THE PRE-REGISTRATION IS WHAT MAKES claim_name UNNECESSARY: `save_mount(..., alive=False)`
    seeds the launch anchor's own durable row BEFORE the process exists, so the spawned
    session's own `mount(cwd=office, job_dir=anchor)` call re-attaches through the EXISTING
    durable-row door (`mounts.find_mount`) instead of falling through to a fresh identity
    resolution — the same "seated the moment you exist; a heartbeat is earned by an act,
    never granted by a greeting" discipline `save_mount`'s own docstring already describes
    for an heir minted at ordinary succession time, fired one step earlier than usual: before
    the body exists, not at its first call."""
    from src.orchestrator.agents import _generation, mint_heir
    from src.orchestrator.mounts import save_mount
    from src.orchestrator.seats import bind_holder

    now = datetime.now(UTC)
    ancestor = await _seat_lineage_ancestor(actions.pool, target_seat) or current_holder
    if ancestor:
        ancestor_oid = await actions.create_or_find_object("Agent", ancestor, source)
        heir_id, _heir_oid = await mint_heir(
            actions, ancestor, ancestor_oid,
            because="launch_seat: bind-before-spawn (piece 1, msg 6692)", succession=None,
            now=now, minting_door=anchor, upcoming_project=house)
    else:
        from src.parsers.base import EvidenceClass
        from src.parsers.evidence import confidence_for

        heir_id = f"agent:seat-{target_seat.removeprefix('seat:')}"
        heir_oid = await actions.create_or_find_object("Agent", heir_id, source)
        do = EvidenceClass.DIRECT_OBSERVATION
        conf = confidence_for(do)
        await actions.assert_property(
            heir_oid, "minted_because",
            "launch_seat: bind-before-spawn, no prior holder (piece 1, msg 6692)",
            source, now, conf, evidence_class=do.value)
        await actions.assert_property(heir_oid, "handle", handle, source, now, conf,
                                      evidence_class=do.value)
        await actions.assert_property(heir_oid, "seat_generation", "1", source, now, conf,
                                      evidence_class=do.value)
    # EXPLICIT, ALWAYS — never left to mint_heir's own follow_binding alone (see docstring:
    # a corrected-lineage ancestor may not itself hold `target_seat` yet). Idempotent
    # (bind_holder checks for an already-live identical link before writing) so this is a
    # no-op when follow_binding already did the work, and the actual correction when it
    # didn't.
    await bind_holder(actions, seat_id=target_seat, agent_id=heir_id, source=source)
    await save_mount(actions.pool, job_dir=anchor, agent_id=heir_id, project=house,
                     cwd=office, model=None, session_key=None, alive=False)
    return {"agent": heir_id, "generation": _generation(heir_id)[1]}


async def _seat_lineage_ancestor(pool: asyncpg.Pool, seat_id: str) -> str | None:
    """The generation to mint `_bind_before_spawn`'s heir OF — resolved from the seat's OWN
    lineage, never from its `holds` edge (Thoth's correction, msg 6694). Reads the seat's own
    `handle` assertion's source_id (the property least likely to ever be written by a
    lineage that isn't genuinely this seat's own) and walks it forward via `lineage_head`
    (agents.py — the same succeeded_by walk fork resolution and mailbox routing already
    trust) to the TRUE current generation. None when the seat carries no handle assertion at
    all — nothing lineage-side to read yet.

    THE CONSOLE-ANCESTOR DEFECT (operator's own live catch, 2026-09-03, Chad's own seat):
    "least likely to ever be written by a lineage that isn't genuinely this seat's own" was
    still wrong for a seat established DIRECTLY by a human at the CLI — Chad's own genesis
    `handle` assertion has ALWAYS carried `source_id='console'` (`seats.py`'s own
    `_OPERATOR_ACTORS` sentinel, never an agent canonical), from before any lineage of its
    own ever mounted. `osiris launch chad` fed that source straight into `lineage_head`,
    which found no succeeded_by chain to walk and returned "console" unchanged — a
    non-agent CLI actor label treated as a real ancestor, minting a phantom `console`/
    `console-ii` "lineage" and REBINDING CHAD'S SEAT TO IT (agents.py's own `holds` edge,
    2026-09-03 20:13:29). `_OPERATOR_ACTORS` is EXCLUDED HERE, never treated as a genuine
    ancestor — falls through the SAME safe path as "no handle assertion at all" (the
    caller's own `or current_holder` fallback), because a CLI actor sentinel is exactly
    that: a label for WHO deliberately typed a command, never a fact about which agent
    lineage built this seat."""
    source = await pool.fetchval(
        "SELECT h.source_id FROM current_assertions h JOIN objects o ON o.id=h.object_id "
        "WHERE o.canonical=$1 AND o.type='Seat' AND h.name='handle' "
        "ORDER BY h.confidence DESC, h.observed_at DESC LIMIT 1", seat_id)
    if not source:
        return None
    from src.orchestrator.agents import _generation, lineage_head
    from src.orchestrator.seats import _OPERATOR_ACTORS

    if source in _OPERATOR_ACTORS:
        return None
    base = _generation(str(source))[0]
    return await lineage_head(pool, base)


def _bg_boot_prompt_bound(*, office: str, anchor: str, handle: str, agent: str,
                          generation: int) -> str:
    """PIECE 1's boot prompt: a STATEMENT, not an instruction (Thoth's acceptance criterion,
    msg 6692 — `claim_name` disappears from this text; if it survives, the inversion did not
    happen). The session's identity is already written (`_bind_before_spawn`, run just before
    this); mount() re-attaches to it through the pre-registered anchor row, nothing left to
    negotiate or re-derive. THE ONLY BOOT PROMPT NOW (Thoth dispatch 6713): the unbound
    `_bg_boot_prompt` this function was deliberately kept separate from — dispatch_dm's own
    fresh-heir fallback was its last caller — is gone; dispatch_dm now binds before spawn
    the same way launch_seat does, through this function."""
    from src.orchestrator.agents import _to_roman

    label = f"{handle} {_to_roman(generation)}" if generation > 1 else handle
    return (
        f'You are {label}, {agent}, already bound to your seat\'s office. Call '
        f'mount(cwd="{office}", job_dir="{anchor}") to attach — it re-earns your binding '
        f"from the graph directly. Then inbox() for your opening brief. No one is "
        f"watching this window: work the brief to completion, a real blocker, or a "
        f"seam — never park on a question typed into this empty room; a genuine ask "
        f"goes out as mail (grade='ask')."
    )


async def _launch_twin_check(
    pool: asyncpg.Pool, agents_json: Any, launch_cwd: str, *, seat_id: str | None = None,
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
    verdict for, only ONE OR THE OTHER OR NEITHER, and this reports exactly which.

    BY LINEAGE TOO, NOT ONLY BY CWD (operator, 2026-09-03 — the office ruling's own
    hazard): the moment a seat's spawn location moves (tree_cwd rebound off a fabricated
    ~/code/<handle>, resume now spawning at the office), a body of the SAME seat still
    sitting at the OLD cwd is invisible to a cwd-keyed check, and the next launch/resume
    forks the mind. With `seat_id`, this also reads the seat's current holder lineage:
    any harness row whose sessionId is one of the lineage's own graph sessions, or any
    live agent_mounts row for the lineage, at ANY cwd, refuses the same way. Jesus's live
    shape tonight: its real mind sat at ~/code/jesus while the seat's launch cwd had just
    become ~/code/REPOS/Godel — the cwd check saw nothing there."""
    try:
        roster = await agents_json(cwd=launch_cwd)
    except (OSError, TimeoutError, ValueError):
        roster = []
    from_harness = next((r for r in roster
                         if isinstance(r, dict) and r.get("cwd") == launch_cwd), None)
    from_mounts_row = await pool.fetchrow(
        "SELECT agent_id, last_seen FROM agent_mounts WHERE cwd=$1 "
        "ORDER BY last_seen DESC NULLS LAST LIMIT 1", launch_cwd)
    from src.orchestrator.mounts import is_live
    from_mounts = None
    if from_mounts_row is not None and from_mounts_row["last_seen"] is not None:
        if is_live(from_mounts_row["last_seen"]):
            from_mounts = {"agent_id": from_mounts_row["agent_id"],
                           "last_seen": from_mounts_row["last_seen"].isoformat()}
    if seat_id and from_harness is None and from_mounts is None:
        from src.orchestrator.agents import _generation
        from src.orchestrator.seats import seat_receipt
        from src.orchestrator.succession import succession_chain
        holder = ((await seat_receipt(pool, seat_id)) or {}).get("holder")
        if holder:
            root = _generation(holder)[0]
            sessions = {str(h["session"]) for h in await succession_chain(pool, holder)
                        if h.get("session")}
            try:
                everywhere = await agents_json()
            except (OSError, TimeoutError, ValueError):
                everywhere = []
            from_harness = next(
                (r for r in everywhere if isinstance(r, dict)
                 and any(str(r.get("sessionId") or "").startswith(sid) for sid in sessions)),
                None)
            lineage_row = await pool.fetchrow(
                "SELECT agent_id, last_seen, cwd FROM agent_mounts "
                "WHERE agent_id=$1 OR agent_id LIKE $1 || '-%' "
                "ORDER BY last_seen DESC NULLS LAST LIMIT 1", root)
            if (lineage_row is not None and lineage_row["last_seen"] is not None
                    and is_live(lineage_row["last_seen"])):
                from_mounts = {"agent_id": lineage_row["agent_id"],
                               "last_seen": lineage_row["last_seen"].isoformat(),
                               "cwd": lineage_row["cwd"]}
    if from_harness is None and from_mounts is None:
        return None
    return {"harness": from_harness, "mounts": from_mounts}


async def _launch_target_setup(
    actions: Actions, *, caller: str, target: str, agents_json: Any,
) -> dict[str, Any]:
    """THE GATE + FACTS SHARED BY BOTH launch_seat AND resume_seat (extracted, task #199
    lane 3C, ruling 41a41437/msgs 6823/6831: "one shared orchestration shell, the
    managed_by-vs-operator gate as the single irreducible branch" — this is that shell's
    MCP-agent half; the caller-trust gate below is the one piece that genuinely differs
    from the CLI's own operator door and stays irreducible on purpose, not collapsed).

    Runs the managed_by trust gate, resolves seat_facts/tree_cwd, builds the attach line,
    and enforces ONE SEAT, ONE LIVE LINEAGE HEAD (ruling 921eabcf item 1) — checked here,
    once, so neither launch nor resume can ever fork a second eligible head.

    A dict with NO `target_seat` key is an ERROR — return it to the caller UNCHANGED.
    Otherwise every field a launch or resume decision needs is resolved and ready:
    target_seat, handle, house, office, tree_cwd, launch_cwd, attach, anchor,
    current_holder, facts."""
    pool = actions.pool
    from src.orchestrator.seats import held_seat, seat_receipt

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

    tree_cwd = facts["tree_cwd"]
    launch_cwd = office
    if tree_cwd:
        if not _tree_exists(tree_cwd):
            return {"status": "refused-no-tree",
                    "detail": f"{handle} ({target_seat}) names tree_cwd={tree_cwd!r} but it "
                              "does not exist on disk — osiris expects the harness (or a "
                              "human, via EnterWorktree) to have created it before launch; "
                              "it never provisions one itself"}
        fabricated = await fabricated_tree_verdict(pool, target_seat, tree_cwd)
        if fabricated is not None:
            repo, real_path = fabricated
            return {"status": "refused-fabricated-tree",
                    "detail": f"{handle} ({target_seat}) names tree_cwd={tree_cwd!r}, which "
                              f"holds no git tree, while its charter governs {repo!r} at "
                              f"{real_path!r} (a real tree) — the #199 mint-time "
                              "fabrication; a body spawned there lands in the wrong cwd. "
                              f"Fix it with: bind_seat_tree(seat_id={target_seat!r}, "
                              f"tree_cwd={real_path!r}, because='...')"}
        launch_cwd = tree_cwd

    anchor = _launch_anchor(target_seat)
    attach = {"office": office, "tree_cwd": tree_cwd, "session_anchor": anchor,
             "command": f'python -m src.manager.attach "[{_house_tag(house)}] {handle}"'}

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

    return {"target_seat": target_seat, "handle": handle, "house": house, "office": office,
            "tree_cwd": tree_cwd, "launch_cwd": launch_cwd, "attach": attach, "anchor": anchor,
            "current_holder": current_holder, "facts": facts}


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
    lane's — either way, without a live daemon or a live `claude` binary.

    ALWAYS A FRESH MINT NOW, NEVER AN AUTOMATIC RESUME (ruling 41a41437, task #199 lane 3C,
    mirroring the CLI's own ruling 60c78788 exactly — "the verb IS the property, no flag,
    no automatic guess"). This lane used to check the seat's own lineage for a resumable
    session before minting fresh — that automatic branch is GONE. Continuing a dormant
    session is now `resume_seat`'s own, separate job (right below this function); its own
    docstring carries the full history this one used to (the lineage walk, the resident-
    unknown refusal, the one-shot `-p --resume` shape, the receipt's decision-naming
    discipline) — read it there, not here, if that is what you are looking for."""
    pool = actions.pool
    from src.orchestrator.agents import house_of
    manager = manager or _manager_control
    windows = windows or _manager_windows
    spawn = spawn or _spawn_claude_bg
    agents_json = agents_json or _claude_agents_json
    cost_reader = cost_reader or _bg_session_cost

    setup = await _launch_target_setup(
        actions, caller=caller, target=target, agents_json=agents_json)
    if "target_seat" not in setup:
        return setup
    target_seat, handle, house = setup["target_seat"], setup["handle"], setup["house"]
    office, tree_cwd = setup["office"], setup["tree_cwd"]
    launch_cwd, attach, anchor = setup["launch_cwd"], setup["attach"], setup["anchor"]
    current_holder, facts = setup["current_holder"], setup["facts"]

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
        twin = await _launch_twin_check(pool, agents_json, launch_cwd, seat_id=target_seat)
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

        # NO RESUME CHECK HERE, DELIBERATELY (ruling 41a41437/60c78788, task #199 lane 3C):
        # launch ALWAYS mints fresh now, mirroring the CLI's own osiris launch exactly — the
        # resume-lineage walk this lane used to run first (and the receipt's resume_check
        # field explaining "why booted fresh") moved wholesale to resume_seat below, which
        # owns that decision now. Continuing a dormant session is resume_seat's job, never
        # an automatic branch inside launch again.

        # IDENTITY IS BOUND HERE, BEFORE THE BODY EXISTS (Piece 1, Thoth dispatch msg 6692,
        # d161a156 one layer up — replacing the prior design, which told the session to
        # re-derive its own binding via mount()+claim_name() through its first turn). THE
        # LIVE SPECIMEN: Marquee's own claim_name refused (the name was held by an unrelated
        # lineage) and the session's own autonomy clause converted that refusal into a
        # phantom mint rather than parking on the question — the agent behaved correctly,
        # the machinery handed it a fallible re-derivation to do instead of a fact.
        # `launch_seat` already holds everything generation math needs (office, handle,
        # house, `current_holder` from the occupancy check above) — `_bind_before_spawn`
        # writes that identity NOW, deterministically, server-side.
        bound = await _bind_before_spawn(
            actions, target_seat=target_seat, handle=handle, house=house,
            current_holder=current_holder, office=office, anchor=anchor, source=caller)
        # THE BOOT PROMPT ANCHORS mount() AT THE OFFICE, ALWAYS — never `launch_cwd`. Identity
        # lives at the office regardless of where the process's own cwd happens to sit (#103's
        # whole point); a tree-bound seat's session boots WITH its shell cwd at tree_cwd (the
        # `spawn(launch_cwd, ...)` below) but is told to mount(cwd=office) all the same, the
        # identical pattern every seat in this house already follows by hand. It is now a
        # STATEMENT, not an instruction — claim_name does not appear (Thoth's acceptance
        # criterion, msg 6692): mount() re-attaches to the identity `_bind_before_spawn` just
        # wrote, through the pre-registered anchor row, nothing left to negotiate.
        boot_prompt = _bg_boot_prompt_bound(
            office=office, anchor=anchor, handle=handle, agent=bound["agent"],
            generation=bound["generation"])

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
        # NO resume_check FIELD HERE ANYMORE (ruling 41a41437): there is no resume decision
        # for this receipt to name — launch never walks the lineage now, it just mints. Use
        # resume() to continue a dormant session; its own receipt carries this same kind of
        # decision-naming discipline for the question it actually answers.
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


async def resume_seat(
    actions: Actions, *, caller: str, target: str, message: str = "",
    model: str | None = None, settings: Settings | None = None,
    agents_json: Any = None, resume_spawn: Any = None,
) -> dict[str, Any]:
    """Continue a seat's own DORMANT session — `launch_seat`'s former auto-resume branch,
    now its own verb (ruling 41a41437, task #199 lane 3C, mirroring the CLI's own
    ruling 60c78788: launch always mints fresh, resume always continues, no flag, no
    automatic guess — the VERB is the property). Same managed_by gate and seat/occupancy
    facts as launch_seat (`_launch_target_setup`, the one shared shell), then walks the
    seat's own LINEAGE for a resumable session and either continues it or REFUSES —
    NEVER falls through to a fresh mint, mirroring the CLI's own `_cmd_resume_harness`
    exactly: minting on this door is `launch_seat`'s job alone, never resume's.

    WALKS THE LINEAGE (`_lineage_resume_candidate`, not the plain agent_mounts-keyed
    `_agent_resumable`/`wakeable_identity` dispatch_dm's DM lane uses) — a `--bg`-launched
    seat's every generation shares ONE durable per-seat mount anchor (`_launch_anchor`),
    fixed in CLAUDE_JOB_DIR for that OS process's entire life, so `agent_mounts`' one
    shared row for it NEVER encodes a real session id, only whichever generation most
    recently mounted. `_lineage_resume_candidate` reads `session` instead — a GRAPH
    property, immune to the shared-anchor collapse (live-fire finding, 2026-08-04, Thoth
    msg 3691, Sekhmet) — and the receipt NAMES the decision every time (which generation,
    how many hops back, the actual transcript size and gate numbers), never a silent
    correct-by-accident refusal (4ef68cfe).

    ONE-SHOT, DELIBERATELY, not a standing window: a `-p --resume` body runs its one turn
    and exits (confirmed live: `claude agents --json --all` cannot retain it even when the
    body itself calls mount() mid-turn — a harness fact, not ours to fix, decision
    a829a15d). This is not a downgrade from `--bg`'s persistent window — it is
    RE-SUMMONABLE, not unreachable: dispatch_dm's own resume lane wakes the same session
    again on the next mail, so a standing window here would be a SECOND mechanism doing a
    job dispatch_dm's resume lane already does (38c71544's shape). Decision 696d302c named
    "the launch window is a property of launch, not a per-launch question" as the general
    rule for this whole class of recurring coordinator decisions, and this function already
    lives that rule. `resume_spawn` injects `_spawn_claude` (the `-p --resume` lane) for
    tests, parallel to `agents_json` above.

    THE UNKNOWN ARM NEVER MINTS A STRANGER (thread ef88e2bb, operator, 2026-08-17, ruling
    7d6815bb): a `resident-unknown` gate is an ABSENCE of signed testimony, not a positive
    finding of a different mind — it used to fall through to a fresh mint (back when this
    logic lived inside launch_seat) the same as a real `crossed-registry` finding, minting
    strangers over ferryman's and halcyon's actually-resumable heads. It refuses the WHOLE
    resume (`status: refused-resume-unknown`), spawning nothing, naming the exact
    `claude -p --resume <sid>` a human can run by hand. `crossed-registry` REFUSES here too
    now (`status: refused-nothing-to-resume`) — that session was never this seat's, and
    resume has nothing left to fall through TO; the caller wants `launch` instead."""
    pool = actions.pool
    from src.orchestrator.agents import _generation, house_of
    from src.orchestrator.seats import seat_receipt
    agents_json = agents_json or _claude_agents_json
    resume_spawn = resume_spawn or _spawn_claude

    setup = await _launch_target_setup(
        actions, caller=caller, target=target, agents_json=agents_json)
    if "target_seat" not in setup:
        return setup
    target_seat, handle = setup["target_seat"], setup["handle"]
    launch_cwd, attach = setup["launch_cwd"], setup["attach"]
    office, facts = setup["office"], setup["facts"]

    st = settings or get_settings()
    argv_model = model or facts.get("intended_model") or st.osiris_wake_model or None

    # THE RESUME LANE (mechanism moved verbatim from launch_seat's own former harness
    # branch, task #199 lane 3C — behavior UNCHANGED, only where it lives). `holder` (not
    # `target_seat`) is the identity `_lineage_resume_candidate`/`_resume_guard` need — a
    # Seat is never itself an Agent lineage.
    holder = ((await seat_receipt(pool, target_seat)) or {}).get("holder")
    resume_outcome = await _lineage_resume_candidate(
        pool, holder, st, repo=launch_cwd, seat_id=target_seat,
    ) if holder else ["no seat holder on record"]
    resume_log = resume_outcome[1] if isinstance(resume_outcome, tuple) else resume_outcome
    resume = resume_outcome[0] if isinstance(resume_outcome, tuple) else None
    if resume is not None:
        assert holder is not None
        # hop count (#173a): READ DIRECTLY off `resume`'s own 6th field (task #200
        # residual, decision 6a0b1236/6d6bf4e8) — never re-derived from
        # `len(resume_log) - 1`, which silently miscounts whenever
        # `_lineage_resume_candidate` appends a second log line for the winning hop.
        gate, refusal = await _resume_guard(
            pool, (resume[0], resume[1], resume[2], resume[3]), _generation(holder)[0],
            seat_id=target_seat, st=st, hop=resume[5], launch_cwd=launch_cwd)
        if gate == "resident-unknown":
            return {"status": "refused-resume-unknown", "seat": target_seat,
                    "session": resume[0], "body_exists": False, "can_receive": False,
                    "detail": f"{refusal} — refusing rather than minting a stranger "
                              f"over a possibly-resumable head; run `claude -p --resume "
                              f"{resume[0]}` by hand to confirm it yourself, or clear "
                              "the seat's stale session pointer if it's truly dead"}
        if gate is not None:
            resume_log = [*resume_log, f"{gate} guard refused it: {refusal}"]
            resume = None

    if resume is None:
        # NEVER FALLS THROUGH TO A FRESH MINT (the one deliberate behavior change from
        # launch_seat's former combined lane, per ruling 41a41437/_cmd_resume_harness's
        # own precedent): resume answers ONE question, and "nothing to resume" is a real
        # answer to it, not an invitation to do launch's job instead.
        return {"status": "refused-nothing-to-resume", "seat": target_seat,
                "body_exists": False, "can_receive": False, "resume_check": resume_log,
                "detail": f"no resumable session found for {handle} ({'; '.join(resume_log)}) "
                          f"(gate: min_tail_bytes={st.osiris_resume_min_tail_bytes}, "
                          f"ceiling={st.osiris_resume_ceiling_bytes}b) — nothing was spawned; "
                          "use launch() to mint a fresh body instead"}

    session_id, materialized_at = resume[0], resume[4]
    # office, never the resume candidate's own repo field — see dispatch_dm's own
    # identical line for why.
    spawn_cwd = materialized_at or await _resume_office(pool, target_seat, fallback=office)
    # THE MESSAGE LANDS BEFORE THE SPAWN, deliberately unlike launch's fresh-mint lane
    # (which sends its brief AFTER spawning): a fresh `--bg` body takes seconds to boot,
    # mount, and claim_name before its first inbox() call, so send-after-spawn there is
    # safely ordered by the boot lag alone. A RESUMED body has no such lag — its first
    # turn IS the inbox() check (_DM_RESUME_PROMPT) — so the mail row must exist first,
    # the same "ledger before spawn" discipline dispatch_dm's own resume branch follows.
    resume_brief_id: int | None = None
    if message.strip():
        sent = await send_message(
            pool, from_agent=caller, from_project=await house_of(pool, caller),
            to_agent=target_seat, body=message, grade="ask")
        resume_brief_id = sent.get("id")
    await resume_spawn(spawn_cwd, _DM_RESUME_PROMPT, resume_session=session_id,
                       model=argv_model, allowed_tools=st.osiris_wake_allowed_tools or None)
    adoption = await _adopt_resumed_body(
        pool, agents_json=agents_json, office=spawn_cwd, requested_sid=session_id,
        holder=str(holder), project=facts.get("house"))
    if adoption.get("copied"):
        resume_log.append(f"harness started a COPY ({str(adoption['session_id'])[:8]}) "
                          f"instead of continuing {session_id[:8]} — a stopped record was "
                          "still on file; adopted as the seat's own continuation")
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


async def _real_kill_pid(pid: int, job_dir_key: str | None) -> None:
    """PREFER THE HARNESS'S OWN `claude stop <id>` — live-reproduced, Wave 6 (thread 6002,
    Thoth's follow-up dispatch): a raw SIGTERM to the inner process of a `--bg`-substrate
    body (the DEFAULT launch lane, `osiris_launch_substrate="harness"`) gets SILENTLY
    RESPAWNED by the harness's own background-agent daemon within about a minute — the
    daemon reads an unexpected process exit as a crash to heal, re-resuming the SAME named
    window onto a fresh pid, not as a stop request. `stop_seat` reported `status="stopped"`
    while the body kept coming back — the exact false-clean reading the operator's own
    standing instruction on spawn/stop hygiene warns against ("a stop verb that reports
    success while leaving a body is strictly worse than no stop verb"). `claude stop <id>`
    is the daemon-aware verb ("Its conversation is kept... `claude --resume` works once it
    is stopped" — the harness's own `stop|kill` help text) and does NOT trigger that
    auto-heal; `job_dir_key` (registry_census's own field, identical to `claude agents
    --json`'s own `id`) is already resolved by every caller of this function, so no new
    lookup is needed to use it.

    FALLS BACK to a raw SIGTERM only when no harness-tracked id exists at all — the PTY-
    broker fallback lane (`osiris_launch_substrate="pty"`), which the harness's own daemon
    never sees and so cannot auto-heal; a graceful ask there, not SIGKILL, exactly as
    before. Also falls back if `claude stop` itself exits non-zero (an unknown id, a dark
    daemon) — better an ordinary SIGTERM than silence.

    Injectable so no test ever touches a real process or spawns a real subprocess."""
    if job_dir_key:
        proc = await asyncio.create_subprocess_exec(
            "claude", "stop", job_dir_key,
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        if await proc.wait() == 0:
            # THEN REMOVE THE STOPPED RECORD (measured live, 2026-09-03, harness 2.1.259):
            # `claude stop` leaves a "stopped" background record, and `claude --bg
            # --resume <id>` against a session that still has ANY record "starts a copy
            # and says so" (the harness's own --bg help text) — a NEW session id, a fresh
            # transcript, and osiris's whisper meeting a stranger. Three copies of Chad in
            # a row tonight (092b7418, 5f54e1fc, a9d4118e) until the record was removed;
            # with it removed, `--bg --resume` continued 7451509a under its own id. `claude
            # rm` "deletes a background session and its worktree" — the RECORD, never the
            # transcript (all three of Chad's files stayed on disk) — so a stopped seat is
            # resumable in place, which is the whole point of stopping instead of killing.
            # Best effort: a failed rm leaves exactly the pre-fix state, never worse.
            rm = await asyncio.create_subprocess_exec(
                "claude", "rm", job_dir_key,
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
            await rm.wait()
            return
    import signal
    os.kill(pid, signal.SIGTERM)


# The human's own address, reused from the mailbox rather than invented here — one notion
# of "the operator" in the codebase, never a second (task #82's lesson: a third copy of the
# operator-actor list was exactly how 'analyst:operator' went missing from one of them).
_OPERATOR_CALLER = "operator"


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
    boolean flag a later caller has to remember to clear.

    RELEASES THE SIGNAL IT OWNS (thread 1e07af65, obligation a14f1528's own live finding):
    a killed process's `agent_mounts` row used to survive the kill untouched, reading as
    live to `_launch_twin_check`'s `is_live()` for a full LIVENESS_WINDOW_MINUTES (15) —
    stop made a signal it never cleaned up, so an immediate resume()/launch() right after
    a genuine stop saw a body that was already gone. SUSPENDS, never deletes (#178's own
    law — `mounts.release_session_mounts`, the epoch sentinel), every row this LINEAGE'S
    OWN sessions could be filed under, not just the matched one — the same "any
    generation, not just today's exact label" widening the lineage-fallback match above
    already needed, for the identical staleness reason.

    MATCHES BY LINEAGE TOO, NOT ONLY BY EXACT HOLDER ID (obligation 2e110f63, the same
    branch `_launch_twin_check` already carries): `registry_census`'s own `matched` list
    reconciles a live body to whatever `agent_mounts.agent_id` its job_dir cache last
    recorded — a CACHE that can lag the seat's current holder generation (Jesus's own
    live shape: the seat succeeded, the live body's own mount row hadn't caught up yet).
    A holder that misses THAT check falls back to the graph directly: any of this
    lineage's own `succession_chain` sessions, /proc-verified live in the census, is the
    same body wearing a different label — never declared `no-live-body` just because the
    cache is stale."""
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
    elif caller == _OPERATOR_CALLER:
        # THE OPERATOR'S OWN HAND (Thoth LXXXVII, 2026-08-28, operator's instruction "make
        # sure to also build the other end of stopping and cleaning up"). DOWNWARD-ONLY
        # exists to stop AGENTS reaching sideways or upward at each other; the human at a
        # terminal is not a peer in that chart, he is above all of it — and `osiris launch`
        # has had a terminal door since #72 while stop had none, so a body could be started
        # from the shell and never ended from it. That asymmetry is the whole defect.
        #
        # NOTHING ELSE IS RELAXED: the seat must resolve, it must have a real holder, and
        # the body must be /proc-CONFIRMED by the same registry_census every other door
        # reads. This skips ONE check — the managed_by edge — and says so in the receipt
        # (`authority: "operator"`), because an authority that is not visible in the record
        # is indistinguishable from a missing check.
        assert target is not None
        target_seat = await _seat_for_target(actions, target)
        if target_seat is None:
            return {"status": "refused-no-seat",
                    "detail": f"'{target}' resolves to no living Seat — nothing to stop"}
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
    # THE LINEAGE'S OWN SESSIONS, ALWAYS COMPUTED (not just on a match miss): the match
    # fallback below needs it, and so does the release step after a successful kill — the
    # SAME "any generation, not just today's exact label" reasoning applies to both, since
    # a stale mount row can be filed under any ancestor generation's own session, not only
    # the current holder's.
    from src.orchestrator.succession import succession_chain
    sessions = {str(h["session"]) for h in await succession_chain(pool, holder)
                if h.get("session")}
    if match is None:
        # THE LINEAGE FALLBACK (obligation 2e110f63, the same branch _launch_twin_check
        # already carries): `matched` reconciles by job_dir -> agent_mounts.agent_id, a
        # CACHE that can lag the seat's own current holder generation (Jesus's own live
        # shape) — a body genuinely IS this lineage's own, /proc-confirmed right now,
        # just filed under a stale label the exact-equality check above never finds. Ask
        # the graph directly: any of this lineage's own succession_chain sessions,
        # verified live in the census, is the same body under a different name.
        if sessions:
            match = next(
                (v for v in census.get("verified", [])
                 if any(str(v.get("session_id") or "").startswith(sid) for sid in sessions)),
                None)
    pid = match.get("pid") if match else None
    job_dir_key = match.get("job_dir_key") if match else None
    if not isinstance(pid, int):
        return {"status": "no-live-body", "seat": target_seat, "holder": holder,
                "detail": f"{holder} carries no harness/proc-confirmed live body right "
                          "now — nothing to signal (already dead, or never really live)"}
    try:
        await kill(pid, job_dir_key)
    except ProcessLookupError:
        return {"status": "no-live-body", "seat": target_seat, "holder": holder, "pid": pid,
                "detail": "the process was already gone by the time the signal landed"}
    except OSError as exc:
        return {"status": "refused-signal", "seat": target_seat, "holder": holder, "pid": pid,
                "detail": f"the stop signal itself failed ({exc}) — nothing else changed"}

    # RELEASE THE SIGNAL STOP ITSELF OWNS (thread 1e07af65): suspend (never delete, #178's
    # own law) every agent_mounts row this lineage's own sessions could be filed under —
    # not just the matched one — so `_launch_twin_check`'s is_live() stops reading a dead
    # body for LIVENESS_WINDOW_MINUTES after a genuine, successful kill. Every session's
    # own job_dir is derived the same way SessionEnd's own handler already does
    # (handshake._derive_job_dir, NEVER the census's short job_dir_key — that's sid[:8]
    # alone, not the real agent_mounts.job_dir path, and would never match by job_dir at
    # all) — release_session_mounts's own `job_dir=$1 OR session_key=$2` clause still
    # finds the right row even when the derived job_dir itself is stale or wrong, since
    # the session_key match alone suffices.
    from src.orchestrator.handshake import _derive_job_dir
    from src.orchestrator.mounts import release_session_mounts
    matched_sid = str((match or {}).get("session_id") or "")
    release_ids = set(sessions) | ({matched_sid} if matched_sid else set())
    for sid in release_ids:
        jd = _derive_job_dir(sid)
        if jd:
            await release_session_mounts(pool, job_dir=jd, session_id=sid)

    now = datetime.now(UTC)
    oid = await actions.create_or_find_object("Seat", target_seat, caller)
    await actions.assert_property(oid, "stopped_at", now.isoformat(), caller, now, 0.9,
                                  evidence_class="self_declared")
    if reason:
        await actions.assert_property(oid, "stopped_reason", reason[:500], caller, now, 0.9,
                                      evidence_class="self_declared")
    return {"status": "stopped", "seat": target_seat, "holder": holder, "pid": pid,
            "by": caller, "authority": ("self" if self_stop else
                                        "operator" if caller == _OPERATOR_CALLER
                                        else "manager"),
            **({"reason": reason} if reason else {}),
            "released_mounts": len(release_ids),
            "detail": f"stop signal sent to {holder}'s live body (pid {pid}); "
                      f"{len(release_ids)} lineage mount row(s) suspended — reachability "
                      "afterward is governed entirely by the SAME occupancy authority "
                      "launch()/resume() already consult, and its own agent_mounts "
                      "signal no longer lags this stop"}


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
    fresh = await asyncio.to_thread(
        _turn_fresh_sync, root, resume[0], st.osiris_dm_active_secs, resume[3])
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
    spawner pre-authorizes the graph tools, or the wake is born with its hands tied.

    THE BACKSTOP (decision 27259e4d, thread bc11a2d3): `repo` becomes `cwd=` on the
    subprocess call below with no existence check of its own — `osiris launch`'s own CLI
    caller now refuses loudly, by name, before ever reaching this function, but every OTHER
    caller (a daemon wake, a triage trigger) resolves its own `repo` and has no such
    pre-check. Same fire-and-forget discipline the receipt-open failure just above already
    follows: a bad `repo` here degrades to a logged no-op, never a raised, uncaught
    FileNotFoundError out of create_subprocess_exec — this function's own callers already
    assume it never raises for a wake attempt, same as they assume a full disk or an
    unwritable receipt directory doesn't crash them either."""
    if not _tree_exists(repo):
        _log.error("trigger: _spawn_claude refused — repo=%r does not exist on disk, "
                   "no subprocess spawned (job_dir=%s, resume_session=%s)",
                   repo, job_dir, resume_session)
        return
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

async def _spawn_claude_bg(
    repo: str, *, name: str | None = None, model: str | None = None,
    prompt: str | None = None, allowed_tools: str | None = None,
    resume_session: str | None = None,
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
    is worse than one that refuses cleanly on the old ones.

    SAME BACKSTOP AS `_spawn_claude` (decision 27259e4d, thread bc11a2d3): `repo` reaches
    create_subprocess_exec's `cwd=` with no existence check of its own; refused here as a
    logged no-op, never a raised, uncaught FileNotFoundError.

    `resume_session` (Thoth dispatch 6484/6515, the operator's own "impossible to actually
    resume agents" complaint): #173's premise — "claude --bg silently ignores --resume" —
    was TRUE on the harness version it was measured against and is FALSE now, confirmed
    live against the currently installed harness (2.1.258): `claude --bg --resume
    <session-id> <prompt>` genuinely continues that session under the SAME background id,
    writes to the SAME transcript, and reappears in `claude agents --json` afterward —
    verified with a real disposable probe session before this code was written, not
    inferred from `--help` text alone. `osiris resume` now rides this instead of the old
    one-shot `-p --resume` lane, closing the exact gap Jesus's specimen showed: a resumed
    session that ran its turn and then vanished from the operator's own roster."""
    if not _tree_exists(repo):
        _log.error("trigger: _spawn_claude_bg refused — repo=%r does not exist on disk, "
                   "no subprocess spawned (name=%s)", repo, name)
        return
    env = os.environ.copy()
    # same anchor discipline as _spawn_claude: a spawner's own anchor must never leak into
    # the child (the anchor-collision class, 2294e95d) — inert for --bg today (env vars
    # don't reach the claimed spare either way) but cheap and harmless to keep scrubbed.
    env.pop("CLAUDE_JOB_DIR", None)
    cmd = ["claude", "--bg"]
    if resume_session:
        cmd += ["--resume", resume_session]
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
