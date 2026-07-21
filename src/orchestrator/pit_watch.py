"""THE PAIR HEARTBEAT — Pit Watch Stage B (thread 449bf55d, decision be79f567; design brief
DM 974, amended by DM 975). Manager/worker pairs must never stall silently: an ask-graded DM
addressed to either party sits unread past the mail lease window while its addressee is
provably not mid-turn -> an alarm lands in pit_watch_alarms; after enough consecutive
sightings, ONE brief reaches the operator's desk naming the pair and the stuck message, and a
tombstone stops it from ever firing twice for the same message.

Deliberately does NOT dispatch or spawn anything (DM 975: "do not build a wake mechanism of
your own... alarm and escalate to the desk, which is honest and sufficient" — until a clean
managed_by-gated wake() verb exists, that IS this mechanism's whole action). A message that
gets read simply stops appearing in the stuck-query on the next pass — no separate 'resolved'
tombstone is needed, only 'escalated' stops the count (the same idiom agent_wakes already uses
for its own 'abandoned' tombstone).

THE NO-CANDIDATES CASE (DM 975's one required amendment): an addressee with ZERO live session
candidates reads as NOT MID-TURN, never as 'cannot determine, skip' — that is literally this
morning's own incident (running but never mounted, an ask-graded assignment sitting unseen),
and a watch that goes silent on an empty candidate set has rebuilt the pit inside the pit
detector.
"""
from __future__ import annotations

import asyncio
import contextlib
from pathlib import Path
from typing import Any

import asyncpg

from src.config.settings import Settings, get_settings

_PIT_WATCH_AGENT = "agent:osiris-pit-watch"


async def _managed_pairs(pool: asyncpg.Pool) -> list[tuple[str, str]]:
    """Every active (worker_seat, manager_seat) pair, one pass — no per-pair query. Mirrors
    osiris_stophook._manager_seat's single-direction query, generalized to enumerate all of
    them (the managed_by edge is asserted once at mint/adopt and never rewritten, so this is
    cheap to re-derive every tick rather than cache)."""
    rows = await pool.fetch(
        "SELECT f.canonical AS worker_seat, t.canonical AS manager_seat "
        "FROM links l "
        "JOIN objects f ON f.id=l.from_id AND f.type='Seat' AND f.status='active' "
        "JOIN objects t ON t.id=l.to_id AND t.type='Seat' AND t.status='active' "
        "WHERE l.type='managed_by' AND (l.valid_until IS NULL OR l.valid_until > now())")
    return [(str(r["worker_seat"]), str(r["manager_seat"])) for r in rows]


async def _oldest_stuck_dm(
    pool: asyncpg.Pool, *, addressee_seat: str, lease_secs: int,
) -> dict[str, Any] | None:
    """The oldest unread ask-graded DM addressed to this seat, past the lease window, or
    None. A seat address has ONE inbox regardless of who currently holds it, so this only
    needs whether anyone has settled it yet (read_at) — not mailbox.py's full per-reader
    lease bookkeeping, which exists for broadcasts with many independent readers."""
    row = await pool.fetchrow(
        "SELECT id, from_agent, created_at, "
        " extract(epoch FROM (now() - created_at)) AS age_secs "
        "FROM fleet_messages "
        "WHERE to_agent=$1 AND grade='ask' AND read_at IS NULL "
        "AND created_at < now() - make_interval(secs => $2) "
        "ORDER BY created_at ASC LIMIT 1", addressee_seat, lease_secs)
    return dict(row) if row is not None else None


async def _live_session_candidates(
    pool: asyncpg.Pool, agent_id: str, root: Path,
) -> list[str]:
    """Full session ids worth checking for mid-turn liveness — every mount this agent's
    LINEAGE holds, freshest first, resolved to its actual transcript stem (a job_dir alone
    only names the anchor; locate_current_transcript finds what it actually points at).
    Empty is a real, common, EXPECTED answer (an agent that has never mounted at all) —
    callers must treat it as 'not mid-turn', never as 'unknown, skip'."""
    from src.ingest.sessions import locate_current_transcript
    from src.orchestrator.agents import _generation

    base = _generation(agent_id)[0]
    rows = await pool.fetch(
        "SELECT job_dir FROM agent_mounts WHERE (agent_id=$1 OR agent_id LIKE $1 || '-%') "
        "AND job_dir IS NOT NULL ORDER BY last_seen DESC LIMIT 5", base)
    sids: list[str] = []
    for r in rows:
        t = await asyncio.to_thread(
            locate_current_transcript, root, r["job_dir"], anchored_only=True)
        if t is not None:
            sids.append(t.stem)
    return sids


async def _addressee_mid_turn(
    pool: asyncpg.Pool, addressee_agent: str, *, active_secs: int, sessions_root: Path,
) -> bool:
    """THE ONE LIVENESS WITNESS: _turn_fresh_sync (trigger.py) reads a session's own
    transcript tail for a moving timestamp — no lock file, no last_seen (the fleet killed
    that superstition twice: the statusline-heartbeat confound and the Aegis phantom mtime).
    An addressee with NO session candidates at all is not an unknown to fail open on — it is
    this morning's own incident (running but never mounted), and it must read as not-mid-turn."""
    from src.orchestrator.trigger import _turn_fresh_sync

    candidates = await _live_session_candidates(pool, addressee_agent, sessions_root)
    if not candidates:
        return False  # NOT a skip — the exact shape this watch exists to catch
    for sid in candidates:
        if await asyncio.to_thread(_turn_fresh_sync, sessions_root, sid, active_secs):
            return True
    return False


async def _attempts_on(pool: asyncpg.Pool, message_id: int) -> int:
    """Consecutive sightings for this message — excluded once it carries the 'escalated'
    tombstone, the same shape agent_wakes uses for its own 'mode <> abandoned' count."""
    n = await pool.fetchval(
        "SELECT count(*) FROM pit_watch_alarms WHERE message_id=$1 AND outcome='sighted' "
        "AND NOT EXISTS (SELECT 1 FROM pit_watch_alarms e WHERE e.message_id=$1 "
        "AND e.outcome='escalated')", message_id)
    return int(n or 0)


async def _already_escalated(pool: asyncpg.Pool, message_id: int) -> bool:
    return bool(await pool.fetchval(
        "SELECT 1 FROM pit_watch_alarms WHERE message_id=$1 AND outcome='escalated'",
        message_id))


async def _sight(
    pool: asyncpg.Pool, *, worker_seat: str, manager_seat: str, message_id: int,
    addressee_seat: str,
) -> None:
    await pool.execute(
        "INSERT INTO pit_watch_alarms (worker_seat, manager_seat, message_id, "
        "addressee_seat, outcome) VALUES ($1,$2,$3,$4,'sighted')",
        worker_seat, manager_seat, message_id, addressee_seat)


async def _escalate(
    pool: asyncpg.Pool, *, worker_seat: str, manager_seat: str, message_id: int,
    addressee_seat: str, age_secs: float, attempts: int,
) -> None:
    """ONE brief, never a second: the tombstone lands BEFORE the send, so a failed brief
    (desk unreachable, a network hiccup) never re-arms and floods the next tick — a failed
    brief costs a missed escalation, never a repeated one (mirrors trigger._abandon's own
    order: write the tombstone, then attempt the courtesy)."""
    await pool.execute(
        "INSERT INTO pit_watch_alarms (worker_seat, manager_seat, message_id, "
        "addressee_seat, outcome) VALUES ($1,$2,$3,$4,'escalated')",
        worker_seat, manager_seat, message_id, addressee_seat)
    age_min = round(age_secs / 60)
    body = (
        f"STALLED PAIR — {worker_seat} <-> {manager_seat}: message {message_id}, addressed "
        f"to {addressee_seat}, has sat unread for {age_min} minute(s) while {addressee_seat} "
        f"is not mid-turn. Sighted {attempts} consecutive time(s); no further alarm will "
        f"fire for this message. inbox() reads it, or wake the addressee by hand — nothing "
        f"is lost, nothing was deleted."
    )
    from src.orchestrator.mailbox import OPERATOR_ADDR, send_message
    with contextlib.suppress(Exception):  # a failed brief must never crash the tick
        await send_message(pool, from_agent=_PIT_WATCH_AGENT, from_project="osiris",
                           to_project=OPERATOR_ADDR, body=body, desk_kind="hands")


async def _watch_one_direction(
    pool: asyncpg.Pool, *, worker_seat: str, manager_seat: str, addressee_seat: str,
    lease_secs: int, active_secs: int, escalate_at: int, sessions_root: Path,
) -> str | None:
    """One (pair, direction)'s worth of the tick. Returns the outcome ('sighted' |
    'escalated') or None when nothing here is stuck."""
    from src.orchestrator.seats import seat_receipt

    stuck = await _oldest_stuck_dm(pool, addressee_seat=addressee_seat, lease_secs=lease_secs)
    if stuck is None:
        return None
    receipt = await seat_receipt(pool, addressee_seat)
    holder = receipt.get("holder") if receipt else None
    if holder is None:
        return None  # a vacant seat has nobody to be mid-turn or not — nothing to alarm yet
    if await _addressee_mid_turn(
        pool, holder, active_secs=active_secs, sessions_root=sessions_root,
    ):
        return None  # genuinely mid-turn — its own turn's end surfaces the mail; not stuck
    if await _already_escalated(pool, stuck["id"]):
        return None  # already told the desk once; never again for this message
    attempts = await _attempts_on(pool, stuck["id"]) + 1
    if attempts >= escalate_at:
        await _escalate(
            pool, worker_seat=worker_seat, manager_seat=manager_seat,
            message_id=stuck["id"], addressee_seat=addressee_seat,
            age_secs=float(stuck["age_secs"] or 0), attempts=attempts)
        return "escalated"
    await _sight(
        pool, worker_seat=worker_seat, manager_seat=manager_seat,
        message_id=stuck["id"], addressee_seat=addressee_seat)
    return "sighted"


async def pit_watch_tick(actions: Any, *, settings: Settings | None = None) -> dict[str, int]:
    """One pass over every managed_by pair, both directions. Returns {pairs, sighted,
    escalated} for the cron wrapper to log. A no-op (report all zeros) unless
    osiris_pit_watch_enabled — the kill switch, same law as osiris_trigger_enabled: a
    mechanism that pages the operator earns its own, never inherits one. Every per-direction
    failure is caught and skipped — one broken pair must never blind the watch on every
    other one."""
    st = settings or get_settings()
    if not st.osiris_pit_watch_enabled:
        return {"pairs": 0, "sighted": 0, "escalated": 0}
    pool = actions.pool
    sessions_root = (Path(st.osiris_sense_sessions) if st.osiris_sense_sessions
                     else Path.home() / ".claude" / "projects")
    pairs = await _managed_pairs(pool)
    sighted = escalated = 0
    for worker_seat, manager_seat in pairs:
        for addressee_seat in (worker_seat, manager_seat):
            try:
                outcome = await _watch_one_direction(
                    pool, worker_seat=worker_seat, manager_seat=manager_seat,
                    addressee_seat=addressee_seat, lease_secs=st.osiris_mail_lease_secs,
                    active_secs=st.osiris_dm_active_secs,
                    escalate_at=st.osiris_pit_watch_escalate_at, sessions_root=sessions_root)
            except Exception:  # noqa: BLE001 — one pair's failure must never blind the rest
                continue
            if outcome == "sighted":
                sighted += 1
            elif outcome == "escalated":
                escalated += 1
    return {"pairs": len(pairs), "sighted": sighted, "escalated": escalated}
