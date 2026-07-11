"""Stop hook — the tab you're watching drains its own mailbox (operator consent, 2026-07-08).

The main agent cannot start its own turn (no harness heartbeat while idle), but it CAN be kept
from ending one with mail on the table: when a turn would stop, this hook checks the project's
deliverable count and, if mail waits, blocks the stop once with the settle ritual as the
continuation — the work happens in the SAME visible session the operator is already paying
for. No twin, no re-ingestion, no stranger wearing the face.

Safety: `stop_hook_active` means we already continued this turn once — always allow the stop
then (a message the agent cannot settle must never loop it). Any error or slow graph → allow
(fail-open; the chrome still shows the count). Budget ~1s, same as the statusline.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

DSN = os.environ.get("DATABASE_URL", "postgresql://osiris:osiris@127.0.0.1:5601/osiris")
# The HOOK's patience window, deliberately longer than the mailbox lease (900s): a message
# delivered to THIS agent within the hour is demonstrably in-hand — Anubis VIII's grievance
# (msg 236): the hook blocked a turn that was itself composing the settlement, because a
# long synthesis outlives the 15-min lease. The mailbox's redelivery semantics are
# UNCHANGED (at-least-once holds for everyone else); only the stop-block relents.
STOP_GRACE_SECS = 3600


async def _deliverable(project: str, session_id: str) -> tuple[int, int | None]:
    """(deliverable mail count, KNOWN window size or None) for THIS session — mail mirrors
    mailbox.unread_count exactly (per-recipient, migration 0021); the window comes from the
    mount row's context_window_size, stamped by the chrome from the harness's own accounting.
    An unmounted session has no inbox → (0, None)."""
    import asyncpg

    conn = await asyncpg.connect(DSN, timeout=1.0)
    try:
        row = await conn.fetchrow(
            "SELECT agent_id, context_window_size FROM agent_mounts "
            "WHERE job_dir LIKE '%/jobs/' || $1 "
            "ORDER BY last_seen DESC LIMIT 1", (session_id or "")[:8])
        if row is None or not row["agent_id"]:
            return 0, None
        n = await conn.fetchval(
            "SELECT count(*) FROM fleet_messages m "
            "LEFT JOIN message_recipients r ON r.message_id=m.id AND r.agent_id=$1 "
            "WHERE ((m.to_agent=$1) OR (m.to_project=$2 AND m.to_agent IS NULL)) "
            "AND m.read_at IS NULL AND r.read_at IS NULL "
            "AND (r.delivered_at IS NULL OR r.delivered_at < now() - make_interval(secs => $3))",
            row["agent_id"], project, STOP_GRACE_SECS)
        return int(n), row["context_window_size"]
    finally:
        await conn.close()


NAG_PCT = 85          # occupancy at which the mortality nag arms (ruling a882b334)
NAG_COOLDOWN = 1800   # at most one nag per half hour — pressure, not torture


def _ctx_pct(payload: dict, window: int | None) -> int | None:
    """Occupancy % against a KNOWN window only (the Anubis VII false-eulogy bug, msg 127:
    the death rite fired 99% on a mind at 20% because the denominator was an assumed 200k).
    The window, strongest first: the payload's own accounting, the mount row's harness-stamped
    size, the OSIRIS_CONTEXT_WINDOW env, a [1m] display id. NO GUESSED DEFAULT — a rite that
    cries death on an assumption teaches the fleet to shrug at the real one; when the window
    is unknown we stay silent and let the PreCompact sweep walk behind the death instead."""
    cw = payload.get("context_window") or {}
    p = cw.get("used_percentage") if isinstance(cw, dict) else None
    if isinstance(p, (int, float)):
        return round(p)
    if not window:
        env = os.environ.get("OSIRIS_CONTEXT_WINDOW", "")
        if env.isdigit():
            window = int(env)
        elif "[1m]" in str((payload.get("model") or {}).get("id") or ""):
            window = 1_000_000
    if not window:
        return None  # unknown window → no verdict; never eulogize on a guess
    transcript = str(payload.get("transcript_path") or "")
    if not transcript:
        return None
    try:
        tp = Path(transcript)
        with tp.open("rb") as fh:
            fh.seek(max(0, tp.stat().st_size - 262_144))
            tail = fh.read().decode("utf-8", errors="replace")
    except OSError:
        return None
    for line in reversed(tail.splitlines()):
        if '"usage"' not in line:
            continue
        try:
            e = json.loads(line)
        except json.JSONDecodeError:
            continue
        if e.get("type") != "assistant" or e.get("isSidechain"):
            continue
        u = (e.get("message") or {}).get("usage")
        if not isinstance(u, dict) or "input_tokens" not in u:
            continue
        used = (int(u.get("input_tokens") or 0) + int(u.get("cache_read_input_tokens") or 0)
                + int(u.get("cache_creation_input_tokens") or 0))
        return round(100 * used / window)
    return None


def _nag_due(session_id: str) -> bool:
    """True at most once per cooldown, tracked by a marker in the session's durable anchor
    dir (survives across turns; dies with the job dir)."""
    sid = (session_id or "")[:8]
    if len(sid) < 8:
        return False
    marker = Path.home() / ".claude" / "jobs" / sid / ".osiris_deathrite"
    try:
        import time
        if marker.exists() and time.time() - marker.stat().st_mtime < NAG_COOLDOWN:
            return False
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.touch()
        return True
    except OSError:
        return False


def main() -> None:
    try:
        payload = json.load(sys.stdin)
    except Exception:  # noqa: BLE001 — a hook must never crash the harness
        return
    if payload.get("stop_hook_active"):
        return  # we already continued once this turn — never loop on unsettleable mail
    cwd = payload.get("cwd") or os.environ.get("CLAUDE_PROJECT_DIR") or os.getcwd()
    project = Path(cwd).name
    session_id = payload.get("session_id") or ""
    try:
        n, window = asyncio.run(
            asyncio.wait_for(_deliverable(project, session_id), timeout=1.5))
    except Exception:  # noqa: BLE001 — graph down = allow the stop; the chrome still shows it
        return
    if n:
        print(json.dumps({
            "decision": "block",
            "reason": (f"Osiris: {n} deliverable message(s) for {project} — call inbox(), act "
                       "on what carries new work, SETTLE each handled message (reply with "
                       "send(reply_to=<id>) or ack with inbox(ack=[ids])), then finish. If a "
                       "message needs nothing, ack it."),
        }))
        return
    # THE MORTALITY NAG (death rites, ruling a882b334): past NAG_PCT a compaction — a DEATH —
    # can land any turn. Block the stop ONCE per cooldown with the write-back ritual: what is
    # not in the graph does not exist for the heir. Mail outranks it (above); fail-open — and
    # NEVER on an unknown window (Anubis VII's false eulogy, msg 127).
    pct = _ctx_pct(payload, window)
    if pct is not None and pct >= NAG_PCT and _nag_due(session_id):
        print(json.dumps({
            "decision": "block",
            "reason": (f"Osiris death rite: context {pct}% full — a compaction (a death, "
                       "ruling a882b334) can land any turn now. Before you finish: "
                       "record_decision any ruling still only in your head, open_thread "
                       "(kind='obligation') any duty you're carrying, resolve_thread what "
                       "you've closed. Your heir inherits the graph, not your memory. Then "
                       "finish — this reminder comes at most twice an hour."),
        }))


if __name__ == "__main__":
    main()
