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
LEASE_SECS = 900  # mirror osiris_mail_lease_secs — deliverable = unsettled + no live lease


async def _deliverable(project: str, session_id: str) -> int:
    """Deliverable mail for THIS session's agent — mirrors mailbox.unread_count exactly (the
    per-recipient model, migration 0021): broadcasts to its project + DMs to it, unsettled by
    IT and not under ITS live lease, honoring the legacy per-message settle. The agent id is
    resolved from the session (its durable mount row); an unmounted session has no inbox → 0."""
    import asyncpg

    conn = await asyncpg.connect(DSN, timeout=1.0)
    try:
        agent = await conn.fetchval(
            "SELECT agent_id FROM agent_mounts WHERE job_dir LIKE '%/jobs/' || $1 "
            "ORDER BY last_seen DESC LIMIT 1", (session_id or "")[:8])
        if not agent:
            return 0
        return await conn.fetchval(  # type: ignore[no-any-return]
            "SELECT count(*) FROM fleet_messages m "
            "LEFT JOIN message_recipients r ON r.message_id=m.id AND r.agent_id=$1 "
            "WHERE ((m.to_agent=$1) OR (m.to_project=$2 AND m.to_agent IS NULL)) "
            "AND m.read_at IS NULL AND r.read_at IS NULL "
            "AND (r.delivered_at IS NULL OR r.delivered_at < now() - make_interval(secs => $3))",
            agent, project, LEASE_SECS)
    finally:
        await conn.close()


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
        n = asyncio.run(asyncio.wait_for(_deliverable(project, session_id), timeout=1.5))
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


if __name__ == "__main__":
    main()
