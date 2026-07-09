"""The membrane in the window chrome — Osiris's statusline for Claude Code (Lane A, 9d2aaf4d).

The operator lives in the terminal; Osiris's upward streams lived behind an agent-keyhole or
:8011. This renders them AMBIENT: one line, every turn, in the window itself — the operator's
desk (unread briefs), this project's mailbox, the fleet's mounted-live count, the wake chain's
last hour, and a LIVE model-identity check (the swap confession moved into the chrome, where a
demotion is visible the turn it happens — ruling f2ae6346's banner, made permanent).

Claude Code pipes session JSON on stdin; we print one line and exit. HARD BUDGET: this runs
per render, so one connection, one query, ~1s timeout, and ANY failure degrades to a quiet
minimal line — the statusline must never block or break the window it serves.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

DSN = os.environ.get("DATABASE_URL", "postgresql://osiris:osiris@127.0.0.1:5601/osiris")
EXPECTED = os.environ.get("OSIRIS_EXPECTED_MODEL", "claude-fable-5")
CONSOLE = os.environ.get("OSIRIS_CONSOLE_URL", "http://127.0.0.1:8011")
SUCCESSION = os.environ.get("OSIRIS_SUCCESSION_URL", "http://127.0.0.1:8790/succession")
LINKS = os.environ.get("OSIRIS_STATUSLINE_LINKS", "1") != "0"  # kill switch if a terminal balks
LEASE_SECS = 900  # mirror osiris_mail_lease_secs — deliverable = unsettled + no live lease

DIM = "\033[2m"
RED = "\033[31m"
GREEN = "\033[32m"
AMBER = "\033[33m"
RESET = "\033[0m"


def _short(model_id: str) -> str:
    return model_id.removeprefix("claude-")


def _succession(session_id: str, model_id: str) -> str | None:
    """POST a live model seam to the server (ruling a882b334): the mind changed under this
    tab, so the seat passes NOW — the server mints the heir and moves the mount row. Returns
    the heir's agent id, or None on any failure (fail-open: the row kept the OLD model, so
    the very next render sees the same divergence and retries)."""
    import urllib.request
    try:
        req = urllib.request.Request(
            SUCCESSION, data=json.dumps({"session_id": session_id, "model": model_id}).encode(),
            headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=0.8) as resp:
            out = json.load(resp)
        return str(out["minted"]) if out.get("minted") else None
    except Exception:  # noqa: BLE001 — the chrome never blocks on its own sensor
        return None


def _link(text: str, anchor: str) -> str:
    """OSC 8 hyperlink into the /membrane lens — the statusline's click-through. Terminals
    without OSC 8 support render the plain text; the escapes are invisible either way."""
    if not LINKS:
        return text
    return f"\033]8;;{CONSOLE}/membrane#{anchor}\033\\{text}\033]8;;\033\\"


async def _counts(
    project: str, session_id: str, model_id: str = ""
) -> tuple[int, int, int, int, int]:
    import asyncpg

    conn = await asyncpg.connect(DSN, timeout=1.0)
    try:
        agent = None
        if session_id:
            # THE HEARTBEAT: a tab rendering its chrome is ALIVE — bump its registry row so
            # the wake dispatch never mints a twin beside a tab the operator is actively
            # driving (msg-78 lesson: 'live' must mean the tab, not the last osiris call).
            # The row's model is now succession-owned (ruling a882b334): first render STAMPS
            # it (COALESCE fills a NULL only); any later divergence is a live model seam —
            # the mind changed under this tab — and the SERVER mints the heir and moves the
            # row, so the chrome never overwrites the one signal that witnesses the seam.
            row0 = await conn.fetchrow(
                "UPDATE agent_mounts SET last_seen=now(), "
                "model=COALESCE(model, NULLIF($2,'')) "
                "WHERE job_dir LIKE '%/jobs/' || $1 RETURNING agent_id, model",
                session_id[:8], model_id)
            agent = row0["agent_id"] if row0 else None
            stored = row0["model"] if row0 else None
            if agent and model_id and stored and stored != model_id:
                agent = _succession(session_id, model_id) or agent
        agent = agent or ""
        # counts are PER-RECIPIENT now (migration 0021): desk = operator's own unread, mail =
        # THIS agent's broadcasts+DMs unread, flight = a SIBLING's live lease on shared broadcasts.
        row = await conn.fetchrow(
            "SELECT "
            " (SELECT count(*) FROM fleet_messages m WHERE m.to_project='operator' "
            "   AND m.to_agent IS NULL AND m.read_at IS NULL "
            "   AND NOT EXISTS(SELECT 1 FROM message_recipients r WHERE r.message_id=m.id "
            "     AND r.agent_id='operator' AND r.read_at IS NOT NULL) "
            "   AND NOT EXISTS(SELECT 1 FROM message_recipients r WHERE r.message_id=m.id "
            "     AND r.agent_id='operator' AND r.delivered_at >= now() "
            "       - make_interval(secs => $2))) AS desk, "
            " (SELECT count(*) FROM fleet_messages m LEFT JOIN message_recipients r "
            "   ON r.message_id=m.id AND r.agent_id=$3 "
            "   WHERE ((m.to_agent=$3) OR (m.to_project=$1 AND m.to_agent IS NULL)) "
            "   AND m.read_at IS NULL AND r.read_at IS NULL "
            "   AND (r.delivered_at IS NULL "
            "     OR r.delivered_at < now() - make_interval(secs => $2))) AS mail, "
            " (SELECT count(*) FROM fleet_messages m JOIN message_recipients r "
            "   ON r.message_id=m.id WHERE m.to_project=$1 AND m.to_agent IS NULL "
            "   AND r.agent_id <> $3 AND r.read_at IS NULL AND r.delivered_at IS NOT NULL "
            "   AND r.delivered_at >= now() - make_interval(secs => $2)) AS flight, "
            " (SELECT count(*) FROM agent_mounts "
            "   WHERE last_seen > now() - interval '15 minutes') AS live, "
            " (SELECT count(*) FROM agent_wakes "
            "   WHERE woke_at > now() - interval '1 hour') AS wakes",
            project, LEASE_SECS, agent)
        return row["desk"], row["mail"], row["flight"], row["live"], row["wakes"]
    finally:
        await conn.close()


def main() -> None:
    try:
        payload = json.load(sys.stdin)
    except Exception:  # noqa: BLE001 — chrome must render even on garbage input
        payload = {}
    ws = payload.get("workspace") or {}
    cwd = ws.get("current_dir") or ws.get("project_dir") or os.getcwd()
    project = Path(cwd).name
    model_id = str((payload.get("model") or {}).get("id") or "")
    session_id = str(payload.get("session_id") or "")

    try:
        desk, mail, flight, live, wakes = asyncio.run(
            asyncio.wait_for(_counts(project, session_id, model_id), timeout=1.5))
        desk_s = f"{RED}desk {desk}{RESET}" if desk else f"{DIM}desk 0{RESET}"
        # mail N(+M) — M = in flight: leased by another hand, thread moving (msg-78 lesson)
        flight_s = f"{AMBER}+{flight}{RESET}" if flight else ""
        mail_s = (f"mail {mail}{flight_s}" if (mail or flight)
                  else f"{DIM}mail 0{RESET}")
        parts = [
            _link(f"◈ {project}", "desk"),
            _link(desk_s, "desk"),
            _link(mail_s, "conversations"),
            _link(f"fleet {live}●", "fleet"),
            _link(f"wakes {wakes}/h", "wakes"),
        ]
    except Exception:  # noqa: BLE001 — the graph being down is information, not an error
        parts = [f"◈ {project}", f"{DIM}graph unreachable{RESET}"]

    if model_id and model_id != EXPECTED:  # the swap confession, ambient — every single turn
        parts.append(f"{RED}⚠ {_short(model_id)} (intent: {_short(EXPECTED)}){RESET}")
    elif model_id:
        parts.append(f"{GREEN}{_short(model_id)}{RESET}")

    print(f" {DIM}│{RESET} ".join(parts))


if __name__ == "__main__":
    main()
