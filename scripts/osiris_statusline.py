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
LEASE_SECS = 900  # mirror osiris_mail_lease_secs — deliverable = unsettled + no live lease

DIM = "\033[2m"
RED = "\033[31m"
GREEN = "\033[32m"
RESET = "\033[0m"


def _short(model_id: str) -> str:
    return model_id.removeprefix("claude-")


async def _counts(project: str) -> tuple[int, int, int, int]:
    import asyncpg

    conn = await asyncpg.connect(DSN, timeout=1.0)
    try:
        row = await conn.fetchrow(
            "SELECT "
            " (SELECT count(*) FROM fleet_messages WHERE to_project='operator' "
            "   AND read_at IS NULL AND (delivered_at IS NULL "
            "   OR delivered_at < now() - make_interval(secs => $2))) AS desk, "
            " (SELECT count(*) FROM fleet_messages WHERE to_project=$1 "
            "   AND read_at IS NULL AND (delivered_at IS NULL "
            "   OR delivered_at < now() - make_interval(secs => $2))) AS mail, "
            " (SELECT count(*) FROM agent_mounts "
            "   WHERE last_seen > now() - interval '15 minutes') AS live, "
            " (SELECT count(*) FROM agent_wakes "
            "   WHERE woke_at > now() - interval '1 hour') AS wakes",
            project, LEASE_SECS)
        return row["desk"], row["mail"], row["live"], row["wakes"]
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

    try:
        desk, mail, live, wakes = asyncio.run(asyncio.wait_for(_counts(project), timeout=1.5))
        desk_s = f"{RED}desk {desk}{RESET}" if desk else f"{DIM}desk 0{RESET}"
        mail_s = f"mail {mail}" if mail else f"{DIM}mail 0{RESET}"
        parts = [f"◈ {project}", desk_s, mail_s, f"fleet {live}●", f"wakes {wakes}/h"]
    except Exception:  # noqa: BLE001 — the graph being down is information, not an error
        parts = [f"◈ {project}", f"{DIM}graph unreachable{RESET}"]

    if model_id and model_id != EXPECTED:  # the swap confession, ambient — every single turn
        parts.append(f"{RED}⚠ {_short(model_id)} (intent: {_short(EXPECTED)}){RESET}")
    elif model_id:
        parts.append(f"{GREEN}{_short(model_id)}{RESET}")

    print(f" {DIM}│{RESET} ".join(parts))


if __name__ == "__main__":
    main()
