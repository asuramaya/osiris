#!/usr/bin/env python3
"""The whisper — SessionStart hook (operator's blessing, 2026-07-08): every session on the
box wakes up already remembering Osiris.

Claude Code runs this at session start with hook JSON on stdin ({session_id, cwd, ...});
we post it to the MCP server's /automount (which mounts the session through the tested
mount path) and print ONE paragraph to stdout — SessionStart stdout is injected into the
agent's opening context, so the agent is a fleet member before its first token: it knows
its name, its project, its mail, and what happened while its lineage slept.

STDLIB ONLY (runs for every project on the box — no venv assumption) and FAIL-OPEN: any
failure prints a manual-mount hint (or nothing) and exits 0; a session must never be
slowed or broken by its own whisper. Timeout 3s total.
"""
from __future__ import annotations

import json
import sys
import urllib.request

AUTOMOUNT = "http://127.0.0.1:8790/automount"


def main() -> int:
    try:
        hook = json.load(sys.stdin)
    except Exception:  # noqa: BLE001
        return 0
    session_id = hook.get("session_id") or ""
    cwd = hook.get("cwd") or ""
    if not session_id or not cwd:
        return 0
    try:
        req = urllib.request.Request(
            AUTOMOUNT, data=json.dumps({"session_id": session_id, "cwd": cwd}).encode(),
            headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=3) as resp:
            out = json.load(resp)
    except Exception:  # noqa: BLE001 — server down: hint at the manual door, never block
        print("◈ OSIRIS (fleet memory) is configured but its server is unreachable right "
              "now. When your work touches shared knowledge, try the MCP tool "
              f"mount(cwd='{cwd}') — it may be back.")
        return 0
    if out.get("error"):
        print(f"◈ OSIRIS available — automount failed ({out['error']}); "
              f"call mount(cwd='{cwd}') by hand, then orient().")
        return 0

    bits = [f"◈ OSIRIS — the fleet's shared memory. You are ALREADY MOUNTED as "
            f"{out['agent']}" + (f" (project {out['project']}" if out.get("project") else "(")
            + (f", {out['model']})" if out.get("model") else ")")]
    if out.get("minted"):
        bits.append(f"You were MINTED as this lineage's successor — ancestor {out['minted']}; "
                    "your first act: read orient()'s succession note.")
    if out.get("swap"):
        bits.append(f"Model seam on your lineage: {out['swap']} — confess it to the operator "
                    "in your first reply.")
    mail = out.get("mail", 0)
    if mail:
        bits.append(f"Your project has {mail} unread fleet message{'s' if mail != 1 else ''} "
                    "→ inbox() (reading LEASES; settle by reply send(reply_to=<id>) or "
                    "inbox(ack=[ids])).")
    if out.get("away"):
        away = out["away"]
        n = len(away.get("threads") or [])
        who = ", ".join(away.get("acted_in_your_name") or [])
        bits.append("While your lineage slept: "
                    + (f"{who} acted in your project's name; " if who else "")
                    + (f"{n} conversation{'s' if n != 1 else ''} moved; " if n else "")
                    + "orient() shows the fold.")
    if out.get("pulse"):
        bits.append(f"Fleet pulse: {out['pulse']}.")
    if out.get("job_dir"):
        # the durable anchor: if the MCP server ever drops and you must re-mount, pass THIS
        # (your $CLAUDE_JOB_DIR is empty) so you re-attach to yourself instead of minting a twin.
        bits.append(f"YOUR DURABLE ANCHOR is job_dir='{out['job_dir']}'. If you ever need to "
                    f"mount again (e.g. after an MCP reconnect), call "
                    f"mount(cwd=..., job_dir='{out['job_dir']}') — NOT $CLAUDE_JOB_DIR (empty "
                    "here); that re-attaches you instead of splitting your identity.")
    bits.append("RITUAL: write back AS YOU GO — record_decision / open_thread "
                "(kind='obligation') / resolve_thread. A session can die at any instant; "
                "what is not in the graph does not exist. orient() for bearings.")
    print(" ".join(bits))
    return 0


if __name__ == "__main__":
    sys.exit(main())
