#!/usr/bin/env python3
"""SubagentStart / SubagentStop hook — the harness announces every spawn to the graph.

A sub-agent inherits its parent's $CLAUDE_JOB_DIR and MCP connection, so the graph used to
learn about spawns only from the miner's 10-minute disk round — the operator (and the parent
agent) met them by surprise. This posts the harness's own record ({agent_id, agent_type},
plus the child's transcript path at Stop) to the MCP server's /spawn the moment the child
starts or finishes: the child exists in the graph, spawned_by the session's seat, while it
is still running.

STDLIB ONLY and FAIL-OPEN: any failure exits 0 silently — a spawn must never be slowed or
broken by its own announcement. Timeout 2s.
"""
from __future__ import annotations

import json
import sys
import urllib.request

SPAWN = "http://127.0.0.1:8790/spawn"


def main() -> int:
    try:
        hook = json.load(sys.stdin)
    except Exception:  # noqa: BLE001
        return 0
    agent_id = str(hook.get("agent_id") or "")
    if not agent_id:
        return 0
    body = {
        "session_id": str(hook.get("session_id") or ""),
        "agent_id": agent_id,
        "agent_type": str(hook.get("agent_type") or ""),
        "phase": "stop" if hook.get("hook_event_name") == "SubagentStop" else "start",
    }
    tp = str(hook.get("agent_transcript_path") or "")
    if tp:
        body["agent_transcript_path"] = tp
    try:
        req = urllib.request.Request(
            SPAWN, data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"}, method="POST")
        urllib.request.urlopen(req, timeout=2)
    except Exception:  # noqa: BLE001 — server down: the miner's round still catches the spawn
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
