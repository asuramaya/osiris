#!/usr/bin/env python3
"""PreToolUse hook — FORCE the durable anchor + STAMP the true caller (threads 883a24f4,
0344e536).

Two jobs, both zero-compliance (the harness's own record, never the agent's claim):

1. THE ANCHOR (mount only): inject job_dir derived from the session id off the hook's stdin
   JSON, so every mount carries a durable, reconnect-stable anchor. A FOREIGN anchor the
   agent supplied deliberately (a seat claim) is respected — the session's own dir travels
   as session_anchor so the server can BIND session → seat (thread 33838160).

2. THE SPAWN STAMP (every osiris tool that writes or orients): inside a sidechain the hook
   payload carries `agent_id`/`agent_type` — present ONLY for sub-agent calls. Stamp them
   into the tool input so the server attributes the call to the CHILD (spawned_by its
   parent), never to the seat: a spawn shares its parent's $CLAUDE_JOB_DIR and MCP
   connection, and without the stamp it was greeted as the seat itself, full authority
   (live repro, 2026-07-10). In MAIN-session calls the same keys are STRIPPED — an agent
   cannot masquerade DOWN as a spawn either. The stamp travels only to tools whose schema
   accepts it (SPAWN_AWARE); other tools pass untouched.

Hooks run BEFORE the permission/classifier gates and are never rejected by them. This one
only TIGHTENS. Fail-open: any glitch → emit nothing → the call proceeds unmodified.

Install (PreToolUse, matcher mcp__osiris__.*) via onboard.py --anchor; operator-gated.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

# tools whose server signature accepts the spawn stamp — keep in lockstep with mcp_server
SPAWN_AWARE = {
    "mcp__osiris__mount", "mcp__osiris__orient", "mcp__osiris__record_decision",
    "mcp__osiris__open_thread", "mcp__osiris__resolve_thread", "mcp__osiris__hold_tension",
    "mcp__osiris__send", "mcp__osiris__inbox", "mcp__osiris__ingest_reference",
    "mcp__osiris__reclassify_thread",
}


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except Exception:  # noqa: BLE001 — never block a tool call on a hook glitch
        return 0
    tool = str(payload.get("tool_name") or "")
    if not tool.startswith("mcp__osiris__"):
        return 0
    ti = dict(payload.get("tool_input") or {})
    changed = False
    child = str(payload.get("agent_id") or "")

    if tool in SPAWN_AWARE:
        if child:  # SIDECHAIN: stamp the true caller off the harness's own payload
            ti["subagent_id"] = child
            agent_type = str(payload.get("agent_type") or "")
            if agent_type:
                ti["subagent_type"] = agent_type
            if tool == "mcp__osiris__mount":
                ti.pop("job_dir", None)  # a spawn gets no seat anchor
                ti.pop("session_anchor", None)
                tp = str(payload.get("transcript_path") or "")
                if tp.endswith(".jsonl"):  # the child's OWN transcript — the model probe
                    handle = child.removeprefix("agent-")
                    ti["subagent_transcript"] = f"{tp[:-6]}/subagents/agent-{handle}.jsonl"
            changed = True
        else:  # MAIN session: nobody masquerades DOWN as a spawn
            for key in ("subagent_id", "subagent_type", "subagent_transcript"):
                if key in ti:
                    ti.pop(key)
                    changed = True

    if tool == "mcp__osiris__mount" and not child:
        sid = str(payload.get("session_id") or "")
        if len(sid) >= 8:
            derived = str(Path.home() / ".claude" / "jobs" / sid[:8])
            # respect an explicit, valid anchor the agent already supplied; only fill a
            # missing/empty/unexpanded one (the '$CLAUDE_JOB_DIR'-literal case)
            existing = str(ti.get("job_dir") or "")
            if existing.startswith("/") and "$" not in existing:
                if existing.rstrip("/") != derived:
                    # a FOREIGN anchor — a mind deliberately wearing a seat. Hand the server
                    # the session's own dir too, so it can BIND session → seat (33838160).
                    ti["session_anchor"] = derived
                    changed = True
            else:
                ti["job_dir"] = derived
                changed = True

    if changed:
        print(json.dumps({"hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "allow",
            "updatedInput": ti,
        }}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
