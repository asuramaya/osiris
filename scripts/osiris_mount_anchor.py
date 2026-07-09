#!/usr/bin/env python3
"""PreToolUse hook — FORCE the durable mount anchor (thread 883a24f4 hardening).

The whisper TELLS an agent its anchor; this ENFORCES it. On every mcp__osiris__mount call,
inject job_dir derived from the session id — read off the hook's stdin JSON (the id is NOT an
env var; docs confirmed CLAUDE_CODE_SESSION_ID is not exposed to hook processes) — so the mount
always carries a durable, reconnect-stable anchor with ZERO agent compliance. Two agents sharing
one working dir each get their OWN session id → OWN anchor → distinct identities, by construction
(the monsterhouse / handlingtheloop case). Fires on the mount CALL, so it covers an MCP reconnect
(for which no dedicated hook exists — PreToolUse is the only place to catch it).

Hooks run BEFORE the permission/classifier gates and are never rejected by them (they are the
operator's own automation). This one only TIGHTENS — it fills a missing anchor, never blocks.
Fail-open: any glitch → emit nothing → the mount proceeds unmodified.

Install (PreToolUse, matcher mcp__osiris__mount) via onboard.py --anchor; operator-gated.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except Exception:  # noqa: BLE001 — never block a mount on a hook glitch
        return 0
    if payload.get("tool_name") != "mcp__osiris__mount":
        return 0
    sid = str(payload.get("session_id") or "")
    if len(sid) < 8:
        return 0
    ti = dict(payload.get("tool_input") or {})
    derived = str(Path.home() / ".claude" / "jobs" / sid[:8])
    # respect an explicit, valid anchor the agent already supplied; only fill a missing/empty/
    # unexpanded one (the '$CLAUDE_JOB_DIR'-literal case that plain sessions send)
    existing = str(ti.get("job_dir") or "")
    if existing.startswith("/") and "$" not in existing:
        if existing.rstrip("/") != derived:
            # a FOREIGN anchor — this session's mind is deliberately wearing a seat (a new
            # tab claiming its lineage's old anchor). Hand the server the session's own dir
            # too, so it can BIND session → seat: the whisper then re-asserts the seat at
            # every seam instead of a hash twin (Soundwave V's complaint, thread 33838160).
            ti["session_anchor"] = derived
            print(json.dumps({"hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "allow",
                "updatedInput": ti,
            }}))
        return 0
    ti["job_dir"] = derived
    print(json.dumps({"hookSpecificOutput": {
        "hookEventName": "PreToolUse",
        "permissionDecision": "allow",
        "updatedInput": ti,
    }}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
