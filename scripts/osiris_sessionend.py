#!/usr/bin/env python3
"""SessionEnd hook — the ghost-seat fix (heinrich's filing, thread 1fe6811c).

Closing or killing a Claude tab never told Osiris anything: Stop fires PER-TURN (it cannot
mean "closed" — a turn ending is not a tab closing), so a dead session's durable mount kept
its `last_seen` stamp for up to 15 minutes after nobody was listening — the fleet carrying
277 of these stale ghosts at filing time. SessionEnd is the harness's REAL close signal: this
posts {session_id} to the MCP server's /session-end, which releases the ending session's
durable mount row the SAME WAY retire()'s tool releases a seat — the row stops answering any
liveness probe immediately, instead of aging out.

Deliberately NOT a retire(): no permanent 'retired=true' certificate is stamped here (that is
a MIND's own deliberate, signed farewell). A session that later resumes (`claude --resume`)
re-earns its row from a fresh automount, exactly as if this hook had never fired.

STDLIB ONLY and FAIL-OPEN, mirroring osiris_whisper.py: any failure is silent, exit 0 — a
session must always be able to end. Timeout 2s.
"""
from __future__ import annotations

import json
import sys
import urllib.request

SESSION_END = "http://127.0.0.1:8790/session-end"


def main() -> int:
    try:
        hook = json.load(sys.stdin)
    except Exception:  # noqa: BLE001
        return 0
    session_id = str(hook.get("session_id") or "")
    if not session_id:
        return 0
    try:
        req = urllib.request.Request(
            SESSION_END, data=json.dumps({"session_id": session_id}).encode(),
            headers={"Content-Type": "application/json"}, method="POST")
        urllib.request.urlopen(req, timeout=2)
    except Exception:  # noqa: BLE001 — server down: the row just ages out as it does today
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
