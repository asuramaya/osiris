#!/usr/bin/env python3
"""The death rite — PreCompact hook (task #22, under ruling a882b334).

A compaction is a DEATH: the whisper mints the heir at the next SessionStart, but the dying
mind gets no turn to speak. This hook fires AT the seam and rings the sweep doorbell: the
worker's miner senses this session's transcript immediately, so anything the mind lived but
never deliberately recorded is mined (DERIVED) around the seam — the heir's first orient()
already shows it, instead of waiting on the miner's 10-minute rounds.

STDLIB ONLY and FAIL-OPEN: a missed ring costs at most one miner lag, never a blocked
compaction. Timeout 2s.
"""
from __future__ import annotations

import json
import sys
import urllib.request

SWEEP = "http://127.0.0.1:8790/sweep"


def main() -> int:
    try:
        hook = json.load(sys.stdin)
    except Exception:  # noqa: BLE001
        return 0
    transcript = str(hook.get("transcript_path") or "")
    if not transcript.startswith("/"):
        return 0
    try:
        req = urllib.request.Request(
            SWEEP, data=json.dumps({
                "transcript_path": transcript,
                "session_id": str(hook.get("session_id") or ""),
                "trigger": str(hook.get("trigger") or ""),
            }).encode(),
            headers={"Content-Type": "application/json"}, method="POST")
        urllib.request.urlopen(req, timeout=2)
    except Exception:  # noqa: BLE001 — the rite is best-effort; the rounds still come
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
