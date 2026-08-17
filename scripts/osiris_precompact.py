#!/usr/bin/env python3
"""The death rite — PreCompact hook (task #22, under ruling a882b334).

A compaction is a DEATH: the whisper mints the heir at the next SessionStart, but the dying
mind gets no turn to speak. This hook fires AT the seam and rings the sweep doorbell: the
worker's miner senses this session's transcript immediately, so anything the mind lived but
never deliberately recorded is mined (DERIVED) around the seam — the heir's first orient()
already shows it, instead of waiting on the miner's 10-minute rounds.

STDLIB ONLY and FAIL-OPEN: a missed ring costs at most one miner lag, never a blocked
compaction. Timeout 2s.

OSIRIS_SWEEP_URL overrides the target (thread from Sekhmet's #40 finding, decision
66318e03): the hardcoded loopback only ever reaches a worker running on THIS box, so a
compaction on a machine that isn't the fleet's worker box (hyper-docker, the operator's
laptop) rang a doorbell nobody was listening at, invisibly. Default unchanged, so nothing
moves on the worker's own box; an off-box machine sets the env var to the real worker
URL. Every attempt is logged to stderr: which URL, and whether the POST connected.
"""
from __future__ import annotations

import json
import os
import sys
import urllib.request

SWEEP = os.environ.get("OSIRIS_SWEEP_URL", "http://127.0.0.1:8790/sweep")


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
        print(f"osiris_precompact: rang {SWEEP} — connected", file=sys.stderr)
    except Exception as exc:  # noqa: BLE001 — the rite is best-effort; the rounds still come
        print(f"osiris_precompact: rang {SWEEP} — failed ({exc})", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
