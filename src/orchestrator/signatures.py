"""THE RESIDENT'S SIGNATURE, shared (2026-09-03): who actually lives in a session, read off
its own append-only transcript: a mount's `{"agent":"agent:…","project":…}` receipt, a
send's `{"sent":N,"from":"agent:…"}`, the SessionStart whisper's "knows you as agent:…".
Lifted out of trigger.py so the session LEDGER's write side (handshake.record_session_anchor)
can read the same evidence the resume gate reads, without an import cycle.

TWO GRADES, NOT ONE: a mount/send receipt is the agent's own act; a whisper greeting is the
server's resolution of who the window is, injected as an attachment. One incident (2026-09-03)
showed why the distinction matters: a session carried two greetings naming a different agent's
lineage (from an anchor-leaked hand resume) and not one act by that other agent, while every
act in the file belonged to the session's real occupant. Read as testimony, that greeting
would have misidentified the seat's own session as crossed-registry and stamped the session
ledger to the wrong lineage, so every later resume would re-bind the window to the wrong agent.
An act outranks a greeting."""
from __future__ import annotations

import re

SIGNED_ACTS = [
    re.compile(r'\\?"sent\\?":\s*\d+,\s*\\?"from\\?":\s*\\?"(agent:[A-Za-z0-9._-]+)'),
    re.compile(r'\\?"agent\\?":\s*\\?"(agent:[A-Za-z0-9._-]+)\\?",\s*\\?"project\\?"'),
]
SIGNED_WHISPERS = [
    re.compile(r"knows you as (agent:[A-Za-z0-9._-]+)"),
]
SIGNED = [*SIGNED_ACTS, *SIGNED_WHISPERS]


def newest_signatures(lines: list[str]) -> tuple[str | None, str | None]:
    """(newest ACT signature, newest WHISPER greeting) in `lines`, newest-first scan.
    Stops as soon as an act is found (anything older is not the newest of either kind
    that matters: an act newer than every greeting settles the resident by itself).
    `whisper` is therefore only ever non-None when it is NEWER than the act."""
    whisper: str | None = None
    for line in reversed(lines):
        for pat in SIGNED_ACTS:
            m = pat.search(line)
            if m:
                return m.group(1), whisper
        if whisper is None:
            for pat in SIGNED_WHISPERS:
                m = pat.search(line)
                if m:
                    whisper = m.group(1)
                    break
    return None, whisper
