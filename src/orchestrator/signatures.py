"""THE RESIDENT'S SIGNATURE, shared (2026-09-03): who actually lives in a session, read off
its own append-only transcript — a mount's `{"agent":"agent:…","project":…}` receipt, a
send's `{"sent":N,"from":"agent:…"}`, the SessionStart whisper's "knows you as agent:…".
Lifted out of trigger.py so the session LEDGER's write side (handshake.record_session_anchor)
can read the same evidence the resume gate reads, without an import cycle.

TWO GRADES, NOT ONE: a mount/send receipt is the MIND's own act; a whisper greeting is the
SERVER's resolution of who the window is, injected as an attachment. Chad, 2026-09-03: two
greetings naming Khnum's lineage (an anchor-leaked hand resume, class 2294e95d) and not one
act by it, while every act in the file was Chad's — read as testimony, that greeting refused
the seat's own session as crossed-registry AND stamped the session ledger to the wrong
lineage, so every later resume re-bound the window to Khnum. An act outranks a greeting."""
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
    """(newest ACT signature, newest WHISPER greeting) in `lines`, newest-first scan —
    stops as soon as an act is found (anything older is not the newest of either kind
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
