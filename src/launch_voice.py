"""What `osiris launch` and `osiris resume` say to a person at a terminal.

At most three short lines by default: what happened and, only when needed, one next step as a
command to copy. Internals (the harness's quirks, ledger rows, hop counts, byte sizes, model
ids, a refusal's full diagnostic) sit behind `--verbose`. Pure text, no I/O, so the wording is
pinned by tests and one voice serves both verbs.
"""
from __future__ import annotations

import os
import re
from pathlib import Path

_UNTRUSTED = re.compile(r"not trusted", re.I)


def is_untrusted(text: object) -> bool:
    """Claude Code refused a directory the user has not trusted yet."""
    return bool(_UNTRUSTED.search(str(text)))


def home_short(path: str) -> str:
    """`/home/me/code/x` as `~/code/x`, for a line a person reads and copies."""
    home = str(Path.home())
    if path == home:
        return "~"
    if path.startswith(home + os.sep):
        return "~" + path[len(home):]
    return path


_TRUST_DIR = re.compile(r"`claude` in (\S+?) once", re.I)


def untrusted_line(verb: str, cwd: str, detail: object = "") -> str:
    """Granting trust stays the person's own act; osiris only says exactly where. The
    harness names the directory it refused; that one is used when the message carries it."""
    m = _TRUST_DIR.search(str(detail))
    if m:
        cwd = m.group(1)
    short = home_short(cwd)
    name = Path(cwd).name or short
    return (f"osiris {verb}: {name} is not trusted by Claude Code yet: run  "
            f"cd {short} && claude  , accept, then {verb} again.")


def first_sentence(text: object, limit: int = 160) -> str:
    """The first sentence of a diagnostic, never cut mid-word, for the default line."""
    flat = " ".join(str(text).split())
    m = re.match(r"(.+?[.!?])(?:\s|$)", flat)
    out = m.group(1) if m else flat
    if len(out) > limit:
        out = out[:limit].rsplit(" ", 1)[0] + "..."
    return out


def running(verb: str, handle: str, window: str | None) -> str:
    where = f" ('{window}' in claude agents)" if window else ""
    return f"osiris {verb}: {handle} is running{where}."


def already_running(verb: str, handle: str) -> str:
    return f"osiris {verb}: {handle} is already running; nothing started."


def started_not_visible(verb: str, handle: str) -> str:
    return (f"osiris {verb}: {handle} was started but is not in claude agents yet; "
            "check `osiris fleet` in a few seconds.")


def never_appeared(verb: str, handle: str, session: str) -> str:
    return (f"osiris {verb}: {handle} did not start (session {session} never appeared). "
            f"Check: claude logs {session}")


def resumed(verb: str, handle: str, sid: str) -> str:
    return f"osiris {verb}: {handle} resumed (session {sid[:8]})."


def nothing_to_resume(handle: str) -> str:
    return (f"osiris resume: {handle} has nothing to resume. "
            f"Start a new session instead: osiris launch {handle}")


def cannot_verify(verb: str, handle: str, sid: str) -> str:
    return (f"osiris {verb}: {handle}'s last session ({sid[:8]}) can't be confirmed as its "
            f"own, so it was not resumed. Start a new session instead: osiris launch {handle}")


def other_account(verb: str, handle: str) -> str:
    return (f"osiris {verb}: {handle}'s earlier session was written under another Claude "
            "account, so Claude Code cannot load it. Starting a new one with a briefing "
            "built from its history.")


def would_not_start(verb: str, handle: str, reason: object) -> str:
    return (f"osiris {verb}: Claude Code would not start {handle}: "
            f"{first_sentence(reason).rstrip('.')}. Nothing is running.")
