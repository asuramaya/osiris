"""The session-miner's SCOPE, "armed for one project" as a mechanism, not an improvisation.

OSIRIS_SENSE_SESSIONS is the session-miner's LICENCE (may it read transcripts with a model at
all); OSIRIS_SENSE_PROJECTS is its SCOPE (which projects' transcripts the licence covers).
Empty scope = every project, exactly the pre-scope behavior, so the lever ships dark and
arming it is left to the user's own comfort, per the half-lever doctrine.

THE SEMANTICS THAT MATTER: scope DEFERS reading, it never buries it. A scoped-out
transcript is not listed, not swept, and, critically, never marked swept: the moment the
scope widens, the orphan reaper finds those sessions exactly as it finds any
ended-and-unread transcript, and the backlog drains through the normal licensed lanes.
Nothing is lost by narrowing; only spending is narrowed.

Matching is over the transcript PROJECT DIR SLUG (`~/.claude/projects/<slug>/`), the one
name every enumeration point already holds: an entry matches when the slug ENDS with
`-<entry>`, for example 'pokex' matches '-home-asuramaya-code-pokex', and a project seat
slug like '-home-asuramaya--osiris-seats-<name>' matches the same way, and multi-segment
entries ('code/pokex') normalize their '/' to '-' and match the same way. Suffix matching
is deliberately simple: the scope is a spend-comfort lever the user arms by hand, not an
identity system.

Kept in its own tiny module so the orphan reaper can import it without pulling the heavy
session-miner (the same reason redact.py stands alone).
"""
from __future__ import annotations


def sense_scopes(raw: str) -> list[str]:
    """Parse OSIRIS_SENSE_PROJECTS: comma/space-separated entries, normalized to the
    slug vocabulary (lowered, '/'→'-', outer dashes stripped). Empty input → []."""
    out: list[str] = []
    for part in raw.replace(",", " ").split():
        frag = part.strip().lower().replace("/", "-").strip("-")
        if frag:
            out.append(frag)
    return out


def scope_match(dirname: str, scopes: list[str]) -> bool:
    """Does a transcript project-dir slug fall inside the scope? Empty scope means everything
    (the unarmed default)."""
    if not scopes:
        return True
    d = dirname.lower()
    return any(d == s or d.endswith("-" + s) for s in scopes)
