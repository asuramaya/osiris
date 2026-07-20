"""The adversary's SCOPE — "armed for one project" as a mechanism, not an improvisation
(task #37, the adversary arc's enabler).

OSIRIS_SENSE_SESSIONS is the adversary's LICENCE (may it read transcripts with a model at
all); OSIRIS_SENSE_PROJECTS is its SCOPE (which projects' transcripts the licence covers).
Empty scope = every project — exactly the pre-scope behavior, so the lever ships dark and
arming it is the operator's comfort, per the half-lever doctrine.

THE SEMANTICS THAT MATTER: scope DEFERS reading, it never buries it. A scoped-out
transcript is not listed, not swept, and — critically — never marked swept: the moment the
operator widens the scope, the orphan reaper finds those sessions exactly as it finds any
ended-and-unread transcript, and the backlog drains through the normal licensed lanes.
Nothing is lost by narrowing; only spending is narrowed.

Matching is over the transcript PROJECT DIR SLUG (`~/.claude/projects/<slug>/`), the one
name every enumeration point already holds: an entry matches when the slug ENDS with
`-<entry>` — 'pokex' matches '-home-asuramaya-code-pokex', 'thoth' matches the office slug
'-home-asuramaya--osiris-seats-thoth' — and multi-segment entries ('code/pokex') normalize
their '/' to '-' and match the same way. Suffix matching is deliberately simple: the scope
is a spend-comfort lever the operator arms by hand, not an identity system.

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
    """Does a transcript project-dir slug fall inside the scope? Empty scope = everything
    (the unarmed default)."""
    if not scopes:
        return True
    d = dirname.lower()
    return any(d == s or d.endswith("-" + s) for s in scopes)
