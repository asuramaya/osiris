"""THE CHARTER — a house is what a seat RULES, not where it sits (Phase 1 §4.1, `dd47c1da`).

`path = project = identity` is the bug under the whole fold: today a seat's authority over a
repo is only ever implied by works_in — one project, wherever its folder happens to be right
now. alfred's charter is six repos; none of them is "the folder he's sitting in this week". A
CHARTER makes that authority an explicit, first-class fact instead of an inference from cwd,
so a folder move (§4.1's seat-rebind primitive, `mounts.rebind_seat`) has nothing to orphan.

`set_charter` declares the WHOLE charter each call — an idempotent statement of "these are the
repos this seat rules now", not an increment. Repos newly named mint a `governs` link
(SELF_DECLARED, the seat's own act); repos that drop off are healed by a COMPENSATING EVENT
(`links.valid_until`, constitution #3 — never DELETE), so a shrinking charter stays a fact the
graph remembers rather than information destroyed. `charter_of` reads back the currently
active set. Wave 2 builds charter-scoped briefing aggregation on top of this; this lane only
makes the charter a fact and makes it VISIBLE (orient()'s one line).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

_EC = EvidenceClass.SELF_DECLARED.value
_CONF = confidence_for(EvidenceClass.SELF_DECLARED)


async def charter_of(pool: asyncpg.Pool, agent_id: str) -> list[str]:
    """The repos this seat currently governs — the ACTIVE `governs` links (a compensated-away
    grant does not count), plain repo labels (the `repo:` scheme stripped), sorted. Empty for
    the common seat that has never declared a charter (works_in still names its home)."""
    rows = await pool.fetch(
        "SELECT p.canonical FROM links l "
        "JOIN objects a ON a.id=l.from_id AND a.type='Agent' AND a.canonical=$1 "
        "JOIN objects p ON p.id=l.to_id AND p.type='SoftwareProject' "
        "WHERE l.type='governs' AND (l.valid_until IS NULL OR l.valid_until > now())",
        agent_id,
    )
    return sorted(r["canonical"].removeprefix("repo:") for r in rows)


async def set_charter(actions: Actions, agent_id: str, repos: list[str]) -> dict[str, Any]:
    """Declare the WHOLE charter — replacing whatever this seat ruled before, never adding to
    it blind. Self-declared, same evidence grade as claim_name (a seat's own word about its own
    jurisdiction). Repos newly named mint a fresh `governs` link; repos dropped from the list
    are healed by `invalidate_link` (compensating, never DELETE) rather than left to rot as a
    stale grant nobody can tell apart from a current one. Calling it twice with the identical
    set is a no-op: nothing minted, nothing healed."""
    now = datetime.now(UTC)
    wanted = sorted({r.strip() for r in repos if r and r.strip()})
    current = set(await charter_of(actions.pool, agent_id))
    added = [r for r in wanted if r not in current]
    removed = sorted(current - set(wanted))
    if not added and not removed:
        return {"agent": agent_id, "charter": sorted(current), "added": [], "removed": []}
    a = await actions.create_or_find_object("Agent", agent_id, agent_id)
    for repo in added:
        proj = await actions.create_or_find_object("SoftwareProject", f"repo:{repo}", agent_id)
        await actions.assert_property(proj, "name", repo, agent_id, now, _CONF,
                                      evidence_class=_EC)
        await actions.create_link(a, proj, "governs", agent_id, now, _CONF, evidence_class=_EC)
    for repo in removed:
        proj = await actions.create_or_find_object("SoftwareProject", f"repo:{repo}", agent_id)
        await actions.invalidate_link(a, proj, "governs", agent_id, now)
    return {"agent": agent_id, "charter": wanted, "added": added, "removed": removed}
