"""lift(ref, handle) — pull a QUIET rogue instance out of its ad hoc cwd and into a clean
osiris home + seat, one coherent act (thread 67f11cbd, the operator's framing: import a
running guest into the managed home, preserve its state, give it a clean managed identity).

COMPOSES rather than reinvents: `doors()` (Wave 2, this module's sibling) already answers
"who is this, and are they home right now"; `claim_name` already names a lineage and binds
its seat; `establish_office` already rebind-extracts a named seat into
`~/.osiris/seats/<handle>/`, re-addressing transcripts so resume works in place. lift() runs
the three as one act, plus a fourth: a fresh `doors()` read AFTER the write, because a
receipt that only echoes what claim_name/establish_office each individually claimed is
exactly the lying-receipt shape the fleet has been fighting all week (dispatch_dm's
"delivered"-that-wasn't, 5af93c89; the compaction-seam refusal that was actually stale,
a580490f) — `verified` here is an independent re-derivation, never a collapsed echo.

SELF-LIFT IS STRUCTURALLY IMPOSSIBLE, not just unbuilt — proven, not assumed. A caller's own
`agent_mounts.last_seen` is kept perpetually fresh by ITS OWN TERMINAL's statusline heartbeat
(`scripts/osiris_statusline.py`: `UPDATE agent_mounts SET last_seen=now()` fires on every
render, independent of any tool call), so a live session can never observe itself as quiet
from inside a call — and even bypassing that check, the running harness process keeps
appending to its OLD transcript path regardless of what the database says (fixed at session
start). So lift() always targets a NAMED, ALREADY-QUIET rogue from a DIFFERENT live session
— never itself. `establish_office`'s own live-seat refusal (the rollout guard) is reused in
spirit here, front-loaded as a `doors()` pre-check so the whole act refuses atomically
before `claim_name` ever runs, rather than leaving a claimed name stranded with no office.

ONE PRE-EXISTING GAP, named and deliberately left alone (not lift()'s to fix): `held_seat`
does an EXACT generation match with no lineage-aware fallback (bug 5ec86731), and
`mint_heir` never calls `bind_holder` on succession — so a FUTURE successor of a lifted
agent could still land in the same unreachable-head class Ra hit (aeae9977/5af93c89) if that
gap outlives this build. lift() itself is sound for the generation it acts on; the
dependency is Khnum's, tracked on thread 67f11cbd.
"""
from __future__ import annotations

from typing import Any

import asyncpg


async def lift(
    pool: asyncpg.Pool, ref: str, handle: str, *, actor: str | None = None,
    office_root: Any = None, projects_root: Any = None, claude_json: Any = None,
) -> dict[str, Any]:
    """Extract a named, quiet rogue into a clean office. `doors(ref)` resolves the target —
    refuses on 0 matches (never invents one), on >1 (an ambiguous multi-tenant cwd — name a
    specific `agent:` id instead of guessing), and on a LIVE match (moving a live seat splits
    its running session's history between two homes; close its tab first). Then
    `claim_name(handle)` names the lineage, propagating its own real refusals honestly (a
    visitor, a name held live elsewhere, a cross-house collision) rather than reinventing
    them. Then `establish_office` performs the actual move. Then `doors()` runs AGAIN on the
    result — the receipt's `verified` field is that fresh, independent read, never an echo of
    what the sub-steps each individually claimed. Non-side-channel: pure graph + file
    operations, never the daemon reply lane."""
    from src.actions.core import Actions
    from src.orchestrator.agents import claim_name as _claim_name
    from src.orchestrator.doors import doors as _doors
    from src.orchestrator.offices import establish_office as _establish_office

    ref = (ref or "").strip()
    pre = await _doors(pool, ref)
    matches = pre["matches"]
    if not matches:
        return {"error": f"no such agent/seat/cwd: {ref!r} — lift never guesses at a "
                         "target; an unresolved ref is refused, not invented"}
    if len(matches) > 1:
        return {"error": f"{ref!r} is ambiguous — {len(matches)} distinct souls have "
                         "mounted there; name a specific agent: id instead of a shared cwd",
                "candidates": [m["agent_id"] for m in matches]}
    target = matches[0]
    if target["live"]:
        return {"error": f"{target['agent_id']} is LIVE right now (last seen "
                         f"{target['last_seen']}) — moving a live seat splits its running "
                         "session's history between two homes. Close its tab first, then "
                         "lift; it wakes up in the office"}
    agent_id = target["agent_id"]
    actions = Actions(pool)

    claimed = await _claim_name(actions, agent_id, handle, source=actor or "ceremony:lift")
    if "error" in claimed:
        return {"error": claimed["error"], "step": "claim_name"}

    established = await _establish_office(
        actions, seat_or_agent=agent_id, actor=actor,
        office_root=office_root, projects_root=projects_root, claude_json=claude_json)
    if "error" in established:
        return {"error": established["error"], "step": "establish_office", "claimed": claimed}

    post = await _doors(pool, agent_id)
    post_match = post["matches"][0] if post["matches"] else None
    verified = {
        "resolved": bool(post_match),
        "seat_bound": bool(post_match and post_match.get("seat")),
        "handle_matches": bool(
            post_match and post_match.get("seat")
            and post_match["seat"]["handle"].lower() == handle.lower()),
        "cwd_is_office": bool(
            post_match and post_match.get("cwd") == established.get("office")),
    }
    return {
        "agent": agent_id, "handle": handle,
        "claimed": claimed, "established": established,
        "verified": verified,
        "post_lift": post_match,
        "note": (f"{handle} lifted into {established.get('office')} — `verified` is a fresh "
                 "doors() read taken AFTER the write, not an echo of what claim_name or "
                 "establish_office each individually claimed"),
    }
