"""walk_in: the entry point for a session that has nothing but this server, arriving cold
with neither a manager nor prior context, so that a caller doesn't need multiple separate
steps just to get the agent mounted into osiris. The other half, mounting a not-yet-mounted
caller, is session-anchor plumbing that lives one layer up, in the MCP wrapper
(mcp_server.py's `walk_in`), the same wrapper/orchestrator split `lift` already uses. This
module is the pure, testable core: given an already-mounted agent, name it and
(optionally) give it an office, with skip detection, stop-on-refusal, and each step's own
result returned verbatim.

Composes rather than reinventing: `claim_name` and `establish_office` run untouched; their
real results are returned as-is, never summarized or re-worded. If a step refuses,
walk_in_named stops there and surfaces that exact text; it never proceeds to a later step
and lets that step's own downstream refusal stand in as a misleading final error one step
removed from the real cause.

Skips a completed step honestly, never silently and never falsely: an agent that already
claimed a name gets `claim_name` skipped, not re-run, with `ran: False` and a note naming
the name already held. Passing a different handle than the one already claimed refuses
rather than guessing which one was meant (this never renames; that's rename_seat's job,
deliberately not composed here). A step's `ran` field is never True unless it actually ran
this call.

`wants_office` is never defaulted: a one-off visitor session is a real, already-documented
class in this graph, and forcing a seat onto every walk-in would erase the reason that
class exists. Callers state it explicitly."""
from __future__ import annotations

from typing import Any

import asyncpg

from src.actions.core import Actions


async def walk_in_named(
    pool: asyncpg.Pool, *, agent_id: str, handle: str, wants_office: bool,
    agents_json: Any = None, read_exe: Any = None, read_cwd: Any = None,
) -> dict[str, Any]:
    """The naming and office half, given an already-mounted `agent_id`. Refuses on a blank
    handle (never guesses one) and on a handle that collides with one already claimed by
    this same agent under a different name (never silently renames). Every other refusal
    is `claim_name`'s or `establish_office`'s own, propagated verbatim with a `step` field
    naming which one fired."""
    handle = (handle or "").strip()
    if not handle:
        return {"error": "a name is required, walk_in never guesses one; pass the "
                         "name you want to claim"}

    steps: dict[str, Any] = {}
    from src.orchestrator.offices import _handle_of
    existing_handle = await _handle_of(pool, agent_id)
    if existing_handle:
        if existing_handle.strip().lower() != handle.lower():
            return {"error": f"{agent_id} already claimed a different name "
                             f"({existing_handle!r}), walk_in never renames; pass "
                             f"handle={existing_handle!r} to proceed with the existing "
                             "name, or use rename_seat if you deliberately want to change it",
                    "step": "claim_name", "steps_so_far": steps}
        final_handle = existing_handle
        steps["claim_name"] = {"ran": False,
                               "note": f"already claimed as {existing_handle!r}, skipping"}
    else:
        from src.orchestrator.agents import claim_name as _claim_name
        claimed = await _claim_name(
            Actions(pool), agent_id, handle, source=agent_id,
            agents_json=agents_json, read_exe=read_exe, read_cwd=read_cwd)
        if "error" in claimed:
            return {"error": claimed["error"], "step": "claim_name", "steps_so_far": steps}
        final_handle = handle
        steps["claim_name"] = {"ran": True, "result": claimed}

    if not wants_office:
        steps["establish_office"] = {
            "ran": False,
            "note": "wants_office=False: no office setup run; this identity stays a "
                    "visitor, not a seated worker",
        }
    else:
        from src.orchestrator.offices import establish_office as _establish_office
        established = await _establish_office(
            Actions(pool), seat_or_agent=agent_id, actor=agent_id)
        if "error" in established:
            return {"error": established["error"], "step": "establish_office",
                    "steps_so_far": steps}
        steps["establish_office"] = {"ran": True, "result": established}

    return {
        "agent": agent_id, "handle": final_handle, "wants_office": wants_office,
        **steps,
        "note": ("each step above is exactly what it did this call: ran=false means "
                "genuinely already true before you called walk_in, never a disguised "
                "success; ran=true carries that step's own real result verbatim, never "
                "a summary of it"),
    }


async def promote_visitor(
    pool: asyncpg.Pool, *, target: str, handle: str, because: str, actor: str,
    ruling: str | None = None, repos: list[str] | None = None,
) -> dict[str, Any]:
    """The visitor-to-registered-agent collapse in one act: claim_name + charter_for +
    establish_office, converging on the same end-state the managed path reaches
    (seat:<uuid8> + office + deed), for a third party, never the caller's own identity
    (that self-service shape is `walk_in_named`, above; this is a sibling, not a
    replacement). A visitor here is a specific known state: a resolved anchor with a real
    `agent_mounts` row and no `objects` row of type Agent, i.e. a registry row and nothing
    else, no Agent object (see mount()'s own docstring). This is exactly the population an
    operator or manager promotes by hand today: a session with real, repeated presence in
    the graph that has simply never claimed its own name.

    Authorization is enforced, not merely named: a third-party act minting a third
    party's whole identity is not routine, so `actor` must resolve to a recognized operator
    identity (`charter.is_operator_actor`, a global recognition with no single project in
    scope for a whole-identity mint), or hold a seat that itself manages at least one
    worker (`seats.seats_managed_by`, a manager's word, the same shape of enforced check
    `charter.charter_for` runs for its own third-party act), or `ruling` must pass
    `capture.verify_ruling` (the same ruling-citation check `charter_for` itself now
    uses, adopted here rather than left as a second, weaker bespoke check): the citation
    must resolve to a real Decision, that decision's own `kind` must read 'ruling' (an
    ordinary decision is not standing authority), and its own summary or rationale text
    must actually name "promote_visitor"; a ruling about something else cannot silently
    authorize this write just because a caller cited it. `because` is always required
    regardless, the same testimony discipline `charter_for`/`rename_seat` already run for
    a third-party act. This is the whole act's one and only authorization gate,
    deliberately not `charter_for` itself (see the note at its call site below: that
    call's own gate checks the target seat's manager, which cannot exist yet for a seat
    this same call is about to mint).

    Refuses on a target that isn't a genuine, known visitor: an `objects` row of type
    Agent already existing for `target` means this isn't a promotion; claim_name/
    establish_office/charter_for compose directly for an already-real identity, and
    running this action over one would silently redo work that already happened. A `target`
    with no `agent_mounts` row at all is not a visitor either, it is nothing; this never
    mints a label invented on the spot, the anchor must be real.

    Order matters: claim_name mints the Agent object (and its seat); the charter write
    runs before establish_office because `project_of`'s own resolution order (agents.py)
    reads a seat's declared charter as its second tier, and establish_office refuses
    outright on an agent with no durable project label; reversing this order would make a
    visitor's own genuine, repeated cwd unusable as the source of its office. Stops on the
    first refusal, the same rule `walk_in_named` already keeps: a later step's own
    downstream refusal must never stand in as a misleading final error one step removed
    from the real cause."""
    from src.orchestrator.agents import claim_name as _claim_name
    from src.orchestrator.capture import verify_ruling
    from src.orchestrator.charter import is_operator_actor
    from src.orchestrator.charter import set_charter as _set_charter
    from src.orchestrator.offices import establish_office as _establish_office
    from src.orchestrator.seats import held_seat, seats_managed_by

    target, handle = (target or "").strip(), (handle or "").strip()
    because = (because or "").strip()
    if not target:
        return {"error": "a target is required, promote_visitor never guesses one"}
    if not handle:
        return {"error": "a name is required, promote_visitor never guesses one"}
    if not because:
        return {"error": "because is required: promoting a third party's whole identity "
                         "on their behalf is testimony, same discipline charter_for and "
                         "rename_seat already run"}

    authorized = await is_operator_actor(pool, actor)
    auth_note = "operator" if authorized else None
    if not authorized:
        caller_seat = await held_seat(pool, actor)
        caller_seat_id = caller_seat["seat_id"] if caller_seat else None
        if caller_seat_id and await seats_managed_by(pool, str(caller_seat_id)):
            authorized, auth_note = True, "manager"
    ruling_id = None
    ruling_check: dict[str, Any] | None = None
    if not authorized and ruling:
        ruling_check = await verify_ruling(pool, ruling, write_name="promote_visitor")
        if ruling_check["ok"]:
            ruling_id, authorized, auth_note = ruling_check["ruling_id"], True, "ruling"
    if not authorized:
        if ruling_check is not None:  # a ruling was cited but verify_ruling refused it,
            return {"error": ruling_check["error"]}  # so return its own reason, never re-derived
        return {"error": f"{actor} is not authorized to promote {target!r}: this needs "
                         "the operator's word (an operator actor), a manager's word "
                         "(a seat that itself manages at least one worker), or a "
                         "`ruling=` citation naming a real Decision; none was found"}

    already = await pool.fetchval(
        "SELECT 1 FROM objects WHERE type='Agent' AND canonical=$1", target)
    if already:
        return {"error": f"{target} already has an Agent object: this is not a "
                         "visitor, and promote_visitor is not the path for an already-"
                         "real identity; claim_name/establish_office/charter_for "
                         "compose directly for that instead"}
    seen = await pool.fetchval(
        "SELECT 1 FROM agent_mounts WHERE agent_id=$1 LIMIT 1", target)
    if not seen:
        return {"error": f"{target} has never mounted: nothing to promote; "
                         "promote_visitor never mints a label invented on the spot"}

    steps: dict[str, Any] = {}
    claimed = await _claim_name(Actions(pool), target, handle, source=actor)
    if "error" in claimed:
        return {"error": claimed["error"], "step": "claim_name", "steps_so_far": steps}
    steps["claim_name"] = claimed
    seat_id = claimed.get("seat_id")
    if not seat_id:
        return {"error": claimed.get("seat_error") or
                         "claim_name minted no seat: nothing to charter or office",
                "step": "claim_name", "steps_so_far": steps}

    final_repos = repos
    if not final_repos:
        final_repos = [p for p in [await pool.fetchval(
            "SELECT project FROM agent_mounts WHERE agent_id=$1 AND project IS NOT NULL "
            "ORDER BY last_seen DESC NULLS LAST LIMIT 1", target)] if p]
    if not final_repos:
        return {"error": f"no repos given and {target} carries no project on its own "
                         "mount record, promote_visitor never guesses a charter; pass "
                         "repos= explicitly",
                "step": "charter_for", "steps_so_far": steps}
    # `set_charter`, not `charter_for`, deliberately: charter_for runs its own separate
    # authorization gate (actor must be an operator or the target seat's own manager).
    # For a seat that was just minted this same call, nobody is its manager yet, so a
    # caller authorized by promote_visitor's own broader gate (an operator, any manager,
    # or a ruling citation) would be refused a second time by a narrower gate that can
    # never pass here. promote_visitor's own gate above is the one and only
    # authorization check this whole act runs; set_charter (the primitive charter_for
    # itself wraps) carries none of its own, so it composes cleanly.
    chartered = await _set_charter(Actions(pool), seat_id, final_repos, actor=actor)
    if "error" in chartered:
        return {"error": chartered["error"], "step": "charter_for", "steps_so_far": steps}
    steps["charter_for"] = chartered

    officed = await _establish_office(Actions(pool), seat_or_agent=target, actor=actor)
    if "error" in officed:
        return {"error": officed["error"], "step": "establish_office",
                "steps_so_far": steps}
    steps["establish_office"] = officed

    return {
        "promoted": target, "was_visitor": True, "handle": handle, "seat": seat_id,
        **steps,
        "authorized_by": {"actor": actor, "because": because, "via": auth_note,
                          **({"ruling": ruling_id and str(ruling_id)} if ruling_id else {})},
        "note": "target moved from a bare registry row (no Agent object) to a fully "
                "realized identity: claimed a name, was chartered over its repo, and "
                "now holds an office+deed, the managed path's own end-state, in one act",
    }
