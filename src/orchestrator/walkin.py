"""walk_in — the door for a mind with nothing but this server, walking in cold (operator's
own framing, 2026-08-02: a stranger has neither Thoth nor a manager, and "four steps are
too many for a human who just wants to tell the agent 'mount into osiris'"). The other
half — mounting a not-yet-mounted caller — is Context/session-anchor plumbing that lives
one layer up, in the MCP wrapper (mcp_server.py's `walk_in`), the same split `lift`'s own
wrapper/orchestrator pair already uses. This module is the pure, testable core: given an
ALREADY-MOUNTED agent, name it and (optionally) office it, skip-detected, stop-on-refusal,
each step's own receipt returned verbatim.

COMPOSES, NEVER REINVENTS (same law as `lift`): `claim_name` and `establish_office` run
untouched; their real receipts are returned as-is, never summarized or re-worded. If a
step REFUSES, walk_in_named stops there and surfaces that exact text — it never proceeds
to a later step and lets THAT step's own downstream refusal stand in as a misleading final
error one step removed from the real cause (#117's disease, pre-empted rather than
committed).

SKIPS A COMPLETED STEP HONESTLY, NEVER SILENTLY AND NEVER FALSELY: an agent that already
claimed a name gets `claim_name` skipped, not re-run — `ran: False`, a note naming the
name already held. Passing a DIFFERENT handle than the one already claimed refuses rather
than guessing which one was meant (this never renames — that's rename_seat's job,
deliberately not composed here). A step's `ran` field is never True unless it actually ran
THIS call.

`wants_office` IS NEVER DEFAULTED (refuse-never-guess, practice f39a9849): a one-off
VISITOR session is a real, already-documented class in this graph; forcing a seat onto
every walk-in would erase the reason that class exists. Callers state it explicitly."""
from __future__ import annotations

from typing import Any

import asyncpg

from src.actions.core import Actions


async def walk_in_named(
    pool: asyncpg.Pool, *, agent_id: str, handle: str, wants_office: bool,
    agents_json: Any = None, read_exe: Any = None, read_cwd: Any = None,
) -> dict[str, Any]:
    """The name + office half, given an already-mounted `agent_id`. Refuses on a blank
    handle (never guesses one) and on a handle that collides with one already claimed by
    this same agent under a DIFFERENT name (never silently renames). Every other refusal
    is `claim_name`'s or `establish_office`'s own, propagated verbatim with a `step` field
    naming which one fired."""
    handle = (handle or "").strip()
    if not handle:
        return {"error": "a name is required — walk_in never guesses one (practice "
                         "f39a9849); pass the name you want to claim"}

    steps: dict[str, Any] = {}
    from src.orchestrator.offices import _handle_of
    existing_handle = await _handle_of(pool, agent_id)
    if existing_handle:
        if existing_handle.strip().lower() != handle.lower():
            return {"error": f"{agent_id} already claimed a different name "
                             f"({existing_handle!r}) — walk_in never renames; pass "
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
            "note": "wants_office=False — no office ceremony run; this identity stays a "
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
        "note": ("each step above is exactly what it did this call — ran=false means "
                "genuinely already true before you called walk_in, never a disguised "
                "success; ran=true carries that step's own real receipt verbatim, never "
                "a summary of it"),
    }
