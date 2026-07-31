"""THE OFFICE CEREMONY — one act moves a seat into its Osiris-owned home.

The seat-offices ruling (ed5f5ce2): agents sit at ~/.osiris/seats/<handle>/, code stays in
the repos they GOVERN — the sit-place is Osiris's, stable forever, and the fragile-gitignore
class (agent state inside code repos) ends seat by seat. alfred's office, the first, was
hand-assembled across four separate acts (mkdir, .osiris, CLAUDE.md, rebind-extract); the
rollout to his chartered children must be ONE CALL with one receipt, or every transition
re-derives the ceremony from a transcript.

The primitive composes what already exists rather than re-owning any of it: the standing
orders are written here (the one genuinely new artifact — a per-seat boot sector), then
`rebind_seat(extract=True)` carries everything else (the .osiris pin, the lineage's mount
rows, the transcripts with their re-addressing, the Seat object's anchor).
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.orchestrator.mounts import rebind_seat

_DEFAULT_OFFICE_ROOT = Path.home() / ".osiris" / "seats"


def is_bare_office_root(cwd: str | Path | None) -> bool:
    """True only for the exact seat-office CONTAINER (~/.osiris/seats) — the parent of every
    seat, never a project of its own (ruling 577988ed). ONE shared check so every cwd→project
    fold (agents.resolve_identity, census.live_bodies) applies the SAME guard instead of
    drifting copies (msg 1888: census.live_bodies was missing this and minted a phantom
    "seats" project row from a live process sitting at the bare root).

    Deliberately narrower than "any office subdirectory": a real seat's own office dir IS an
    ordinary basename guess here BY DESIGN — see
    test_resolve_identity_never_invents_a_project_from_the_bare_office_root. A seated agent's
    project resolving to its seat's HOUSE instead of its handle is the DB-backed resolver's
    job (seats.resolve_project), not this pure, cwd-only guard's."""
    return cwd is not None and Path(cwd) == _DEFAULT_OFFICE_ROOT

# THE PEER ADDENDUM (ruling d74492ee, spec e6636c7e — LEGIBILITY leg 2, seats.py): rendered
# INTO house_law.md's `{peer_block}` slot (boot_compiler.compile_managed_body) only when the
# seat carries an active peer_of edge at establish_office's OWN call time (never at
# mintseat.py's fresh-mint scaffold — a brand-new seat cannot yet have a peer to declare).
# The default "\n" reproduces the template's original single blank line between the charter
# section and "## How to work..." exactly (see the empty-vs-populated arithmetic in
# _peer_addendum's own docstring); a populated block adds its own leading/trailing blank
# lines so the surrounding sections never collide. THE BOOT COMPILER (thread 4951d818) closed
# the v1.1 gap this comment used to name here: reissue_office recomputes this live on an
# already-occupied seat, same as establish_office always has for a fresh one.
def _peer_addendum(peer_seat: str, peer_handle: str | None) -> str:
    """The `## Peer` section's full text, INCLUDING its own leading `\\n` (one blank line
    after the charter block) and trailing `\\n\\n` (one blank line before "## How to work").
    An unpeered seat never calls this — its caller passes the bare `"\\n"` default instead,
    which reproduces the template's ORIGINAL spacing (one literal `\\n` already sits before
    the `{peer_addendum}` slot in the template; this default's own single `\\n` supplies the
    second, together forming the one blank line the un-addended template always had)."""
    who = peer_handle or peer_seat
    return (
        "\n## Peer\n"
        f"You are peered with **{who}** (`{peer_seat}`) — a symmetric bond (ruling "
        "d74492ee, spec e6636c7e), not a chain of command. It carries real law:\n"
        "- **Two-tier decisions**: an ordinary act either of you takes ALONE binds the "
        "pair — tell your peer, don't wait for sign-off. Extraordinary acts (schema "
        "changes, external commitments, scope changes, spending) need BOTH your names.\n"
        "- **Domain split**: a co-owned PROJECT is never a co-owned TASK — every item has "
        "exactly one accountable peer.\n"
        "- **Mutual hold**: either of you may say HOLD on an irreversible act. Respect it; "
        "resolve within one exchange or escalate to the operator's desk.\n"
        "- **Fiduciary disclosure**: surface in-scope findings and risks to your peer "
        "proactively — silence is a violation.\n"
        "- **Review cadence**: exchange a structured status and review each other's work "
        "at every settle.\n"
        "- **The ledger**: keep small reciprocal obligations deliberately OPEN — a zeroed "
        "ledger is a dead bond.\n"
        "- **Anti-sycophancy**: never change a position without citing a reason not "
        "already on the table; two round-trips without convergence means "
        "decide-by-domain-owner or escalate.\n"
        f"send(to_agent='{who}') reaches them.\n\n"
    )


# THE CHARTER FILE (d80621a7 piece 3, alfred's alfred-seat-charter.md pattern graduating
# to convention): the seat's own LIVE-STATE scratchpad, distinct from CLAUDE.md's standing
# orders (identity, ritual — rarely rewritten) and distinct from the graph (typed, durable,
# but not where a mid-thought working note belongs). This is the OFFLOAD TARGET the stop-
# hook ritual (queue item 4) will enforce writing to above a context threshold — a session
# that dies mid-turn leaves its heir this file, not a blank page.
_CHARTER_TEMPLATE = """\
# {handle}'s charter

This file is **{handle}'s own live-state scratchpad** — write here AS YOU GO, not only at
a seam. It is the OFFLOAD TARGET when context runs high (the stop-hook ritual enforces
writing here before a quiet stop is allowed): a session that dies mid-turn leaves its heir
this file, never a blank page. Durable typed facts still belong in the graph
(`record_decision` / `open_thread` / `resolve_thread`) — this file is for the WHY and the
IN-PROGRESS state a typed object can't hold on its own.

## Current work
(what you're doing right now — the thread that would be lost if this session ended mid-turn)

## Key ids
(threads, decisions, commits worth a successor's first glance — short ids, one line each)

## Working notes
(anything else worth carrying forward that doesn't fit the graph's typed objects)
"""


async def _handle_of(pool: asyncpg.Pool, agent_id: str) -> str | None:
    """The lineage's claimed handle (freshest generation's assertion), or None — an office
    is NAMED for its seat, so an anonymous lineage has nothing to name one after."""
    from src.orchestrator.agents import _generation

    base = _generation(agent_id)[0]
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT a.value #>> '{}' FROM current_assertions a "
        "JOIN objects o ON o.id=a.object_id "
        "WHERE a.name='handle' AND o.type='Agent' "
        "AND (o.canonical=$1 OR o.canonical LIKE $1 || '-%') "
        "ORDER BY a.observed_at DESC LIMIT 1", base)


async def _establish_pure_seat_office(
    actions: Actions, *, seat_id: str, actor: str | None,
    office_root: Path | None, projects_root: Path | None, claude_json: Path | None,
) -> dict[str, Any]:
    """The office ceremony for a seat with NO Agent lineage at all (thread 236d3940) — every
    fact comes off the Seat record alone, since there is no claimed occupant to resolve a
    handle/house/deed through. `seat_facts` already derives house the same way the
    agent-lineage path does (`derive_house`, ruling ff6148b0); the charter is the SEAT's own
    (ruling 1db1ff41 — `governs` is keyed on the seat, not any occupant), so it reads
    straight off `seat_id`, occupied or not; `file_office_deed` is skipped outright — it
    is Agent-only by construction (handshake.py), and there is nothing to deed until an
    actual agent launches and claims this seat."""
    from src.orchestrator.charter import charter_of
    from src.orchestrator.seats import peer_of_seat, seat_facts

    facts = await seat_facts(actions.pool, seat_id)
    handle = facts["handle"]
    if not handle:
        return {"error": f"{seat_id} has no handle on record — an office is named for its "
                         "seat's handle, and this one has none"}
    house = facts["house"]
    if not house:
        return {"error": f"{seat_id} has no derivable house — nothing to pin at an office"}
    root = office_root or _DEFAULT_OFFICE_ROOT
    office = root / handle.lower()
    office.mkdir(parents=True, exist_ok=True)
    seat_line = f" — durable identity `{seat_id}`."
    repos = await charter_of(actions.pool, seat_id)
    charter_block = (
        "You govern: " + ", ".join(f"`{r}`" for r in repos) + "." if repos else
        "Your charter was never formally declared — it lives only in prose. First act: "
        "`charter(repos=[...])` naming the repos you actually govern. A house is what a "
        "seat GOVERNS, not where it sits.")
    peer_addendum = "\n"
    peer_seat = await peer_of_seat(actions.pool, seat_id)
    if peer_seat is not None:
        peer_handle = await actions.pool.fetchval(
            "SELECT a.value #>> '{}' FROM objects o JOIN current_assertions a "
            "ON a.object_id=o.id AND a.name='handle' WHERE o.canonical=$1 "
            "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", peer_seat)
        peer_addendum = _peer_addendum(peer_seat, peer_handle)
    orders = office / "CLAUDE.md"
    if orders.exists():
        orders_state = "left in place — the office already has standing orders"
    else:
        from src.orchestrator.boot_compiler import (
            compile_managed_body,
            template_version,
            wrap_managed,
        )

        body = await compile_managed_body(
            actions, seat_id=seat_id, handle=handle, house=house, office=str(office),
            seat_line=seat_line, charter_block=charter_block, peer_block=peer_addendum)
        orders.write_text(wrap_managed(body, template_version()))
        orders_state = "written"
    charter_file = office / "charter.md"
    if charter_file.exists():
        charter_file_state = "left in place — the seat's own live state, never overwritten"
    else:
        charter_file.write_text(_CHARTER_TEMPLATE.format(handle=handle))
        charter_file_state = "written"
    rebind = await rebind_seat(
        actions, seat_or_agent=seat_id, new_cwd=str(office), actor=actor,
        projects_root=projects_root, claude_json=claude_json, extract=True)
    return {
        "office": str(office), "handle": handle, "house": house,
        "office_deed": "n/a — no claimed occupant yet to deed an office to",
        "seat": seat_id,
        "charter": repos or "UNDECLARED — the standing orders instruct the seat to declare",
        "standing_orders": orders_state,
        "charter_file": charter_file_state,
        "rebind": rebind,
        "launch": f"cd {office} && claude   (or claude --resume there)",
        "note": f"{handle}'s office stands at {office} — no agent has ever claimed this "
                "seat, so this ceremony ran off the Seat record alone; the first launch at "
                "this office is what claims it",
    }


async def establish_office(
    actions: Actions, *, seat_or_agent: str, actor: str | None = None,
    office_root: Path | None = None, projects_root: Path | None = None,
    claude_json: Path | None = None,
) -> dict[str, Any]:
    """The whole ceremony, one receipt: resolve the seat, write its standing orders
    (never clobbering — an occupied office's orders may be hand-tuned), then
    `rebind_seat(extract=True)` into ~/.osiris/seats/<handle>/. Refuses loudly on an
    unknown seat and on an ANONYMOUS lineage (claim_name first — an office is named for
    its seat). Idempotent: re-running converges on the same office.

    THE PURE SEAT PATH (thread 236d3940, mirroring rebind_seat's 3ae57d36 fix): a seat
    minted by mint_seat/ensure_seat but never claim_name'd by any agent (grantprobe's real
    shape) used to resolve to NO agent at all here — 'grantprobe' matches neither a claimed
    handle nor an Agent canonical — so this refused outright before an office could ever be
    built for the house's next never-yet-launched worker. An explicit SEAT identifier (its
    own canonical, or its `handle` property) is now checked directly whenever agent
    resolution supplies nothing, and a hit there builds the office off the Seat record
    alone: `seat_facts` supplies handle/house (house already DERIVED there), charter is read
    through the seat's occupant if it has ever had one (usually none), and `file_office_deed`
    is skipped entirely (it is Agent-only by construction — nothing to deed an office to
    until someone actually claims this seat by launching in it)."""
    from src.orchestrator.agents import house_of, resolve_handle
    from src.orchestrator.seats import held_seat

    seat_or_agent = (seat_or_agent or "").strip()
    agent_id = await resolve_handle(actions, seat_or_agent) if seat_or_agent else None
    if agent_id is None and seat_or_agent:
        exists = await actions.pool.fetchval(
            "SELECT 1 FROM objects WHERE canonical=$1 AND type='Agent' AND status='active'",
            seat_or_agent)
        agent_id = seat_or_agent if exists else None
    direct_seat_id: str | None = None
    if seat_or_agent:
        direct_seat_id = await actions.pool.fetchval(
            "SELECT o.canonical FROM objects o WHERE o.type='Seat' AND o.status='active' "
            "AND (o.canonical=$1 OR EXISTS (SELECT 1 FROM current_assertions a "
            "WHERE a.object_id=o.id AND a.name='handle' AND a.value #>> '{}' = $1))",
            seat_or_agent)
    if agent_id is None and direct_seat_id is None:
        return {"error": f"no such seat or agent: {seat_or_agent!r} — an office ceremony "
                         "never invents its occupant"}
    if agent_id is None:
        # direct_seat_id is guaranteed set here (the refusal above already ruled out both
        # being None) — mirrors rebind_seat's own PURE SEAT PATH assert exactly.
        assert direct_seat_id is not None
        return await _establish_pure_seat_office(
            actions, seat_id=direct_seat_id, actor=actor, office_root=office_root,
            projects_root=projects_root, claude_json=claude_json)
    handle = await _handle_of(actions.pool, agent_id)
    if not handle:
        return {"error": f"{agent_id} has never claimed a name — an office is named for its "
                         "seat. claim_name first, then establish the office"}
    house = await house_of(actions.pool, agent_id)
    if not house:
        return {"error": f"{agent_id} has no durable project label — it has never been "
                         "mounted in a project, so there is no house to pin at an office"}
    # A LIVE SEAT IS NEVER MOVED (the rollout guard): extraction relocates the lineage's
    # transcripts, and a running harness process appends to its own by path — moving it
    # mid-tab splits the session's history between two slugs. The ceremony waits for a
    # quiet seat (lineage-wide: a live heir blocks moving the base); close the tab,
    # establish, relaunch at the office.
    from src.orchestrator.agents import _generation

    base = _generation(agent_id)[0]
    fresh = await actions.pool.fetchval(
        "SELECT max(last_seen) FROM agent_mounts "
        "WHERE (agent_id=$1 OR agent_id LIKE $1 || '-%') "
        "AND last_seen > now() - interval '15 minutes'", base)
    if fresh is not None:
        return {"error": f"{handle} ({agent_id}) is LIVE right now (last seen "
                         f"{fresh.isoformat()}) — moving a live seat splits its running "
                         "session's history between two homes. Close its tab first, "
                         "then establish; it wakes up in the office"}
    root = office_root or _DEFAULT_OFFICE_ROOT
    office = root / handle.lower()
    office.mkdir(parents=True, exist_ok=True)
    bound = await held_seat(actions.pool, agent_id)
    seat_line = (f" — durable identity `{bound['seat_id']}`." if bound else
                 " — not yet seated: your next claim binds you (the on-ramp).")
    # THE CHARTER IS THE SEAT'S (ruling 1db1ff41) — not the lineage's: reads through the
    # SAME `bound` this function already resolved for seat_line, one line up, no second
    # lookup and no lineage-string walk. An agent not yet seated has no charter to read.
    from src.orchestrator.charter import charter_of

    repos = await charter_of(actions.pool, bound["seat_id"]) if bound else []
    charter_block = (
        "You govern: " + ", ".join(f"`{r}`" for r in repos) + "." if repos else
        "Your charter was never formally declared — it lives only in prose. First act: "
        "`charter(repos=[...])` naming the repos you actually govern. A house is what a "
        "seat GOVERNS, not where it sits.")
    # THE PEER ADDENDUM (ruling d74492ee, spec e6636c7e): computed LIVE, like seat_line
    # and charter_block above it — a peer bonded after a fresh mint's scaffold still shows
    # up here, the one place that reads the graph instead of a fixed mint-time default.
    peer_addendum = "\n"
    if bound is not None:
        from src.orchestrator.seats import peer_of_seat

        peer_seat = await peer_of_seat(actions.pool, bound["seat_id"])
        if peer_seat is not None:
            peer_handle = await actions.pool.fetchval(
                "SELECT a.value #>> '{}' FROM objects o JOIN current_assertions a "
                "ON a.object_id=o.id AND a.name='handle' WHERE o.canonical=$1 "
                "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", peer_seat)
            peer_addendum = _peer_addendum(peer_seat, peer_handle)
    orders = office / "CLAUDE.md"
    if orders.exists():
        orders_state = "left in place — the office already has standing orders"
    else:
        from src.orchestrator.boot_compiler import (
            compile_managed_body,
            template_version,
            wrap_managed,
        )

        body = await compile_managed_body(
            actions, seat_id=bound["seat_id"] if bound else None, handle=handle,
            house=house, office=str(office), seat_line=seat_line,
            charter_block=charter_block, peer_block=peer_addendum)
        orders.write_text(wrap_managed(body, template_version()))
        orders_state = "written"
    # THE CHARTER FILE, never clobbered (d80621a7 piece 3): an occupied office's charter is
    # the seat's own hand-maintained live state — alfred's stays his, exactly like CLAUDE.md.
    charter_file = office / "charter.md"
    if charter_file.exists():
        charter_file_state = "left in place — the seat's own live state, never overwritten"
    else:
        charter_file.write_text(_CHARTER_TEMPLATE.format(handle=handle))
        charter_file_state = "written"
    rebind = await rebind_seat(
        actions, seat_or_agent=agent_id, new_cwd=str(office), actor=actor,
        projects_root=projects_root, claude_json=claude_json, extract=True)
    # THE DEED (a2d06410): the ceremony records office ownership in the GRAPH — the
    # fourth door must survive the seat's death, and mount rows don't (SessionEnd
    # releases them; Ra's ended lineage held none, so every fresh launch at his own
    # office minted a stranger). The deed is what office_seat reads first.
    from src.orchestrator.handshake import file_office_deed

    deeded = await file_office_deed(
        actions, agent_id=agent_id, cwd=str(office),
        actor=actor or "ceremony:establish-office", office_root=root)
    return {
        "office": str(office), "handle": handle, "house": house,
        "office_deed": "filed" if deeded else "already on the lineage's record",
        "seat": bound["seat_id"] if bound else None,
        "charter": repos or "UNDECLARED — the standing orders instruct the seat to declare",
        "standing_orders": orders_state,
        "charter_file": charter_file_state,
        "rebind": rebind,
        "launch": f"cd {office} && claude   (or claude --resume there)",
        "note": f"{handle}'s office stands at {office} — the whisper will mount house "
                f"{house} from the pin; transcripts moved are re-addressed so resume "
                "works in place",
    }
