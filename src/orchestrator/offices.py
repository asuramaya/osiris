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

_STANDING_ORDERS = """\
# {handle} — seat office

This is your OFFICE (`{office}/`), not a code repo. The seat-offices ruling
(ed5f5ce2): agents sit in Osiris-owned homes; code stays in the code repos you GOVERN.
Launch here, resume here — this path never moves again, whatever happens to the repos.

## Who you are
- Seat: **{handle}**, house **{house}**{seat_line}
- The `.osiris` file beside this pins your house; your mail, attribution, and lineage
  all key on it. Do not edit either file's `project` line.
- Osiris knows this office as your anchor. `mount(cwd=<this dir>)`, then `orient()`.
- Your cwd IS this office — whatever an older conversation of yours remembers. A resumed
  memory of a former home is stale the moment you read it here.

## Your charter
{charter_block}

## How to work from an office
- Your code lives elsewhere. Work on it with absolute paths, or `cd` within commands;
  each repo's own CLAUDE.md loads as you touch its files — read the boot sector of any
  repo before real work in it.
- Seat-local state (notes, scratch, drafts) belongs HERE — never in a repo behind a
  fragile .gitignore. That class of mess is what this office exists to end.

## Ritual (unchanged)
Write back AS YOU GO: `record_decision` / `open_thread` (kind='obligation') /
`resolve_thread`. A session can die at any instant; what is not in the graph does not
exist. The fleet DMs you as send(to_agent='{handle}') — the seat is the address now; mail
waits for you across successions.
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


async def _lineage_charter(pool: asyncpg.Pool, agent_id: str) -> list[str]:
    """The lineage's ACTIVE governs links (charter_of is exact-id; a charter declared by an
    earlier generation still rules — the seat governs, not the session that spoke)."""
    from src.orchestrator.agents import _generation

    base = _generation(agent_id)[0]
    rows = await pool.fetch(
        "SELECT DISTINCT p.canonical FROM links l "
        "JOIN objects a ON a.id=l.from_id AND a.type='Agent' "
        "AND (a.canonical=$1 OR a.canonical LIKE $1 || '-%') "
        "JOIN objects p ON p.id=l.to_id AND p.type='SoftwareProject' "
        "WHERE l.type='governs' AND (l.valid_until IS NULL OR l.valid_until > now())",
        base)
    return sorted(r["canonical"].removeprefix("repo:") for r in rows)


async def establish_office(
    actions: Actions, *, seat_or_agent: str, actor: str | None = None,
    office_root: Path | None = None, projects_root: Path | None = None,
    claude_json: Path | None = None,
) -> dict[str, Any]:
    """The whole ceremony, one receipt: resolve the seat, write its standing orders
    (never clobbering — an occupied office's orders may be hand-tuned), then
    `rebind_seat(extract=True)` into ~/.osiris/seats/<handle>/. Refuses loudly on an
    unknown seat and on an ANONYMOUS lineage (claim_name first — an office is named for
    its seat). Idempotent: re-running converges on the same office."""
    from src.orchestrator.agents import house_of, resolve_handle
    from src.orchestrator.seats import held_seat

    seat_or_agent = (seat_or_agent or "").strip()
    agent_id = await resolve_handle(actions, seat_or_agent) if seat_or_agent else None
    if agent_id is None and seat_or_agent:
        exists = await actions.pool.fetchval(
            "SELECT 1 FROM objects WHERE canonical=$1 AND type='Agent' AND status='active'",
            seat_or_agent)
        agent_id = seat_or_agent if exists else None
    if agent_id is None:
        return {"error": f"no such seat or agent: {seat_or_agent!r} — an office ceremony "
                         "never invents its occupant"}
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
    repos = await _lineage_charter(actions.pool, agent_id)
    charter_block = (
        "You govern: " + ", ".join(f"`{r}`" for r in repos) + "." if repos else
        "Your charter was never formally declared — it lives only in prose. First act: "
        "`charter(repos=[...])` naming the repos you actually govern. A house is what a "
        "seat GOVERNS, not where it sits.")
    orders = office / "CLAUDE.md"
    if orders.exists():
        orders_state = "left in place — the office already has standing orders"
    else:
        orders.write_text(_STANDING_ORDERS.format(
            handle=handle, office=office, house=house, seat_line=seat_line,
            charter_block=charter_block))
        orders_state = "written"
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
        "rebind": rebind,
        "launch": f"cd {office} && claude   (or claude --resume there)",
        "note": f"{handle}'s office stands at {office} — the whisper will mount house "
                f"{house} from the pin; transcripts moved are re-addressed so resume "
                "works in place",
    }
