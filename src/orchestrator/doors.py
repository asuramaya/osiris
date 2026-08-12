"""doors(ref) — one coherent answer about an agent, a seat, or a cwd (thread 1aa2ff36, Wave 2).

`agent_mounts` is doing five jobs at once — registry, liveness, mail-address, seat-binding,
office-anchor — and 192 hand-rolled queries across the corpus is the bill for every caller
re-deriving its own answer. Two of those jobs already have a rich, correct authority this
verb leans on rather than re-implements: seat-binding is `held_seat`/`seat_occupancy`, off
the `holds` GRAPH LINK — NEVER `agent_mounts.seat_id`, a one-directional cache written
alongside the link and backfilled FROM it when NULL, never trusted as truth; mail-address is
`resolve_seat`, the richest existing consolidator (name → live seat, with candidates and an
honest warning). The genuinely UNSERVED job is OFFICE-ANCHOR: cwd → agent has no general
verb anywhere in the corpus — `handshake.office_seat` answers only for the
`~/.osiris/seats/<handle>` deed convention, and every other cwd resolution (folds.py,
daemon.py, trigger.py) is ad hoc raw SQL against `agent_mounts.cwd`.

`doors(pool, ref)` sniffs `ref` — an `agent:` id, a `seat:` id, a bare handle, or an absolute
cwd path (`/...` or `~/...`) — and returns ONE UNIFORM shape regardless of which:
`{ref, resolved, matches: [...]}`. An agent:/seat:/handle ref resolves to zero-or-ONE match (a
lineage-folded identity, same discipline as everywhere else in this graph); a cwd resolves to
zero-or-MANY (an office can be multi-tenant — Ptah's once showed four bodies where one lived).
The uniform list means no caller ever branches on return TYPE, only on `len(matches)`.

Every match, regardless of how it was found, is the SAME record shape — built by re-deriving
that lineage's CURRENT state (freshest mount row across every generation, current seat off
the link), never the stale row that matched the query. For a cwd lookup this means a match's
own `cwd` field can differ from the path you asked about — that IS the honest answer to "is
this office's occupant still actually there," not a bug: a soul that mounted here once and
has since moved shows exactly that, rather than freezing them at a memory."""
from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

import asyncpg


def _normed(cwd: str) -> str:
    return os.path.normpath(os.path.expanduser(cwd)) if cwd else ""


async def _record(pool: asyncpg.Pool, agent_id: str, *, resolved_via: str) -> dict[str, Any]:
    """The one coherent record for a KNOWN-OR-GUESSED agent id: that lineage's freshest mount
    row (registry + liveness + office-anchor, folded across every generation) plus its seat
    off the `holds` link (never the agent_mounts.seat_id cache column)."""
    from src.orchestrator.agents import _generation
    from src.orchestrator.seats import held_seat

    base = _generation(agent_id)[0]
    row = await pool.fetchrow(
        "SELECT agent_id, project, cwd, model, last_seen "
        "FROM agent_mounts WHERE agent_id=$1 OR agent_id LIKE $1 || '-%' "
        "ORDER BY last_seen DESC NULLS LAST LIMIT 1", base)
    seat = await held_seat(pool, agent_id)
    last_seen = row["last_seen"] if row else None
    age = (datetime.now(UTC) - last_seen).total_seconds() if last_seen else None
    live = age is not None and age < 900
    return {
        "agent_id": row["agent_id"] if row else agent_id,
        "seat": ({"seat_id": seat["seat_id"], "handle": seat["handle"], "house": seat["house"]}
                if seat else None),
        "project": row["project"] if row else None,
        "cwd": row["cwd"] if row else None,
        "model": row["model"] if row else None,
        "live": live,
        "last_seen": last_seen.isoformat() if last_seen else None,
        "age_secs": age,
        # today, liveness IS delivery-worthiness for this identity lookup — the finer dispatch
        # question (pull-only manager vs an injectable worker) belongs to mailbox.py/trigger.py's
        # own dispatch functions, a different concern from "what do I know about this identity".
        "reachable": {"mail": live},
        "resolved_via": resolved_via,
    }


async def doors(pool: asyncpg.Pool, ref: str) -> dict[str, Any]:
    """The one steward read verb for 'what do I know about this agent / seat / cwd' — replacing
    a hand-rolled query against agent_mounts with a single call. Sniffs `ref`: 'agent:...' → that
    lineage's identity; 'seat:...' → that seat's current holder (vacant/cold seats resolve to no
    match, never a phantom record); a bare name → resolve_seat's handle resolution; an absolute
    path ('/...' or '~/...') → every distinct lineage (soul-folded) that has EVER mounted there."""
    ref = (ref or "").strip()
    matches: list[dict[str, Any]] = []

    if ref.startswith("agent:"):
        rec = await _record(pool, ref, resolved_via="agent-id")
        if rec["last_seen"] is not None or rec["seat"] is not None:
            matches = [rec]
    elif ref.startswith("seat:"):
        from src.orchestrator.seats import seat_occupancy

        occ = await seat_occupancy(pool, ref)
        if occ["holder"]:
            matches = [await _record(pool, occ["holder"], resolved_via="seat-link")]
    elif ref.startswith("/") or ref.startswith("~"):
        cwd = _normed(ref)
        souls = await pool.fetch(
            "SELECT DISTINCT ON (soul) soul FROM ("
            "  SELECT agent_id, regexp_replace(agent_id, '-[ivxlcdm]+$', '') AS soul, last_seen "
            "  FROM agent_mounts WHERE cwd=$1"
            ") s ORDER BY soul, last_seen DESC NULLS LAST", cwd)
        for s in souls:
            matches.append(await _record(pool, s["soul"], resolved_via="cwd"))
    else:
        from src.actions.core import Actions
        from src.orchestrator.agents import resolve_seat
        from src.orchestrator.seats import seat_holder_ineligible

        # THE SAME GUARD send() USES (task #142 punch-list item 3): a name whose unique seat
        # has only ineligible holders would otherwise fall to resolve_seat's un-seated-
        # lineage fallback and confidently return some OTHER, older, unmarked generation —
        # a wrong-but-real-looking match for a pure lookup tool, worse than an honest miss.
        # doors() never refuses (it's read-only), so this DISTINGUISHES rather than escalates:
        # zero matches, plus a note naming exactly why, instead of a fabricated "resolved".
        ineligible = await seat_holder_ineligible(pool, ref)
        if ineligible is not None:
            return {"ref": ref, "resolved": False, "matches": [], "note": ineligible}
        found = await resolve_seat(Actions(pool), ref)
        if found["agent"]:
            matches = [await _record(pool, found["agent"], resolved_via="handle")]

    return {"ref": ref, "resolved": bool(matches), "matches": matches}
