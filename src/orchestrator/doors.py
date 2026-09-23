"""doors(ref): one coherent answer about an agent, a seat, or a working directory.

`agent_mounts` is doing five jobs at once: registry, liveness, mail address, seat binding,
office anchor. Callers were hand-rolling their own queries against it to re-derive an
answer it already has. Two of those jobs already have a rich, correct authority this
verb leans on rather than re-implements: seat binding is `held_seat`/`seat_occupancy`, off
the `holds` graph link, never `agent_mounts.seat_id` (a one-directional cache written
alongside the link and backfilled from it when null, never trusted as truth); mail address
is `resolve_seat`, the richest existing consolidator (name to live seat, with candidates and
an honest warning). The genuinely unserved job is office anchor: working directory to agent
had no general verb anywhere in the corpus. `handshake.office_seat` answers only for the
`~/.osiris/seats/<handle>` convention, and every other cwd resolution (folds.py,
daemon.py, trigger.py) was ad hoc raw SQL against `agent_mounts.cwd`.

`doors(pool, ref)` sniffs `ref`, an `agent:` id, a `seat:` id, a bare handle, or an absolute
cwd path (`/...` or `~/...`), and returns one uniform shape regardless of which:
`{ref, resolved, matches: [...]}`. An agent:/seat:/handle ref resolves to zero or one match
(a lineage-folded identity, same discipline as everywhere else in this graph); a cwd resolves
to zero or many (an office can be multi-tenant: one has shown four sessions where one lived).
The uniform list means no caller ever branches on return type, only on `len(matches)`.

Every match, regardless of how it was found, is the same record shape, built by re-deriving
that lineage's current state (freshest mount row across every generation, current seat off
the link), never the stale row that matched the query. For a cwd lookup this means a match's
own `cwd` field can differ from the path you asked about. That is the honest answer to "is
this office's occupant still actually there," not a bug: a soul that mounted here once and
has since moved shows exactly that, rather than freezing them at a memory."""
from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

import asyncpg


def _normed(cwd: str) -> str:
    return os.path.normpath(os.path.expanduser(cwd)) if cwd else ""


async def _record(
    pool: asyncpg.Pool, agent_id: str, *, resolved_via: str,
    agents_json: Any = None, read_exe: Any = None, read_cwd: Any = None,
) -> dict[str, Any]:
    """The one coherent record for a known or guessed agent id: that lineage's freshest mount
    row (registry, liveness, and office anchor, folded across every generation) plus its seat
    off the `holds` link (never the agent_mounts.seat_id cache column).

    `live` is a real gate, not just display: `lift()`'s own pre-claim_name refusal
    reads this exact field to decide whether a target is too live to lift. Unlike this
    project's other cache-based liveness reads (co_agents, fleet_pulse), this one actually
    blocks an action, and both of `doors()`'s own callers (this MCP tool, `lift()`) are
    rare, deliberate calls, never a per-mount/per-message path run at high frequency, so
    cross-checking the real harness+/proc authority here is both correct and affordable. A
    fresh mount row alone no longer counts as live; it must be registry_census-confirmed too."""
    from src.orchestrator.agents import _generation, is_occupied_by_a_live_body
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
    if live:
        live = await is_occupied_by_a_live_body(
            pool, str(row["agent_id"]),
            agents_json=agents_json, read_exe=read_exe, read_cwd=read_cwd)
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
        # Today, liveness is delivery-worthiness for this identity lookup. The finer dispatch
        # question (pull-only manager vs an injectable worker) belongs to mailbox.py/trigger.py's
        # own dispatch functions, a different concern from "what do I know about this identity".
        "reachable": {"mail": live},
        "resolved_via": resolved_via,
    }


async def doors(
    pool: asyncpg.Pool, ref: str, *,
    agents_json: Any = None, read_exe: Any = None, read_cwd: Any = None,
) -> dict[str, Any]:
    """The one steward read verb for 'what do I know about this agent / seat / cwd', replacing
    a hand-rolled query against agent_mounts with a single call. Sniffs `ref`: 'agent:...' means
    that lineage's identity; 'seat:...' means that seat's current holder (vacant/cold seats
    resolve to no match, never a phantom record); a bare name uses resolve_seat's handle
    resolution; an absolute path ('/...' or '~/...') means every distinct lineage (soul-folded)
    that has ever mounted there.

    `agents_json`/`read_exe`/`read_cwd` are the same injection seam `_record`'s own harness
    confirmation check uses (default: the real census). Both of this verb's own callers
    (the MCP tool, `lift()`'s own pre-claim refusal) are rare, deliberate calls, never run
    at high frequency, so paying that real cost here is affordable and correct."""
    ref = (ref or "").strip()
    matches: list[dict[str, Any]] = []
    kw = {"agents_json": agents_json, "read_exe": read_exe, "read_cwd": read_cwd}

    if ref.startswith("agent:"):
        rec = await _record(pool, ref, resolved_via="agent-id", **kw)
        if rec["last_seen"] is not None or rec["seat"] is not None:
            matches = [rec]
    elif ref.startswith("seat:"):
        from src.orchestrator.seats import seat_occupancy

        occ = await seat_occupancy(pool, ref)
        if occ["holder"]:
            matches = [await _record(pool, occ["holder"], resolved_via="seat-link", **kw)]
    elif ref.startswith("/") or ref.startswith("~"):
        from src.orchestrator.agents import SOUL_SQL_TEMPLATE

        cwd = _normed(ref)
        souls = await pool.fetch(
            "SELECT DISTINCT ON (soul) soul FROM ("
            f"  SELECT agent_id, {SOUL_SQL_TEMPLATE.format(col='agent_id')} AS soul, last_seen "
            "  FROM agent_mounts WHERE cwd=$1"
            ") s ORDER BY soul, last_seen DESC NULLS LAST", cwd)
        for s in souls:
            matches.append(await _record(pool, s["soul"], resolved_via="cwd", **kw))
    else:
        from src.actions.core import Actions
        from src.orchestrator.agents import resolve_seat
        from src.orchestrator.seats import seat_holder_ineligible

        # The same guard send() uses: a name whose unique seat has only ineligible holders
        # would otherwise fall to resolve_seat's un-seated-lineage fallback and confidently
        # return some other, older, unmarked generation, a wrong-but-real-looking match for
        # a pure lookup tool, worse than an honest miss. doors() never refuses (it's
        # read-only), so this distinguishes rather than escalates: zero matches, plus a note
        # naming exactly why, instead of a fabricated "resolved".
        ineligible = await seat_holder_ineligible(pool, ref)
        if ineligible is not None:
            return {"ref": ref, "resolved": False, "matches": [], "note": ineligible}
        # resolve_seat's own harness cross-check is a separate fix, not yet merged here,
        # so no kw is threaded through it yet.
        found = await resolve_seat(Actions(pool), ref)
        if found["agent"]:
            matches = [await _record(pool, found["agent"], resolved_via="handle", **kw)]

    return {"ref": ref, "resolved": bool(matches), "matches": matches}
