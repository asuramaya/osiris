"""OWNER NORMALIZATION (thread 6d8f87a3, decision 0d863363 item 2, ruling 0d863363's own
"ships mechanically, never a coordinator's hand pass" mandate) -- every OPEN obligation
Thread whose `owner` is one of three recognizable non-durable shapes gets a compensating
assertion pointing it at a durable one:

  (a) a bare PROJECT NAME (e.g. "ballgem", not "seat:..." or a seat handle) resolves to
      that project's COORDINATING SEAT -- reuses roster()'s own hard-won agreement
      classification (governed/shared-house/single-match/conflict/no-match, obligation_
      hygiene.py's resolve_owner_target ladder rung 1's own precedent) rather than a
      second, cheaper, and inevitably drifting re-derivation of the same judgment calls.
  (b) a DEAD/RETIRED agent id resolves to that lineage's live head (agents.py's own
      `lineage_head`, the same forward succeeded_by walk send_message's reply-routing
      already trusts) -- "dead" is never read off objects.status (a superseded ancestor
      can stay status='active' forever, lineage_head's own documented husk gap), only
      whether lineage_head's own walk lands somewhere else.
  (c) an EMPTY owner (no current assertion, or an empty string) on a thread that DOES
      carry a `repo` property resolves the same way as (a), via that repo.

A row this can't resolve a coordinator for (no-match/conflict/shared-house-with-no-
manager, or an empty owner with no repo to resolve from at all) is never guessed at --
it folds into ONE Thread on the operator's own backlog per affected project (never one
row-per-obligation; the whole point is the desk stops being the pile, obligation_hygiene.
py's own "128 of 151 nudges landing on the operator's desk" specimen is exactly the
failure this folding avoids), with a STABLE summary text carrying no live count (today's
own landing-auditor lesson, commit 81fcad6: an embedded count breaks open_thread's own
dedup-on-summary and mints one thread per run instead of one, ever).

Compensating, never destructive: `assert_property` at the SAME evidence class the prior
owner almost always carries (self_declared) writes a NEW current row from a NEW source
(`migration:<name>`) -- the prior owner's own assertion stays exactly where it was, under
its own source, in history (constitution's own compensating-event law); this is ordinary
multi-source corroboration, the same shape decision d873301c's own census/retire pass
already normalized 4,922 other (object,name) pairs through this session.

A live agent id, a `seat:` canonical, a bare seat HANDLE (checked before the project-name
rung -- resolve_owner_target's own rung 0 correction, msg 7425: a seat's bare handle and a
project's bare name are indistinguishable strings), or the literal 'operator' are already
durable or already-routed and are left untouched.
"""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

MIGRATION_SOURCE = "migration:0059_owner_normalization"
_EC = EvidenceClass.SELF_DECLARED.value
_CONF = confidence_for(EvidenceClass.SELF_DECLARED)


async def _coordinating_seat_for_project(
    pool: asyncpg.Pool, project: str,
) -> tuple[str | None, str | None]:
    """The durable SEAT that coordinates `project` -- roster()'s own agreement
    classification, stopped at the seat id (never descended to a live holder: this writes
    a permanent owner, not a DM target, so occupancy must never enter the choice).
    (seat_id, None) on a clean resolution; (None, reason) otherwise, never guessed."""
    from src.orchestrator.seats import manager_of_seat, peer_of_seat, roster

    out = await roster(pool, repo=project)
    agreement, matches = out.get("agreement"), out.get("matches") or []

    if agreement == "conflict" and len(matches) == 2:
        seat_a, seat_b = matches[0]["seat"], matches[1]["seat"]
        if await peer_of_seat(pool, seat_a) == seat_b:
            return None, (f"peer pair for project {project!r} ({seat_a}, {seat_b}) -- "
                          "shared ownership, no single coordinator to pick")
        manager_seat = (
            seat_b if await manager_of_seat(pool, seat_a) == seat_b else
            seat_a if await manager_of_seat(pool, seat_b) == seat_a else None)
        if manager_seat is not None:
            return manager_seat, None
        seats = ", ".join(m["seat"] for m in matches)
        return None, f"ambiguous: {len(matches)} seats claim project {project!r} ({seats})"

    if agreement == "governed":
        chosen = next((m for m in matches if "charter" in m["via"]), None)
        if chosen is not None:
            return chosen["seat"], None
        return None, f"governed project {project!r} has no charter-holding seat"

    if agreement == "shared-house":
        manager_seat = out.get("manager")
        if manager_seat:
            return manager_seat, None
        return None, f"shared-house project {project!r} has no manager on record"

    if agreement == "single-match":
        return matches[0]["seat"], None

    if agreement == "no-match":
        return None, f"no seat's charter or pin names project {project!r}"

    seats = ", ".join(m["seat"] for m in matches) if matches else ""
    reason = (f"ambiguous project {project!r} ({seats})" if seats
              else f"no coordinator for project {project!r}")
    return None, reason


async def classify_thread_owner(
    pool: asyncpg.Pool, owner: str | None, repo: str | None,
) -> dict[str, Any]:
    """One thread's own owner, classified against the three rewritable shapes. Returns
    `{"class": "ok"|"project_name"|"dead_agent"|"empty", "new_owner": <seat canonical>|
    None, "project": <the project this verdict resolved against, for no-coordinator
    folding>|None, "reason": <why new_owner is None, when class != "ok">|None}`. "ok"
    covers everything already durable or intentionally left alone -- never touched."""
    from src.orchestrator.agents import lineage_head
    from src.orchestrator.obligation_hygiene import _project_name_for

    raw = (owner or "").strip()

    if not raw:
        if not repo:
            return {"class": "empty", "new_owner": None, "project": None,
                    "reason": "no repo on this thread -- nothing to resolve a coordinator "
                             "from"}
        seat, reason = await _coordinating_seat_for_project(pool, repo)
        return {"class": "empty", "new_owner": seat, "project": repo, "reason": reason}

    if raw.lower() == "operator" or raw.startswith("seat:"):
        return {"class": "ok", "new_owner": None, "project": None, "reason": None}

    if raw.startswith("agent:"):
        # "dead/retired generation" is never read off objects.status -- a superseded
        # ancestor can stay status='active' forever (lineage_head's own documented husk
        # gap: false_mint healing never flips it). lineage_head's own forward walk is the
        # one authority for "is this id still the name a live mind answers to."
        head = await lineage_head(pool, raw)
        if head != raw:
            return {"class": "dead_agent", "new_owner": head, "project": None, "reason": None}
        return {"class": "ok", "new_owner": None, "project": None, "reason": None}

    is_seat_handle = await pool.fetchval(
        "SELECT 1 FROM objects o JOIN current_assertions a ON a.object_id=o.id "
        "WHERE o.type='Seat' AND o.status='active' AND a.name='handle' "
        "AND lower(a.value #>> '{}') = lower($1) LIMIT 1", raw)
    if is_seat_handle:
        return {"class": "ok", "new_owner": None, "project": None, "reason": None}

    project_name = await _project_name_for(pool, raw)
    if project_name is not None:
        seat, reason = await _coordinating_seat_for_project(pool, project_name)
        return {"class": "project_name", "new_owner": seat, "project": project_name,
               "reason": reason}

    return {"class": "ok", "new_owner": None, "project": None, "reason": None}


_OPEN_OBLIGATIONS_SQL = """
    SELECT o.id, o.canonical,
        (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id
         AND a.name='owner' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1)
        AS owner,
        (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id
         AND a.name='repo' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1)
        AS repo
    FROM objects o
    WHERE o.type='Thread' AND o.status='active'
      AND EXISTS (SELECT 1 FROM current_assertions ca WHERE ca.object_id=o.id
                  AND ca.name='kind' AND ca.value #>> '{}' = 'obligation')
      AND EXISTS (SELECT 1 FROM current_assertions ca WHERE ca.object_id=o.id
                  AND ca.name='status' AND ca.value #>> '{}' = 'open')
"""


async def plan_owner_normalization(pool: asyncpg.Pool) -> dict[str, Any]:
    """DRY RUN -- never writes. Every open obligation whose owner needs normalizing, plus
    the folded no-coordinator surfacing grouped by project (or, for an empty owner with no
    repo at all, grouped under `None`)."""
    rows = await pool.fetch(_OPEN_OBLIGATIONS_SQL)
    resolved: list[dict[str, Any]] = []
    no_coordinator: dict[str | None, list[dict[str, Any]]] = {}
    for row in rows:
        verdict = await classify_thread_owner(pool, row["owner"], row["repo"])
        if verdict["class"] == "ok":
            continue
        entry = {"id": row["id"], "thread": row["canonical"], "current_owner": row["owner"],
                 "repo": row["repo"], **verdict}
        if verdict["new_owner"] is None:
            no_coordinator.setdefault(verdict["project"], []).append(entry)
        else:
            resolved.append(entry)
    return {
        "resolved": resolved, "no_coordinator": no_coordinator,
        "obligations_scanned": len(rows),
        "resolved_count": len(resolved),
        "no_coordinator_projects": len(no_coordinator),
    }


async def apply_owner_normalization(actions: Actions) -> dict[str, Any]:
    """Applies `plan_owner_normalization`'s own plan: a compensating owner assertion per
    resolved row, and one folded Thread per project with no resolvable coordinator (never
    one per obligation -- the desk-is-the-pile failure this exists to avoid). Idempotent
    in RESULT (the winning owner value never drifts on a re-run) though not in ROW COUNT
    (assert_property mints a fresh same-value row at a later observed_at each run --
    "confirmed still true at T2" is real information, assert_property's own documented
    law, never a reason to special-case a skip here)."""
    from src.orchestrator.capture import open_thread

    now = datetime.now(UTC)
    plan = await plan_owner_normalization(actions.pool)
    written = []
    for entry in plan["resolved"]:
        assert entry["new_owner"] is not None
        await actions.assert_property(
            entry["id"], "owner", entry["new_owner"], MIGRATION_SOURCE, now, _CONF,
            evidence_class=_EC)
        written.append({"thread": entry["thread"], "new_owner": entry["new_owner"]})
    surfaced = []
    for project, entries in plan["no_coordinator"].items():
        label = project or "(no project on record)"
        summary = (f"OWNER NORMALIZATION: no coordinating seat resolves for {label}'s "
                   "open obligations -- migration 0059 could not pick one mechanically, "
                   "needs a human call")
        thread_id = await open_thread(
            actions, summary, kind="question", owner="operator", repo=project,
            source=MIGRATION_SOURCE,
            unlinked_because="owner-normalization migration surfacing a gap it "
                             "deliberately refuses to guess at (thread 6d8f87a3)")
        surfaced.append({"project": project, "thread": str(thread_id),
                         "obligation_count": len(entries)})
    return {"written": written, "surfaced": surfaced,
           "obligations_scanned": plan["obligations_scanned"]}
