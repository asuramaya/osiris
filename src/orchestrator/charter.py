"""THE CHARTER — a house is what a seat RULES, not where it sits (Phase 1 §4.1, `dd47c1da`).

`path = project = identity` is the bug under the whole fold: today a seat's authority over a
repo is only ever implied by works_in — one project, wherever its folder happens to be right
now. alfred's charter is six repos; none of them is "the folder he's sitting in this week". A
CHARTER makes that authority an explicit, first-class fact instead of an inference from cwd,
so a folder move (§4.1's seat-rebind primitive, `mounts.rebind_seat`) has nothing to orphan.

RE-KEYED ONTO THE SEAT (operator ruling 1db1ff41, "declared, all roads lead to explicit"):
`governs` used to originate from whichever AGENT generation happened to declare it — schema.py's
own docstring said "the repos a SEAT rules" while its from_type said Agent only, so the intent
and the schema disagreed inside the same declaration (Lane C, decision 1913683e, proven live: a
charter declared by generation III read back EMPTY for generation IV, since a `governs` link
does not carry forward across succession the way `holds`/`handle`/`mail` do). Keying on the
Seat's own durable object id instead — one seat, one link, forever — closes that gap AND
dissolves `invalidate_link`'s exact-from_id limitation entirely: there is no longer an
ancestor/successor distinction to trip over, because every generation of a lineage resolves to
the SAME from_id. It also solves peer-seat divergence for free (deckard/metron, halcyon/ferryman,
jenny/nebbercracker already have distinct Seat objects) — no house-level charter object needed.

`set_charter` declares the WHOLE charter each call — an idempotent statement of "these are the
repos this seat rules now", not an increment. Repos newly named mint a `governs` link
(SELF_DECLARED, the seat's own act); repos that drop off are healed by a COMPENSATING EVENT
(`links.valid_until`, constitution #3 — never DELETE), so a shrinking charter stays a fact the
graph remembers rather than information destroyed. `charter_of` reads back the currently
active set. Wave 2 builds charter-scoped briefing aggregation on top of this; this lane only
makes the charter a fact and makes it VISIBLE (orient()'s one line).

NO PRIMITIVE MAY MINT A REPO OUT OF A SELF-DECLARED STRING ALONE (Thoth's design constraint,
msg 2402 — atlas's garbled 28-entry charter, `Us`/`apple`/`vector`, came from a MIND typing bad
fragments, not a splitter; there is none in this codebase). `set_charter` never mints a
SoftwareProject: it resolves each requested name through capture.py's own validated
`_resolve_repo` — the SAME check `#107`'s guard runs, i.e. "does the graph already have
independent evidence this repo is real" (a prior git ingest, or an earlier legitimate charter
declaration) — and REFUSES, per-name, whatever it cannot find. A charter is SELF_DECLARED
evidence; it must never be the first and only witness that a repo exists.
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


async def charter_of(pool: asyncpg.Pool, seat_id: str) -> list[str]:
    """The repos this SEAT currently governs — the ACTIVE `governs` links (a compensated-away
    grant does not count), plain repo labels (the `repo:` scheme stripped), sorted. Empty for
    the common seat that has never declared a charter (works_in still names its home), and
    empty forever for a seat that never will — no lineage walk, no LIKE-prefix guess: one
    durable object id, matched exactly."""
    rows = await pool.fetch(
        "SELECT p.canonical FROM links l "
        "JOIN objects s ON s.id=l.from_id AND s.type='Seat' AND s.canonical=$1 "
        "JOIN objects p ON p.id=l.to_id AND p.type='SoftwareProject' "
        "WHERE l.type='governs' AND (l.valid_until IS NULL OR l.valid_until > now())",
        seat_id,
    )
    return sorted(r["canonical"].removeprefix("repo:") for r in rows)


async def set_charter(
    actions: Actions, seat_id: str, repos: list[str], *, actor: str,
) -> dict[str, Any]:
    """Declare the WHOLE charter — replacing whatever this seat ruled before, never adding to
    it blind. Self-declared, same evidence grade as claim_name (a seat's own word about its own
    jurisdiction) — but sourced from the SEAT (`seat_id`), not the calling session, so two
    generations of the same lineage declaring in succession are the same source, never two.
    `actor` is the agent that actually typed this (audit trail only; never the graph edge's
    origin). Repos newly named mint a fresh `governs` link; repos dropped from the list are
    healed by `invalidate_link` (compensating, never DELETE) rather than left to rot as a stale
    grant nobody can tell apart from a current one. Calling it twice with the identical set is
    a no-op: nothing minted, nothing healed.

    Refuses per-name, never wholesale: a name `_resolve_repo` can't find as an already-real
    SoftwareProject (git-ingested, or already named on some other record) lands in `rejected`
    with why, and the REST of the call still applies — the same "one bad item never sinks the
    whole batch" discipline `settle()` already runs. Never mints a SoftwareProject itself."""
    from src.orchestrator.capture import _resolve_repo

    now = datetime.now(UTC)
    seat_oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1 AND type='Seat' AND status='active'",
        seat_id,
    )
    if seat_oid is None:
        return {"error": f"no such active seat: {seat_id!r} — a charter is declared BY a "
                         "seat, and this one doesn't exist (or isn't active)"}
    candidates = sorted({r.strip().removeprefix("repo:") for r in repos if r and r.strip()})
    resolved: dict[str, Any] = {}
    rejected: list[dict[str, str]] = []
    for name in candidates:
        proj = await _resolve_repo(actions.pool, name)
        if proj is None:
            rejected.append({"repo": name, "error": "not a known repo — the graph has no "
                             "independent evidence it's real (no git ingest, no prior record); "
                             "ingest it or confirm it exists, then declare your charter over it"})
            continue
        resolved[name] = proj
    wanted = sorted(resolved)
    current = set(await charter_of(actions.pool, seat_id))
    added = [r for r in wanted if r not in current]
    removed = sorted(current - set(wanted))
    out: dict[str, Any] = {"seat": seat_id, "charter": wanted, "added": added,
                           "removed": removed}
    if rejected:
        out["rejected"] = rejected
    if not added and not removed:
        return out
    for repo in added:
        await actions.create_link(seat_oid, resolved[repo], "governs", seat_id, now, _CONF,
                                  evidence_class=_EC, actor=actor)
    for repo in removed:
        proj = await _resolve_repo(actions.pool, repo)
        if proj is not None:
            await actions.invalidate_link(seat_oid, proj, "governs", actor, now)
    return out


async def charter_for(
    actions: Actions, seat_id: str, repos: list[str], *, because: str, actor: str,
) -> dict[str, Any]:
    """THE MANAGER-INVOKED SIBLING (thread 2446), not a widening of `charter()`: the
    operator's own model, 2026-07-31 — "a seat with no manager should be able to
    recharter itself, it has no upstream agent authority, IM THE NODE THAT EVERY AGENT
    LINKS BACK TO" — makes the rule uniform. A seat may declare its own charter; its
    manager may declare FOR it; the operator is every seat's ultimate manager, so no seat
    is ever authority-less. `charter()` stays EXACTLY as it is — self-declaration only,
    no target param — because that is what makes the STRANGER case work unaided, with no
    operator in the loop at all (ruling 1db1ff41's own acceptance bar). This is the OTHER
    half: declaring on behalf of a seat that cannot yet speak for itself (ruling 5's own
    24 undeclared seats).

    GUARD, ENFORCED (not merely a naming convention — rename_seat/set_seat_attended's own
    "manager/operator-invoked" claim is not actually checked in their code; this one
    checks): `actor` must be either one of `seats._OPERATOR_ACTORS`'s sentinels, OR the
    seat `actor`'s own lineage currently holds must BE the target seat's manager
    (`manager_of_seat`'s live `managed_by` edge). Refuses loudly otherwise, naming both
    who the caller resolved to and who the seat's actual manager is (or that it has
    none on record) — never a bare permission-denied.

    `because` is required, same testimony discipline `rename_seat` runs: declaring a
    charter on someone else's behalf is a deliberate act, not a routine one. Every write
    stamps `actor` (rebind_seat's own law: a rebind is a mind's act on another seat, so
    the record must say whose hand moved it — applied identically here); the receipt
    carries both `because` and who declared it.

    BLIND TO LEGACY AGENT-ORIGIN GOVERNS EDGES, BY CONSTRUCTION (Seshat's blocker 2, live
    and unresolved: `migrate_charter_to_seat` has not run — Atlas's 27 governs edges
    still sit on `agent:f84d55be-v` as Agent-origin links while his Seat reads zero).
    This delegates straight to `set_charter`, which only ever reads/writes governs links
    FROM a Seat object — it cannot see, heal, or interact with an Agent-origin row at
    all, so calling `charter_for` on an unmigrated seat is safe from THAT angle: it never
    touches the legacy rows, never orphans them further. It does NOT solve the reverse
    risk, named here so nobody finds it by accident rather than fixed (out of scope for
    this piece): if `migrate_charter_to_seat` runs LATER on a seat `charter_for` already
    declared for, that function computes its target set purely from the legacy
    Agent-origin union, with no awareness of a charter already declared post-rekey — it
    could heal away what `charter_for` just wrote. A real, separate gap in the migration
    itself, still open."""
    from src.orchestrator.seats import _OPERATOR_ACTORS, held_seat, manager_of_seat

    because = (because or "").strip()
    if not because:
        return {"error": "because is required — a charter declared on another seat's "
                         "behalf is testimony, same discipline rename_seat runs"}
    if actor not in _OPERATOR_ACTORS:
        caller_seat = await held_seat(actions.pool, actor)
        caller_seat_id = str(caller_seat["seat_id"]) if caller_seat else None
        manager_seat_id = await manager_of_seat(actions.pool, seat_id)
        if caller_seat_id is None or caller_seat_id != manager_seat_id:
            caller_desc = (f"{actor} (seat {caller_seat_id})" if caller_seat_id
                          else f"{actor} (holds no seat)")
            manager_desc = manager_seat_id or "no manager on record"
            return {"error": f"{caller_desc} is not authorized to declare a charter for "
                             f"{seat_id} — its manager is {manager_desc}, and {actor} is "
                             "neither that manager nor an operator actor"}
    out = await set_charter(actions, seat_id, repos, actor=actor)
    if "error" not in out:
        out["because"] = because
        out["declared_by"] = actor
    return out


async def migrate_charter_to_seat(
    actions: Actions, *, dry_run: bool = True, only_seats: set[str] | None = None,
) -> dict[str, Any]:
    """THE ONE-TIME MIGRATION (ruling 1db1ff41): before this, `governs` originated from
    whichever Agent generation happened to declare it, and a successor re-declaring couldn't
    heal an ancestor's grant (`invalidate_link` needs the exact from_id) — so a lineage's
    EFFECTIVE charter (what orient()/charter()'s lineage-walk showed, 742df26) could silently
    accumulate repos nobody meant to keep. This walks every ACTIVE Agent-origin `governs` link,
    resolves each Agent to the Seat its lineage currently holds (`held_seat` — the same
    lineage-aware resolution orient()/mount() already trust: the presented id, its bare root,
    or any `-<suffix>` generation), and for each seat re-declares the UNION of everything its
    lineage ever actively declared as a fresh Seat-origin charter — through `set_charter`
    itself, the same validated, idempotent write path a live seat would use — then heals every
    migrated Agent-origin link (compensating event, never deleted).

    DRY-RUN REPORTS THE PLAN AND WRITES NOTHING. Idempotent: a second run finds no active
    Agent-origin `governs` links left (the first pass healed them all) and is a no-op.

    An Agent whose OWN lineage holds no seat right now (never attached/claimed, or reassigned/
    retired since it declared) is reported in `unresolved`, NEVER guessed — its links are left
    untouched, a named residual exactly like the propose-and-approve batch (ruling 5) this is
    NOT: this heals a rekey, it does not clean up what was declared, however it looks."""
    from src.orchestrator.seats import held_seat

    rows = await actions.pool.fetch(
        "SELECT a.canonical AS agent_id, p.canonical AS repo "
        "FROM links l "
        "JOIN objects a ON a.id=l.from_id AND a.type='Agent' "
        "JOIN objects p ON p.id=l.to_id AND p.type='SoftwareProject' "
        "WHERE l.type='governs' AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "ORDER BY a.canonical, p.canonical"
    )
    by_seat: dict[str, dict[str, Any]] = {}
    unresolved: list[dict[str, str]] = []
    for row in rows:
        agent_id = row["agent_id"]
        repo = row["repo"].removeprefix("repo:")
        bound = await held_seat(actions.pool, agent_id)
        if bound is None:
            unresolved.append({"agent_id": agent_id, "repo": repo,
                               "note": "no seat currently held by this agent's lineage — "
                                       "left untouched"})
            continue
        seat_id = str(bound["seat_id"])
        if only_seats is not None and seat_id not in only_seats:
            continue
        entry = by_seat.setdefault(seat_id, {"seat_id": seat_id, "repos": set(), "pairs": []})
        entry["repos"].add(repo)
        entry["pairs"].append((agent_id, repo))
    plan = [{"seat_id": v["seat_id"], "repos": sorted(v["repos"]),
             "from_agents": sorted({p[0] for p in v["pairs"]})} for v in by_seat.values()]
    applied = 0
    rejected_total: list[dict[str, Any]] = []
    if not dry_run:
        now = datetime.now(UTC)
        for v in by_seat.values():
            out = await set_charter(actions, v["seat_id"], sorted(v["repos"]),
                                    actor="migration:governs-seat-key")
            if out.get("rejected"):
                rejected_total.append({"seat_id": v["seat_id"], "rejected": out["rejected"]})
            for agent_id, repo in v["pairs"]:
                agent_oid = await actions.pool.fetchval(
                    "SELECT id FROM objects WHERE canonical=$1 AND type='Agent'", agent_id)
                proj_oid = await actions.pool.fetchval(
                    "SELECT id FROM objects WHERE canonical=$1 AND type='SoftwareProject'",
                    f"repo:{repo}")
                if agent_oid is not None and proj_oid is not None:
                    await actions.invalidate_link(agent_oid, proj_oid, "governs",
                                                  "migration:governs-seat-key", now)
            applied += 1
    return {"plan": plan, "unresolved": unresolved, "applied": not dry_run,
            "seats_migrated": applied, "rejected": rejected_total}
