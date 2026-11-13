"""GRAPH SHAPE REPAIRS, THE MIGRATIONS: three name-dispatched repair entry points, the
same dry-run-default/idempotent/compensating-event shape `backfill.py` already
established for exactly this class of work (`BACKFILL_TARGETS`/`run_backfill`). A
separate registry here (`MIGRATION_TARGETS`/`run_migration`), not a new entry in that
one, because these are graph-shape repairs feeding the physics layout and the
renderer, not the identity/provenance backfill's own population. `osiris graph-migrate
<name> [--dry-run/--apply]` is the CLI command (cli.py), named to avoid colliding with
the pre-existing `osiris migrate` (alembic's env-correct schema tool, unrelated).

Each target: `dry_run` defaults True; `dry_run=False` REQUIRES a non-blank `because`
(the same audit-trail contract every backfill target already holds itself to); every
write is a compensating event (`assert_property`/`create_link`/`invalidate_link`),
never a delete; a repeat call after a real write finds nothing left to do."""

from __future__ import annotations

import uuid
from collections import Counter, defaultdict
from datetime import UTC, datetime
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.orchestrator.seats import seat_by_handle
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

MIGRATION_TARGETS = frozenset({
    "repo_seats_fix", "file_the_unfiled", "assertion_links",
    "owned_by_second_pass", "file_the_residual", "commits_to_agents",
    "house_to_project", "holds_sandwich", "project_name_singular",
})

_BIND_BEFORE_SPAWN_PREFIX = "launch_seat: bind-before-spawn"

# THE EVIDENCE GATE: a live dry run found five real
# sandwiches, not the roughly two the diagnosis anticipated, one spanning six weeks. A
# phantom's own window longer than this is not "near-instant" bookkeeping by any
# reading, whatever the real holder was doing meanwhile. It cleanly separates the
# four short specimens actually found (9h/40h/12h/8h) from the six-week outlier
# without needing a per-seat judgment call. One week, not one day, because a real
# holder's own genuinely idle stretch (a weekend, a short leave) must not itself
# trip this gate. The separate in-gap-activity check below is what catches an
# actual vacancy; this constant only catches "this was never a brief blip".
_NEAR_INSTANT_MAX_SECONDS = 7 * 24 * 3600

_TIER = EvidenceClass.DIRECT_OBSERVATION
_CONF = confidence_for(_TIER)
_EC = _TIER.value
_SOURCE = "graph_migrations"


async def run_migration(
    pool: asyncpg.Pool, name: str, *, actor: str, dry_run: bool = True,
    because: str | None = None, only_seat: str | None = None,
) -> dict[str, Any]:
    """Dispatch on `name`, see `MIGRATION_TARGETS` for the full set. `only_seat`
    (a seat's canonical `seat:<id>` or its bare handle) narrows `holds_sandwich` to
    one seat, refused for every other target, since none of them are seat-scoped."""
    if only_seat and name != "holds_sandwich":
        return {"error": f"--only is only supported for holds_sandwich, not {name!r}"}
    actions = Actions(pool)
    if name == "repo_seats_fix":
        return await migrate_repo_seats_fix(
            actions, actor=actor, dry_run=dry_run, because=because)
    if name == "file_the_unfiled":
        return await migrate_file_the_unfiled(
            actions, actor=actor, dry_run=dry_run, because=because)
    if name == "assertion_links":
        return await migrate_assertion_links(
            actions, actor=actor, dry_run=dry_run, because=because)
    if name == "owned_by_second_pass":
        return await migrate_owned_by_second_pass(
            actions, actor=actor, dry_run=dry_run, because=because)
    if name == "file_the_residual":
        return await migrate_file_the_residual(
            actions, actor=actor, dry_run=dry_run, because=because)
    if name == "commits_to_agents":
        return await migrate_commits_to_agents(
            actions, actor=actor, dry_run=dry_run, because=because)
    if name == "house_to_project":
        return await migrate_house_to_project(
            actions, actor=actor, dry_run=dry_run, because=because)
    if name == "holds_sandwich":
        return await migrate_holds_sandwich(
            actions, actor=actor, dry_run=dry_run, because=because, only_seat=only_seat)
    if name == "project_name_singular":
        return await migrate_project_name_singular(
            actions, actor=actor, dry_run=dry_run, because=because)
    return {"error": f"unknown migration {name!r}", "valid_targets": sorted(MIGRATION_TARGETS)}


async def migrate_repo_seats_fix(
    actions: Actions, *, actor: str, dry_run: bool = True, because: str | None = None,
) -> dict[str, Any]:
    """THE repo:seats BUG: `repo:seats` is
    a phantom SoftwareProject minted from ~/.osiris/seats, the bare seat-office
    container, never a real project, the same "seats" basename `offices.
    is_bare_office_root` already guards `seats.resolve_project` against, reached
    here through a different route (git-ingest's own
    `sessions._repo_from_cwd`, fixed alongside this migration so the derivation
    itself stops producing new damage while this repairs the historical kind).

    Compensating, never a delete: every Agent whose current `project` assertion
    reads "seats" is re-stamped to "osiris" (the fleet's own shared root; the bare
    container belongs to no one seat, so there is no per-seat house to derive,
    only the shared fleet root) via `assert_singular_property`, not the plain
    `assert_property` the first cut of this entry point used. A live finding on an
    earlier apply showed `assert_property`'s own supersession is
    same-source only, so a re-stamp written by this migration's own actor sat
    beside the agent's own prior self-declared "seats" row rather than retiring
    it: both simultaneously `is_current`, and the stream header (reading
    whichever `current_assertions` row it finds first, over a set the write
    path never proved unique, the exact failure shape a non-total ORDER BY
    lesson names) kept filing all 54 agents
    under repo:seats even after the "successful" apply. `assert_singular_property`
    is this codebase's own blessed entry point for this shape: a property that is
    single-valued per object regardless of who wrote
    the prior value collapses every other current row for (object, "project")
    down to the one this call mints, cross-source. Every live link into
    repo:seats (any type, since a
    phantom container's own edges are fixed whatever its status; a
    correction found the original status=='active' gate silently skipped
    the repair the instant the container drifted out of that one status) is
    invalidated and re-minted pointing at repo:osiris instead. Retired via
    `projects.retire_project` (a real status flip, never a raw DELETE) only
    once every edge off it is moved and it was not already retired, since
    calling retire_project on an already-retired object is a wasted, confusing call,
    not a real repair.

    THE 8,609 FIGURE WAS A DIFFERENT MEASUREMENT: not edges
    on repo:seats itself (measured ~95: 57 works_in, 13+5+2 in_repo, 17
    informs, 1 same_as) but edges between the 54 seats-stamped Agents and
    other osiris objects, counted cross-district by an earlier spike that
    folded nearby unfiled messages into repo:seats by nearest centroid,
    an artifact of that fold, not a defect this migration repairs. Reported
    read-only, for the record, unchanged by this migration either way.

    DRY RUN IS THE DEFAULT. `dry_run=False` REQUIRES a non-blank `because`.
    Idempotent: a repeat call finds no "seats"-stamped agents and no live edges
    into repo:seats (already retired) left to touch."""
    if not dry_run and not (because or "").strip():
        return {"error": "migrating without a because is an un-audited repair. Cite "
                         "the evidence/ruling that authorizes it"}
    pool = actions.pool
    seats_row = await pool.fetchrow(
        "SELECT id, status FROM objects WHERE canonical='repo:seats' AND type='SoftwareProject'")
    if seats_row is None:
        return {"dry_run": dry_run, "already_clean": True, "note": "no repo:seats object exists"}
    seats_id, seats_status = seats_row["id"], seats_row["status"]
    osiris_id = await pool.fetchval(
        "SELECT id FROM objects WHERE canonical='repo:osiris' AND type='SoftwareProject' "
        "AND status='active'")
    if osiris_id is None:
        return {"error": "repo:osiris is not an active SoftwareProject. Refusing to "
                         "re-file into a target that isn't there"}

    agent_rows = await pool.fetch(
        "SELECT o.id, o.canonical FROM objects o "
        "JOIN current_assertions a ON a.object_id=o.id "
        "WHERE a.name='project' AND a.value #>> '{}' = 'seats' AND o.type='Agent'")
    agent_ids = [r["id"] for r in agent_rows]
    # How many current `project` rows each agent carries right now, any source.
    # assert_singular_property collapses every one of them to the single new
    # "osiris" row it mints, so this count is the per-agent supersede count
    # (1 in the clean case; >1 if a prior partial/botched apply already left an
    # extra current row beside the original "seats" one, per an earlier finding).
    current_project_counts: dict[uuid.UUID, int] = {}
    if agent_ids:
        count_rows = await pool.fetch(
            "SELECT object_id, count(*) AS n FROM current_assertions "
            "WHERE object_id = ANY($1::uuid[]) AND name='project' GROUP BY object_id",
            agent_ids)
        current_project_counts = {r["object_id"]: r["n"] for r in count_rows}
    # Every live link into repo:seats, any type; the container's own status is
    # never consulted here (per an earlier correction): a phantom's edges are fixed
    # whatever state the phantom itself is in.
    edge_rows = await pool.fetch(
        "SELECT l.from_id, l.type, o.type AS from_type "
        "FROM links l JOIN objects o ON o.id=l.from_id "
        "WHERE l.to_id=$1 "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", seats_id)
    # READ-ONLY, for the record, never written: live links between the 54
    # seats-stamped agents and any other object already in osiris (in_repo/
    # works_in to repo:osiris, or a current project assertion of 'osiris').
    # These become same-district the moment the agents above are re-stamped
    # and need no edge rewrite of their own.
    agents_to_osiris_count = 0
    if agent_ids:
        agents_to_osiris_count = await pool.fetchval(
            "SELECT count(*) FROM links l "
            "WHERE (l.valid_until IS NULL OR l.valid_until > now()) "
            "AND ((l.from_id = ANY($1::uuid[]) AND ("
            "    EXISTS (SELECT 1 FROM links l2 WHERE l2.from_id=l.to_id "
            "      AND l2.to_id=$2 AND l2.type IN ('in_repo','works_in') "
            "      AND (l2.valid_until IS NULL OR l2.valid_until > now())) "
            "    OR EXISTS (SELECT 1 FROM current_assertions a WHERE a.object_id=l.to_id "
            "      AND a.name='project' AND a.value #>> '{}' = 'osiris'))) "
            "  OR (l.to_id = ANY($1::uuid[]) AND ("
            "    EXISTS (SELECT 1 FROM links l2 WHERE l2.from_id=l.from_id "
            "      AND l2.to_id=$2 AND l2.type IN ('in_repo','works_in') "
            "      AND (l2.valid_until IS NULL OR l2.valid_until > now())) "
            "    OR EXISTS (SELECT 1 FROM current_assertions a WHERE a.object_id=l.from_id "
            "      AND a.name='project' AND a.value #>> '{}' = 'osiris'))))",
            agent_ids, osiris_id)

    now = datetime.now(UTC)
    agents_plan = [
        {"agent": str(r["id"])[:8], "canonical": r["canonical"],
         "superseded": current_project_counts.get(r["id"], 1)}
        for r in agent_rows
    ]
    superseded_total = sum(a["superseded"] for a in agents_plan)
    edges_by_type: Counter[str] = Counter(r["type"] for r in edge_rows)

    if not dry_run:
        for r in agent_rows:
            await actions.assert_singular_property(
                r["id"], "project", "osiris", actor, now, _CONF,
                because=f"{because} (migrate_repo_seats_fix: cross-source collapse of "
                        "the agent's prior 'seats' self-declaration -- project is "
                        "single-valued per object, per standing ruling)",
                evidence_class=_EC, actor=actor)
        for r in edge_rows:
            await actions.invalidate_link(
                r["from_id"], seats_id, r["type"], actor, now,
                reason="migrate_repo_seats_fix: repo:seats is a phantom project minted "
                       "from the bare seat-office container, never a real repo")
            await actions.create_link(
                r["from_id"], osiris_id, r["type"], actor, now, _CONF, evidence_class=_EC)

    osiris_seats_edges_after = None
    retired = None
    if not dry_run:
        osiris_seats_edges_after = await pool.fetchval(
            "SELECT count(*) FROM links WHERE to_id=$1 "
            "AND (valid_until IS NULL OR valid_until > now())", seats_id)
        if osiris_seats_edges_after == 0 and seats_status != "retired":
            from src.orchestrator.projects import retire_project

            result = await retire_project(
                actions, project="repo:seats", actor=actor,
                because=f"{because} (migrate_repo_seats_fix: every live edge into this "
                        "phantom project has been re-filed into repo:osiris)")
            retired = result.get("retired_project") or result.get("error")

    return {
        "dry_run": dry_run,
        "agents_scanned": len(agent_rows), "agents_plan": agents_plan,
        "superseded_total": superseded_total,
        "edges_scanned": len(edge_rows), "edges_retired_or_refiled_by_type": dict(edges_by_type),
        "agents_to_osiris_links_unchanged": agents_to_osiris_count,
        "osiris_seats_edges_after": osiris_seats_edges_after,
        "seats_status_before": seats_status,
        "retired": retired,
        "because": because if not dry_run else None,
    }


_UNFILED_EXCLUDED_TYPES = ("SoftwareProject", "Person", "Seat")
_MAX_FILING_PASSES = 10


async def _unfiled_pass(
    pool: asyncpg.Pool, *, already_filed: dict[uuid.UUID, str],
) -> dict[str, Any]:
    """One pass of the majority vote, `already_filed` (oid -> project bare name)
    carrying every object a prior pass in this same run filed. In a dry run
    nothing is written, so this is the only way a later pass can see an earlier
    pass's own result; in a real run the DB read below already reflects it, and
    `already_filed` just widens the excluded-from-rescan set for objects this
    run itself already resolved. See `migrate_file_the_unfiled`'s own docstring
    for the full rule."""
    unfiled_rows = await pool.fetch(
        "SELECT o.id FROM objects o "
        "WHERE o.status NOT IN ('archived','merged','retired') "
        "AND o.type != ALL($1::text[]) "
        "AND NOT EXISTS (SELECT 1 FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='project') "
        "AND NOT EXISTS (SELECT 1 FROM links l WHERE l.from_id=o.id "
        "  AND l.type IN ('in_repo','works_in') "
        "  AND (l.valid_until IS NULL OR l.valid_until > now()))",
        list(_UNFILED_EXCLUDED_TYPES))
    unfiled_ids: set[uuid.UUID] = {r["id"] for r in unfiled_rows if r["id"] not in already_filed}
    if not unfiled_ids:
        return {"scanned": 0, "filed": {}, "ties": [], "still_unfiled": 0}

    neighbour_edge_rows = await pool.fetch(
        "SELECT from_id, to_id FROM links "
        "WHERE (valid_until IS NULL OR valid_until > now()) "
        "AND (from_id = ANY($1::uuid[]) OR to_id = ANY($1::uuid[]))",
        list(unfiled_ids))
    neighbours_by_unfiled: dict[uuid.UUID, set[uuid.UUID]] = defaultdict(set)
    for r in neighbour_edge_rows:
        f, t = r["from_id"], r["to_id"]
        if f in unfiled_ids and t not in unfiled_ids:
            neighbours_by_unfiled[f].add(t)
        if t in unfiled_ids and f not in unfiled_ids:
            neighbours_by_unfiled[t].add(f)

    all_neighbour_ids = {n for s in neighbours_by_unfiled.values() for n in s}
    project_name_by_neighbour: dict[uuid.UUID, str] = {}
    if all_neighbour_ids:
        sw_rows = await pool.fetch(
            "SELECT id, canonical FROM objects WHERE id = ANY($1::uuid[]) "
            "AND type='SoftwareProject' AND status='active'", list(all_neighbour_ids))
        for r in sw_rows:
            project_name_by_neighbour[r["id"]] = r["canonical"].removeprefix("repo:")
        remaining = [n for n in all_neighbour_ids if n not in project_name_by_neighbour]
        if remaining:
            link_rows = await pool.fetch(
                "SELECT l.from_id AS oid, p.canonical AS pcanon FROM links l "
                "JOIN objects p ON p.id=l.to_id AND p.type='SoftwareProject' "
                "  AND p.status='active' "
                "WHERE l.type IN ('in_repo','works_in') AND l.from_id = ANY($1::uuid[]) "
                "AND (l.valid_until IS NULL OR l.valid_until > now())", remaining)
            for r in link_rows:
                project_name_by_neighbour.setdefault(
                    r["oid"], r["pcanon"].removeprefix("repo:"))
            still_remaining = [n for n in remaining if n not in project_name_by_neighbour]
            if still_remaining:
                # A neighbour filed by this same run (a prior pass, real write or
                # simulated dry-run) or by an earlier live process: its own current
                # `project` assertion, resolved against an active SoftwareProject's
                # bare name. This is the fixed-point iteration's own load-bearing step,
                # since a project assertion alone (no in_repo/works_in link) is
                # exactly the shape this migration itself mints.
                assertion_rows = await pool.fetch(
                    "SELECT a.object_id AS oid, a.value #>> '{}' AS pname "
                    "FROM current_assertions a "
                    "WHERE a.object_id = ANY($1::uuid[]) AND a.name='project'",
                    still_remaining)
                names = {r["pname"] for r in assertion_rows if r["pname"]}
                active_by_name: dict[str, str] = {}
                if names:
                    active_rows = await pool.fetch(
                        "SELECT canonical FROM objects WHERE type='SoftwareProject' "
                        "AND status='active' AND canonical = ANY($1::text[])",
                        [f"repo:{n}" for n in names])
                    active_by_name = {
                        r["canonical"].removeprefix("repo:"): r["canonical"]
                        for r in active_rows}
                for r in assertion_rows:
                    pname = r["pname"]
                    if pname is not None and pname in active_by_name:
                        project_name_by_neighbour.setdefault(r["oid"], pname)
    for oid, name in already_filed.items():
        project_name_by_neighbour.setdefault(oid, name)

    filed: dict[uuid.UUID, str] = {}
    filed_votes: dict[uuid.UUID, int] = {}
    ties: list[dict[str, Any]] = []
    still_unfiled_count = 0
    for oid in unfiled_ids:
        tally: Counter[str] = Counter()
        for n in neighbours_by_unfiled.get(oid, ()):
            neighbour_project = project_name_by_neighbour.get(n)
            if neighbour_project is not None:
                tally[neighbour_project] += 1
        if not tally:
            still_unfiled_count += 1
            continue
        top_count = tally.most_common(1)[0][1]
        winners = [name for name, c in tally.items() if c == top_count]
        if len(winners) > 1:
            ties.append({
                "object": str(oid)[:8], "candidates": sorted(winners), "votes": top_count})
            still_unfiled_count += 1
            continue
        filed[oid] = winners[0]
        filed_votes[oid] = top_count

    return {
        "scanned": len(unfiled_ids), "filed": filed, "filed_votes": filed_votes,
        "ties": ties, "still_unfiled": still_unfiled_count,
    }


async def migrate_file_the_unfiled(
    actions: Actions, *, actor: str, dry_run: bool = True, because: str | None = None,
) -> dict[str, Any]:
    """FILE THE UNFILED: ~7,300 active,
    non-SoftwareProject objects carry neither a `project` assertion nor a live
    in_repo/works_in link, the physics layout's own "unfiled fog," placed only by
    neighbour-centroid pull today, never
    actually filed. Person and Seat objects are excluded from being filed at all:
    a global with heavy structural fan-in (a principal, a seat)
    is a landmark, not a member of whichever district happens to touch it most,
    since filing one would drag an unrelated fan into a single district.

    MAJORITY PROJECT OVER DIRECT NEIGHBOURS, ANY LINK TYPE: every live edge touching
    an unfiled object (either direction, any type; membership is a vote here, not
    a spring, and the structural/semantic split governs the layout, not
    this tally) contributes one vote for that neighbour's own project: itself,
    when the neighbour is an active SoftwareProject, else the neighbour's own
    in_repo/works_in target, else (a neighbour this migration itself already filed,
    this run or an earlier one) its own current `project` assertion. TIE OR EMPTY
    STAYS UNFILED, AND IS COUNTED, EVERY PASS: a tie between two or more top
    projects, or an unfiled object with no neighbour that resolves to any project
    at all, is left exactly as it was, never a guess between equally-supported
    candidates.

    ITERATES TO A FIXED POINT: a single hop cannot see a
    project across a whole cluster of mutually-unfiled objects (messages, sub-
    agents) that only touch a real project through another unfiled object, so each
    pass files what it can, then re-runs the vote over what is still unfiled
    (now able to see the previous pass's own newly-filed neighbours), capped at
    `_MAX_FILING_PASSES` (10) and stopping the moment a pass files nothing new.
    The result carries one entry per pass plus the summed totals.

    The winning project asserts as `project` (the bare name, matching every other
    `project` assertion's own shape in this codebase, e.g. `resolve_and_persist_
    seated_project`'s), source=`graph_migrations`. The vote tally itself is the
    evidence, carried in full in this function's own result, not a second copy
    embedded in the assertion row.

    DRY RUN IS THE DEFAULT. `dry_run=False` REQUIRES a non-blank `because`.
    Idempotent: a repeat call finds no unfiled objects left that this run actually
    filed (a still-unfiled tie/empty object stays a legitimate candidate for a
    later run, once more links exist to break the tie)."""
    if not dry_run and not (because or "").strip():
        return {"error": "migrating without a because is an un-audited repair. Cite "
                         "the evidence/ruling that authorizes it"}
    pool = actions.pool
    now = datetime.now(UTC)
    already_filed: dict[uuid.UUID, str] = {}
    passes: list[dict[str, Any]] = []
    by_project_tally: Counter[str] = Counter()
    all_ties: list[dict[str, Any]] = []

    for pass_num in range(1, _MAX_FILING_PASSES + 1):
        result = await _unfiled_pass(pool, already_filed=already_filed)
        scanned = result["scanned"]
        filed_this_pass: dict[uuid.UUID, str] = result["filed"]
        if not filed_this_pass:
            passes.append({
                "pass": pass_num, "scanned": scanned, "filed": 0,
                "ties": len(result["ties"]), "still_unfiled": result["still_unfiled"]})
            all_ties.extend(result["ties"])
            break
        if not dry_run:
            for oid, name in filed_this_pass.items():
                await actions.assert_property(
                    oid, "project", name, _SOURCE, now, _CONF, evidence_class=_EC)
        for name in filed_this_pass.values():
            by_project_tally[name] += 1
        already_filed.update(filed_this_pass)
        all_ties.extend(result["ties"])
        passes.append({
            "pass": pass_num, "scanned": scanned, "filed": len(filed_this_pass),
            "ties": len(result["ties"]), "still_unfiled": result["still_unfiled"]})

    return {
        "dry_run": dry_run,
        "passes": len(passes), "pass_detail": passes,
        "scanned": passes[0]["scanned"] if passes else 0,
        "filed": len(already_filed), "by_project": dict(by_project_tally),
        "ties": len(all_ties), "ties_plan": all_ties,
        "still_unfiled": passes[-1]["still_unfiled"] if passes else 0,
        "because": because if not dry_run else None,
    }


async def _resolve_ref(
    pool: asyncpg.Pool, value: str | None, *, object_type: str | None = None,
) -> uuid.UUID | None:
    """A stored assertion value (a canonical string, e.g. "agent:xyz"/"seat:xyz", or a
    raw uuid) resolved to a real, active object id: exact canonical match first,
    then a raw uuid parse. Never a name/fuzzy lookup: an assertion recorded a specific
    reference at write time, this only confirms it still resolves, it never guesses a
    new one. `object_type`, when given, narrows both attempts to that type."""
    if not value:
        return None
    if object_type:
        row = await pool.fetchval(
            "SELECT id FROM objects WHERE canonical=$1 AND type=$2 AND status='active'",
            value, object_type)
    else:
        row = await pool.fetchval(
            "SELECT id FROM objects WHERE canonical=$1 AND status='active'", value)
    if row is not None:
        return row  # type: ignore[no-any-return]
    try:
        oid = uuid.UUID(value)
    except (ValueError, AttributeError, TypeError):
        return None
    if object_type:
        row2 = await pool.fetchval(
            "SELECT id FROM objects WHERE id=$1 AND type=$2 AND status='active'",
            oid, object_type)
    else:
        row2 = await pool.fetchval(
            "SELECT id FROM objects WHERE id=$1 AND status='active'", oid)
    return row2  # type: ignore[no-any-return]


async def _link_exists(
    pool: asyncpg.Pool, from_id: uuid.UUID, to_id: uuid.UUID, type_: str,
) -> bool:
    return bool(await pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type=$3 "
        "AND (valid_until IS NULL OR valid_until > now())", from_id, to_id, type_))


async def _mint_links_from_property(
    actions: Actions, *, subject_type: str, prop_name: str, link_type: str,
    target_type: str | None = None, dry_run: bool, now: datetime,
) -> dict[str, int]:
    """The shared shape behind two of the six ASSERTION LINKS sub-migrations
    (closed_by/admitted_by). The other four (recorded_by, supersedes, owned_by,
    vendor_of) each need something this plain shape can't give them, and this
    reads exactly what is plain: every `subject_type` object's current
    `prop_name` assertion's own value, resolved via `_resolve_ref` (optionally
    narrowed to `target_type`), mints `(subject) -[link_type]-> (target)` unless
    that exact live edge already exists. recorded_by is not this shape: it
    needs the assertion row's own `source_id` (who wrote it), never its value
    (what it says); supersedes needs a custom from/to normalisation across two
    property names; owned_by needs a second, project-name fallback resolution
    pass this helper doesn't have; vendor_of's value is a
    free-text name, not a canonical/uuid `_resolve_ref` can read directly. Three
    buckets, never silently merged: minted, skipped_unresolvable (the value no
    longer resolves to a real object), already_present (idempotent re-run)."""
    pool = actions.pool
    rows = await pool.fetch(
        "SELECT o.id AS subject_id, a.value #>> '{}' AS value "
        "FROM objects o JOIN current_assertions a ON a.object_id=o.id "
        "WHERE o.type=$1 AND o.status='active' AND a.name=$2", subject_type, prop_name)
    minted = skipped_unresolvable = already_present = 0
    for r in rows:
        target_id = await _resolve_ref(pool, r["value"], object_type=target_type)
        if target_id is None:
            skipped_unresolvable += 1
            continue
        if await _link_exists(pool, r["subject_id"], target_id, link_type):
            already_present += 1
            continue
        minted += 1
        if not dry_run:
            await actions.create_link(
                r["subject_id"], target_id, link_type, _SOURCE, now, _CONF,
                evidence_class=_EC)
    return {"minted": minted, "skipped_unresolvable": skipped_unresolvable,
           "already_present": already_present}


async def _mint_recorded_by(
    actions: Actions, *, subject_type: str, dry_run: bool, now: datetime,
) -> dict[str, int]:
    """recorded_by's own shape, distinct from `_mint_links_from_property`: the target
    is the assertion row's own `source_id` column (who wrote the object's current
    `summary`), never the assertion's `value` (what it says). A `source_id` of
    "agent:xyz" resolved via `_resolve_ref` the same way any other canonical is."""
    pool = actions.pool
    rows = await pool.fetch(
        "SELECT o.id AS subject_id, a.source_id AS source_id "
        "FROM objects o JOIN current_assertions a ON a.object_id=o.id "
        "WHERE o.type=$1 AND o.status='active' AND a.name='summary'", subject_type)
    minted = skipped_unresolvable = already_present = 0
    for r in rows:
        agent_id = await _resolve_ref(pool, r["source_id"], object_type="Agent")
        if agent_id is None:
            skipped_unresolvable += 1
            continue
        if await _link_exists(pool, r["subject_id"], agent_id, "recorded_by"):
            already_present += 1
            continue
        minted += 1
        if not dry_run:
            await actions.create_link(
                r["subject_id"], agent_id, "recorded_by", _SOURCE, now, _CONF,
                evidence_class=_EC)
    return {"minted": minted, "skipped_unresolvable": skipped_unresolvable,
           "already_present": already_present}


async def migrate_assertion_links(
    actions: Actions, *, actor: str, dry_run: bool = True, because: str | None = None,
) -> dict[str, Any]:
    """ASSERTION LINKS: six property-
    pair-to-real-link mints, each idempotent and independently reported (minted /
    skipped_unresolvable / already_present) so a partial resolve rate in one never
    hides behind another's. `recorded_by`/`owned_by`/`admitted_by`/`vendor_of` are
    structural, `supersedes` semantic (link_classes.py), attribution/membership
    edges versus a real content claim, the same
    split every other type in this codebase already sorts by.

    NOT acknowledges: Decision.prior_art_
    acknowledged's own confirmation already mints a real edge, `mint_cites`, via
    `acknowledge_prior_art`'s own docstring ("promoted from a string to a real
    edge"), so a distinct `acknowledges` link would be redundant with an
    existing `cites` edge for the same fact, not a real gap. Dropped entirely,
    confirmed live by the first dry run's own 0-minted/110-skipped result.

    recorded_by: every active Decision/Thread's current `summary` assertion's own
    `source_id` (the agent that actually wrote it), when that source_id resolves to
    a real Agent.

    supersedes: the one special case (custom, not the shared helper). Decision.
    supersedes/superseded_by are a property pair by deliberate design ("no
    link-retraction primitive needed" for the event-sourced property
    itself); this migration does not change that design or retire the properties,
    it adds a real `supersedes` link alongside them purely so the renderer's path
    lens (space.js's own PATH_EDGE_TYPES, which already names `supersedes`) has an
    edge to walk. Both properties read, normalised to one outgoing edge per pair
    (A supersedes B mints A->B once, whichever property named it) so a pair
    asserted from either side is never double-counted.

    owned_by: Thread.owner, a Seat/Agent canonical first, else a bare active
    SoftwareProject name (an older, legacy shape),
    never a canonical/uuid; `minted_as_project` breaks out that second path, and
    `unresolvable_samples` carries up to 20 raw values still left, so the result
    names what a skip actually looks like rather than a bare count. closed_by:
    Thread.resolved_in, where missing only. closed_by is an existing, actively-
    minted link type (`_mint_closed_by` and friends); this only fills the
    historical gap where the property exists but the link never landed, never a
    second edge alongside a real one. admitted_by: Thread.admitted_by. vendor_of:
    Reference.vendor, the one genuinely fuzzy resolution here (a free-text
    vendor name, not a canonical/uuid), resolved against an active SoftwareProject's
    own `repo:<name>` canonical. A vendor string that never names a real project
    abstains, honestly, rather than minting a link to a guess.

    DRY RUN IS THE DEFAULT. `dry_run=False` REQUIRES a non-blank `because`."""
    if not dry_run and not (because or "").strip():
        return {"error": "migrating without a because is an un-audited repair. Cite "
                         "the evidence/ruling that authorizes it"}
    pool = actions.pool
    now = datetime.now(UTC)
    receipt: dict[str, dict[str, Any]] = {}

    recorded_by_d = await _mint_recorded_by(actions, subject_type="Decision", now=now,
                                            dry_run=dry_run)
    recorded_by_t = await _mint_recorded_by(actions, subject_type="Thread", now=now,
                                            dry_run=dry_run)
    receipt["recorded_by"] = {
        k: recorded_by_d[k] + recorded_by_t[k] for k in recorded_by_d}

    # supersedes: the one custom case, see the docstring above.
    pair_rows = await pool.fetch(
        "SELECT o.id AS subject_id, a.name, a.value #>> '{}' AS value "
        "FROM objects o JOIN current_assertions a ON a.object_id=o.id "
        "WHERE o.type='Decision' AND o.status='active' "
        "AND a.name IN ('supersedes','superseded_by')")
    supersedes_edges: set[tuple[uuid.UUID, uuid.UUID]] = set()
    supersedes_unresolvable = 0
    for r in pair_rows:
        other_id = await _resolve_ref(pool, r["value"], object_type="Decision")
        if other_id is None:
            supersedes_unresolvable += 1
            continue
        edge = (r["subject_id"], other_id) if r["name"] == "supersedes" \
            else (other_id, r["subject_id"])
        supersedes_edges.add(edge)
    supersedes_minted = supersedes_already = 0
    for from_id, to_id in supersedes_edges:
        if await _link_exists(pool, from_id, to_id, "supersedes"):
            supersedes_already += 1
            continue
        supersedes_minted += 1
        if not dry_run:
            await actions.create_link(
                from_id, to_id, "supersedes", _SOURCE, now, _CONF, evidence_class=_EC)
    receipt["supersedes"] = {
        "minted": supersedes_minted, "skipped_unresolvable": supersedes_unresolvable,
        "already_present": supersedes_already}

    # owned_by: Thread.owner, a Seat/Agent canonical first (the generic
    # resolver), then a bare
    # active SoftwareProject name (an older, legacy shape), never a canonical/uuid,
    # reported separately
    # (`minted_as_project`) and with a sample of what's still left unresolved so
    # the result names what those values actually look like, not just a count.
    owner_rows = await pool.fetch(
        "SELECT o.id AS subject_id, a.value #>> '{}' AS value "
        "FROM objects o JOIN current_assertions a ON a.object_id=o.id "
        "WHERE o.type='Thread' AND o.status='active' AND a.name='owner'")
    owner_minted = owner_minted_as_project = owner_already = 0
    owner_unresolvable_samples: list[str] = []
    for r in owner_rows:
        value = r["value"]
        target_id = await _resolve_ref(pool, value)
        via_project = False
        if target_id is None and value:
            target_id = await pool.fetchval(
                "SELECT id FROM objects WHERE canonical=$1 AND type='SoftwareProject' "
                "AND status='active'", f"repo:{value.strip()}")
            via_project = target_id is not None
        if target_id is None:
            if len(owner_unresolvable_samples) < 20:
                owner_unresolvable_samples.append(value or "<empty>")
            continue
        if await _link_exists(pool, r["subject_id"], target_id, "owned_by"):
            owner_already += 1
            continue
        owner_minted += 1
        if via_project:
            owner_minted_as_project += 1
        if not dry_run:
            await actions.create_link(
                r["subject_id"], target_id, "owned_by", _SOURCE, now, _CONF,
                evidence_class=_EC)
    receipt["owned_by"] = {
        "minted": owner_minted, "minted_as_project": owner_minted_as_project,
        "skipped_unresolvable": len(owner_rows) - owner_minted - owner_already,
        "already_present": owner_already,
        "unresolvable_samples": owner_unresolvable_samples,
    }

    receipt["closed_by"] = await _mint_links_from_property(
        actions, subject_type="Thread", prop_name="resolved_in", link_type="closed_by",
        dry_run=dry_run, now=now)
    receipt["admitted_by"] = await _mint_links_from_property(
        actions, subject_type="Thread", prop_name="admitted_by", link_type="admitted_by",
        target_type="Agent", dry_run=dry_run, now=now)

    # vendor_of: the one fuzzy resolution, a free-text vendor name against an
    # active SoftwareProject's own repo:<name> canonical, never a raw uuid parse
    # (a vendor string is never one).
    vendor_rows = await pool.fetch(
        "SELECT o.id AS subject_id, a.value #>> '{}' AS value "
        "FROM objects o JOIN current_assertions a ON a.object_id=o.id "
        "WHERE o.type='Reference' AND o.status='active' AND a.name='vendor'")
    vendor_minted = vendor_unresolvable = vendor_already = 0
    for r in vendor_rows:
        name = (r["value"] or "").strip()
        target_id = (await pool.fetchval(
            "SELECT id FROM objects WHERE canonical=$1 AND type='SoftwareProject' "
            "AND status='active'", f"repo:{name}")) if name else None
        if target_id is None:
            vendor_unresolvable += 1
            continue
        if await _link_exists(pool, target_id, r["subject_id"], "vendor_of"):
            vendor_already += 1
            continue
        vendor_minted += 1
        if not dry_run:
            await actions.create_link(
                target_id, r["subject_id"], "vendor_of", _SOURCE, now, _CONF,
                evidence_class=_EC)
    receipt["vendor_of"] = {
        "minted": vendor_minted, "skipped_unresolvable": vendor_unresolvable,
        "already_present": vendor_already}

    return {"dry_run": dry_run, "receipt": receipt, "because": because if not dry_run else None}


async def migrate_owned_by_second_pass(
    actions: Actions, *, actor: str, dry_run: bool = True, because: str | None = None,
) -> dict[str, Any]:
    """OWNED_BY SECOND PASS: the 847
    Thread.owner values `migrate_assertion_links`'s own owned_by sub-migration left
    unresolvable are shapes neither of its two resolution paths (a Seat/Agent
    canonical, or a bare active SoftwareProject name) can reach: the literal word
    "operator" (the human desk, minted as a Person under `principal:analyst:operator`
    by every `register_agent` call, never a canonical the generic resolver would try),
    a raw seat canonical (e.g. "seat:34f4e5fa", live, active, caught on the
    first apply's `unresolvable_samples`; this function's own first cut
    never actually re-tried the plain canonical resolve its own docstring claimed it
    did, only "operator" and a bare handle, a real gap between the doc and the
    code, not a data problem), and a bare seat handle (no `seat:` prefix,
    never resolved by a plain canonical/uuid lookup), the last resolved via
    `seats.seat_by_handle`, this codebase's own name-to-Seat lookup (the same shape
    `team`'s own `--seat` argument already resolves through), then the ordinary
    canonical resolve on the Seat it names.

    ITS OWN MIGRATION TARGET, not folded back into `migrate_assertion_links`'s owned_by
    sub-migration: the first pass's own two resolution paths are unchanged and still
    correct for what they cover; this only adds the fallback paths the live
    `unresolvable_samples` actually showed, so a value that already resolved under
    the first pass is simply `already_present` here (owned_by is idempotent,
    `_link_exists` checked before every mint, same as every other sub-migration in
    this module).

    DRY RUN IS THE DEFAULT. `dry_run=False` REQUIRES a non-blank `because`."""
    if not dry_run and not (because or "").strip():
        return {"error": "migrating without a because is an un-audited repair. Cite "
                         "the evidence/ruling that authorizes it"}
    pool = actions.pool
    now = datetime.now(UTC)
    owner_rows = await pool.fetch(
        "SELECT o.id AS subject_id, a.value #>> '{}' AS value "
        "FROM objects o JOIN current_assertions a ON a.object_id=o.id "
        "WHERE o.type='Thread' AND o.status='active' AND a.name='owner'")
    minted = minted_as_operator = minted_as_canonical = minted_as_handle = 0
    already_present = 0
    unresolvable_samples: list[str] = []
    for r in owner_rows:
        value = (r["value"] or "").strip()
        target_id = None
        via_operator = via_canonical = via_handle = False
        if value == "operator":
            target_id = await _resolve_ref(
                pool, "principal:analyst:operator", object_type="Person")
            via_operator = target_id is not None
        if target_id is None and value:
            target_id = await _resolve_ref(pool, value, object_type="Seat")
            via_canonical = target_id is not None
        if target_id is None and value:
            seat = await seat_by_handle(pool, value)
            if seat is not None:
                target_id = await _resolve_ref(pool, seat["seat_id"], object_type="Seat")
                via_handle = target_id is not None
        if target_id is None:
            if len(unresolvable_samples) < 20:
                unresolvable_samples.append(value or "<empty>")
            continue
        if await _link_exists(pool, r["subject_id"], target_id, "owned_by"):
            already_present += 1
            continue
        minted += 1
        if via_operator:
            minted_as_operator += 1
        if via_canonical:
            minted_as_canonical += 1
        if via_handle:
            minted_as_handle += 1
        if not dry_run:
            await actions.create_link(
                r["subject_id"], target_id, "owned_by", _SOURCE, now, _CONF,
                evidence_class=_EC)
    return {
        "dry_run": dry_run,
        "scanned": len(owner_rows),
        "minted": minted, "minted_as_operator": minted_as_operator,
        "minted_as_canonical": minted_as_canonical, "minted_as_handle": minted_as_handle,
        "skipped_unresolvable": len(owner_rows) - minted - already_present,
        "already_present": already_present,
        "unresolvable_samples": unresolvable_samples,
        "because": because if not dry_run else None,
    }


async def migrate_file_the_residual(
    actions: Actions, *, actor: str, dry_run: bool = True, because: str | None = None,
) -> dict[str, Any]:
    """FILE THE RESIDUAL: 1,280 objects
    stayed unfiled after `migrate_file_the_unfiled`'s own neighbour-majority vote,
    mostly Message objects. The top live ribbons off the unfiled fog are osiris-to-
    unfiled `broadcast_to` (800) and `addressed_to` (638), a rate the generic any-link-
    type vote can't clear cleanly: a Message's own `sent_by`/`addressed_to` agents
    routinely sit in different projects (a cross-project DM), so the generic vote ties
    and gives up exactly where a message-specific priority rule would not.

    MESSAGE-ONLY, ITS OWN RULE, NOT A SECOND GENERIC PASS: a `broadcast_to` link names
    the project directly (the SoftwareProject is the target, its own bare name wins
    outright, no vote needed) and wins first, whenever present, so a message broadcast
    to a project is never miscounted as a tie against its own sender's project. Absent
    that, every `sent_by`/`addressed_to` agent's own current `project` assertion is
    tallied; a single distinct project among them files the message, more than one
    distinct project is a genuine tie (a real cross-project DM) and stays unfiled,
    counted, the same "never guess between equally-supported candidates" rule
    `migrate_file_the_unfiled` already holds itself to. An object with neither shape
    (no broadcast_to, no sent_by/addressed_to agent with a resolvable project) is
    empty, also unfiled, also counted.

    SCOPE DELIBERATELY NARROW: only `type='Message'` objects currently unfiled (no
    `project` assertion, no live in_repo/works_in link). The residual's other
    members (non-Message) have no comparable rule stated for this pass and are left
    exactly as `migrate_file_the_unfiled` already reported them, not silently guessed
    at here.

    DRY RUN IS THE DEFAULT. `dry_run=False` REQUIRES a non-blank `because`. Idempotent:
    a repeat call finds no Message left both unfiled and resolvable by this rule."""
    if not dry_run and not (because or "").strip():
        return {"error": "migrating without a because is an un-audited repair. Cite "
                         "the evidence/ruling that authorizes it"}
    pool = actions.pool
    now = datetime.now(UTC)
    unfiled_rows = await pool.fetch(
        "SELECT o.id FROM objects o "
        "WHERE o.type='Message' AND o.status NOT IN ('archived','merged','retired') "
        "AND NOT EXISTS (SELECT 1 FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='project') "
        "AND NOT EXISTS (SELECT 1 FROM links l WHERE l.from_id=o.id "
        "  AND l.type IN ('in_repo','works_in') "
        "  AND (l.valid_until IS NULL OR l.valid_until > now()))")
    unfiled_ids = [r["id"] for r in unfiled_rows]
    filed: dict[uuid.UUID, str] = {}
    filed_via_broadcast = 0
    ties: list[dict[str, Any]] = []
    still_unfiled = 0
    if unfiled_ids:
        broadcast_rows = await pool.fetch(
            "SELECT l.from_id AS oid, p.canonical AS pcanon FROM links l "
            "JOIN objects p ON p.id=l.to_id AND p.type='SoftwareProject' "
            "  AND p.status='active' "
            "WHERE l.type='broadcast_to' AND l.from_id = ANY($1::uuid[]) "
            "AND (l.valid_until IS NULL OR l.valid_until > now())", unfiled_ids)
        broadcast_project: dict[uuid.UUID, str] = {
            r["oid"]: r["pcanon"].removeprefix("repo:") for r in broadcast_rows}

        remaining = [oid for oid in unfiled_ids if oid not in broadcast_project]
        agent_rows = await pool.fetch(
            "SELECT l.from_id AS oid, l.to_id AS agent_id FROM links l "
            "WHERE l.type IN ('sent_by','addressed_to') AND l.from_id = ANY($1::uuid[]) "
            "AND (l.valid_until IS NULL OR l.valid_until > now())", remaining) \
            if remaining else []
        agents_by_oid: dict[uuid.UUID, set[uuid.UUID]] = defaultdict(set)
        for r in agent_rows:
            agents_by_oid[r["oid"]].add(r["agent_id"])
        all_agent_ids = {a for s in agents_by_oid.values() for a in s}
        project_by_agent: dict[uuid.UUID, str] = {}
        if all_agent_ids:
            proj_rows = await pool.fetch(
                "SELECT object_id, value #>> '{}' AS pname FROM current_assertions "
                "WHERE object_id = ANY($1::uuid[]) AND name='project'", list(all_agent_ids))
            project_by_agent = {r["object_id"]: r["pname"] for r in proj_rows if r["pname"]}

        for oid in unfiled_ids:
            if oid in broadcast_project:
                filed[oid] = broadcast_project[oid]
                filed_via_broadcast += 1
                continue
            candidates = {project_by_agent[a] for a in agents_by_oid.get(oid, ())
                         if a in project_by_agent}
            if not candidates:
                still_unfiled += 1
                continue
            if len(candidates) > 1:
                ties.append({"object": str(oid)[:8], "candidates": sorted(candidates)})
                still_unfiled += 1
                continue
            filed[oid] = next(iter(candidates))

        if not dry_run:
            for oid, name in filed.items():
                await actions.assert_property(
                    oid, "project", name, _SOURCE, now, _CONF, evidence_class=_EC)

    return {
        "dry_run": dry_run,
        "scanned": len(unfiled_ids),
        "filed": len(filed), "filed_via_broadcast": filed_via_broadcast,
        "ties": len(ties), "ties_plan": ties,
        "still_unfiled": still_unfiled,
        "because": because if not dry_run else None,
    }


async def migrate_commits_to_agents(
    actions: Actions, *, actor: str, dry_run: bool = True, because: str | None = None,
) -> dict[str, Any]:
    """COMMITS ATTRIBUTED TO AGENT IDENTITIES, THE BACKFILL ENTRY POINT: an earlier
    commit taught `ingest_repo`
    to mint `committed_by` going forward; every Commit it minted before that landed has
    none. Same resolution as the going-forward path, applied retroactively: the
    Commit's own `in_repo` SoftwareProject's registered `on_disk_path` names the
    worktree (`_worktree_seat`/`tree_seat_hint` resolves the bound seat from it, never a
    string-guess), crossed with which generation held that seat at the Commit's own
    `authored_date` (`_seat_holder_at`, the holds link's own time-windowed history).
    Never guesses.

    THREE RESULT BUCKETS: `minted_by_worktree_time`
    (this pass's own resolution, the only kind it mints); `sharpened_by_trailer`
    (reserved, always 0 here; the session-trailer disambiguation step is its own
    scoped follow-up, built only if `abstained` below turns out big enough to warrant
    it); `abstained` (no committed_by minted, cause named
    per reason: no registered on_disk_path for the commit's repo, no seat bound to that
    path, or no holder's window covers the commit's own author time, with a small
    sample of each, for a human to spot-check).

    DRY RUN IS THE DEFAULT. `dry_run=False` REQUIRES a non-blank `because`. Idempotent:
    a repeat call only ever considers Commits still missing committed_by; a real write
    here can never re-mint or duplicate."""
    if not dry_run and not (because or "").strip():
        return {"error": "migrating without a because is an un-audited repair. Cite "
                         "the evidence/ruling that authorizes it"}
    from src.ingest.gitlog import _seat_holder_at, _worktree_seat

    pool = actions.pool
    now = datetime.now(UTC)
    rows = await pool.fetch(
        "SELECT c.id AS commit_id, c.canonical AS commit_canonical, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=c.id "
        "   AND a.name='authored_date' ORDER BY a.confidence DESC, a.observed_at DESC "
        "   LIMIT 1) AS authored_date, "
        " p.id AS project_id "
        "FROM objects c "
        "JOIN links l ON l.from_id=c.id AND l.type='in_repo' "
        "  AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "JOIN objects p ON p.id=l.to_id AND p.type='SoftwareProject' "
        "WHERE c.type='Commit' AND c.status='active' "
        "AND NOT EXISTS (SELECT 1 FROM links cb WHERE cb.from_id=c.id "
        "  AND cb.type='committed_by' "
        "  AND (cb.valid_until IS NULL OR cb.valid_until > now()))")

    project_ids = list({r["project_id"] for r in rows})
    on_disk_paths: dict[uuid.UUID, str | None] = {}
    if project_ids:
        path_rows = await pool.fetch(
            "SELECT o.id, (SELECT a.value #>> '{}' FROM current_assertions a "
            "  WHERE a.object_id=o.id AND a.name='on_disk_path' "
            "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS on_disk_path "
            "FROM objects o WHERE o.id = ANY($1::uuid[])", project_ids)
        on_disk_paths = {r["id"]: r["on_disk_path"] for r in path_rows}

    seat_by_project: dict[uuid.UUID, str | None] = {}
    minted: dict[uuid.UUID, str] = {}
    abstained: Counter[str] = Counter()
    abstained_sample: dict[str, list[str]] = defaultdict(list)

    def _abstain(reason: str, commit_canon: str) -> None:
        abstained[reason] += 1
        if len(abstained_sample[reason]) < 5:
            abstained_sample[reason].append(commit_canon)

    for r in rows:
        commit_id, commit_canon = r["commit_id"], r["commit_canonical"]
        project_id, authored_date = r["project_id"], r["authored_date"]
        if not authored_date:
            _abstain("no-authored-date", commit_canon)
            continue
        at = datetime.fromisoformat(authored_date.replace("Z", "+00:00"))
        if project_id not in seat_by_project:
            path = on_disk_paths.get(project_id)
            seat_by_project[project_id] = (
                await _worktree_seat(pool, worktree_path=path) if path else None)
        seat_id = seat_by_project[project_id]
        if seat_id is None:
            _abstain("no-seat-bound-to-worktree", commit_canon)
            continue
        holder = await _seat_holder_at(pool, seat_id=seat_id, at=at)
        if holder is None:
            _abstain("no-holder-at-author-time", commit_canon)
            continue
        minted[commit_id] = holder

    if not dry_run and minted:
        for commit_id, holder in minted.items():
            agent_oid = await actions.create_or_find_object("Agent", holder, actor)
            await actions.create_link(commit_id, agent_oid, "committed_by", actor, now,
                                      _CONF, evidence_class=_EC)

    return {
        "dry_run": dry_run,
        "scanned": len(rows),
        "minted_by_worktree_time": len(minted),
        "sharpened_by_trailer": 0,
        "abstained": sum(abstained.values()),
        "abstained_reasons": dict(abstained),
        "abstained_sample": dict(abstained_sample),
        "because": because if not dry_run else None,
    }


async def migrate_house_to_project(
    actions: Actions, *, actor: str, dry_run: bool = True, because: str | None = None,
) -> dict[str, Any]:
    """THE Seat.house REPAIR: fleet-wide backfill for every active Seat whose stamped
    `house` is null,
    fabricated, or simply out of step with what its own charter actually governs,
    the same sweep shape `sweep_seat_trees` already established for `tree_cwd`, one
    property over. Reuses `seats.resync_seat_project` for every real write (the same
    re-derive-from-charter entry point the CLI's own `resync-seat-project` calls), never a
    second implementation.

    A seat's project is derived, never a second value: this migration only ever
    touches a seat whose charter governs exactly one project, and stamps that.
    Every other shape is reported, never guessed: no charter at all (`refused_why:
    "no charter"`), a charter governing more than one project (`refused_why:
    "ambiguous charter"`), a seat whose stamped house already matches its charter's
    own single governed project (skipped, not listed, nothing to repair), or a
    seat whose stamped house is non-null and disagrees with the charter's own
    single governed project (`refused_why: "stamped house disagrees with charter:
    <old> vs <new>"`, a deliberate carve-out: a real value already
    on the seat is a fact this entry point has no standing to overwrite; only a null house
    is repaired here, a disagreement goes to a human to resolve).

    DRY RUN IS THE DEFAULT. `dry_run=False` REQUIRES a non-blank `because`.
    Idempotent: a repeat call finds every already-repaired seat's house matching its
    charter and skips it."""
    if not dry_run and not (because or "").strip():
        return {"error": "migrating without a because is an un-audited repair. Cite "
                         "the evidence/ruling that authorizes it"}
    from src.orchestrator.charter import charter_of, project_current_name
    from src.orchestrator.seats import resync_seat_project

    pool = actions.pool
    rows = await pool.fetch(
        "SELECT o.canonical AS seat_id, "
        "  (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='house' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS house "
        "FROM objects o WHERE o.type='Seat' AND o.status='active'")

    entries: list[dict[str, Any]] = []
    for r in rows:
        seat_id, house = r["seat_id"], (r["house"] or None)
        governed = await charter_of(pool, seat_id)
        if len(governed) != 1:
            entries.append({
                "seat": seat_id, "old_house": house, "new_project": None,
                "refused_why": "no charter" if not governed else "ambiguous charter",
            })
            continue
        # THE CURRENT NAME, NEVER THE CANONICAL: a live specimen showed a project
        # renamed after its canonical was minted; comparing against
        # governed[0]'s bare canonical made an already-
        # correct stamp look like a disagreement that never existed.
        new_project = await project_current_name(pool, governed[0])
        if house == new_project:
            continue  # already correct, nothing to repair
        if house is not None:
            entries.append({
                "seat": seat_id, "old_house": house, "new_project": None,
                "refused_why": f"stamped house disagrees with charter: "
                               f"{house} vs {new_project}",
            })
            continue
        if dry_run:
            entries.append({"seat": seat_id, "old_house": house,
                            "new_project": new_project, "refused_why": None})
            continue
        result = await resync_seat_project(
            actions, seat_id, source=actor,
            reason=f"{because} (migrate_house_to_project: fleet-wide backfill)")
        entries.append({
            "seat": seat_id, "old_house": house,
            "new_project": result.get("project") if "error" not in result else None,
            "refused_why": result.get("error")})
    return {
        "dry_run": dry_run, "scanned": len(rows), "swept": len(entries),
        "repaired": sum(1 for e in entries if e["new_project"] and not e["refused_why"]),
        "refused": sum(1 for e in entries if e["refused_why"]),
        "entries": entries,
        "because": because if not dry_run else None,
    }


async def migrate_holds_sandwich(
    actions: Actions, *, actor: str, dry_run: bool = True, because: str | None = None,
    only_seat: str | None = None,
) -> dict[str, Any]:
    """THE HOLDS-SANDWICH REPAIR, found while diagnosing commits_to_agents:
    `_bind_before_spawn` used to mint a
    fresh, real generation and move the seat's `holds` link onto it purely as pre-spawn
    bookkeeping (agents.py's `mint_heir` now takes `bind_seat=False` for that one call
    site, the going-forward fix, committed alongside this one). Every holds history
    minted before that fix can carry a real generation's own continuous tenure split
    into two rows with a near-instant `launch_seat: bind-before-spawn`-minted phantom
    sandwiched between them. The holds link is the fact of record for "who held this
    seat at time T," wrong data regardless of how few readers ever asked that exact
    question.

    A SANDWICH: three consecutive holds rows on one seat (ordered by `first_seen`)
    where the middle row's holder carries a `minted_because` starting with
    "launch_seat: bind-before-spawn" and the first and third rows' holder is the same
    agent. Never a bare "the gap belongs to the nearest neighbour" guess (that would
    also swallow a genuine vacancy after a real vacate_dead_seat); this recognizes only
    the one diagnosed, precisely-named shape.

    REPAIR, COMPENSATING, NEVER A DELETE: `invalidate_link`'s own entry point only ever closes
    a currently-open link (`WHERE valid_until IS NULL`), so it cannot touch these three
    rows, since every one of them is already closed, historical, by the time this migration
    ever runs. This is the one shape in this file that reaches past that entry point on
    purpose: it extends the first row's own `valid_until` forward to the third row's own
    `valid_until` (re-opening the real holder's continuous tenure across the whole
    sandwich) and closes the phantom middle row and the now-redundant third row down to
    zero width at their own `first_seen`, retired, never deleted. Every original row
    stays exactly where it was written, in whose name, and why; only the recorded
    interval each one covers changes.

    THE EVIDENCE GATE: a live dry run found five
    real sandwiches, not the roughly two the diagnosis anticipated, one spanning six weeks,
    and a bridge across that much time risks laundering a genuine vacancy into continuous
    tenure. Every sandwich found now carries, in the result, two distinct windows.
    Conflating them was this gate's own first bug, caught by a later live probe: a
    real specimen on one seat had a 174ms phantom immediately followed by a
    further ~43-minute stretch with no holds row at all before the real holder's next
    row began, so gap evidence checked against the phantom's own window alone found
    nothing even though the 19 commits from the original diagnosis sat inside that wider
    stretch:
      - `phantom_window`/`phantom_duration_seconds`: the middle row's own tenure
        (`w2.first_seen` -> `w2.valid_until`), how long the bind-before-spawn mint sat
        there before something superseded it. Gates on `_NEAR_INSTANT_MAX_SECONDS`.
      - `gap_evidence`: activity from the real holder (the shared `w1`/`w3` agent id:
        messages sent, commits committed_by them, Decisions/Threads they authored)
        timestamped anywhere between the two real-holder rows (`w1.valid_until` ->
        `w3.first_seen`), which is not always the same interval as the phantom's own
        window, since the holds chain's rows need not be back-to-back. This is the span
        described literally as "between the two rows."
    A sandwich is refused (reported, never written, even under `--apply`) when either
    gate fails: zero gap evidence ("vacancy, not a cut"), or a phantom window longer
    than `_NEAR_INSTANT_MAX_SECONDS` (not a brief blip by any reading, whatever the
    evidence says). `only_seat` (a seat's `seat:<id>` or its bare handle) narrows the
    whole scan to one seat, refused with an `error` result if it does not resolve to
    exactly one active seat.

    DRY RUN IS THE DEFAULT. `dry_run=False` REQUIRES a non-blank `because`, and writes
    only the sandwiches that pass both gates; a refused sandwich stays listed, with
    `refused_why`, on every call, applied or not. Idempotent: a repeat call finds no
    sandwich left among the ones it wrote (the middle and third rows read zero-width,
    never matching the three-consecutive-rows shape again)."""
    if not dry_run and not (because or "").strip():
        return {"error": "migrating without a because is an un-audited repair. Cite "
                         "the evidence/ruling that authorizes it"}
    pool = actions.pool

    seat_filter: str | None = None
    if only_seat:
        if only_seat.startswith("seat:"):
            seat_filter = only_seat
        else:
            resolved = await seat_by_handle(pool, only_seat)
            if resolved is None:
                return {"error": f"--only {only_seat!r} does not resolve to exactly "
                                 "one active seat"}
            seat_filter = resolved["seat_id"]

    rows = await pool.fetch(
        "SELECT t.canonical AS seat, f.canonical AS holder, l.id, l.first_seen, "
        " l.valid_until, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=f.id "
        "   AND a.name='minted_because' ORDER BY a.confidence DESC, a.observed_at DESC "
        "   LIMIT 1) AS minted_because "
        "FROM links l JOIN objects f ON f.id=l.from_id JOIN objects t ON t.id=l.to_id "
        "WHERE l.type='holds' AND t.type='Seat' "
        + ("AND t.canonical=$1 " if seat_filter else "")
        + "ORDER BY t.canonical, l.first_seen",
        *([seat_filter] if seat_filter else []))

    by_seat: dict[str, list[Any]] = defaultdict(list)
    for r in rows:
        by_seat[r["seat"]].append(r)

    sandwiches: list[dict[str, Any]] = []
    for seat, windows in by_seat.items():
        i = 0
        while i + 2 < len(windows):
            w1, w2, w3 = windows[i], windows[i + 1], windows[i + 2]
            phantom = (w2["minted_because"] or "").startswith(_BIND_BEFORE_SPAWN_PREFIX)
            # IDEMPOTENCY: a repaired sandwich's own middle/third rows read zero-width
            # (valid_until == first_seen). A live, real window never does (`create_link`
            # always mints valid_until IS NULL, later closed to a real, later instant by
            # a real event), so a zero-width row here means this exact sandwich was
            # already repaired, never a fresh match on the very rows this migration wrote.
            already_repaired = (w2["valid_until"] == w2["first_seen"]
                                or w3["valid_until"] == w3["first_seen"])
            if (phantom and not already_repaired
                    and w1["holder"] == w3["holder"] and w1["holder"] != w2["holder"]):
                sandwiches.append({
                    "seat": seat, "real_holder": w1["holder"], "phantom": w2["holder"],
                    "window": [w1["first_seen"].isoformat(),
                              w3["valid_until"].isoformat() if w3["valid_until"] else None],
                    "_w1_id": w1["id"], "_w2_id": w2["id"], "_w3_id": w3["id"],
                    "_w3_valid_until": w3["valid_until"],
                    # PHANTOM WINDOW: the bookkeeping row's own tenure, how long the
                    # bind-before-spawn mint sat there before something superseded it.
                    "_phantom_start": w2["first_seen"], "_phantom_end": w2["valid_until"],
                    # THE REAL GAP: between the two real-holder rows, not the phantom's
                    # own window; these are not always the same instant. The holds
                    # chain's own rows are consecutive by construction (this migration's
                    # query), but w2.valid_until need not equal w3.first_seen: a live
                    # specimen showed a phantom lasting
                    # 174ms immediately followed by a further ~43-minute stretch with no
                    # holds row at all before the real holder's next row begins. Evidence
                    # of activity belongs to this wider span; "between the two rows" is
                    # literally w1's end to w3's start, whatever sits in between.
                    "_gap_start": w1["valid_until"], "_gap_end": w3["first_seen"],
                })
                i += 3  # the whole sandwich is consumed, never re-match its own pieces
            else:
                i += 1

    for s in sandwiches:
        phantom_start, phantom_end = s.pop("_phantom_start"), s.pop("_phantom_end")
        gap_start, gap_end = s.pop("_gap_start"), s.pop("_gap_end")
        duration_s = (phantom_end - phantom_start).total_seconds() if phantom_end else None
        near_instant = duration_s is not None and duration_s <= _NEAR_INSTANT_MAX_SECONDS
        evidence = await _gap_activity(
            pool, agent=s["real_holder"], start=gap_start, end=gap_end)
        s["phantom_window"] = [phantom_start.isoformat(),
                               phantom_end.isoformat() if phantom_end else None]
        s["phantom_duration_seconds"] = duration_s
        s["gap_evidence"] = evidence
        refused_why = []
        if evidence["count"] == 0:
            refused_why.append("vacancy_no_evidence_in_gap")
        if not near_instant:
            refused_why.append("phantom_not_near_instant")
        s["refused_why"] = refused_why or None

    if not dry_run:
        for s in sandwiches:
            if s["refused_why"]:
                continue
            await pool.execute(
                "UPDATE links SET valid_until=$1 WHERE id=$2",
                s["_w3_valid_until"], s["_w1_id"])
            await pool.execute(
                "UPDATE links SET valid_until=first_seen WHERE id=$1", s["_w2_id"])
            await pool.execute(
                "UPDATE links SET valid_until=first_seen WHERE id=$1", s["_w3_id"])
            s["applied"] = True

    for s in sandwiches:  # internal row ids are an implementation detail, never in the result
        s.setdefault("applied", False)
        for key in ("_w1_id", "_w2_id", "_w3_id", "_w3_valid_until"):
            del s[key]

    return {
        "dry_run": dry_run,
        "sandwiches_found": len(sandwiches),
        "sandwiches": sandwiches,
        "because": because if not dry_run else None,
    }


async def _gap_activity(
    pool: asyncpg.Pool, *, agent: str, start: datetime, end: datetime | None,
) -> dict[str, Any]:
    """Evidence the `agent` (a canonical `agent:<id>`) was actively doing something:
    a message sent, a commit committed_by them, a Decision/Thread they authored,
    timestamped in `[start, end)`. `end=None` (an open holds row, which should not occur
    for an already-closed phantom middle row, but a real caller never crashes on it)
    reads as "no upper bound". Uses `created_at` (server-assigned at write time, never
    backdatable by the writer) for messages/Decisions/Threads, and the `committed_by`
    link's own `first_seen` (the commit's real author date, from git, not this
    migration's clock) for commits, never `observed_at`, which a source can honestly
    claim for a past instant and so proves nothing about when the work happened."""
    end_clause = "AND ts < $3" if end is not None else ""
    args: list[Any] = [agent, start] + ([end] if end is not None else [])
    row = await pool.fetchrow(
        "WITH ev AS ("
        " SELECT created_at AS ts FROM fleet_messages"
        "   WHERE from_agent=$1 AND created_at >= $2"
        " UNION ALL"
        " SELECT a.created_at AS ts FROM assertions a JOIN objects o ON o.id=a.object_id"
        "   WHERE o.type IN ('Decision', 'Thread') AND a.name='summary'"
        "     AND a.source_id=$1 AND a.created_at >= $2"
        " UNION ALL"
        " SELECT l.first_seen AS ts FROM links l JOIN objects t ON t.id=l.to_id"
        "   WHERE l.type='committed_by' AND t.canonical=$1 AND l.first_seen >= $2"
        ") "
        "SELECT count(*) AS n, min(ts) AS first_ts, max(ts) AS last_ts "
        f"FROM ev WHERE 1=1 {end_clause}",
        *args)
    return {
        "count": row["n"],
        "first": row["first_ts"].isoformat() if row["first_ts"] else None,
        "last": row["last_ts"].isoformat() if row["last_ts"] else None,
    }


async def migrate_project_name_singular(
    actions: Actions, *, actor: str, dry_run: bool = True, because: str | None = None,
) -> dict[str, Any]:
    """THE PROJECT-NAME COLLAPSE: a SoftwareProject's `name` is a singular fact, exactly one
    current value, ever, but `assert_property`'s own same-source-only supersession
    lets a genuine rename sit beside every prior self-declared/
    disk-census/ingest-sourced name rather than retiring them, so a project can carry
    many simultaneously-current names at once. A live specimen: one project's canonical
    read
    27 competing current `name` assertions (three variant spellings) from six different
    sources. `dossier`/`triage` already mark this as "contradicted"
    (a general rule), but marking is not resolving, and every "which project"
    derivation other related fixes (`project_current_name`, charter.py) now
    depend on needs exactly one answer to read back.

    THE WINNER: highest confidence, ties broken by most recent `observed_at`, the
    same `ORDER BY a.confidence DESC, a.observed_at DESC` every other "current winning
    value" reader in this codebase already uses (charter_of, governed_trees, the seat-
    facts readers). `rename_project`'s own writes are confidence 0.95, deliberately
    above every ordinary evidence-class ceiling (SELF_DECLARED's own 0.9 is the highest
    ordinary tier), so a live human-directed rename always outranks a stale disk-census
    or ingest-sourced guess without needing a second, source-string-based special case.

    COMPENSATING, VIA `assert_singular_property` (cross-source collapse, the intended
    route for exactly this shape): every losing current
    assertion is superseded, none deleted. The full history of every name this
    project ever carried, and who claimed it, stays in the assertion log forever.

    DRY RUN IS THE DEFAULT. `dry_run=False` REQUIRES a non-blank `because`. Idempotent:
    a repeat call finds no SoftwareProject left with more than one current `name`."""
    if not dry_run and not (because or "").strip():
        return {"error": "migrating without a because is an un-audited repair. Cite "
                         "the evidence/ruling that authorizes it"}
    pool = actions.pool
    dupes = await pool.fetch(
        "SELECT o.id AS oid, o.canonical FROM objects o "
        "JOIN current_assertions a ON a.object_id=o.id AND a.name='name' "
        "WHERE o.type='SoftwareProject' AND o.status='active' "
        "GROUP BY o.id, o.canonical HAVING count(DISTINCT a.value #>> '{}') > 1")

    entries: list[dict[str, Any]] = []
    for r in dupes:
        names = await pool.fetch(
            "SELECT a.value #>> '{}' AS name FROM current_assertions a "
            "WHERE a.object_id=$1 AND a.name='name' "
            "ORDER BY a.confidence DESC, a.observed_at DESC", r["oid"])
        winner = names[0]["name"]
        entries.append({
            "project": r["canonical"], "winner": winner,
            "current_names": [n["name"] for n in names],
        })
        if not dry_run:
            await actions.assert_singular_property(
                r["oid"], "name", winner, actor, datetime.now(UTC), _CONF,
                because=f"{because} (migrate_project_name_singular: collapsed "
                        f"{len(names)} competing current names, highest-confidence/"
                        "newest value wins)",
                evidence_class=_EC)
    return {
        "dry_run": dry_run, "found": len(entries), "entries": entries,
        "because": because if not dry_run else None,
    }
