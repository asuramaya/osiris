"""THE STUB CULL (msg 1675, Thoth's dispatch) — a sanctioned verb to retire a dead
SoftwareProject, shaped after seats.py's retire_seat/vacate_holder: a single self-contained
function that gathers its own refusal evidence and, only once every check clears, performs
the write. Unlike vacate_dead_seat's seat-liveness check, every signal a project needs
(commits, open threads, a recent mount) is already a graph/table read — no external process
or transcript evidence to gather, so this needed no trigger.py-style split.

THE STATUS FLIP IS A COMPENSATING EVENT, not a bare property assertion: `Actions.set_status`
writes an append-only `object_events` row (event_type='status_change') alongside the
`objects.status` column flip, so a retirement is auditable and reversible (re-`set_status`
back to 'active') exactly like every other lifecycle transition in this codebase — never a
DELETE.

REFUSES LOUDLY on: a blank `because`; a `project` ref that doesn't resolve to a
SoftwareProject (scoped to that type ONLY — never resolves through to a Seat or Agent of the
same name, the exact ambiguity Thoth flagged for 'seshat'/'ra', which are stub PROJECTS
distinct from the seats of the same names); a project already non-active; any commit
recorded against it (`in_repo` from a Commit); any open Thread pointing in (`in_repo`,
status='active'); or a mount seen against it within the live window (15 minutes, the same
threshold `mounts.agent_liveness` already uses for a mind). The receipt always names the
project by its CANONICAL (`repo:<name>`), never a bare name, so it can never be misread as a
seat's receipt."""
from __future__ import annotations

import re
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

_LIVE_MOUNT_WINDOW = timedelta(minutes=15)
_EC = EvidenceClass.SELF_DECLARED.value
_CONF = confidence_for(EvidenceClass.SELF_DECLARED)


class AmbiguousProjectRef(Exception):
    """Raised by `_resolve_software_project` when a bare label resolves to MORE THAN ONE
    active SoftwareProject (#110, decision 1db1ff41 — the ballgem/sutra shape: two live
    objects, one canonicaled by its bare name and one by its full disk path, each still
    answering to the same `name` assertion). The old fallback was `LIMIT 1` with no
    `ORDER BY` — whichever row postgres felt like, silently, for every caller including
    retire_project and fold_project. A PERMANENT correctness property, not a workaround
    for today's specific duplicates: a verb that mutates the graph must never guess which
    of two live objects a bare label meant, this month or the next time a pair collides.
    `candidates` names every match so the caller reports, never guesses."""

    def __init__(self, ref: str, candidates: list[str]) -> None:
        self.ref = ref
        self.candidates = candidates
        super().__init__(f"{ref!r} resolves to {len(candidates)} active SoftwareProjects: "
                         f"{', '.join(candidates)}")


async def _resolve_software_project(pool: asyncpg.Pool, ref: str) -> asyncpg.Record | None:
    """A SoftwareProject ONLY — a full UUID, an 8-char short id, an exact canonical
    (`repo:<name>` accepted with or without the prefix), or its `name` property. Never
    widens to another object type: this is the structural half of the seshat/ra
    disambiguation, not just wording in the receipt.

    Raises `AmbiguousProjectRef` (never silently picks) when the `name`-property fallback
    matches more than one distinct active object — the UUID/short-id/canonical paths above
    stay exact-key lookups and can never collide."""
    ref = ref.strip()
    try:
        oid = uuid.UUID(ref)
    except ValueError:
        oid = None
    if oid is not None:
        row = await pool.fetchrow(
            "SELECT id, canonical, status FROM objects WHERE id=$1 AND type='SoftwareProject'",
            oid)
        if row is not None:
            return row  # an exact object id — never ambiguous by construction
    short = ref.lower()
    if re.fullmatch(r"[0-9a-f]{8}[0-9a-f-]*", short):
        row = await pool.fetchrow(
            "SELECT id, canonical, status FROM objects "
            "WHERE type='SoftwareProject' AND id::text LIKE $1 || '%' LIMIT 1", short)
        if row is not None:
            return row  # a short id prefix — practically unique, same convention elsewhere

    # From here `ref` denotes a LABEL, not an object id — canonical-string match and
    # `name`-property match are BOTH label lookups and must be checked for a collision
    # TOGETHER, or an object findable only by canonical (repo:ballgem) would hide a
    # sibling findable only by its own `name` property (a differently-canonicaled twin
    # whose winning name still equals this same bare label) without either lookup ever
    # noticing the other existed.
    bare = ref.removeprefix("repo:")
    canon = ref if ref.startswith("repo:") else f"repo:{ref}"
    rows = await pool.fetch(
        "SELECT o.id, o.canonical, o.status FROM objects o "
        "WHERE o.type='SoftwareProject' AND o.status='active' "
        "AND (o.canonical=$1 OR (SELECT a.value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='name' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) = $2)", canon, bare)
    if len(rows) > 1:
        raise AmbiguousProjectRef(ref, sorted(str(r["canonical"]) for r in rows))
    if rows:
        return rows[0]
    # a non-active (retired/merged) object is reachable only by its exact canonical — a
    # caller resolving a KNOWN-dead label (retire_project's own "already retired" message)
    # must still find it, just never through the ambiguity-checked label path above
    return await pool.fetchrow(
        "SELECT id, canonical, status FROM objects WHERE type='SoftwareProject' AND "
        "canonical=$1", canon)


async def _resolve_project_ref(
    pool: asyncpg.Pool, ref: str, *, verb: str,
) -> tuple[asyncpg.Record | None, dict[str, Any] | None]:
    """Shared refusal wrapper over `_resolve_software_project`: (row, None) on a clean
    resolution, (None, error_dict) on an ambiguous ref — every verb below reports that
    shape identically rather than re-writing the same try/except each time. A `None` row
    with no error still means "not found," exactly as `_resolve_software_project` itself
    signals it; callers keep their own "no such SoftwareProject" message."""
    try:
        row = await _resolve_software_project(pool, ref)
    except AmbiguousProjectRef as amb:
        return None, {"error": f"{amb.ref!r} is ambiguous — {len(amb.candidates)} active "
                               f"SoftwareProjects answer to it: "
                               f"{', '.join(amb.candidates)}. Name the exact one "
                               f"(canonical or id) — {verb} never guesses which."}
    return row, None


async def retire_project(
    actions: Actions, *, project: str, actor: str, because: str,
) -> dict[str, Any]:
    """Retire a dead SoftwareProject stub — status flip to 'retired' via a compensating
    event (`Actions.set_status`). Refuses on: blank `because`; an unresolved or non-active
    project; any commit, any open thread pointing in, or any mount seen within the last 15
    minutes against it (live signal — this verb never evicts a project that's actually in
    use)."""
    because = (because or "").strip()
    if not because:
        return {"error": "because is required — retiring a project is a deliberate act on "
                         "the record"}
    project = (project or "").strip()
    if not project:
        return {"error": "project is required"}
    row, err = await _resolve_project_ref(actions.pool, project, verb="retire_project")
    if err:
        return err
    if row is None:
        return {"error": f"no such SoftwareProject: {project!r}"}
    pid, canonical, status = row["id"], row["canonical"], row["status"]
    if status != "active":
        return {"error": f"{canonical} is already {status} — nothing to retire"}
    commits = await actions.pool.fetchval(
        "SELECT count(*) FROM links l JOIN objects c ON c.id=l.from_id "
        "WHERE l.to_id=$1 AND l.type='in_repo' AND c.type='Commit'", pid)
    if commits:
        return {"error": f"{canonical} has {commits} commit(s) — live signal, "
                         "retire_project refuses"}
    open_threads = await actions.pool.fetchval(
        "SELECT count(*) FROM links l JOIN objects t ON t.id=l.from_id "
        "WHERE l.to_id=$1 AND l.type='in_repo' AND t.type='Thread' AND t.status='active'", pid)
    if open_threads:
        return {"error": f"{canonical} has {open_threads} open thread(s) pointing in — "
                         "live signal, retire_project refuses"}
    bare_name = canonical.removeprefix("repo:")
    mount_seen = await actions.pool.fetchval(
        "SELECT max(last_seen) FROM agent_mounts WHERE project=$1", bare_name)
    if mount_seen is not None and (datetime.now(UTC) - mount_seen) < _LIVE_MOUNT_WINDOW:
        return {"error": f"{canonical} has a live mount (seen {mount_seen.isoformat()}) — "
                         "live signal, retire_project refuses"}
    await actions.set_status(pid, "retired", because, actor)
    return {"retired_project": canonical, "id": str(pid)[:8], "because": because}


async def assert_project_property(
    actions: Actions, *, project: str, name: str, value: str, actor: str,
) -> dict[str, Any]:
    """The sanctioned write for a SINGLE project-scoped property — task #74's own gap:
    the reap (msg 1675/1689) had no verb for anything beyond a status flip, forcing
    in-process scripts for every other lifecycle stamp. Resolves `project` exactly like
    retire_project (UUID/short-id/canonical/name, SoftwareProject ONLY — the same
    seshat/ra disambiguation). NOT self-scoped, and OPEN BY DESIGN, not merely
    unenforced (census a5e53ed8/3f97f9c7 found "any authorized caller" a vacuous claim —
    no authorization concept was ever checked here; corrected 2026-08-02 to say what is
    actually true): any mounted caller may stamp any named project's property; `actor` is
    attribution, never an authority gate (same reasoning as correct_agent_house). A
    property write is reversible (a fresh assert supersedes cleanly) and fully visible in
    the record — if a manager-only restriction is ever wanted, it belongs here as a real
    check, not as prose a reader has to trust.

    Refuses LOUDLY on: blank project/name/value; an unresolved project; `name=='status'`
    — status has its OWN compensating-event path (retire_project -> Actions.set_status,
    the object_events audit trail); a bare assertion here would silently reopen the exact
    STATUS GAP class already fixed once for seats (retire_seat, commit 122d642)."""
    project = (project or "").strip()
    name = (name or "").strip()
    value = (value or "").strip()
    if not project:
        return {"error": "project is required"}
    if not name:
        return {"error": "name is required — a property needs a name"}
    if not value:
        return {"error": "value is required — asserting a blank value is not a fact"}
    if name == "status":
        return {"error": "status has its own compensating-event path — use "
                         "retire_project (or a future sibling), never a bare property "
                         "assertion"}
    row, err = await _resolve_project_ref(actions.pool, project, verb="assert_project_property")
    if err:
        return err
    if row is None:
        return {"error": f"no such SoftwareProject: {project!r}"}
    await actions.assert_property(row["id"], name, value, actor, datetime.now(UTC), _CONF,
                                  evidence_class=_EC)
    return {"project": row["canonical"], "name": name, "value": value}


# --- fold_project (task #102's LANE 2, Thoth's dispatch DM 2302/2310) --------------------

async def _contradicting_properties(
    pool: asyncpg.Pool, a_id: uuid.UUID, b_id: uuid.UUID,
) -> list[str]:
    """Task #102's mark-not-resolve primitive (dossier.py's `agreement` field,
    compositions.py's `contradicted` triage bucket), reused here as a REFUSAL SIGNAL
    instead of a display marker: for every property name either object currently
    carries, does each object's own BELIEF (its winning value — the same
    `confidence DESC, observed_at DESC` resolution every other belief-read site in this
    codebase already uses, e.g. `trace_evidence`'s `believes`) disagree between the two
    objects? `name`/`tag` are excluded — a label difference is a fold's own PREMISE (two
    tags for one referent is exactly the operator's "SAME data, DIFFERENT tags -> merge
    is correct" case), never a sign these are two different things; every OTHER property
    disagreeing is the opposite case ("SAME tag, DIFFERENT data") the operator named as
    the one merge must never cross.

    BELIEF-RESOLVED, NOT RAW (Sekhmet's ramstein trace, decision 9ae4feee; obligation
    114f7ac9's sibling fork, resolved (B)): `current_assertions` is every un-superseded
    assertion — and `assert_property` only supersedes WITHIN the same source
    (actions/core.py's own advisory lock is keyed on object+name+SOURCE), so a stale
    assertion from a DIFFERENT source than the one that later corrected it stays live in
    that view forever. The old raw `GROUP BY name HAVING count(DISTINCT value) > 1`
    query saw that stale cross-source row as a second "current" value even on a SINGLE
    object with no real disagreement at all — merge()'s gate was comparing "has anyone
    ever asserted something different" (raw), not "do these two objects currently
    disagree" (belief). A corrector (assert_project_property, etc.) that WINS on
    confidence/recency must be able to unblock a fold without a second, unexposed act
    (retiring the loser assertion by row id — no read verb surfaces one) just to make the
    old row stop counting."""
    rows = await pool.fetch(
        "WITH belief AS ("
        "  SELECT DISTINCT ON (object_id, name) object_id, name, "
        "    value #>> '{}' AS v "
        "  FROM current_assertions "
        "  WHERE object_id = ANY($1::uuid[]) AND name NOT IN ('name', 'tag') "
        "  ORDER BY object_id, name, confidence DESC, observed_at DESC"
        ") "
        "SELECT name FROM belief GROUP BY name HAVING count(DISTINCT v) > 1",
        [a_id, b_id])
    return sorted(r["name"] for r in rows)


# every live link type link_repo/works_in/governs/informs can put ON a SoftwareProject
# (src/ontology/schema.py) — re-pointed FROM `dupe` TO `into` before the kernel merge,
# since Actions.merge_objects never touches a pre-existing `links` row itself (see its
# own docstring: "assertions are never rewritten"; the same is true of links).
_PROJECT_ESTATE_LINK_TYPES = ("in_repo", "works_in", "governs", "informs")


async def fold_project(
    actions: Actions, *, dupe: str, into: str, evidence: str, actor: str,
) -> dict[str, Any]:
    """Fold SoftwareProject `dupe` into `into` — the deliberate, evidence-gated cure for a
    TWIN (two SoftwareProject objects that are really one project under two labels; #107's
    own path-shaped mint and a basename collision are tonight's live cases, not a merge
    machine built ahead of need).

    BOTH ENDPOINTS MUST ALREADY EXIST (Thoth's correction, DM 2310, from redmonth's own
    counterexample: a project whose graph presence is real — hundreds of edges, a seat, a
    succession chain — while its code identity has moved on needs a RENAME-WITH-SUCCESSION
    primitive, a different verb, not this one; `into` not existing means the caller wants
    that verb). fold_project never find-or-CREATEs `into` — that would be #107's own
    disease reborn inside a merge verb. The refusal on a missing target NAMES the right
    tool rather than silently minting one.

    THE GUARD IS THE DESIGN (the operator's own categorical rule, via Thoth's dispatch
    DM 2279, task #102): SAME data, DIFFERENT tags -> one referent, merge is correct. SAME
    tag, DIFFERENT data -> two things, or one thing with contested facts -> merge is
    WRONG. `_contradicting_properties` checks every property OTHER than name/tag (a label
    difference is this fold's own premise, not a conflict) for genuine cross-object
    disagreement between `dupe` and `into` — a hit refuses loudly and names exactly what
    conflicts, because a wrong merge does not just lose a duplicate, it DESTROYS a
    recorded disagreement, which was data.

    Never gates on commits, or the lack of them (unlike retire_project's stub-cull guard)
    — a project's graph presence can be real while its code presence is zero, and this
    verb must fit that shape even though redmonth itself is not its case.

    ESTATE moved BEFORE the kernel call: every live edge of `_PROJECT_ESTATE_LINK_TYPES`
    pointing FROM another object INTO `dupe` re-points to `into` (idempotent — a link
    already live to `into` is never duplicated), then `agent_mounts.project` (a loose
    string match, never a FK — same shape fold_agent's raw `agent_mounts.agent_id` UPDATE)
    is re-addressed the same way. Only then does `Actions.merge_objects` run.

    REVERSIBLE, never a delete: the kernel stamps `merged_into` + `status='merged'` on
    `dupe`; `unmerge_objects` restores that projection. The re-pointed estate is NOT
    automatically restored by an unmerge (the same `estate_unreturnable` class
    `unfold_agent` already reports) — the receipt names every edge moved so a reversal by
    hand has the list.

    Refuses LOUDLY, nothing written, on: empty evidence (an auto-merge wearing a
    signature); blank dupe/into; dupe==into; either label not resolving to an ACTIVE
    SoftwareProject (missing, wrong type, or already merged); a genuine cross-object
    contradiction on any non-name/tag property."""
    dupe, into = (dupe or "").strip(), (into or "").strip()
    if not (evidence or "").strip():
        return {"error": "a fold without evidence is an auto-merge wearing a signature — "
                         "cite what proves these are one project"}
    if not dupe or not into:
        return {"error": "fold_project needs both labels: dupe and into"}
    if dupe == into:
        return {"error": "dupe and into name the same project — nothing to fold"}
    dupe_row, dupe_err = await _resolve_project_ref(actions.pool, dupe, verb="fold_project")
    if dupe_err:
        return dupe_err
    into_row, into_err = await _resolve_project_ref(actions.pool, into, verb="fold_project")
    if into_err:
        return into_err
    if dupe_row is None or into_row is None:
        missing = [label for label, row in ((dupe, dupe_row), (into, into_row))
                  if row is None]
        return {"error": f"unknown SoftwareProject(s): {', '.join(missing)} — fold_project "
                         "never invents either side; if the target doesn't exist yet, this "
                         "is a RENAME, not a fold — a different verb for a different act"}
    if dupe_row["status"] == "merged":
        return {"error": f"{dupe_row['canonical']} is already folded — nothing to do"}
    if into_row["status"] == "merged":
        return {"error": f"{into_row['canonical']} is itself folded — fold into the "
                         "living project instead"}
    conflicts = await _contradicting_properties(actions.pool, dupe_row["id"], into_row["id"])
    if conflicts:
        return {"error": f"{dupe_row['canonical']} and {into_row['canonical']} carry "
                         f"contradicting values on: {', '.join(conflicts)} — this may be "
                         "two different projects, not one under two names; fold_project "
                         "refuses rather than destroy the disagreement",
                "contradicted_on": conflicts}
    now = datetime.now(UTC)
    moved, mounts_moved = await _move_project_estate(
        actions, dupe_row["id"], into_row["id"], dupe_row["canonical"],
        into_row["canonical"], actor, now)
    await actions.merge_objects(into_row["id"], dupe_row["id"], justification=evidence,
                                actor=actor)
    return {"folded": dupe_row["canonical"], "into": into_row["canonical"],
           "edges_moved": moved, "mounts_moved": mounts_moved}


async def unfold_project(
    actions: Actions, *, dupe: str, because: str, actor: str, execute: bool = False,
) -> dict[str, Any]:
    """Reverse a wrongful fold_project — the Project sibling of `folds.unfold_agent`, built
    for PARITY (ruling 31c02dca: fold_project shipped with NO reversal at all, so a fold
    here was permanent and unrepairable — task #127's own named case). DRY RUN IS THE
    DEFAULT (`execute=False`): returns the plan without writing.

    Refuses LOUDLY on: blank `because`; `dupe` not resolving to a SoftwareProject (the SAME
    flexible resolution `fold_project` itself uses — UUID, short id, canonical, or bare
    name — an already-merged object is reachable only by its exact canonical, exactly as
    `_resolve_software_project` already documents); `dupe.status != 'merged'`; the original
    fold's own justification citing the operator's word when `because` doesn't carry a
    fresh one.

    ESTATE: every `_PROJECT_ESTATE_LINK_TYPES` edge `fold_project` moved is event-sourced
    and restored automatically WHEN AND ONLY WHEN nothing has re-pointed it since
    (`folds._reversible_moved_links` — the same 'unchanged since the fold' proof
    `unfold_agent`'s own thread reversal uses, generalized to links). `agent_mounts.project`
    was moved by a raw UPDATE — reported as `estate_unreturnable`, never guessed back,
    exactly like `fold_agent`'s own mount rows."""
    from src.orchestrator.folds import _reversible_moved_links

    dupe, because = (dupe or "").strip(), (because or "").strip()
    if not because:
        return {"error": "an unfold without a because is an un-audited reversal — cite "
                         "the evidence/ruling that proves the fold was wrong"}
    if not dupe:
        return {"error": "unfold_project needs a dupe label"}
    try:
        row = await _resolve_software_project(actions.pool, dupe)
    except AmbiguousProjectRef as amb:
        return {"error": f"{amb.ref!r} is ambiguous — {len(amb.candidates)} active "
                         f"SoftwareProjects answer to it: {', '.join(amb.candidates)}. "
                         "Name the exact one (canonical or id) — unfold_project never "
                         "guesses which."}
    if row is None:
        return {"error": f"no such SoftwareProject: {dupe!r} — an unfold never invents "
                         "a label"}
    if row["status"] != "merged":
        return {"error": f"{row['canonical']} is not folded (status={row['status']}) — "
                         "nothing to unfold"}
    into_id = await actions.pool.fetchval(
        "SELECT merged_into FROM objects WHERE id=$1", row["id"])
    into_canon = await actions.pool.fetchval(
        "SELECT canonical FROM objects WHERE id=$1", into_id)
    ev = await actions.pool.fetchrow(
        "SELECT payload, actor, created_at FROM object_events "
        "WHERE event_type='merge' AND related_id=$1 ORDER BY created_at DESC LIMIT 1",
        row["id"])
    original_evidence = str((ev["payload"] or {}).get("justification", "")) if ev else ""
    if "operator" in original_evidence.lower() and "operator" not in because.lower():
        return {"error": f"{row['canonical']}'s fold was justified by citing the "
                         f"operator's word ({original_evidence!r}) — an unfold needs the "
                         "operator's word too; add it to `because` or get it first"}

    moved: dict[str, list[dict[str, Any]]] = {}
    for link_type in _PROJECT_ESTATE_LINK_TYPES:
        found = await _reversible_moved_links(
            actions.pool, dupe_id=row["id"], into_id=into_id, link_type=link_type,
            from_dupe=True)
        if found:
            moved[link_type] = found
    unreturnable_mounts = [dict(r) for r in await actions.pool.fetch(
        "SELECT job_dir, cwd, mounted_at FROM agent_mounts WHERE project=$1 "
        "AND mounted_at <= $2 ORDER BY mounted_at",
        str(into_canon).removeprefix("repo:"),
        ev["created_at"] if ev else datetime.now(UTC))]

    plan: list[dict[str, Any]] = [
        {"op": "unmerge_objects", "target": row["canonical"], "detail":
         f"status merged→active, merged_into cleared (was {into_canon})"}]
    for link_type, items in moved.items():
        for it in items:
            plan.append({"op": "move_link", "target": it["label"], "detail":
                        f"{link_type} {into_canon} → {row['canonical']}"})

    report: dict[str, Any] = {
        "dupe": row["canonical"], "was_merged_into": into_canon,
        "fold_actor": ev["actor"] if ev else None, "fold_justification": original_evidence,
        "plan": plan,
        "estate_unreturnable": {
            "mounts": unreturnable_mounts,
            "note": ("pre-fold UPDATEs overwrote agent_mounts.project in place — these "
                     "predate the fold and still sit on the living project, but nothing "
                     "proves they were ever mounted against dupe rather than already "
                     "into's own; read them and judge by hand, never auto-moved")
                    if unreturnable_mounts else
                    "none found — no pre-fold mount rows sit unclaimed on the living "
                    "project",
        },
        "execute": execute,
    }
    if not execute:
        return report

    now = datetime.now(UTC)
    await actions.unmerge_objects(row["id"], because, actor)
    edges_restored = 0
    for link_type, items in moved.items():
        for it in items:
            await actions.invalidate_link(it["fid"], into_id, link_type, actor, now)
            await actions.create_link(it["fid"], row["id"], link_type, actor, now, _CONF,
                                      evidence_class=_EC)
            edges_restored += 1
    report.update({
        "unmerged": True, "edges_restored": edges_restored,
        "note": (f"{row['canonical']} is active again — provenance for the folded era "
                 "stays on the record (the merge event and same_as link are witnesses, "
                 "never erased). "
                 + (f"{edges_restored} edge(s) restored. " if edges_restored else "")
                 + ("Unreturnable mount rows are listed above for a human to judge by "
                    "hand." if unreturnable_mounts else "")),
    })
    return report


async def _move_project_estate(
    actions: Actions, dupe_oid: uuid.UUID, into_oid: uuid.UUID, dupe_canonical: str,
    into_canonical: str, actor: str, now: datetime,
) -> tuple[dict[str, int], int]:
    """The estate-move itself, factored out of fold_project so reconcile_project_fold
    (#127) can run the EXACT same repair on an already-merged pair rather than a second
    implementation that could drift from what a normal fold already does. Every live
    _PROJECT_ESTATE_LINK_TYPES edge on `dupe_oid` re-points to `into_oid` (idempotent —
    a link already live to `into_oid` is never duplicated); `agent_mounts.project` is
    re-addressed the same way. Never calls merge_objects — that stays each caller's own
    decision about whether a merge should happen at all."""
    moved: dict[str, int] = {}
    for link_type in _PROJECT_ESTATE_LINK_TYPES:
        rows = await actions.pool.fetch(
            "SELECT from_id AS oid FROM links WHERE to_id=$1 AND type=$2 "
            "AND (valid_until IS NULL OR valid_until > now())", dupe_oid, link_type)
        n = 0
        for r in rows:
            exists = await actions.pool.fetchval(
                "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type=$3 "
                "AND (valid_until IS NULL OR valid_until > now())",
                r["oid"], into_oid, link_type)
            await actions.invalidate_link(r["oid"], dupe_oid, link_type, actor, now)
            if not exists:
                await actions.create_link(r["oid"], into_oid, link_type, actor, now, _CONF,
                                          evidence_class=_EC)
            n += 1
        if n:
            moved[link_type] = n
    bare_dupe = dupe_canonical.removeprefix("repo:")
    bare_into = into_canonical.removeprefix("repo:")
    mount_tag = await actions.pool.execute(
        "UPDATE agent_mounts SET project=$1 WHERE project=$2", bare_into, bare_dupe)
    return moved, int(mount_tag.rsplit(" ", 1)[-1])


# --- reconcile_project_fold (#127, P0 — the repair path fold_project never had) ----------

async def reconcile_project_fold(
    actions: Actions, *, dupe: str, into: str, actor: str,
) -> dict[str, Any]:
    """THE REPAIR PATH fold_project never had (Thoth's own framing, DM 2487): folds are
    idempotent-by-REFUSAL when they need to be idempotent-by-REPAIR — the refusal that
    makes a SECOND fold safe (`dupe.status=='merged'` -> "already folded, nothing to
    do") is exactly what makes a PARTIAL first fold permanent, because nothing can ever
    revisit it. This is that revisit: re-points any live _PROJECT_ESTATE_LINK_TYPES
    edge still aimed at an ALREADY-merged dupe, using the SAME `_move_project_estate`
    fold_project itself calls — not a second implementation that could drift from what
    a normal fold already does.

    THE INVERSE PRECONDITION of fold_project, on purpose, so the two verbs' refusal
    conditions never overlap and a caller can never reach the merge event through this
    door: fold_project REQUIRES status=='active' on both sides and refuses a merged
    dupe; reconcile REQUIRES dupe.status=='merged' AND dupe's own `merged_into`
    pointing at exactly `into` (refuses to redirect a dupe merged into some OTHER
    object — never guesses which pair a caller means).

    NEVER re-performs the merge: no `merge_objects` call, no `_contradicting_properties`
    gate (that gate decides whether a merge SHOULD happen; this object already IS
    merged, so the question this asks is only "was the estate-move complete").

    NEGATIVE CONTROL BY CONSTRUCTION: a dupe whose estate was already fully re-pointed
    (a clean prior fold, or a reconcile that already ran) has nothing live left to find
    — reports every count as zero and writes nothing. Safe to run on a healthy fold;
    safe to run twice.

    Refuses LOUDLY on: blank dupe/into; dupe==into; dupe not resolving to a
    SoftwareProject (ambiguity refused the same way as every other project verb);
    dupe.status != 'merged' (fold_project's job, not this one's); dupe's own
    `merged_into` not equal to `into`'s id; into not resolving, ambiguous, or not
    ACTIVE."""
    dupe, into = (dupe or "").strip(), (into or "").strip()
    if not dupe or not into:
        return {"error": "reconcile_project_fold needs both labels: dupe and into"}
    if dupe == into:
        return {"error": "dupe and into name the same project — nothing to reconcile"}
    try:
        dupe_row = await _resolve_software_project(actions.pool, dupe)
    except AmbiguousProjectRef as amb:
        return {"error": f"{amb.ref!r} is ambiguous — {len(amb.candidates)} active "
                         f"SoftwareProjects answer to it: {', '.join(amb.candidates)}. "
                         "Name the exact one (canonical or id) — reconcile_project_fold "
                         "never guesses which."}
    if dupe_row is None:
        return {"error": f"no such SoftwareProject: {dupe!r}"}
    if dupe_row["status"] != "merged":
        return {"error": f"{dupe_row['canonical']} is {dupe_row['status']}, not merged — "
                         "reconcile_project_fold only repairs an ALREADY-completed fold; "
                         "use fold_project to merge it in the first place"}
    into_row, err = await _resolve_project_ref(actions.pool, into,
                                               verb="reconcile_project_fold")
    if err:
        return err
    if into_row is None:
        return {"error": f"no such SoftwareProject: {into!r}"}
    if into_row["status"] != "active":
        return {"error": f"{into_row['canonical']} is {into_row['status']}, not active"}
    actual_target = await actions.pool.fetchval(
        "SELECT merged_into FROM objects WHERE id=$1", dupe_row["id"])
    if actual_target != into_row["id"]:
        target_canon = (await actions.pool.fetchval(
            "SELECT canonical FROM objects WHERE id=$1", actual_target)
            if actual_target else None)
        return {"error": f"{dupe_row['canonical']} is merged into "
                         f"{target_canon or '(unknown)'}, not {into_row['canonical']} — "
                         "reconcile_project_fold never redirects a merge; name the "
                         "ACTUAL survivor"}
    now = datetime.now(UTC)
    moved, mounts_moved = await _move_project_estate(
        actions, dupe_row["id"], into_row["id"], dupe_row["canonical"],
        into_row["canonical"], actor, now)
    return {"reconciled": dupe_row["canonical"], "into": into_row["canonical"],
           "edges_moved": moved, "mounts_moved": mounts_moved}


# --- correct_project_name (#110, decision 1db1ff41 — the delegated exception) ------------

async def find_case_variant_projects(pool: asyncpg.Pool) -> dict[str, Any]:
    """SURVEY, never writes: every active SoftwareProject carrying more than one
    distinct `name` value, classified by the EXACT same proof correct_project_name
    itself runs before writing — a caller of this function never gets a different
    verdict from the settle step than this survey promised. `proven` entries reduce to
    one `strip().casefold()` form (bytebye's OWN 'ByeByte' would NOT qualify — it is a
    genuine transposition, not a case variant, and belongs in `genuine` beside every
    other real rename). Built for the operator's own widened rule (2026-07-31: "the
    capitalization merging should be automatic not bottlenecked by me") — this is the
    "find every pair first" half; correct_project_name itself is the "settle it" half,
    called separately, per project, only after a caller has seen this report."""
    projects = await pool.fetch(
        "SELECT id, canonical FROM objects WHERE type='SoftwareProject' AND status='active'")
    proven: list[dict[str, Any]] = []
    genuine: list[dict[str, Any]] = []
    for p in projects:
        rows = await pool.fetch(
            "SELECT value #>> '{}' AS v FROM current_assertions "
            "WHERE object_id=$1 AND name='name'", p["id"])
        distinct_raw = sorted({r["v"] for r in rows if r["v"]})
        if len(distinct_raw) <= 1:
            continue
        entry = {"project": p["canonical"], "distinct_names": distinct_raw}
        normalized = {v.strip().casefold() for v in distinct_raw}
        (proven if len(normalized) == 1 else genuine).append(entry)
    return {"scanned": len(projects), "proven_case_variant": proven,
           "genuine_contradiction": genuine}


async def correct_project_name(
    actions: Actions, *, project: str, actor: str, because: str | None = None,
) -> dict[str, Any]:
    """THE DELEGATED EXCEPTION (operator ruling 1db1ff41, verbatim: case/whitespace
    normalization "where identity is provably unchanged... is a CORRECTION, not a
    rename, and is agent-safe"). Mirrors correct_house/correct_agent_house's own naming
    and self-authorizing shape: no operator citation required — BECAUSE this function
    re-proves the identity-unchanged claim itself before writing, rather than trusting
    the caller's word for it. `because` is accepted for the record but never required;
    the proof below is what authorizes the write, not a stated reason.

    PROOF, not assumption: every current `name` value this project's own assertion
    history carries (not just the winning one — bytebye's own case has 20, 12 lowercase
    and 8 mixed-case, all at the SAME confidence, so "current" has been flip-flopping
    for two weeks depending only on which was asserted last) must reduce to exactly ONE
    normalized form (`strip().casefold()`). A single genuinely different name anywhere
    in that set — redmonth vs ballgem, not bytebye vs ByeByte — REFUSES loudly and names
    rename_project as the right tool instead; that refusal IS the negative control that
    keeps this delegation safe (ruling 1db1ff41's own bar: "prove a genuine rename
    refuses without an explicit declaration").

    SETTLES TO THE MAJORITY, not the most recent: most-recent-wins is the exact
    mechanism that let bytebye's name genuinely oscillate for two weeks — majority vote
    among the raw historical assertions picks whichever casing this object's own history
    actually favors, using that casing's own most-recent instance to break nothing extra.
    A TIE refuses rather than guess; the caller settles it by hand
    (assert_project_property) with a stated reason on the record.

    PRIOR-ART SURFACED, NEVER REFUSED (obligation e4612853's sibling, ruling 38c71544's
    family — the bytebye/byebyte incident: this verb's own majority-vote CAN legitimately
    re-settle onto a casing a standing operator ruling specifically rejected, since
    "provably identity-unchanged" says nothing about WHICH equivalent form was ordered).
    The receipt's own `prior_art`/`prior_art_flag` keys, when present, name a standing
    Decision that may already cover this exact name — the same search()-based guard
    record_decision runs on itself, generalized here. Cannot distinguish a deliberate
    correction from an uninformed one; only ensures the write does not land silently
    unread."""
    row, err = await _resolve_project_ref(actions.pool, project, verb="correct_project_name")
    if err:
        return err
    if row is None:
        return {"error": f"no such SoftwareProject: {project!r}"}
    if row["status"] != "active":
        return {"error": f"{row['canonical']} is {row['status']}, not active — nothing to "
                         "correct"}
    values = await actions.pool.fetch(
        "SELECT value #>> '{}' AS v, observed_at FROM current_assertions "
        "WHERE object_id=$1 AND name='name' ORDER BY observed_at", row["id"])
    distinct_raw = sorted({v["v"] for v in values if v["v"]})
    if len(distinct_raw) <= 1:
        return {"project": row["canonical"], "corrected": False,
                "note": "already a single value — nothing to correct"}
    normalized = {v.strip().casefold() for v in distinct_raw}
    if len(normalized) > 1:
        return {"error": f"{row['canonical']} carries genuinely different names, not just "
                         f"case/whitespace drift: {', '.join(distinct_raw)} — this is a "
                         "rename, not a correction; correct_project_name refuses rather "
                         "than guess. Use rename_project with an explicit declaration "
                         "instead.",
                "distinct_names": distinct_raw}
    counts: dict[str, int] = {}
    latest: dict[str, Any] = {}
    for v in values:
        val = v["v"]
        if not val:
            continue
        counts[val] = counts.get(val, 0) + 1
        latest[val] = v["observed_at"]
    ranked = sorted(counts.items(), key=lambda kv: (-kv[1], -latest[kv[0]].timestamp()))
    if len(ranked) > 1 and ranked[0][1] == ranked[1][1]:
        return {"error": f"{row['canonical']}'s name assertions are tied "
                         f"{ranked[0][1]}-{ranked[1][1]} between {ranked[0][0]!r} and "
                         f"{ranked[1][0]!r} — correct_project_name refuses to break a tie "
                         "by guessing; settle it by hand (assert_project_property) with a "
                         "stated reason.",
                "vote": dict(counts)}
    settled = ranked[0][0]
    now = datetime.now(UTC)
    await actions.assert_property(row["id"], "name", settled, actor, now, _CONF,
                                  evidence_class=_EC)
    from src.orchestrator.capture import property_prior_art

    prior_art_bits = await property_prior_art(
        actions.pool, subject_canonical=row["canonical"], field="name",
        new_value=settled, because=because or "", actor=actor)
    return {"project": row["canonical"], "corrected": True, "settled_to": settled,
           "was": distinct_raw, "vote": dict(counts),
           "because": because or "case/whitespace-only correction, self-proved",
           **prior_art_bits}


# --- normalize_project_casing (operator ruling d02f2cdd, thread 3ed5b3d2) ----------------

def _peek_pin_value(path: str, key: str) -> dict[str, Any]:
    """Read-only preflight for ONE seat's `.osiris` pin — same TOML parsing
    `correct_pin_value` itself uses, never writes, never advances any state. Returns
    `{"ok": True, "value": <current>}` when the file is valid TOML and carries `key`,
    or `{"ok": False, "error": ...}` naming EXACTLY what's wrong (missing file, invalid
    TOML, missing key) — `normalize_project_casing`'s own precondition check calls this
    for every named seat BEFORE writing anything anywhere, so a bad path is refused
    up front rather than discovered mid-write with the graph side already moved."""
    import tomllib

    p = Path(path) / ".osiris"
    if not p.is_file():
        return {"ok": False, "error": f"{p} does not exist"}
    try:
        existing = tomllib.loads(p.read_text())
    except (tomllib.TOMLDecodeError, ValueError) as exc:
        return {"ok": False, "error": f"{p} is not valid TOML ({type(exc).__name__}: {exc})"}
    if key not in existing:
        return {"ok": False, "error": f"{key!r} is not declared in {p}"}
    return {"ok": True, "value": existing[key]}


async def normalize_project_casing(
    actions: Actions, *, populated: str, phantom: str, correct_case: str, evidence: str,
    actor: str, seat_pin_paths: tuple[str, ...] = (), pin_key: str = "project",
) -> dict[str, Any]:
    """THE COMPOSITION operator ruling d02f2cdd asked for (thread 3ed5b3d2) — the
    TWIN-COLLAPSE shape specifically: two SoftwareProject objects already exist under
    case-variant canonicals (RAMstein/ramstein, bytebye/byebyte), one populated, one an
    empty phantom (agent_count 0). NOT A SIXTH DOOR: every real write here is
    `fold_project` (already moves the exact edge set Alfred enumerated —
    `_PROJECT_ESTATE_LINK_TYPES` = in_repo/works_in/governs/informs, read from its own
    code, not assumed from its docstring), `rename_project` (the display-name fix, same
    object, same id — never re-derived here either), and `correct_pin_value` (the one
    piece no graph verb reaches, by design — a seat's `.osiris` pin is a local file, not
    a graph object). This function is the hallway between three existing rooms, not a
    fourth room.

    DIRECTION MATTERS, AND GETTING IT WRONG SILENTLY DESTROYS DATA — the finding that
    changed this function's own first draft: `Actions.merge_objects` NEVER copies
    property assertions from the retired object onto the survivor (its own docstring:
    "assertions are never rewritten... leave assertions in place"), and
    `current_assertions` is scoped strictly by `object_id`, never following
    `merged_into` — confirmed by reading both, not assumed. So folding the POPULATED
    object (real `on_disk_path`/`remote_url`/etc.) INTO the empty PHANTOM would silently
    discard every one of the populated object's own properties, keeping only what
    `_PROJECT_ESTATE_LINK_TYPES` moves (links, never properties). This function always
    folds the OTHER way — `phantom` is retired INTO `populated` (which has nothing to
    lose; it was empty), and ONLY THEN does `rename_project` fix `populated`'s own
    `name` to `correct_case` IN PLACE, same object, same id, every property untouched
    throughout. `populated`'s canonical never changes (rename_project's own guarantee)
    — only its display `name` does.

    ATOMIC OR REFUSED — THE INVERTED RULE (577988ed inverts here, per the operator's own
    standing line and Thoth's explicit instruction: a partial rename is strictly WORSE
    than none, because it destroys the one signal — agent_count 0 — that currently tells
    the populated project apart from its phantom twin). EVERY precondition for EVERY
    step is checked BEFORE any write happens anywhere: `fold_project`'s own guards
    (both active, neither already merged, no contradicting non-name property) via the
    SAME `_resolve_project_ref`/`_contradicting_properties` fold_project itself uses —
    never re-derived, so this can never drift from what a bare fold_project call would
    decide — AND every named seat pin (`_peek_pin_value`, read-only) must exist, parse
    as valid TOML, and already declare `pin_key`. A pin already correct (its OWN current
    value equals `correct_case`) is not a failure — nothing to write for that seat,
    named in the receipt's `pins_already_correct`. ANY OTHER precondition failure
    anywhere — the fold's own guards OR a single unreadable/missing/keyless pin —
    REFUSES THE WHOLE OPERATION before a single write, naming exactly what failed.

    CROSS-DOMAIN HONESTY: the graph writes (fold, then rename — two separate
    transactions, since each verb owns its own) and each pin file (a separate local
    filesystem write) cannot share one atomic commit — no verb in this codebase can
    promise that, and this one does not pretend to. What it DOES promise: every
    precondition is proven BEFORE the first write, so the only way a partial state can
    occur is a genuine race between the preflight check and the write itself —
    vanishingly unlikely for a one-seat-at-a-time correction, but reported LOUDLY rather
    than silently if it happens: the receipt's `rename_failed`/`pin_write_failed` keys
    name exactly what's left inconsistent, and `unfold_project` is named as the recovery
    path for the fold half, never auto-invoked (a rollback is its own deliberate act,
    same law ack_handoff/every other reversal in this codebase already holds).

    SEAT-PIN REACH IS LOCAL-BOX ONLY, named not hidden (the same limit `push_guard`'s
    own reach question surfaced two lanes ago): `seat_pin_paths` must be filesystem
    paths reachable from wherever this call runs. This function does not discover which
    seats need correcting — that is a separate, harder question (which seats exist,
    which box each one's `.osiris` actually lives on) deliberately left to the caller,
    matching correct_pin_value's own existing calling convention rather than inventing
    auto-discovery this pass.

    NAMED, NOT CLOSED, RESIDUAL (Sekhmet's own trace, thread 4710): `on_disk_path`/
    `remote_url` are real SoftwareProject properties (census_trees/Rule 2), never in
    Alfred's own five-thing enumeration, and this function's DIRECTION CHOICE is what
    keeps them safe (the populated survivor's own properties are simply never touched)
    — but that is a consequence of the direction, not a property this function proves.
    A caller who accidentally names the EMPTY object as `populated` would not be caught
    here; the property-preservation guarantee rests on the caller correctly identifying
    which side is actually populated, same trust boundary `fold_project` itself already
    carries for dupe/into.

    Refuses LOUDLY, nothing written, on: blank populated/phantom/correct_case/evidence;
    populated==phantom; either not resolving to an ACTIVE SoftwareProject; either
    already merged; a genuine cross-object contradiction on any non-name/tag property
    (fold_project's own guard, reused verbatim); or ANY named seat pin failing its
    preflight read."""
    from src.orchestrator.offices import correct_pin_value
    from src.orchestrator.project_identity import rename_project

    populated = (populated or "").strip()
    phantom = (phantom or "").strip()
    correct_case = (correct_case or "").strip()
    evidence = (evidence or "").strip()
    if not evidence:
        return {"error": "evidence is required — a fold without evidence is an "
                         "auto-merge wearing a signature"}
    if not populated or not phantom or not correct_case:
        return {"error": "normalize_project_casing needs populated, phantom, and "
                         "correct_case"}
    if populated == phantom:
        return {"error": "populated and phantom name the same project — nothing to "
                         "normalize"}

    populated_row, populated_err = await _resolve_project_ref(
        actions.pool, populated, verb="normalize_project_casing")
    if populated_err:
        return populated_err
    phantom_row, phantom_err = await _resolve_project_ref(
        actions.pool, phantom, verb="normalize_project_casing")
    if phantom_err:
        return phantom_err
    if populated_row is None or phantom_row is None:
        missing = [label for label, row in
                  ((populated, populated_row), (phantom, phantom_row)) if row is None]
        return {"error": f"unknown SoftwareProject(s): {', '.join(missing)} — "
                         "normalize_project_casing never invents either side; both twins "
                         "must already exist"}
    if phantom_row["status"] == "merged":
        return {"error": f"{phantom_row['canonical']} is already folded — nothing to do"}
    if populated_row["status"] != "active":
        return {"error": f"{populated_row['canonical']} is {populated_row['status']}, "
                         "not active"}
    if phantom_row["status"] != "active":
        return {"error": f"{phantom_row['canonical']} is {phantom_row['status']}, "
                         "not active"}
    conflicts = await _contradicting_properties(
        actions.pool, phantom_row["id"], populated_row["id"])
    if conflicts:
        return {"error": f"{phantom_row['canonical']} and {populated_row['canonical']} "
                         f"carry contradicting values on: {', '.join(conflicts)} — this "
                         "may be two different projects, not one under two names; "
                         "normalize_project_casing refuses rather than destroy the "
                         "disagreement, exactly as fold_project would",
                "contradicted_on": conflicts}

    pins_ok: list[str] = []
    pins_already_correct: list[str] = []
    pin_failures: list[dict[str, Any]] = []
    for pin_path in seat_pin_paths:
        peek = _peek_pin_value(pin_path, pin_key)
        if not peek["ok"]:
            pin_failures.append({"path": pin_path, "error": peek["error"]})
            continue
        if peek["value"] == correct_case:
            pins_already_correct.append(pin_path)
        else:
            pins_ok.append(pin_path)
    if pin_failures:
        return {"error": "one or more seat pins failed preflight — REFUSING THE WHOLE "
                         "OPERATION before any write, a partial normalization is worse "
                         "than none",
                "pin_failures": pin_failures}

    fold_result = await fold_project(
        actions, dupe=phantom_row["canonical"], into=populated_row["canonical"],
        evidence=evidence, actor=actor)
    if fold_result.get("error"):
        # every precondition above was proven immediately before this call — a refusal
        # here would mean the graph changed between the check and the write (a real
        # race, not a bug in the check), and NOTHING has been written anywhere yet, so
        # the operation is still cleanly all-nothing.
        return {"error": f"fold_project itself refused despite passing every "
                         f"precondition above (a concurrent write, most likely): "
                         f"{fold_result['error']}"}

    rename_result = await rename_project(
        actions, project=populated_row["canonical"], new_name=correct_case,
        because=evidence, actor=actor)
    if rename_result.get("error"):
        return {"error": rename_result["error"], "folded": fold_result["folded"],
               "into": fold_result["into"],
               "note": "THE FOLD SUCCEEDED BUT THE RENAME THAT WAS SUPPOSED TO FOLLOW IT "
                       "REFUSED — a genuine race (the graph changed between the "
                       "precondition check and this call), not a design gap. The twin "
                       "is already retired; only the display NAME is still wrong. "
                       f"Recover with rename_project directly once the cause is clear: "
                       f"{rename_result['error']}"}

    pin_writes: list[dict[str, Any]] = []
    pin_write_failed: list[dict[str, Any]] = []
    for pin_path in pins_ok:
        result = correct_pin_value(
            pin_path, pin_key, correct_case,
            reason=f"normalize_project_casing: {evidence}")
        if result.get("error"):
            pin_write_failed.append({"path": pin_path, "error": result["error"]})
        else:
            pin_writes.append({"path": pin_path, **result})

    out: dict[str, Any] = {
        "folded": fold_result["folded"], "into": fold_result["into"],
        "edges_moved": fold_result["edges_moved"], "mounts_moved": fold_result["mounts_moved"],
        "renamed": rename_result["project"], "old_name": rename_result["old_name"],
        "new_name": rename_result["new_name"],
        "pins_written": pin_writes, "pins_already_correct": pins_already_correct,
    }
    if pin_write_failed:
        out["pin_write_failed"] = pin_write_failed
        out["note"] = ("THE GRAPH SIDE (FOLD + RENAME) SUCCEEDED BUT AT LEAST ONE PIN "
                      "WRITE FAILED AFTER PASSING PREFLIGHT — a genuine race, not a "
                      "design gap; this is a PARTIAL STATE, reported loudly rather than "
                      "silently. Recovery: unfold_project reverses the fold half (a "
                      "deliberate, separate act, never auto-invoked here); the failed "
                      "pin(s) above still need hand correction either way.")
    return out
