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
    seshat/ra disambiguation). NOT self-scoped: any authorized caller may stamp any
    named project; `actor` is attribution, never a same-caller requirement (same
    reasoning as correct_agent_house).

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
    carries, does the union of both objects' current values disagree? Same
    `count(DISTINCT value) > 1` logic as both siblings above, scoped here to exactly
    two candidate objects rather than a type census or one object's own multi-source
    set. `name`/`tag` are excluded — a label difference is a fold's own PREMISE (two
    tags for one referent is exactly the operator's "SAME data, DIFFERENT tags ->
    merge is correct" case), never a sign these are two different things; every OTHER
    property disagreeing is the opposite case ("SAME tag, DIFFERENT data") the operator
    named as the one merge must never cross."""
    rows = await pool.fetch(
        "SELECT name FROM current_assertions "
        "WHERE object_id = ANY($1::uuid[]) AND name NOT IN ('name', 'tag') "
        "GROUP BY name HAVING count(DISTINCT (value #>> '{}')) > 1",
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
    dupe_oid, into_oid = dupe_row["id"], into_row["id"]
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
    bare_dupe = dupe_row["canonical"].removeprefix("repo:")
    bare_into = into_row["canonical"].removeprefix("repo:")
    mount_tag = await actions.pool.execute(
        "UPDATE agent_mounts SET project=$1 WHERE project=$2", bare_into, bare_dupe)
    mounts_moved = int(mount_tag.rsplit(" ", 1)[-1])
    await actions.merge_objects(into_oid, dupe_oid, justification=evidence, actor=actor)
    return {"folded": dupe_row["canonical"], "into": into_row["canonical"],
           "edges_moved": moved, "mounts_moved": mounts_moved}


# --- correct_project_name (#110, decision 1db1ff41 — the delegated exception) ------------

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
    (assert_project_property) with a stated reason on the record."""
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
    return {"project": row["canonical"], "corrected": True, "settled_to": settled,
           "was": distinct_raw, "vote": dict(counts),
           "because": because or "case/whitespace-only correction, self-proved"}
