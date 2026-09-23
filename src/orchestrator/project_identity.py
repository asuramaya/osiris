"""PROJECT IDENTITY: the read-only evidence resolver.

Before a rename or a fork can be declared, someone has to see what the evidence actually
says, tier by tier, without any one tier being crowned first for every case. A proposed
"the git remote is authoritative when it exists" rule correctly resolves several sample
repos, and fails completely on a repo with a real working checkout that has no origin
remote at all. No single evidence source ranks first across the population: remote is
authoritative for repos with a configured origin and blind for repos without one;
write-attribution is authoritative for the latter and blind for the former (a repo whose
work is entirely filed under a stale name can go undetected for weeks on remote evidence
alone). This module supplies the naming/marking machinery the situation demands, never a
fixed precedence pick, and reuses this house's five-tier evidence catalog rather than
inventing new ones:

  OPERATOR_CONFIRMED, a caller-SUPPLIED citation (a decision id / quote). This function
    never parses decision prose looking for a quote; that is a human's read, not a query,
    and pretending otherwise would be the same silent-pick failure this module exists to
    refuse. Reported back verbatim, `checked=True`, so a caller who already did that
    reading gets it folded into one report rather than a second lookup.

  DECLARED_CHARTER, a seat's own governs edges, checked from BOTH origins: the Seat
    object directly (the re-key this whole primitive depends on) AND every Agent lineage
    that has ever held the seat (the schema and code for the Seat-typed edge shipped, but
    the migration from Agent-typed governs edges is dry-run-only as of this build; every
    live governs edge today is still Agent-typed, and the day it flips this tier keeps
    working unchanged, because it already checks both).

  SELF_AUTHORED, EXISTENCE only, never content: a seat's own CLAUDE.md/charter.md is
    reported by path/size/mtime. Reading its prose for a project claim is the same
    human-judgment problem as tier 1, not a query this function pretends to answer.

  PIN, the seat's own `.osiris` project= label, read at its office (anchor_cwd).

  REMOTE, `git remote get-url origin`, run against each CANDIDATE project's registered
    on_disk_path (census_trees's own stored fact, never a guessed root, the same
    discipline discover_trees already holds). A seat's own anchor_cwd is its office, not
    its code repo (mintseat.py/greatfold.py: anchor_cwd is always the office path), so
    remote is checked per-candidate against the graph's on_disk_path, not against the
    seat directly.

  WRITE_ATTRIBUTION (DERIVED, weakest, flagged explicitly wherever it's the only signal),
    the majority in_repo target across every Thread/Decision this seat's lineage has ever
    filed, the tier of last resort when nothing else has signal.

`project_identity_evidence` never writes; `rename_project` and `fork_project` below are
the two declared-succession actions a caller invokes once a human has read that report and
made the call it can't make for them (correct_project_name, the third writer and the one
delegated exception that IS self-authorizing, lives in projects.py beside retire_project/
fold_project, the sibling lifecycle actions it belongs with).

RENAME vs FORK are different acts: rename keeps ONE object's stable `canonical` id and
only ever changes its mutable `name` property (old value stays in assertion history,
never deleted, the same discipline rename_seat already holds for a seat's handle); fork
connects TWO objects that already exist with a `forked_from` edge and moves no state at
all. THE CALLER DECLARES WHICH BY CALLING THE RIGHT FUNCTION, there is no shared `kind=`
parameter to get wrong, because the codebase's own idiom (retire_project/fold_project/
peer_seats/rename_seat/correct_house) is one action per function, and that shape was
chosen over a mode switch for exactly this reason: a declaration hidden behind an argument
default is not a declaration.
"""
from __future__ import annotations

import logging
import subprocess
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

logger = logging.getLogger("osiris.project_identity")

_EC = EvidenceClass.SELF_DECLARED.value
_CONF = confidence_for(EvidenceClass.SELF_DECLARED)

# A DECLARED RENAME MUST OUTRANK AN ORDINARY MOUNT'S OWN GUESS (live specimen: a project
# renamed, then an ordinary later mount re-asserted the old name at the SAME
# self_declared/0.9 confidence rename_project itself used. current_assertions' tie-break
# (confidence DESC, observed_at DESC) then falls through to pure recency, so a later,
# uninformed mount silently overturned a deliberate, testimony-backed rename). Written at
# the SAME evidence_class (still a self-declaration, never a different kind of claim) but
# a confidence strictly above the 0.9 ceiling every ordinary self_declared write is capped
# at (BASE_CONFIDENCE[SELF_DECLARED]), the same higher-confidence pattern already used
# elsewhere in this graph for a declared act that must never lose a tie to routine traffic.
_RENAME_CONF = 0.95


def _remote_basename(url: str | None) -> str | None:
    """The repo name a remote URL implies, for comparison against a bare project label:
    'git@github.com:x/example.git' and 'https://github.com/x/example' both read as
    'example'. None in, None out."""
    if not url:
        return None
    tail = url.rstrip("/").rsplit("/", 1)[-1]
    if ":" in tail and "/" not in url.rstrip("/").rsplit("/", 1)[-1]:
        tail = tail.rsplit(":", 1)[-1]
    return tail.removesuffix(".git") or None


def _git_remote(path: str) -> tuple[bool, str | None]:
    """(is_a_git_repo, origin_url_or_None) at `path`, same subprocess.run/try-except
    shape as pulse.py's own _git_head/repo_name, never raising on a missing/broken repo."""
    try:
        top = subprocess.run(["git", "-C", path, "rev-parse", "--show-toplevel"],
                             capture_output=True, text=True, timeout=5, check=True)
    except (OSError, subprocess.SubprocessError, subprocess.CalledProcessError):
        return False, None
    if not top.stdout.strip():
        return False, None
    try:
        url = subprocess.run(["git", "-C", path, "remote", "get-url", "origin"],
                             capture_output=True, text=True, timeout=5, check=True)
        return True, (url.stdout.strip() or None)
    except (OSError, subprocess.SubprocessError, subprocess.CalledProcessError):
        return True, None  # a real repo, just no `origin` configured


def worktree_parent_path(path: str) -> str | None:
    """WORKTREES AS A FIRST-CLASS SHAPE: `path`'s own MAIN checkout, or None when `path`
    is not a git worktree at all (including "not a git repo" and "IS the main checkout").
    `--git-common-dir` is always the shared `.git` (worktree or not); `--git-dir` is the
    PRIVATE one a worktree gets (`<main>/.git/worktrees/<name>`), and they agree exactly
    when `path` is the main checkout itself, so comparing them (never hand-parsing the
    `.git` FILE's own `gitdir: ...` pointer text, which can be relative or absolute and
    is git's own implementation detail, not a contract) is the one signal that is both
    necessary and sufficient. The parent's root is `--git-common-dir` with its own
    trailing `/.git` stripped, never re-derived from `--show-toplevel` run FROM `path`,
    which answers "top of THIS worktree", not "top of the main checkout"."""
    try:
        common = subprocess.run(["git", "-C", path, "rev-parse", "--git-common-dir"],
                                capture_output=True, text=True, timeout=5, check=True)
        own = subprocess.run(["git", "-C", path, "rev-parse", "--git-dir"],
                             capture_output=True, text=True, timeout=5, check=True)
    except (OSError, subprocess.SubprocessError, subprocess.CalledProcessError):
        return None
    common_dir, own_dir = common.stdout.strip(), own.stdout.strip()
    if not common_dir or not own_dir:
        return None
    common_abs = str(Path(path) / common_dir) if not common_dir.startswith("/") else common_dir
    own_abs = str(Path(path) / own_dir) if not own_dir.startswith("/") else own_dir
    if str(Path(common_abs).resolve()) == str(Path(own_abs).resolve()):
        return None  # the main checkout, not a worktree
    common_resolved = Path(common_abs).resolve()
    if common_resolved.name != ".git":
        return None  # an unexpected shape (bare repo, submodule): refuse rather than guess
    return str(common_resolved.parent)


def git_current_branch(path: str) -> str | None:
    """The checked-out branch at `path`, or None on a detached HEAD or any git failure,
    same never-raise shape as `_git_remote`."""
    try:
        out = subprocess.run(["git", "-C", path, "rev-parse", "--abbrev-ref", "HEAD"],
                             capture_output=True, text=True, timeout=5, check=True)
    except (OSError, subprocess.SubprocessError, subprocess.CalledProcessError):
        return None
    branch = out.stdout.strip()
    return branch if branch and branch != "HEAD" else None


async def project_name_for_disk_path(pool: asyncpg.Pool, path: str) -> str | None:
    """The registered SoftwareProject `name` for a checkout at `path`, read off its own
    `on_disk_path` assertion, never re-derived from the directory basename (the same
    binding-line discipline `census_trees`'s rename fix already established: a renamed
    directory keeps its OLD name/canonical on purpose). None when nothing at this exact
    path is registered, this never mints, never guesses a nearby path, it is a plain
    lookup for `project_of`'s own worktree step: a worktree whose parent checkout the
    disk census hasn't reached yet correctly falls through to charter/lineage instead
    of a fabricated answer."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT (SELECT a2.value #>> '{}' FROM current_assertions a2 "
        " WHERE a2.object_id=o.id AND a2.name='name' "
        " ORDER BY a2.confidence DESC, a2.observed_at DESC LIMIT 1) "
        "FROM objects o JOIN current_assertions a ON a.object_id=o.id "
        "WHERE o.type='SoftwareProject' AND o.status='active' AND a.name='on_disk_path' "
        "AND a.value #>> '{}' = $1 LIMIT 1", path)


def _self_authored(office: str | None) -> dict[str, Any]:
    """Existence/path/size/mtime for a seat's own CLAUDE.md and charter.md at its office,
    never their content. What they SAY is a human's read (same reasoning as tier 1); what
    this can honestly report is that they exist and when they last changed."""
    out: dict[str, Any] = {}
    for fname in ("CLAUDE.md", "charter.md"):
        out[fname] = {"exists": False, "path": None, "size": None, "mtime": None}
        if not office:
            continue
        p = Path(office) / fname
        if p.is_file():
            st = p.stat()
            out[fname] = {"exists": True, "path": str(p), "size": st.st_size,
                          "mtime": st.st_mtime}
    return out


async def _seat_lineage_bases(pool: asyncpg.Pool, seat_oid: Any) -> list[str]:
    """Every Agent lineage BASE that has ever held this seat (any generation, healed or
    active, `holds` link history): write-attribution and the still-live Agent-origin
    governs edges both need this: a seat's holder churns across successions, but its
    authored history, and (until migrate_charter_to_seat actually runs) its charter, stay
    keyed on whichever generation was live when each was asserted."""
    from src.orchestrator.agents import _generation

    rows = await pool.fetch(
        "SELECT DISTINCT f.canonical FROM links l JOIN objects f ON f.id=l.from_id "
        "WHERE l.to_id=$1 AND l.type='holds'", seat_oid)
    return sorted({_generation(str(r["canonical"]))[0] for r in rows})


async def _live_label(
    pool: asyncpg.Pool | asyncpg.Connection, oid: Any, canonical: str,
) -> str:
    """A SoftwareProject's CURRENT display label, its live `name` property when one
    exists, falling back to the canonical's own bare form only for an object that never
    got a `name` asserted at all. Every candidate key in this module goes through this:
    rename_project changes ONLY the `name` property, never `canonical` (the same
    discipline that keeps every existing edge correct without re-pointing), so a reader
    keyed on canonical instead would report the OLD label forever after every future
    rename. Caught live: re-running this tool against two other tools right after
    renaming a project still reported the old name with the remote 'disagreeing', when
    the rename had already made them agree; the read-back that was supposed to CONFIRM
    the rename would have reported it as still broken.

    Type widened to accept a raw `asyncpg.Connection` too (additional callers that hold
    a bare connection rather than a pool), same `fetchval` surface either way, one
    implementation for both."""
    name = await pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='name' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", oid)
    return str(name) if name else str(canonical).removeprefix("repo:")


async def charter_display_label(pool: asyncpg.Pool | asyncpg.Connection, entry: str) -> str:
    """A charter entry (`charter_of`'s own canonical-only output) rendered for a HUMAN,
    never for a comparison: charter_of and set_charter are correct to operate in
    canonical space forever (that's WHY a rename can never need to touch a governs
    edge), but a human reads project names, not canonicals, and an office's own "You
    govern: `oldname`" line after a real rename to "newname" reads as stale even though
    the graph is exactly right. Resolved LIVE at render time (never cached, never
    written anywhere) so a rename shows through without any cascade touching the
    charter itself, "newname (repo:oldname)" when the two differ, or a bare "oldname"
    (no redundant parenthetical) when the project's live name still equals its own
    canonical. Degrades to the bare entry on any failure (a presentation refinement
    must never be the reason a charter line goes blind)."""
    bare = entry.removeprefix("repo:")
    try:
        from src.orchestrator.capture import _resolve_repo

        oid = await _resolve_repo(pool, bare)
        if oid is None:
            return bare
        canon = await pool.fetchval("SELECT canonical FROM objects WHERE id=$1", oid)
        label = await _live_label(pool, oid, str(canon))
    except Exception:  # noqa: BLE001, see note above
        return bare
    return f"{label} (repo:{bare})" if label != bare else bare


async def charter_display_labels(pool: asyncpg.Pool | asyncpg.Connection,
                                  entries: list[str]) -> list[str]:
    """`charter_display_label` over a whole charter list, same order as given."""
    return [await charter_display_label(pool, entry) for entry in entries]


async def _normalize_project_label_through_merge(
    conn_or_pool: asyncpg.Pool | asyncpg.Connection, label: str,
) -> tuple[str, str | None]:
    """Resolve a project label through `merged_into` to its current survivor's live
    label: a label that has since been FOLDED into another must compare as the SURVIVOR,
    the same live label `_write_attribution`'s own in_repo-edge lookup already reports
    for the write side, otherwise every fold produces a permanent false 'disagrees' for
    any caller comparing against it.

    MOVED HERE FROM agents.py, NOT REWRITTEN: this module already hosts `_live_label`,
    the label-resolution primitive every candidate in this file goes through, and by the
    time a third caller needed the same "resolve through the fold" logic
    (project_identity_evidence's own `pin`, this module), keeping a second private copy
    in agents.py would have been the disease one level up from the one being fixed:
    three call sites is the threshold past which "just copy it" stops being cheaper than
    a shared home.

    WIDENED FOR A FOURTH AND FIFTH CALLER (settle.py's filed_under_check, agents.py's
    misfiled_by_lineage), NOT A SIXTH COPY: the chain-walk itself is now inlined here
    rather than delegated to `Actions.resolve_object_id`, because filed_under_check's
    own `conn_or_pool` accepts EITHER an `asyncpg.Pool` (the /settle MCP tool) OR a raw
    `asyncpg.Connection` (the Stop hook, which cannot hold a pool across its ~1s budget,
    module docstring). `Actions` requires a real Pool to `.acquire()` from, so wrapping a
    bare Connection in one would break the hook's own caller. Both types expose the same
    `fetch`/`fetchval` surface this walk needs, so one implementation genuinely serves
    every caller. Behavior is otherwise verbatim: same 100-iteration cycle guard, same
    confess-don't-guess refusal.

    Never guesses: the label names its own SoftwareProject by EXACT canonical only
    (case-insensitive, matching `_resolve_or_mint_project`'s own lookup); no string
    similarity, no directory-name fallback (a prior phantom-repo defect this guards
    against). A label that names no object returns itself unchanged, second element
    None. A chain too deep to resolve (a cycle) is NEVER silently picked through: the
    original label comes back untouched alongside a confession string for the caller to
    surface, never a guessed winner (this house names disagreement, it never crowns a
    side).

    A label naming an object that was never MERGED (winner == the object itself) still
    goes through `_live_label` before returning: `rename_project` changes only the
    `name` property, never `canonical`. The OLD label the caller passed in still
    resolves here (canonical is forever), but comparing that stale label against the
    object's live remote/pin/charter forever reads as a false disagreement after a plain
    rename with no fold involved at all, the exact rename specimen `_live_label`'s own
    docstring already names for the merged case. Only a genuinely stale label is
    translated; a label that already matches the object's own live name passes through
    unchanged either way.

    EXACT MATCH FIRST, CASE-INSENSITIVE ONLY AS A REFUSAL-GATED FALLBACK (a real bug
    caught by a deployed suite going intermittently red): a case-insensitive-only lookup
    here is AMBIGUOUS whenever two case-variant objects coexist for the same label.
    With no `ORDER BY`, `.fetchrow()` on such a query nondeterministically returns
    EITHER object, sometimes the duplicate (chain-walks correctly), sometimes the
    survivor itself (whose own merged_into is None, so the walk trivially returns
    unchanged, silently no-op'ing the very normalization this function exists to do).
    Exact canonical match is deterministic and resolves this specimen outright; the
    case-insensitive fallback only fires when nothing matches exactly, and only trusts it
    when it resolves to exactly one candidate: an ambiguous fallback confesses rather
    than picks, the same doctrine `_resolve_software_project`'s own AmbiguousProjectRef
    already holds."""
    target_canon = f"repo:{label}"
    row = await conn_or_pool.fetchrow(
        "SELECT id, canonical FROM objects WHERE type='SoftwareProject' "
        "AND canonical=$1", target_canon)
    if row is None:
        candidates = await conn_or_pool.fetch(
            "SELECT id, canonical FROM objects WHERE type='SoftwareProject' "
            "AND lower(canonical)=lower($1)", target_canon)
        if len(candidates) > 1:
            return label, (f"'{label}' matches {len(candidates)} SoftwareProjects that "
                           "differ only by case, so it was compared unnormalized rather "
                           "than guessing which one this label means")
        row = candidates[0] if candidates else None
    if row is None:
        # A RETIRED CANONICAL (object_aliases: a rename migrated the canonical): a stale
        # pin/charter spelling names the SAME object, so it resolves to that object's
        # live label instead of comparing as an unknown label.
        row = await conn_or_pool.fetchrow(
            "SELECT o.id, o.canonical FROM object_aliases al JOIN objects o ON o.id=al.object_id "
            "WHERE al.type='SoftwareProject' AND al.alias=$1 AND o.canonical <> al.alias",
            target_canon)
    if row is None:
        return label, None
    current = row["id"]
    for _ in range(100):
        nxt = await conn_or_pool.fetchval("SELECT merged_into FROM objects WHERE id=$1",
                                          current)
        if nxt is None:
            winner = current
            break
        current = nxt
    else:
        return label, (f"merge chain for {label!r} could not be resolved (broken/cyclic "
                       "merged_into edge), so it was compared unnormalized rather than "
                       "guessing a winner")
    if winner == row["id"]:
        return await _live_label(conn_or_pool, winner, row["canonical"]), None
    canon = await conn_or_pool.fetchval("SELECT canonical FROM objects WHERE id=$1", winner)
    return await _live_label(conn_or_pool, winner, canon), None


async def resolve_merge_survivors(
    conn_or_pool: asyncpg.Pool | asyncpg.Connection, ids: set[uuid.UUID],
) -> dict[uuid.UUID, uuid.UUID]:
    """MEMBERSHIP FOLLOWS A MERGE: batch id->id resolution through the `merged_into`
    chain, for a caller that already has real object ids in hand (graph_stream's own
    project_canonical join, graph_physics' `_project_membership`) rather than a bare
    project LABEL. `_normalize_project_label_through_merge`'s own shape, one round trip
    per label, is the wrong tool at graph-snapshot scale (thousands of member objects
    resolving down to a small, repeated set of project ids); this does the identical
    walk (same terminal condition: `merged_into IS NULL`, same cycle refusal) but ONE
    recursive query for the WHOLE distinct set at once, keyed by id rather than by
    canonical string.

    Every id in `ids` that resolves cleanly maps to its living survivor id (itself,
    when never merged). An id caught in a cycle (the walk revisits an id already seen
    on ITS OWN chain) is DROPPED from the result rather than guessed at, same refusal
    doctrine as the label-based walk's own confession string, just without a string to
    carry it (a caller reads a missing key as "unresolved, fall back to the id as
    given," never as "resolves to nothing"). Empty `ids` is a no-op, no query issued."""
    if not ids:
        return {}
    rows = await conn_or_pool.fetch(
        "WITH RECURSIVE chain(start_id, current_id, merged_into, depth) AS ( "
        "  SELECT o.id, o.id, o.merged_into, 0 FROM objects o WHERE o.id = ANY($1::uuid[]) "
        "  UNION ALL "
        "  SELECT c.start_id, o.id, o.merged_into, c.depth + 1 "
        "  FROM objects o JOIN chain c ON o.id = c.merged_into WHERE c.depth < 100"
        ") "
        "SELECT start_id, current_id FROM chain WHERE merged_into IS NULL",
        list(ids))
    survivors: dict[uuid.UUID, uuid.UUID] = {}
    for r in rows:
        # a cycle never reaches merged_into IS NULL within the depth cap, so its
        # start_id simply never appears above -- dropped, never guessed.
        survivors.setdefault(r["start_id"], r["current_id"])
    return survivors


async def _declared_charter(pool: asyncpg.Pool, seat_id: str, seat_oid: Any,
                            bases: list[str]) -> list[str]:
    """governs targets, checked from BOTH origins (module docstring), live display
    labels (see `_live_label`), deduplicated."""
    rows = list(await pool.fetch(
        "SELECT DISTINCT ro.id, ro.canonical FROM links l JOIN objects ro ON ro.id=l.to_id "
        "WHERE l.from_id=$1 AND l.type='governs' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", seat_oid))
    if bases:
        rows += await pool.fetch(
            "SELECT DISTINCT ro.id, ro.canonical FROM links l "
            "JOIN objects fo ON fo.id=l.from_id AND fo.type='Agent' "
            "JOIN objects ro ON ro.id=l.to_id "
            "WHERE l.type='governs' AND (l.valid_until IS NULL OR l.valid_until > now()) "
            "AND EXISTS (SELECT 1 FROM unnest($1::text[]) b "
            "            WHERE fo.canonical=b OR fo.canonical LIKE b || '-%')", bases)
    labels = set()
    for r in rows:
        labels.add(await _live_label(pool, r["id"], r["canonical"]))
    return sorted(labels)


async def _write_attribution(pool: asyncpg.Pool, bases: list[str]) -> dict[str, Any]:
    """The majority in_repo target across every Thread/Decision/Commit this lineage's
    write-attribution names, DERIVED, the weakest tier, used as evidence (e.g. 145 of
    163 filings) when nothing else has signal at all. Keyed by live display label (see
    `_live_label`), not canonical. LIVE edges only (`valid_until`): fold_project heals
    a project record by invalidating the old in_repo edge and creating a fresh one on
    the surviving object; counting both would double-count exactly the writes a fold
    was just run to consolidate, showing a split that no longer exists."""
    if not bases:
        return {"total": 0, "top": None, "breakdown": {}}
    rows = await pool.fetch(
        "SELECT ro.id, ro.canonical, count(*) AS n FROM links l "
        "JOIN objects ro ON ro.id=l.to_id AND ro.type='SoftwareProject' "
        "WHERE l.type='in_repo' "
        "AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "AND EXISTS (SELECT 1 FROM unnest($1::text[]) b "
        "            WHERE l.source_id=b OR l.source_id LIKE b || '-%') "
        "GROUP BY ro.id, ro.canonical ORDER BY n DESC", bases)
    breakdown: dict[str, int] = {}
    for r in rows:
        label = await _live_label(pool, r["id"], r["canonical"])
        breakdown[label] = breakdown.get(label, 0) + int(r["n"])
    total = sum(breakdown.values())
    top = max(breakdown, key=lambda k: breakdown[k]) if breakdown else None
    return {"total": total, "top": top, "breakdown": breakdown}


async def project_identity_evidence(
    pool: asyncpg.Pool, *, seat_id: str, operator_citation: str | None = None,
) -> dict[str, Any]:
    """Gather whichever tiers have signal for `seat_id`'s project identity and report each
    one's answer plus per-candidate agreement/disagreement, NEVER picking a winner. See
    the module docstring for the five tiers and why no fixed precedence list can work
    across the population (two projects can each be the other's counter-example).

    Every distinct project label surfaced by ANY tier becomes a CANDIDATE row,
    cross-checked against every other tier that can speak to it (declared_charter
    membership, pin match, write-attribution share, and, via the candidate's own
    registered on_disk_path, never a guessed root, a live git remote check). A seat with
    zero holders ever (bases == []) still runs: pin/remote/self-authored are cwd-based,
    not lineage-based, and a seat whose whole history predates the Seat object must not
    go blind just because `bases` came back empty."""
    from src.orchestrator.projects import _resolve_software_project
    from src.orchestrator.seats import seat_facts

    row = await pool.fetchrow("SELECT id FROM objects WHERE canonical=$1 AND type='Seat'",
                              seat_id)
    if row is None:
        return {"error": f"no such seat: {seat_id!r}"}
    seat_oid = row["id"]
    facts = await seat_facts(pool, seat_id)
    office = facts.get("anchor_cwd")
    bases = await _seat_lineage_bases(pool, seat_oid)
    charter = await _declared_charter(pool, seat_id, seat_oid, bases)
    pin = None
    pin_merge_confession: str | None = None
    if office:
        from src.orchestrator.agents import read_project_label
        pin = read_project_label(office)
        if pin:
            # NORMALIZE THROUGH merged_into (a related fix confirmed independently
            # elsewhere): a pin naming a label that has since been FOLDED into another
            # must compare as the SURVIVOR, or it shows up as its OWN stale candidate,
            # pin_match true only against itself, declared_charter/remote_agrees reading
            # the LOSER's own stale properties, "agreement" landing on 'disagree' even
            # after the pin was corrected. Degrades to the raw pin on any failure (a
            # diagnostic refinement must never be the reason this read goes blind).
            try:
                pin, pin_merge_confession = await _normalize_project_label_through_merge(
                    pool, pin)
            except Exception:  # noqa: BLE001, see note above
                pass
    self_authored = _self_authored(office)
    write_attr = await _write_attribution(pool, bases)

    candidate_names = sorted({
        *charter,
        *([pin] if pin else []),
        *(k for k in write_attr["breakdown"]),
    })
    candidates: dict[str, Any] = {}
    for name in candidate_names:
        entry: dict[str, Any] = {
            # PRESENTATION, NEVER THE COMPARISON: `name` stays the raw canonical every
            # comparison field below keys and compares on. `display` adds the
            # name-with-canonical rendering a human reading this result actually wants,
            # resolved live.
            "display": await charter_display_label(pool, name),
            "declared_charter": name in charter,
            "pin_match": name == pin,
            "write_attribution": {
                "count": write_attr["breakdown"].get(name, 0), "total": write_attr["total"],
            },
            "on_disk_path": None, "is_git_repo": None, "remote_url": None,
            "remote_agrees": None,
        }
        proj_row = await _resolve_software_project(pool, name)
        if proj_row is not None:
            path = await pool.fetchval(
                "SELECT a.value #>> '{}' FROM current_assertions a "
                "WHERE a.object_id=$1 AND a.name='on_disk_path' "
                "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", proj_row["id"])
            entry["on_disk_path"] = path
            if path:
                is_repo, remote_url = _git_remote(path)
                entry["is_git_repo"] = is_repo
                entry["remote_url"] = remote_url
                if remote_url:
                    entry["remote_agrees"] = (_remote_basename(remote_url) == name)
        candidates[name] = entry

    # write_attribution only counts as SUPPORT for the majority target: a single stray
    # commit filed under the wrong project is not the same claim as a strong majority.
    #
    # REMOTE NEVER COUNTS AS SUPPORT (found on a live re-run against real data):
    # remote_agrees answers a DIFFERENT question, whether the on-disk checkout's git
    # remote has caught up, and this action never moves or renames that checkout
    # (rename_project's own documented scope, `_cascade_governing_seats`'s `could_not_
    # reach.folder_path`). Folding it into "supported" meant a project object every
    # OTHER real tier (charter, pin) already agrees on still read "disagree" for as long
    # as nobody had separately moved the directory, the graph was correct and this
    # function said otherwise. `remote_agrees` stays on every candidate's own entry for
    # a human to read (a real, useful signal, see the stale-remote test), it just never
    # manufactures a rival CANDIDATE or drives `agreement` on its own; the project
    # object's own tiers (declared_charter, pin_match, and write_attribution's majority)
    # are what this function actually verifies.
    supported = {n for n, e in candidates.items()
                if e["declared_charter"] or e["pin_match"]
                or n == write_attr["top"]}
    if len(supported) <= 1:
        agreement = "single-candidate" if supported else "no-signal"
    else:
        agreement = "disagree"

    return {
        "seat_id": seat_id, "handle": facts.get("handle"), "office": office,
        "lineage_bases": bases,
        "operator_confirmed": {"citation": operator_citation,
                               "checked": operator_citation is not None},
        "self_authored": self_authored,
        "candidates": candidates,
        "agreement": agreement,
        **({"pin_merge_confession": pin_merge_confession} if pin_merge_confession else {}),
    }


def rename_evidence_verdict(evidence: dict[str, Any], new_name: str) -> str:
    """The NAMED SIGNAL rename_project's pre-write check surfaces (the governing rule:
    DO NOT CROWN A TIER, wire the evidence in, pick nothing). `evidence` is one seat's
    own `project_identity_evidence` report. Three answers, never a silent pick between
    them (the same principle as the coordination layer's own tri-state handling):

      "no-signal"  : this seat's evidence found no candidate at all (no charter, no pin,
        no attributed work), OR found candidates but none carries real positive signal
        (declared_charter/pin_match/remote_agrees, write_attribution alone never counts,
        a single stray commit is not the same claim `agreement` already warns against).
      "confirms"   : `new_name` is the seat's ONE AND ONLY strongly-evidenced candidate,
        reuses `evidence["agreement"] == "single-candidate"` directly, never a second,
        independently-drifting copy of that computation.
      "disagrees"  : the seat's evidence disagrees WITH ITSELF (`agreement == "disagree"`,
        more than one candidate carries real signal) regardless of whether `new_name` is
        one of the rivals, OR the seat's one strongly-evidenced candidate is something
        other than `new_name`. LIVE-VERIFIED SPECIMEN: for one seat and project, remote_
        agrees and write_attribution both backed the new project name (strongly
        evidenced) while the seat's OWN PIN still named the old project, a naive "does
        new_name have signal" check would have called this "confirms" and buried the
        exact stale-pin disagreement this check exists to catch. Ambiguity IS the
        finding here, not a tiebreak new_name wins by having the stronger case."""
    candidates: dict[str, Any] = evidence.get("candidates") or {}
    if not candidates:
        return "no-signal"
    agreement = evidence.get("agreement")
    if agreement == "disagree":
        return "disagrees"
    if agreement == "single-candidate":
        entry = candidates.get(new_name)
        if entry and (entry.get("declared_charter") or entry.get("pin_match")
                     or entry.get("remote_agrees")):
            return "confirms"
        return "disagrees"  # the seat's one supported candidate is NOT new_name
    return "no-signal"  # agreement == "no-signal"


# --- rename_project / fork_project -------------------------------------------------------

def _dir_exists(target: str) -> bool:
    """A plain sync helper (ASYNC240: file I/O stays out of async function bodies, same
    convention identity_heal.py's own `_office_dir_exists`/trigger.py's `_tree_exists`
    already document); the tree-binding tier below only ever DETECTS, never provisions."""
    return Path(target).is_dir()


async def _cascade_governing_seats(
    pool: asyncpg.Pool, *, project_oid: Any, old_name: str, new_name: str,
    because: str, actor: str, dry_run: bool,
) -> dict[str, Any]:
    """THE RENAME CASCADE ITSELF (the governing principle behind it: there has to be an
    action that links the rename mechanically so agents don't get lost tracking it down
    themselves). For every seat governing the renamed project, reaches five tiers, pin,
    house, charter, office render, tree binding, with the CASCADE'S OWN elevated
    authority, never the caller's own: a project rename is a project-level act, not a
    manager-subordinate one, so it calls the SAME "not headship-gated, callers
    responsible" third-party functions this house already built for exactly this shape
    (`correct_pin_value_third_party`, `resync_seat_house_third_party`) plus the two
    functions that were never gated at all (`set_charter`, `reissue_office`, a parallel
    fix is what makes `set_charter` atomic-with-read-back; this calls the identical
    function, no new one).

    NEVER SILENCE ON A PARTIAL RESULT: returns a MANIFEST, every tier named 'touched' /
    'already-correct' / 'could-not' per seat, so a caller sees the exact remainder
    instead of a result that looks complete when it isn't. `dry_run=True` previews every
    tier via each function's own peek/dry-run shape without writing anything;
    `dry_run=False` executes them, one tier's failure caught and reported rather than
    aborting the rest (a rename cascade that stops at the second seat because the first
    seat's office had no CLAUDE.md would be strictly worse than a graph-only rename).

    NEVER GUESSES A FILESYSTEM MOVE: tree binding is DETECT-ONLY, this action reports
    when a seat's `tree_cwd` still names the old label and whether a same-shaped new-
    named path exists on disk, but never calls `bind_seat_tree` itself; inferring and
    rebinding a code checkout's location is a deliberate act a human confirms, not
    something a name-property write should trigger sight-unseen. The project's own
    on-disk folder stays permanently out of scope by the same rule, named honestly in
    the manifest's own `could_not_reach`, never silently skipped.

    THE REPO-ROOT `.osiris` FILE, a THIRD, distinct pin copy from any of a seat's own
    office/anchor/workspace pins `correct_pin_value_third_party` already reaches, IS
    now reached: a real per-seat tier, resolved off the SAME `tree_cwd` the
    tree-binding check above already trusts, via the identical `correct_pin_value`
    primitive the other three copies already use. Touched when a real, existing
    `tree_cwd` carries a `.osiris` declaring `project`; an honest could-not otherwise
    (no tree_cwd, a vanished directory, no `.osiris` there, or no `project` key
    declared), never a guess at a path this cascade has no other evidence for."""
    from src.orchestrator.boot_compiler import reissue_office
    from src.orchestrator.capture import _resolve_repo
    from src.orchestrator.charter import charter_of, set_charter
    from src.orchestrator.offices import correct_pin_value, correct_pin_value_third_party
    from src.orchestrator.projects import _peek_pin_value
    from src.orchestrator.seats import resync_seat_house_third_party, seat_facts

    seat_rows = await pool.fetch(
        "SELECT s.canonical FROM links l JOIN objects s ON s.id=l.from_id "
        "WHERE l.to_id=$1 AND l.type='governs' AND s.type='Seat' AND s.status='active' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", project_oid)
    governing = [r["canonical"] for r in seat_rows]
    actions = Actions(pool)

    seats_out: dict[str, Any] = {}
    for seat_id in governing:
        facts = await seat_facts(pool, seat_id)
        tiers: dict[str, Any] = {}

        # PIN: correct_pin_value_third_party already reaches all three copies
        # (office/anchor/workspace) in one call; its own dry_run=True IS the peek.
        try:
            peek = await correct_pin_value_third_party(
                pool, seat_id, "project", new_name, reason=because, dry_run=True)
            plan = peek.get("plan") or {}
            if peek.get("error"):
                tiers["pin"] = {"status": "could-not", "detail": peek["error"]}
            elif not plan:
                tiers["pin"] = {"status": "already-correct"}
            elif dry_run:
                tiers["pin"] = {"status": "touched", "plan": plan}
            else:
                real = await correct_pin_value_third_party(
                    pool, seat_id, "project", new_name, reason=because, dry_run=False)
                # PER-COPY SUCCESS, NOT THE BLUNT TOP-LEVEL ERROR: correct_own_pin_value
                # (which this delegates to) reports the OFFICE copy's own outcome at the
                # TOP LEVEL and the anchor/workspace copies nested under their own keys.
                # A seat with no conventional office (only a real anchor/workspace copy,
                # a live shape this house already treats as legitimate) sees a top-level
                # `error` from the office attempt alone even when the copy the PLAN
                # actually named got written. Success is "any copy the plan named
                # actually landed," never "the office copy in particular succeeded."
                planned_ok = any(
                    (real.get("written") is not False and not real.get("error")
                     if label == "office" else
                     isinstance(real.get(label), dict) and not real[label].get("error"))
                    for label in plan
                )
                tiers["pin"] = ({"status": "touched", "detail": real} if planned_ok else
                                {"status": "could-not",
                                 "detail": real.get("error") or real})
        except Exception as exc:  # noqa: BLE001, one tier's failure must never sink the rest
            tiers["pin"] = {"status": "could-not", "detail": str(exc)}

        # HOUSE: only touch when it currently names the OLD label; a house a seat
        # legitimately shares with siblings under a third name is never this cascade's
        # to overwrite (house is DERIVED, not every seat's house IS its project name).
        house = facts.get("house")
        if house == new_name:
            tiers["house"] = {"status": "already-correct"}
        elif house != old_name:
            tiers["house"] = {"status": "already-correct",
                              "note": f"house={house!r} names neither {old_name!r} nor "
                                      f"{new_name!r}, so it was left untouched; not this "
                                      "cascade's to guess"}
        elif dry_run:
            tiers["house"] = {"status": "touched",
                              "plan": f"{old_name!r} -> {new_name!r}"}
        else:
            try:
                hres = await resync_seat_house_third_party(
                    actions, seat_id, new_name, source=actor, reason=because)
                tiers["house"] = ({"status": "could-not", "detail": hres["error"]}
                                  if hres.get("error") else {"status": "touched", "detail": hres})
            except Exception as exc:  # noqa: BLE001
                tiers["house"] = {"status": "could-not", "detail": str(exc)}

        # CHARTER: set_charter replaces the WHOLE list; RESOLVE each entry against the
        # PROJECT'S OWN ID, never a literal string match against old_name/new_name.
        # `charter_of` derives its entries from live `governs` edges' own canonical,
        # which is IMMUTABLE (rename_project's docstring: canonical never changes, only
        # the `name` property does), so an entry can be the project's mint-time label
        # from a PRIOR rename, neither this call's old_name nor its new_name, and a bare
        # string compare silently no-ops exactly when the charter is most stale (a
        # specimen seen live: an old label never matches either side of a later
        # rename). `old_name` is a permanent read alias forever (this house's own
        # convention), so the only honest test is "does this entry resolve to the SAME
        # project id being renamed", the identical write-vs-read-key split idiom
        # PIN/HOUSE above already hold.
        # RUNS REGARDLESS OF new_name == old_name (a correction caught during a dry-run
        # re-check): a self-rename is exactly how this action's own repair population
        # gets invoked, calling rename(X, X) on purpose to HEAL a stale alias, not a
        # no-op to protect. Skipping the resolve loop on new_name==old_name silently
        # no-op'd the very specimen this tier exists to fix. A genuinely vacuous
        # self-rename (no stale alias present at all) still naturally reports
        # "already-correct" below, nothing in `stale`, so removing the guard changes
        # nothing for that case, only unblocks the real one.
        try:
            current_charter = await charter_of(pool, seat_id)
            stale: list[str] = []
            for entry in current_charter:
                if entry == new_name:
                    continue
                resolved = await _resolve_repo(pool, entry)
                if resolved is not None and resolved == project_oid:
                    stale.append(entry)
            if stale:
                new_charter = sorted(
                    ({new_name} | set(current_charter)) - set(stale))
                # PREDICT set_charter's OWN resolve-then-diff BEFORE trusting `stale` as
                # proof a write is coming (found during a real dry-run/apply
                # comparison: dry run promised a plan changing the charter, but the
                # real apply's own set_charter detail showed no actual additions or
                # removals, "the manifest now reports a write it did not make").
                # `stale` only tests "isn't literally new_name," but `new_name` ALWAYS
                # resolves back down to project_oid's own ETERNAL canonical
                # (rename_project's contract: canonical never changes, only the
                # mutable `name` property does), the exact same canonical `stale`'s
                # own entries already are. Mirroring set_charter's real
                # resolve-then-diff (charter.py) here, using project_oid's own
                # canonical directly for the `new_name` candidate (rather than
                # resolving it, on a dry run the name-property write hasn't landed
                # yet, so `new_name` cannot resolve to anything), makes PLAN and APPLY
                # predict the identical outcome regardless of dry_run: a plan can
                # never again promise a correction set_charter's own diff then
                # silently no-ops.
                project_canon = str(await pool.fetchval(
                    "SELECT canonical FROM objects WHERE id=$1", project_oid)
                    ).removeprefix("repo:")
                resolved_canons: set[str] = set()
                for cand in new_charter:
                    if cand == new_name:
                        resolved_canons.add(project_canon)
                        continue
                    cand_id = await _resolve_repo(pool, cand)
                    if cand_id is None:
                        continue  # mirrors set_charter's own `rejected`: dropped, not kept raw
                    resolved_canons.add(str(await pool.fetchval(
                        "SELECT canonical FROM objects WHERE id=$1", cand_id)
                        ).removeprefix("repo:"))
                real_added = sorted(resolved_canons - set(current_charter))
                real_removed = sorted(set(current_charter) - resolved_canons)
                if not real_added and not real_removed:
                    # THE PHANTOM-WRITE CASE, NOW HONEST: every entry `stale` flagged
                    # resolves right back to the SAME canonical it already was, nothing
                    # set_charter could ever actually add or remove, structurally, for
                    # a plain rename (never a fold, no second object exists to swap
                    # to). Reporting "touched" here was the lie; "already-correct" is
                    # what the graph, and set_charter's own real diff, would show.
                    tiers["charter"] = {"status": "already-correct"}
                elif dry_run:
                    tiers["charter"] = {"status": "touched",
                                        "plan": f"{sorted(current_charter)} -> "
                                                f"{sorted(resolved_canons)}",
                                        "added": real_added, "removed": real_removed}
                else:
                    cres = await set_charter(actions, seat_id, new_charter, actor=actor)
                    tiers["charter"] = ({"status": "could-not", "detail": cres["error"]}
                                        if cres.get("error")
                                        else {"status": "touched", "detail": cres})
            elif new_name in current_charter:
                # VERIFIED-EQUAL: current_charter genuinely contains new_name's own
                # literal string, a real check, a real match, never a guess.
                tiers["charter"] = {"status": "already-correct"}
            else:
                # NOT-EVALUATED, NEVER "ALREADY-CORRECT" (an earlier version of this
                # check wrongly labeled this case "already-correct" without verifying
                # it; this comment exists to prevent repeating that mistake).
                # Structurally this should not fire for any seat THIS LOOP
                # ever reaches: `governing` above is queried by an active `governs`
                # edge to project_oid, so charter_of(seat_id) is guaranteed to
                # return at least one entry whose own canonical IS project_oid's own
                # canonical, which resolves to project_oid trivially and always
                # lands in `stale` above unless it already equals new_name. Reaching
                # this branch at all means something is wrong beneath the surface
                # (a governs edge outliving its own target row, a resolver failure),
                # distinct status so a caller/test can tell "verified, matches" from
                # "the check itself found nothing to compare," never conflating the
                # two under one green word.
                tiers["charter"] = {"status": "not-evaluated",
                                    "note": "charter names neither the old nor the new "
                                            "label, and no entry resolved to this "
                                            "project, so it was left untouched; this "
                                            "should not happen for a seat with an active "
                                            "governs edge to the renamed project"}
        except Exception as exc:  # noqa: BLE001
            tiers["charter"] = {"status": "could-not", "detail": str(exc)}

        # OFFICE RENDER: recompile CLAUDE.md so it reflects whatever charter/house
        # this same call already corrected above (runs after, on purpose). GATED ON
        # UPSTREAM ACTUALLY CHANGING (a second observation from the same review:
        # "office reports touched on every run even when nothing upstream changed; it
        # should be already-correct when charter and house are verified-equal"), this
        # used to reissue unconditionally whenever an office file exists, minting a new
        # version every call even when pin/house/charter all read "already-correct",
        # a real write (a fresh version stamp) recompiling byte-identical content,
        # which is a "touched" every bit as phantom as the charter bug noted above.
        # Skip the reissue entirely when nothing upstream this loop touched actually
        # changed.
        upstream_touched = any(
            tiers.get(tier, {}).get("status") == "touched" for tier in ("pin", "house", "charter"))
        anchor = facts.get("anchor_cwd")
        if not anchor or not (Path(anchor) / "CLAUDE.md").is_file():
            tiers["office"] = {"status": "could-not",
                               "detail": "no office/CLAUDE.md on record for this seat"}
        elif not upstream_touched:
            tiers["office"] = {"status": "already-correct"}
        elif dry_run:
            tiers["office"] = {"status": "touched",
                               "plan": "would reissue_office to reflect the updated "
                                       "charter/house"}
        else:
            try:
                ores = await reissue_office(actions, seat_id=seat_id, because=because,
                                            actor=actor)
                tiers["office"] = ({"status": "could-not", "detail": ores["error"]}
                                   if ores.get("error") else {"status": "touched", "detail": ores})
            except Exception as exc:  # noqa: BLE001
                tiers["office"] = {"status": "could-not", "detail": str(exc)}

        # TREE BINDING: DETECT ONLY, never written (see docstring above).
        #
        # CHECK THE NEW NAME FIRST (a live specimen with a rename where old and new
        # names briefly coexisted): a rename that migrates ONLY the canonical, the
        # exact shape where a project already renamed under the old, name-only
        # convention has old_name == new_name by the time this tier runs, used to
        # compare tree_cwd against old_name alone. When old_name == new_name (or
        # old_name merely happens to be a substring the new label also carries), that
        # comparison finds the label IN THE ALREADY-CORRECT PATH and misreports a
        # fully up-to-date tree_cwd as "references the old name", comparing against
        # the wrong side of the rename. A path that already carries new_name is
        # correct regardless of what old_name says, so that check runs first and
        # short-circuits the rest of this tier.
        tree_cwd = facts.get("tree_cwd")
        if not tree_cwd or f"/{new_name}" in tree_cwd:
            tiers["tree"] = {"status": "already-correct"}
        elif f"/{old_name}" in tree_cwd:
            candidate = tree_cwd.replace(f"/{old_name}", f"/{new_name}")
            if not _dir_exists(tree_cwd) and _dir_exists(candidate):
                tiers["tree"] = {
                    "status": "could-not",
                    "detail": f"tree_cwd {tree_cwd!r} no longer exists on disk and "
                             f"{candidate!r} does. This cascade never rebinds a tree "
                             "automatically; confirm it, then call "
                             f"seat(action='bind_tree', seat_id={seat_id!r}, "
                             f"tree_cwd={candidate!r}) yourself"}
            else:
                tiers["tree"] = {
                    "status": "could-not",
                    "detail": f"tree_cwd {tree_cwd!r} references the old name; this "
                             "cascade never moves or infers folders, only detects"}
        else:
            tiers["tree"] = {"status": "already-correct"}

        # REPO-ROOT .OSIRIS: named as a THIRD, distinct pin copy from a seat's own
        # office/anchor/workspace pins, this cascade's docstring used to call it
        # permanently unreachable ("no sanctioned write function exists for it yet").
        # NOT AS DISTINCT AS FIRST ASSUMED, discovered while landing this tier:
        # `correct_own_pin_value`'s own "workspace" copy already resolves off
        # `tree_cwd` when one is declared (a prior tree_cwd fix to seat_facts), the
        # SAME on-disk file this tier targets, written by the "pin" tier just above,
        # in the same loop iteration, before this tier ever runs. So for the common
        # case (a seat with a real tree_cwd on record) this tier's own
        # `correct_pin_value` call would only ever find the value the pin tier already
        # wrote, a redundant no-op, not a second real write. Rather than attempt one
        # anyway, this tier credits the pin tier's own workspace copy when that's what
        # actually moved the file (see the `via_pin` check below), and only reaches
        # for its OWN `correct_pin_value` call in the genuine remainder: a `tree_cwd`
        # whose resolved workspace path was excluded from the pin tier's own targets
        # (identical to office/anchor, so workspace never appears in its plan) yet the
        # repo-root file itself still names the old label. `correct_pin_value`/
        # `_peek_pin_value` refuse on a missing file, invalid TOML, or a missing
        # `project` key, a repo whose root carries no `.osiris` at all, or one that
        # never declared `project`, reports an honest could-not naming why, never a
        # silent skip. NEVER GUESSES A PATH: no `tree_cwd` on record, or one that
        # doesn't exist on disk (the exact "tree" tier finding just above), is itself
        # an honest could-not here too.
        if not tree_cwd or not _dir_exists(tree_cwd):
            tiers["repo_root_osiris"] = {
                "status": "could-not",
                "detail": "no real tree_cwd on record for this seat; nothing to "
                         "check a repo-root .osiris pin against (see the tree tier "
                         "above)"}
        else:
            peek = _peek_pin_value(tree_cwd, "project")
            if not peek["ok"]:
                tiers["repo_root_osiris"] = {"status": "could-not", "detail": peek["error"]}
            elif peek["value"] == new_name:
                # SAME FILE THE PIN TIER'S OWN "workspace" COPY JUST WROTE, NOT A
                # SEPARATE ALREADY-CORRECT STATE: correct_own_pin_value's workspace
                # copy already resolves off this identical tree_cwd (a prior blind-spot
                # fix to seat_facts) and runs BEFORE this tier in the loop above, so a
                # real rename that just wrote it here reads back as
                # "no correction needed" unless the pin tier's own write is credited.
                # Telling the two apart (rather than a second redundant write attempt,
                # which would only ever find this exact same value already in place)
                # keeps the manifest honest about which copy actually moved.
                pin_detail = tiers["pin"].get("detail")
                via_pin = (
                    tiers["pin"].get("status") == "touched"
                    and isinstance(pin_detail, dict)
                    and isinstance(pin_detail.get("workspace"), dict)
                    and pin_detail["workspace"].get("written")
                    and pin_detail["workspace"].get("path") == str(Path(tree_cwd) / ".osiris"))
                tiers["repo_root_osiris"] = (
                    {"status": "touched",
                     "detail": "corrected via the pin tier's own workspace copy, "
                              "same tree_cwd path, one write not two"}
                    if via_pin else {"status": "already-correct"})
            elif dry_run:
                tiers["repo_root_osiris"] = {
                    "status": "touched",
                    "plan": f"{tree_cwd}/.osiris: project {peek['value']!r} -> "
                            f"{new_name!r}"}
            else:
                real = correct_pin_value(tree_cwd, "project", new_name, reason=because)
                tiers["repo_root_osiris"] = (
                    {"status": "could-not", "detail": real["error"]}
                    if real.get("error") else {"status": "touched", "detail": real})

        seats_out[seat_id] = tiers

    return {
        "seats": seats_out,
        # THE REMAINDER: `repo_root_osiris` moved OUT of this dict, it is now a real
        # per-seat tier (`tiers["repo_root_osiris"]` above), touched/already-correct/
        # could-not the same as every other copy this cascade reaches, never a static
        # non-answer. `folder_path` stays here, by DESIGN, not by gap: this cascade
        # never moves or infers a directory rename, see each seat's own "tree" tier
        # for the real, evidence-based detection (does a same-shaped renamed path
        # already exist on disk) this exact question already gets, per seat; this
        # entry is the one honest, permanent statement of scope the per-seat tiers
        # don't already carry on their own.
        "could_not_reach": {
            "folder_path": "the project's on-disk directory is never moved or inferred "
                           "by this function, by design; see each seat's own 'tree' tier "
                           "for the real per-seat detection of whether a renamed path "
                           "already exists; mv it yourself first if the rename should "
                           "follow the code",
        },
    }


# THE OFF-GRAPH STRING COLUMNS a project label lives in OUTSIDE objects/links/assertions
# (measured against the live schema): plain `text` project fields, never a foreign key.
# A rename's canonical migration re-addresses every one of them from the old label(s) to
# the new through the SAME function. tests/test_project_identity.py asserts this list
# still equals every `%project%` text column in the schema, so a new table cannot
# silently join the population unmigrated.
CANONICAL_STRING_COLUMNS: tuple[tuple[str, str], ...] = (
    ("agent_mounts", "project"),
    ("agent_wakes", "to_project"),
    ("body_usage", "project"),
    ("fleet_messages", "from_project"),
    ("fleet_messages", "to_project"),
    ("harness_sessions", "project"),
)


async def _canonical_occurrences(
    pool: asyncpg.Pool, old_labels: set[str],
) -> dict[str, int]:
    """Row counts per off-graph `table.column` currently holding one of `old_labels`.
    This is the dry-run result's own list of every literal occurrence the cascade will
    touch."""
    out: dict[str, int] = {}
    if not old_labels:
        return out
    for table, col in CANONICAL_STRING_COLUMNS:
        n = await pool.fetchval(
            f"SELECT count(*) FROM {table} WHERE {col} = ANY($1::text[])",  # noqa: S608
            sorted(old_labels))
        out[f"{table}.{col}"] = int(n)
    return out


async def rename_project(
    actions: Actions, *, project: str, new_name: str, because: str, actor: str,
    dry_run: bool = True, merge_into: bool = False,
) -> dict[str, Any]:
    """RENAME: ONE object (its uuid, every edge, every assertion) survives; the name AND
    the canonical move. THE CANONICAL MIGRATES TOO. This reverses the older assumption
    that the canonical is immutable forever for SoftwareProject: `repo:<old>` becomes
    `repo:<new_name>` by a compensating `canonical_changed` object_event
    (`Actions.change_canonical`, never a delete), and the retired string is appended to
    `object_aliases` so every resolver (create_or_find_object, resolve_ref,
    _resolve_repo, _resolve_software_project, _resolve_or_mint_project) still resolves
    it to the SAME object. A write naming the alias lands on the migrated object, never
    a stub. A MERGED stub squatting the target canonical is moved aside
    (`repo:<new>~merged-<id8>`); an active/retired holder refuses even with merge_into.
    The `name` PROPERTY changes in the same atomic block, old values kept in assertion
    history (never deleted). Every off-graph project string (see
    CANONICAL_STRING_COLUMNS) is re-addressed in the same call; the result's
    `canonical_migration` names every occurrence and what could not be reached.

    ZERO graph EDGES move: works_in/governs/in_repo already point at this object's
    stable `id`, not at its name or canonical, so every existing edge stays correct
    automatically, unlike fold_project, which re-points a set of relationships because
    it retires a SECOND object. Only `agent_mounts.project` (a loose string column,
    never an FK, the same shape fold_project's own relationship move already handles)
    is re-addressed old bare canonical -> new_name, so a fresh mount under the
    corrected name resolves cleanly.

    THE CASCADE: this function is no longer graph-only. Every SEAT governing this project
    (a live `governs` link) has its own pin/house/charter/office cascaded under this
    function's OWN elevated authority, see `_cascade_governing_seats`, rather than left to
    drift the way earlier specimens did. The result's `manifest` names every tier
    `touched`/`already-correct`/`could-not`, per seat, so a partial cascade hands back
    the exact remainder rather than silence. OUT OF SCOPE, named honestly rather than
    silently skipped (the same discipline rename_seat holds for the harness window
    title it cannot reach): the project's own on-disk folder is never moved by any
    Osiris function, named in the manifest's own `could_not_reach`, though each governing
    seat's own `tree` tier still detects whether a renamed path already exists. The
    repo's own ROOT `.osiris` file (a third copy, distinct from any seat's own
    office/anchor/workspace pins) is NO LONGER out of scope: a real per-seat tier,
    `repo_root_osiris`, reaches it via the same `correct_pin_value` primitive the
    other three copies already use.

    THE CALLER DECLARES; THIS FUNCTION NEVER INFERS. `because` is mandatory, a rename
    is testimony, the same discipline rename_seat/correct_house already hold. This
    function does no evidence-gathering of its own; that is project_identity_evidence's
    job, run BEFORE this is called, by a human who read its report.

    PRIOR-ART SURFACED, NEVER REFUSED: the result's own `prior_art`/`prior_art_flag`
    keys, when present, name a standing Decision that may already cover this exact
    rename, the same search()-based guard record_decision already runs on itself,
    generalized here. This CANNOT tell a deliberate correction of an earlier decision
    from an uninformed overwrite of one; it does not try to. It only ensures the write
    does not land silently unread.

    Refuses LOUDLY on: a blank `new_name` or `because`; an unresolved or ambiguous
    `project` ref (AmbiguousProjectRef, named exactly like every other project function);
    a non-active project; `new_name` already resolving to a DIFFERENT SoftwareProject
    that is active OR retired (a real collision either way, since retired is a dormant,
    revivable identity, not a dead one), unless `merge_into=True` is passed explicitly,
    acknowledging the caller has already seen the collision and means to reuse the name
    anyway (this still never merges the two objects itself; it only lifts the refusal).
    An ALREADY-MERGED object under `new_name` is NEVER a collision: `merged` is
    permanent and terminal (only fold_project ever sets it, and nothing un-sets it), a
    dead identity is exactly what a rename is free to reclaim; `merge_into` is never
    needed for that case.

    `dry_run=True` (the default, same convention as every other write function in this
    file) returns the exact plan: resolved project, old/new name, any collision found,
    without writing anything: no `assert_property`, no `agent_mounts` repoint, no
    prior-art search. Pass `dry_run=False` explicitly to actually rename.

    THE POST-WRITE READ-BACK: on a real write, `dry_run=False` returns
    `current_name_after_write` (the SAME confidence-ordered current-value read every
    other "which name wins" reader in this codebase uses, run immediately after the
    write) and `rename_confirmed` (whether it equals `new_name`). The write above
    succeeding is proof the row was WRITTEN, never proof it is still the winner a
    moment later (register_agent's own same-source clobber guard, before its own fix,
    is exactly the kind of write that could erase this one)."""
    from src.orchestrator.projects import AmbiguousProjectRef, _resolve_software_project

    project = (project or "").strip()
    new_name = (new_name or "").strip()
    because = (because or "").strip()
    if not project:
        return {"error": "project is required"}
    if not new_name:
        return {"error": "new_name is required"}
    if not because:
        return {"error": "because is required: a rename is testimony; the reason it "
                         "changed must be on the record"}
    try:
        row = await _resolve_software_project(actions.pool, project)
    except AmbiguousProjectRef as amb:
        return {"error": f"{amb.ref!r} is ambiguous: {len(amb.candidates)} active "
                         f"SoftwareProjects answer to it: {', '.join(amb.candidates)}. "
                         "Name the exact one (canonical or id); rename_project never "
                         "guesses which."}
    if row is None:
        return {"error": f"no such SoftwareProject: {project!r}"}
    if row["status"] != "active":
        return {"error": f"{row['canonical']} is {row['status']}, not active; nothing "
                         "to rename"}
    # REFUSE RATHER THAN SILENTLY FALL BACK: a database that hasn't yet run alembic 0072
    # has no object_aliases table. `_resolve_software_project`'s own alias fallback (and
    # every resolve site downstream) needs it to exist. Checked BEFORE any lookup that
    # could touch it, so a stray direct call against an un-migrated database gets one
    # clear refusal instead of a raw asyncpg error. The deploy runs alembic before code
    # lands, so this should never fire live.
    if not await actions.pool.fetchval("SELECT to_regclass('object_aliases') IS NOT NULL"):
        return {"error": "object_aliases does not exist on this database; run alembic "
                         "upgrade (migration 0072) before renaming a project; a canonical "
                         "migration with nothing to alias the old string to would orphan it"}
    try:
        collide = await _resolve_software_project(actions.pool, new_name)
    except AmbiguousProjectRef:
        collide = None  # an ambiguity already living under new_name is a pre-existing
                        # problem this rename did not create and is not asked to solve
    # A MERGED OBJECT IS NOT A HOLDER OF THE NAME: `_resolve_software_project`'s own
    # fallback finds a non-active object by exact canonical with no status filter (its
    # own comment: a caller resolving a KNOWN-dead label must still find it), which is
    # correct for THAT caller but wrong reused here unfiltered. A project this rename's
    # own caller just folded (fold_project sets status='merged', never deletes the row)
    # still answers to its old canonical, so renaming the survivor back onto that exact
    # label read as a live collision requiring merge_into=True even though nothing
    # active disputes the name at all. `merged` is a PERMANENT terminal state (only
    # fold_project ever sets it, and it never un-sets), unlike `retired`, which a project
    # can be revived from: the retired-collision test (a distinct, still-correct case: a
    # dormant-but-revivable identity really can collide) stays exactly as strict as
    # before. Only `merged` is excluded here; `retired` still counts as a real collision
    # requiring `merge_into=True` to lift.
    if (collide is not None and collide["id"] != row["id"] and collide["status"] != "merged"
            and not merge_into):
        return {"error": f"{new_name!r} already names a DIFFERENT project "
                         f"({collide['canonical']}, status={collide['status']}). "
                         "rename_project never collides two identities silently; pass "
                         "merge_into=True if this is deliberate (it only lifts this "
                         "refusal, it does not itself merge the two objects; "
                         "fold_project is the evidence-gated function for that), or name a "
                         "genuinely free new_name instead"}
    old_name = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='name' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1",
        row["id"])
    # THE CANONICAL MIGRATES TOO: repo:<old> -> repo:<new_name>, the uuid and every edge
    # stay; the retired string becomes an ALIAS (object_aliases). Every refusal that
    # could apply is decided HERE, before any write.
    from src.orchestrator.capture import _REPO_NAME_RE

    old_canonical = str(row["canonical"])
    new_canonical = f"repo:{new_name}"
    migrates = new_canonical != old_canonical
    holder = None
    canonical_skip: str | None = None
    if migrates:
        if not _REPO_NAME_RE.fullmatch(new_name):
            return {"error": f"{new_name!r} cannot be a canonical: a rename now migrates "
                             "the canonical too, so new_name must be a bare project name "
                             "(the same shape capture's repo= accepts), never a path or "
                             "placeholder"}
        holder = await actions.pool.fetchrow(
            "SELECT id, canonical, status FROM objects WHERE type='SoftwareProject' "
            "AND canonical=$1 AND id<>$2", new_canonical, row["id"])
        if holder is not None and holder["status"] != "merged":
            if not merge_into:
                return {"error": f"{new_canonical} is already held by a "
                                 f"{holder['status']} SoftwareProject; a canonical "
                                 "cannot be shared; fold_project is the evidence-gated "
                                 "function for two live identities"}
            # merge_into=True is the caller's explicit "reuse the NAME anyway": a unique
            # canonical cannot be shared, so the name moves and the canonical stays put,
            # said plainly in the result rather than silently skipped.
            migrates = False
            canonical_skip = (f"{new_canonical} is held by a {holder['status']} "
                              "SoftwareProject (merge_into=True kept the name-only "
                              "rename); canonical NOT migrated")
            holder = None
        other_alias = await actions.pool.fetchval(
            "SELECT object_id FROM object_aliases WHERE type='SoftwareProject' AND alias=$1",
            new_canonical)
        if other_alias is not None and other_alias != row["id"]:
            return {"error": f"{new_canonical} is a retired canonical (alias) of a "
                             "DIFFERENT project: refusing to steal an alias"}
    old_labels = {old_canonical.removeprefix("repo:"), *([old_name] if old_name else [])}
    old_labels.discard(new_name)
    canonical_plan: dict[str, Any] = {
        "migrates": migrates, "from": old_canonical, "to": new_canonical,
        "skipped_because": canonical_skip,
        "alias_retained": old_canonical if migrates else None,
        "merged_holder_moved_aside": (
            {"id": str(holder["id"]), "to": f"{new_canonical}~merged-{str(holder['id'])[:8]}"}
            if holder is not None else None),
        "off_graph_occurrences": await _canonical_occurrences(actions.pool, old_labels),
        "not_migrated": ["Agent `project` assertion values (each agent's own "
                         "self-declaration; healed at its next mount/register_agent)",
                         "the project's on-disk folder (never moved by any Osiris function)"],
    }
    if dry_run:
        manifest = await _cascade_governing_seats(
            actions.pool, project_oid=row["id"], old_name=old_name or "", new_name=new_name,
            because=because, actor=actor, dry_run=True)
        return {"project": row["canonical"], "old_name": old_name, "new_name": new_name,
                "canonical_migration": canonical_plan,
                "because": because, "dry_run": True,
                "collision": (f"{collide['canonical']} (status={collide['status']}); "
                              f"would proceed only because merge_into={merge_into!r}"
                              if collide is not None and collide["id"] != row["id"]
                              and collide["status"] != "merged"
                              else None),
                "manifest": manifest,
                "note": "preview only; pass dry_run=False to actually rename"}
    now = datetime.now(UTC)
    # SINGULAR, NOT SAME-SOURCE-ONLY: an earlier live specimen showed a project's
    # dossier listing nine old name rows beside the new one as agreement=contradicting.
    # assert_property's own supersession is same-source-only by design, correct for
    # genuine multi-source corroboration, but wrong for a DECLARED rename, which is a
    # workflow transition exactly like resolve_thread's own status write
    # (assert_singular_property's own documented shape): once a human declares the
    # identity has changed, every OTHER source's still-current "name" opinion is not a
    # competing witness to preserve, it is exactly what the rename supersedes. Using
    # assert_property here left every session that had ever self-declared the OLD name
    # still "current" forever after, so a real, confirmed rename read as a live,
    # unresolved dispute in entity_dossier: the graph was right (current_assertions' own
    # confidence-ordered read already picked the new name) and the dossier's own
    # multi-source agreement view said otherwise.
    async with actions.atomic() as a:
        if holder is not None:  # a MERGED stub squatting the target canonical: move aside
            await a.change_canonical(
                holder["id"], canonical_plan["merged_holder_moved_aside"]["to"],
                f"rename_project freeing {new_canonical} from a merged stub: {because}",
                actor, alias_old=False)
        if migrates:
            await a.change_canonical(row["id"], new_canonical,
                                     f"rename_project: {because}", actor)
        await a.assert_singular_property(
            row["id"], "name", new_name, actor, now, _RENAME_CONF,
            because=f"rename_project: {because}", evidence_class=_EC)
    # THE POST-WRITE READ-BACK, PROJECT IDENTITY DRIFT: an earlier live specimen read
    # NINE current old-name values and none of the new name at all after an earlier
    # rename. A write this function just made is not proof the write STUCK: the same-source
    # clobber register_agent's own project-name guard could still (pre-fix) commit had
    # already erased a rename's own row before its writer ever checked. Reads back the
    # SAME confidence-ordered query every other "which name wins" reader in this
    # codebase already uses, immediately after the write, so the result PROVES what
    # actually landed rather than assuming the call above succeeded just because it
    # didn't raise. `assert_property` returning cleanly says the row was written, never
    # that it's still the winner a moment later.
    current_name_after_write = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name='name' "
        "ORDER BY confidence DESC, observed_at DESC LIMIT 1", row["id"])
    rename_confirmed = current_name_after_write == new_name
    if not rename_confirmed:
        logger.warning(
            "rename_project(%s): wrote name=%r but the confidence-ordered current "
            "value reads %r immediately after; the write did not win",
            row["canonical"], new_name, current_name_after_write)
    bare_old = row["canonical"].removeprefix("repo:")
    off_graph_moved: dict[str, int] = {}
    for table, col in CANONICAL_STRING_COLUMNS:
        tag = await actions.pool.execute(
            f"UPDATE {table} SET {col}=$1 WHERE {col} = ANY($2::text[])",  # noqa: S608
            new_name, sorted(old_labels))
        off_graph_moved[f"{table}.{col}"] = int(tag.rsplit(" ", 1)[-1])
    mounts_moved = off_graph_moved["agent_mounts.project"]
    manifest = await _cascade_governing_seats(
        actions.pool, project_oid=row["id"], old_name=old_name or bare_old, new_name=new_name,
        because=because, actor=actor, dry_run=False)
    from src.orchestrator.capture import property_prior_art
    from src.orchestrator.identity_heal import detect_possibly_stale_seats

    prior_art_bits = await property_prior_art(
        actions.pool, subject_canonical=new_canonical, field="name",
        new_value=new_name, because=because, actor=actor)
    stale = await detect_possibly_stale_seats(actions.pool, old_name or bare_old)
    real_canonical_migration = {k: v for k, v in canonical_plan.items()
                                if k != "off_graph_occurrences"}
    real_canonical_migration["off_graph_moved"] = off_graph_moved
    return {"project": new_canonical, "old_canonical": old_canonical,
           "old_name": old_name, "new_name": new_name,
           "canonical_migration": real_canonical_migration,
           "manifest": manifest,
           "current_name_after_write": current_name_after_write,
           "rename_confirmed": rename_confirmed,
           "mounts_moved": mounts_moved, "because": because,
           "note": f"{old_canonical} -> {new_canonical}: the uuid and every edge stay (a "
                   "compensating canonical_changed event, the old canonical an ALIAS "
                   "that resolves forever); every GOVERNING SEAT's own pin/"
                   "house/charter/office/repo-root-.osiris is cascaded (see manifest); "
                   "the project's own on-disk folder is not (manifest's own "
                   "could_not_reach names why; each seat's own 'tree' tier still "
                   "detects whether a renamed path already exists)",
           "possibly_stale_seats": stale,
           **prior_art_bits}


async def fork_project(
    actions: Actions, *, project: str, fork_into: str, because: str, actor: str,
) -> dict[str, Any]:
    """FORK: an earlier project-split specimen established the shape: a new sibling
    project, with the original left untouched. TWO objects, BOTH already active
    SoftwareProjects, this function never mints either side, reusing fold_project's own
    refusal shape deliberately (if the target doesn't exist yet, this is a RENAME, a
    different function for a different act). Mints ONE `forked_from` edge, `fork_into` ->
    `project` (the successor names its ancestor, the same direction convention
    succeeded_from already holds for an Agent lineage: heir -> ancestor).

    NO RELATIONSHIPS MOVE: the deliberate opposite of fold_project, every existing
    in_repo/works_in/governs edge on BOTH objects stays exactly where it is. The
    original project's in_repo edges were never meant to move; a fork records a NEW
    relationship between two objects that each keep their own, complete history.

    THE CALLER DECLARES; THIS FUNCTION NEVER INFERS, and it never runs
    project_identity_evidence itself. That report is read BEFORE this is called, by a
    human who then names both sides explicitly.

    Refuses LOUDLY on: a blank `because`; `project`==`fork_into`; either ref ambiguous
    (AmbiguousProjectRef) or unresolved; either not an ACTIVE SoftwareProject; a live
    `forked_from` edge already connecting this exact pair (idempotent refusal, never a
    duplicate mint)."""
    from src.orchestrator.projects import AmbiguousProjectRef, _resolve_software_project

    project = (project or "").strip()
    fork_into = (fork_into or "").strip()
    because = (because or "").strip()
    if not because:
        return {"error": "because is required: a fork is a declared act on the record"}
    if not project or not fork_into:
        return {"error": "fork_project needs both labels: project and fork_into"}
    if project == fork_into:
        return {"error": "project and fork_into name the same label; nothing to fork"}

    async def _resolve(ref: str) -> tuple[Any, dict[str, Any] | None]:
        try:
            got = await _resolve_software_project(actions.pool, ref)
        except AmbiguousProjectRef as amb:
            return None, {"error": f"{amb.ref!r} is ambiguous: {len(amb.candidates)} "
                                   f"active SoftwareProjects answer to it: "
                                   f"{', '.join(amb.candidates)}. Name the exact one "
                                   "(canonical or id); fork_project never guesses which."}
        return got, None

    proj_row, err = await _resolve(project)
    if err:
        return err
    into_row, err = await _resolve(fork_into)
    if err:
        return err
    if proj_row is None or into_row is None:
        missing = [label for label, row in ((project, proj_row), (fork_into, into_row))
                  if row is None]
        return {"error": f"unknown SoftwareProject(s): {', '.join(missing)}. "
                         "fork_project never invents either side; mint the successor as "
                         "a real project first before recording the succession"}
    if proj_row["status"] != "active":
        return {"error": f"{proj_row['canonical']} is {proj_row['status']}, not active"}
    if into_row["status"] != "active":
        return {"error": f"{into_row['canonical']} is {into_row['status']}, not active"}
    exists = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='forked_from' "
        "AND (valid_until IS NULL OR valid_until > now())", into_row["id"], proj_row["id"])
    if exists:
        return {"error": f"{into_row['canonical']} already carries a live forked_from "
                         f"edge to {proj_row['canonical']}; nothing to do"}
    now = datetime.now(UTC)
    await actions.create_link(into_row["id"], proj_row["id"], "forked_from", actor, now,
                              _CONF, properties={"because": because}, evidence_class=_EC)
    return {"forked_from": proj_row["canonical"], "into": into_row["canonical"],
           "because": because,
           "note": "no relationships moved: every existing in_repo/works_in/governs edge "
                   "on both objects stays exactly where it is"}


async def unfork_project(
    actions: Actions, *, project: str, fork_into: str, because: str, actor: str,
) -> dict[str, Any]:
    """Invalidate a live `forked_from` edge, the compensating-event complement to
    fork_project, same shape as seats.py's unpeer over peer_of. REVERSIBILITY PROVEN,
    not claimed: since fork_project never moves any relationships, there is nothing to
    move BACK either. The whole reversal is this one healed edge, by design.

    Refuses LOUDLY on: blank `because`; either ref unresolved to a SoftwareProject
    (ambiguity is refused the same way, though an already-forked pair is by definition
    unambiguous, both sides were named explicitly to create the edge); or no active
    `forked_from` edge from `fork_into` to `project`."""
    from src.orchestrator.projects import AmbiguousProjectRef, _resolve_software_project

    because = (because or "").strip()
    if not because:
        return {"error": "because is required: unforking is a deliberate act on the "
                         "record"}

    async def _resolve(ref: str) -> tuple[Any, dict[str, Any] | None]:
        try:
            got = await _resolve_software_project(actions.pool, ref)
        except AmbiguousProjectRef as amb:
            return None, {"error": f"{amb.ref!r} is ambiguous: {len(amb.candidates)} "
                                   f"active SoftwareProjects answer to it: "
                                   f"{', '.join(amb.candidates)}. Name the exact one "
                                   "(canonical or id); unfork_project never guesses "
                                   "which."}
        return got, None

    proj_row, err = await _resolve((project or "").strip())
    if err:
        return err
    into_row, err = await _resolve((fork_into or "").strip())
    if err:
        return err
    if proj_row is None or into_row is None:
        missing = [label for label, row in ((project, proj_row), (fork_into, into_row))
                  if row is None]
        return {"error": f"unknown SoftwareProject(s): {', '.join(missing)}"}
    link = await actions.pool.fetchrow(
        "SELECT from_id, to_id FROM links WHERE type='forked_from' "
        "AND from_id=$1 AND to_id=$2 "
        "AND (valid_until IS NULL OR valid_until > now())", into_row["id"], proj_row["id"])
    if link is None:
        return {"error": f"{into_row['canonical']} carries no live forked_from edge to "
                         f"{proj_row['canonical']}; nothing to unfork"}
    now = datetime.now(UTC)
    await actions.invalidate_link(link["from_id"], link["to_id"], "forked_from", actor, now)
    return {"unforked": proj_row["canonical"], "was_into": into_row["canonical"],
           "because": because}


# --- create_project (the CREATE half of the project-identity function set) --------------

async def create_project(
    actions: Actions, *, name: str, because: str, actor: str,
) -> dict[str, Any]:
    """Declare a NEW SoftwareProject: the deliberate, user-facing CREATE path, built not
    as a separate mint mechanism but as a thin wrapper over two guards this codebase
    already built and validated: an earlier validated choke point
    (`_validate_repo_name`/`_resolve_repo`, capture.py, reused verbatim, refuses a
    path-shaped or malformed `name` before any object is touched) LAYERED WITH a
    case-insensitive de-dup (a real case-collision specimen between two differently
    cased project names is the live proof shape-validation alone doesn't catch a case
    collision: the first guard validates SHAPE, the second prevents CASE-COLLISION
    duplicates, complementary, not redundant).

    Refuses LOUDLY on: a blank `because` (the same mandatory-testimony discipline
    rename_project/fork_project/unfork_project already hold, creating a project is
    testimony too) or a malformed/path-shaped `name`.

    NEVER MINTS A DUPLICATE: if `name` already resolves, by exact canonical or
    `name`-property match (`_resolve_repo`) OR a case-insensitive canonical match, the
    EXISTING object is returned, `created=False`, never a fresh mint. A genuinely new
    name (zero matches either way) mints fresh and stamps `because` as founding
    testimony, same as `_mint_or_find_repo`'s own name-property assert."""
    from src.orchestrator.capture import _resolve_repo, _validate_repo_name

    because = (because or "").strip()
    if not because:
        return {"error": "because is required: creating a project is a deliberate act "
                         "on the record"}
    raw = (name or "").strip()
    stripped = raw.removeprefix("repo:").strip()
    try:
        _validate_repo_name(stripped, raw)
    except ValueError as exc:
        return {"error": str(exc)}
    existing = await _resolve_repo(actions.pool, stripped)
    if existing is None:
        twins = await actions.pool.fetch(
            "SELECT id FROM objects WHERE type='SoftwareProject' AND status='active' "
            "AND lower(canonical) = lower($1)", f"repo:{stripped}")
        if len(twins) == 1:
            existing = twins[0]["id"]
    if existing is not None:
        row = await actions.pool.fetchrow("SELECT canonical FROM objects WHERE id=$1",
                                          existing)
        return {"canonical": row["canonical"], "created": False,
                "note": "already exists: reused, never minted as a duplicate"}
    now = datetime.now(UTC)
    proj = await actions.create_or_find_object("SoftwareProject", f"repo:{stripped}", actor)
    await actions.assert_property(proj, "name", stripped, actor, now, _CONF,
                                  evidence_class=_EC)
    row = await actions.pool.fetchrow("SELECT canonical FROM objects WHERE id=$1", proj)
    return {"canonical": row["canonical"], "created": True, "because": because}
