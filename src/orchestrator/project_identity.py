"""PROJECT IDENTITY — the read-only evidence resolver (#110, decision 1db1ff41, Thoth's
dispatch DM 2427/2435, built first and alone per his explicit order).

Before a rename or a fork can be DECLARED, someone has to see what the evidence actually
says, tier by tier, without any one tier being crowned first for every case. Thoth's own
proposed "the git remote is authoritative when it exists" rule correctly resolves xxit,
tony, RAMstein and ByeByte — and fails completely on ballgem, John's real working repo,
which has no origin remote at all (Sekhmet, decision e221128e). No single evidence source
ranks first across the population: remote is authoritative for xxit and blind for ballgem;
write-attribution is authoritative for ballgem and blind for xxit (100% of xxit's own work
is filed under the stale name, which is why it went undetected for weeks). This module
supplies the naming/marking machinery that trap demands — NEVER a fixed precedence pick —
and reuses Sekhmet's own five-tier catalog (4e2cfeb6/e221128e) rather than inventing new
ones:

  OPERATOR_CONFIRMED — a caller-SUPPLIED citation (a decision id / quote). This function
    never parses decision prose looking for a quote; that is a human's read, not a query,
    and pretending otherwise would be the same silent-pick failure this module exists to
    refuse. Reported back verbatim, `checked=True`, so a caller who already did that
    reading gets it folded into one report rather than a second lookup.

  DECLARED_CHARTER — a seat's own governs edges, checked from BOTH origins: the Seat
    object directly (ruling 1db1ff41's ruling 3, the re-key this whole primitive depends
    on) AND every Agent lineage that has ever held the seat (ruling 3 shipped the schema
    and code, b9b5ce9, but `migrate_charter_to_seat` is dry-run-only as of this build —
    every live governs edge today is still Agent-typed; the day it flips this tier keeps
    working unchanged, because it already checks both).

  SELF_AUTHORED — EXISTENCE only, never content: a seat's own CLAUDE.md/charter.md is
    reported by path/size/mtime. Reading its prose for a project claim is the same
    human-judgment problem as tier 1, not a query this function pretends to answer.

  PIN — the seat's own `.osiris` project= label, read at its office (anchor_cwd).

  REMOTE — `git remote get-url origin`, run against each CANDIDATE project's registered
    on_disk_path (census_trees's own stored fact, never a guessed root — the same
    discipline discover_trees already holds). A seat's own anchor_cwd is its OFFICE, not
    its code repo (mintseat.py/greatfold.py: anchor_cwd is always the office path), so
    remote is checked per-candidate against the graph's on_disk_path, not against the
    seat directly.

  WRITE_ATTRIBUTION (DERIVED, weakest, flagged explicitly wherever it's the only signal) —
    the majority in_repo target across every Thread/Decision this seat's lineage has ever
    filed, the ballgem tier by necessity.

`project_identity_evidence` never writes; `rename_project` and `fork_project` below are
the two DECLARED-succession verbs a caller invokes once a human has read that report and
made the call it can't make for them (correct_project_name, the third writer and the one
delegated exception that IS self-authorizing, lives in projects.py beside retire_project/
fold_project — the sibling lifecycle verbs it belongs with).

RENAME vs FORK are different acts (ruling 1db1ff41): rename keeps ONE object's stable
`canonical` id and only ever changes its mutable `name` property (old value stays in
assertion history, never deleted — the same discipline rename_seat already holds for a
seat's handle); fork connects TWO objects that already exist with a `forked_from` edge and
moves no estate at all. THE CALLER DECLARES WHICH BY CALLING THE RIGHT FUNCTION — there is
no shared `kind=` parameter to get wrong, because the codebase's own idiom (retire_project/
fold_project/peer_seats/rename_seat/correct_house) is one verb, one act, and Thoth ruled
that shape over a mode switch for exactly this reason (DM 2435): a declaration hidden
behind an argument default is not a declaration.
"""
from __future__ import annotations

import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

_EC = EvidenceClass.SELF_DECLARED.value
_CONF = confidence_for(EvidenceClass.SELF_DECLARED)

# A DECLARED RENAME MUST OUTRANK AN ORDINARY MOUNT'S OWN GUESS (thread 04907b12, live
# specimen: repo:xxit renamed to handlingtheloop, then an ordinary metron/deckard mount
# re-asserted "xxit" at the SAME self_declared/0.9 confidence rename_project itself used
# — current_assertions' tie-break (confidence DESC, observed_at DESC) then falls through
# to pure recency, so a later, uninformed mount silently overturned a deliberate,
# testimony-backed rename). Written at the SAME evidence_class (still a self-declaration,
# never a different KIND of claim) but a confidence strictly above the 0.9 ceiling every
# ordinary self_declared write is capped at (BASE_CONFIDENCE[SELF_DECLARED]) — the exact
# "operator-ruling"-source pattern already used elsewhere in this graph (seat_generation
# backfills at 0.95-0.99) for a declared act that must never lose a tie to routine traffic.
_RENAME_CONF = 0.95


def _remote_basename(url: str | None) -> str | None:
    """The repo name a remote URL implies, for comparison against a bare project label —
    'git@github.com:x/handlingtheloop.git' and 'https://github.com/x/handlingtheloop' both
    read as 'handlingtheloop'. None in, None out."""
    if not url:
        return None
    tail = url.rstrip("/").rsplit("/", 1)[-1]
    if ":" in tail and "/" not in url.rstrip("/").rsplit("/", 1)[-1]:
        tail = tail.rsplit(":", 1)[-1]
    return tail.removesuffix(".git") or None


def _git_remote(path: str) -> tuple[bool, str | None]:
    """(is_a_git_repo, origin_url_or_None) at `path` — same subprocess.run/try-except
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
        return True, None  # a real repo, just no `origin` configured — ballgem's own shape


def worktree_parent_path(path: str) -> str | None:
    """WORKTREES AS A FIRST-CLASS SHAPE (thread 922d920c): `path`'s own MAIN checkout —
    None when `path` is not a git worktree at all (including "not a git repo" and "IS the
    main checkout"). `--git-common-dir` is always the shared `.git` (worktree or not);
    `--git-dir` is the PRIVATE one a worktree gets (`<main>/.git/worktrees/<name>`) — they
    agree exactly when `path` is the main checkout itself, so comparing them (never
    hand-parsing the `.git` FILE's own `gitdir: ...` pointer text, which can be relative or
    absolute and is git's own implementation detail, not a contract) is the one signal that
    is both necessary and sufficient. The parent's root is `--git-common-dir` with its own
    trailing `/.git` stripped — never re-derived from `--show-toplevel` run FROM `path`,
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
        return None  # an unexpected shape (bare repo, submodule) — refuse rather than guess
    return str(common_resolved.parent)


def git_current_branch(path: str) -> str | None:
    """The checked-out branch at `path`, or None on a detached HEAD or any git failure —
    same never-raise shape as `_git_remote`."""
    try:
        out = subprocess.run(["git", "-C", path, "rev-parse", "--abbrev-ref", "HEAD"],
                             capture_output=True, text=True, timeout=5, check=True)
    except (OSError, subprocess.SubprocessError, subprocess.CalledProcessError):
        return None
    branch = out.stdout.strip()
    return branch if branch and branch != "HEAD" else None


async def project_name_for_disk_path(pool: asyncpg.Pool, path: str) -> str | None:
    """The registered SoftwareProject `name` for a checkout at `path` — read off its own
    `on_disk_path` assertion, never re-derived from the directory basename (the same
    binding-line discipline `census_trees`'s rename fix already established: a renamed
    directory keeps its OLD name/canonical on purpose). None when nothing at this exact
    path is registered — this never mints, never guesses a nearby path, it is a plain
    lookup for `project_of`'s own worktree rung (thread 922d920c): a worktree whose
    parent checkout the disk census hasn't reached yet correctly falls through to
    charter/lineage instead of a fabricated answer."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT (SELECT a2.value #>> '{}' FROM current_assertions a2 "
        " WHERE a2.object_id=o.id AND a2.name='name' "
        " ORDER BY a2.confidence DESC, a2.observed_at DESC LIMIT 1) "
        "FROM objects o JOIN current_assertions a ON a.object_id=o.id "
        "WHERE o.type='SoftwareProject' AND o.status='active' AND a.name='on_disk_path' "
        "AND a.value #>> '{}' = $1 LIMIT 1", path)


def _self_authored(office: str | None) -> dict[str, Any]:
    """Existence/path/size/mtime for a seat's own CLAUDE.md and charter.md at its office —
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
    active, `holds` link history) — write-attribution and the still-live Agent-origin
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
    """A SoftwareProject's CURRENT display label — its live `name` property when one
    exists, falling back to the canonical's own bare form only for an object that never
    got a `name` asserted at all. Every candidate key in this module goes through this:
    rename_project changes ONLY the `name` property, never `canonical` (the same
    discipline that keeps every existing edge correct without re-pointing) — a reader
    keyed on canonical instead would report the OLD label forever after every future
    rename. Caught live: re-running this tool against deckard/metron right after
    renaming xxit->handlingtheloop still reported 'xxit' with the remote 'disagreeing',
    when the rename had already made them agree — the read-back that was supposed to
    CONFIRM the rename would have reported it as still broken.

    Type widened to accept a raw `asyncpg.Connection` too (decision 6b4d185e's fourth/
    fifth callers) — same `fetchval` surface either way, one implementation for both."""
    name = await pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='name' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", oid)
    return str(name) if name else str(canonical).removeprefix("repo:")


async def charter_display_label(pool: asyncpg.Pool | asyncpg.Connection, entry: str) -> str:
    """A charter entry (`charter_of`'s own canonical-only output) rendered for a HUMAN,
    never for a comparison — Thoth/Deckard, mail 8788, the presentation half of the
    5031a74 finding: charter_of and set_charter are correct to operate in canonical
    space forever (that's WHY a rename can never need to touch a governs edge), but a
    human reads offices, not canonicals, and an office's own "You govern: `xxit`" line
    after a real rename to "handlingtheloop" reads as stale even though the graph is
    exactly right. Resolved LIVE at render time (never cached, never written anywhere)
    so a rename shows through without any cascade touching the charter itself —
    "handlingtheloop (repo:xxit)" when the two differ, or a bare "xxit" (no redundant
    parenthetical) when the project's live name still equals its own canonical. Degrades
    to the bare entry on any failure (577988ed — a presentation refinement must never be
    the reason a charter line goes blind)."""
    bare = entry.removeprefix("repo:")
    try:
        from src.orchestrator.capture import _resolve_repo

        oid = await _resolve_repo(pool, bare)
        if oid is None:
            return bare
        canon = await pool.fetchval("SELECT canonical FROM objects WHERE id=$1", oid)
        label = await _live_label(pool, oid, str(canon))
    except Exception:  # noqa: BLE001 — see note above
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
    label (obligation a980aff2, henry->shellbiz): a label that has since been FOLDED into
    another must compare as the SURVIVOR, the same live label `_write_attribution`'s own
    in_repo-edge lookup already reports for the write side — otherwise every fold produces
    a permanent false 'disagrees' for any caller comparing against it.

    MOVED HERE FROM agents.py (decision 540007ca's sibling finding), NOT REWRITTEN: this
    module already hosts `_live_label`, the label-resolution primitive every candidate in
    this file goes through, and by the time a THIRD caller needed the same "resolve
    through the fold" logic (project_identity_evidence's own `pin`, this module),
    keeping a second private copy in agents.py would have been the disease one level up
    from the one being fixed — three call sites is the threshold past which "just copy
    it" stops being cheaper than a shared home.

    WIDENED FOR A FOURTH AND FIFTH CALLER (decision 6b4d185e — settle.py's
    filed_under_check, agents.py's misfiled_by_lineage), NOT A SIXTH COPY: the chain-walk
    itself is now inlined here rather than delegated to `Actions.resolve_object_id`,
    because filed_under_check's own `conn_or_pool` accepts EITHER an `asyncpg.Pool` (the
    /settle MCP tool) OR a raw `asyncpg.Connection` (the Stop hook, which cannot hold a
    pool across its ~1s budget, module docstring) — `Actions` requires a real Pool to
    `.acquire()` from, so wrapping a bare Connection in one would break the hook's own
    caller. Both types expose the same `fetch`/`fetchval` surface this walk needs, so one
    implementation genuinely serves every caller. Behavior is otherwise verbatim: same
    100-iteration cycle guard, same confess-don't-guess refusal.

    Never guesses: the label names its own SoftwareProject by EXACT canonical only
    (case-insensitive, matching `_resolve_or_mint_project`'s own lookup) — no string
    similarity, no directory-name fallback (13af22fc's phantom-repo defect). A label that
    names no object returns itself unchanged, second element None. A chain too deep to
    resolve (a cycle) is NEVER silently picked through — the original label comes back
    untouched alongside a confession string for the caller to surface, never a guessed
    winner (this house names disagreement, it never crowns a side).

    A label naming an object that was never MERGED (winner == the object itself) still
    goes through `_live_label` before returning (7f90f394): `rename_project` changes
    only the `name` property, never `canonical` — the OLD label the caller passed in
    still resolves here (canonical is forever), but comparing that stale label against
    the object's live remote/pin/charter forever reads as a false disagreement after a
    plain rename with no fold involved at all, the exact xxit/handlingtheloop specimen
    `_live_label`'s own docstring already names for the merged case. Only a genuinely
    stale label is translated; a label that already matches the object's own live name
    passes through unchanged either way.

    EXACT MATCH FIRST, CASE-INSENSITIVE ONLY AS A REFUSAL-GATED FALLBACK (a real bug
    caught tonight by a deployed suite going intermittently red — Thoth's own catch): a
    case-insensitive-only lookup here is AMBIGUOUS whenever two case-variant objects
    coexist for the same label, exactly the RAMstein/ramstein shape this whole reign has
    been about. With no `ORDER BY`, `.fetchrow()` on such a query nondeterministically
    returns EITHER object — sometimes the dupe (chain-walks correctly), sometimes the
    survivor itself (whose own merged_into is None, so the walk trivially returns
    unchanged, silently no-op'ing the very normalization this function exists to do).
    Exact canonical match is deterministic and resolves this specimen outright; the
    case-insensitive fallback only fires when nothing matches exactly, and only trusts it
    when it resolves to exactly one candidate — an ambiguous fallback confesses rather
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
                           "differ only by case — compared unnormalized rather than "
                           "guessing which one this label means")
        row = candidates[0] if candidates else None
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
                       "merged_into edge) — compared unnormalized rather than guessing a "
                       "winner")
    if winner == row["id"]:
        return await _live_label(conn_or_pool, winner, row["canonical"]), None
    canon = await conn_or_pool.fetchval("SELECT canonical FROM objects WHERE id=$1", winner)
    return await _live_label(conn_or_pool, winner, canon), None


async def _declared_charter(pool: asyncpg.Pool, seat_id: str, seat_oid: Any,
                            bases: list[str]) -> list[str]:
    """governs targets, checked from BOTH origins (module docstring) — live display
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
    write-attribution names — DERIVED, the weakest tier, John/ballgem's own evidence
    (145 of 163) when nothing else has signal at all. Keyed by live display label (see
    `_live_label`), not canonical. LIVE edges only (`valid_until`): fold_project heals
    an estate by invalidating the old in_repo edge and creating a fresh one on the
    surviving object — counting both would double-count exactly the writes a fold was
    just run to consolidate, showing a split that no longer exists."""
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
    one's answer plus per-candidate agreement/disagreement — NEVER picking a winner. See
    the module docstring for the five tiers and why no fixed precedence list can work
    across the population (xxit/ballgem are each other's counter-example).

    Every distinct project label surfaced by ANY tier becomes a CANDIDATE row, cross-
    checked against every other tier that can speak to it (declared_charter membership,
    pin match, write-attribution share, and — via the candidate's own registered
    on_disk_path, never a guessed root — a live git remote check). A seat with zero
    holders ever (bases == []) still runs: pin/remote/self-authored are cwd-based, not
    lineage-based, and John-shaped cases (a seat whose whole history predates the Seat
    object) must not go blind just because `bases` came back empty."""
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
            # NORMALIZE THROUGH merged_into (decision 540007ca's sibling finding,
            # confirmed independently by Till): a pin naming a label that has since been
            # FOLDED into another must compare as the SURVIVOR, or it shows up as its OWN
            # stale candidate — pin_match true only against itself, declared_charter/
            # remote_agrees reading the LOSER's own stale properties, "agreement" landing
            # on 'disagree' even after the operator corrected the pin. Degrades to the
            # raw pin on any failure (577988ed — a diagnostic refinement must never be
            # the reason this read goes blind).
            try:
                pin, pin_merge_confession = await _normalize_project_label_through_merge(
                    pool, pin)
            except Exception:  # noqa: BLE001 — see note above
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
            # PRESENTATION, NEVER THE COMPARISON (Thoth/Deckard, mail 8788): `name` stays
            # the raw canonical every comparison field below keys and compares on —
            # `display` adds the name-with-canonical rendering a human reading this
            # receipt actually wants, resolved live.
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

    # write_attribution only counts as SUPPORT for the majority target — a single stray
    # commit filed under the wrong project is not the same claim as 145/163 (ballgem)
    supported = {n for n, e in candidates.items()
                if e["declared_charter"] or e["pin_match"] or e["remote_agrees"]
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
    """The NAMED SIGNAL rename_project's pre-write check surfaces (#137's own arc, the
    operator's ruling: DO NOT CROWN A TIER — wire the evidence in, pick nothing).
    `evidence` is one seat's own `project_identity_evidence` report. Three answers, never
    a silent pick between them (the same law as the coordination-lane's tri-state, ruling
    f624d114):

      "no-signal"  — this seat's evidence found no candidate at all (no charter, no pin,
        no attributed work), OR found candidates but none carries real positive signal
        (declared_charter/pin_match/remote_agrees — write_attribution alone never counts,
        a single stray commit is not the same claim `agreement` already warns against).
      "confirms"   — `new_name` is the seat's ONE AND ONLY strongly-evidenced candidate —
        reuses `evidence["agreement"] == "single-candidate"` directly, never a second,
        independently-drifting copy of that computation (ruling 70493925's own warning).
      "disagrees"  — the seat's evidence disagrees WITH ITSELF (`agreement == "disagree"`,
        more than one candidate carries real signal) regardless of whether `new_name` is
        one of the rivals, OR the seat's one strongly-evidenced candidate is something
        other than `new_name`. LIVE-VERIFIED SPECIMEN (seat:ddafff44/khepri, project
        repo:tony, 2026-08-13): remote_agrees + write_attribution both back
        "cultural-infrastructure" (`new_name` itself, strongly evidenced) while the
        seat's OWN PIN still says "tony" — a naive "does new_name have signal" check
        would have called this "confirms" and buried the exact stale-pin disagreement
        #137 exists to catch. Ambiguity IS the finding here, not a tiebreak new_name wins
        by having the stronger case."""
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


# --- rename_project / fork_project (#110, decision 1db1ff41, rulings 1-2) -----------------

def _dir_exists(target: str) -> bool:
    """A plain sync helper (ASYNC240: file I/O stays out of async function bodies, same
    convention identity_heal.py's own `_office_dir_exists`/trigger.py's `_tree_exists`
    already document) — the tree-binding tier below only ever DETECTS, never provisions."""
    return Path(target).is_dir()


async def _cascade_governing_seats(
    pool: asyncpg.Pool, *, project_oid: Any, old_name: str, new_name: str,
    because: str, actor: str, dry_run: bool,
) -> dict[str, Any]:
    """THE RENAME CASCADE ITSELF (dispatch 2589353a, operator 2026-09-07 verbatim: "there
    has to be a verb that links the rename mechanically so agents don't get lost, it's an
    osiris problem"). For every seat governing the renamed project, reaches five tiers —
    pin, house, charter, office render, tree binding — with the CASCADE'S OWN elevated
    authority, never the caller's own: a project rename is a project-level act, not a
    manager-subordinate one, so it calls the SAME "not headship-gated, callers
    responsible" third-party doors this house already built for exactly this shape
    (`correct_pin_value_third_party`, `resync_seat_house_third_party` — task #152/
    fff496fe22b0's own precedent pair) plus the two doors that were never gated at all
    (`set_charter`, `reissue_office` — Khnum's parallel fba386dc lane is what makes
    `set_charter` atomic-with-read-back; this calls the identical function, no new one).

    NEVER SILENCE ON A PARTIAL RESULT: returns a MANIFEST, every tier named 'touched' /
    'already-correct' / 'could-not' per seat, so a caller sees the exact remainder
    instead of a receipt that looks complete when it isn't. `dry_run=True` previews every
    tier via each door's own peek/dry-run shape without writing anything; `dry_run=False`
    executes them, one tier's failure caught and reported rather than aborting the rest
    (a rename cascade that stops at the second seat because the first seat's office had
    no CLAUDE.md would be strictly worse than a graph-only rename).

    NEVER GUESSES A FILESYSTEM MOVE: tree binding is DETECT-ONLY — this verb reports
    when a seat's `tree_cwd` still names the old label and whether a same-shaped new-
    named path exists on disk, but never calls `bind_seat_tree` itself; inferring and
    rebinding a code checkout's location is a deliberate act a human confirms, not
    something a name-property write should trigger sight-unseen. Same law for the two
    tiers this cascade can never reach at all: the project's own on-disk folder (never
    moved by any Osiris verb, ff3bdc37) and the repo's own root `.osiris` file (a
    third, distinct copy from any of a seat's own office/anchor/workspace pins that
    `correct_pin_value_third_party` already reaches — no sanctioned write door exists
    for it yet) — both named honestly in `could_not_reach`, never silently skipped."""
    from src.orchestrator.boot_compiler import reissue_office
    from src.orchestrator.capture import _resolve_repo
    from src.orchestrator.charter import charter_of, set_charter
    from src.orchestrator.offices import correct_pin_value_third_party
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

        # PIN — correct_pin_value_third_party already reaches all three copies
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
                # TOP LEVEL and the anchor/workspace copies nested under their own keys —
                # a seat with no conventional office (only a real anchor/workspace copy,
                # a live shape this house already treats as legitimate, #199) sees a
                # top-level `error` from the office attempt alone even when the copy the
                # PLAN actually named got written. Success is "any copy the plan named
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
        except Exception as exc:  # noqa: BLE001 — one tier's failure must never sink the rest
            tiers["pin"] = {"status": "could-not", "detail": str(exc)}

        # HOUSE — only touch when it currently names the OLD label; a house a seat
        # legitimately shares with siblings under a third name is never this cascade's
        # to overwrite (house is DERIVED, ruling ff6148b0 — not every seat's house IS
        # its project name).
        house = facts.get("house")
        if house == new_name:
            tiers["house"] = {"status": "already-correct"}
        elif house != old_name:
            tiers["house"] = {"status": "already-correct",
                              "note": f"house={house!r} names neither {old_name!r} nor "
                                      f"{new_name!r} — left untouched, not this cascade's "
                                      "to guess"}
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

        # CHARTER — set_charter replaces the WHOLE list; RESOLVE each entry against the
        # PROJECT'S OWN ID, never a literal string match against old_name/new_name.
        # `charter_of` derives its entries from live `governs` edges' own canonical —
        # which is IMMUTABLE (rename_project's docstring: canonical never changes, only
        # the `name` property does) — so an entry can be the project's mint-time label
        # from a PRIOR rename, neither this call's old_name nor its new_name, and a bare
        # string compare silently no-ops exactly when the charter is most stale
        # (Deckard's/Metron's own specimen, decisions 0afe7d35/76559373: "xxit" never
        # matches either side of a later handlingtheloop->something rename). `old_name`
        # is a permanent read alias forever (this house's own convention), so the only
        # honest test is "does this entry resolve to the SAME project id being renamed"
        # — the identical write-vs-read-key split idiom PIN/HOUSE above already hold.
        # RUNS REGARDLESS OF new_name == old_name (correction, Thoth's own dry-run
        # catch on Deckard's re-run, mail 8678): a self-rename is exactly how this
        # verb's own repair population gets invoked — an operator calling rename(X, X)
        # on purpose to HEAL a stale alias, not a no-op to protect. Skipping the resolve
        # loop on new_name==old_name silently no-op'd the very specimen this tier exists
        # to fix. A genuinely vacuous self-rename (no stale alias present at all) still
        # naturally reports "already-correct" below — nothing in `stale` — so removing
        # the guard changes nothing for that case, only unblocks the real one.
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
                # proof a write is coming (Thoth/Deckard, mail 8749, Deckard's real run
                # on 7b709ce: dry run promised plan ['xxit'] -> ['handlingtheloop'], the
                # real apply's own set_charter detail showed added:[], removed:[] — "the
                # manifest now reports a write it did not make"). `stale` only tests
                # "isn't literally new_name," but `new_name` ALWAYS resolves back down
                # to project_oid's own ETERNAL canonical (rename_project's contract:
                # canonical never changes, only the mutable `name` property does) — the
                # exact same canonical `stale`'s own entries already are. Mirroring
                # set_charter's real resolve-then-diff (charter.py) here, using
                # project_oid's own canonical directly for the `new_name` candidate
                # (rather than resolving it — on a dry run the name-property write
                # hasn't landed yet, so `new_name` cannot resolve to anything), makes
                # PLAN and APPLY predict the identical outcome regardless of dry_run —
                # a plan can never again promise a correction set_charter's own diff
                # then silently no-ops.
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
                        continue  # mirrors set_charter's own `rejected` — dropped, not kept raw
                    resolved_canons.add(str(await pool.fetchval(
                        "SELECT canonical FROM objects WHERE id=$1", cand_id)
                        ).removeprefix("repo:"))
                real_added = sorted(resolved_canons - set(current_charter))
                real_removed = sorted(set(current_charter) - resolved_canons)
                if not real_added and not real_removed:
                    # THE PHANTOM-WRITE CASE, NOW HONEST: every entry `stale` flagged
                    # resolves right back to the SAME canonical it already was — nothing
                    # set_charter could ever actually add or remove, structurally, for
                    # a plain rename (never a fold — no second object exists to swap
                    # to). Reporting "touched" here was the lie; "already-correct" is
                    # what the graph — and set_charter's own real diff — would show.
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
                # VERIFIED-EQUAL (Deckard's addendum, mail 8687): current_charter
                # genuinely contains new_name's own literal string — a real check,
                # a real match, never a guess.
                tiers["charter"] = {"status": "already-correct"}
            else:
                # NOT-EVALUATED, NEVER "ALREADY-CORRECT" (Deckard's own words: "the
                # lie") — structurally this should not fire for any seat THIS LOOP
                # ever reaches: `governing` above is queried by an active `governs`
                # edge to project_oid, so charter_of(seat_id) is guaranteed to
                # return at least one entry whose own canonical IS project_oid's own
                # canonical, which resolves to project_oid trivially and always
                # lands in `stale` above unless it already equals new_name. Reaching
                # this branch at all means something is wrong beneath the surface
                # (a governs edge outliving its own target row, a resolver failure) —
                # distinct status so a caller/test can tell "verified, matches" from
                # "the check itself found nothing to compare," never conflating the
                # two under one green word.
                tiers["charter"] = {"status": "not-evaluated",
                                    "note": "charter names neither the old nor the new "
                                            "label, and no entry resolved to this "
                                            "project — left untouched; this should not "
                                            "happen for a seat with an active governs "
                                            "edge to the renamed project"}
        except Exception as exc:  # noqa: BLE001
            tiers["charter"] = {"status": "could-not", "detail": str(exc)}

        # OFFICE RENDER — recompile CLAUDE.md so it reflects whatever charter/house
        # this same call already corrected above (runs after, on purpose). GATED ON
        # UPSTREAM ACTUALLY CHANGING (Thoth/Deckard, mail 8749, second observation:
        # "office reports touched on every run even when nothing upstream changed; it
        # should be already-correct when charter and house are verified-equal") — this
        # used to reissue unconditionally whenever an office file exists, minting a new
        # version every call even when pin/house/charter all read "already-correct" —
        # a real write (a fresh version stamp) recompiling byte-identical content,
        # which is a "touched" every bit as phantom as the charter bug this same
        # dispatch reported. Skip the reissue entirely when nothing upstream this loop
        # touched actually changed.
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

        # TREE BINDING — DETECT ONLY, never written (see docstring above).
        tree_cwd = facts.get("tree_cwd")
        if tree_cwd and f"/{old_name}" in tree_cwd:
            candidate = tree_cwd.replace(f"/{old_name}", f"/{new_name}")
            if not _dir_exists(tree_cwd) and _dir_exists(candidate):
                tiers["tree"] = {
                    "status": "could-not",
                    "detail": f"tree_cwd {tree_cwd!r} no longer exists on disk and "
                             f"{candidate!r} does — this cascade never rebinds a tree "
                             "automatically; confirm it, then call "
                             f"seat(action='bind_tree', seat_id={seat_id!r}, "
                             f"tree_cwd={candidate!r}) yourself"}
            else:
                tiers["tree"] = {
                    "status": "could-not",
                    "detail": f"tree_cwd {tree_cwd!r} references the old name — this "
                             "cascade never moves or infers folders, only detects"}
        else:
            tiers["tree"] = {"status": "already-correct"}

        seats_out[seat_id] = tiers

    return {
        "seats": seats_out,
        "could_not_reach": {
            "folder_path": "the project's on-disk directory is never moved by this "
                           "verb — mv it yourself first if the rename should follow "
                           "the code",
            "repo_root_osiris": "a repo's own .osiris pin file at its root (distinct "
                                "from any seat's own office/anchor/workspace pin "
                                "copies) has no sanctioned write door yet — correct "
                                "it by hand",
        },
    }


async def rename_project(
    actions: Actions, *, project: str, new_name: str, because: str, actor: str,
    dry_run: bool = True, merge_into: bool = False,
) -> dict[str, Any]:
    """RENAME: ONE object keeps its stable `canonical` id FOREVER — even though
    SoftwareProject's canonical happens to be name-shaped (`repo:<name>`), it is treated
    exactly as immutable as Seat's uuid-based `seat:<id>` is everywhere else in this
    codebase; nothing here, or anywhere, ever rewrites `objects.canonical`. Only the
    mutable `name` PROPERTY changes — a fresh `assert_property`, old values kept forever
    in assertion history (never deleted), the same discipline rename_seat already holds
    for a seat's handle. `_resolve_software_project`'s name-property fallback is what
    makes the NEW name resolvable afterward; the OLD canonical string keeps resolving
    too, forever — a rename never orphans either name.

    ZERO graph EDGES move: works_in/governs/in_repo already point at this object's
    stable `id`, not at its name or canonical, so every existing edge stays correct
    automatically — unlike fold_project, which re-points an estate because it retires a
    SECOND object. Only `agent_mounts.project` (a loose string column, never an FK — the
    same shape fold_project's own estate move already handles) is re-addressed old bare
    canonical -> new_name, so a fresh mount under the corrected name resolves cleanly.

    THE CASCADE (dispatch 2589353a, operator 2026-09-07: "there has to be a verb that
    links the rename mechanically so agents don't get lost, it's an osiris problem"):
    this verb is no longer graph-only. Every SEAT governing this project (a live
    `governs` link) has its own pin/house/charter/office cascaded under this verb's
    OWN elevated authority — see `_cascade_governing_seats` — never left to drift the
    way Deckard's/Metron's own specimens did (decisions 0afe7d35/76559373). The
    receipt's `manifest` names every tier `touched`/`already-correct`/`could-not`, per
    seat, so a partial cascade hands back the exact remainder rather than silence.
    OUT OF SCOPE STILL, named honestly rather than silently skipped (the same
    discipline rename_seat holds for the harness window title it cannot reach): the
    project's own on-disk folder (never moved by any Osiris verb) and the repo's own
    ROOT `.osiris` file (a third copy, distinct from any seat's own office/anchor/
    workspace pins) — both named in the manifest's own `could_not_reach`.

    THE CALLER DECLARES; THIS FUNCTION NEVER INFERS (ruling 1db1ff41, verbatim:
    "declared, all roads lead to explicit"). `because` is mandatory — a rename is
    testimony, same discipline rename_seat/correct_house already hold — this function
    does no evidence-gathering of its own; that is project_identity_evidence's job,
    run BEFORE this is called, by a human who read its report.

    PRIOR-ART SURFACED, NEVER REFUSED (obligation e4612853's sibling, ruling 38c71544's
    family — the bytebye/byebyte incident, decision 1db87191's own ruling silently
    overturned by a later, uninformed agent's re-assertion): the receipt's own
    `prior_art`/`prior_art_flag` keys, when present, name a standing Decision that may
    already cover this exact rename — the same search()-based guard record_decision
    already runs on itself, generalized here. This CANNOT tell a deliberate correction
    of an earlier decision from an uninformed overwrite of one — it does not try to;
    it only ensures the write does not land silently unread.

    Refuses LOUDLY on: a blank `new_name` or `because`; an unresolved or ambiguous
    `project` ref (AmbiguousProjectRef, named exactly like every other project verb);
    a non-active project; `new_name` already resolving to a DIFFERENT SoftwareProject
    OF ANY STATUS — active, retired, or already-merged (a real collision, never
    silently merged — fold_project is the deliberate, evidence-gated verb for that) —
    unless `merge_into=True` is passed explicitly, acknowledging the caller has already
    seen the collision and means to reuse the name anyway (this still never merges the
    two objects itself; it only lifts the refusal).

    `dry_run=True` (the default, same convention as every other write verb in this
    file) returns the exact plan — resolved project, old/new name, any collision found —
    without writing anything: no `assert_property`, no `agent_mounts` repoint, no
    prior-art search. Pass `dry_run=False` explicitly to actually rename."""
    from src.orchestrator.projects import AmbiguousProjectRef, _resolve_software_project

    project = (project or "").strip()
    new_name = (new_name or "").strip()
    because = (because or "").strip()
    if not project:
        return {"error": "project is required"}
    if not new_name:
        return {"error": "new_name is required"}
    if not because:
        return {"error": "because is required — a rename is testimony; the reason it "
                         "changed must be on the record"}
    try:
        row = await _resolve_software_project(actions.pool, project)
    except AmbiguousProjectRef as amb:
        return {"error": f"{amb.ref!r} is ambiguous — {len(amb.candidates)} active "
                         f"SoftwareProjects answer to it: {', '.join(amb.candidates)}. "
                         "Name the exact one (canonical or id) — rename_project never "
                         "guesses which."}
    if row is None:
        return {"error": f"no such SoftwareProject: {project!r}"}
    if row["status"] != "active":
        return {"error": f"{row['canonical']} is {row['status']}, not active — nothing "
                         "to rename"}
    try:
        collide = await _resolve_software_project(actions.pool, new_name)
    except AmbiguousProjectRef:
        collide = None  # an ambiguity already living under new_name is a pre-existing
                        # problem this rename did not create and is not asked to solve
    if collide is not None and collide["id"] != row["id"] and not merge_into:
        return {"error": f"{new_name!r} already names a DIFFERENT project "
                         f"({collide['canonical']}, status={collide['status']}) — "
                         "rename_project never collides two identities silently; pass "
                         "merge_into=True if this is deliberate (it only lifts this "
                         "refusal, it does not itself merge the two objects — "
                         "fold_project is the evidence-gated verb for that), or name a "
                         "genuinely free new_name instead"}
    old_name = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='name' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1",
        row["id"])
    if dry_run:
        manifest = await _cascade_governing_seats(
            actions.pool, project_oid=row["id"], old_name=old_name or "", new_name=new_name,
            because=because, actor=actor, dry_run=True)
        return {"project": row["canonical"], "old_name": old_name, "new_name": new_name,
                "because": because, "dry_run": True,
                "collision": (f"{collide['canonical']} (status={collide['status']}) — "
                              f"would proceed only because merge_into={merge_into!r}"
                              if collide is not None and collide["id"] != row["id"]
                              else None),
                "manifest": manifest,
                "note": "preview only — pass dry_run=False to actually rename"}
    now = datetime.now(UTC)
    await actions.assert_property(row["id"], "name", new_name, actor, now, _RENAME_CONF,
                                  evidence_class=_EC)
    bare_old = row["canonical"].removeprefix("repo:")
    mount_tag = await actions.pool.execute(
        "UPDATE agent_mounts SET project=$1 WHERE project=$2", new_name, bare_old)
    mounts_moved = int(mount_tag.rsplit(" ", 1)[-1])
    manifest = await _cascade_governing_seats(
        actions.pool, project_oid=row["id"], old_name=old_name or bare_old, new_name=new_name,
        because=because, actor=actor, dry_run=False)
    from src.orchestrator.capture import property_prior_art
    from src.orchestrator.identity_heal import detect_possibly_stale_seats

    prior_art_bits = await property_prior_art(
        actions.pool, subject_canonical=row["canonical"], field="name",
        new_value=new_name, because=because, actor=actor)
    stale = await detect_possibly_stale_seats(actions.pool, old_name or bare_old)
    return {"project": row["canonical"], "old_name": old_name, "new_name": new_name,
           "manifest": manifest,
           "mounts_moved": mounts_moved, "because": because,
           "note": f"{row['canonical']}'s canonical id never changes; edges already "
                   "pointing at it are unaffected; every GOVERNING SEAT's own pin/"
                   "house/charter/office is cascaded (see manifest) — the project's "
                   "own on-disk folder and its repo-root .osiris are not (manifest's "
                   "own could_not_reach names both)",
           "possibly_stale_seats": stale,
           **prior_art_bits}


async def fork_project(
    actions: Actions, *, project: str, fork_into: str, because: str, actor: str,
) -> dict[str, Any]:
    """FORK: John's own redmonth/ballgem shape (decision 58597670, verbatim: "new
    sibling project, redmonth untouched"). TWO objects, BOTH already active
    SoftwareProjects — this verb never mints either side, reusing fold_project's own
    refusal shape deliberately ("if the target doesn't exist yet, this is a RENAME... a
    different verb for a different act"). Mints ONE `forked_from` edge, `fork_into` ->
    `project` (the successor names its ancestor — the same direction convention
    succeeded_from already holds for an Agent lineage: heir -> ancestor).

    NO ESTATE MOVES — the deliberate opposite of fold_project: every existing
    in_repo/works_in/governs edge on BOTH objects stays exactly where it is. redmonth's
    145 in_repo edges were never meant to move; a fork records a NEW relationship
    between two objects that each keep their own, complete history.

    THE CALLER DECLARES; THIS FUNCTION NEVER INFERS, and it never runs
    project_identity_evidence itself — that report is read BEFORE this is called, by a
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
        return {"error": "because is required — a fork is a declared act on the record"}
    if not project or not fork_into:
        return {"error": "fork_project needs both labels: project and fork_into"}
    if project == fork_into:
        return {"error": "project and fork_into name the same label — nothing to fork"}

    async def _resolve(ref: str) -> tuple[Any, dict[str, Any] | None]:
        try:
            got = await _resolve_software_project(actions.pool, ref)
        except AmbiguousProjectRef as amb:
            return None, {"error": f"{amb.ref!r} is ambiguous — {len(amb.candidates)} "
                                   f"active SoftwareProjects answer to it: "
                                   f"{', '.join(amb.candidates)}. Name the exact one "
                                   "(canonical or id) — fork_project never guesses which."}
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
        return {"error": f"unknown SoftwareProject(s): {', '.join(missing)} — "
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
                         f"edge to {proj_row['canonical']} — nothing to do"}
    now = datetime.now(UTC)
    await actions.create_link(into_row["id"], proj_row["id"], "forked_from", actor, now,
                              _CONF, properties={"because": because}, evidence_class=_EC)
    return {"forked_from": proj_row["canonical"], "into": into_row["canonical"],
           "because": because,
           "note": "no estate moved — every existing in_repo/works_in/governs edge on "
                   "both objects stays exactly where it is"}


async def unfork_project(
    actions: Actions, *, project: str, fork_into: str, because: str, actor: str,
) -> dict[str, Any]:
    """Invalidate a live `forked_from` edge — the compensating-event complement to
    fork_project, same shape as seats.py's unpeer over peer_of. REVERSIBILITY PROVEN,
    not claimed (Thoth's own gate, DM 2427): since fork_project never moves any estate,
    there is nothing to move BACK either — the whole reversal is this one healed edge,
    by design.

    Refuses LOUDLY on: blank `because`; either ref unresolved to a SoftwareProject
    (ambiguity is refused the same way, though an already-forked pair is by definition
    unambiguous — both sides were named explicitly to create the edge); or no active
    `forked_from` edge from `fork_into` to `project`."""
    from src.orchestrator.projects import AmbiguousProjectRef, _resolve_software_project

    because = (because or "").strip()
    if not because:
        return {"error": "because is required — unforking is a deliberate act on the "
                         "record"}

    async def _resolve(ref: str) -> tuple[Any, dict[str, Any] | None]:
        try:
            got = await _resolve_software_project(actions.pool, ref)
        except AmbiguousProjectRef as amb:
            return None, {"error": f"{amb.ref!r} is ambiguous — {len(amb.candidates)} "
                                   f"active SoftwareProjects answer to it: "
                                   f"{', '.join(amb.candidates)}. Name the exact one "
                                   "(canonical or id) — unfork_project never guesses "
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
                         f"{proj_row['canonical']} — nothing to unfork"}
    now = datetime.now(UTC)
    await actions.invalidate_link(link["from_id"], link["to_id"], "forked_from", actor, now)
    return {"unforked": proj_row["canonical"], "was_into": into_row["canonical"],
           "because": because}


# --- create_project (#139's CREATE half, task #163's arc) --------------------------------

async def create_project(
    actions: Actions, *, name: str, because: str, actor: str,
) -> dict[str, Any]:
    """Declare a NEW SoftwareProject — the deliberate, user-facing CREATE door #139 asked
    for, built NOT AS A SEVENTH MINT DOOR (Thoth's explicit ruling) but as a thin wrapper
    over the two guards this house already built and re-validated at the merge point
    today: task #107's validated choke point (`_validate_repo_name`/`_resolve_repo`,
    capture.py, reused verbatim — refuses a path-shaped or malformed `name` before any
    object is touched) LAYERED WITH task #137's case-insensitive de-dup (the
    ramstein/RAMstein twin is the live proof shape-validation alone doesn't catch a case
    collision — #107 validates SHAPE, #137 prevents CASE-COLLISION twins, complementary,
    not redundant).

    Refuses LOUDLY on: a blank `because` (the same mandatory-testimony discipline
    rename_project/fork_project/unfork_project already hold — creating a project is
    testimony too) or a malformed/path-shaped `name`.

    NEVER MINTS A TWIN: if `name` already resolves — by exact canonical or `name`-property
    match (`_resolve_repo`) OR a case-insensitive canonical match (task #137's own layer)
    — the EXISTING object is returned, `created=False`, never a fresh mint. A genuinely
    new name (zero matches either way) mints fresh and stamps `because` as founding
    testimony, same as `_mint_or_find_repo`'s own name-property assert."""
    from src.orchestrator.capture import _resolve_repo, _validate_repo_name

    because = (because or "").strip()
    if not because:
        return {"error": "because is required — creating a project is a deliberate act "
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
                "note": "already exists — reused, never minted as a twin"}
    now = datetime.now(UTC)
    proj = await actions.create_or_find_object("SoftwareProject", f"repo:{stripped}", actor)
    await actions.assert_property(proj, "name", stripped, actor, now, _CONF,
                                  evidence_class=_EC)
    row = await actions.pool.fetchrow("SELECT canonical FROM objects WHERE id=$1", proj)
    return {"canonical": row["canonical"], "created": True, "because": because}
