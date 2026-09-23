"""Neighborhood consolidation: keeps search answering from consolidated memory instead
of scattered fragments.

Two motions serve that one purpose.

(a) The mechanical pass crons consolidate_memory over Threads and Decisions: derived
echoes fold into the deliberate captures they reword, the only auto-merge direction
allowed (two deliberate records or two derived records stay surfaced for review rather
than being merged automatically). Fewer near-duplicate nodes means search hits stop
splitting their rank across copies.

(b) The summary pass writes one Reference node per active repo neighborhood: an LLM
digest of the neighborhood's open lines and recent rulings, so a project's dense
history is recallable as one page instead of many. It is incremental by fingerprint:
the neighborhood's member set plus its last movement hash into a watermark stamped on
the Reference, so an unchanged neighborhood costs nothing to re-check. Metered in
llm_usage (purpose='neighborhood-summary') so the digest's cost is tracked per call.
Recalled through consult_canon unchanged (it reads all Reference nodes), and found by
both search paths: full-text search immediately, the embedding cron on its next walk.

Ownership boundary: summaries are written by `neighborhood-miner` and graded derived,
since a machine's digest of testimony is not itself testimony. It never touches the
member objects themselves.
"""
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.config.settings import get_settings
from src.ingest.mined import consolidate_memory
from src.ingest.providers import LLMClient, Usage
from src.ingest.usage import record_usage
from src.orchestrator.capture import ingest_reference
from src.parsers.base import EvidenceClass

_SOURCE = "neighborhood-miner"
_PURPOSE = "neighborhood-summary"
_WINDOW_DAYS = 30
_MAX_MEMBERS = 40           # the digest reads the neighborhood's densest recent core

# "Group objects by project" is a reusable concept, not a one-off. It used to live only
# inside the summary miner, so every surface that wanted the same grouping re-derived it
# by hand instead of sharing one implementation. A SoftwareProject is the grouping unit,
# and anything hanging off it via an `in_repo` link belongs to its neighborhood.
# `neighborhoods_of` names the project each object belongs to in one batched query (no
# N+1), and every other surface that needs "group this by project" composes from it
# instead of re-deriving it.
NEIGHBORHOOD = "neighborhood"

_MAX_SUMMARIES = 3          # LLM budget per pass; stalest-first rotation covers the rest
_EC = EvidenceClass.DERIVED.value
_CONF = 0.5

_SYSTEM = (
    "You are the neighborhood consolidator of a provenance-first memory graph. You are "
    "given one project's recent memory: open threads (unfinished lines of work) and "
    "recent decisions (rulings, with rationales). Write a compact digest — under 300 "
    "words, plain prose with short paragraphs — that a returning mind could read INSTEAD "
    "of the fragments: what this project is about right now, the main open lines and how "
    "they relate, and what was settled recently (with the WHY when it matters). Never "
    "invent facts not present in the input; never editorialize about priorities.")


async def discover_trees(pool: asyncpg.Pool, *, watched: list[str]) -> list[dict[str, Any]]:
    """Reports the gap between the projects the graph already knows about and the projects
    the ingest pipeline actually watches.

    This gap is real: the commit-ingest watch list is a hand-typed list of repo paths,
    while other ingest paths (like the session miner) auto-discover their sources. So the
    graph can end up hearing about work in a project through threads and decisions while
    never reading a single commit from it on disk, even when the graph already has plenty
    of testimony filed under that project.

    This function only reports the gap; it does not close it. Growing the watch list stays
    a deliberate, explicit action rather than a side effect of running this report.

    Every active SoftwareProject is reported, not just ones the graph already has
    Thread/Commit testimony against. A truly fresh, zero-everything project must still show
    up here rather than being silently absent, since a project nobody can see is a project
    nobody can act on. `path` is read from the stored `on_disk_path` property (written by
    `census_trees`, or by a future explicit registration), never re-derived by matching this
    project's name against a caller-supplied search root: a directory-name match is only a
    guess, and guesses can be wrong (a mounted working directory need not even be a git
    repository, while the real repo lives elsewhere under a different name). No path
    recorded means no path claimed, an honest "unknown" rather than a guess.

    `reason` names why `commits` is zero, so a zero never reads as plain silence: no
    on_disk_path at all; on disk but not in `watched`; watched but ingest has never ticked
    (no `devhead:<tree>` watermark yet, the ingest cursor whose `updated_at` is exactly
    "last ingested"); or ingest has genuinely run and found nothing (the path may no longer
    be a real git repo). A non-zero `commits` needs no reason and gets none.
    """
    rows = await pool.fetch(
        "SELECT p.id, replace(p.canonical, 'repo:', '') AS tree, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=p.id "
        "   AND a.name='on_disk_path' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS path, "
        " (SELECT count(*) FROM links l JOIN objects c ON c.id=l.from_id AND c.type='Commit' "
        "  WHERE l.to_id=p.id AND l.type='in_repo' "
        "  AND (l.valid_until IS NULL OR l.valid_until > now())) AS commits, "
        " (SELECT count(*) FROM links l JOIN objects m ON m.id=l.from_id "
        "  WHERE l.to_id=p.id AND l.type='in_repo' "
        "  AND (l.valid_until IS NULL OR l.valid_until > now())) AS activity "
        "FROM objects p WHERE p.type='SoftwareProject' AND p.status='active' "
        "ORDER BY commits DESC, activity DESC")
    last_ingested = {
        r["key"].removeprefix("devhead:"): r["updated_at"]
        for r in await pool.fetch(
            "SELECT key, updated_at FROM watermarks WHERE key LIKE 'devhead:%'")
    }
    seen = {Path(w).name for w in watched if w.strip()}
    out: list[dict[str, Any]] = []
    for r in rows:
        tree, path, commits = r["tree"], r["path"], r["commits"]
        is_watched = tree in seen
        when = last_ingested.get(tree)
        reason = None
        if commits == 0:
            if not path:
                reason = "no on_disk_path registered — the disk census hasn't found it yet"
            elif not is_watched:
                reason = f"on disk at {path}, not in the ingest watch list"
            elif when is None:
                reason = "in the watch list but ingest has never ticked for it yet"
            else:
                reason = "ingest has run and found nothing — the path may no longer be a git repo"
        out.append({
            "tree": tree, "path": path, "watched": is_watched, "commits": commits,
            "activity": r["activity"], "last_ingested_at": when, "reason": reason,
            # the gap that matters: the graph knows this tree, work exists on disk, nobody looks
            "blind": bool(path) and not is_watched,
        })
    return out


_CENSUS_SKIP = {".claude", "node_modules", ".venv", "venv", "__pycache__"}


def _path_gone(path: str | None) -> bool:
    """A plain sync wrapper so `census_trees` (async) never calls a blocking Path method
    inline (ASYNC240, this codebase's own ruff gate). No recorded path at all counts as
    gone too, since there is nothing to have moved from."""
    return not path or not Path(path).exists()


def _git_dirs(roots: list[str], *, max_depth: int = 2) -> list[Path]:
    """Every git repository under the census roots (bounded walk, sync disk IO).

    Depth 2 covers a typical `~/code/<repo>` and `~/code/REPOS/<repo>` layout without
    crawling the world; hidden dirs, tool caches, and worktree nests are skipped.

    _CENSUS_SKIP matches "venv" as well as ".venv": before this fix a bare `venv` survived
    only by accident, in layouts where it happened to sit exactly at the depth-2 cutoff
    (e.g. `mirror/venv`), because `depth >= max_depth` returned before its own children
    were ever iterated. A `venv` one level shallower had no such protection and the walker
    would descend straight into it."""
    found: list[Path] = []

    def walk(d: Path, depth: int) -> None:
        try:
            if (d / ".git").exists():
                found.append(d)
                return  # a repo's SUBdirs are its own business (worktrees, vendored trees)
            if depth >= max_depth:
                return
            for c in sorted(d.iterdir()):
                if c.is_dir() and not c.name.startswith(".") and c.name not in _CENSUS_SKIP:
                    walk(c, depth + 1)
        except OSError:
            return

    for root in roots:
        p = Path(root).expanduser()
        if p.is_dir():
            walk(p, 0)
    return found


async def census_trees(actions: Actions, *, roots: list[str]) -> dict[str, Any]:
    """The disk census: the graph only models what it was told about, while the disk holds
    the full history of what actually exists, and a memory graph that doesn't know its own
    history can lead someone to reinvent work they already did. This walks the census roots
    and makes "exists on disk" a first-class graph fact: a repo the graph has never met is
    minted as a SoftwareProject with its on_disk_path and discovered='disk-census'; a known
    project gains its path if the graph lacked one. A working directory whose `.git` is a
    file (a worktree's own gitdir pointer, never a directory) is filed as a Worktree of its
    parent project via a `worktree_of` link instead (see the worktree branch below): this
    never mints a second SoftwareProject for a checkout that is really just another view
    onto one it already knows. Observation only: nothing here grows the ingest watch list
    (that stays a deliberate action, per `discover_trees`), and remote-only repos stay
    honestly out of scope (no network read). Idempotent: an unchanged disk costs reads,
    never writes.

    The mint goes through the same validation as every other legitimate mint:
    `_mint_or_find_repo` runs `_validate_repo_name` before touching the graph. This guard
    was previously bypassed here entirely, since this walk minted straight off
    `create_or_find_object`, so a directory basename that isn't a well-formed project
    reference reached the graph unrefused (two real duplicate projects, folded separately,
    are what that gap already cost). A refusal degrades per entry, never the whole batch:
    one hostile directory name costs that one census row, not the walk.

    remote_url rides the same walk: a git remote is configuration, not content, cheap to
    read (`git remote get-url origin` touches only .git/config, no network), so it costs
    nothing new to capture on the walk this function already runs on a fixed cadence. This
    is the only place remote_url is ever written; the hot mount()-time identity-resolution
    path reads it as a plain DB column and never shells out live, so no subprocess sits on
    the path every session in the fleet traverses. Staleness is bounded by the existing
    cadence: a repo whose origin changes self-heals on this function's own next run, at
    most one cycle later, no new staleness window is introduced, because on_disk_path
    already carries this same bound and nothing here reads faster than the walk that
    writes it. A repo with no configured origin gets no remote_url assertion at all;
    absence stays absence, never a persisted null standing in for "checked, found
    nothing", because a reader downstream must be able to tell "no signal" from "confirmed
    empty", and skipping the write is how that distinction survives here.

    The location fix: a renamed directory fails the name lookup above and, before this
    fix, minted a duplicate SoftwareProject rather than reconnecting to the one that
    already existed under its old name. When name lookup finds nothing but the directory
    has a real remote_url, this now tries that remote_url as a second signal: an
    unambiguous match (exactly one active SoftwareProject already carrying it) reconnects,
    updating only `on_disk_path`/`remote_url` on the existing object; more than one match
    refuses per entry rather than guessing. This carries location forward and never
    re-derives identity: `name`/`canonical` stay `rename_project`'s own deliberate act,
    the same discipline that closed an earlier phantom-repo defect. Reconnect is
    additionally gated on the old on_disk_path being confirmed gone (or never having been
    recorded): a live old path means this is a copy of a working tree, not a move, and a
    copy must mint its own object rather than steal another checkout's identity. One walk,
    with a loud receipt (`reconnected` in the return dict): the same idempotent cadence
    that already bounds on_disk_path staleness bounds a missed reconnect too, and a bad
    reconnect can't reach the graph in the first place because the ambiguous case above
    refuses outright, so a second confirmation walk would buy no correctness gain here,
    only latency."""
    from src.orchestrator.capture import _mint_or_find_repo, _resolve_repo, _resolve_repo_by_remote
    from src.orchestrator.project_identity import (
        _git_remote,
        git_current_branch,
        worktree_parent_path,
    )

    observed = datetime.now(UTC)
    ec = EvidenceClass.DIRECT_OBSERVATION.value
    minted: list[str] = []
    pathed: list[str] = []
    remoted: list[str] = []
    reconnected: list[str] = []
    worktrees: list[str] = []
    refused: list[dict[str, str]] = []
    known = 0
    for repo in _git_dirs(roots):
        name = repo.name

        # Worktrees are a first-class shape: `.git` being a file, not a directory, is the
        # unambiguous worktree signal that `_git_dirs`'s own bare `.exists()` check can't
        # distinguish from a real repo. Before this, a worktree living as a sibling under a
        # census root (never one nested under a repo's own `.claude/worktrees`, which
        # `_git_dirs`'s early-return-on-`.git` already keeps unreached) minted its own
        # SoftwareProject, keyed on the worktree directory's own basename. It is filed as a
        # Worktree of its parent project via a `worktree_of` link instead, never a second
        # SoftwareProject.
        if (repo / ".git").is_file():
            parent_path = worktree_parent_path(str(repo))
            if parent_path is None:
                refused.append({"name": name, "path": str(repo),
                                "reason": "looks like a worktree (.git is a file) but its "
                                "parent checkout could not be resolved — refusing to guess"})
                continue
            parent_name = Path(parent_path).name
            parent_obj = await _resolve_repo(actions.pool, parent_name)
            if parent_obj is None:
                try:
                    parent_obj = await _mint_or_find_repo(
                        actions, parent_name, observed, source="disk-census",
                        evidence_class=ec, confidence=0.9)
                    await actions.assert_property(parent_obj, "on_disk_path", parent_path,
                                                  "disk-census", observed, 0.9,
                                                  evidence_class=ec)
                except ValueError as e:
                    refused.append({"name": name, "path": str(repo),
                                    "reason": f"parent project {parent_name!r}: {e}"})
                    continue
            tree_obj = await actions.create_or_find_object(
                "Worktree", f"worktree:{name}", "disk-census")
            await actions.assert_property(tree_obj, "on_disk_path", str(repo),
                                          "disk-census", observed, 0.9, evidence_class=ec)
            branch = git_current_branch(str(repo))
            if branch:
                await actions.assert_property(tree_obj, "branch", branch, "disk-census",
                                              observed, 0.9, evidence_class=ec)
            exists = await actions.pool.fetchval(
                "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='worktree_of' "
                "AND (valid_until IS NULL OR valid_until > now()) LIMIT 1",
                tree_obj, parent_obj)
            if not exists:
                await actions.create_link(tree_obj, parent_obj, "worktree_of",
                                          "disk-census", observed, 0.9, evidence_class=ec)
            worktrees.append(name)
            continue

        _, remote_url = _git_remote(str(repo))
        existing = await _resolve_repo(actions.pool, name)
        if existing is None and remote_url:
            # The location fix: a renamed directory fails the name lookup above and would
            # otherwise mint a duplicate. Try the remote_url as a second signal before
            # minting. This carries location forward and never re-derives identity: only
            # on_disk_path/remote_url move here, name/canonical stay rename_project's own
            # deliberate act. An unambiguous match reconnects; more than one candidate
            # refuses per entry rather than guessing.
            candidates = await _resolve_repo_by_remote(actions.pool, remote_url)
            if len(candidates) > 1:
                refused.append({
                    "name": name, "path": str(repo),
                    "reason": f"ambiguous remote_url match: {len(candidates)} active "
                    "SoftwareProjects already carry this remote — refusing to guess which "
                    "one this directory reconnects to (a human/seat declaration is needed, "
                    "same doctrine as fork_project)",
                })
                continue
            if len(candidates) == 1:
                candidate = candidates[0]
                old_path = await actions.pool.fetchval(
                    "SELECT a.value #>> '{}' FROM current_assertions a "
                    "WHERE a.object_id=$1 AND a.name='on_disk_path' LIMIT 1", candidate)
                if old_path == str(repo):
                    # Already reconnected here on a prior walk: the object's own `name`
                    # deliberately never moved to match the directory, so a name lookup
                    # will keep missing forever. Recognize this exact location as known
                    # rather than re-walking the move/copy gate below on every future
                    # census (idempotency for the reconnected case).
                    existing = candidate
                # Reconnect only once the old path is confirmed gone: that's what keeps a
                # copy (two live checkouts of one remote) from being misread as a move. No
                # recorded path at all is not a copy risk either, so it clears the gate too.
                elif _path_gone(old_path):
                    await actions.assert_property(
                        candidate, "on_disk_path", str(repo), "disk-census",
                        observed, 0.9, evidence_class=ec)
                    await actions.assert_property(
                        candidate, "remote_url", remote_url, "disk-census",
                        observed, 0.9, evidence_class=ec)
                    reconnected.append(name)
                    continue
                # else: the old path still exists, this is a copy, not a move; falls
                # through to mint its own object rather than steal the original's identity.
        if existing is None:
            try:
                obj = await _mint_or_find_repo(actions, name, observed, source="disk-census",
                                               evidence_class=ec, confidence=0.9)
            except ValueError as e:
                refused.append({"name": name, "path": str(repo), "reason": str(e)})
                continue
            # Same check-current-first pattern as the "known" branch just below:
            # `_mint_or_find_repo` finding `obj` here does not mean this is genuinely the
            # first sighting. When `_resolve_repo`'s own (narrower) lookup keeps missing an
            # object that already exists under this exact canonical, this branch runs on
            # every census sweep for it; measured live at 3,140 identical rows apiece
            # across discovered/on_disk_path/remote_url for seven objects. Checked once
            # per property so a repeat sweep of an already-discovered object writes
            # nothing.
            discovered_current = await actions.pool.fetchval(
                "SELECT 1 FROM current_assertions WHERE object_id=$1 "
                "AND name='discovered' AND source_id='disk-census' LIMIT 1", obj)
            if not discovered_current:
                await actions.assert_property(obj, "discovered", "disk-census", "disk-census",
                                              observed, 0.9, evidence_class=ec)
            # Same-source scoping: this branch's own on_disk_path/remote_url checks used to
            # compare against `current_assertions` (the cross-source, evidence-graded
            # winner) rather than disk-census's own prior value, which is undefined once a
            # different, higher-confidence source also holds a current assertion on the
            # same property. This is the same bug the "known" branch below already fixed
            # via `would_be_noop_assert`, just missed here on first sighting.
            if not await actions.would_be_noop_assert(obj, "on_disk_path", "disk-census",
                                                       str(repo)):
                await actions.assert_property(obj, "on_disk_path", str(repo), "disk-census",
                                              observed, 0.9, evidence_class=ec)
            if remote_url and not await actions.would_be_noop_assert(
                obj, "remote_url", "disk-census", remote_url
            ):
                await actions.assert_property(obj, "remote_url", remote_url, "disk-census",
                                              observed, 0.9, evidence_class=ec)
                remoted.append(name)
            minted.append(name)
            continue
        known += 1
        # No-op reassertion guard: this used to compare against `current_assertions` (the
        # cross-source, evidence-graded winner) rather than disk-census's own prior value,
        # so once any other, higher-confidence source (e.g. a human rename_project)
        # disagreed with disk-census's own on_disk_path/remote_url, this census could never
        # "win" its own comparison and re-asserted the identical value on every walk,
        # forever (1.5-3k rows per triple, live). `would_be_noop_assert` is scoped to this
        # source specifically, matching assert_property's own same-source supersession.
        if not await actions.would_be_noop_assert(existing, "on_disk_path", "disk-census",
                                                   str(repo)):
            await actions.assert_property(existing, "on_disk_path", str(repo),
                                          "disk-census", observed, 0.9, evidence_class=ec)
            pathed.append(name)
        if remote_url and not await actions.would_be_noop_assert(
            existing, "remote_url", "disk-census", remote_url
        ):
            await actions.assert_property(existing, "remote_url", remote_url,
                                          "disk-census", observed, 0.9, evidence_class=ec)
            remoted.append(name)
    return {"known": known, "minted": minted, "pathed": pathed, "remoted": remoted,
            "reconnected": reconnected, "worktrees": worktrees, "refused": refused}


async def neighborhoods_of(
    pool: asyncpg.Pool, ids: list[Any],
) -> dict[Any, dict[str, Any]]:
    """The project each of these objects belongs to: {object_id: {name, id}} for every
    object with an `in_repo` edge. Objects with no project are simply absent (the caller
    decides what that means, e.g. a dashboard renders '-' or groups them separately).

    One query for the whole set. The newest edge wins if an object was re-filed."""
    if not ids:
        return {}
    rows = await pool.fetch(
        "SELECT DISTINCT ON (l.from_id) l.from_id AS oid, p.id AS pid, "
        " replace(p.canonical, 'repo:', '') AS hood "
        "FROM links l JOIN objects p ON p.id = l.to_id "
        "WHERE l.from_id = ANY($1::uuid[]) AND l.type = 'in_repo' "
        "  AND p.type = 'SoftwareProject' AND p.status = 'active' "
        "ORDER BY l.from_id, l.created_at DESC", ids)
    return {r["oid"]: {"name": r["hood"], "id": str(r["pid"])} for r in rows}
async def consolidate_pass(actions: Actions) -> dict[str, int]:
    """The mechanical motion: fold derived echoes into deliberate captures, both memory
    types. Pure token-overlap, no LLM, cheap enough to walk daily."""
    out: dict[str, int] = {}
    for typ, prefix in (("Thread", "thread:"), ("Decision", "decision:")):
        out.update(await consolidate_memory(actions, object_type=typ, prefix=prefix))
    return out


async def _neighborhoods(pool: asyncpg.Pool) -> list[dict[str, Any]]:
    """Repos whose memory moved inside the window, each with a fingerprint of its member
    set plus last movement, the watermark that makes summarization incremental. Ordered
    stalest-summary-first so the per-pass budget rotates fairly."""
    return [dict(r) for r in await pool.fetch(
        "WITH member AS ("
        "  SELECT p.id AS repo_id, p.canonical AS repo_canon, o.id AS member_id, "
        "   (SELECT max(a.observed_at) FROM assertions a WHERE a.object_id=o.id) AS moved "
        "  FROM objects p "
        "  JOIN links l ON l.to_id = p.id AND l.type = 'in_repo' "
        "  JOIN objects o ON o.id = l.from_id AND o.status = 'active' "
        "   AND o.type IN ('Thread','Decision') "
        "  WHERE p.type = 'SoftwareProject' AND p.status = 'active') "
        "SELECT repo_id, repo_canon, count(*) AS members, "
        " md5(string_agg(member_id::text || COALESCE(moved::text, ''), ',' "
        "   ORDER BY member_id)) AS fingerprint, "
        " (SELECT a.value #>> '{}' FROM current_assertions a "
        "   JOIN objects ref ON ref.id = a.object_id "
        "   WHERE ref.canonical = 'ref:neighborhood-' || trim(both '-' from regexp_replace("
        "     lower(replace(m.repo_canon, 'repo:', '')), '[^a-z0-9]+', '-', 'g')) "
        "    AND a.name = 'watermark' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS current_watermark, "
        " (SELECT max(a.observed_at) FROM current_assertions a "
        "   JOIN objects ref ON ref.id = a.object_id "
        "   WHERE ref.canonical = 'ref:neighborhood-' || trim(both '-' from regexp_replace("
        "     lower(replace(m.repo_canon, 'repo:', '')), '[^a-z0-9]+', '-', 'g')) "
        "    AND a.name = 'watermark') AS summarized_at "
        "FROM member m "
        "GROUP BY repo_id, repo_canon "
        "HAVING max(moved) > now() - make_interval(days => $1) AND count(*) >= 3 "
        "ORDER BY summarized_at ASC NULLS FIRST", _WINDOW_DAYS)]


async def _member_texts(pool: asyncpg.Pool, repo_id: Any) -> str:
    """The neighborhood's readable core: open threads first, then recent decisions with
    their rationale. Winner texts only, newest movement first, capped."""
    rows = await pool.fetch(
        "SELECT o.type, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS summary, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='rationale' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS rationale, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS status, "
        " (SELECT max(a.observed_at) FROM assertions a WHERE a.object_id=o.id) AS moved "
        "FROM objects o JOIN links l ON l.from_id=o.id AND l.type='in_repo' "
        "AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "WHERE l.to_id=$1 AND o.status='active' AND o.type IN ('Thread','Decision') "
        "ORDER BY (o.type = 'Thread' AND EXISTS (SELECT 1 FROM current_assertions s "
        "  WHERE s.object_id=o.id AND s.name='status' AND s.value #>> '{}' = 'open')) DESC, "
        " moved DESC LIMIT $2", repo_id, _MAX_MEMBERS)
    lines = []
    for r in rows:
        if not r["summary"]:
            continue
        tag = f"[{r['type'].lower()}:{r['status'] or '?'}]"
        line = f"{tag} {r['summary'][:400]}"
        if r["rationale"]:
            line += f" — WHY: {r['rationale'][:300]}"
        lines.append(line)
    return "\n".join(lines)


async def summarize_neighborhoods(
    actions: Actions, llm: LLMClient, *, model: str | None = None,
    cap: int = _MAX_SUMMARIES,
) -> dict[str, int]:
    """The summary motion: for up to `cap` neighborhoods whose fingerprint moved, write
    (or refresh) the `ref:neighborhood-<repo>` Reference. Fingerprint match = free skip."""
    model = model or get_settings().osiris_extract_model
    hoods = await _neighborhoods(actions.pool)
    out = {"candidates": len(hoods), "summarized": 0, "skipped": 0}
    for h in hoods:
        if h["current_watermark"] == h["fingerprint"]:
            out["skipped"] += 1
            continue
        if out["summarized"] >= cap:
            continue  # next pass rotates here (stalest-first ordering)
        repo = str(h["repo_canon"]).removeprefix("repo:")
        corpus = await _member_texts(actions.pool, h["repo_id"])
        if not corpus:
            out["skipped"] += 1
            continue
        usage_out: list[Usage] = []
        body = await llm.complete(
            system=_SYSTEM, prompt=f"Project: {repo}\n\n{corpus}", model=model,
            max_tokens=1024, usage_out=usage_out)
        if usage_out:
            await record_usage(actions.pool, purpose=_PURPOSE, usage=usage_out[-1])
        if not body.strip():
            continue  # an empty digest is not a memory, leave the old one standing
        ref_id, _canon = await ingest_reference(
            actions, f"Neighborhood — {repo}", vendor="osiris",
            body=body.strip()[:8000], repo=repo, source=_SOURCE)
        # DERIVED, not testimony: ingest_reference grades an agent's READ self_declared;
        # the watermark is the miner's own bookkeeping at the machine's grade
        await actions.assert_property(
            ref_id, "watermark", h["fingerprint"], _SOURCE, datetime.now(UTC), _CONF,
            evidence_class=_EC)
        out["summarized"] += 1
    return out
