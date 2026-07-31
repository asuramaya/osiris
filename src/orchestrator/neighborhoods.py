"""Rung 4 — neighborhood consolidation (thread 0deaec4f; operator ruling a0cfcca1).

Two motions, one purpose: search must answer from CONSOLIDATED memory, not fragments.

(a) The MECHANICAL pass crons consolidate_memory over Threads and Decisions: DERIVED
echoes fold into the deliberate captures they reword — the only auto-merge direction
(two deliberate / two derived stay surfaced for review; the membrane, constitution #6).
Fewer near-twin nodes = search hits stop splitting their rank across copies.

(b) The SUMMARY pass writes one Reference node per active repo neighborhood — an LLM
digest of the neighborhood's open lines and recent rulings — so a project's dense season
is recallable as ONE remembered page. Incremental by FINGERPRINT: the neighborhood's
member set + last movement hash into a watermark stamped on the Reference; an unchanged
neighborhood costs nothing (the skip-unchanged discipline the ladder named). Metered in
llm_usage (purpose='neighborhood-summary' — the digest's cost stream sees every call).
Recalled through consult_canon unchanged (it reads all Reference nodes), found by BOTH
search doors: FTS immediately, the embed cron vectorizes the body on its next walk.

Ownership boundary: summaries are written by `neighborhood-miner`, DERIVED — a machine's
digest of testimony is not testimony. It never touches the member objects themselves.
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

# THE NEIGHBORHOOD AS A PRIMITIVE (operator, 2026-07-11: "we don't hardcode, we build
# primitives... a fanout and organizing per project helps across the entire stack. think about
# neighborhoods and bundling. think about the garden of eden, and each project is a tree with
# fruits"). The concept already lived here — but PRIVATE, owned by the summary miner, so every
# surface that wanted "group this by project" re-derived it by hand: the operator's desk, the
# wall's rollup, and (nearly) a bespoke console renderer. Three hand-rolled copies of one idea
# is a primitive announcing itself.
#
# So: the garden is the graph, a TREE is a SoftwareProject, and the FRUIT is anything hanging
# off it by `in_repo`. `neighborhoods_of` names the tree each fruit grew on — one batched query,
# no N+1 — and everything above it (bundle/fanout, the desk roster, the wall) composes from it
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
    """THE GARDEN AUDITS ITSELF (operator, 2026-07-12: "does it not detect the changes or is
    that my fault"). Neither: one project had 126 commits on disk and the graph never read one,
    because the pulse's repo list is a HAND-TYPED env var (OSIRIS_DEV_REPOS, 7 paths) while the
    session-miner auto-discovers transcripts from every project alive. So Osiris learned what to
    LISTEN TO from reality and what to LOOK AT from a list — it heard every promise in the fleet
    and witnessed delivery in seven repos.

    It never had to be that way: the graph ALREADY KNOWS its trees. 368 threads are filed under
    that one repo. It was told, 368 times, and had no mechanism to act on it.

    This is the mechanism — and it only REPORTS. Osiris has no hands: it names the gap between
    the trees it knows and the trees it watches, and a deliberate act closes it. Growing the
    watch list is a decision, never a side effect (constitution #6: never silently).

    THE READ-BACK A HOUSE NEEDS TO SEE ITS OWN INVISIBILITY (redmonth's report, thread 2309:
    "I could not have discovered my own zero; you had to tell me... I would rank [being able
    to tell] FIRST — being unwatched is recoverable; being unwatched and unable to tell is
    not"). Every active SoftwareProject is reported, not just ones the graph already has
    Thread/Commit testimony against — a truly fresh, zero-everything project used to be
    silently absent from this function's own output, the exact "invisible until someone
    tells you" defect it exists to close. `path` is READ from the stored `on_disk_path`
    property (census_trees's own write, or a future explicit registration) — never
    re-derived by matching this tree's name against a caller-supplied search root, which
    the earlier version of this function did: a directory-name match is a GUESS, and
    redmonth proved the guess can be wrong (his own mounted cwd is not a git repository at
    all; his real repo lives elsewhere, under a different name). No path recorded means no
    path claimed — the honest "I don't know" the earlier version couldn't say.

    `reason` NAMES why `commits` is zero, so a zero never reads as silence: no on_disk_path
    at all; on disk but not in `watched`; watched but ingest has never ticked (no
    `devhead:<tree>` watermark yet — pulse.py's own cursor, so "last ingested" is exactly
    that watermark's `updated_at`); or ingest has genuinely run and found nothing (the path
    may no longer be a real git repo). A non-zero `commits` needs no reason and gets none.
    """
    rows = await pool.fetch(
        "SELECT p.id, replace(p.canonical, 'repo:', '') AS tree, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=p.id "
        "   AND a.name='on_disk_path' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS path, "
        " (SELECT count(*) FROM links l JOIN objects c ON c.id=l.from_id AND c.type='Commit' "
        "  WHERE l.to_id=p.id AND l.type='in_repo') AS commits, "
        " (SELECT count(*) FROM links l JOIN objects m ON m.id=l.from_id "
        "  WHERE l.to_id=p.id AND l.type='in_repo') AS activity "
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


_CENSUS_SKIP = {".claude", "node_modules", ".venv", "__pycache__"}


def _git_dirs(roots: list[str], *, max_depth: int = 2) -> list[Path]:
    """Every git repository under the census roots (bounded walk, sync disk IO).

    Depth 2 covers the operator's layout (~/code/<repo> and ~/code/REPOS/<repo>) without
    crawling the world; hidden dirs, tool caches, and worktree nests are skipped."""
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
    """THE DISK CENSUS (thread 5e37630b, Atlas II's costliest ask): the graph models what
    it was TOLD about; the disk holds the operator's whole history — and 'a memory that
    doesn't know your ancestors cannot stop you reinventing them' (he was one conversation
    from rebuilding an eval harness he had already written). This walks the census roots
    and makes 'exists on disk' a FIRST-CLASS graph fact: a repo the graph has never met
    is minted as a SoftwareProject with its on_disk_path and discovered='disk-census';
    a known project gains its path if the graph lacked one. OBSERVATION ONLY — nothing
    here grows the pulse's watch list (that remains a deliberate act, discover_trees'
    doctrine), and remote-only repos stay honestly out of scope (no network read).
    Idempotent: an unchanged disk costs reads, never writes.

    THE MINT GOES THROUGH THE SAME CHOKE POINT AS EVERY OTHER LEGITIMATE MINT (ruling
    1db1ff41, both halves): `_mint_or_find_repo` runs `_validate_repo_name` before
    touching the graph — task #107's guard, previously bypassed here entirely (this walk
    minted straight off `create_or_find_object`, so a directory basename that isn't a
    well-formed project ref reached the graph unrefused; the two real "/home/.../ballgem"
    and "/home/.../REPOS/sutra" duplicates, folded separately, are what that gap already
    cost). A refusal degrades PER ENTRY, never the whole batch — #107's own third-order
    defect (settle()'s batch-abort gap) is the standing precedent: one hostile directory
    name costs that one census row, not the walk."""
    from src.orchestrator.capture import _mint_or_find_repo, _resolve_repo

    observed = datetime.now(UTC)
    ec = EvidenceClass.DIRECT_OBSERVATION.value
    minted: list[str] = []
    pathed: list[str] = []
    refused: list[dict[str, str]] = []
    known = 0
    for repo in _git_dirs(roots):
        name = repo.name
        existing = await _resolve_repo(actions.pool, name)
        if existing is None:
            try:
                obj = await _mint_or_find_repo(actions, name, observed, source="disk-census",
                                               evidence_class=ec, confidence=0.9)
            except ValueError as e:
                refused.append({"name": name, "path": str(repo), "reason": str(e)})
                continue
            await actions.assert_property(obj, "discovered", "disk-census", "disk-census",
                                          observed, 0.9, evidence_class=ec)
            await actions.assert_property(obj, "on_disk_path", str(repo), "disk-census",
                                          observed, 0.9, evidence_class=ec)
            minted.append(name)
            continue
        known += 1
        current = await actions.pool.fetchval(
            "SELECT a.value #>> '{}' FROM current_assertions a "
            "WHERE a.object_id=$1 AND a.name='on_disk_path' LIMIT 1", existing)
        if current != str(repo):
            await actions.assert_property(existing, "on_disk_path", str(repo),
                                          "disk-census", observed, 0.9, evidence_class=ec)
            pathed.append(name)
    return {"known": known, "minted": minted, "pathed": pathed, "refused": refused}


async def neighborhoods_of(
    pool: asyncpg.Pool, ids: list[Any],
) -> dict[Any, dict[str, Any]]:
    """The TREE each of these objects hangs from — {object_id: {name, id}} for every object
    with an `in_repo` edge. Objects with no tree are simply absent (the caller decides what
    rootless fruit means: the desk calls it '—', a bundle gives it its own pile).

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
    """The mechanical motion: fold DERIVED echoes into deliberate captures, both memory
    types. Pure token-overlap, no LLM — cheap enough to walk daily."""
    out: dict[str, int] = {}
    for typ, prefix in (("Thread", "thread:"), ("Decision", "decision:")):
        out.update(await consolidate_memory(actions, object_type=typ, prefix=prefix))
    return out


async def _neighborhoods(pool: asyncpg.Pool) -> list[dict[str, Any]]:
    """Repos whose memory moved inside the window, each with a FINGERPRINT of its member
    set + last movement — the watermark that makes summarization incremental. Ordered
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
    their rationale — winner texts only, newest movement first, capped."""
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
            continue  # an empty digest is not a memory — leave the old one standing
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
