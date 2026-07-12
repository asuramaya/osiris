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
