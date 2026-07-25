"""Compositions — the composer's primitive: a saved, forkable spec over the graph.

The front end was never a page; it's the *composer* — the place where intent becomes a
composition over neutral primitives. A composition is a small op-tree the substrate
executes. It unifies the *watch* (a saved subscription) and the *lens* (a saved query)
into ONE first-class object, so opinion lives in the composition the USER owns — not
welded into engine code. Claude authors them from a sentence (the MCP tools); the
substrate runs them; the views render them.

The op set is small and CLOSED — grounded in Palantir's Object Set API + Notion's
rollups, which independently land on the same vocabulary (see docs/COMPOSER.md). Anything
the ops can't express is a Function (a named transform), never a new op.

Ops (neutral, composable — the equivalent of Notion's filter/relation/rollup):
  {"op":"subject"}                                 -> the object you're looking at
  {"op":"select","object_type":?,"where":[...]}    -> objects matching conditions (.filter)
  {"op":"traverse","from":N,"direction":,"hops":}  -> objects N hops away (.searchAround)
  {"op":"collect","from":N,"properties":[],"transform":?} -> the values of those props
  {"op":"subtract","left":N,"right":N}             -> values in left not in right (.subtract)
  {"op":"union","sets":[N,...]}                     -> combine sets (.union)
  {"op":"intersect","sets":[N,...]}                 -> objects/values in ALL sets (.intersect)
  {"op":"aggregate","from":N,"group_by":[],"metric":{...}} -> group + a metric (.groupBy / rollup)
  {"op":"table","from":N,"columns":[...]}          -> one ROW per object, columns = a property
       OR a rollup-over-a-link (Notion's database+rollups / Palantir's object-set+per-object
       aggregate). column = {"name":,"property":P} | {"name":,"rollup":{"direction":in|out|both,
       "link_type":?,"object_type":?,"of":count|first|max|min|sum|avg,"property":?}}. `first` =
       Notion's show-original (pluck a single relation's value, incl. an object column like
       `canonical` — how a linked commit/entity is named).
  {"op":"order","from":N,"by":?,"dir":}            -> rank a set/rows (.orderBy)
  {"op":"take","from":N,"n":K}                      -> top-N (.take)
  {"op":"sections","sections":[{"title":,"body":N},...]} -> stack named sub-compositions into
       one titled read-model (Notion's page-of-blocks). Each body is its own op-tree; the
       result is {title: rendered-items}. This is what a "briefing"/"dossier" IS — a page of
       compositions, not bespoke code.
  {"op":"function","name":,"args":{}}              -> a registered Function (the escape hatch)

The old `discrepancy` read-model is just one composition (opinion left the engine):
  subtract( collect(location, country) over traverse(subject, 2 hops),
            collect(home-props, country) over subject )

There is deliberately NO generic `join` — relating two sets is `intersect` (set algebra)
or `traverse` (a link), and fuzzy matching (screening) is a Function. Caps (Palantir's,
load-tested): `traverse` ≤ 3 hops, `aggregate` ≤ 3 group_by dimensions.
"""

from __future__ import annotations

import json
import re
import uuid
from collections import Counter
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg

from src.ontology.resolution import screen_network
from src.orchestrator.coinvest import coinvestment_ties
from src.orchestrator.discrepancy import _HOME_PROPS, country_of
from src.orchestrator.frontier import subject_report
from src.orchestrator.monitor import match_condition
from src.orchestrator.neighborhoods import NEIGHBORHOOD, neighborhoods_of

# Named pure transforms a `collect` op may apply to a value. Kept tiny and neutral —
# `country` is the only domain helper, shared with the (soon-vestigial) discrepancy code.
_TRANSFORMS: dict[str, Any] = {
    "identity": lambda v: v,
    "country": country_of,
    "lower": lambda v: v.lower() if isinstance(v, str) else v,
}

# Functions — the escape hatch (Palantir's exact split: a small closed op set + arbitrary
# registered logic for anything the ops can't express). A read-model whose precision lives
# in domain logic — merge-aware cluster resolution, a platform-degree filter, multi-signal
# fuzzy matching — is a FUNCTION, not a worse pure-op rewrite. Registering it here lets a
# forkable composition REFERENCE it ({"op":"function","name":...}), so the opinion leaves
# engine code and becomes a named, listable, swappable artifact the user owns — without
# losing a drop of the analytics. The subject passed to `run_composition` is the function's
# anchor (an entity for coinvest/screen; a case for subject_report).
Function = Callable[[asyncpg.Pool, uuid.UUID | None, dict[str, Any]], Awaitable[Any]]


async def _fn_coinvest(pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]) -> Any:
    assert subject is not None  # the op guard requires a subject for this Function
    return await coinvestment_ties(
        pool, subject,
        limit=int(args.get("limit", 25)), platform_degree=int(args.get("platform_degree", 12)),
    )


async def _fn_subject_report(
    pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]
) -> Any:
    assert subject is not None
    return await subject_report(pool, subject)  # `subject` is the case id here


async def _fn_screen(pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]) -> Any:
    assert subject is not None
    return await screen_network(pool, subject, min_len=int(args.get("min_len", 5)))


# NB: `briefing` was a hand-written Function (three SQL queries). It is GONE — it decomposed
# into a `sections` op-tree (see BRIEFING below): each section is a pure select→table, so the
# "where am I?" read-model is now a composition the user owns, not bespoke code. This is the
# canonical proof that a "briefing"/"dossier" is a PAGE OF COMPOSITIONS, never a coded page.

_CANON_MAX_HITS = 12
_CANON_SNIPPET = 500


def _canon_sections(body: str) -> list[tuple[str, str]]:
    """Split a markdown reference into (heading, text) sections by `## ` headers. Text before
    the first `##` (minus the H1) is the '(overview)' section. Pure."""
    parts = re.split(r"^##\s+(.+)$", body, flags=re.M)
    out: list[tuple[str, str]] = []
    intro = re.sub(r"^#\s+.+$", "", parts[0], count=1, flags=re.M).strip()
    if intro:
        out.append(("(overview)", intro))
    for i in range(1, len(parts), 2):
        out.append((parts[i].strip(), parts[i + 1].strip() if i + 1 < len(parts) else ""))
    return out


def _trim(text: str) -> str:
    return text[:_CANON_SNIPPET] + ("…" if len(text) > _CANON_SNIPPET else "")


# evidence grades → rank weights for fn_search: search inherits the SAME epistemics as every
# other read surface — a deliberate ruling outranks a mined echo at equal textual relevance.
_GRADE_W = ("CASE a.evidence_class WHEN 'self_declared' THEN 1.0 "
            "WHEN 'authoritative_api' THEN 0.95 WHEN 'corroborated' THEN 0.9 "
            "WHEN 'direct_observation' THEN 0.8 WHEN 'derived' THEN 0.5 "
            "ELSE 0.35 END")


def _fuse_ranked(
    lex: list[dict[str, Any]], sem: list[dict[str, Any]], limit: int, *, k: int = 60,
) -> list[dict[str, Any]]:
    """Reciprocal-rank fusion of the lexical and semantic hit lists — scale-free (an FTS
    rank product and a cosine share no axis; their POSITIONS do). One row per object: a
    hit found by both doors keeps the lexical row (its snippet is a real headline) and is
    marked via='both'. Pure — the fusion policy is trivially testable."""
    scores: dict[str, float] = {}
    meta: dict[str, dict[str, Any]] = {}
    for pos, h in enumerate(lex):
        scores[h["id"]] = scores.get(h["id"], 0.0) + 1.0 / (k + pos + 1)
        meta.setdefault(h["id"], h)
    for pos, h in enumerate(sem):
        scores[h["id"]] = scores.get(h["id"], 0.0) + 1.0 / (k + pos + 1)
        if h["id"] in meta:
            meta[h["id"]]["via"] = "both"
        else:
            meta[h["id"]] = h
    out = sorted(meta.values(), key=lambda h: -scores[h["id"]])[:limit]
    for h in out:
        h["rank"] = round(scores[h["id"]], 6)
    return out


# python-side grade weights for the semantic door (the SQL door uses _GRADE_W — same table)
_GRADE_W_PY = {"self_declared": 1.0, "authoritative_api": 0.95, "corroborated": 0.9,
               "direct_observation": 0.8, "derived": 0.5}


# ═══════════ THE REFLECTION ACL — house-scoped memories (ruling 6c18709f) ═══════════
# A Reflection is a memory lived with the operator's agents, not work knowledge: readable
# within its OWN HOUSE and by the operator, opaque to other houses. Decisions, threads,
# canon, tensions, blind spots stay fleet-readable — cross-repo recall is the product; the
# boundary is reflections ONLY. Enforcement lives at the READ LENSES (this module is where
# every discovery read converges): the record itself stays append-only and whole — an ACL
# is a lens, never a delete. The caller rides a ContextVar so the recursive op evaluator
# (select, and any op stacked over it) inherits it without threading a parameter through
# every branch; run_spec sets it, _fn_search/_fn_lap fall back to it.

_ACL_CALLER: ContextVar[str | None] = ContextVar("_ACL_CALLER", default=None)
# the operator's own surfaces — the desk and the :8011 console self-identify as these
_OPERATOR_CALLERS = {"operator", "console"}


async def _caller_house(pool: asyncpg.Pool, caller: str | None) -> str | None:
    """The house a caller reads reflections as: '*' for the operator's own surfaces,
    None for an anonymous caller (reads NO reflections — an unmounted stranger has no
    house), else the caller's seat house (a seat belongs to a house across successions)
    falling back to its project label (most projects are their own house)."""
    if caller in _OPERATOR_CALLERS:
        return "*"
    if not caller:
        return None
    from src.orchestrator.agents import house_of
    from src.orchestrator.seats import held_seat
    bound = await held_seat(pool, caller)
    if bound and bound.get("house"):
        return str(bound["house"])
    return await house_of(pool, caller)


async def _visible_reflections(
    pool: asyncpg.Pool, ids: list[uuid.UUID], caller: str | None
) -> set[uuid.UUID]:
    """Which of these Reflection ids this caller may read: those whose in_repo project
    matches the caller's house (a reflection filed with no house inherits its home
    project's house — per the ruling; one filed with NO project at all is operator-only,
    the conservative default)."""
    if not ids:
        return set()
    house = await _caller_house(pool, caller)
    if house == "*":
        return set(ids)
    if house is None:
        return set()
    rows = await pool.fetch(
        "SELECT DISTINCT l.from_id AS id FROM links l JOIN objects p ON p.id = l.to_id "
        "WHERE l.from_id = ANY($1::uuid[]) AND l.type='in_repo' "
        "AND lower(regexp_replace(p.canonical, '^repo:', '')) = lower($2)",
        ids, house)
    return {r["id"] for r in rows}


async def _hide_foreign_reflections(
    pool: asyncpg.Pool, hits: list[dict[str, Any]], caller: str | None
) -> list[dict[str, Any]]:
    """Drop Reflection hits outside the caller's house from a search result — silently
    (a boundary that names what it hides has already leaked that it exists)."""
    refl = [uuid.UUID(h["id"]) for h in hits if h.get("type") == "Reflection"]
    if not refl:
        return hits
    ok = {str(i) for i in await _visible_reflections(pool, refl, caller)}
    return [h for h in hits if h.get("type") != "Reflection" or h["id"] in ok]


_ID_TOKEN_OUTER = re.compile(r"[0-9a-f][0-9a-f-]{5,35}")
_ID_TOKEN_HEX = re.compile(r"[0-9a-f]{6,32}")


def _is_id_token(word: str) -> bool:
    """A bare hex fragment (6-32 hex chars, dashes allowed) is a RULING/THREAD ID quoted
    by its prefix — never ordinary vocabulary (no real word fullmatches this). Pure, so
    both the whole-query and embedded-token cases share one definition of 'looks like an
    id'."""
    w = word.lower()
    return bool(_ID_TOKEN_OUTER.fullmatch(w) and _ID_TOKEN_HEX.fullmatch(w.replace("-", "")))


async def _fn_search(pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]) -> Any:
    """search v2, MAX LEVEL (operator ruling a0cfcca1) — ONE engine, FOUR doors, one fused
    answer. Lexical ladder: strict FTS (websearch AND) → OR-relaxation (any-term bags) →
    TRIGRAM (pg_trgm word_similarity: every query word must fuzzily appear — the typo door).
    Beside it, the SEMANTIC door: local static embeddings (semantics.py) cosine over the
    vector index, so meaning matches where words don't. Doors fuse by reciprocal rank
    (positions, not incomparable scores); grade × recency weight both sides — a deliberate
    ruling outranks a mined echo at equal relevance in EVERY door. Every hit carries
    TESTIMONY (field, source, grade, when, snippet, via) and every call lands in search_log
    with which doors answered (relaxed / fuzzy / semantic) — the quality telemetry this
    engine is judged by."""
    q = str(args.get("q", "")).strip()[:300]  # a 50KB paste is not a query
    limit = max(1, min(int(args.get("limit") or 15), 50))  # a negative limit is a PG error
    # the caller identifies itself in args (the MCP tool, the console bar); a composition
    # embedding search inherits run_spec's caller via the ACL contextvar
    caller = str(args.get("caller") or "") or _ACL_CALLER.get()
    if not q:
        return {"hits": [], "note": "pass q — words, phrases, or \"quoted phrases\""}
    # THE ID DOOR (Soundwave, msg 244: 'tonight's cross-referencing is all manual memory';
    # extended per Alfred V's repro, thread 4ffe0eb9): a hex TOKEN is a RULING/THREAD ID,
    # not vocabulary — a holder may quote it as the OBJECT's uuid prefix (dd27f61f…) or as
    # its CANONICAL short hash (thread:23423ff856ab — the fleet's actual quoting habit),
    # alone or embedded in a longer query ('dd27f61f succession torch'). Look it up by
    # prefix against BOTH forms, wherever the token sits in the query, and answer directly,
    # testimony included.
    words = [w for w in q.split() if w]
    id_candidates = list(dict.fromkeys(w.lower() for w in words if _is_id_token(w)))
    id_hits: list[dict[str, Any]] = []
    if id_candidates:
        idrows = await pool.fetch(
            "SELECT DISTINCT ON (o.id) o.id, o.type, o.canonical, a.value #>> '{}' AS text, "
            " a.source_id, a.evidence_class, a.observed_at "
            "FROM objects o LEFT JOIN current_assertions a ON a.object_id = o.id "
            " AND a.name IN ('summary','name') "
            "WHERE o.status = 'active' AND EXISTS ("
            "  SELECT 1 FROM unnest($1::text[]) AS frag "
            "  WHERE o.id::text LIKE frag || '%' "
            "     OR regexp_replace(o.canonical, '^[^:]+:', '') LIKE frag || '%') "
            "ORDER BY o.id, a.confidence DESC, a.observed_at DESC "
            "LIMIT $2", id_candidates, min(20, 5 * len(id_candidates)))
        if idrows:
            id_hits = [
                {"id": str(r["id"]), "type": r["type"], "canonical": r["canonical"],
                 "field": "id", "snippet": (r["text"] or r["canonical"])[:160],
                 **({"source": r["source_id"], "grade": r["evidence_class"],
                     "when": r["observed_at"].isoformat()} if r["source_id"] else {}),
                 "rank": 1.0, "via": "id"}
                for r in idrows]
            # the id door is still a read lens — knowing a reflection's id is not a key
            id_hits = await _hide_foreign_reflections(pool, id_hits, caller)
    if len(words) == 1 and id_hits:
        # the WHOLE query is one id token — answer directly, the legacy shape unchanged
        await pool.execute(
            "INSERT INTO search_log (query, caller, hits, top_rank, relaxed) "
            "VALUES ($1,$2,$3,1.0,false)", q, caller, len(id_hits))
        return {"hits": id_hits, "q": q, "note": "id-fragment lookup (prefix match)"}
    # an id token embedded in a longer query falls through here: the FTS ladder below
    # still runs on the FULL query (never regress the pure-FTS path) and id_hits merge in
    # ABOVE the fused text hits further down.
    # stopword-only / punctuation-only queries parse to an EMPTY tsquery: zero hits by
    # construction, not a recall failure — returning early keeps them OUT of the misses log
    # (they would poison the exact telemetry the embeddings tripwire reads) — UNLESS an id
    # token already answered part of the query, in which case that answer stands and the
    # (harmlessly empty) FTS leg below just contributes nothing further.
    if not id_hits and not await pool.fetchval(
            "SELECT websearch_to_tsquery('english', $1)::text", q):
        return {"hits": [], "q": q,
                "note": "query is all stopwords/punctuation — nothing to match (not logged)"}
    # rank inside `cand`, headline ONLY the surviving rows (`top`): ts_headline is the
    # expensive part and a broad query can match thousands of candidates. ts_rank
    # normalization 1 divides by 1+log(doc length) so a long rationale can't outrank a
    # short summary on term frequency alone.
    _SQL = (
        "WITH tq AS (SELECT websearch_to_tsquery('english', $1) AS v), "
        "cand AS ("
        "  SELECT DISTINCT ON (o.id) o.id, o.type, o.canonical, a.name AS field, "
        "   a.value #>> '{}' AS text, a.source_id, a.evidence_class, a.observed_at, "
        "   (ts_rank(to_tsvector('english', a.value #>> '{}'), tq.v, 1) * " + _GRADE_W + " * "
        "    (1.0 / (1.0 + EXTRACT(epoch FROM (now() - a.observed_at)) / 7776000.0)))::real "
        "     AS rank "
        "  FROM current_assertions a JOIN objects o ON o.id = a.object_id "
        "   AND o.status = 'active', tq "
        "  WHERE a.name IN ('name','summary','rationale') "
        "    AND to_tsvector('english', a.value #>> '{}') @@ tq.v "
        "  ORDER BY o.id, rank DESC), "
        "top AS (SELECT * FROM cand ORDER BY rank DESC LIMIT $2) "
        "SELECT top.*, ts_headline('english', top.text, tq.v, "
        "         'MaxWords=20, MinWords=8, MaxFragments=1') AS snippet "
        "FROM top, tq ORDER BY top.rank DESC")
    rows = await pool.fetch(_SQL, q, limit)
    relaxed = fuzzy = False
    # explicit syntax (quotes, OR, minus) means the asker KNEW the language — no door
    # behind this one may second-guess it (the OR-relaxation's law, inherited by trigram)
    # (`words` was already split off `q` by the id door above; q is unchanged since)
    plain_bag = ('"' not in q and " or " not in q.lower()
                 and not any(w.startswith("-") for w in words))  # a leading '-' is NOT
    # syntax; an inner hyphen (hands-free, a-sibling) is just a word
    if not rows and len(words) > 1 and plain_bag:
        # PROGRESSIVE RELAXATION (field report, agent e46a657e-ii, msg 124): websearch
        # semantics AND every term, so a keyword BAG ('Hector background skills experience
        # projects') needs all of them in ONE document — zero by construction, and the
        # docstring promises bags work. When strict-AND finds nothing and the query is a
        # plain multi-word bag, retry as ANY-term — RARITY-WEIGHTED (thread 15b976ce):
        # plain ts_rank over an OR query ranked flat, so a common word ('set') outranked
        # a distinctive one ('HTL') on every bag. Each candidate now scores the SUMMED
        # idf of the words it matches — one rare word beats three ubiquitous ones, and a
        # word in every document contributes exactly zero.
        or_q = " OR ".join(words)
        if await pool.fetchval(
                "SELECT websearch_to_tsquery('english', $1)::text", or_q):
            rows = await pool.fetch(_RELAX_SQL, [w.lower() for w in words], or_q, limit)
            relaxed = bool(rows)
    if not rows and plain_bag:
        # THE TRIGRAM DOOR (max-level, a0cfcca1): a misspelled word survives no tsquery —
        # 'compositon' matches nothing lexically forever. word_similarity is strict-AND
        # with typo tolerance: EVERY query word must fuzzily appear somewhere in the text.
        # Last lexical rung: full-scan word_similarity is fine at this corpus size and
        # only runs when both exact doors missed.
        trgm_words = [w.lower() for w in re.findall(r"[a-z0-9][a-z0-9-]{2,}", q.lower())]
        if trgm_words:
            rows = await pool.fetch(_TRGM_SQL, trgm_words, limit)
            fuzzy = bool(rows)
    lex_hits = [
        {"id": str(r["id"]), "type": r["type"], "canonical": r["canonical"],
         "field": r["field"], "snippet": r["snippet"], "source": r["source_id"],
         "grade": r["evidence_class"], "when": r["observed_at"].isoformat(),
         "rank": round(float(r["rank"]), 6), "via": "fuzzy" if fuzzy else "lexical"}
        for r in rows
    ]
    # THE SEMANTIC DOOR runs beside the lexical ladder, never instead of it: meaning
    # matches where words don't ('model downgrade' → the warm-swap rulings). Closed (no
    # embedder / empty index / any error) it contributes nothing and costs nothing.
    sem_hits = await _semantic_hits(pool, q, limit)
    hits = _fuse_ranked(lex_hits, sem_hits, limit)
    if id_hits:
        # an id token embedded in the query found an exact match — it leads, the fused
        # textual/semantic hits follow beneath it, deduped so nothing appears twice
        seen_ids = {h["id"] for h in id_hits}
        hits = id_hits + [h for h in hits if h["id"] not in seen_ids]
        hits = hits[:limit]
    # the house boundary (6c18709f): every door's rows converge here — one filter covers
    # strict, relaxed, trigram and semantic alike (id_hits were already filtered when
    # computed, so re-filtering here is a no-op for them and real work for the rest)
    hits = await _hide_foreign_reflections(pool, hits, caller)
    semantic = any(h["via"] in ("semantic", "both") for h in hits)
    # a superseded decision must not read as live testimony (the supersedes verb,
    # dd04d7dd): one batched lookup marks such hits — still findable, honestly flagged
    if hits:
        buried = {str(r["object_id"]): r["v"] for r in await pool.fetch(
            "SELECT DISTINCT ON (object_id) object_id, value #>> '{}' AS v "
            "FROM current_assertions WHERE name='superseded_by' "
            "AND object_id = ANY($1::uuid[]) "
            "ORDER BY object_id, confidence DESC, observed_at DESC",
            [h["id"] for h in hits]) if (r["v"] or "").strip()}
        for h in hits:
            if h["id"] in buried:
                h["superseded"] = f"by decision {buried[h['id']][:8]} — read the successor"
    await pool.execute(
        "INSERT INTO search_log (query, caller, hits, top_rank, relaxed, fuzzy, semantic) "
        "VALUES ($1,$2,$3,$4,$5,$6,$7)",
        q, caller, len(hits), (hits[0]["rank"] if hits else None), relaxed, fuzzy, semantic)
    # opportunistic retention: telemetry keeps 90 days (indexed delete, usually 0 rows)
    await pool.execute(
        "DELETE FROM search_log WHERE searched_at < now() - interval '90 days'")
    return {"hits": hits, "q": q,
            **({"note": "strict match (ALL terms) found nothing — these hits match ANY term, "
                        "best-covered first"} if relaxed and not fuzzy else {}),
            **({"note": "exact matches found nothing — these are spelling-tolerant "
                        "(trigram) matches"} if fuzzy else {}),
            **({"note": "no hits — logged; the zero-hit rate is watched"} if not hits else {})}


# THE RARITY-WEIGHTED RELAXATION (thread 15b976ce): the ANY-term retry's rank is the sum
# of ln(N/df) over the query words each candidate matches — df computed against the same
# corpus the door searches, in the same statement (the corpus CTE materializes once, and
# this leg only runs when strict-AND found nothing). Stopwords parse to empty tsqueries
# and drop out; a word present in EVERY document scores ln(1)=0 by construction. The
# snippet still headlines with the OR query, and grade × recency weigh exactly as the
# strict door does.
_RELAX_SQL = (
    "WITH words AS (SELECT DISTINCT lower(w) AS w FROM unnest($1::text[]) AS w), "
    "corpus AS ("
    "  SELECT o.id, o.type, o.canonical, a.name AS field, "
    "   a.value #>> '{}' AS text, a.source_id, a.evidence_class, a.observed_at, "
    "   to_tsvector('english', a.value #>> '{}') AS tv, " + _GRADE_W + " AS gw "
    "  FROM current_assertions a JOIN objects o ON o.id = a.object_id "
    "   AND o.status = 'active' "
    "  WHERE a.name IN ('name','summary','rationale')), "
    "n AS (SELECT count(*)::float + 1 AS total FROM corpus), "
    "df AS (SELECT w.w, count(*) + 1 AS d FROM words w "
    "       JOIN corpus c ON c.tv @@ plainto_tsquery('english', w.w) GROUP BY w.w), "
    "cand AS ("
    "  SELECT DISTINCT ON (c.id) c.id, c.type, c.canonical, c.field, c.text, "
    "   c.source_id, c.evidence_class, c.observed_at, "
    "   ((SELECT sum(ln(n.total / df.d)) FROM df, n "
    "     WHERE c.tv @@ plainto_tsquery('english', df.w)) * c.gw * "
    "    (1.0 / (1.0 + EXTRACT(epoch FROM (now() - c.observed_at)) / 7776000.0)))::real "
    "     AS rank "
    "  FROM corpus c "
    "  WHERE EXISTS (SELECT 1 FROM df WHERE c.tv @@ plainto_tsquery('english', df.w)) "
    "  ORDER BY c.id, rank DESC), "
    "top AS (SELECT * FROM cand ORDER BY rank DESC LIMIT $3) "
    "SELECT top.*, ts_headline('english', top.text, websearch_to_tsquery('english', $2), "
    "         'MaxWords=20, MinWords=8, MaxFragments=1') AS snippet "
    "FROM top ORDER BY top.rank DESC")


_TRGM_SQL = (
    "WITH words AS (SELECT unnest($1::text[]) AS w), "
    "cand AS ("
    "  SELECT DISTINCT ON (o.id) o.id, o.type, o.canonical, a.name AS field, "
    "   a.value #>> '{}' AS text, a.source_id, a.evidence_class, a.observed_at, "
    "   ((SELECT avg(word_similarity(words.w, a.value #>> '{}')) FROM words) * "
    + _GRADE_W + " * "
    "    (1.0 / (1.0 + EXTRACT(epoch FROM (now() - a.observed_at)) / 7776000.0)))::real "
    "     AS rank "
    "  FROM current_assertions a JOIN objects o ON o.id = a.object_id "
    "   AND o.status = 'active' "
    "  WHERE a.name IN ('name','summary','rationale') "
    "    AND (SELECT min(word_similarity(words.w, a.value #>> '{}')) FROM words) > 0.4 "
    "  ORDER BY o.id, rank DESC), "
    "top AS (SELECT * FROM cand ORDER BY rank DESC LIMIT $2) "
    "SELECT top.*, left(top.text, 160) AS snippet FROM top ORDER BY top.rank DESC")


async def _semantic_hits(pool: asyncpg.Pool, q: str, limit: int) -> list[dict[str, Any]]:
    """The semantic door's hit list, testimony included: cosine candidates hydrated with
    the winner assertion behind each (object, field), ordered cos × grade × recency —
    the same epistemics as the SQL doors, applied python-side. [] when the door is closed."""
    from src.orchestrator import semantics

    embedder = semantics.resolve_embedder()
    if embedder is None:
        return []
    cands = await semantics.semantic_candidates(pool, embedder, q, k=limit * 2)
    if not cands:
        return []
    rows = await pool.fetch(
        "SELECT DISTINCT ON (o.id, a.name) o.id, o.type, o.canonical, a.name AS field, "
        " a.value #>> '{}' AS text, a.source_id, a.evidence_class, a.observed_at "
        "FROM current_assertions a JOIN objects o ON o.id = a.object_id "
        "JOIN unnest($1::uuid[], $2::text[]) AS want(oid, fld) "
        "  ON want.oid = o.id AND want.fld = a.name "
        "WHERE o.status = 'active' "
        "ORDER BY o.id, a.name, a.confidence DESC, a.observed_at DESC",
        [c["object_id"] for c in cands], [c["field"] for c in cands])
    by_key = {(r["id"], r["field"]): r for r in rows}
    out: list[dict[str, Any]] = []
    now = datetime.now(UTC)
    for c in cands:
        r = by_key.get((c["object_id"], c["field"]))
        if r is None:
            continue  # the index lags the graph by one backfill — skip, never invent
        age_days = max(0.0, (now - r["observed_at"]).total_seconds() / 86400.0)
        w = (c["cos"] * _GRADE_W_PY.get(r["evidence_class"], 0.35)
             * (1.0 / (1.0 + age_days / 90.0)))
        out.append(
            {"id": str(r["id"]), "type": r["type"], "canonical": r["canonical"],
             "field": r["field"], "snippet": (r["text"] or "")[:160],
             "source": r["source_id"], "grade": r["evidence_class"],
             "when": r["observed_at"].isoformat(), "rank": round(w, 6), "via": "semantic"})
    out.sort(key=lambda h: -h["rank"])
    seen: set[str] = set()  # one row per object, best field wins (mirrors DISTINCT ON o.id)
    deduped = []
    for h in out:
        if h["id"] not in seen:
            seen.add(h["id"])
            deduped.append(h)
    return deduped


async def _fn_canon(pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]) -> Any:
    """Consult the design canon — Palantir/Notion's models + Osiris's own docs, ingested as
    `Reference` objects (src/ingest/reference.py). 'Cite, don't re-derive': given a query (a
    topic, a `grounds` module path, or a design word), return the matching canon SECTIONS
    ranked, each with its source + the module it grounds. Empty query → the canon index (one
    overview row per reference). Subject-FREE — it answers a design question, not an entity;
    this is what a designer (human or Claude, via `consult_canon`) calls BEFORE re-deriving a
    problem Palantir/Notion already solved (the closed op set, aggregation caps, the kinetic
    write path, the renderer's view rules)."""
    q = str(args.get("q", "")).strip().lower()
    scope = str(args.get("project", "") or "").strip().lower()
    refs = await pool.fetch(
        "SELECT o.canonical AS canonical, "
        " (SELECT value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='name' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS title, "
        " (SELECT value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='vendor' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS vendor, "
        " (SELECT value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='topic' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS topic, "
        " (SELECT value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='grounds' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS grounds, "
        " (SELECT value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='source_url' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS source, "
        " (SELECT value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='body' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS body "
        "FROM objects o WHERE o.type='Reference' AND o.status='active'"
    )
    # scope: the shared design canon (vendor-tagged) is visible to everyone; UNVENDORED project
    # history (ref:<project>-*) is visible only to its OWN project — a caller never bleeds another
    # repo's memory. Unscoped (no project) searches everything, as before.
    if scope:
        pref = f"ref:{scope}-"
        refs = [r for r in refs if r["vendor"] or (r["canonical"] or "").startswith(pref)]
    if not q:  # the index — one overview row per reference, ordered by vendor then title
        index = []
        for r in sorted(refs, key=lambda x: (x["vendor"] or "", x["title"] or "")):
            secs = _canon_sections(r["body"] or "")
            index.append({"reference": r["title"], "vendor": r["vendor"],
                          "grounds": r["grounds"], "source": r["source"],
                          "text": _trim(secs[0][1]) if secs else ""})
        return {"Design canon — Palantir · Notion · own docs": index}
    # rank by how many QUERY TERMS hit (meta > heading > body), NOT a whole-string match — a
    # natural multi-word query (e.g. migrated project history recall) matched nothing contiguously
    # and returned empty, silently breaking the migration's promised bounded-query recall path.
    terms = [t for t in re.split(r"[^a-z0-9_]+", q) if len(t) >= 3] or [q]
    hits: list[tuple[int, dict[str, Any]]] = []
    for r in refs:
        meta = " ".join(
            filter(None, [r["title"], r["topic"], r["vendor"], r["grounds"]])).lower()
        for heading, text in _canon_sections(r["body"] or ""):
            head, txt = heading.lower(), text.lower()
            score = sum((3 if t in meta else 0) + (2 if t in head else 0)
                        + (1 if t in txt else 0) for t in terms)
            if q in head or q in txt:  # exact contiguous phrase is a strong precision signal
                score += 4
            if score <= 0:
                continue
            hits.append((score, {
                "reference": r["title"], "vendor": r["vendor"], "grounds": r["grounds"],
                "section": heading, "source": r["source"], "text": _trim(text),
            }))
    hits.sort(key=lambda h: h[0], reverse=True)
    return {f'Canon — "{q}"': [h[1] for h in hits[:_CANON_MAX_HITS]]}


# NB: `decisions` was a hand-written Function (a SQL query joining decided_in → commit). It is
# GONE — it decomposed into DECISION_LOG (a `sections` op-tree): select Decision (summary-present)
# → table with `of:"first"` rollups plucking the decided_in commit's canonical + date → order.
# The show-original rollup is what made this possible without abusing max(). A kind filter is now
# a `where` on the select the user forks in, not a bespoke arg.


# config roles where two repos' files SHOULD agree in CONTENT (legal/config), unlike prose
# (readme/changelog/contributing) which are expected to differ — so the drift audit skips prose.
_CONFIG_ROLES = frozenset(
    {"license", "gitignore", "editorconfig", "makefile", "ci", "manifest", "dockerfile"})


async def _family_repos(pool: asyncpg.Pool, args: dict[str, Any]) -> dict[uuid.UUID, str]:
    """Repos in scope for a family audit — those with an ingested file tree, optionally filtered
    to `args.repos`. Returns {repo_id: name}; a family needs ≥2 (caller guards)."""
    want = {str(w).lower() for w in (args.get("repos") or [])}
    repos = await pool.fetch(
        "SELECT o.id, (SELECT value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='name' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS name "
        "FROM objects o WHERE o.type='SoftwareProject' AND o.status='active'")
    have_files = {r["repo"] for r in await pool.fetch(
        "SELECT DISTINCT l.to_id AS repo FROM links l "
        "JOIN objects f ON f.id=l.from_id AND f.type='File' WHERE l.type='in_repo'")}
    return {r["id"]: r["name"] for r in repos
            if r["name"] and r["id"] in have_files and (not want or r["name"].lower() in want)}


async def _fn_family(
    pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]
) -> Any:
    """Family consistency audit — make a set of similar repos a real FAMILY by auditing what
    drifted across them. Compares repos by file ROLE (license/readme/ci/manifest/…): for each
    role, which repos have it and which LACK it. A role present in some-but-not-all is an
    inconsistency (a missing license, a CI one repo skipped). Subject-free; `args.repos` scopes
    the family (default: every repo whose tree is ingested). CANDIDATE-GATED — blocks by role,
    never all-files-all-pairs — so it holds as the family grows. (Presence audit; content drift
    is `family_drift`.)"""
    rmap = await _family_repos(pool, args)
    if len(rmap) < 2:
        return {"Family consistency": [{"note": "need ≥2 repos with ingested file trees"}]}
    pairs = await pool.fetch(
        "SELECT DISTINCT l.to_id AS repo, a.value #>> '{}' AS role "
        "FROM links l JOIN objects f ON f.id=l.from_id AND f.type='File' "
        "JOIN current_assertions a ON a.object_id=f.id AND a.name='role' "
        "WHERE l.type='in_repo' AND l.to_id = ANY($1::uuid[])", list(rmap))
    role_repos: dict[str, set[uuid.UUID]] = {}
    for p in pairs:
        role_repos.setdefault(p["role"], set()).add(p["repo"])
    everyone = set(rmap)
    rows = []
    for role in role_repos:
        missing = everyone - role_repos[role]
        rows.append({"role": role,
                     "have": ", ".join(sorted(rmap[i] for i in role_repos[role])),
                     "missing": ", ".join(sorted(rmap[i] for i in missing)) or "—",
                     "consistent": not missing})
    rows.sort(key=lambda r: (r["consistent"], r["role"]))   # inconsistencies (the findings) first
    return {f"Family consistency — {len(rmap)} repos ({', '.join(sorted(rmap.values()))})": rows}


async def _fn_family_drift(
    pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]
) -> Any:
    """Content-drift audit — the deeper layer over the presence audit. For the files a family
    SHARES, do they AGREE? License TYPE across the family (MIT in one repo, Apache in another is
    a real inconsistency), and config files (gitignore/ci/editorconfig/makefile/manifest/
    dockerfile) byte-identical or diverged. Prose roles (readme/changelog/contributing) are
    EXPECTED to differ, so they're excluded. Reads pre-computed hashes/license-types from the
    graph (content stays in git). Role-blocked, so it scales like the presence audit."""
    rmap = await _family_repos(pool, args)
    if len(rmap) < 2:
        return {"Family content drift": [{"note": "need ≥2 repos with ingested file trees"}]}
    rows = await pool.fetch(
        "SELECT l.to_id AS repo, "
        " (SELECT value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=f.id AND a.name='role' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS role, "
        " (SELECT value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=f.id AND a.name='content_hash' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS h, "
        " (SELECT value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=f.id AND a.name='license_type' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS lt "
        "FROM links l JOIN objects f ON f.id=l.from_id AND f.type='File' "
        "WHERE l.type='in_repo' AND l.to_id = ANY($1::uuid[])", list(rmap))
    # role -> repo -> [hashes]; a repo may have several files of one role (e.g. CI workflows),
    # so its signature is all of them combined — then signatures compare across repos.
    sigs: dict[str, dict[uuid.UUID, list[str]]] = {}
    ltypes: dict[uuid.UUID, set[str]] = {}
    for r in rows:
        if r["role"] in _CONFIG_ROLES and r["h"]:
            sigs.setdefault(r["role"], {}).setdefault(r["repo"], []).append(r["h"])
            if r["role"] == "license" and r["lt"]:
                ltypes.setdefault(r["repo"], set()).add(r["lt"])
    out = []
    for role, per in sigs.items():
        if len(per) < 2:               # only one repo has it → presence audit's job, not drift
            continue
        if role == "license":
            types = {rmap[rid]: "/".join(sorted(ltypes.get(rid) or {"unknown"})) for rid in per}
            drift = len(set(types.values())) > 1
            detail = ", ".join(f"{r}: {t}" for r, t in sorted(types.items()))
        else:
            signatures = {rid: "|".join(sorted(hs)) for rid, hs in per.items()}
            distinct = len(set(signatures.values()))
            drift = distinct > 1
            repos_l = ", ".join(sorted(rmap[rid] for rid in per))
            detail = (f"{distinct} distinct versions across {repos_l}" if drift
                      else f"identical across {repos_l}")
        out.append({"role": role, "drift": drift, "detail": detail})
    out.sort(key=lambda x: (not x["drift"], x["role"]))       # drift (the findings) first
    return {f"Family content drift — {len(rmap)} repos": out}


async def _fn_project(
    pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]
) -> Any:
    """One project's scope — its recent commits, decisions, and file roles, as lists. The drill
    from the project browser (`projects`). The subject is the repo (focus it, or pass
    `args.repo` = the name). Volume as lists, never a graph."""
    repo = subject
    if repo is None and args.get("repo"):
        repo = await pool.fetchval(
            "SELECT o.id FROM objects o JOIN current_assertions a ON a.object_id=o.id "
            "WHERE o.type='SoftwareProject' AND a.name='name' AND a.value #>> '{}'=$1 LIMIT 1",
            str(args["repo"]))
    if repo is None:
        return {"Project": [{"note": "focus a repo, or pass args.repo = its name"}]}
    name = await pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name='name' "
        "ORDER BY confidence DESC, observed_at DESC LIMIT 1",
        repo) or "(project)"
    commits = await pool.fetch(
        "SELECT (SELECT value #>> '{}' FROM current_assertions a WHERE a.object_id=c.id "
        "        AND a.name='summary' "
        "        ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS summary, "
        " (SELECT value #>> '{}' FROM current_assertions a WHERE a.object_id=c.id "
        "        AND a.name='authored_date' "
        "        ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS date "
        "FROM links l JOIN objects c ON c.id=l.from_id AND c.type='Commit' "
        "WHERE l.to_id=$1 AND l.type='in_repo' ORDER BY date DESC NULLS LAST LIMIT 15", repo)
    decisions = await pool.fetch(
        "SELECT DISTINCT ON (d.id) "
        " (SELECT value #>> '{}' FROM current_assertions a WHERE a.object_id=d.id "
        "  AND a.name='summary' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS summary, "
        " (SELECT value #>> '{}' FROM current_assertions a WHERE a.object_id=d.id "
        "  AND a.name='kind' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS kind "
        "FROM objects d JOIN links dl ON dl.from_id=d.id AND dl.type='decided_in' "
        "JOIN links rl ON rl.from_id=dl.to_id AND rl.type='in_repo' AND rl.to_id=$1 "
        "WHERE d.type='Decision' LIMIT 20", repo)
    roles = await pool.fetch(
        "SELECT DISTINCT (SELECT value #>> '{}' FROM current_assertions a WHERE a.object_id=f.id "
        "        AND a.name='role' "
        "        ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS role "
        "FROM links l JOIN objects f ON f.id=l.from_id AND f.type='File' "
        "WHERE l.to_id=$1 AND l.type='in_repo'", repo)
    role_list = sorted(r["role"] for r in roles if r["role"])
    return {
        f"{name} — recent commits": [
            {"when": (c["date"] or "")[:10], "commit": c["summary"]}
            for c in commits if c["summary"]],
        f"{name} — decisions": [
            {"kind": d["kind"], "decision": d["summary"]} for d in decisions if d["summary"]],
        f"{name} — file roles": [{"roles": ", ".join(role_list) or "—"}],
    }


_WORD = re.compile(r"[a-z][a-z0-9_+]{3,}")   # a term ≥4 chars — the derivation's unit
_EXT = re.compile(r"\.([a-z0-9]{1,6})$")     # a file extension


async def _fn_portfolio(
    pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]
) -> Any:
    """The developer's project portfolio, DERIVED — for each ingested repo: its STACK (top file
    types), what it's ABOUT (the terms DISTINCTIVE to it), and its size. The substrate half of
    cross-repo cognition: it gathers the candidate signal deterministically; NAMING the shared
    primitive that recurs across repos is a judgment left to the lens (Claude on the MCP) or the
    tripwire (a small model on the cron) — gather here, judge there.

    'Distinctive' = LOW document-frequency (a term in ≤ ~a third of repos), which is the whole
    trick: the terms every repo shares are generic noise ('config', 'test', 'while'); the signal
    is in the rare terms that name a repo ('cgroup'/'metered' → a throttle; 'miracast'/'airplay'
    → a cast receiver). So generic words fall out FOR FREE with no stoplist — the same 'never
    the whole flat corpus, only the distinctive bounded set' law the resolver uses. Subject-free;
    `args.repos` scopes it, `args.terms` caps the per-repo term count (default 10)."""
    want = {str(w).lower() for w in (args.get("repos") or [])}
    rmap = {r["id"]: r["name"] for r in await pool.fetch(
        "SELECT o.id, (SELECT value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='name' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS name "
        "FROM objects o WHERE o.type='SoftwareProject' AND o.status='active'")
        if r["name"] and (not want or r["name"].lower() in want)}
    if not rmap:
        return {"Portfolio": [{"note": "no ingested repos"}]}

    exts: dict[uuid.UUID, Counter[str]] = {rid: Counter() for rid in rmap}
    for r in await pool.fetch(
        "SELECT l.to_id AS repo, o.canonical FROM links l "
        "JOIN objects o ON o.id=l.from_id AND o.type='File' "
        "WHERE l.type='in_repo' AND l.to_id = ANY($1::uuid[])", list(rmap)):
        m = _EXT.search((r["canonical"] or "").lower())
        if m and r["repo"] in exts:
            exts[r["repo"]][m.group(1)] += 1

    tf: dict[uuid.UUID, Counter[str]] = {rid: Counter() for rid in rmap}
    ncommits: Counter[uuid.UUID] = Counter()
    for r in await pool.fetch(
        "SELECT l.to_id AS repo, "
        " (SELECT value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='summary' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS s, "
        " (SELECT value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='rationale' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS r "
        "FROM links l JOIN objects o ON o.id=l.from_id AND o.type='Commit' "
        "WHERE l.type='in_repo' AND l.to_id = ANY($1::uuid[])", list(rmap)):
        if r["repo"] not in tf:
            continue
        ncommits[r["repo"]] += 1
        for t in set(_WORD.findall(f"{r['s'] or ''} {r['r'] or ''}".lower())):
            tf[r["repo"]][t] += 1

    docfreq: Counter[str] = Counter()
    for c in tf.values():
        docfreq.update(c.keys())
    cutoff = max(2, round(len(rmap) * 0.35))     # distinctive: in ≤ ~a third of repos
    cap = max(1, int(args.get("terms", 10)))

    rows = []
    for rid, name in rmap.items():
        distinctive = sorted(
            (t for t in tf[rid] if docfreq[t] <= cutoff and tf[rid][t] >= 2),
            key=lambda t: (docfreq[t], -tf[rid][t]))       # rarest-across-repos first
        rows.append({
            "repo": name,
            "stack": ", ".join(e for e, _ in exts[rid].most_common(4)) or "—",
            "about": ", ".join(distinctive[:cap]) or "—",
            "commits": ncommits[rid],
        })
    rows.sort(key=lambda x: -x["commits"])
    return {f"Portfolio — {len(rmap)} repos": rows}


# The digest reads the pulse LOG on demand, so its liveness verdict is re-derived every time
# you look — the loop can die (the daemon stops) without the dead-man's-switch dying with it.
# A newest-pulse older than this ⇒ the heartbeat is DEAD, surfaced as the lead row.
_PULSE_STALE = timedelta(minutes=45)
# Default window: aggregate findings across the last 24h of pulses, ANCHORED at the most recent
# one (so a stale/dead loop still shows what changed before it stopped) — not just the single
# last tick. Bounded so a fast `--watch` loop can't return thousands of rows.
_PULSE_WINDOW = timedelta(hours=24)
_PULSE_CAP = 200
_PULSE_TITLE = "Pulse — what changed while you were away"


def _ago(delta: timedelta) -> str:
    """A compact human age: 'under a minute' / 'N minutes' / 'N hours' / 'N days'."""
    mins = max(0, int(delta.total_seconds())) // 60
    if mins < 1:
        return "under a minute"
    if mins < 60:
        return f"{mins} minute{'s' if mins != 1 else ''}"
    hours = mins // 60
    if hours < 24:
        return f"{hours} hour{'s' if hours != 1 else ''}"
    days = hours // 24
    return f"{days} day{'s' if days != 1 else ''}"


def _pulse_now(args: dict[str, Any]) -> datetime:
    """'Now' for the staleness verdict — overridable (a datetime or ISO string) so tests can
    freeze it; production reads the wall clock ON EACH LOOK (that IS the dead-man's-switch)."""
    v = args.get("now")
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=UTC)
    if isinstance(v, str):
        try:
            d = datetime.fromisoformat(v)
            return d if d.tzinfo else d.replace(tzinfo=UTC)
        except ValueError:
            pass
    return datetime.now(UTC)


async def _fn_pulse(
    pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]
) -> Any:
    """The heartbeat digest — what the off-the-clock loop (src/orchestrator/pulse.py) found
    while you were away, aggregated across a real WINDOW of pulses (newest first), not just the
    last tick. (The old bug: `LIMIT 1` read only the newest pulse, so a quiet last tick lied
    'no pulse yet' with a full log, and a loop that DIED with its daemon looked like one that
    never ran.) It ALWAYS leads with a heartbeat-status row derived ON READ: a newest pulse
    older than ~45 min ⇒ the loop is DEAD — the lens re-checks liveness every look, so it can't
    die with the daemon (the dead-man's-switch). `args.last` overrides the window to the last N
    pulses; default = the last 24h of pulses (bounded)."""
    override = args.get("last")
    cap = max(1, int(override)) if override is not None else _PULSE_CAP
    rows = await pool.fetch(
        "SELECT ran_at, synced, findings FROM dev_pulses ORDER BY id DESC LIMIT $1", cap)
    if not rows:                                    # the loop has NEVER run — keep the how-to row
        return {_PULSE_TITLE: [
            {"finding": "no pulse yet — run `python -m src.orchestrator.pulse`",
             "when": "—", "synced": "—"}]}

    last_ran = rows[0]["ran_at"]
    age = _pulse_now(args) - last_ran
    if age > _PULSE_STALE:                          # DEAD — lead with the dead-since row
        status = {"finding": f"heartbeat DEAD — no pulse since {str(last_ran)[:19]} "
                             f"({_ago(age)} ago); the loop is not running",
                  "when": str(last_ran)[:19], "synced": "—"}
    else:
        status = {"finding": f"heartbeat alive — last pulse {_ago(age)} ago",
                  "when": str(last_ran)[:19], "synced": "—"}

    # explicit `last` ⇒ exactly those N pulses; else the last 24h anchored at the newest pulse.
    window = rows if override is not None else [
        r for r in rows if r["ran_at"] >= last_ran - _PULSE_WINDOW]
    findings: list[dict[str, Any]] = []
    for r in window:
        for finding in (_coerce(r["findings"]) or []):
            findings.append({"finding": finding, "when": str(r["ran_at"])[:19],
                             "synced": ", ".join(_coerce(r["synced"]) or []) or "—"})
    if not findings:                                # pulses ran, nothing changed in the window
        findings = [{"finding": f"quiet — no changes across {len(window)} pulse"
                                f"{'s' if len(window) != 1 else ''} since "
                                f"{str(window[-1]['ran_at'])[:19]}",
                     "when": str(last_ran)[:19], "synced": "—"}]
    return {_PULSE_TITLE: [status, *findings]}


async def resolve_ref(pool: asyncpg.Pool, ref: str) -> uuid.UUID | None:
    """Accept a UUID, an exact canonical, or a name; resolve to an object id. Name matching
    tries exact first (most-described wins), then substring (shortest name wins — closest to
    the query). ONE definition shared by the server's tools and the composition functions,
    so the console and an agent always resolve the same words to the same object."""
    try:
        return uuid.UUID(ref)
    except ValueError:
        pass
    oid = await pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1 AND status='active' LIMIT 1", ref)
    if oid is not None:
        return uuid.UUID(str(oid))
    for predicate, order in (
        ("lower(a.value #>> '{}') = lower($1)",
         "(SELECT count(*) FROM current_assertions x WHERE x.object_id=a.object_id) DESC"),
        ("a.value #>> '{}' ILIKE '%'||$1||'%'",
         "length(a.value #>> '{}') ASC"),
    ):
        row = await pool.fetchval(
            "SELECT a.object_id FROM current_assertions a "
            "JOIN objects o ON o.id=a.object_id AND o.status='active' "
            f"WHERE a.name='name' AND {predicate} ORDER BY {order} LIMIT 1",
            ref,
        )
        if row is not None:
            return uuid.UUID(str(row))
    return None


_LAP_CELL = 200  # a timeline cell shows enough to recognize the assertion, not the essay


def _cell(v: str | None) -> str:
    s = v or ""
    return s[:_LAP_CELL] + ("…" if len(s) > _LAP_CELL else "")


async def _fn_lap(pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]) -> Any:
    """rung 3 — the LAP LENS (campaign 5c57f54d): ONE object's full provenance timeline.
    Every assertion (with its supersession fate), every link (both directions, retractions
    marked), every kernel event, in observed order, each carrying source + grade +
    confidence. search answers WHAT the graph knows; lap answers HOW IT CAME TO KNOW IT —
    the Palantir half of the knowledge layer. Pure SQL, no LLM. `ref` = uuid | canonical |
    name (a uuid also reaches merged/retired corpses — the timeline is exactly where you
    autopsy them); `limit` keeps the newest N entries and REPORTS what it dropped."""
    ref = str(args.get("ref") or "").strip()
    oid = (await resolve_ref(pool, ref)) if ref else subject
    if oid is None:
        return {"note": (f"nothing matches {ref!r}" if ref else
                         "pass ref=<uuid|canonical|name> (or focus a subject) — "
                         "lap answers for ONE object")}
    head = await pool.fetchrow(
        "SELECT id, type, canonical, status, merged_into, created_at "
        "FROM objects WHERE id=$1", oid)
    if head is None:
        return {"note": f"no object {oid}"}
    # the house boundary (6c18709f): lap is the deepest read lens of all — it serves the
    # BODY. A foreign house's reflection answers exactly like a missing object (a boundary
    # that names what it hides has already leaked that it exists).
    if head["type"] == "Reflection":
        caller = str(args.get("caller") or "") or _ACL_CALLER.get()
        if oid not in await _visible_reflections(pool, [oid], caller):
            return {"note": (f"nothing matches {ref!r}" if ref else f"no object {oid}")}
    limit = max(1, min(int(args.get("limit") or 200), 1000))
    a_rows = await pool.fetch(
        "SELECT a.name, a.value #>> '{}' AS v, a.source_id, a.evidence_class, a.confidence, "
        " a.observed_at, EXISTS(SELECT 1 FROM assertions s WHERE s.supersedes=a.id) AS dead "
        "FROM assertions a WHERE a.object_id=$1 ORDER BY a.observed_at, a.id", oid)
    l_rows = await pool.fetch(
        "SELECT l.type, l.source_id, l.evidence_class, l.first_seen, l.valid_until, "
        " CASE WHEN l.from_id=$1 THEN 'out' ELSE 'in' END AS dir, o.canonical AS other "
        "FROM links l JOIN objects o "
        " ON o.id = CASE WHEN l.from_id=$1 THEN l.to_id ELSE l.from_id END "
        "WHERE l.from_id=$1 OR l.to_id=$1 ORDER BY l.first_seen, l.id", oid)
    e_rows = await pool.fetch(
        "SELECT event_type, actor, related_id, created_at, "
        " (object_id <> $1) AS witnessed "  # an event RECORDED elsewhere that names this object
        "FROM object_events WHERE object_id=$1 OR related_id=$1 "
        "ORDER BY created_at, id", oid)
    timeline: list[dict[str, Any]] = []
    for a in a_rows:
        timeline.append({"at": a["observed_at"].isoformat(), "kind": "assert",
                         "field": a["name"], "value": _cell(a["v"]),
                         "source": a["source_id"], "grade": a["evidence_class"],
                         "confidence": round(float(a["confidence"]), 3),
                         **({"superseded": True} if a["dead"] else {})})
    for ln in l_rows:
        timeline.append({"at": ln["first_seen"].isoformat(), "kind": f"link-{ln['dir']}",
                         "link": ln["type"], "other": ln["other"],
                         "source": ln["source_id"], "grade": ln["evidence_class"],
                         **({"retracted_at": ln["valid_until"].isoformat()}
                            if ln["valid_until"] is not None else {})})
    for ev in e_rows:
        timeline.append({"at": ev["created_at"].isoformat(), "kind": "event",
                         "event": ev["event_type"], "actor": ev["actor"],
                         **({"related": str(ev["related_id"])} if ev["related_id"] else {}),
                         **({"witnessed": True} if ev["witnessed"] else {})})
    timeline.sort(key=lambda e: str(e["at"]))
    dropped = max(0, len(timeline) - limit)
    believes = {r["name"]: _cell(r["v"]) for r in await pool.fetch(
        "SELECT name, value #>> '{}' AS v FROM winning_props(ARRAY[$1]::uuid[])", oid)}
    return {
        "object": {"id": str(head["id"]), "type": head["type"],
                   "canonical": head["canonical"], "status": head["status"],
                   **({"merged_into": str(head["merged_into"])}
                      if head["merged_into"] else {}),
                   "born": head["created_at"].isoformat()},
        "believes": believes,
        "timeline": timeline[-limit:],
        "counts": {"assertions": len(a_rows), "links": len(l_rows), "events": len(e_rows),
                   "superseded": sum(1 for a in a_rows if a["dead"])},
        **({"note": f"newest {limit} of {len(timeline)} entries shown "
                    f"({dropped} older dropped — raise limit to see them)"} if dropped else {}),
    }




async def _fn_echoes(pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]) -> Any:
    """The collapsed pile, listable (ruling 758ded94): open threads the LENS ranks off the
    wall — miner echoes no mind has ever touched (not one self_declared assertion, AT ANY AGE)
    plus judged questions (kind='question'). Their status is OPEN and stays open: untouched is
    a fact about readers, never a resolution. Oldest first — triage drains from the bottom.
    `args.repo` scopes to one project; report-only."""
    repo = str(args.get("repo") or "").strip()
    limit = max(1, min(int(args.get("limit") or 100), 500))
    rows = await pool.fetch(
        "SELECT o.id, o.created_at, "
        " (SELECT p.canonical FROM links l JOIN objects p ON p.id=l.to_id "
        "   WHERE l.from_id=o.id AND l.type='in_repo' LIMIT 1) AS project, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='summary' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS summary, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='kind' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS kind, "
        # THE ROT CANDIDATE'S EVIDENCE (closure-miner, operator ruling 2026-07-12): a later
        # commit that looks like it did this. It is a QUESTION, never a verdict — it rides the
        # pile so the human can confirm a whole tree in one sitting instead of re-deriving each.
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='rot_candidate' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS probably_done, "
        " NOT EXISTS (SELECT 1 FROM assertions sa WHERE sa.object_id=o.id "
        "   AND sa.evidence_class='self_declared') AS untouched "
        "FROM objects o "
        "WHERE o.type='Thread' AND o.merged_into IS NULL AND o.status='active' "
        "  AND COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
        "   WHERE a.object_id=o.id AND a.name='status' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1),'open')='open' "
        "ORDER BY o.created_at ASC LIMIT 3000")
    echoes = []
    for r in rows:
        if not r["summary"]:
            continue
        if repo and (r["project"] or "").removeprefix("repo:") != repo.removeprefix("repo:"):
            continue
        # A DECLARED duty never hides — declaring TOUCHES the thread. A guess folds into the
        # pile IMMEDIATELY: no freshness week (see the scoped lens below for why the "loud week"
        # is exactly what let the pile grow). A ROT CANDIDATE rides the pile whatever its age —
        # it carries EVIDENCE, which is the entire point of it.
        if r["kind"] == "question" or r["probably_done"] or bool(r["untouched"]):
            echoes.append({
                "id": str(r["id"])[:8], "born": r["created_at"].date().isoformat(),
                "project": (r["project"] or "").removeprefix("repo:") or None,
                "kind": r["kind"], "summary": r["summary"][:200],
                **({"probably_done": r["probably_done"]} if r["probably_done"] else {})})
    # evidence first: these are the ones a human can actually settle in one glance
    echoes.sort(key=lambda e: (0 if e.get("probably_done") else 1, e["born"]))
    return {
        "echoes": echoes[:limit],
        "count": len(echoes),
        "with_evidence": sum(1 for e in echoes if e.get("probably_done")),
        **({"note": f"showing oldest {limit} of {len(echoes)}"} if len(echoes) > limit else {}),
        "verbs": ("triage with testimony, never bulk writes: reclassify_thread(id, "
                  "kind='obligation') to adopt · resolve_thread(id, because=…) when done/moot "
                  "· reclassify_thread(id, kind='question') for a question that is not work"),
    }


_LINT_CAP = 50  # findings LISTED per check; totals are always reported — no silent caps
_ROMAN_HEIR = re.compile(r"-[ivxlcdm]+$")
_SEVERITY_RANK = {"error": 0, "warn": 1, "info": 2}


async def _fn_lint(pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]) -> Any:
    """rung 2 — GRAPH LINT (campaign 5c57f54d): the knowledge layer's immune system. Audits
    the graph ITSELF — report-only, pure SQL + credence, no LLM, and NO WRITES (rule #7: a
    lint that healed would be a loop pathology; findings are testimony for a mind to judge).
    Seven checks, each born from a lived bug: CONTRADICTION (near-tie multi-source winners
    on one NON-lifecycle fact — surfaced, never resolved), STATUS-REGRESSION (an 'open'
    newer than another source's 'resolved' — a deliberate close overridden by recency; the
    lifecycle property's one real failure mode, its normal transitions never flagged),
    LAUNDERING (an agent carrying a fact above its origin grade — via credence_props, the
    mandated read path for grade-is-the-message), LINEAGE (succeeded_by cycles / dangling
    heir pointers / heirs without ancestry / retired-yet-live agents / healed false mints),
    ORPHAN-LINK (info-grade: historical edges on non-active objects — expected under
    resolve-on-read, metered as consolidation debt, merge markers excluded),
    STALE-OBLIGATION (open duties older than `stale_days` — duties rot silently),
    ROT-CANDIDATE (info: open threads whose repo's later commits share their vocabulary —
    'probably resolved, confirm?' dealt to a mind's triage verbs, never auto-resolved),
    ATTRIBUTION (writes from agent ids the graph never registered — the impersonation
    class, made a standing tripwire), PHANTOM-TWIN (an anonymous un-spawned agent mounted
    at a Seat's office beside a different holder lineage — a resumed soul wearing a second
    row; the one identity degradation that must never be silent), PARALLEL-LIVES (a
    generation minted while a different door of its own lineage held a live pulse — the
    predecessor was not dead; reads the parallel_pulse_door stamp mint_heir writes at
    the mint, thread 4bcd6541)."""
    stale_days = max(1, min(int(args.get("stale_days") or 14), 365))
    eps = float(args.get("eps") or 0.05)          # "near-tie" on the confidence axis
    live_secs = int(args.get("live_secs") or 900)  # a mount seen this recently is LIVE
    findings: list[dict[str, Any]] = []
    counts: dict[str, int] = {}

    def land(check: str, severity: str, rows: list[dict[str, Any]]) -> None:
        counts[check] = len(rows)
        for r in rows[:_LINT_CAP]:
            findings.append({"check": check, "severity": severity, **r})

    # CONTRADICTION — same (object, field), different values from different sources, the top
    # two winners within eps of each other: the grade-then-recency resolver is deciding this
    # fact on a coin flip. Surface the tie; resolving it is a mind's job (tension audit).
    # The LIFECYCLE FAMILY is EXCLUDED: `status` because open→resolved from another hand is
    # the state machine working, not a war (the first live lint flagged 23 of these; zero
    # were real), and `resolved_in`/`resolved_because` because two sources both writing them
    # means two hands BOTH closed the thread — a same-status double-resolution is
    # CORROBORATION, two witnesses attesting one fact from their own vantages (operator
    # ruling 64adf08a, the 94ddca1f adjudication: keep both witnesses, never pick one to
    # satisfy the lint). The family's one true failure mode — a close being overridden —
    # gets its own check below (status-regression).
    con = await pool.fetch(
        "WITH multi AS (SELECT object_id, name FROM current_assertions "
        "  WHERE name NOT IN ('status', 'resolved_in', 'resolved_because') "
        "  GROUP BY object_id, name HAVING count(DISTINCT source_id) > 1), "
        "ranked AS (SELECT ca.object_id, ca.name, ca.value #>> '{}' AS v, ca.source_id, "
        "  ca.confidence, row_number() OVER (PARTITION BY ca.object_id, ca.name "
        "    ORDER BY ca.confidence DESC, ca.observed_at DESC) AS rn "
        "  FROM current_assertions ca JOIN multi USING (object_id, name)) "
        "SELECT o.canonical, w.name AS field, w.v AS winner, w.source_id AS winner_source, "
        "  w.confidence AS winner_conf, r.v AS rival, r.source_id AS rival_source, "
        "  r.confidence AS rival_conf "
        "FROM ranked w JOIN ranked r ON r.object_id=w.object_id AND r.name=w.name "
        "  AND w.rn=1 AND r.rn=2 "
        "JOIN objects o ON o.id=w.object_id "
        "WHERE w.v IS DISTINCT FROM r.v AND w.source_id <> r.source_id "
        "  AND w.confidence - r.confidence <= $1 "
        "ORDER BY o.canonical, w.name", eps)
    land("contradiction", "warn", [
        {"subject": r["canonical"], "field": r["field"],
         "detail": f"'{_cell(r['winner'])}' ({r['winner_source']}, "
                   f"{round(float(r['winner_conf']), 3)}) wins over "
                   f"'{_cell(r['rival'])}' ({r['rival_source']}, "
                   f"{round(float(r['rival_conf']), 3)}) by ≤{eps} — a coin-flip winner"}
        for r in con])

    # STATUS-REGRESSION — the lifecycle property's ONE real failure mode: an 'open' NEWER
    # than a different source's 'resolved' at comparable confidence means a deliberate close
    # is being overridden by recency (the miner-re-opens-what-a-session-resolved class). A
    # normal transition (resolved newer than open) is never flagged.
    reg = await pool.fetch(
        "WITH s AS (SELECT ca.object_id, ca.value #>> '{}' AS v, ca.source_id, "
        "  ca.confidence, ca.observed_at "
        "  FROM current_assertions ca JOIN objects o ON o.id=ca.object_id "
        "  WHERE ca.name='status' AND o.type='Thread' AND o.status='active') "
        "SELECT o.canonical, op.source_id AS reopener, op.observed_at AS reopened_at, "
        "  re.source_id AS resolver, "
        "  (SELECT value #>> '{}' FROM current_assertions WHERE object_id=op.object_id "
        "    AND name='summary' ORDER BY confidence DESC, observed_at DESC LIMIT 1) AS summary "
        "FROM s op JOIN s re ON re.object_id=op.object_id "
        "JOIN objects o ON o.id=op.object_id "
        "WHERE op.v='open' AND re.v='resolved' AND op.observed_at > re.observed_at "
        "  AND op.source_id <> re.source_id AND op.confidence >= re.confidence - $1 "
        "ORDER BY op.observed_at", eps)
    land("status-regression", "error", [
        {"subject": r["canonical"],
         "detail": f"re-opened by {r['reopener']} AFTER {r['resolver']} resolved it "
                   f"({str(r['reopened_at'])[:19]}) — a deliberate close is being overridden "
                   f"by recency: {_cell(r['summary'])}"}
        for r in reg])

    # LAUNDERING — through credence_props, the module whose own invariant demands every
    # grade-is-the-message read path route through it. Candidates: only co-asserted objects
    # (same fact, >1 source) — the lineage discipline is meaningless on a single voice.
    cand_rows = await pool.fetch(
        "SELECT object_id, max(observed_at) AS latest FROM current_assertions "
        "WHERE (object_id, name) IN (SELECT object_id, name FROM current_assertions "
        "  GROUP BY object_id, name HAVING count(DISTINCT source_id) > 1) "
        "GROUP BY object_id ORDER BY latest DESC LIMIT 500")
    laundering: list[dict[str, Any]] = []
    if cand_rows:
        from src.actions.core import Actions
        from src.orchestrator.credence import credence_props

        oids = [r["object_id"] for r in cand_rows]
        names = {r["id"]: r["canonical"] for r in await pool.fetch(
            "SELECT id, canonical FROM objects WHERE id = ANY($1::uuid[])", oids)}
        cred = await credence_props(Actions(pool), oids)
        laundering = [
            {"subject": names.get(uuid.UUID(w.object_id), w.object_id), "field": w.name,
             "detail": f"{', '.join(w.laundering)} carried this fact above its origin "
                       f"grade (winner: {w.source_id})"}
            for w in cred.winners if w.laundering]
    land("laundering", "warn", laundering)

    # LINEAGE — the succession invariants the identity layer lives by (ruling a882b334).
    # THE WALK COVERS EVERY GENERATION THE GRAPH EVER REGISTERED, whatever its status:
    # lineage_head deliberately walks THROUGH inactive generations (a historical middle is
    # ancestry, not absence), and a lint that loads active-only diverged from that law —
    # four bases whose -ii heirs had been archived read as 'dangling' for two sessions
    # (task #20, 2026-07-19: every flagged edge pointed at a real, historical object).
    # `canons` (active-only) still scopes the OTHER checks below; only the walk widened.
    ag_rows = await pool.fetch(
        "SELECT id, canonical, status FROM objects WHERE type='Agent'")
    ag_ids = [r["id"] for r in ag_rows]
    canon_of = {r["id"]: r["canonical"] for r in ag_rows}
    known = set(canon_of.values())
    canons = {r["canonical"] for r in ag_rows if r["status"] == "active"}
    props: dict[str, dict[str, str]] = {}
    if ag_ids:
        for r in await pool.fetch(
                "SELECT object_id, name, value #>> '{}' AS v "
                "FROM winning_props($1::uuid[]) "
                "WHERE name IN ('succeeded_by','succeeded_from','retired','false_mint')",
                ag_ids):
            props.setdefault(canon_of[r["object_id"]], {})[r["name"]] = r["v"] or ""
    succ_by = {c: p["succeeded_by"] for c, p in props.items() if p.get("succeeded_by")}
    cycles: list[dict[str, Any]] = []
    dangling: list[dict[str, Any]] = []
    seen_cycles: set[frozenset[str]] = set()
    seen_dangling: set[str] = set()
    for start in succ_by:
        walk = [start]
        walked = {start}
        while (nxt := succ_by.get(walk[-1])) is not None:
            if nxt not in known:
                if walk[-1] not in seen_dangling:
                    seen_dangling.add(walk[-1])
                    dangling.append({"subject": walk[-1],
                                     "detail": f"succeeded_by points at {nxt!r}, "
                                               "which no Agent object of any status "
                                               "carries — a pointer into the void"})
                break
            if nxt in walked:
                members = frozenset(walk[walk.index(nxt):])
                if members not in seen_cycles:
                    seen_cycles.add(members)
                    cycles.append({"subject": nxt,
                                   "detail": "succession cycle: "
                                             + " → ".join(walk[walk.index(nxt):] + [nxt])})
                break
            walk.append(nxt)
            walked.add(nxt)
    land("lineage-cycle", "error", cycles)
    land("lineage-dangling", "error", dangling)
    land("orphan-heir", "warn", [
        {"subject": c, "detail": "a generation suffix with no succeeded_from — an heir "
                                 "with no recorded ancestor"}
        for c in sorted(canons)
        if _ROMAN_HEIR.search(c) and not props.get(c, {}).get("succeeded_from")])
    live = {r["agent_id"] for r in await pool.fetch(
        "SELECT DISTINCT agent_id FROM agent_mounts "
        "WHERE last_seen > now() - make_interval(secs => $1)", live_secs)}
    land("retired-live", "error", [
        {"subject": c, "detail": "carries a winning retired=true yet holds a LIVE mount — "
                                 "a closed name is being worn"}
        for c in sorted(canons)
        if props.get(c, {}).get("retired") == "true" and c in live])
    land("false-mint", "info", [
        {"subject": c, "detail": "a healed false mint (compensating events) — expected to "
                                 "be retired; listed so the healing stays visible"}
        for c in sorted(canons) if props.get(c, {}).get("false_mint") == "true"])

    # ORPHAN-LINK — FKs make truly dangling links impossible, and the kernel's merge is
    # resolve-on-read BY DESIGN (assertions and links are never rewritten — provenance
    # survives; the loser's same_as → winner IS the merge marker). So edges on non-active
    # objects are HISTORY, not errors: this check is an INFO-grade consolidation-debt meter
    # (rung 4's queue), with the merge markers themselves excluded — flagging the merge
    # mechanism as damage taught the first live run to cry wolf 159 times.
    _ORPHAN_WHERE = (
        "FROM links l JOIN objects fo ON fo.id=l.from_id JOIN objects t ON t.id=l.to_id "
        "WHERE (l.valid_until IS NULL OR l.valid_until > now()) "
        "  AND (fo.status <> 'active' OR t.status <> 'active') "
        "  AND NOT (l.type = 'same_as' AND fo.merged_into IS NOT DISTINCT FROM l.to_id)")
    orphan_total = await pool.fetchval(f"SELECT count(*) {_ORPHAN_WHERE}")
    orphans = await pool.fetch(
        "SELECT l.type, fo.canonical AS from_c, fo.status AS from_s, "
        f" t.canonical AS to_c, t.status AS to_s {_ORPHAN_WHERE} "
        "ORDER BY l.last_seen DESC LIMIT $1", _LINT_CAP)
    land("orphan-link", "info", [
        {"subject": f"{r['from_c']} -{r['type']}-> {r['to_c']}",
         "detail": "historical edge on a non-active object ("
                   + ", ".join(f"{c} is {s}" for c, s in
                               ((r["from_c"], r["from_s"]), (r["to_c"], r["to_s"]))
                               if s != "active")
                   + ") — expected under resolve-on-read; the count meters consolidation "
                     "debt, not damage"}
        for r in orphans])
    counts["orphan-link"] = int(orphan_total)

    # STALE-OBLIGATION — a duty nobody resolved or resolved-away; age from birth, honestly
    # crude (the graph has no per-thread activity clock yet).
    th = await pool.fetch(
        "SELECT o.id, o.created_at, "
        " (SELECT value #>> '{}' FROM current_assertions WHERE object_id=o.id "
        "   AND name='status' ORDER BY confidence DESC, observed_at DESC LIMIT 1) AS st, "
        " (SELECT value #>> '{}' FROM current_assertions WHERE object_id=o.id "
        "   AND name='kind' ORDER BY confidence DESC, observed_at DESC LIMIT 1) AS kind, "
        " (SELECT value #>> '{}' FROM current_assertions WHERE object_id=o.id "
        "   AND name='summary' ORDER BY confidence DESC, observed_at DESC LIMIT 1) AS summary "
        "FROM objects o WHERE o.type='Thread' AND o.status='active' "
        "  AND o.created_at < now() - make_interval(days => $1)", stale_days)
    now = datetime.now(UTC)
    land("stale-obligation", "warn", [
        {"subject": str(r["id"]), "age_days": (now - r["created_at"]).days,
         "detail": f"open obligation, {(now - r['created_at']).days}d old: "
                   f"{_cell(r['summary'])}"}
        for r in sorted(th, key=lambda r: r["created_at"])
        if r["st"] == "open" and r["kind"] == "obligation"])

    # ROT-CANDIDATE (info) — an open thread whose repo's COMMITS, landed AFTER the
    # thread's last movement, share its distinctive vocabulary: the work probably
    # happened and nobody testified (two witnesses: Metron IV fa918939, Soundwave
    # b813e389 — 'I re-derive which obligations are actually alive at every mount').
    # Report-only, ruling 758ded94 intact: the finding DEALS the thread to a mind's
    # triage verbs; the status change stays testimony, never lint's.
    from src.ingest.mined import distinctive_terms

    open_th = await pool.fetch(
        "SELECT o.id, p.canonical AS repo, "
        " (SELECT value #>> '{}' FROM current_assertions WHERE object_id=o.id "
        "   AND name='summary' ORDER BY confidence DESC, observed_at DESC LIMIT 1) "
        "   AS summary, "
        " (SELECT max(a.observed_at) FROM assertions a WHERE a.object_id=o.id) AS moved "
        "FROM objects o JOIN links l ON l.from_id=o.id AND l.type='in_repo' "
        "JOIN objects p ON p.id=l.to_id AND p.type='SoftwareProject' "
        "WHERE o.type='Thread' AND o.status='active' "
        "AND (SELECT value #>> '{}' FROM current_assertions WHERE object_id=o.id "
        "  AND name='status' ORDER BY confidence DESC, observed_at DESC LIMIT 1) = 'open' "
        "ORDER BY moved ASC LIMIT 200")
    rot: list[dict[str, Any]] = []
    repos = {r["repo"] for r in open_th if r["summary"]}
    commits: dict[str, list[Any]] = {}
    for repo in repos:
        commits[repo] = await pool.fetch(
            "SELECT o.canonical, o.created_at, "
            " (SELECT value #>> '{}' FROM current_assertions WHERE object_id=o.id "
            "   AND name='summary' ORDER BY confidence DESC, observed_at DESC LIMIT 1) "
            "   AS summary "
            "FROM objects o JOIN links l ON l.from_id=o.id AND l.type='in_repo' "
            "JOIN objects p ON p.id=l.to_id AND p.canonical=$1 "
            "WHERE o.type='Commit' AND o.status='active' "
            "ORDER BY o.created_at DESC LIMIT 300", repo)
    for r in open_th:
        if not r["summary"]:
            continue
        want = distinctive_terms(r["summary"])
        if len(want) < 4:
            continue  # a thin summary matches everything; never deal it on weak evidence
        for c in commits.get(r["repo"], ()):
            if r["moved"] and c["created_at"] <= r["moved"]:
                continue  # only commits NEWER than the thread's last movement testify
            got = distinctive_terms(c["summary"] or "")
            shared = want & got
            if len(shared) >= 3 and len(shared) >= 0.4 * len(want):
                rot.append({
                    "subject": str(r["id"]),
                    "detail": f"probably resolved, confirm? open thread "
                              f"'{_cell(r['summary'])}' — later commit {c['canonical']} "
                              f"shares its vocabulary ({', '.join(sorted(shared)[:5])}); "
                              "if truly done: resolve_thread with the commit as the "
                              "because — your judgment is the testimony"})
                break
    land("rot-candidate", "info", rot)

    # ATTRIBUTION — writes stamped from an agent id that was never registered as an Agent:
    # the impersonation class (thread 33838160) as a standing tripwire, not a one-off hunt.
    # THE MATCH SEES THROUGH AN ANNOTATION: a writer may suffix its id with a parenthetical
    # provenance note — 'agent:<id> (relaying operator ruling ...)' — and 338 of XLIV's
    # relay writes read as an unregistered impersonator for two sessions because the exact
    # match couldn't (task #21, 2026-07-19). The id is judged; the note rides along.
    ghosts = await pool.fetch(
        "SELECT w.source_id, count(*) AS writes, max(w.at) AS last FROM ("
        "  SELECT source_id, observed_at AS at FROM assertions "
        "   WHERE source_id LIKE 'agent:%' "
        "  UNION ALL SELECT source_id, first_seen FROM links "
        "   WHERE source_id LIKE 'agent:%') w "
        "WHERE NOT EXISTS (SELECT 1 FROM objects o "
        "  WHERE o.type='Agent' AND o.canonical = split_part(w.source_id, ' (', 1)) "
        "GROUP BY w.source_id ORDER BY count(*) DESC")
    land("attribution", "error", [
        {"subject": r["source_id"], "writes": int(r["writes"]),
         "detail": f"{r['writes']} write(s) from an agent id the graph never registered "
                   f"(last {r['last'].isoformat()[:19]}) — who wore this face?"}
        for r in ghosts])

    # PHANTOM-TWIN — an ANONYMOUS, un-spawned, un-seated agent mounted at a cwd that is
    # some Seat's anchor (an OFFICE — single-occupant by design, ed5f5ce2) while the seat's
    # holder is a different lineage. The bridged-resume path mints exactly this shape when
    # its receipts are missing (agent:6ebb4445 beside alfred, 2026-07-16): the same soul
    # wearing a second registry row. Adoption cures the cases with evidence; this tripwire
    # makes the evidence-less remainder LOUD — the one degradation that touches identity
    # must never be silent. Flag, never guess (blind adoption-by-location was the cwd-guess
    # bug class; seating is deliberate or it is nothing).
    twins = await pool.fetch(
        "SELECT m.agent_id AS suspect, m.cwd AS office, s.canonical AS seat, "
        "  h.canonical AS holder, m.last_seen "
        "FROM agent_mounts m "
        "JOIN current_assertions a ON a.name='anchor_cwd' AND a.value #>> '{}' = m.cwd "
        "JOIN objects s ON s.id=a.object_id AND s.type='Seat' "
        "JOIN links l ON l.to_id=s.id AND l.type='holds' "
        "  AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "JOIN objects h ON h.id=l.from_id AND h.type='Agent' "
        "WHERE m.seat_id IS NULL "
        "AND substring(m.agent_id from '^agent:[0-9a-f]{8}') "
        "  <> substring(h.canonical from '^agent:[0-9a-f]{8}') "
        "AND NOT EXISTS (SELECT 1 FROM current_assertions ha "
        "  JOIN objects ao ON ao.id=ha.object_id "
        "  WHERE ha.name='handle' AND ao.canonical = m.agent_id) "
        "AND NOT EXISTS (SELECT 1 FROM links sp "
        "  JOIN objects so ON so.id=sp.from_id "
        "  WHERE sp.type='spawned_by' AND so.canonical = m.agent_id)")
    land("phantom-twin", "warn", [
        {"subject": r["suspect"], "office": r["office"], "seat": r["seat"],
         "detail": f"anonymous agent {r['suspect']} mounted at {r['holder']}'s office "
                   f"({r['seat']}, last seen "
                   f"{r['last_seen'].isoformat()[:19] if r['last_seen'] else 'never'}) — "
                   "likely the same soul wearing a second row (a bridged resume without "
                   "its receipts). Verify and heal by hand; never auto-merge"}
        for r in twins])

    # PARALLEL-LIVES (thread 4bcd6541, invariant 3 of the guarantee cd35bb1d) — a
    # generation whose MINT captured a live pulse on a DIFFERENT door of its own lineage:
    # the predecessor was not dead when the heir was crowned (g40-v/vi were minted while
    # g40-iv worked; each would have tripped this within a minute). The evidence is the
    # `parallel_pulse_door` stamp mint_heir writes AT the mint — rows are hot state and
    # the pulse is gone by lint time, so the stamp is the only witness. Testimony for
    # the fold tray; the seam may still have been real (verify), never auto-fold.
    par = await pool.fetch(
        "SELECT o.canonical AS heir, "
        "  max(p.value #>> '{}') FILTER (WHERE p.name='parallel_pulse_door') AS door, "
        "  max(p.value #>> '{}') FILTER (WHERE p.name='predecessor_last_seen') AS pulse_at, "
        "  max(p.value #>> '{}') FILTER (WHERE p.name='minted_because') AS because "
        "FROM objects o JOIN current_assertions p ON p.object_id=o.id "
        "WHERE o.type='Agent' AND o.status='active' "
        "AND p.name IN ('parallel_pulse_door','predecessor_last_seen','minted_because') "
        "GROUP BY o.canonical "
        "HAVING max(p.value #>> '{}') FILTER (WHERE p.name='parallel_pulse_door') "
        "  IS NOT NULL")
    land("parallel-lives", "warn", [
        {"subject": r["heir"],
         "detail": f"{r['heir']} was minted ({r['because'] or 'unknown seam'}) while "
                   f"door {r['door']} of its own lineage held a live pulse (predecessor "
                   f"last seen {r['pulse_at'] or '?'}) — a parallel life: the "
                   "predecessor was not dead. Verify the seam; fold by hand if false"}
        for r in par])

    findings.sort(key=lambda f: (_SEVERITY_RANK.get(str(f["severity"]), 9), str(f["check"])))
    capped = {c: n - _LINT_CAP for c, n in counts.items() if n > _LINT_CAP}
    return {
        "findings": findings,
        "counts": counts,
        "clean": sorted(c for c, n in counts.items() if n == 0),
        **({"capped": capped,
            "note": "some checks list only their first "
                    f"{_LINT_CAP} findings; counts hold the true totals"} if capped else {}),
        "ran_at": now.isoformat(),
        "discipline": "report-only — the lint never writes (rule #7); "
                      "findings are testimony, not verdicts",
    }


# ── THE ONE WALL LAW (operator ruling 923c380f, 2026-07-11) ─────────────────────────────
# The console's briefing rendered the RAW open-thread select — 919 rows, 92% of them miner
# echoes no mind ever touched — while orient() had the graded law all along. The law now
# lives HERE, and mcp_server imports it: one wall, every lens.

ORIENT_OPEN_THREADS = 25


def rank_open_threads(
    rows: list[dict[str, Any]], me: frozenset[str] = frozenset(),
) -> tuple[list[dict[str, Any]], int]:
    """Rank open threads for display and cap. Obligations — DUTIES an action minted — float
    above ordinary threads. WITHIN each kind group, ownership orders for the READER (`me` =
    the caller's agent id + project; the console passes {'operator'}): MINE TO ACT first,
    another mind's claims next, 'waiting on the human' last. Input (recency) order breaks
    remaining ties — Python's sort is stable. Pure."""
    def whose_move(r: dict[str, Any]) -> int:
        owner = (r.get("owner") or "").strip()
        if not owner or owner in me:
            return 0  # mine to act (unowned = anyone who reads it may act)
        return 2 if owner == "operator" else 1
    summ = [r for r in rows if r.get("summary")]
    ranked = sorted(summ, key=lambda r: (r.get("kind") != "obligation", whose_move(r)))
    shown = ranked[:ORIENT_OPEN_THREADS]
    return shown, len(ranked) - len(shown)


# A HALTED project's work is not the fleet's debt (the operator halts a program BY NAME — it is on
# the record, testimony, not a guess). Its threads are real yield on a PAUSED tree: not garbage, so
# never swept; not debt, so never counted. 333 of them were inflating every number in the system
# (257 in one, 78 in another). Resume the project and they all come back — this is a LENS.
_NOT_HALTED = (
    "NOT EXISTS (SELECT 1 FROM links hl JOIN objects hp ON hp.id=hl.to_id "
    "  JOIN current_assertions ha ON ha.object_id=hp.id AND ha.name='lifecycle' "
    "  WHERE hl.from_id=o.id AND hl.type='in_repo' AND hp.type='SoftwareProject' "
    "    AND ha.value #>> '{}' = 'halted')"
)

async def open_thread_wall(
    pool: asyncpg.Pool, proj: uuid.UUID,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """One project's open threads, SPLIT: (wall, echoes). An ECHO is a thread no mind has
    ever touched — not one self_declared assertion in its whole history — that is either
    kind='question' or older than the freshness window. Its status stays OPEN in the record
    (untouched ≠ resolved, ruling 758ded94); only the LENS stops hauling it. Rows carry the
    8-char short id so triage verbs can name their target directly."""
    rows = await pool.fetch(
        "SELECT o.id, o.created_at, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='summary' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS summary, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='kind' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS kind, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='owner' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS owner, "
        " NOT EXISTS (SELECT 1 FROM assertions sa WHERE sa.object_id=o.id "
        "   AND sa.evidence_class='self_declared') AS untouched "
        "FROM objects o JOIN links l ON l.from_id=o.id AND l.type='in_repo' AND l.to_id=$1 "
        "WHERE o.type='Thread' AND o.merged_into IS NULL AND o.status='active' "
        "  AND COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
        "   WHERE a.object_id=o.id AND a.name='status' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1),'open')='open' "
        "ORDER BY o.created_at DESC LIMIT 400", proj)
    # THE UNFILED OWNER MATCH (thread 4ffe0eb9, Alfred V's succession repro): a thread
    # opened without repo= carries no in_repo link, so the join above can never see it —
    # Alfred IV's succession handoff hid from his own successor's orient while its owner
    # said 'alfred' the whole time, and the successor's first instinct was to regex the
    # transcript. Owner already means "whose move it is": an unfiled open thread whose
    # owner IS this project (by name, or an agent mounted in it) belongs on its wall.
    pname = await pool.fetchval(
        "SELECT replace(canonical, 'repo:', '') FROM objects WHERE id=$1", proj)
    if pname:
        seen = {r["id"] for r in rows}
        unfiled = await pool.fetch(
            "SELECT o.id, o.created_at, "
            " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
            "   AND a.name='summary' "
            "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS summary, "
            " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
            "   AND a.name='kind' "
            "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS kind, "
            " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
            "   AND a.name='owner' "
            "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS owner, "
            " NOT EXISTS (SELECT 1 FROM assertions sa WHERE sa.object_id=o.id "
            "   AND sa.evidence_class='self_declared') AS untouched "
            "FROM objects o "
            "WHERE o.type='Thread' AND o.merged_into IS NULL AND o.status='active' "
            "  AND COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
            "   WHERE a.object_id=o.id AND a.name='status' "
            "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1),'open')='open' "
            "  AND NOT EXISTS (SELECT 1 FROM links fl WHERE fl.from_id=o.id "
            "   AND fl.type='in_repo') "
            "  AND (COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
            "   WHERE a.object_id=o.id AND a.name='owner' "
            "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1),'') = $1 "
            "   OR COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
            "   WHERE a.object_id=o.id AND a.name='owner' "
            "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1),'') IN ("
            "   SELECT o2.canonical FROM objects o2 "
            "     JOIN current_assertions pa ON pa.object_id=o2.id AND pa.name='project' "
            "     WHERE o2.type='Agent' AND o2.status='active' "
            "     AND pa.value #>> '{}' = $1)) "
            "ORDER BY o.created_at DESC LIMIT 100", str(pname))
        rows = list(rows) + [r for r in unfiled if r["id"] not in seen]
    wall: list[dict[str, Any]] = []
    echoes: list[dict[str, Any]] = []
    for r in rows:
        if not r["summary"]:
            continue
        # kind/owner render only when DECLARED (no null-key noise): an absent kind means no
        # mind — and no mechanical rule — ever said what this is (Fulcrum III's verdict,
        # answered at the lens).
        item = {"id": str(r["id"])[:8], "summary": r["summary"]}
        if r["kind"]:
            item["kind"] = r["kind"]
        if r["owner"]:  # whose move it is — absent means anyone's
            item["owner"] = r["owner"]
        # THE MINER MAY NOTICE, BUT MUST NEVER OBLIGE (ruling 61c1b20d, extended from the desk
        # to the wall — 2026-07-12, the operator: "it's a snowball to hell").
        #
        # An UNTOUCHED thread is one no mind has ever laid a self_declared assertion on: nobody
        # opened it, nobody claimed it, nobody so much as triaged it. It exists because an LLM
        # read a conversation and inferred that somebody, somewhere, owes work. That is a
        # SUGGESTION, and it was riding the wall with the full authority of a declaration.
        #
        # It used to get a "loud week" before folding. That window is exactly what let the pile
        # grow: the miner mints faster than seven days, so the wall was permanently full of
        # fresh guesses. 908 of the fleet's 1067 open threads (85%) are untouched miner
        # inferences; ONE PAUSED PROJECT was showing 181 of them.
        #
        # So: a guess does not get a week. It goes to the `echoes` pile immediately, where it is
        # COUNTED and one click away (land on counts, walk in). The wall now shows only what a
        # MIND touched. Nothing is deleted, nothing is hidden — the record keeps every thread
        # open until testimony says otherwise (untouched ≠ resolved, 758ded94). It simply stops
        # being presented as though someone had promised it.
        is_echo = r["kind"] == "question" or bool(r["untouched"])
        (echoes if is_echo else wall).append(
            {**item, "born": r["created_at"].date().isoformat()} if is_echo else item)
    echoes.reverse()  # oldest first — triage drains from the bottom of the pile
    return wall, echoes


async def _fn_wall(pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]) -> Any:
    """THE WALL as a composition: what is GENUINELY unresolved, graded. With a subject
    (a SoftwareProject) — that project's wall exactly as orient renders it: obligations
    first, owner-banded, echo pile counted, capped. WITHOUT a subject — the fleet ROLLUP:
    per-project counts + the top obligations across the whole graph, never 900 raw rows
    (the 2026-07-11 console showed 919 'unresolved'; 850 were untouched miner echoes)."""
    me_arg = args.get("me")
    me = frozenset(me_arg) if isinstance(me_arg, list | tuple | set) else frozenset()
    if subject is not None:
        wall, echoes = await open_thread_wall(pool, subject)
        shown, more = rank_open_threads(wall, me)
        return {"wall": shown, "more_on_wall": more,
                "echo_pile": {"count": len(echoes),
                              "note": "untouched miner echoes + judged questions — the "
                                      "record keeps them open; triage them in the echoes "
                                      "lens (adopt / question / resolve)"},
                "note": "the graded wall — one law with orient(): obligations first, "
                        "yours-to-act before others' claims before waiting-on-the-human"}
    # same partition per project: open = wall + pile, and `obligations` counts only DECLARED
    # duties (a miner-guessed obligation nobody touched is a guess, not a debt)
    projects = [dict(r) for r in await pool.fetch(
        "SELECT p.canonical AS project, count(*) AS open, "
        " count(*) FILTER (WHERE EXISTS (SELECT 1 FROM assertions sa "
        "     WHERE sa.object_id=o.id AND sa.evidence_class='self_declared') "
        "   AND (SELECT a.value #>> '{}' FROM current_assertions a "
        "     WHERE a.object_id=o.id AND a.name='kind' "
        "     ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) = 'obligation') "
        "   AS obligations, "
        " count(*) FILTER (WHERE EXISTS (SELECT 1 FROM assertions sa "
        "   WHERE sa.object_id=o.id AND sa.evidence_class='self_declared')) AS wall, "
        " count(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM assertions sa "
        "   WHERE sa.object_id=o.id AND sa.evidence_class='self_declared')) AS pile "
        "FROM objects o JOIN links l ON l.from_id=o.id AND l.type='in_repo' "
        "JOIN objects p ON p.id=l.to_id AND p.type='SoftwareProject' AND p.status='active' "
        "  AND NOT EXISTS (SELECT 1 FROM current_assertions ha WHERE ha.object_id=p.id "
        "    AND ha.name='lifecycle' AND ha.value #>> '{}' = 'halted') "
        "WHERE o.type='Thread' AND o.status='active' AND o.merged_into IS NULL "
        "  AND COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
        "   WHERE a.object_id=o.id AND a.name='status' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1),'open')='open' "
        "GROUP BY p.canonical ORDER BY count(*) DESC LIMIT 30")]
    # the fleet's TOP OF WALL: obligations (a duty never hides) plus any thread a mind
    # actually TOUCHED — never the untouched echo mass; repo-less threads included (a
    # deliberate open_thread with no repo must still surface where it was promised to)
    top_rows = [dict(r) for r in await pool.fetch(
        "SELECT str_id AS id, summary, kind, owner, project FROM ("
        " SELECT substr(o.id::text, 1, 8) AS str_id, o.created_at, "
        "  (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "    AND a.name='summary' "
        "    ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS summary, "
        "  (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "    AND a.name='kind' "
        "    ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS kind, "
        "  (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "    AND a.name='owner' "
        "    ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS owner, "
        "  (SELECT p.canonical FROM links l JOIN objects p ON p.id=l.to_id "
        "    WHERE l.from_id=o.id AND l.type='in_repo' LIMIT 1) AS project, "
        "  EXISTS (SELECT 1 FROM assertions sa WHERE sa.object_id=o.id "
        "    AND sa.evidence_class='self_declared') AS touched "
        " FROM objects o "
        " WHERE o.type='Thread' AND o.status='active' AND o.merged_into IS NULL "
        "   AND COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
        "    WHERE a.object_id=o.id AND a.name='status' "
        "    ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1),'open')='open' "
        " ORDER BY o.created_at DESC LIMIT 400) t "
        # ONE RULE, THREE LENSES. This predicate, the fleet pile above, and the scoped wall
        # below must agree or the numbers cannot add up — and the operator caught them
        # disagreeing ("1051 open · 334 obligations · 951 pile — this doesn't add up").
        # A thread rides the wall IFF A MIND TOUCHED IT. A guessed obligation used to get a
        # week's grace here; the miner mints faster than a week, so the grace WAS the pile.
        "WHERE t.summary IS NOT NULL AND t.touched")]
    shown, more = rank_open_threads(top_rows, me)
    # totals over the WHOLE record — a repo-less thread must count even though the
    # per-project breakdown can't file it
    # THE NUMBERS MUST ADD UP (operator, 2026-07-12: "1051 open · 334 obligations · 951 pile —
    # this doesn't add up, the numbers are absurd"). He was right, and they never could have.
    #
    # These were THREE OVERLAPPING CUTS of one set, stacked as if they were three SLICES of it:
    # `open` was the whole, `obligations` cut it by KIND, `pile` cut it by TOUCHED-NESS — and an
    # obligation can sit in the pile, so 381 + 951 = 1332 > 1114. Any reader who tried to
    # reconcile them was doing arithmetic on a category error.
    #
    # Now they PARTITION: open = wall + pile, exactly, always. And `obligations` is reported as
    # what it actually is — a SUBSET OF THE WALL, and only the DECLARED ones. Of 381 threads
    # carrying kind='obligation', 259 were the MINER's guess that somebody owed something and no
    # mind ever touched them. Counting those as duties inflated the fleet's debt threefold. The
    # real number is 122. (Same law, one more altitude: the miner may notice, but never oblige.)
    trow = await pool.fetchrow(
        "SELECT count(*) AS open, "
        " count(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM assertions sa "
        "   WHERE sa.object_id=o.id AND sa.evidence_class='self_declared')) AS pile, "
        " count(*) FILTER (WHERE EXISTS (SELECT 1 FROM assertions sa "
        "     WHERE sa.object_id=o.id AND sa.evidence_class='self_declared') "
        "   AND (SELECT a.value #>> '{}' FROM current_assertions a "
        "     WHERE a.object_id=o.id AND a.name='kind' "
        "     ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) = 'obligation') "
        "   AS obligations, "
        " count(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM assertions sa "
        "     WHERE sa.object_id=o.id AND sa.evidence_class='self_declared') "
        "   AND (SELECT a.value #>> '{}' FROM current_assertions a "
        "     WHERE a.object_id=o.id AND a.name='kind' "
        "     ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) = 'obligation') "
        "   AS guessed_obligations "
        "FROM objects o "
        "WHERE o.type='Thread' AND o.status='active' AND o.merged_into IS NULL "
        "  AND COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
        "   WHERE a.object_id=o.id AND a.name='status' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1),'open')='open' "
        "  AND " + _NOT_HALTED)
    halted = await pool.fetchval(
        "SELECT count(*) FROM objects o "
        "WHERE o.type='Thread' AND o.status='active' AND o.merged_into IS NULL "
        "  AND COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
        "   WHERE a.object_id=o.id AND a.name='status' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1),'open')='open' "
        "  AND NOT " + _NOT_HALTED)
    totals = {
        "open": trow["open"],
        "wall": trow["open"] - trow["pile"],   # a mind touched it — open = wall + pile, exactly
        "pile": trow["pile"],
        "obligations": trow["obligations"],    # DECLARED duties, a subset of `wall`
        "guessed_obligations": trow["guessed_obligations"],  # the miner's, sitting in the pile
        "halted": halted,   # real yield on programs the operator killed BY NAME — not debt
        "reads": "open = wall + pile, over LIVE projects only. `obligations` are DECLARED duties "
                 "and are a subset of `wall` — never add them to anything. `halted` is work on "
                 "programs the operator stopped: not garbage (never swept), not debt (never "
                 "counted); resume the project and it returns.",
    }
    return {"totals": totals, "projects": projects,
            "top_of_wall": shown, "more_on_wall": more,
            "note": "the fleet wall — the graded top (declared duties + threads a mind touched), "
                    "never the raw scroll. `pile` is untouched miner echoes (no mind has read "
                    "them); `guessed_obligations` are duties the MINER inferred and nobody "
                    "confirmed — they are NOT debt. Focus a project to see its own graded wall."}


_FUNCTIONS: dict[str, Function] = {
    "coinvest": _fn_coinvest,
    "subject_report": _fn_subject_report,
    "screen_network": _fn_screen,
    "canon": _fn_canon,
    "search": _fn_search,
    "family": _fn_family,
    "family_drift": _fn_family_drift,
    "portfolio": _fn_portfolio,
    "pulse": _fn_pulse,
    "project": _fn_project,
    "lap": _fn_lap,
    "lint": _fn_lint,
    "echoes": _fn_echoes,
    "wall": _fn_wall,
}

# Functions that brief the whole project rather than anchor on one entity — no subject needed.
# `project` is here too: it drills into ONE repo, taken from the focused subject OR `args.repo`,
# so it must run without a bound subject (it returns a "focus a repo" note if given neither).
# NB: `projects`, `briefing`, `decisions` are GONE as Functions — they decomposed into pure
# op-trees (a `table`, a `sections`, a `sections`+show-original — see DEFAULT_COMPOSITIONS):
# opinion → primitives the user owns.
# `lap` anchors on args.ref OR the subject; `lint` audits the whole graph, no anchor at all.
_SUBJECT_FREE = {"canon", "search", "family", "family_drift", "portfolio", "pulse", "project",
                 "lap", "lint", "echoes", "wall"}


def list_functions() -> list[str]:
    """The registered Functions a composition may reference (the authoring channel reads
    this to know what's beyond the closed op set)."""
    return sorted(_FUNCTIONS)


# Guardrails adopted from Palantir's Object Set API (load-tested, not arbitrary).
MAX_TRAVERSE_HOPS = 3
MAX_AGGREGATE_DIMS = 3


@dataclass
class Result:
    """A composition's output — an object set, a value list, or aggregate rows."""

    kind: str  # "objects" | "values" | "rows" | "data"
    objects: list[uuid.UUID] = field(default_factory=list)
    values: list[str] = field(default_factory=list)
    rows: list[dict[str, Any]] = field(default_factory=list)
    data: Any = None  # a Function's native output (list/dict) — opaque to the ops


def _coerce(v: Any) -> Any:
    return json.loads(v) if isinstance(v, str) else v


def _num(v: Any) -> float | None:
    """Best-effort numeric coercion for ordering/aggregation; None if not a number."""
    if isinstance(v, bool):  # bool is an int subclass — exclude it
        return None
    if isinstance(v, int | float):
        return float(v)
    if isinstance(v, str):
        try:
            return float(v.replace(",", "").strip())
        except ValueError:
            return None
    return None


async def _props(pool: asyncpg.Pool, oid: uuid.UUID) -> dict[str, str]:
    # The current value of each property is the WINNING assertion across sources, resolved
    # by evidence GRADE first (constitution #5: SELF_DECLARED > … > DERIVED), then recency —
    # so a fresh DERIVED re-assertion never overrides an older SELF_DECLARED one (the miner
    # re-opening a thread a session already resolved must NOT win). winning_props (migration
    # 0015) is the ONE definition of that ordering; every read site calls it, so it can't drift.
    rows = await pool.fetch(
        "SELECT name, value #>> '{}' AS v FROM winning_props(ARRAY[$1]::uuid[])", oid,
    )
    return {r["name"]: r["v"] for r in rows}


def _distinct[T](values: list[T]) -> list[T]:
    seen: set[T] = set()
    out: list[T] = []
    for v in values:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _setop[T](op: str, lists: list[list[T]]) -> list[T]:
    """union (concat+dedup) or intersect, preserving the first set's order."""
    if not lists:
        return []
    if op == "intersect":
        common = set(lists[0]).intersection(*(set(x) for x in lists[1:]))
        return [x for x in _distinct(lists[0]) if x in common]
    return _distinct([x for lst in lists for x in lst])


async def _eval(pool: asyncpg.Pool, node: dict[str, Any], subject: uuid.UUID | None) -> Result:
    op = node.get("op")

    if op == "subject":
        return Result("objects", objects=[subject] if subject else [])

    if op == "select":
        ot = node.get("object_type")
        cp = node.get("canonical_prefix")
        where = node.get("where", []) or []
        rows = await pool.fetch(
            "SELECT id FROM objects WHERE status='active' AND ($1::text IS NULL OR type=$1) "
            "AND ($2::text IS NULL OR canonical LIKE $2 || '%')", ot, cp
        )
        # the house boundary (6c18709f): a composition selecting Reflections — by type or
        # by an untyped select-all — reads only the caller's own house; the record stays
        # whole, this lens narrows
        if ot in (None, "Reflection"):
            refl = [r["id"] for r in await pool.fetch(
                "SELECT id FROM objects WHERE id = ANY($1::uuid[]) AND type='Reflection'",
                [r["id"] for r in rows])] if ot is None else [r["id"] for r in rows]
            if refl:
                ok = await _visible_reflections(pool, refl, _ACL_CALLER.get())
                hidden = set(refl) - ok
                rows = [r for r in rows if r["id"] not in hidden]
        # `neighborhood` is a DIMENSION, not a property — an object's tree is an in_repo EDGE,
        # not an assertion. Resolving it here (batched, only when asked for) makes it filterable
        # like any other fact, which is what lets `bundle`'s rows drill straight back into their
        # fruit with no bespoke drill path: select Thread where neighborhood=osiris.
        hoods: dict[Any, dict[str, Any]] = {}
        if any(c.get("property") == NEIGHBORHOOD for c in where):
            hoods = await neighborhoods_of(pool, [r["id"] for r in rows])
        out: list[uuid.UUID] = []
        for r in rows:
            facts = await _props(pool, r["id"])
            if hoods or any(c.get("property") == NEIGHBORHOOD for c in where):
                facts[NEIGHBORHOOD] = (hoods.get(r["id"]) or {}).get("name", "(no tree)")
            if all(match_condition(facts.get(c.get("property")), c.get("op", "contains"),
                                   c.get("value")) for c in where):
                out.append(r["id"])
        return Result("objects", objects=out)

    if op == "traverse":
        base = await _eval(pool, node["from"], subject)
        seeds = base.objects
        direction = node.get("direction", "both")
        hops = min(int(node.get("hops", 1)), MAX_TRAVERSE_HOPS)
        ltype = node.get("link_type")
        seen, frontier = set(seeds), set(seeds)
        for _ in range(hops):
            if not frontier:
                break
            ids = list(frontier)
            clause = {
                "out": "from_id = ANY($1::uuid[])",
                "in": "to_id = ANY($1::uuid[])",
            }.get(direction, "(from_id = ANY($1::uuid[]) OR to_id = ANY($1::uuid[]))")
            rows = await pool.fetch(
                "SELECT CASE WHEN from_id = ANY($1::uuid[]) THEN to_id ELSE from_id END AS n "
                f"FROM links WHERE {clause} AND ($2::text IS NULL OR type=$2)",
                ids, ltype,
            )
            nxt = {r["n"] for r in rows} - seen
            seen |= nxt
            frontier = nxt
        # the neighbourhood is everything reached EXCEPT the seeds themselves
        return Result("objects", objects=[i for i in seen if i not in set(seeds)])

    if op == "collect":
        base = await _eval(pool, node["from"], subject)
        props = node.get("properties", []) or []
        transform = _TRANSFORMS.get(node.get("transform", "identity"), _TRANSFORMS["identity"])
        vals: list[str] = []
        for oid in base.objects:
            facts = await _props(pool, oid)
            for p in props:
                v = facts.get(p)
                if v is None:
                    continue
                t = transform(v)
                if t:
                    vals.append(t)
        return Result("values", values=_distinct(vals))

    if op == "subtract":
        left = await _eval(pool, node["left"], subject)
        right = await _eval(pool, node["right"], subject)
        rset = set(right.values)
        return Result("values", values=[v for v in left.values if v not in rset])

    if op in ("union", "intersect"):
        sets = [await _eval(pool, s, subject) for s in node.get("sets", [])]
        if not sets:
            return Result("objects")
        kind = sets[0].kind
        if any(s.kind != kind for s in sets) or kind == "rows":
            raise ValueError(f"{op} requires same-kind object/value sets")
        if kind == "objects":
            return Result("objects", objects=_setop(op, [s.objects for s in sets]))
        return Result("values", values=_setop(op, [s.values for s in sets]))

    if op == "bundle":
        # FANOUT — the garden's primitive (operator, 2026-07-11: "each project is a tree with
        # fruits"). Collapse ANY object set into its trees: one row per neighborhood, counted,
        # ordered by weight. Returns the same {group, metric} shape `aggregate` does, so it
        # renders through the console's existing ranked-table renderer AND drills back through
        # the existing drill — because `neighborhood` is a real dimension of `select` (below),
        # clicking a tree re-selects exactly its fruit. Nothing about this knows what a Thread
        # is: bundle a set of commits, files, decisions or threads and the garden works the same.
        base = await _eval(pool, node["from"], subject)
        by = node.get("by", NEIGHBORHOOD)
        if by != NEIGHBORHOOD:
            raise ValueError(
                f"bundle fans out by '{NEIGHBORHOOD}' (the in_repo tree); "
                f"for a plain property use aggregate(group_by=['{by}'])")
        hoods = await neighborhoods_of(pool, base.objects)
        tally: dict[str, dict[str, Any]] = {}
        for oid in base.objects:
            h = hoods.get(oid)
            key = h["name"] if h else "(no tree)"
            row = tally.setdefault(key, {"group": {NEIGHBORHOOD: key}, "metric": 0,
                                         "id": h["id"] if h else None})
            row["metric"] += 1
        return Result("rows", rows=sorted(tally.values(), key=lambda r: -int(r["metric"])))

    if op == "aggregate":
        base = await _eval(pool, node["from"], subject)
        group_by = node.get("group_by", []) or []
        if len(group_by) > MAX_AGGREGATE_DIMS:
            raise ValueError(f"aggregate supports ≤{MAX_AGGREGATE_DIMS} group_by dimensions")
        metric = node.get("metric", {"type": "count"}) or {"type": "count"}
        return Result("rows", rows=await _aggregate(pool, base.objects, group_by, metric))

    if op == "table":
        base = await _eval(pool, node["from"], subject)
        return Result("rows", rows=await _table(pool, base.objects, node.get("columns", []) or []))

    if op == "order":
        base = await _eval(pool, node["from"], subject)
        return await _order(pool, base, node.get("by"), node.get("dir", "asc"))

    if op == "take":
        base = await _eval(pool, node["from"], subject)
        n = max(0, int(node.get("n", 0)))
        return Result(base.kind, objects=base.objects[:n], values=base.values[:n],
                      rows=base.rows[:n], data=base.data)

    if op == "sections":
        # a page of compositions: eval each section's body, render it to items, key by title.
        # This is Notion's page-of-blocks / a "briefing" — the shape every subject-free
        # read-model already returns, now composed from primitives instead of hand-written SQL.
        data: dict[str, Any] = {}
        for sec in node.get("sections", []) or []:
            title = str(sec.get("title", "section"))
            body = sec.get("body")
            res = await _eval(pool, body, subject) if body else Result("values")
            data[title] = await _package(pool, res)
        return Result("data", data=data)

    if op == "function":
        name = str(node.get("name", ""))
        fn = _FUNCTIONS.get(name)
        if fn is None:
            raise ValueError(f"unknown function: {name!r}")
        if subject is None and name not in _SUBJECT_FREE:
            raise ValueError(f"function {name!r} requires a subject")
        return Result("data", data=await fn(pool, subject, node.get("args", {}) or {}))

    raise ValueError(f"unknown composition op: {op!r}")


async def _table(
    pool: asyncpg.Pool, objects: list[uuid.UUID], columns: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """One ROW per object; each column is a property value or a rollup over a link (Notion's
    database+rollups / Palantir's object-set + per-object aggregate). Over a bounded set (a
    select/traverse result), so per-object queries are fine — the whole point is that the SET
    is already candidate-gated by the op that produced it."""
    rows: list[dict[str, Any]] = []
    for oid in objects:
        facts = await _props(pool, oid)
        row: dict[str, Any] = {}
        for col in columns:
            name = str(col.get("name") or col.get("property") or "col")
            if col.get("property") == "id":
                # THE ROW'S OWN IDENTITY (task #60, thread b81b0fac): not an assertion — a
                # row is a candidate object, not a fact ABOUT one — so it never lived in
                # `facts`. Same 8-char short-id convention every other read site uses
                # (_owned_open_threads' substring(o.id::text,1,8), the open-thread wall's
                # own ids). Lets a caller recover a summary this table truncated.
                row[name] = str(oid)[:8]
            elif "property" in col:
                row[name] = facts.get(str(col["property"]))
            elif "rollup" in col:
                row[name] = await _rollup(pool, oid, col["rollup"])
            else:
                row[name] = None
        rows.append(row)
    return rows


# object columns a rollup may pluck directly (not assertions) — the canonical is how a linked
# Commit/entity is named, so `show-original` over a link needs it. Whitelisted → safe to inline.
_OBJ_COLS = frozenset({"canonical", "type", "status"})


async def _rollup(pool: asyncpg.Pool, oid: uuid.UUID, spec: dict[str, Any]) -> Any:
    """A single rollup over one object's links: count / max / min / sum / avg of a property on
    the objects reached by `link_type` in `direction`, optionally filtered to `object_type`.
    `of:"first"` is Notion's SHOW-ORIGINAL — for a single relation, plucks the one related
    object's value (a property OR an object column like `canonical`); the enabler for showing
    a linked commit's hash/date without abusing max()."""
    direction = spec.get("direction", "both")
    ltype = spec.get("link_type")
    if direction == "in":
        clause, rel = "l.to_id = $1", "l.from_id"
    elif direction == "out":
        clause, rel = "l.from_id = $1", "l.to_id"
    else:
        clause = "(l.from_id = $1 OR l.to_id = $1)"
        rel = "CASE WHEN l.from_id = $1 THEN l.to_id ELSE l.from_id END"
    rids = [r["rid"] for r in await pool.fetch(
        f"SELECT {rel} AS rid FROM links l WHERE {clause} AND ($2::text IS NULL OR l.type=$2)",
        oid, ltype)]
    otype = spec.get("object_type")
    if otype and rids:
        rids = [r["id"] for r in await pool.fetch(
            "SELECT id FROM objects WHERE id = ANY($1::uuid[]) AND type=$2", rids, otype)]
    of = spec.get("of", "count")
    if of == "count":
        return len(rids)
    prop = spec.get("property")
    if not rids or not prop:
        return None
    if str(prop) in _OBJ_COLS:            # an object column (canonical/type/status), whitelisted
        values = [r["v"] for r in await pool.fetch(
            f"SELECT {prop} AS v FROM objects WHERE id = ANY($1::uuid[]) ORDER BY {prop}",
            rids) if r["v"] is not None]
    else:
        values = [r["v"] for r in await pool.fetch(
            "SELECT value #>> '{}' AS v FROM winning_props($1::uuid[]) WHERE name=$2",
            rids, str(prop)) if r["v"] is not None]
    if not values:
        return None
    if of == "first":                     # show-original: the single related object's value
        return values[0]
    if of == "max":
        return max(values)
    if of == "min":
        return min(values)
    nums = [n for n in (_num(v) for v in values) if n is not None]
    if of == "sum":
        return sum(nums)
    if of == "avg":
        return sum(nums) / len(nums) if nums else None
    return None


async def _aggregate(
    pool: asyncpg.Pool, objects: list[uuid.UUID], group_by: list[str], metric: dict[str, Any]
) -> list[dict[str, Any]]:
    """Group objects by property values, compute one metric per group (Palantir groupBy /
    Notion rollup). group_by=[] aggregates the whole set into a single row."""
    mtype = metric.get("type", "count")
    field_name = metric.get("field")
    groups: dict[tuple[str | None, ...], list[dict[str, str]]] = {}
    for oid in objects:
        facts = await _props(pool, oid)
        key = tuple(facts.get(g) for g in group_by)
        groups.setdefault(key, []).append(facts)
    rows: list[dict[str, Any]] = []
    for key, members in groups.items():
        group = {g: k for g, k in zip(group_by, key, strict=True)}
        if mtype == "count":
            value: float | int = len(members)
        else:
            raw = [m.get(field_name) for m in members] if field_name else []
            if mtype == "cardinality":
                value = len({v for v in raw if v is not None})
            else:
                nums = [n for n in (_num(v) for v in raw) if n is not None]
                value = _metric_over(mtype, nums)
        rows.append({"group": group, "metric": value})
    return rows


def _metric_over(mtype: str, nums: list[float]) -> float:
    if not nums:
        return 0.0
    if mtype == "sum":
        return sum(nums)
    if mtype == "avg":
        return sum(nums) / len(nums)
    if mtype == "min":
        return min(nums)
    if mtype == "max":
        return max(nums)
    raise ValueError(f"unknown aggregate metric: {mtype!r}")


async def _order(
    pool: asyncpg.Pool, base: Result, by: str | None, direction: str
) -> Result:
    rev = direction == "desc"
    if base.kind == "rows":
        def rkey(r: dict[str, Any]) -> tuple[float, str]:
            if by in (None, "metric"):
                v = r.get("metric")
            elif by in r:                       # a flat `table` column
                v = r.get(by)
            else:                               # an aggregate group dimension
                v = r.get("group", {}).get(by)
            n = _num(v)
            return (n if n is not None else 0.0, str(v))
        return Result("rows", rows=sorted(base.rows, key=rkey, reverse=rev))
    if base.kind == "values":
        def vkey(v: str) -> tuple[float, str]:
            n = _num(v)
            return (n if n is not None else float("inf"), v)
        return Result("values", values=sorted(base.values, key=vkey, reverse=rev))
    # objects — 'recency' orders by object birth (created_at), the one axis a property sort
    # can't reach (created_at is an object column, not an assertion): "newest first" / "recent
    # N" views (orient's recent decisions, any what's-new lens). Surfaced by dogfooding the
    # scoped briefing — the composer couldn't order by time until here.
    if by in ("recency", "newest", "created"):
        rows = await pool.fetch(
            "SELECT id FROM objects WHERE id = ANY($1::uuid[]) "
            f"ORDER BY created_at {'DESC' if rev else 'ASC'}, id",
            base.objects,
        )
        return Result("objects", objects=[r["id"] for r in rows])
    # objects — order by a property (numeric if possible, else lexical)
    keyed: list[tuple[float, str, uuid.UUID]] = []
    for oid in base.objects:
        raw = (await _props(pool, oid)).get(by) if by else None
        n = _num(raw)
        keyed.append((n if n is not None else float("inf"), str(raw or ""), oid))
    keyed.sort(key=lambda t: (t[0], t[1]), reverse=rev)
    return Result("objects", objects=[t[2] for t in keyed])


# --- persistence + run ------------------------------------------------------

async def save_composition(
    pool: asyncpg.Pool, name: str, spec: dict[str, Any], kind: str = "lens",
    *, webhook_url: str | None = None, active: bool = True, room_id: uuid.UUID | None = None,
    description: str | None = None, section: str | None = None,
) -> uuid.UUID:
    """Save (or update) a composition by name. Fork = save under a new name. `webhook_url`
    and `active` are a watch's execution metadata (a lens ignores them). `room_id` scopes it
    to a stance (NULL = unassigned; a re-save without a room keeps the existing one).
    `description` = one line of 'when to open this'; `section` = which shelf of the composer
    sidebar (arrive | wall | memory | fleet | engine | casework) — both keep their prior
    value when omitted on a re-save."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "INSERT INTO compositions (name, kind, spec, webhook_url, active, room_id, "
        " description, section) "
        "VALUES ($1,$2,$3,$4,$5,$6,$7,$8) "
        "ON CONFLICT (name) DO UPDATE SET spec=EXCLUDED.spec, kind=EXCLUDED.kind, "
        "  webhook_url=EXCLUDED.webhook_url, active=EXCLUDED.active, "
        "  room_id=COALESCE(EXCLUDED.room_id, compositions.room_id), "
        "  description=COALESCE(EXCLUDED.description, compositions.description), "
        "  section=COALESCE(EXCLUDED.section, compositions.section) RETURNING id",
        name, kind, spec, webhook_url, active, room_id, description, section,
    )


async def save_watch(
    pool: asyncpg.Pool, name: str, object_type: str | None, where: list[dict[str, Any]],
    *, canonical_prefix: str | None = None, webhook_url: str | None = None,
    room_id: uuid.UUID | None = None,
) -> uuid.UUID:
    """Save a WATCH — a composition whose spec is a `select` op. The same spec runs as a
    lens (current members) and drives the evaluator (alert on a new member). One primitive."""
    spec: dict[str, Any] = {"op": "select", "object_type": object_type, "where": where}
    if canonical_prefix:
        spec["canonical_prefix"] = canonical_prefix
    return await save_composition(pool, name, spec, "watch", webhook_url=webhook_url,
                                  room_id=room_id)


async def _spec_of(pool: asyncpg.Pool, ref: str) -> dict[str, Any] | None:
    row = await pool.fetchrow(
        "SELECT spec FROM compositions WHERE name=$1 OR id::text=$1", ref
    )
    return _coerce(row["spec"]) if row else None


# --- rooms: the stance (a Room is a composition of compositions — authorable by Claude) --

async def create_room(
    pool: asyncpg.Pool, name: str, config: dict[str, Any] | None = None
) -> uuid.UUID:
    """Create (or update) a Room — a saved stance the operator switches between. The FDE
    move: Claude mints one from a sentence, then assigns compositions to it."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "INSERT INTO rooms (name, config) VALUES ($1,$2) "
        "ON CONFLICT (name) DO UPDATE SET config=EXCLUDED.config RETURNING id",
        name, config or {},
    )


async def resolve_room(pool: asyncpg.Pool, ref: str | None) -> uuid.UUID | None:
    """A room by name or id (None ref → None = the All/unassigned scope)."""
    if not ref:
        return None
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT id FROM rooms WHERE name=$1 OR id::text=$1", ref
    )


async def list_rooms(pool: asyncpg.Pool) -> list[dict[str, Any]]:
    return [
        {"id": str(r["id"]), "name": r["name"], "config": _coerce(r["config"])}
        for r in await pool.fetch("SELECT id, name, config FROM rooms ORDER BY created_at")
    ]


async def list_compositions(
    pool: asyncpg.Pool, room_id: uuid.UUID | None = None
) -> list[dict[str, Any]]:
    """Saved compositions. `room_id` scopes to a stance (None = all rooms — the god view)."""
    return [
        {"id": str(r["id"]), "name": r["name"], "kind": r["kind"], "spec": _coerce(r["spec"]),
         "webhook_url": r["webhook_url"], "active": r["active"],
         "room_id": str(r["room_id"]) if r["room_id"] else None,
         "description": r["description"], "section": r["section"]}
        for r in await pool.fetch(
            "SELECT id, name, kind, spec, webhook_url, active, room_id, description, section "
            "FROM compositions "
            "WHERE ($1::uuid IS NULL OR room_id=$1) ORDER BY created_at", room_id
        )
    ]


async def object_items(pool: asyncpg.Pool, ids: list[uuid.UUID]) -> list[dict[str, Any]]:
    """Label a result set's objects AND carry their compact properties — in two batch
    queries, not N. The view-switcher needs this: the Graph view uses label/type, the
    Table view shows property columns (sector, date, …) without a per-row fetch."""
    if not ids:
        return []
    objs = await pool.fetch(
        "SELECT id, type, canonical FROM objects WHERE id = ANY($1::uuid[])", ids
    )
    props: dict[uuid.UUID, dict[str, str]] = {}
    for r in await pool.fetch(
        "SELECT object_id, name, value #>> '{}' AS v FROM winning_props($1::uuid[])", ids,
    ):
        props.setdefault(r["object_id"], {})[r["name"]] = r["v"]
    meta = {o["id"]: o for o in objs}
    out: list[dict[str, Any]] = []
    for oid in ids:  # preserve the composition's order
        o = meta.get(oid)
        if o is None:
            continue
        p = props.get(oid, {})
        # the best human label — never a raw hash when a name/title/summary exists
        label = (p.get("name") or p.get("title") or p.get("summary")
                 or p.get("subject") or o["canonical"])
        out.append({"id": str(oid), "type": o["type"], "canonical": o["canonical"],
                    "label": label, "props": p})
    return out


async def _package(pool: asyncpg.Pool, res: Result) -> Any:
    """Render a Result into the items the generic renderer consumes — objects become labelled
    rows, rows/values/data pass through. Shared by `run_spec` and the `sections` op (a section
    body is packaged exactly as a top-level composition would be)."""
    if res.kind == "objects":
        return await object_items(pool, res.objects)
    if res.kind == "rows":
        return res.rows
    if res.kind == "data":
        return res.data  # a Function's / sections' native output, passed through untouched
    return res.values


async def run_spec(
    pool: asyncpg.Pool, spec: dict[str, Any], subject: uuid.UUID | None = None,
    name: str = "(spec)", caller: str | None = None,
) -> dict[str, Any]:
    """Evaluate an op-tree and package the Result for the generic renderer. The inline
    composer (W4) runs an EPHEMERAL working spec through here as you edit chips — no save.
    `caller` is who is reading (an agent id, 'operator'/'console' for the human's own
    surfaces, None for anonymous) — the reflection ACL's input (6c18709f), carried to the
    ops on a contextvar so a nested select inherits it without parameter-threading."""
    token = _ACL_CALLER.set(caller) if caller is not None else None
    try:
        res = await _eval(pool, spec, subject)
        items = await _package(pool, res)
    finally:
        if token is not None:
            _ACL_CALLER.reset(token)
    count = len(items) if isinstance(items, list | dict) else 1
    return {"composition": name, "kind": res.kind, "count": count, "items": items, "spec": spec}


async def run_composition(
    pool: asyncpg.Pool, ref: str, subject: uuid.UUID | None = None,
    caller: str | None = None,
) -> dict[str, Any]:
    """Execute a saved composition (by name or id), optionally against a subject.
    `caller` = who is reading (the reflection ACL's input — see run_spec)."""
    spec = await _spec_of(pool, ref)
    if spec is None:
        return {"error": f"no composition {ref!r}"}
    return await run_spec(pool, spec, subject, name=ref, caller=caller)


# --- default compositions (templates — the engine's opinions, now forkable) --
# `operational-vs-disclosed-geography` IS the former `discrepancy` read-model, expressed
# as a composition: opinion has left the engine — it's a named, forkable spec the user
# owns. (Single-subject; the cluster-following nuance of discrepancy.py is left aside.)
GEOGRAPHY_DISCREPANCY: dict[str, Any] = {
    "op": "subtract",
    "left": {"op": "collect", "transform": "country", "properties": ["location"],
             "from": {"op": "traverse", "from": {"op": "subject"},
                      "direction": "both", "hops": 2}},
    "right": {"op": "collect", "transform": "country", "properties": list(_HOME_PROPS),
              "from": {"op": "subject"}},
}
# `briefing` — "where am I?" restored in one read (the arrival prosthesis: a returning human
# and a fresh Claude share the same zero-context state). Formerly three hand-written SQL queries;
# now a `sections` op-tree — each section a pure select→(order→take)→table. Opinion left the
# engine: the briefing is a page of compositions the user can fork, not coded output.
BRIEFING: dict[str, Any] = {
    "op": "sections",
    "sections": [
        # the RAW open-thread select showed 919 rows on 2026-07-11, 850 of them untouched
        # miner echoes — the wall function is the graded truth (ruling 923c380f)
        {"title": "The wall — what's genuinely unresolved",
         "body": {"op": "function", "name": "wall"}},
        {"title": "Recent work — what just happened",
         "body": {"op": "table",
                  "from": {"op": "take", "n": 8,
                           "from": {"op": "order", "by": "authored_date", "dir": "desc",
                                    "from": {"op": "select", "object_type": "Commit",
                                             "where": [{"property": "summary",
                                                        "op": "present"}]}}},
                  "columns": [{"name": "change", "property": "summary"},
                              {"name": "scope", "property": "scope"},
                              {"name": "when", "property": "authored_date"}]}},
        {"title": "Resolved — self-healed by later commits",
         "body": {"op": "table",
                  "from": {"op": "select", "object_type": "Thread",
                           "where": [{"property": "status", "op": "eq", "value": "resolved"}]},
                  "columns": [{"name": "thread", "property": "summary"},
                              {"name": "by", "property": "resolved_in"},
                              {"name": "because", "property": "resolved_because"}]}},
    ],
}
# `decision-log` — the project's WHY, mined into `Decision` objects. Formerly a SQL Function;
# now a single-section `sections` op-tree: the `of:"first"` (show-original) rollup plucks each
# decision's `decided_in` commit canonical + date (the renderer formats the ISO date), ordered
# newest-first. `summary present` drops empty rows. A kind filter = a `where` the user forks in.
DECISION_LOG: dict[str, Any] = {
    "op": "sections",
    "sections": [
        {"title": "Decisions — the project's WHY (mined from commit rationale)",
         "body": {"op": "order", "by": "when", "dir": "desc",
                  "from": {"op": "table",
                           "from": {"op": "select", "object_type": "Decision",
                                    "where": [{"property": "summary", "op": "present"}]},
                           "columns": [
                               {"name": "decision", "property": "summary"},
                               {"name": "kind", "property": "kind"},
                               # the gray: a value here means this entry is DEAD — its
                               # successor's short id + summary ride along (ruling dd04d7dd),
                               # so the log skims honestly without hiding the history
                               {"name": "superseded", "property": "superseded_because"},
                               {"name": "in", "rollup": {"direction": "out",
                                    "link_type": "decided_in", "of": "first",
                                    "property": "canonical"}},
                               {"name": "when", "rollup": {"direction": "out",
                                    "link_type": "decided_in", "of": "first",
                                    "property": "authored_date"}},
                           ]}}},
    ],
}
# the SCOPED arrival briefing — orient's project view as a pure composition (was bespoke SQL
# in mcp_server._project_briefing, #20). The fleet-wide `briefing`'s selects, INTERSECTED with
# the subject project's in_repo neighbourhood and recency-ordered. Run with a SoftwareProject
# subject; each section is take(table(order(intersect(select, traverse)))).
PROJECT_BRIEFING: dict[str, Any] = {
    "op": "sections",
    "sections": [
        # open_threads: the project's unresolved set, recency-desc. The pure op-tree can't
        # express "obligations first" (single-key order, no computed priority), so orient's
        # assembly layer (mcp_server._project_briefing) RE-RANKS this — obligations float above
        # ordinary threads — and display-caps at 25. The `take` here is only a safety ceiling on
        # the set the ranker sees; a standalone fork still gets the recency-ordered lens.
        {"title": "open_threads", "body": {
            "op": "take", "n": 100, "from": {
                "op": "table", "columns": [{"property": "summary"}, {"property": "kind"}],
                "from": {"op": "order", "by": "recency", "dir": "desc", "from": {
                    "op": "intersect", "sets": [
                        {"op": "select", "object_type": "Thread", "where": [
                            {"property": "status", "op": "eq", "value": "open"}]},
                        {"op": "traverse", "from": {"op": "subject"}, "direction": "in",
                         "link_type": "in_repo", "hops": 1}]}}}}},
        # superseded decisions leave the RECENT lens (the supersedes verb, ruling dd04d7dd):
        # a corrected hypothesis must not brief the next session as if it still stood. The
        # record keeps it; the decision-log (audit view) still lists it under its successor.
        {"title": "recent_decisions", "body": {
            "op": "take", "n": 15, "from": {
                "op": "table", "columns": [{"property": "id"}, {"property": "summary"},
                                           {"property": "kind"}],
                "from": {"op": "order", "by": "recency", "dir": "desc", "from": {
                    "op": "intersect", "sets": [
                        {"op": "select", "object_type": "Decision", "where": [
                            {"property": "superseded_by", "op": "absent"},
                            # ...and never brief a mind with the miner's retracted slop: the
                            # janitor sweeps its own output, and the record keeps every row
                            {"property": "retracted", "op": "absent"}]},
                        {"op": "traverse", "from": {"op": "subject"}, "direction": "in",
                         "link_type": "in_repo", "hops": 1}]}}}}},
        # the live tensions — held polarities the session inherits (not a verdict). A Tension
        # is its own type, so grade-resolution / consolidation never flatten it into an answer.
        {"title": "tensions", "body": {
            "op": "take", "n": 20, "from": {
                "op": "table", "columns": [{"property": "pole_a"}, {"property": "pole_b"},
                                           {"property": "lean"}],
                "from": {"op": "order", "by": "recency", "dir": "desc", "from": {
                    "op": "intersect", "sets": [
                        {"op": "select", "object_type": "Tension"},
                        {"op": "traverse", "from": {"op": "subject"}, "direction": "in",
                         "link_type": "in_repo", "hops": 1}]}}}}},
        # the project's registered BLIND SPOTS — the shape of its own ignorance (8e26cd10):
        # what the harness here cannot see, and where the real verification lives. Held like
        # a Tension (its own type, never resolved away); orient speaks them so a session
        # knows before it trusts a green harness.
        {"title": "blind_spots", "body": {
            "op": "take", "n": 10, "from": {
                "op": "table", "columns": [{"property": "surface"}, {"property": "cannot_see"},
                                           {"property": "verify_with"}],
                "from": {"op": "order", "by": "recency", "dir": "desc", "from": {
                    "op": "intersect", "sets": [
                        {"op": "select", "object_type": "BlindSpot"},
                        {"op": "traverse", "from": {"op": "subject"}, "direction": "in",
                         "link_type": "in_repo", "hops": 1}]}}}}},
    ],
}


DEFAULT_COMPOSITIONS: dict[str, dict[str, Any]] = {
    "operational-vs-disclosed-geography": GEOGRAPHY_DISCREPANCY,
    # the arrival briefing — a `sections` op-tree, no longer a hand-written Function.
    "briefing": BRIEFING,
    # the SCOPED briefing — orient's per-project bearings, subject = a SoftwareProject (#20).
    "project-briefing": PROJECT_BRIEFING,
    # the former bespoke read-models, now forkable compositions over named Functions —
    # opinion left engine code (no more hardcoded read-model + bespoke MCP tool per lens).
    "co-investment-ties": {"op": "function", "name": "coinvest"},
    "who-is-this": {"op": "function", "name": "subject_report"},
    "screen-financing-network": {"op": "function", "name": "screen_network"},
    # the dedicated canon view: the project's design memory (Palantir/Notion + own docs),
    # rendered as a sectioned read-model. Run with no subject; `consult_canon(q)` queries it.
    "design-canon": {"op": "function", "name": "canon", "args": {}},
    # the decision log: the project's WHY — a `sections` op-tree (was a Function).
    "decision-log": DECISION_LOG,
    # the family audit: what drifted across a set of similar repos (every ingested family).
    "family-consistency": {"op": "function", "name": "family"},
    # content drift: for the files a family shares, do they AGREE (license type / config bytes)?
    "family-drift": {"op": "function", "name": "family_drift"},
    # the portfolio map: every ingested repo's stack + what it's ABOUT (distinctive terms) —
    # cross-repo cognition's gather step; the lens/tripwire names the shared primitives.
    "portfolio": {"op": "function", "name": "portfolio"},
    # the heartbeat digest: what the off-the-clock pulse found while you were away.
    "pulse-digest": {"op": "function", "name": "pulse"},
    # the fleet roster — every Claude instance registered in the shared graph, its model and
    # project. "A man and all his imaginary friends", on the human console. A pure op-tree
    # (select Agent → table), not a hardcoded panel: the composer discipline holds even here.
    "fleet": {
        "op": "table",
        "from": {"op": "select", "object_type": "Agent"},
        "columns": [
            {"name": "agent", "property": "name"},
            {"name": "model", "property": "source_model"},
            {"name": "project", "property": "project"},
        ],
    },
    # the developer project browser — DECOMPOSED: no longer a hardcoded Function, a pure op-tree
    # the user owns. `select` the repos → `table` with rollup columns (Notion database+rollups)
    # → `order` by last-touched. This is what "everything is composed" means in practice.
    "projects": {
        "op": "order", "by": "last_touched", "dir": "desc",
        "from": {
            "op": "table",
            "from": {"op": "select", "object_type": "SoftwareProject"},
            "columns": [
                {"name": "project", "property": "name"},
                {"name": "commits", "rollup": {"direction": "in", "link_type": "in_repo",
                                               "object_type": "Commit", "of": "count"}},
                {"name": "files", "rollup": {"direction": "in", "link_type": "in_repo",
                                             "object_type": "File", "of": "count"}},
                {"name": "last_touched", "rollup": {"direction": "in", "link_type": "in_repo",
                                                    "object_type": "Commit", "of": "max",
                                                    "property": "authored_date"}},
            ],
        },
    },
    "project": {"op": "function", "name": "project"},
    # rung 3: the per-object provenance timeline — how the graph came to believe a thing.
    "lap": {"op": "function", "name": "lap"},
    # rung 2: the graph auditing itself — report-only findings, testimony not verdicts.
    "graph-lint": {"op": "function", "name": "lint"},
    "echoes": {"op": "function", "name": "echoes"},
    # THE ONE WALL LAW (ruling 923c380f): the graded unresolved view — orient's law as a lens.
    "the-wall": {"op": "function", "name": "wall", "args": {"me": ["operator"]}},
}

# THE SHELF (ruling 923c380f): which sidebar section a lens belongs to + one line of 'when
# to open this'. Applied by the seeder to defaults AND to already-saved compositions whose
# names it knows — the lens clusterfuck was 19 flat chips in builder-dialect.
_COMP_META: dict[str, tuple[str, str]] = {
    "briefing": ("arrive", "start here — the graded wall, recent work, what self-healed"),
    "pulse-digest": ("arrive", "what the autonomic loop sensed lately"),
    "the-wall": ("wall", "what is GENUINELY unresolved — obligations first, echoes counted"),
    "open threads": ("wall", "the raw unresolved list (ungraded — prefer the-wall)"),
    "echoes": ("wall", "the triage pile: untouched miner echoes, oldest first"),
    "decision-log": ("memory", "every decision with its WHY; superseded entries grayed"),
    "design-canon": ("memory", "the design memory — ask it before re-deriving"),
    "recent work": ("memory", "latest commits across the graph"),
    "changelog by area": ("memory", "what changed, grouped by area"),
    "fable-commits": ("memory", "commits authored by fleet sessions"),
    "the composer arc": ("memory", "the composer's own build history"),
    "fleet": ("fleet", "every agent the graph knows"),
    "projects": ("fleet", "all repos by recency of touch"),
    "project": ("fleet", "one repo's brief — focus a repo or pass args.repo"),
    "project-briefing": ("fleet", "a project's scoped briefing (what orient reads)"),
    "portfolio": ("fleet", "the operator's repos as a portfolio"),
    "graph-lint": ("engine", "the graph auditing itself — findings, not verdicts"),
    "family-consistency": ("engine", "config families that should agree but don't"),
    "family-drift": ("engine", "how config families drift over time"),
    "lap": ("engine", "one object's provenance timeline — how belief formed"),
    "operational-vs-disclosed-geography": ("casework", "where an org operates vs claims"),
    "co-investment-ties": ("casework", "who co-invests with the subject"),
    "who-is-this": ("casework", "the subject's dossier at a glance"),
    "screen-financing-network": ("casework", "the subject's financing network, screened"),
}


async def seed_default_compositions(pool: asyncpg.Pool) -> int:
    for name, spec in DEFAULT_COMPOSITIONS.items():
        section, desc = _COMP_META.get(name, (None, None))
        await save_composition(pool, name, spec, "lens", description=desc, section=section)
    # the shelf also reaches saved, non-default lenses it knows by name (agent-authored
    # twins of the defaults) — metadata only, never their spec
    for name, (section, desc) in _COMP_META.items():
        if name not in DEFAULT_COMPOSITIONS:
            await pool.execute(
                "UPDATE compositions SET section=$2, description=$3 "
                "WHERE name=$1 AND section IS NULL", name, section, desc)
    return len(DEFAULT_COMPOSITIONS)
