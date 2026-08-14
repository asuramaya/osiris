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
  {"op":"table","from":N,"columns":[...],"row_action":?} -> one ROW per object, columns = a
       property OR a rollup-over-a-link (Notion's database+rollups / Palantir's object-set+
       per-object aggregate). column = {"name":,"property":P} | {"name":,"rollup":
       {"direction":in|out|both,"link_type":?,"object_type":?,
       "of":count|first|max|min|sum|avg,"property":?}}. `first` = Notion's show-original
       (pluck a single relation's value, incl. an object column like `canonical` — how a
       linked commit/entity is named). `row_action` (ruling c5b184cd, thread d56e7073/#44 —
       the write leg) declares a CONTROL every row carries, not a column:
       {"action":<name>,"args":{<argname>:{"property":P}}}, resolved per-row into a private
       `_action` key the renderer turns into a button — the op-tree only ever DECLARES the
       shape; `/act`'s own registry is what enforces which actions/args are real.
  {"op":"order","from":N,"by":?,"dir":}            -> rank a set/rows (.orderBy)
  {"op":"take","from":N,"n":K}                      -> top-N (.take)
  {"op":"sections","sections":[{"title":,"body":N},...]} -> stack named sub-compositions into
       one titled read-model (Notion's page-of-blocks). Each body is its own op-tree; the
       result is {title: rendered-items}. This is what a "briefing"/"dossier" IS — a page of
       compositions, not bespoke code.
  {"op":"group","from":N,"by":P,"body":N,"sequence":?}  -> one section PER DISTINCT VALUE of
       property P (a DYNAMIC `sections` — titles come from the data, not the spec). `body`
       is evaluated once per partition; {"op":"these"} inside it means "this partition's
       members," so `body` may itself be another `group` — arc->status->owner IS three of
       these nested, nothing more (ruling c5b184cd, thread d56e7073/#44). Capped at
       MAX_GROUP_DEPTH so nesting stays closed, not open-ended.
       `sequence` (ruling d42c543b, Thoth msg 1937) imposes a caller-given order on the
       partition TITLES — distinct from `order`, which sorts rows/objects by a DERIVED
       property value, never a caller-literal key sequence. Listed titles render first, in
       that order (one absent from the data just doesn't appear — never an error); any
       title NOT in the sequence appends after, alphabetically, so an unanticipated value
       is visible, never dropped. Independent per nesting level — an outer and inner
       `group` may each carry their own `sequence` or none.
  {"op":"these"}                                    -> the nearest enclosing `group`'s own
       partition (empty outside one) — {"op":"subject"}'s sibling for a group body.
  {"op":"function","name":,"args":{},"row_action":?,"row_actions":?} -> a registered
       Function (the escape hatch). When its own output is row-shaped (task #60's data->rows
       reclassification), `row_action` works exactly as `table`'s own (msg 1952, gating msg
       1950's server-side proposal) — SIMPLER, even: a Function's row is already a plain
       dict, so args resolve via `row.get(property)` directly, no `_props` indirection. The
       client (table() recognizing a lone `_action` as a control, a click-delegate POSTing
       to /act) shipped in 37af8b7 — browser-verified against live-desk's own resolve
       button. An action named `"run:<function>"` is NAVIGATION rather than a write — the
       client dispatches a DOM event instead of POSTing, the page shell runs the named
       Function via /compositions/run-spec and shows its Result (task #90, Thoth msg 1976/
       2005) — see `mail_overview`'s own row_action for the motivating case.
       `row_actions` (plural, msg 1976 gating msg 1971's proposal) is for a row that affords
       MORE than one verb — a list of {label,action,args}, producing `_actions:[...]` on the
       row. Its own arg templates add `{"literal":v}` alongside `{"property":p}` (exactly
       one of the two, or the composer refuses loudly — see `_row_action_arg`'s own
       docstring). The client renders `_actions` as N buttons, same click delegate, same /act
       round trip per button as the singular form (task #91, Thoth msg 1976/2029) — a Function
       may also embed `_actions`/`_action` directly on rows it returns, without this node-level
       declaration, when a saved composition can't express the shape. Two distinct reasons seen
       so far, not one: `desk_project` (two row kinds in one list, needing two different action
       shapes — see its own docstring) and `_fn_echoes` (task #92: the node-level grammar
       decorates a Function's OWN top-level rows, but echoes' top level is a dict wrapping a
       nested list — nothing for it to hook. Two Functions hitting embedding for the SAME
       reason would be the signal to promote that reason to a real op; these are two different
       reasons, so it isn't, yet — see `_fn_echoes`'s own docstring).

The old `discrepancy` read-model is just one composition (opinion left the engine):
  subtract( collect(location, country) over traverse(subject, 2 hops),
            collect(home-props, country) over subject )

There is deliberately NO generic `join` — relating two sets is `intersect` (set algebra)
or `traverse` (a link), and fuzzy matching (screening) is a Function. Caps (Palantir's,
load-tested): `traverse` ≤ 3 hops, `aggregate` ≤ 3 group_by dimensions.
"""

from __future__ import annotations

import json
import logging
import re
import uuid
from collections import Counter
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg

from src.ontology.labels import disambiguate_labels, fetch_label_props, resolve_label
from src.ontology.resolution import screen_network
from src.orchestrator.agents import _generation
from src.orchestrator.coinvest import coinvestment_ties
from src.orchestrator.discrepancy import _HOME_PROPS, country_of
from src.orchestrator.frontier import subject_report
from src.orchestrator.monitor import match_condition
from src.orchestrator.neighborhoods import NEIGHBORHOOD, neighborhoods_of
from src.orchestrator.seats import _OPERATOR_ACTORS

logger = logging.getLogger("osiris.compositions")

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

# THE INDEXED FIELDS (THE THAW, ruling 1e6d7367, migration 0037): must match
# ix_assertions_fts's partial predicate exactly, or the planner falls off the index. Long
# excluded 'statement' — Superstition's own field has never been searchable since it
# shipped (a latent gap this build heals alongside making Practice findable).
_FTS_FIELDS = "'name','summary','rationale','statement'"


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


async def _caller_house(pool: asyncpg.Pool, caller: str | None) -> str | None:
    """The house a caller reads reflections as — a SECURITY-RELEVANT read (this gates
    cross-house reflection visibility, ruling ff6148b0): '*' for the operator's own
    surfaces (`seats._OPERATOR_ACTORS` — the ONE definition of "this actor is the
    operator's own hand", shared with derive_house's house-anchor check and mintseat's
    cross-house-mint guard; a second, locally-drifted copy here once excluded
    'analyst:operator' — the API layer's own attribution string for the human's triage
    clicks — task #82), None for an anonymous caller (reads NO reflections — an
    unmounted stranger has no house), else the caller's seat house, falling back to its
    project label (most projects are their own house). `held_seat`'s `house` is DERIVED
    (decision 4c9e4bd7) — this inherits that fix for free, no query of its own."""
    if caller in _OPERATOR_ACTORS:
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


async def _attach_labels(pool: asyncpg.Pool, hits: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Task #97 workstream 3 (ruling 52daab71): search hits carry `field`/`snippet` —
    WHERE the query matched — but never what to CALL the object, which is exactly the
    reported bug (a hex hash where a sentence belongs, e.g. `practice:b1eb7520e783`).
    One batched fetch of each hit's LABEL_CHAIN-candidate properties (the same winning-
    value tie-break as everywhere else: confidence DESC, observed_at DESC), resolved
    through the single canonical `resolve_label`, then `disambiguate_labels` across
    THIS result set so two hits that would otherwise truncate identically stay
    distinguishable. Adds `label`/`label_source` (resolve_label's own fields) and
    `display_label` (disambiguated, truncated) to each hit; never raises — a hit with
    nothing resolvable just keeps its canonical, same as before this existed."""
    if not hits:
        return hits
    ids = [uuid.UUID(h["id"]) for h in hits]
    by_uuid = await fetch_label_props(pool, ids)
    props_by_id = {str(k): v for k, v in by_uuid.items()}
    for h in hits:
        res = resolve_label(h["type"], props_by_id.get(h["id"], {}), h["canonical"])
        h["label"], h["label_source"] = res.label, res.source
    disp = disambiguate_labels([(h["id"], h["label"], h["canonical"]) for h in hits])
    for h in hits:
        h["display_label"] = disp[h["id"]]
    return hits


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
        # the WHOLE query is one id token — answer directly, the legacy shape unchanged.
        # This is the MOST likely path to hit the reported bug (someone pastes a bare
        # hash to search it) — label it same as the merged-hits path below.
        id_hits = await _attach_labels(pool, id_hits)
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
        "  WHERE a.name IN (" + _FTS_FIELDS + ") "
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
    hits = await _attach_labels(pool, hits)
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
        # a refuted Practice must stay findable (THE THAW, ruling 1e6d7367: 'a half-
        # remembered refuted lesson is exactly what must remain findable'), unlike a
        # superseded Decision it is NEVER hidden or buried — only flagged, same batched
        # shape as the supersedes lookup above
        refuted = {str(r["object_id"]): r["v"] for r in await pool.fetch(
            "SELECT DISTINCT ON (object_id) object_id, value #>> '{}' AS v "
            "FROM current_assertions WHERE name='refuted_by' "
            "AND object_id = ANY($1::uuid[]) "
            "ORDER BY object_id, confidence DESC, observed_at DESC",
            [h["id"] for h in hits]) if (r["v"] or "").strip()}
        for h in hits:
            if h["id"] in refuted:
                h["refuted"] = (f"by decision {refuted[h['id']][:8]} — a dead lesson, "
                                "not standing law")
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
    "  WHERE a.name IN (" + _FTS_FIELDS + ")), "
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
    "  WHERE a.name IN (" + _FTS_FIELDS + ") "
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


async def _fn_reference_catalog(
    pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any],
) -> Any:
    """The type catalog osiris SHIPS — object/link types, straight from schema.py's static
    declared manifest (task #111, thread 26694d10). Deliberately NOT catalog.py's live,
    accretive one: `ensure_type`'s stub-minting must never touch what this reads (Thoth's
    ruling, msg 2099) — an agent correctly using the accretion path can never turn this
    composition, or the REFERENCE.md doc `docs_compiler.py` renders from the SAME manifest,
    into something that changed underneath them. Ignores `pool`/`subject` entirely: the data
    is pool-free by the same ruling that makes the doc pool-free."""
    from src.ontology.schema import catalog

    return catalog()


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
    # THE UNCITED RULING (Thoth DM 2704, finding 1 of the in_repo audit): a Decision's OWN
    # in_repo edge (record_decision(repo=...) mints one via link_repo, at birth) used to
    # count for nothing here — only a decided_in citation to a commit that is itself
    # in_repo did. A ruling filed under a repo but naming no commit sha (the common case —
    # most of a session's own rulings, including several that named THIS gap) was invisible
    # in its own project's decision browser. UNION, not replace: the commit-derived path
    # still finds decisions whose OWN in_repo edge is missing but whose cited commit's
    # isn't (a real, if rarer, shape); the direct path now also finds decisions with no
    # commit citation at all.
    decisions = await pool.fetch(
        "WITH qualifying AS ("
        "  SELECT DISTINCT d.id FROM objects d "
        "  JOIN links dl ON dl.from_id=d.id AND dl.type='decided_in' "
        "  JOIN links rl ON rl.from_id=dl.to_id AND rl.type='in_repo' AND rl.to_id=$1 "
        "  WHERE d.type='Decision' "
        "  UNION "
        "  SELECT DISTINCT d.id FROM objects d "
        "  JOIN links l ON l.from_id=d.id AND l.type='in_repo' AND l.to_id=$1 "
        "  WHERE d.type='Decision'"
        ") "
        "SELECT "
        " (SELECT value #>> '{}' FROM current_assertions a WHERE a.object_id=q.id "
        "  AND a.name='summary' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS summary, "
        " (SELECT value #>> '{}' FROM current_assertions a WHERE a.object_id=q.id "
        "  AND a.name='kind' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS kind "
        "FROM qualifying q LIMIT 20", repo)
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
    """Accept a UUID, a short-id PREFIX, an exact canonical, or a name; resolve to an object
    id. Name matching tries exact first (most-described wins), then substring (shortest name
    wins — closest to the query). ONE definition shared by the server's tools and the
    composition functions, so the console and an agent always resolve the same words to the
    same object.

    The short-id leg (task #64, ruling ad19a779 — every id a composition row hands out must
    feed straight back in) mirrors capture._find_thread/_find_decision's own convention
    exactly (same regex, same order, one object type wider — this resolves ANY type, not
    just Thread/Decision): a `table`/Function-sourced row's own "id" column IS this 8-char
    prefix (`_col_value`'s special case), and dossier()/focus_object() previously had no way
    to resolve it at all — only recall() (via _find_thread/_find_decision) could. Verified
    live before this fix: dossier("3e96c10e") returned "no object", though recall() resolved
    the identical ref cleanly."""
    try:
        return uuid.UUID(ref)
    except ValueError:
        pass
    short = (ref or "").strip().lower()
    if re.fullmatch(r"[0-9a-f]{8}[0-9a-f-]*", short):
        oid = await pool.fetchval(
            "SELECT id FROM objects WHERE status='active' AND id::text LIKE $1 || '%' LIMIT 1",
            short)
        if oid is not None:
            return uuid.UUID(str(oid))
    oid = await pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1 AND status='active' LIMIT 1", ref)
    if oid is not None:
        return uuid.UUID(str(oid))
    # A FLEET HANDLE ("sekhmet") must resolve to the REAL agent, never a harness sidechain
    # artifact that happens to share the substring (task #114, Seshat XIII, thread
    # 05a72d2c0af0 — found live: dossier("sekhmet") returned "sekhmet I.1", a spawned_by
    # visitor object, ahead of agent:seat-af50a33e, the real body, reachable only by
    # following that artifact's OWN spawned_by edge). agents.resolve_seat is the already-
    # correct, battle-tested resolver mail routing uses for exactly this: it explicitly
    # excludes spawned_by visitors ("a spawn wearing a handle is a leak, resolving mail into
    # it buries the message in a sidechain nobody resumes" — its own docstring) and, among
    # real candidates, a live seat always wins and the latest generation outranks its
    # ancestor. Tried before the generic name-matching legs below, which have no concept of
    # "visitor" at all and would happily match the shorter, ILIKE-friendliest sidechain label.
    from src.actions.core import Actions
    from src.orchestrator.agents import resolve_seat

    seat = await resolve_seat(Actions(pool), ref)
    if seat.get("agent"):
        oid = await pool.fetchval(
            "SELECT id FROM objects WHERE canonical=$1 AND type='Agent'", seat["agent"])
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
    `args.repo` scopes to one project; report-only.

    Each row carries `_actions` (task #92, replacing index.html's own bespoke checkbox+
    bulk-triage bar) embedded directly here rather than declared via the composition's own
    `row_actions` node-level grammar — NOT desk_project's reason (mixed row shapes one
    declaration can't express uniformly): every echo row is the same shape and gets the
    same three verbs. The reason here is structural: this Function's own TOP-LEVEL return is
    a dict (`{"echoes": [...], "count": ..., ...}`, kind='data'), not a bare list[dict]
    (kind='rows') — the node-level grammar decorates a Function's OWN output rows, and a
    dict has none to decorate; only the nested `echoes` list does. A second, distinct
    boundary condition from Thoth's ruling on desk_project (msg 2043) — flagged here so it
    doesn't have to be rediscovered."""
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
            eid = str(r["id"])[:8]
            echoes.append({
                "id": eid, "born": r["created_at"].date().isoformat(),
                "project": (r["project"] or "").removeprefix("repo:") or None,
                "kind": r["kind"], "summary": r["summary"][:200],
                **({"probably_done": r["probably_done"]} if r["probably_done"] else {}),
                # per-row triage (task #92 — row_actions, not a bulk-select toolbar: this
                # Function's own `verbs` field below already documented "triage with
                # testimony, never bulk writes" before the UI ever caught up to it). Same
                # ACTION_VERBS/`_find_thread` short-id resolution proven live for desk's
                # debt rows (task #91) — `eid` is the same 8-char truncated form.
                "_actions": [
                    {"label": "resolve", "action": "resolve_thread",
                     "args": {"ref": eid, "because": "operator: resolved from the echo pile"}},
                    {"label": "adopt", "action": "reclassify_thread",
                     "args": {"ref": eid, "kind": "obligation"}},
                    {"label": "question", "action": "reclassify_thread",
                     "args": {"ref": eid, "kind": "question"}},
                ]})
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

# THE RATCHET (Thoth DM 2581/2603, decision fc5b6c5f/5713e1fc, cb38d922): resolved-with-no-
# closure-edge must never increase. Armable now, not just measurable, because all three
# sanctioned closing paths mint an edge unconditionally — capture.py's resolve_thread/
# record_decision (Phase 1a, commit 23c5991), close_by_commits' strong verdict (commit
# 0a629f6), and _resolve_own_threads no longer writes status at all (same commit). Growth
# past this ceiling can only mean a bypass — raw SQL, a new writer nobody gated, or a
# healed/invalidated closure edge with the property left resolved. Fleet-wide baseline
# measured live 2026-08-01 (read-only query, DSN port 5601): 949. Lower this constant the
# moment a deliberate historical backfill lands and reduces the real count — never raise it
# to chase a violation; a ratchet that moves to match the pile it was built to catch is not
# a ratchet.
EDGELESS_CLOSURE_CEILING = 949


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
    ROT-CANDIDATE-UNSCOPED (info: how many open threads have no in_repo edge at all and so
    cannot be evaluated by the check above — a declared boundary, not a defect),
    EDGELESS-CLOSURE-GROWTH (error: fleet-wide resolved-with-no-closure-edge past
    EDGELESS_CLOSURE_CEILING — the cb38d922 ratchet; every sanctioned closing path mints an
    edge unconditionally now, so growth here can only mean a bypass),
    ATTRIBUTION (writes from agent ids the graph never registered — the impersonation
    class, made a standing tripwire), PHANTOM-TWIN (an anonymous un-spawned agent mounted
    at a Seat's office beside a different holder lineage — a resumed soul wearing a second
    row; the one identity degradation that must never be silent), PARALLEL-LIVES (a
    generation minted while a different door of its own lineage held a live pulse — the
    predecessor was not dead; reads the parallel_pulse_door stamp mint_heir writes at
    the mint, thread 4bcd6541), DUPLICATE-WORKS-IN (a currently-LIVE agent carrying more
    than one simultaneously-live works_in edge — orient() resolves through exactly one, so
    a live lineage's own threads/decisions can hide from itself, John XVII's own specimen;
    thread 8640a625/decision fce39baa — invalidate_works_in is the repair, this only
    counts, per ruling 1973d46f's own law that a reconciler with no trigger is worse than
    none), PEER-SILENT (warn: an active peer_of pair with no direct mail between either
    side's holders in `stale_days` — a mechanical proxy for v1's fiduciary-disclosure duty,
    task #76 item 2, spec e6636c7e; testimony that a pair has gone quiet, never proof a
    finding was actually withheld), HELD-PAST-DEADLINE (warn: a hold_action() thread still
    open past its own time-box — task #76 item 4b, the mutual HOLD's auto-escalation half
    built as a lint check rather than a new daemon; testimony a mind takes to the
    operator's desk, never lint's own push).

    `check`/`limit`/`offset` (task #74, thread 12a210ab leg 1): every check hard-caps its
    LISTED findings at `_LINT_CAP` (50) regardless — the reap needed the full 19
    contradiction rows and full 24 false-mint rows and could only get them by hand-writing
    _fn_lint's own SQL again. Pass `check` (one of the `check` values a finding/`counts` key
    carries, e.g. 'false-mint') to list ONLY that check's findings, with `limit`/`offset`
    paginating its FULL row set instead of the 50-cap (default: uncapped, all of it, in one
    page) — a named check is ALWAYS fetched to its true total, however large (thread
    187323d9: orphan-link used to silently self-truncate its fetch at 5000 rows regardless
    of the real population, so an offset past that point returned an empty page while still
    reporting a positive remainder — fixed; every check's own full row set is now genuinely
    reachable, matching this paragraph's own promise). Every OTHER check still just reports
    its `counts` total — unfiltered calls are BYTE-IDENTICAL to before this existed
    (`check=None` is a complete no-op).

    `severity`/`counts_by_severity` (thread 187323d9, Thoth DM 3143): `counts` alone mixes
    info-grade metered history (orphan-link) with warn/error-grade damage in one flat list —
    trusting it at face value overstated this graph's real debt by 54x, live. `severity` maps
    each check name to its grade (info/warn/error); `counts_by_severity` is the one-glance
    rollup — read that before `counts` when the question is how much of this actually
    matters."""
    stale_days = max(1, min(int(args.get("stale_days") or 14), 365))
    eps = float(args.get("eps") or 0.05)          # "near-tie" on the confidence axis
    live_secs = int(args.get("live_secs") or 900)  # a mount seen this recently is LIVE
    check_filter = str(args.get("check") or "").strip() or None
    raw_limit = args.get("limit")
    page_limit = max(1, min(int(raw_limit), 5000)) if raw_limit is not None else None
    page_offset = max(0, int(args.get("offset") or 0))
    findings: list[dict[str, Any]] = []
    counts: dict[str, int] = {}
    severity_by_check: dict[str, str] = {}

    def land(check: str, severity: str, rows: list[dict[str, Any]]) -> None:
        counts[check] = len(rows)
        severity_by_check[check] = severity
        if check_filter is not None:
            if check != check_filter:
                return  # per-check filter: every OTHER check's rows are never listed
            page = rows[page_offset:]
            listed = page if page_limit is None else page[:page_limit]
        else:
            listed = rows[:_LINT_CAP]
        for r in listed:
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
    #
    # TWO MORE EXCLUSIONS (thread 4a7da43a/12a210ab, reap Stage 1b leg 1, 2026-07-28):
    # (1) NON-ACTIVE SUBJECTS — a merged/historical/archived object's internal coin-flips
    # are history, not live ambiguity: nothing in the read-path (lineage_head resolves
    # merged_into before ever touching a loser's own properties) ever surfaces them, so
    # flagging them is the same "cry wolf" class the orphan-link check already excludes for
    # the same reason. (2) SUCCEEDED_BY VS AN EMPTY DEBOUNCE/HEAL GUESS — seam-debounce and
    # husk-heal both write succeeded_by='' at debounce/heal time as a "no successor seen
    # yet" placeholder; once a real generation self-declares succeeded_from back at the
    # predecessor, the guess is permanently stale but NEVER a live dispute (walked and
    # verified live: lineage_head's walk continues through the winner regardless of whether
    # it later gets healed as false_mint itself — decision c41f74a6 — so this is resolver
    # noise from a known automated observer, not a coin-flip a mind needs to referee).
    # rn=1 vs rn=2 ALONE used to miss a rank-3+ rival hiding behind an agreeing top-2 (the
    # auditor's completeness gap, thread 59e95366/decision 93d8d15c — confirmed on
    # repo:bytebye/name: 19 rows sat invisible at rn=3+ purely because rn=1 and rn=2
    # happened to already agree).
    # `per_value` collapses every source's row to ONE best row per DISTINCT VALUE first
    # (same confidence/observed_at tiebreak the ranking already used), so two corroborating
    # sources on the winning value can no longer occupy both of the compared slots and hide
    # a genuinely different value sitting one rank deeper. `ranked` then compares the winner
    # against EVERY other distinct value (r.rn>1), not just the row immediately below it —
    # the RESOLVER's own supersession is untouched by this (it still serves the single
    # current-winning assertion exactly as before); only the AUDITOR's coverage widens.
    con = await pool.fetch(
        "WITH multi AS (SELECT object_id, name FROM current_assertions "
        "  WHERE name NOT IN ('status', 'resolved_in', 'resolved_because') "
        "  GROUP BY object_id, name HAVING count(DISTINCT source_id) > 1), "
        "per_value AS (SELECT DISTINCT ON (ca.object_id, ca.name, ca.value #>> '{}') "
        "  ca.object_id, ca.name, ca.value #>> '{}' AS v, ca.source_id, ca.confidence, "
        "  ca.observed_at "
        "  FROM current_assertions ca JOIN multi USING (object_id, name) "
        "  ORDER BY ca.object_id, ca.name, ca.value #>> '{}', "
        "    ca.confidence DESC, ca.observed_at DESC), "
        "ranked AS (SELECT *, row_number() OVER (PARTITION BY object_id, name "
        "    ORDER BY confidence DESC, observed_at DESC) AS rn "
        "  FROM per_value) "
        "SELECT o.canonical, w.name AS field, w.v AS winner, w.source_id AS winner_source, "
        "  w.confidence AS winner_conf, r.v AS rival, r.source_id AS rival_source, "
        "  r.confidence AS rival_conf "
        "FROM ranked w JOIN ranked r ON r.object_id=w.object_id AND r.name=w.name "
        "  AND w.rn=1 AND r.rn>1 "
        "JOIN objects o ON o.id=w.object_id "
        "WHERE w.v IS DISTINCT FROM r.v AND w.source_id <> r.source_id "
        "  AND w.confidence - r.confidence <= $1 "
        "  AND o.status = 'active' "
        "  AND NOT (w.name = 'succeeded_by' AND r.v = '' "
        "    AND r.source_id IN ('seam-debounce', 'husk-heal')) "
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
    # this check's own SQL pre-limits to _LINT_CAP (unlike every other check, which fetches
    # its FULL row set and only caps at land()'s own display layer) — a genuine, justified
    # optimization for the DEFAULT unfiltered call, where only _LINT_CAP rows are ever
    # displayed regardless of the real population.
    #
    # THE BUG THIS REPLACED (thread 187323d9, decision 6647fcd5, Thoth DM 3143): when the
    # check IS explicitly named, land()'s own offset/limit slicing assumes it received the
    # FULL row set to slice in Python — exactly what every OTHER check already does. This
    # fetch used to hard-cap at min(page_offset + page_limit, 5000) regardless of how large
    # the real population was, so any offset landing past what actually got fetched sliced
    # against a too-short list and silently returned [] — while `counts`/`remaining` (built
    # from the independent COUNT(*) below) kept reporting a genuine positive remainder.
    # Blindness rendered as silence, never a refusal — proven live against a 10,637-row
    # population, reproduced in tests/test_lap_lint.py at 5,010 rows. Fetching exactly
    # `orphan_total` rows when the check is named matches the function's own documented
    # contract ("paginating its FULL row set... default: uncapped, all of it") — the same
    # promise every sibling check already keeps unconditionally.
    orphan_fetch = orphan_total if check_filter == "orphan-link" else _LINT_CAP
    orphans = await pool.fetch(
        "SELECT l.type, fo.canonical AS from_c, fo.status AS from_s, "
        f" t.canonical AS to_c, t.status AS to_s {_ORPHAN_WHERE} "
        "ORDER BY l.last_seen DESC LIMIT $1", orphan_fetch)
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

    # ROT-CANDIDATE-UNSCOPED (info, Thoth DM 2704, finding 2 of the in_repo audit): the check
    # above INNER JOINs in_repo — structurally, not by oversight: a thread's "did a later
    # commit do this" verdict needs THAT repo's commits to compare against, and a repo-less
    # thread has no commit corpus to be compared to. There is no signal to compensate with
    # (open_thread_wall's union-by-owner doesn't apply here — owner names a MIND, not a
    # commit history). So the fix is declaring the boundary, not compensating for it: a
    # single fleet-wide count of open threads this check structurally cannot evaluate,
    # the same shape close_by_commits.unreachable_no_repo and dispose.orphans() already use.
    unscoped = await pool.fetchval(
        "SELECT count(*) FROM objects o WHERE o.type='Thread' AND o.status='active' "
        "AND (SELECT value #>> '{}' FROM current_assertions WHERE object_id=o.id "
        "  AND name='status' ORDER BY confidence DESC, observed_at DESC LIMIT 1) = 'open' "
        "AND NOT EXISTS (SELECT 1 FROM links l WHERE l.from_id=o.id AND l.type='in_repo')")
    land("rot-candidate-unscoped", "info", [
        {"subject": "fleet", "count": int(unscoped),
         "detail": f"{unscoped} open thread(s) have no in_repo edge at all — the "
                   "rot-candidate check above cannot evaluate them (no repo, no commit "
                   "corpus to compare against); not a defect, a structural blind spot "
                   "this check is now honest about"}] if unscoped else [])

    # EDGELESS-CLOSURE-GROWTH (the ratchet, Thoth DM 2581/2603, decision cb38d922): resolved-
    # with-no-closure-edge (resolved_by/answers/closed_by, valid_until open) must never grow
    # past EDGELESS_CLOSURE_CEILING — every sanctioned closing path now mints an edge
    # unconditionally, so growth can only mean a bypass. Fleet-wide by design (unlike most
    # checks here this ignores `subject`/project scope on purpose — a bypass in one repo is
    # exactly as much a defect as one in another, and the ceiling itself was measured
    # fleet-wide). One finding, never a per-thread list — that's enumerate_threads' job, not
    # lint's; this check answers "did the leak reopen," nothing more granular.
    edgeless = await pool.fetchval(
        "SELECT count(*) FROM objects o WHERE o.type='Thread' AND o.status='active' "
        "AND o.merged_into IS NULL AND COALESCE((SELECT a.value #>> '{}' FROM "
        "current_assertions a WHERE a.object_id=o.id AND a.name='status' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1), 'open') = 'resolved' "
        "AND NOT EXISTS (SELECT 1 FROM links l WHERE l.from_id=o.id "
        "  AND l.type IN ('resolved_by', 'closed_by') AND l.valid_until IS NULL) "
        "AND NOT EXISTS (SELECT 1 FROM links l WHERE l.to_id=o.id "
        "  AND l.type='answers' AND l.valid_until IS NULL)")
    land("edgeless-closure-growth", "error", [
        {"subject": "fleet", "count": int(edgeless), "ceiling": EDGELESS_CLOSURE_CEILING,
         "detail": f"resolved-with-no-closure-edge grew to {edgeless}, past the ceiling of "
                   f"{EDGELESS_CLOSURE_CEILING} — every sanctioned closing path mints an "
                   "edge unconditionally now, so this can only mean a bypass: raw SQL, an "
                   "unguarded new writer, or a closure edge healed while status stayed "
                   "resolved"}
    ] if edgeless > EDGELESS_CLOSURE_CEILING else [])

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

    # DUPLICATE-WORKS-IN (thread 8640a625, decision fce39baa — John XVII's own specimen):
    # a LIVE agent carrying more than one simultaneously-live works_in edge. orient()
    # resolves through exactly one of them, so the duplicate is not cosmetic — it can hide
    # a lineage's own threads/decisions from itself while it is running (measured live,
    # 2026-08-03: 41 agents fleet-wide carry the shape, but only currently-LIVE agents are
    # operationally dangerous — a dead generation's leftover duplicate resolves nothing for
    # anyone, the same reasoning orphan-link already applies; that larger historical count
    # is thread 20af2c95's own separate, still-open concern, not this check's). Scoped to
    # `live_secs` — the SAME liveness window phantom-twin already uses, not a second
    # definition of "live". Testimony only: this counts, it never judges which edge is the
    # stale one — invalidate_works_in is the repair, a mind names the target.
    dup = await pool.fetch(
        "WITH live_agents AS (SELECT DISTINCT agent_id FROM agent_mounts "
        "  WHERE last_seen > now() - make_interval(secs => $1)) "
        "SELECT o.canonical AS agent, "
        "  array_agg(DISTINCT p.canonical ORDER BY p.canonical) AS projects, "
        "  count(DISTINCT l.to_id) AS n "
        "FROM links l "
        "JOIN objects o ON o.id=l.from_id AND o.type='Agent' AND o.status='active' "
        "JOIN objects p ON p.id=l.to_id AND p.type='SoftwareProject' "
        "JOIN live_agents la ON la.agent_id=o.canonical "
        "WHERE l.type='works_in' AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "GROUP BY o.canonical HAVING count(DISTINCT l.to_id) > 1 "
        "ORDER BY o.canonical", live_secs)
    land("duplicate-works-in", "warn", [
        {"subject": r["agent"],
         "detail": f"{r['agent']} is live right now and carries {r['n']} simultaneously-"
                   f"live works_in edges ({', '.join(r['projects'])}) — orient() resolves "
                   "through exactly one, so a successor mounting here may see the wrong "
                   "project's threads/decisions entirely. Name the stale one and "
                   "invalidate_works_in it; this check only counts, it never guesses "
                   "which"}
        for r in dup])

    # PEER-SILENT — task #76 item 2 (spec e6636c7e): v1's fiduciary-disclosure duty
    # ("surface in-scope findings and risks to your peer proactively — silence is a
    # violation", offices.py's PEER ADDENDUM) is prose only; nothing in this function
    # measured it. A true "was every in-scope finding disclosed" check needs a disclosure
    # marker that does not exist yet — this is the honest, mechanical proxy available from
    # EXISTING conventions alone (reuse, not new machinery): has this pair exchanged ANY
    # direct mail at all, recently? An active peer_of pair where no DM has passed between
    # either side's holders in `stale_days` (or ever) is flagged — not proof a finding was
    # withheld, but the coarse tripwire the spec's own "silence is a violation" language
    # calls for. Matches EVERY agent that has EVER held either seat (the same `holds` edge
    # `held_seat`/`resolve_seat` read elsewhere), not just the current generation, so a
    # mid-reign swap on either side never produces a false silence. Counts a DM addressed
    # directly agent-to-agent OR to either seat's own address (`to_agent='seat:...'`);
    # deliberately does NOT count project broadcasts — a peer bond is a private duty, and
    # crediting a broadcast neither peer need have read would hide real silence.
    peer_silence = await pool.fetch(
        "WITH active_peers AS ("
        "  SELECT oa.canonical AS seat_a, ob.canonical AS seat_b, oa.id AS seat_a_id, "
        "    ob.id AS seat_b_id, l.properties->>'because' AS because, "
        "    l.first_seen AS peered_since "
        "  FROM links l JOIN objects oa ON oa.id=l.from_id JOIN objects ob ON ob.id=l.to_id "
        "  WHERE l.type='peer_of' AND (l.valid_until IS NULL OR l.valid_until > now())), "
        "holders_a AS (SELECT ap.seat_a AS seat, f.canonical AS agent FROM active_peers ap "
        "  JOIN links hl ON hl.to_id=ap.seat_a_id AND hl.type='holds' "
        "  JOIN objects f ON f.id=hl.from_id), "
        "holders_b AS (SELECT ap.seat_b AS seat, f.canonical AS agent FROM active_peers ap "
        "  JOIN links hl ON hl.to_id=ap.seat_b_id AND hl.type='holds' "
        "  JOIN objects f ON f.id=hl.from_id) "
        "SELECT ap.seat_a, ap.seat_b, ap.because, ap.peered_since, "
        "  MAX(m.created_at) AS last_contact "
        "FROM active_peers ap "
        "LEFT JOIN holders_a ha ON ha.seat=ap.seat_a "
        "LEFT JOIN holders_b hb ON hb.seat=ap.seat_b "
        "LEFT JOIN fleet_messages m "
        "  ON (m.from_agent=ha.agent AND (m.to_agent=hb.agent OR m.to_agent=ap.seat_b)) "
        "  OR (m.from_agent=hb.agent AND (m.to_agent=ha.agent OR m.to_agent=ap.seat_a)) "
        "GROUP BY ap.seat_a, ap.seat_b, ap.because, ap.peered_since "
        "HAVING MAX(m.created_at) IS NULL "
        "  OR MAX(m.created_at) < now() - make_interval(days => $1) "
        "ORDER BY ap.peered_since", stale_days)
    land("peer-silent", "warn", [
        {"subject": f"{r['seat_a']} <-> {r['seat_b']}",
         "detail": f"peered since {r['peered_since'].isoformat()} ({r['because']!r}) — "
                   + (f"last direct mail between them was {r['last_contact'].isoformat()}"
                      if r["last_contact"] is not None
                      else "no direct mail between them has ever been seen")
                   + f", past the {stale_days}-day disclosure window. A proxy for the "
                     "fiduciary-disclosure duty (spec e6636c7e), not proof either peer "
                     "withheld a finding — testimony for a mind to judge, same as every "
                     "other check here"}
        for r in peer_silence])

    # HELD-PAST-DEADLINE — task #76 item 4b (spec e6636c7e, decision e85d3040): the mutual
    # HOLD's auto-escalation-to-the-operator half, built as a LINT check rather than a new
    # daemon (Thoth's ruling, matching the operator's own #169 precedent — match the
    # instrument to the base rate; a periodic read over durable state is exactly graph_lint's
    # own shape, and this reuses the SAME function item 2 just extended, no new machinery).
    # A hold_action() thread (severity='hold') still open past its own hold_deadline is
    # flagged — this is testimony ONLY, same as every other check: the actual push-to-the-
    # operator act stays a mind's (or a later caller's) own move, never lint's.
    held_past_deadline = await pool.fetch(
        "SELECT o.id, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='hold_holder' ORDER BY a.confidence DESC, a.observed_at DESC "
        "   LIMIT 1) AS holder, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='hold_held' ORDER BY a.confidence DESC, a.observed_at DESC "
        "   LIMIT 1) AS held, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='hold_act' ORDER BY a.confidence DESC, a.observed_at DESC "
        "   LIMIT 1) AS act, "
        " (SELECT (a.value #>> '{}')::timestamptz FROM current_assertions a "
        "   WHERE a.object_id=o.id AND a.name='hold_deadline' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS deadline "
        "FROM objects o "
        "WHERE o.type='Thread' AND o.status='active' AND o.merged_into IS NULL "
        "  AND (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "    AND a.name='severity' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "    = 'hold' "
        "  AND (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "    AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "    = 'open' "
        "  AND (SELECT (a.value #>> '{}')::timestamptz FROM current_assertions a "
        "    WHERE a.object_id=o.id AND a.name='hold_deadline' "
        "    ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) < now() "
        "ORDER BY (SELECT (a.value #>> '{}')::timestamptz FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='hold_deadline' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) ASC")
    land("held-past-deadline", "warn", [
        {"subject": f"{r['holder']} holding {r['held']}'s act ({r['act']})",
         "detail": f"time-boxed hold expired {r['deadline'].isoformat()} with no "
                   "resolve_thread call yet — the spec's auto-escalation-to-the-operator "
                   "half, unbuilt as a push, surfaces here instead: this is where a mind "
                   "(or a future caller reading this check) takes it to the operator's "
                   "desk, not lint's own act"}
        for r in held_past_deadline])

    findings.sort(key=lambda f: (_SEVERITY_RANK.get(str(f["severity"]), 9), str(f["check"])))
    if check_filter is not None:
        # a per-check ask paginates ONE check's full set — "capped" now names how much of
        # THAT check's own total is still beyond this page, never the other checks (they
        # were never listed at all, so their own counts need no remaining-count noise)
        total = counts.get(check_filter, 0)
        remaining = total - page_offset - len(findings)
        capped = {check_filter: remaining} if remaining > 0 else {}
        note = (f"showing {len(findings)} of {total} for check={check_filter!r} "
                f"(offset={page_offset}) — {remaining} more; raise limit/offset for the "
                "rest") if remaining > 0 else None
    else:
        capped = {c: n - _LINT_CAP for c, n in counts.items() if n > _LINT_CAP}
        note = ("some checks list only their first "
                f"{_LINT_CAP} findings; counts hold the true totals") if capped else None
    # SEVERITY, ADDED (thread 187323d9, decision 6647fcd5, Thoth DM 3143): `counts` alone
    # flattened info-class metered history (orphan-link: thousands, expected under
    # resolve-on-read) into the same undifferentiated list as warn/error-class damage
    # (contradiction, attribution, ...) — a reader trusting `counts` at face value can
    # overstate this graph's real debt by an order of magnitude (measured live: 54x).
    # `severity` (per-check, non-breaking — `counts` itself keeps its original int-valued
    # shape for every existing caller) lets a reader segment the two without memorizing this
    # function's own docstring; `counts_by_severity` is the one-glance rollup that directly
    # answers the question the flattening obscured — "how much of this is really damage."
    counts_by_severity: dict[str, int] = {}
    for c, n in counts.items():
        counts_by_severity[severity_by_check.get(c, "unknown")] = (
            counts_by_severity.get(severity_by_check.get(c, "unknown"), 0) + n)
    return {
        "findings": findings,
        "counts": counts,
        "severity": severity_by_check,
        "counts_by_severity": counts_by_severity,
        "clean": sorted(c for c, n in counts.items() if n == 0),
        **({"capped": capped, "note": note} if capped else {}),
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
    another mind's claims next, 'waiting on the human' last. Ownership matching is LINEAGE-
    AWARE (finding D, Khnum audit thread ffb13bd9): an obligation owned by an earlier or
    later generation of the same agent lineage (agent:foo-iii vs agent:foo-iv, post-
    compaction succession) still ranks as mine — the compositions layer had never inherited
    the `_generation()` treatment held_seat/manager_of_seat already carry.

    WITHIN each (kind, ownership) band, RELEVANCE OBSERVED, NOT DECLARED (ruling a4bd555c,
    #121's catalog-usage law applied to the object that matters most, Thoth msg 2332):
    `last_touched` — the freshest self_declared `observed_at` a caller of `open_thread_wall`
    hands in — breaks the tie, not raw creation order. A thread minted months ago and
    re-annotated yesterday answers "annotated recently vs. abandoned" directly and outranks
    one merely minted yesterday and never touched since; a thread's `last_touched` is never
    absent on a genuine wall row (an untouched thread is an echo, never a wall entry —
    `open_thread_wall`'s own split). `arc` is the last tie-break — grouping, not priority:
    no arc in the closed taxonomy outranks another, so it never overrides an observed
    signal, only orders otherwise-identical ties so same-arc threads sit together on a
    capped page. Input order is the final fallback — Python's sort is stable. Pure."""
    me_roots = frozenset(_generation(m)[0] for m in me)
    never = datetime.min.replace(tzinfo=UTC)
    latest = datetime.max.replace(tzinfo=UTC)

    def whose_move(r: dict[str, Any]) -> int:
        owner = (r.get("owner") or "").strip()
        if not owner or owner in me or _generation(owner)[0] in me_roots:
            return 0  # mine to act (unowned = anyone who reads it may act)
        return 2 if owner == "operator" else 1

    def touched_at(r: dict[str, Any]) -> datetime:
        v = r.get("last_touched")
        return v if isinstance(v, datetime) else never

    summ = [r for r in rows if r.get("summary")]
    ranked = sorted(summ, key=lambda r: (
        r.get("kind") != "obligation", whose_move(r),
        latest - touched_at(r),        # ascending on this = most-recently-touched first
        r.get("arc") or ""))
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

# THE CORRECTED SUMMARY WINS BY DEFAULT (roadmap ledger-rot stage 3.5, decision c0bc6d33 +
# Thoth LXXIV's DM 4364: "a reader must get the corrected text by default... orient()/roadmap
# must surface the corrected summary, not the original with a footnote"). `corrected_summary`
# (correct_thread_summary, capture.py) is never touched by open_thread's own dedup key, so a
# reader here needs the WINNING one of the two, not the original alone — same COALESCE shape
# everywhere a wall/roadmap query selects `summary` for display. `summary` itself stays
# reachable too (recall(ref) already surfaces both, unchanged since stage 3).
_SUMMARY_DISPLAY_SQL = (
    "COALESCE("
    "(SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
    " AND a.name='corrected_summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1), "
    "(SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
    " AND a.name='summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1))"
)

async def open_thread_wall(
    pool: asyncpg.Pool, proj: uuid.UUID,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """One project's open threads, SPLIT: (wall, echoes). An ECHO is a thread no mind has
    ever touched — not one self_declared assertion in its whole history — that is either
    kind='question' or older than the freshness window. Its status stays OPEN in the record
    (untouched ≠ resolved, ruling 758ded94); only the LENS stops hauling it. Rows carry the
    8-char short id so triage verbs can name their target directly.

    `arc` and `last_touched` ride along for `rank_open_threads` (ruling a4bd555c, same law
    as #121's catalog ranking — RELEVANCE OBSERVED, NOT DECLARED): `last_touched` is the
    freshest self_declared `observed_at` on the object, the same authoritative clock
    `current_assertions` itself resolves "current" by — a thread re-annotated last week
    outranks one merely minted yesterday and never touched again. `untouched` already
    proves this is never null for a WALL row (an untouched thread is an echo, not a wall
    entry)."""
    rows = await pool.fetch(
        "SELECT o.id, o.created_at, "
        f" {_SUMMARY_DISPLAY_SQL} AS summary, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='kind' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS kind, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='owner' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS owner, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='arc' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS arc, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='is_handoff' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS is_handoff, "
        " (SELECT max(sa.observed_at) FROM assertions sa WHERE sa.object_id=o.id "
        "   AND sa.evidence_class='self_declared') AS last_touched, "
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
            f" {_SUMMARY_DISPLAY_SQL} AS summary, "
            " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
            "   AND a.name='kind' "
            "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS kind, "
            " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
            "   AND a.name='owner' "
            "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS owner, "
            " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
            "   AND a.name='arc' "
            "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS arc, "
            " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
            "   AND a.name='is_handoff' "
            "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS is_handoff, "
            " (SELECT max(sa.observed_at) FROM assertions sa WHERE sa.object_id=o.id "
            "   AND sa.evidence_class='self_declared') AS last_touched, "
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
        item: dict[str, Any] = {"id": str(r["id"])[:8], "summary": r["summary"]}
        if r["kind"]:
            item["kind"] = r["kind"]
        if r["owner"]:  # whose move it is — absent means anyone's
            item["owner"] = r["owner"]
        if r["arc"]:
            item["arc"] = r["arc"]
        if r["is_handoff"]:  # Thoth DM 3090: orient()'s own _cap_text reads this to exempt
            item["is_handoff"] = r["is_handoff"]  # a handoff record from the 160-char cap
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
        if is_echo:
            echoes.append({**item, "born": r["created_at"].date().isoformat()})
        else:
            # untouched -> echo above means a WALL row always has a real touch
            wall.append({**item, "last_touched": r["last_touched"]})
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
        "SELECT str_id AS id, summary, kind, owner, arc, last_touched, project FROM ("
        " SELECT substr(o.id::text, 1, 8) AS str_id, o.created_at, "
        f" {_SUMMARY_DISPLAY_SQL} AS summary, "
        "  (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "    AND a.name='kind' "
        "    ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS kind, "
        "  (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "    AND a.name='owner' "
        "    ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS owner, "
        "  (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "    AND a.name='arc' "
        "    ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS arc, "
        "  (SELECT max(sa2.observed_at) FROM assertions sa2 WHERE sa2.object_id=o.id "
        "    AND sa2.evidence_class='self_declared') AS last_touched, "
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
    # UNFILED (Thoth DM 2704, finding 2 of the in_repo audit): `projects` above INNER JOINs
    # in_repo — structurally, a per-project GROUP BY has nowhere to file a thread with no
    # project at all. `totals.open` already counts it correctly (the comment at the top of
    # this block always has); what was missing was saying so IN THE RECEIPT. This is the
    # root cause of a number quoted at the operator more than once without anyone knowing
    # why: summing `projects[].open` will always undercount `totals.open` by exactly this.
    unfiled = await pool.fetchval(
        "SELECT count(*) FROM objects o "
        "WHERE o.type='Thread' AND o.status='active' AND o.merged_into IS NULL "
        "  AND COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
        "   WHERE a.object_id=o.id AND a.name='status' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1),'open')='open' "
        "  AND NOT EXISTS (SELECT 1 FROM links l WHERE l.from_id=o.id AND l.type='in_repo')")
    totals = {
        "open": trow["open"],
        "wall": trow["open"] - trow["pile"],   # a mind touched it — open = wall + pile, exactly
        "pile": trow["pile"],
        "obligations": trow["obligations"],    # DECLARED duties, a subset of `wall`
        "guessed_obligations": trow["guessed_obligations"],  # the miner's, sitting in the pile
        "halted": halted,   # real yield on programs the operator killed BY NAME — not debt
        "unfiled": unfiled,  # counted in `open`, absent from every `projects[]` row — no
                              # in_repo edge to file it under; not garbage, just homeless
        "reads": "open = wall + pile, over LIVE projects only. `obligations` are DECLARED duties "
                 "and are a subset of `wall` — never add them to anything. `halted` is work on "
                 "programs the operator stopped: not garbage (never swept), not debt (never "
                 "counted); resume the project and it returns. `unfiled` is threads with no "
                 "in_repo edge at all — counted in `open`, but structurally absent from every "
                 "`projects[]` row, so summing that list will always undercount `open` by "
                 "exactly this many.",
    }
    return {"totals": totals, "projects": projects,
            "top_of_wall": shown, "more_on_wall": more,
            "note": "the fleet wall — the graded top (declared duties + threads a mind touched), "
                    "never the raw scroll. `pile` is untouched miner echoes (no mind has read "
                    "them); `guessed_obligations` are duties the MINER inferred and nobody "
                    "confirmed — they are NOT debt. Focus a project to see its own graded wall."}


async def _fn_roadmap_open(
    pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]
) -> Any:
    """The OPEN half of the migrated roadmap (ruling c5b184cd, thread d56e7073/#44) — the one
    piece that stays a Function rather than a pure op-tree, and deliberately so: echo-
    filtering (`open_thread_wall`'s exclusion of untouched miner guesses) inspects EVIDENCE
    PROVENANCE across a thread's whole assertion history, which `select`'s property-only
    `where` cannot express — real domain logic, exactly what a Function is for. Unlike a
    Function's output BEFORE task #60 (the function-output-re-entering-the-op-tree
    follow-on), this is no longer a dead-end leaf: returning a flat, arc/owner-tagged list
    lets the composition's own `group by=arc` then `group by=owner` do the nesting via the
    op-tree — the SAME shape `_roadmap_status_group` already uses for resolved/retracted —
    instead of hand-rolling it here in Python. This is the discipline the migration is
    proving: real judgment (the echo-filter) stays a Function, pure layout stays ops.

    `rank_open_threads`'s own cap (ORIENT_OPEN_THREADS) was silently dropping the tail — a
    "no silent caps" violation Thoth caught live: the open section showed a subset of a
    project's real open threads with no line saying so. `_fn_wall` (this function's own
    sibling) already reports its `more` honestly, but as a DICT sidecar key — not available
    here, since this Function must stay list-shaped for `group` to consume it (task #60).
    So the honesty rides IN the list instead: one synthetic trailing row, arc/owner-tagged
    distinctly (never a real ARCS value or a real owner) so it forms its own visible,
    clearly-labeled bucket rather than hiding inside real data — "a count line in the
    section," the generic renderer needing zero special-case code to show it."""
    from src.orchestrator.roadmap import _arc_map

    assert subject is not None  # the op guard requires a subject (the project)
    wall, _echoes = await open_thread_wall(pool, subject)
    ranked, more = rank_open_threads(wall)
    arcs = await _arc_map(pool, [str(t["id"])[:8] for t in ranked])
    items = [{"id": str(t["id"])[:8], "summary": t["summary"], "kind": t.get("kind"),
             "arc": arcs.get(str(t["id"])[:8]) or "unsorted",
             "owner": t.get("owner") or "unowned"} for t in ranked]
    if more:
        items.append({
            "id": None, "kind": None,
            "summary": f"{more} more open thread{'s' if more != 1 else ''} not shown here "
                       f"(ranked lower) — narrow the query or ask for a wider view",
            "arc": "(more)", "owner": "(more)",
        })
    return items


async def _fn_desk_decisions(
    pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]
) -> Any:
    """The 'decisions-awaiting-a-call' leg of live-desk (ruling c5b184cd, thread d56e7073/#44)
    — a Function, not a pure op: `fleet_messages` (the operator's desk) isn't part of the
    object graph `select`/`traverse` operate over at all, so this can't be expressed as ops
    regardless of how the property-matching vocabulary grows. Wraps the SAME thread-lead,
    undismissed predicate every other desk count already uses (`mailbox._DESK_BRIEF_ROW`),
    narrowed to `desk_kind='decision'` — "a call only they can make," per `send()`'s own
    docstring for that triage value."""
    from src.orchestrator.mailbox import _DESK_BRIEF_ROW, OPERATOR_ADDR

    q = ("SELECT m.id, m.from_agent, m.from_project, m.body, m.created_at FROM fleet_messages m "
         "WHERE " + _DESK_BRIEF_ROW + " AND m.desk_kind='decision' "
         "ORDER BY m.created_at DESC").replace("$op", "$1")
    rows = await pool.fetch(q, OPERATOR_ADDR)
    # THE WRITE LEG (ruling c5b184cd, DM 1374): a Function's output is Python-native, so its
    # own `_action` is attached directly here — no `row_action` templating applies to a
    # Function (that's `table`'s own mechanism, over object properties this isn't).
    return [{"id": str(r["id"]), "from": r["from_agent"], "project": r["from_project"],
             "summary": r["body"][:200], "when": str(r["created_at"]),
             "_action": {"action": "settle", "args": {"ids": [r["id"]]}}} for r in rows]


async def _fn_practices(
    pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]
) -> Any:
    """THE THAW's practices composition (ruling 1e6d7367) — surface-scoped, ON-DEMAND
    only (the ruling's own words: 'orient stays lean — surfacing on write-collision + on-
    demand composition only'). Never wired into orient's ambient payload. `surface` narrows
    to one domain vocabulary (BlindSpot's own scoping, e.g. 'deploy', 'succession'); omitted,
    every active Practice. `confirmed` is the live `witnesses` link count — DERIVED, never a
    stored scalar (the same race class thread dc9d1eed found live in bridged_seat/
    record_bridge_anchor would apply to an incremented counter; a link COUNT cannot desync
    from the links it counts). A refuted Practice still lists — flagged, never hidden.
    `amendments` (Thoth DM 3071, capture.amend_practice) — every narrowing `amend_practice`
    has added, oldest first, folded in HERE: this is the ONE live surface every caller
    already uses to read a practice's current guidance, so an amendment that isn't visible
    here isn't visible anywhere a reader would think to look. A Decision's own addenda
    followed the identical reasoning onto its own equivalent surface, `recall()`, rather
    than here (thread 1f4dcc03, fixed) — a Decision has no standing "current guidance"
    listing the way Practices do; recall(kind='decision') is the read that already exists
    for "give me the whole record." """
    surface = str(args.get("surface") or "").strip() or None
    limit = max(1, min(int(args.get("limit") or 50), 200))
    rows = await pool.fetch(
        "WITH p AS ("
        "  SELECT o.id, "
        "   (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "    AND a.name='statement' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "     AS statement, "
        "   (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "    AND a.name='failure_prevented' ORDER BY a.confidence DESC, a.observed_at DESC "
        "    LIMIT 1) AS failure_prevented, "
        "   (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "    AND a.name='surface' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "     AS surface, "
        "   (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "    AND a.name='refuted_by' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "     AS refuted_by, "
        "   (SELECT count(*) FROM links l WHERE l.from_id=o.id AND l.type='witnesses') "
        "     AS confirmed, "
        "   (SELECT array_agg(a.value #>> '{}' ORDER BY a.observed_at ASC) "
        "    FROM current_assertions a WHERE a.object_id=o.id AND a.name LIKE 'amendment:%') "
        "     AS amendments "
        "  FROM objects o WHERE o.type='Practice' AND o.status='active') "
        "SELECT * FROM p WHERE $1::text IS NULL OR surface = $1 "
        "ORDER BY confirmed DESC, statement ASC LIMIT $2",
        surface, limit)
    return [
        {"id": str(r["id"]), "statement": r["statement"],
         "failure_prevented": r["failure_prevented"], "surface": r["surface"],
         "confirmed": r["confirmed"],
         **({"refuted_by": r["refuted_by"]} if r["refuted_by"] else {}),
         **({"amendments": list(r["amendments"])} if r["amendments"] else {})}
        for r in rows
    ]


async def _fn_fleet_live_agents(
    pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]
) -> Any:
    """THE FLEET STRIP's ranked half (task #71 slice two, gated msg 1894/1897) — a Function,
    not a pure op: liveness/seatedness aren't stored graph properties, they're derived at
    read time from agent_mounts (last_seen freshness, the fold-by-soul, the visitor-gate
    discriminator against phantom bg-pty rows) — exactly the class of read chrome.fleet_data
    already does and this wraps rather than re-derives, same discipline as desk_decisions
    wrapping mailbox's own predicate instead of re-deriving it.

    RANKED, NOT THE WALL: only live, seated co-agents on ONE project (`args.project`,
    default 'osiris' — this graph's own control surface) — never the full roster the plain
    "fleet" composition already renders unranked. A flat list[dict] (task #60's own
    reclassification hands this to the generic table for free — no Composition op, no view
    type, nothing in osiris.js)."""
    from src.api.chrome import fleet_data

    project = str(args.get("project") or "osiris")
    try:
        data = await fleet_data(pool)
    except Exception:  # noqa: BLE001 — a fleet read that fails is UNAVAILABLE, never a
        # silent empty table (msg 1894 point 4, degrade-honestly, renderer-independent)
        return [{"agent": "fleet data unavailable", "project": "-", "model": "-"}]
    live = [m for m in data["mounts"]
            if m.get("live") and m.get("seated") and m.get("project") == project]
    return [{"agent": m.get("seat") or m.get("agent_id") or "?",
             "project": m.get("project") or "?",
             "model": str(m.get("model") or "?").removeprefix("claude-")} for m in live]


async def _fn_fleet_pulse_line(
    pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]
) -> Any:
    """The fleet strip's one-line pulse — the SAME string orient() already shows every
    seat (mounts.fleet_pulse), never re-derived. Deliberately a bare string, not a dict: a
    short scalar Function result renders as a header chip (osiris.js renderData), no
    section of its own needed. NOT named 'pulse' — that name is held by the unrelated
    dev_pulses/off-the-clock-loop heartbeat digest (msg 1897's own naming catch)."""
    from src.orchestrator.mounts import fleet_pulse

    try:
        return await fleet_pulse(pool)
    except Exception:  # noqa: BLE001 — see _fn_fleet_live_agents: unavailable, not silent
        return "fleet pulse unavailable"


def _fleet_age(secs: float | None) -> str:
    """A short, human age string — independent of chrome.py's own private `_age()` (this
    Function's output is consumed primarily by osiris.js, not chrome's dying renderer; no
    need to import a leading-underscore symbol across the module boundary for a two-line
    format)."""
    if secs is None:
        return "?"
    s = float(secs)
    if s < 60:
        return f"{int(s)}s ago"
    if s < 3600:
        return f"{int(s // 60)}m ago"
    if s < 86400:
        return f"{int(s // 3600)}h ago"
    return f"{int(s // 86400)}d ago"


def _fleet_door_line(d: dict[str, Any]) -> str:
    key = d.get("session_key") or ""
    when = _fleet_age(d.get("age_secs"))
    if key.startswith("view-of:"):
        return f"tab→{key.removeprefix('view-of:')} {when}"
    if key.startswith("resume-of:"):
        return f"resume→{key.removeprefix('resume-of:')} {when}"
    sid = (d.get("job_dir") or "").rsplit("/", 1)[-1] or "?"
    return f"session {sid} {when}"


def _fleet_doors_summary(doors: list[dict[str, Any]]) -> str | None:
    """FLATTENED, not omitted (Thoth, msg 1936): 'N doors (label, label...)', capped at 4
    labels so a soul with many doors still reads as one line, not an essay. None (never a
    bare 0 or an empty string) when there is nothing to say — the field itself is absent
    from the row rather than a hollow value, same 'a missing field beats a decoded repr'
    law the docstring below states for the row as a whole."""
    if not doors:
        return None
    labels = [_fleet_door_line(d) for d in doors[:4]]
    more = f", +{len(doors) - 4} more" if len(doors) > 4 else ""
    return f"{len(doors)} door{'s' if len(doors) != 1 else ''} ({', '.join(labels)}{more})"


def _fleet_ancestors_summary(ancestors: list[dict[str, Any]]) -> str | None:
    """Same law: a readable sentence or nothing, never a repr. Only the FRESHEST past life
    is named — the rest are counted, not listed, mirroring render_fleet's own economy
    (head line + a count) rather than dumping every generation into one cell."""
    if not ancestors:
        return None

    def _age_key(a: dict[str, Any]) -> float:
        v = a.get("age_secs")
        return float(v) if v is not None else float("inf")

    freshest = min(ancestors, key=_age_key)
    name = freshest.get("seat") or "?"
    when = _fleet_age(freshest.get("age_secs"))
    if len(ancestors) == 1:
        return f"1 earlier life: {name}, {when}"
    return f"{len(ancestors)} earlier lives, most recent {name} {when}"


async def _fn_fleet_live(
    pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]
) -> Any:
    """/fleet's full-fidelity port (rung 2, ruling d42c543b, Thoth msg 1926/1936) —
    ADDITIVE ONLY: /fleet's route is NOT deleted yet, this lands beside it so the two can be
    compared live before anything retires (msg 1936's own sequencing — "nothing is deleted
    before its replacement exists" now means at PARITY, not merely present). Wraps
    chrome.fleet_data verbatim, same discipline as fleet_live_agents/mail_overview — never
    re-derives the soul-fold, only reshapes its already-full-fidelity output for the
    generic renderer.

    UNLIKE fleet_live_agents (RANKED: one project, live+seated only), this is the FULL
    roster — every project, every soul the mount registry currently knows, live or not —
    because full fidelity was the ask (msg 1926: neither the plain "fleet" table nor the
    strip reproduces soul-folding, the wake ledger, the hourly wake budget, the visitor
    split, or the cross-project view; this Function is where all five actually live).

    THE FLATTENING (msg 1936's own bar — "genuinely readable... exactly as you sketched"):
    neither render_composition nor osiris.js's table() recurse into a nested list/dict CELL
    value — a raw `doors=[{...}]` would render as an undecoded repr, worse than not showing
    it. `doors`/`ancestors` become short prose summaries instead (_fleet_doors_summary/
    _fleet_ancestors_summary) — present when there's something to say, ABSENT (never a
    hollow value) when there isn't. Nothing is silently dropped: every field either renders
    readably or is omitted and named here, not decided quietly at build time."""
    from src.api.chrome import fleet_data
    from src.config.settings import get_settings

    try:
        data = await fleet_data(pool, wake_budget=get_settings().osiris_wake_hourly_budget)
    except Exception:  # noqa: BLE001 — see fleet_live_agents: unavailable, never a silent
        # empty table (msg 1894 point 4, degrade-honestly, renderer-independent)
        return {"pulse": "fleet data unavailable"}
    mounts = data["mounts"]
    named = [m for m in mounts if m.get("seat")]
    anon = [m for m in mounts if not m.get("seat")]
    live_n = sum(1 for m in mounts if m["live"] and m.get("seated", True))
    vis_n = sum(1 for m in mounts if m["live"] and not m.get("seated", True))
    budget = f'/{data["wake_budget"]}' if data.get("wake_budget") else ""
    pulse = (
        f"{live_n} live"
        + (f" · {vis_n} visitor{'s' if vis_n != 1 else ''}" if vis_n else "")
        + f" · {len(named)} soul{'s' if len(named) != 1 else ''}"
        + (f" · {len(anon)} unreconciled" if anon else "")
        + f" · wakes {data['wakes_hour']}{budget}/h"
    )

    def _row(m: dict[str, Any]) -> dict[str, Any]:
        row: dict[str, Any] = {
            "seat": m.get("seat") or m.get("agent_id") or "?",
            "project": m.get("project") or "?",
            "model": str(m.get("model") or "?").removeprefix("claude-"),
            "live": bool(m.get("live")),
            "age": _fleet_age(m.get("age_secs")),
        }
        doors = _fleet_doors_summary(m.get("doors") or [])
        if doors:
            row["doors"] = doors
        ancestors = _fleet_ancestors_summary(m.get("ancestors") or [])
        if ancestors:
            row["ancestors"] = ancestors
        return row

    wake_ledger = [
        {"when": str(w["woke_at"])[:16], "project": w.get("to_project") or "?",
         "mode": w.get("mode") or "?", "by": w.get("from_agent") or "?",
         "message_id": w.get("message_id")}
        for w in data["wakes"]
    ]
    return {
        "pulse": pulse,
        "roster": [_row(m) for m in named],
        "unreconciled": [_row(m) for m in anon],
        "wake_ledger": wake_ledger,
    }


async def _fn_mail_overview(
    pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]
) -> Any:
    """/mail's port (ruling d42c543b, msg 1929) — a Function, not a pure op: soul-folding
    (resolving each '@agent:...'/'@seat:...' lane through living_head, nesting agent
    mailboxes under their project) needs imperative per-row async lookups the op-tree
    has no primitive for. Wraps chrome.mail_overview verbatim — never re-derives the fold.
    A flat list[dict] (one row per project's group chat OR per soul, `chrome.py`'s own
    shape unchanged) — task #60's reclassification hands it to the generic table for
    free, same ride fleet_live_agents already took."""
    from src.api.chrome import mail_overview

    try:
        groups = await mail_overview(pool)
    except Exception:  # noqa: BLE001 — a mail read that fails is UNAVAILABLE, never a
        # silent empty table (the same degrade-honestly law fleet_live_agents follows)
        return [{"box": "mail data unavailable", "msgs": "-", "unsettled": "-"}]
    rows: list[dict[str, Any]] = []
    for g in groups:
        room = g.get("room")
        if room:
            rows.append({"box": room["box"], "msgs": room["msgs"],
                         "unsettled": room["unsettled"]})
        for s in g["souls"]:
            rows.append({"box": s.get("soul") or s["box"], "msgs": s["msgs"],
                         "unsettled": s["unsettled"]})
    return rows


async def _fn_mail_threads(
    pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]
) -> Any:
    """One mailbox's threads (ruling d42c543b, msg 1929) — `box` rides as `args.box`, not a
    subject: a mailbox is a project name or a synthetic '@agent:.../@seat:...' string, never
    a graph object's UUID the composer's own subject resolution expects. Wraps
    chrome.mail_threads verbatim. Flat list[dict] — same free ride to the generic table.

    KNOWN GAP, flagged not hidden: `render_mail_box` shows every message's full body inline
    (a per-thread <details> expansion); this row carries only the latest message's snippet
    (a table cell that held every body would just be a stringified JSON blob, not a rendered
    list — the generic table has no nested-table primitive). Per-message detail is real
    information this port does not preserve 1:1 — see the build brief, not silently dropped."""
    from src.api.chrome import mail_threads

    box = str(args.get("box") or "").strip()
    if not box:
        return [{"thread": "no box given — pass args.box (a project name or "
                           "'@agent:...'/'@seat:...')", "between": "-", "msgs": "-"}]
    try:
        threads = await mail_threads(pool, box)
    except Exception:  # noqa: BLE001 — see _fn_mail_overview: unavailable, not silent
        return [{"thread": "mail data unavailable", "between": "-", "msgs": "-"}]
    return [{"thread": t["thread"], "between": ", ".join(t["between"]),
             "msgs": len(t["msgs"]), "unsettled": t["unsettled"],
             "latest": t["msgs"][-1]["body"][:160] if t["msgs"] else ""} for t in threads]


async def _fn_overhead(
    pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]
) -> Any:
    """/overhead's port (task #91) — a Function, not a pure op: TWO independent data sources
    (TranscriptStore's harness-cost accounting, TelemetryStore's retained-events forensics)
    that chrome.py's route already reads separately and lays out on one page; this Function
    makes that same composition once, in Python — a `sections` op-tree calling two Functions
    would hit TranscriptStore.overhead_fleet twice for what is one query. Wraps
    overhead_fleet/summary verbatim, never re-derives either accounting.

    ONE dict, three keys — `totals` (a scalar-valued dict, renders as a header-chip group),
    `top_sessions` (flat list[dict], task #60's reclassification hands it to the generic
    table), `telemetry` (a scalar-valued dict, same free ride as totals — present as a
    one-line note instead of a missing key when TelemetryStore hasn't eaten a file yet: its
    own docstring says absence must never read as a zero-row pretence).

    KNOWN GAP, flagged not hidden: chrome.py's `_fmt_tok`/`_size_cell` compress numbers
    ("1.2M", bytes-vs-tokens chosen per row) for the eye; the generic renderer has no
    per-column formatter, so top_sessions carries the raw ints instead. Content, not a
    decoded blob — a presentation nuance, the same call roadmap's auto-default and fleet's
    doors/ancestors flattening already made, not data this port owes."""
    from src.ingest.telemetry import TelemetryStore
    from src.ingest.transcript_store import TranscriptStore

    try:
        data = await TranscriptStore(pool).overhead_fleet(top=20)
    except Exception:  # noqa: BLE001 — a read that fails is UNAVAILABLE, never a silent
        # empty section (the same degrade-honestly law fleet_live_agents follows)
        return {"totals": "overhead data unavailable"}
    top_sessions = [
        {"session": s["anchor_sid"], "project": s.get("project") or "?",
         "total_tokens": s["total_tokens"], "bytes": s["bytes"],
         "hidden_pct": s["hidden_pct"], "multiplier": s["multiplier"],
         "cache_read_pct": s["cache_read_pct"], "channel_files": s["channel_files"],
         "reminders": s["reminders"], "compactions": s["compactions"]}
        for s in data["top"]
    ]
    out: dict[str, Any] = {"totals": data["totals"], "top_sessions": top_sessions}
    try:
        telemetry = await TelemetryStore(pool).summary()
    except Exception:  # noqa: BLE001 — see above: unavailable, not silent
        out["telemetry"] = "retained-telemetry data unavailable"
    else:
        out["telemetry"] = (
            telemetry if telemetry is not None
            else "nothing retained yet — the store hasn't eaten a telemetry file")
    return out


async def _fn_desk_overview(
    pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]
) -> Any:
    """/desk's landing page (task #91) — the ROSTER, never the contents (operator, 2026-07-11:
    the whole point of this shape is landing on COUNTS so the fleet's backlog doesn't show on
    one scroll — see read_desk's own docstring). Wraps read_desk verbatim, never re-derives
    which project owes what. One row per project — counts and rot-age only; drill into one
    with desk_project(args.project), the same two-step shape mail_overview/mail_threads
    already took.

    THE WRITE SIDE IS ON desk_project, NOT HERE — this Function stays read-only, and not
    because the client can't render it (it can, since 88ad297/task #91's client leg): the
    ROSTER is COUNTS, never individual debts (see the docstring above), so there is no per-
    debt row here to attach a control to. `by_project` carries counts only, exactly what
    render_desk's own roster table shows before its own click-to-walk-in."""
    from src.orchestrator.mailbox import read_desk

    try:
        desk = await read_desk(pool)
    except Exception:  # noqa: BLE001 — a desk read that fails is UNAVAILABLE, never a
        # silent empty roster (the same degrade-honestly law fleet_live_agents follows)
        return {"owed": "desk data unavailable"}
    projects = [
        {"project": p["project"], "debts": len(p.get("debts") or []),
         "asks": len(p.get("asks") or []), "critical": bool(p.get("critical")),
         "oldest": _fleet_age(p.get("oldest_secs"))}
        for p in (desk.get("by_project") or [])
    ]
    out: dict[str, Any] = {"owed": desk["owed"], "letters": desk["letters"],
                            "by_project": projects}
    guesses = (desk.get("miner_guesses") or {}).get("threads") or []
    if guesses:
        out["miner_guesses"] = [
            {"id": t["id"], "project": t["project"], "summary": t["summary"]}
            for t in guesses]
    dimmed = desk.get("dimmed") or []
    if dimmed:
        out["dimmed"] = [
            {"project": d.get("project") or "?", "headline": d.get("headline") or "",
             "moot": d.get("moot") or ""}
            for d in dimmed]
    return out


async def _fn_desk_project(
    pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]
) -> Any:
    """One project's desk, walked into (task #91) — `project` rides as `args.project`, not a
    subject: a project name here is the same free-text string mail_threads's `args.box`
    already established, never a graph object's UUID the composer's own subject resolution
    expects. Wraps read_desk verbatim; the debts + asks render_desk_project shows before its
    own action buttons, flattened to one row each.

    THE WRITE SIDE IS HERE NOW (task #91, Thoth msg 1976/2029) — measured fresh against
    chrome.py's own `_verbs`/`_settle`, not assumed from an earlier description (this
    Function's own prior docstring said "four verbs" as if every row carried all four; reading
    the source showed a DEBT row carries three — done/not mine/later — and an ASK row carries
    a different one, settle. Two shapes, not four options on one row).

    `_actions`/`_action` are embedded DIRECTLY on each row by this Function's own Python,
    NOT declared via the `row_actions`/`row_action` NODE-level grammar `_eval`'s `function` op
    reads: that grammar is for a SAVED composition to attach a control without touching a
    Function's code (mail_overview's own row_action is exactly that). desk_project has no
    saved composition to declare it on (same args.project constraint mail_threads has), and
    its ONE list mixes two row kinds needing DIFFERENT actions — something a single node-level
    declaration, applied uniformly to every row, cannot express. A Function is "the escape
    hatch" for exactly this: domain logic even the declarative layer can't reach.

    Every write verb resolves through the SAME /act registry (actions.ACTION_VERBS) chrome.py
    itself routes through — resolve_thread/assign_thread/defer_thread (chrome's /threads/
    triage, same capture.py calls, same source="analyst:operator") and settle (chrome's
    /desk/settle, same ack_messages call). Same effect, same attribution, a different route to
    get there — verified by reading both call chains, not assumed identical from the names."""
    from src.orchestrator.mailbox import read_desk

    project = str(args.get("project") or "").strip()
    if not project:
        return [{"debt": "no project given — pass args.project", "kind": "-"}]
    try:
        desk = await read_desk(pool)
    except Exception:  # noqa: BLE001 — see desk_overview: unavailable, not silent
        return [{"debt": "desk data unavailable", "kind": "-"}]
    p = next((x for x in (desk.get("by_project") or []) if x["project"] == project), None)
    if p is None:
        return [{"debt": f"nothing owed to {project} — cleared, or never was", "kind": "-"}]
    because_not_mine = f"operator: not mine — {project} owns this"
    rows = [
        {"debt": t["summary"], "kind": t.get("kind") or "-", "id": t["id"],
         "_actions": [
             {"label": "done", "action": "resolve_thread",
              "args": {"ref": t["id"], "because": "operator: done"}},
             {"label": "not mine", "action": "assign_thread",
              "args": {"ref": t["id"], "owner": project, "because": because_not_mine}},
             {"label": "later", "action": "defer_thread",
              "args": {"ref": t["id"], "days": 30, "because": "operator: not now"}},
         ]}
        for t in (p.get("debts") or [])
    ]
    rows += [
        {"debt": (m.get("body") or "")[:160], "kind": "ask",
         "from": m.get("from_project") or "?", "id": m["id"],
         "_action": {"action": "settle", "args": {"ids": [m["id"]]}}}
        for m in (p.get("asks") or [])
    ]
    return rows


# TRIAGE AS A PRIMITIVE (task #98, operator ruling 45b074bf "THE USER NEVER DEBUGS THE
# MACHINERY" + the read-ergonomics arc ad19a779/thread 5e1a46ea/task #65): Thoth ran eight
# hand-written SQL scripts through a shell this session to judge the object set — none of it
# reusable. `objects`/`links` carry no `updated_at` column (confirmed against the live
# schema), so "last touched" is always DERIVED, never a stored fact — the same three-source
# GREATEST every mode below shares.
_TRIAGE_LINK_CTE = """
WITH live_links AS (
    SELECT from_id AS obj_id, GREATEST(first_seen, last_seen, created_at) AS touch
    FROM links WHERE valid_until IS NULL OR valid_until > now()
    UNION ALL
    SELECT to_id AS obj_id, GREATEST(first_seen, last_seen, created_at)
    FROM links WHERE valid_until IS NULL OR valid_until > now()
),
link_stats AS (
    SELECT obj_id, count(*) AS link_count, max(touch) AS last_link_touch
    FROM live_links GROUP BY obj_id
),
assertion_stats AS (
    SELECT object_id, max(observed_at) AS last_assertion
    FROM current_assertions GROUP BY object_id
)
"""

_TRIAGE_BUCKET_PRIORITY = {
    "duplicate_suspect": 0, "bulk_import": 1, "orphan": 2, "hub": 3, "stale": 4, "thin": 5,
    "normal": 6,
    # the catalog's own gap surface (object_type='Type' only, see _triage_type_gaps) —
    # ranked ahead of "normal" so a described-and-labeled Type never crowds out a real gap
    "undescribed": -2, "no_label_rule": -1,
    # task #102 (operator's principle via Thoth's dispatch DM 2279): a LIVE epistemic
    # conflict on a specific property outranks every generic connectivity bucket, including
    # duplicate_suspect — a naming collision is a structural SUSPICION, a contradicted
    # property is a confirmed disagreement already sitting in the data.
    "contradicted": -3,
}


async def _fn_triage(pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]) -> Any:
    """rung 2 — TRIAGE AS A PRIMITIVE (task #98). TWO MODES, one Function (`args.mode`,
    default "census") — this pair IS the left-hand type browser the operator sketched:
    census is the left pane (types + counts + health), buckets is the middle pane (one
    type's objects, triage-labeled). Build once, surface twice; the LABEL half (how a row
    DISPLAYS beyond its raw canonical) is still being designed with the operator and
    deliberately NOT touched here — no labeling scheme is invented in this Function; it
    reads `canonical` raw and leaves a clean seam for whatever the label rule becomes.

    CENSUS — one row per (type, status): `n` (count), `orphans` (zero live links), `thin`
    (1-2 live links), `median_links`/`max_links` (live link count distribution), `born`
    (earliest `created_at` in the group), `last_touch` (latest of: any member's
    `created_at`, any member's most recent assertion, any live link touching a member).

    BUCKETS — requires `args.object_type` (a note naming every real type is returned when
    it's missing or unknown, never a silent empty page); optional `args.status` (default
    "active"), `args.stale_days` (default 30, clamped 1-365), `args.cohort_min` (default 3,
    clamped 2-50 — see `bulk_import` below), `args.limit`/`args.offset` (default 200/0,
    capped 2000 — the no-silent-caps law: CENSUS already carries the true `n` per type,
    this is the browse/page surface, not the count of record). One row per object, ONE
    bucket each by priority (an object can meet more than one definition; the most
    actionable wins): `contradicted` (task #102, operator's principle via Thoth's dispatch
    DM 2279 — this object has a property with more than one DISTINCT live value from
    different sources, neither superseding the other; `current_assertions` already holds
    disagreement by design, this bucket is the first thing that NAMES it, ranked above
    every other bucket here since a confirmed epistemic conflict outranks a structural
    suspicion — the row also carries `contradicted_on`, the sorted list of property names
    in conflict) > `duplicate_suspect` (another object of the SAME type+status shares
    its basename — the canonical's last path segment, or the text after its first ':',
    case-folded; catches File-path collisions and scheme-prefix near-dupes alike) >
    `bulk_import` (task #98 follow-up, Thoth msg 2065 — `args.cohort_min` or more objects
    born in the same calendar second, sharing an IDENTICAL live-link fingerprint — same
    types AND same counts per type, not just the same total; the machine-detectable
    signature of one script's insert loop, distinct from every other bucket here since it
    names WHY an object looks the way it does rather than just how it's currently
    connected) > `orphan` (zero live links) > `hub` (live link count at or above the
    type's OWN 95th percentile, floor 10 — self-normalizing per type rather than one
    global number, since e.g. SoftwareProject's own median can run past 80 while sparser
    types sit near 1) > `stale` (linked, but untouched longer than `stale_days`) > `thin`
    (1-2 live links) > `normal` (none of the above — BUCKETS lists every object in scope,
    not only flagged ones, so it doubles as a plain browse of the type).

    `contradicted` MARKS, it never RESOLVES (the operator's rule): no value is dropped or
    ranked into a winner here, only named as contested — same discipline `entity_dossier`'s
    new `agreement` field follows for a single entity's own property list (dossier.py,
    same task). This bucket is the fleet/project-wide discovery half: which OBJECTS have
    at least one contradicted property, without already knowing which one to look at.

    THE CATALOG'S OWN GAP SURFACE (task #97 workstream 2's other half — "your triage
    Function IS the surface these gaps should appear on, do not build a second
    instrument"): `args.object_type='Type'` routes to a DIFFERENT bucket set, since the
    generic connectivity buckets above are meaningless for Type rows (a Type doesn't
    participate in `links` the way an ordinary object does — every one of them would
    trivially bucket `orphan` and tell a reader nothing). Two gaps instead: `undescribed`
    (blank/missing `description` — a stub nobody has explained yet, exactly what
    accretion mints) > `no_label_rule` (kind='object' only — a link type has no field of
    its own to label; blank/missing `label_field`) > `normal` (both present). Same
    pagination/status/no-silent-caps contract as the generic path.

    Read-only, no writes, same rule graph_lint runs on (a triage that healed would be a
    loop pathology — findings are testimony for a mind's own triage verbs, not an
    auto-apply)."""
    mode = str(args.get("mode") or "census").strip().lower()
    if mode == "buckets":
        return await _triage_buckets(pool, args)
    if mode != "census":
        return [{"note": f"unknown mode {mode!r} — use 'census' or 'buckets'"}]
    rows = await pool.fetch(_TRIAGE_LINK_CTE + """
        SELECT o.type, o.status, count(*) AS n,
               count(*) FILTER (WHERE COALESCE(ls.link_count,0) = 0) AS orphans,
               count(*) FILTER (WHERE COALESCE(ls.link_count,0) BETWEEN 1 AND 2) AS thin,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY COALESCE(ls.link_count,0))
                   AS median_links,
               max(COALESCE(ls.link_count,0)) AS max_links,
               min(o.created_at) AS born,
               max(GREATEST(o.created_at, ast.last_assertion, ls.last_link_touch))
                   AS last_touch
        FROM objects o
        LEFT JOIN link_stats ls ON ls.obj_id = o.id
        LEFT JOIN assertion_stats ast ON ast.object_id = o.id
        GROUP BY o.type, o.status
        ORDER BY o.type, o.status
    """)
    return [
        {"type": r["type"], "status": r["status"], "n": r["n"], "orphans": r["orphans"],
         "thin": r["thin"], "median_links": float(r["median_links"] or 0),
         "max_links": r["max_links"], "born": r["born"].isoformat(),
         "last_touch": r["last_touch"].isoformat()}
        for r in rows
    ]


async def _triage_buckets(pool: asyncpg.Pool, args: dict[str, Any]) -> Any:
    """BUCKETS half of `_fn_triage` — split out so the mode dispatch above stays readable;
    never called directly by a composition/MCP caller (that's `_fn_triage`'s job)."""
    object_type = str(args.get("object_type") or "").strip()
    known = bool(object_type) and await pool.fetchval(
        "SELECT 1 FROM objects WHERE type=$1 LIMIT 1", object_type)
    if not known:
        types = await pool.fetch("SELECT DISTINCT type FROM objects ORDER BY type")
        note = ("buckets mode requires args.object_type" if not object_type
                else f"no objects of type {object_type!r}")
        return [{"note": note, "valid_types": ", ".join(r["type"] for r in types)}]
    status = str(args.get("status") or "active").strip()
    raw_limit = args.get("limit")
    limit = max(1, min(int(raw_limit), 2000)) if raw_limit is not None else 200
    offset = max(0, int(args.get("offset") or 0))
    if object_type == "Type":
        return await _triage_type_gaps(pool, status, limit, offset)
    stale_days = max(1, min(int(args.get("stale_days") or 30), 365))
    cohort_min = max(2, min(int(args.get("cohort_min") or 3), 50))
    rows = await pool.fetch(_TRIAGE_LINK_CTE + """
        , per_object AS (
            SELECT o.id, o.canonical, o.created_at,
                   COALESCE(ls.link_count, 0) AS link_count,
                   GREATEST(o.created_at, ast.last_assertion, ls.last_link_touch)
                       AS last_touch,
                   lower(CASE
                       WHEN o.canonical LIKE '%/%'
                           THEN regexp_replace(o.canonical, '^.*/', '')
                       WHEN o.canonical LIKE '%:%'
                           THEN regexp_replace(o.canonical, '^[^:]*:', '')
                       ELSE o.canonical END) AS basename
            FROM objects o
            LEFT JOIN link_stats ls ON ls.obj_id = o.id
            LEFT JOIN assertion_stats ast ON ast.object_id = o.id
            WHERE o.type = $1 AND o.status = $2
        ),
        -- a percentile is an ORDERED-SET aggregate: postgres refuses it as a window
        -- function (`OVER` unsupported), so the 95th percentile is its own single-row
        -- CTE, cross-joined back in, rather than computed inline per row.
        stats AS (
            SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY link_count) AS p95_links
            FROM per_object
        ),
        basename_counts AS (
            SELECT basename, count(*) AS n FROM per_object GROUP BY basename
        ),
        -- BULK IMPORT (task #98 follow-up, Thoth msg 2065): a per-object FINGERPRINT of its
        -- live link shape — "dir:type:count" pairs, e.g. "in:informs:17" — so two objects
        -- with the SAME shape (not just the same total count) can be told apart from two
        -- objects that coincidentally have equal link counts of different kinds.
        link_shape_counts AS (
            SELECT o.id AS obj_id,
                   (CASE WHEN l.from_id=o.id THEN 'out' ELSE 'in' END || ':' || l.type)
                       AS dir_type,
                   count(*) AS cnt
            FROM objects o
            JOIN links l ON (l.from_id=o.id OR l.to_id=o.id)
                AND (l.valid_until IS NULL OR l.valid_until > now())
            WHERE o.type = $1 AND o.status = $2
            GROUP BY o.id, dir_type
        ),
        link_shapes AS (
            SELECT obj_id, string_agg(dir_type || ':' || cnt, ',' ORDER BY dir_type)
                       AS fingerprint
            FROM link_shape_counts GROUP BY obj_id
        ),
        -- the cohort key: born in the SAME calendar second (a single script's insert loop,
        -- not a coincidence spread across a session) AND an identical link-shape fingerprint.
        -- `cohort_min` objects or more sharing both is the bulk-import signature; a zero-link
        -- object has no fingerprint to cohort on and is never counted here (that's `orphan`'s
        -- job, not this one's).
        cohorts AS (
            SELECT p.id AS obj_id, date_trunc('second', p.created_at) AS born_bucket,
                   ls2.fingerprint
            FROM per_object p JOIN link_shapes ls2 ON ls2.obj_id = p.id
        ),
        cohort_counts AS (
            SELECT born_bucket, fingerprint, count(*) AS n
            FROM cohorts GROUP BY born_bucket, fingerprint
            HAVING count(*) >= $4
        ),
        -- CONTRADICTION (task #102, operator's principle via Thoth's dispatch DM 2279):
        -- current_assertions already coexists two sources' DIFFERING values on the same
        -- property (nobody superseded either) — the storage layer holds it correctly, but
        -- nothing NAMED it. Grouping on (object_id, name) and requiring >1 DISTINCT value
        -- is sufficient to prove genuine multi-source disagreement without checking
        -- source_id explicitly: within-source supersession already collapses a single
        -- source's own repeated assertion to one live row, so two live rows on the same
        -- name can only come from two different sources. Deliberately NOT excluding
        -- name/tag here (unlike entity_dossier's own property listing, which routes those
        -- through resolve_label for display) — a contradicted identity property is exactly
        -- the kind of conflict this bucket exists to surface, not hide.
        contradicted_props AS (
            SELECT ca.object_id, ca.name
            FROM current_assertions ca
            JOIN per_object p2 ON p2.id = ca.object_id
            GROUP BY ca.object_id, ca.name
            HAVING count(DISTINCT (ca.value #>> '{}')) > 1
        ),
        contradicted_objs AS (
            SELECT object_id, array_agg(DISTINCT name ORDER BY name) AS props
            FROM contradicted_props GROUP BY object_id
        )
        SELECT p.id, p.canonical, p.created_at AS born, p.last_touch, p.link_count,
               cont.props AS contradicted_on,
               CASE
                 WHEN cont.props IS NOT NULL THEN 'contradicted'
                 WHEN bc.n > 1 THEN 'duplicate_suspect'
                 WHEN cc.n IS NOT NULL THEN 'bulk_import'
                 WHEN p.link_count = 0 THEN 'orphan'
                 WHEN p.link_count >= GREATEST(10, s.p95_links) THEN 'hub'
                 WHEN p.last_touch < now() - make_interval(days => $3) THEN 'stale'
                 WHEN p.link_count BETWEEN 1 AND 2 THEN 'thin'
                 ELSE 'normal'
               END AS bucket
        FROM per_object p
        JOIN basename_counts bc ON bc.basename = p.basename
        CROSS JOIN stats s
        LEFT JOIN cohorts co ON co.obj_id = p.id
        LEFT JOIN cohort_counts cc
            ON cc.born_bucket = co.born_bucket AND cc.fingerprint = co.fingerprint
        LEFT JOIN contradicted_objs cont ON cont.object_id = p.id
        ORDER BY p.canonical
    """, object_type, status, stale_days, cohort_min)
    bucketed = sorted(rows, key=lambda r: (_TRIAGE_BUCKET_PRIORITY[r["bucket"]], r["canonical"]))
    page = bucketed[offset:offset + limit]
    listed = [
        {"id": str(r["id"]), "canonical": r["canonical"], "bucket": r["bucket"],
         **({"contradicted_on": list(r["contradicted_on"])} if r["contradicted_on"] else {}),
         "links": r["link_count"], "born": r["born"].isoformat(),
         "last_touch": r["last_touch"].isoformat()}
        for r in page
    ]
    return listed or [{"note": f"no {status} objects of type {object_type!r}"}]


async def _triage_type_gaps(
    pool: asyncpg.Pool, status: str, limit: int, offset: int,
) -> Any:
    """THE CATALOG'S OWN GAP SURFACE (task #97 workstream 2) — `_triage_buckets`'
    object_type='Type' branch. Type rows don't participate in `links` the way ordinary
    objects do (a declared type's domain/range/schemes are properties, not graph edges),
    so the generic connectivity buckets (orphan/hub/stale/thin) would trivially label
    every Type 'orphan' and say nothing real — this reads the Type's OWN fields instead:
    `undescribed` (blank/missing `description`, exactly what a bare accretion mints) and
    `no_label_rule` (kind='object' only — a link type has no field of its own to label;
    blank/missing `label_field`), else `normal`. Same pagination/no-silent-caps contract
    as the generic path; never called directly (`_triage_buckets` is the one dispatch
    point, same discipline as that function's own docstring)."""
    rows = await pool.fetch("""
        WITH per_type AS (
            SELECT o.id, o.canonical, o.created_at AS born,
                   (SELECT a.value #>> '{}' FROM current_assertions a
                    WHERE a.object_id = o.id AND a.name = 'kind'
                    ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS kind,
                   (SELECT a.value #>> '{}' FROM current_assertions a
                    WHERE a.object_id = o.id AND a.name = 'description'
                    ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS description,
                   (SELECT a.value #>> '{}' FROM current_assertions a
                    WHERE a.object_id = o.id AND a.name = 'label_field'
                    ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS label_field,
                   GREATEST(o.created_at, (SELECT max(a.observed_at) FROM current_assertions a
                                           WHERE a.object_id = o.id)) AS last_touch
            FROM objects o
            WHERE o.type = 'Type' AND o.status = $1
        )
        SELECT id, canonical, born, last_touch, kind,
               CASE
                 WHEN description IS NULL OR btrim(description) = '' THEN 'undescribed'
                 WHEN kind = 'object' AND (label_field IS NULL OR btrim(label_field) = '')
                     THEN 'no_label_rule'
                 ELSE 'normal'
               END AS bucket
        FROM per_type
        ORDER BY canonical
    """, status)
    bucketed = sorted(rows, key=lambda r: (_TRIAGE_BUCKET_PRIORITY[r["bucket"]], r["canonical"]))
    page = bucketed[offset:offset + limit]
    listed = [
        {"id": str(r["id"]), "canonical": r["canonical"], "bucket": r["bucket"],
         "kind": r["kind"], "born": r["born"].isoformat(),
         "last_touch": r["last_touch"].isoformat()}
        for r in page
    ]
    return listed or [{"note": f"no {status} Type objects"}]


# Khnum's commit 23c5991 (authored 2026-08-01T03:41:38Z) — the moment resolve_thread()
# started minting `closed_by` unconditionally. A resolved_edgeless thread's status observed
# before this is old debt (`pre_fix_sediment`); at or after it, the fallback should have
# fired and didn't — `post_fix_regression`, a live alarm, not history (Thoth DM 2937).
_PHASE_1A_FIX_AT = datetime(2026, 8, 1, 3, 41, 38, tzinfo=UTC)


async def _fn_closure_health(
    pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]
) -> Any:
    """rung 2 — THE FOUR NUMBERS AS A STANDING SURFACE (Thoth DM 2835/2917, decision 6b67210d,
    correction 699fa821). Answers, per project or fleet-wide, ONE question this house could
    previously only get by commissioning an archaeology dig by hand, twice, 16 hours apart:
    is thread closure held together by STRUCTURE (a traversable edge) or by MEMORY (a
    property nobody re-checks)? Composes `thread_closure_status` (thread_closure.py) rather
    than re-deriving its UNION — the exact staleness gap this report tripped over TWICE
    already (once because migration 0044 wasn't deployed yet when the number was first hand-
    run, once because a hand-rolled query drifted from the real view) closes at the source
    by never hand-rolling it a third time.

    FIVE BUCKETS, MUTUALLY EXCLUSIVE AND EXHAUSTIVE BY CONSTRUCTION — every active Thread in
    scope lands in exactly one, checked in this order:
      1. `retracted_or_no_status` — `property_status` is anything other than 'open'/
         'resolved' (including NULL). Kept STRUCTURALLY SEPARATE from `open_both`, never
         folded in: 93% of this graph's task population under this bucket is absence, not
         conflict — a different disease with a different cure (Thoth's own framing, DM 2917).
      2. `disagree` — a closure edge exists AND `property_status='open'`. Returned as a LIST
         OF IDS, never auto-resolved: its whole value is that it FLAGS a genuine conflict for
         a mind to look at (same law `_fn_lint`'s contradiction check and fold_project's
         refuse-rather-than-destroy already follow), and collapsing it to a count would throw
         away the one piece of information a reader can actually act on.
      3. `closed_by_topology` — an edge exists and there's no disagreement. Ground truth
         positive (thread_closure_status's own law). Broken down by `strength` (from
         `thread_closure_status` itself, not re-derived): `strong` (`resolved_by`/`answers`
         — artifact- or ruling-backed) vs `weak` (`closed_by` — self-attested, WHO closed it
         rather than WHAT). This is the category Thoth named after living it (DM 2937): a
         citation that DOES resolve gets `strong`; a citation that does NOT resolve still
         lands here, `weak`, via the unconditional Phase 1a fallback — a real, findable
         closure that nonetheless names no specific commit or decision. Distinct from BOTH
         `resolved_edgeless` axes below, and it is the one that bit this very report.
      4. `resolved_edgeless` — no edge, `property_status='resolved'`. THE ROT METRIC: a
         closure that depends on a citation happening to resolve into a graph object is
         remembered, not structural (ruling 4ef68cfe). See below for its own sub-split.
      5. `open_both` — no edge, `property_status='open'`. Genuinely, unambiguously open.

    `resolved_edgeless` SUB-SPLIT — the enforcement-possibility map Thoth asked for, turning
    one number into a work order with two piles: for each edgeless-resolved thread, re-run
    the EXACT resolution capture.resolve_thread's own artifact path uses
    (`capture._find_artifact`, imported rather than re-implemented) against its
    `resolved_artifact` property, if it has one.
      - `commit_closeable` — a `resolved_artifact` exists AND it resolves to a real graph
        object RIGHT NOW (it may not have when the thread was originally closed — the graph
        keeps growing). A backfill pass could mint the missing edge mechanically, no human
        needed.
      - `needs_human` — everything else: no instrument, mechanical or otherwise, can close
        this without a person supplying or confirming evidence.
      Named separately within that, because Thoth asked for the distinction by name and it
      is NOT the same axis: `cited` (a `resolved_artifact` property exists — someone DID
      point at evidence when they closed it) vs `uncited` (no such property — the close was
      pure prose, `because` only, nothing to even attempt resolving). `commit_closeable` is
      always a subset of `cited`; `needs_human` = `uncited` + the `cited` rows whose artifact
      still doesn't resolve to anything.

    A THIRD, ORTHOGONAL SUB-SPLIT on `resolved_edgeless` (Thoth DM 2937, the same night this
    was proposed): `pre_fix_sediment` (the thread's `status` assertion was observed before
    `_PHASE_1A_FIX_AT` — Khnum's commit 23c5991, the moment resolve_thread started minting
    `closed_by` unconditionally) vs `post_fix_regression` (observed at or after it). Measured
    live at build time: `pre_fix_sediment` was 100% of the osiris-scoped 383, `post_fix_
    regression` was 0 — the fix is holding. This is now a MECHANICAL WATCH on that fact
    rather than a claim someone has to re-verify by hand: `post_fix_regression` should read
    zero forever, and any nonzero reading is a live alarm that something new is bypassing the
    fallback, not a report card on the past.

    `repo` (a project name, string) scopes the read; the subject, if focused, wins over
    `args.repo`; neither given = fleet-wide, matching `enumerate_threads`'s own convention.
    The echoed `"repo"` field in the return value NAMES WHAT ACTUALLY RAN — re-read from the
    resolved `repo` id itself, not from `args.get("repo")` alone (Thoth's own catch, DM
    2951: `run_composition('closure-health', subject='osiris')` resolves `subject` to a real
    id BEFORE this Function ever sees it, so echoing raw `args` reported `null` on a call
    that had in fact scoped correctly — the same shape as the resolve_thread receipt bug two
    commits earlier in this same night, a third specimen of an honest act with a dishonest
    account). Genuinely fleet-wide (neither subject nor args.repo given) echoes the literal
    string `"*fleet*"`, never `null` — a dropped argument and a deliberate fleet-wide scope
    are different facts and must not render identically.
    Deliberately NOT folded into `_fn_lint`'s EDGELESS-CLOSURE-GROWTH ratchet — that stays a
    narrow, fast, fleet-wide ceiling check (a single aggregate number, tripwire-shaped); this
    is a separate, richer, per-project, addressable surface the ratchet could eventually read
    from instead of duplicating. Read-only, no writes, same as every other rung-2 Function
    here."""
    from src.orchestrator.capture import _find_artifact
    from src.orchestrator.thread_closure import thread_closure_status

    repo = subject
    if repo is None and args.get("repo"):
        repo_name = str(args["repo"]).strip()
        repo = await pool.fetchval(
            "SELECT o.id FROM objects o JOIN current_assertions a ON a.object_id=o.id "
            "WHERE o.type='SoftwareProject' AND a.name='name' AND a.value #>> '{}'=$1 LIMIT 1",
            repo_name)
        if repo is None:
            return {"note": f"no SoftwareProject named {repo_name!r}"}

    repo_label: str = "*fleet*"
    if repo is not None:
        repo_label = await pool.fetchval(
            "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name='name' "
            "ORDER BY confidence DESC, observed_at DESC LIMIT 1", repo) or str(repo)

    rows = await thread_closure_status(pool, repo=repo)
    retracted_or_no_status: list[dict[str, Any]] = []
    disagree: list[dict[str, Any]] = []
    closed_by_topology: list[dict[str, Any]] = []
    resolved_edgeless: list[dict[str, Any]] = []
    open_both: list[dict[str, Any]] = []
    for r in rows:
        ps = r["property_status"]
        if ps not in ("open", "resolved"):
            retracted_or_no_status.append(r)
        elif r["closed_by_topology"] and ps == "open":
            disagree.append(r)
        elif r["closed_by_topology"]:
            closed_by_topology.append(r)
        elif ps == "resolved":
            resolved_edgeless.append(r)
        else:
            open_both.append(r)

    edgeless_ids = [r["thread_id"] for r in resolved_edgeless]
    artifact_rows = await pool.fetch(
        "SELECT object_id, value #>> '{}' AS resolved_artifact FROM current_assertions "
        "WHERE object_id = ANY($1::uuid[]) AND name = 'resolved_artifact'",
        edgeless_ids) if edgeless_ids else []
    artifact_by_id = {r["object_id"]: r["resolved_artifact"] for r in artifact_rows}
    commit_closeable = 0
    cited_but_unresolved = 0
    for tid in edgeless_ids:
        artifact = artifact_by_id.get(tid)
        if artifact is None:
            continue
        if await _find_artifact(pool, artifact) is not None:
            commit_closeable += 1
        else:
            cited_but_unresolved += 1
    uncited = len(edgeless_ids) - commit_closeable - cited_but_unresolved

    status_at_rows = await pool.fetch(
        "SELECT DISTINCT ON (object_id) object_id, observed_at FROM current_assertions "
        "WHERE object_id = ANY($1::uuid[]) AND name = 'status' "
        "ORDER BY object_id, confidence DESC, observed_at DESC",
        edgeless_ids) if edgeless_ids else []
    status_at_by_id = {r["object_id"]: r["observed_at"] for r in status_at_rows}
    post_fix_regression = sum(
        1 for tid in edgeless_ids
        if status_at_by_id.get(tid) and status_at_by_id[tid] >= _PHASE_1A_FIX_AT)
    pre_fix_sediment = len(edgeless_ids) - post_fix_regression

    closed_strong = sum(1 for r in closed_by_topology if r["strength"] == "strong")
    closed_weak = sum(1 for r in closed_by_topology if r["strength"] == "weak")

    return {
        "repo": repo_label,
        "total": len(rows),
        "retracted_or_no_status": len(retracted_or_no_status),
        "disagree": [str(r["thread_id"])[:8] for r in disagree],
        "closed_by_topology": {
            "total": len(closed_by_topology),
            "strong": closed_strong,
            "weak": closed_weak,
        },
        "resolved_edgeless": {
            "total": len(resolved_edgeless),
            "commit_closeable": commit_closeable,
            "needs_human": len(edgeless_ids) - commit_closeable,
            "cited": commit_closeable + cited_but_unresolved,
            "uncited": uncited,
            "pre_fix_sediment": pre_fix_sediment,
            "post_fix_regression": post_fix_regression,
        },
        "open_both": len(open_both),
    }


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
    "roadmap_open": _fn_roadmap_open,
    "desk_decisions": _fn_desk_decisions,
    "echoes": _fn_echoes,
    "wall": _fn_wall,
    "practices": _fn_practices,
    "fleet_live_agents": _fn_fleet_live_agents,
    "fleet_pulse_line": _fn_fleet_pulse_line,
    "fleet_live": _fn_fleet_live,
    "mail_overview": _fn_mail_overview,
    "mail_threads": _fn_mail_threads,
    "overhead": _fn_overhead,
    "desk_overview": _fn_desk_overview,
    "desk_project": _fn_desk_project,
    "triage": _fn_triage,
    "closure_health": _fn_closure_health,
    "reference_catalog": _fn_reference_catalog,
}

# Functions that brief the whole project rather than anchor on one entity — no subject needed.
# `project` is here too: it drills into ONE repo, taken from the focused subject OR `args.repo`,
# so it must run without a bound subject (it returns a "focus a repo" note if given neither).
# NB: `projects`, `briefing`, `decisions` are GONE as Functions — they decomposed into pure
# op-trees (a `table`, a `sections`, a `sections`+show-original — see DEFAULT_COMPOSITIONS):
# opinion → primitives the user owns.
# `lap` anchors on args.ref OR the subject; `lint` audits the whole graph, no anchor at all.
# `triage` is the same shape as `lint` — census/buckets both scope via `args`, never a subject.
_SUBJECT_FREE = {"canon", "search", "family", "family_drift", "portfolio", "pulse", "project",
                 "lap", "lint", "echoes", "wall", "desk_decisions", "practices",
                 "fleet_live_agents", "fleet_pulse_line", "fleet_live", "mail_overview",
                 "mail_threads", "overhead", "desk_overview", "desk_project", "triage",
                 "closure_health", "reference_catalog"}


def list_functions() -> list[str]:
    """The registered Functions a composition may reference (the authoring channel reads
    this to know what's beyond the closed op set)."""
    return sorted(_FUNCTIONS)


# Guardrails adopted from Palantir's Object Set API (load-tested, not arbitrary).
MAX_TRAVERSE_HOPS = 3
MAX_AGGREGATE_DIMS = 3
# `group`'s own cap (ruling c5b184cd, thread d56e7073/#44): matches aggregate's own dimension
# cap, not arbitrary — arc->status->owner (roadmap's real depth) is exactly 3.
MAX_GROUP_DEPTH = 3


@dataclass
class Result:
    """A composition's output — an object set, a value list, or aggregate rows."""

    kind: str  # "objects" | "values" | "rows" | "data"
    objects: list[uuid.UUID] = field(default_factory=list)
    values: list[str] = field(default_factory=list)
    rows: list[dict[str, Any]] = field(default_factory=list)
    data: Any = None  # a Function's native output (list/dict) — opaque to the ops


# `group`'s body evaluates against "this partition's members" — threaded the same way
# `_ACL_CALLER` already threads the reflection ACL through nested `_eval` calls (a contextvar,
# not a new parameter on every op handler): a nested `group`'s own `{"op":"these"}` shadows the
# outer one, exactly the lexical scoping a reader expects. Holds the partition's own raw
# `Result` (task #60, the function-output-re-entering-the-op-tree follow-on) — not just a bare
# UUID list — so a Function-sourced partition (kind="rows") nests `group`/`order`/`take` under
# its own body exactly like an object-sourced one (kind="objects"), no second leaf type needed.
_THESE: ContextVar[Result | None] = ContextVar("_THESE", default=None)
_GROUP_DEPTH: ContextVar[int] = ContextVar("_GROUP_DEPTH", default=0)


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


async def _props_batch(
    pool: asyncpg.Pool, oids: list[uuid.UUID],
) -> dict[uuid.UUID, dict[str, str]]:
    """BATCHED sibling of `_props` (task #164, dispatch msg 3982): `winning_props`'s own
    signature already takes an array — `object_items` (line ~4130) already calls it this
    way — but the `select` op's inner loop called `_props` once PER OBJECT instead, a
    singleton-array round trip repeated N times. Measured live: 2947 active Threads,
    ~3.07ms/call, LIVE_DESK's two Thread-scanning sections paying it twice — 2 × 2947 ×
    3.07ms = 18.1s projected against 16-17s measured on the console's own `/` route. This
    is the SAME resolution rule as `_props` (constitution #5, winning_props' own ordering),
    batched — never a second definition of "winning." `_props` itself is UNCHANGED and
    still used by every other call site (`collect`/`focus`/etc.) — this fixes only the
    `select` op's own N+1, per Thoth's explicit scope: batch the read, do not rewrite the
    op. Every requested id gets an entry (possibly empty), so a caller can `.get(oid)`
    without a fallback for 'never asked'."""
    if not oids:
        return {}
    rows = await pool.fetch(
        "SELECT object_id, name, value #>> '{}' AS v FROM winning_props($1::uuid[])", oids,
    )
    out: dict[uuid.UUID, dict[str, str]] = {oid: {} for oid in oids}
    for r in rows:
        out[r["object_id"]][r["name"]] = r["v"]
    return out


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


def _row_action_arg(row: dict[str, Any], spec: dict[str, Any]) -> Any:
    """One arg's value for a `row_actions` (plural) template — EXACTLY one of `{"literal":
    v}` (a caller-given constant, e.g. `because: "operator: done"`) or `{"property": p}`
    (`row.get(p)` — a Function's row is already its own facts, no `_props` indirection
    needed; a list-valued property, e.g. a thread-fold's own id list, passes through
    unchanged — no separate bulk-arg primitive required).

    REFUSES LOUDLY on either malformed shape, rather than picking a silent winner (Thoth
    msg 1976): both keys present is almost certainly a copy-paste mistake with the wrong one
    live, and a quiet precedence there is exactly the kind of bug that survives review
    because the OTHER value looked plausible too. An unknown key is the same class of
    mistake — a KeyError-shaped refusal now beats a None arg the verb accepts silently and
    the operator only notices when the write does the wrong thing. Deliberately NOT shared
    with `row_action`'s (singular) own inline resolution — that one is live in production
    (89df464, browser-verified via 37af8b7) and stays untouched; only `row_actions` gets the
    richer template."""
    unknown = set(spec) - {"literal", "property"}
    if unknown:
        raise ValueError(f"row_actions arg spec has unknown key(s) {sorted(unknown)}: {spec!r}")
    if "literal" in spec and "property" in spec:
        raise ValueError(f"row_actions arg spec has BOTH literal and property — exactly "
                         f"one is required: {spec!r}")
    if "literal" in spec:
        return spec["literal"]
    if "property" in spec:
        return row.get(str(spec["property"]))
    raise ValueError(f"row_actions arg spec needs literal or property: {spec!r}")


async def _eval(pool: asyncpg.Pool, node: dict[str, Any], subject: uuid.UUID | None) -> Result:
    op = node.get("op")

    if op == "subject":
        return Result("objects", objects=[subject] if subject else [])

    if op == "these":
        # the nearest enclosing `group`'s own partition — see _THESE above. Empty outside a
        # group body (never an error: an author testing a fragment standalone gets an empty
        # set, not a crash, same "guess never poisons a real answer" spirit as the rest of
        # this dispatcher). Returns the partition's own Result verbatim — "objects" for a
        # DB-sourced partition, "rows" for a Function-sourced one (task #60) — so whatever
        # nests under `body` (a further group/order/take/table) sees the same kind it would
        # from any other op.
        return _THESE.get() or Result("objects")

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
        # BATCHED, not one winning_props round trip per row (task #164, msg 3982): the
        # console's own `/` route measured 16-17s from exactly this shape, LIVE_DESK
        # scanning ~2947 active Threads twice at ~3ms/call. See `_props_batch`.
        props_by_id = await _props_batch(pool, [r["id"] for r in rows])
        out: list[uuid.UUID] = []
        for r in rows:
            facts = props_by_id.get(r["id"], {})
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
        return Result("rows", rows=await _table(
            pool, base.objects, node.get("columns", []) or [], node.get("row_action")))

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

    if op == "group":
        # THE DYNAMIC section (ruling c5b184cd, thread d56e7073/#44): `sections` needs its
        # titles hardcoded in the spec; `aggregate` groups but keeps only a metric. `group`
        # is the missing middle — one section PER DISTINCT VALUE of a property, each holding
        # its own full sub-result (nestable: a partition's `body` may itself be another
        # `group`, resolving `{"op":"these"}` to THIS partition — arc -> status -> owner is
        # three of these composed, nothing more).
        depth = _GROUP_DEPTH.get()
        if depth >= MAX_GROUP_DEPTH:
            raise ValueError(f"group nesting exceeds {MAX_GROUP_DEPTH} levels")
        by = node.get("by")
        if not by:
            raise ValueError("group requires 'by'")
        body = node.get("body")
        if not body:
            raise ValueError("group requires 'body'")
        base = await _eval(pool, node["from"], subject)
        # Two sources of members to partition: real objects (DB-driven, re-derives the `by`
        # property per object like `aggregate` always has) or a Function's own already-
        # materialized rows (task #60 — the function-output-re-entering-the-op-tree follow-on:
        # a Function's list-of-dicts output arrives here as kind="rows", see the `function` op
        # below). Both converge into the SAME title -> Result map before the existing
        # per-partition eval loop, which doesn't care which source produced it.
        bucket_results: dict[str, Result] = {}
        if base.kind == "rows":
            row_buckets: dict[str, list[dict[str, Any]]] = {}
            for row in base.rows:
                key = row.get(by) if isinstance(row, dict) else None
                title = str(key) if key is not None else "(none)"
                row_buckets.setdefault(title, []).append(row)
            bucket_results = {t: Result("rows", rows=rs) for t, rs in row_buckets.items()}
        else:
            buckets = await _group_by(pool, base.objects, [str(by)])
            bucket_results = {
                (str(key[0]) if key and key[0] is not None else "(none)"):
                    Result("objects", objects=[oid for oid, _ in members])
                for key, members in buckets.items()
            }
        partitions: dict[str, Any] = {}
        depth_token = _GROUP_DEPTH.set(depth + 1)
        try:
            for title, members in bucket_results.items():
                these_token = _THESE.set(members)
                try:
                    partitions[title] = await _package(pool, await _eval(pool, body, subject))
                finally:
                    _THESE.reset(these_token)
        finally:
            _GROUP_DEPTH.reset(depth_token)
        sequence = node.get("sequence")
        if sequence:
            # Listed titles first, in the given order (one absent from the data is a
            # silent no-op — same as an empty group always was). Everything ELSE appends
            # after, alphabetically — visible, never dropped: a typo'd or new title must
            # never vanish just because nobody added it to the sequence yet.
            ordered: dict[str, Any] = {t: partitions[t] for t in sequence if t in partitions}
            ordered.update(sorted((t, v) for t, v in partitions.items() if t not in sequence))
            partitions = ordered
        return Result("data", data=partitions)

    if op == "function":
        name = str(node.get("name", ""))
        fn = _FUNCTIONS.get(name)
        if fn is None:
            raise ValueError(f"unknown function: {name!r}")
        if subject is None and name not in _SUBJECT_FREE:
            raise ValueError(f"function {name!r} requires a subject")
        data = await fn(pool, subject, node.get("args", {}) or {})
        # task #60 (the function-output-re-entering-the-op-tree follow-on): a Function whose
        # own output is already row-shaped (a list of dicts — `desk_decisions`'s messages,
        # `roadmap_open`'s ranked threads) is no longer a dead-end leaf. Shape-based, not an
        # opt-in flag on the Function itself: `group`/`order`/`take` already branch on
        # Result.kind, so this one reclassification is what lets them all reach a Function's
        # output for free. A dict-shaped Function (sections-like output — `pulse`, `wall`)
        # stays kind="data"; nothing there is orderable/groupable and nothing should be.
        if isinstance(data, list) and all(isinstance(x, dict) for x in data):
            row_action = node.get("row_action")
            if row_action:
                # A Function's row is already its own facts, so this needs no `_props`/
                # `_col_value` indirection the way `_table`'s object-backed version does:
                # `row.get(property)` directly. The CLIENT half (table() recognizing
                # `_action` as a control, a click-delegate POSTing to /act) shipped in
                # 37af8b7 — browser-verified against live-desk's resolve button. A
                # `"run:<function>"` action is the navigation form (task #90, Thoth msg
                # 1976/2005) — the client dispatches a DOM event instead of POSTing, and the
                # page shell runs the named Function via /compositions/run-spec, showing its
                # Result. SINGULAR only: one action, one property-templated arg each — see
                # `row_actions` below for a row that affords more than one verb.
                for row in data:
                    row["_action"] = {
                        "action": row_action.get("action"),
                        "args": {arg: row.get(str(spec.get("property")))
                                for arg, spec in (row_action.get("args") or {}).items()},
                    }
            row_actions = node.get("row_actions")
            if row_actions:
                # PLURAL (Thoth msg 1976, gating msg 1971's proposal) — a row that affords
                # more than one verb (chrome.py's /desk: done/not mine/later, three DIFFERENT
                # actions on one debt row) can't be expressed by `row_action`'s single
                # {action, args}. `row_actions` is a list of {label, action, args}; each row
                # gets `_actions: [...]`, one entry per declared verb. The client renders N
                # buttons, same click delegate as the singular form (task #91, Thoth msg
                # 1976/2029). NB: desk_project's own three-verb debt rows embed `_actions`
                # directly in Python rather than declaring `row_actions` here — see that
                # Function's own docstring for why this node-level path isn't the fit there.
                for row in data:
                    row["_actions"] = [
                        {"label": str(ra.get("label") or ra.get("action") or ""),
                         "action": ra.get("action"),
                         "args": {arg: _row_action_arg(row, spec)
                                 for arg, spec in (ra.get("args") or {}).items()}}
                        for ra in row_actions
                    ]
            return Result("rows", rows=data)
        return Result("data", data=data)

    raise ValueError(f"unknown composition op: {op!r}")


def _col_value(oid: uuid.UUID, facts: dict[str, str], prop: str) -> Any:
    """The value one property name resolves to for one row — shared by column resolution and
    `row_action`'s own arg templates, so both name properties the same way. `id` is special
    (task #60, thread b81b0fac): not an assertion — a row is a candidate object, not a fact
    ABOUT one — so it never lives in `facts`. Same 8-char short-id convention every other
    read site uses (_owned_open_threads' substring(o.id::text,1,8), the open-thread wall's
    own ids)."""
    if prop == "id":
        return str(oid)[:8]
    if prop == "summary":
        # THE CORRECTED SUMMARY WINS BY DEFAULT (roadmap ledger-rot stage 3.5, decision
        # c0bc6d33 + Thoth LXXIV's DM 4364): `facts` already carries BOTH properties (winning_
        # props returns every current one, not just requested columns) — same COALESCE law
        # as _SUMMARY_DISPLAY_SQL, applied here for the op-tree's own column path (roadmap's
        # resolved/retracted sections and any other `table` op reading a Thread's summary).
        return facts.get("corrected_summary") or facts.get("summary")
    return facts.get(prop)


async def _table(
    pool: asyncpg.Pool, objects: list[uuid.UUID], columns: list[dict[str, Any]],
    row_action: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """One ROW per object; each column is a property value or a rollup over a link (Notion's
    database+rollups / Palantir's object-set + per-object aggregate). Over a bounded set (a
    select/traverse result), so per-object queries are fine — the whole point is that the SET
    is already candidate-gated by the op that produced it.

    `row_action` (ruling c5b184cd, thread d56e7073/#44 — the write leg) declares a CONTROL
    every row carries, not a displayed column: `{"action":<name>,"args":{<argname>:
    {"property":P}}}`. Each row's own args get resolved HERE, from that SAME row's own
    already-fetched facts (the identical `_col_value` a column would use), and attached as a
    private `_action` key the renderer reads — never a column, never sent to the client as
    displayed data. The op-tree only ever DECLARES the shape; `/act`'s own registry (a
    separate, closed dispatch table) is what actually enforces which actions and args are
    real — this function has no opinion on that, same discipline as everywhere else in this
    dispatcher (a Function/op computes DATA, it never decides what's SAFE to write).

    Props read is BATCHED (task #164 follow-on, dispatch msg 4010): measured live via
    Sekhmet's index-scan protocol, one `_table` call inside `orient()`'s project-briefing
    averaged ~42.8k assertions_supersedes_idx scans / ~15.7k assertions_object_name_idx
    scans and ~2.2s wall-clock for a single call — the same per-object `_props()` loop
    task #164 already fixed in the `select` op, unmigrated here. See `_props_batch`."""
    # OBJECT COLUMNS (canonical/type/status), not assertions — `_props`/`winning_props`
    # only ever reads `current_assertions`, so a column asking for one of these by name
    # would silently resolve to None without this (task #138/#163's arc: caught live
    # wiring `canonical` into the `projects` composition — the same _OBJ_COLS whitelist
    # `_rollup`'s own `of:"first"` already trusts, reused here rather than re-declared).
    obj_cols_needed = {str(c["property"]) for c in columns
                      if "property" in c and str(c["property"]) in _OBJ_COLS}
    obj_cols: dict[uuid.UUID, dict[str, Any]] = {}
    if obj_cols_needed and objects:
        cols_sql = ", ".join(sorted(obj_cols_needed))
        obj_cols = {r["id"]: dict(r) for r in await pool.fetch(
            f"SELECT id, {cols_sql} FROM objects WHERE id = ANY($1::uuid[])", objects)}
    rows: list[dict[str, Any]] = []
    props_by_id = await _props_batch(pool, objects)
    for oid in objects:
        facts = props_by_id.get(oid, {})
        if oid in obj_cols:
            facts = {**facts, **{k: v for k, v in obj_cols[oid].items() if k != "id"}}
        row: dict[str, Any] = {}
        for col in columns:
            name = str(col.get("name") or col.get("property") or "col")
            if "property" in col:
                row[name] = _col_value(oid, facts, str(col["property"]))
            elif "rollup" in col:
                row[name] = await _rollup(pool, oid, col["rollup"])
            else:
                row[name] = None
        if row_action:
            row["_action"] = {
                "action": row_action.get("action"),
                "args": {
                    arg: _col_value(oid, facts, str(spec.get("property")))
                    for arg, spec in (row_action.get("args") or {}).items()
                },
            }
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


async def _group_by(
    pool: asyncpg.Pool, objects: list[uuid.UUID], group_by: list[str],
) -> dict[tuple[str | None, ...], list[tuple[uuid.UUID, dict[str, str]]]]:
    """Bucket objects by one or more property values — the ONE grouping loop shared by
    `aggregate` (which collapses each bucket to a metric, discarding the rest) and `group`
    (which keeps each bucket as its own renderable sub-result). Each bucket keeps (object id,
    its already-fetched facts) so neither caller re-queries the same properties twice."""
    groups: dict[tuple[str | None, ...], list[tuple[uuid.UUID, dict[str, str]]]] = {}
    for oid in objects:
        facts = await _props(pool, oid)
        key = tuple(facts.get(g) for g in group_by)
        groups.setdefault(key, []).append((oid, facts))
    return groups


async def _aggregate(
    pool: asyncpg.Pool, objects: list[uuid.UUID], group_by: list[str], metric: dict[str, Any]
) -> list[dict[str, Any]]:
    """Group objects by property values, compute one metric per group (Palantir groupBy /
    Notion rollup). group_by=[] aggregates the whole set into a single row."""
    mtype = metric.get("type", "count")
    field_name = metric.get("field")
    groups = await _group_by(pool, objects, group_by)
    rows: list[dict[str, Any]] = []
    for key, members in groups.items():
        group = {g: k for g, k in zip(group_by, key, strict=True)}
        if mtype == "count":
            value: float | int = len(members)
        else:
            raw = [m[1].get(field_name) for m in members] if field_name else []
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
    refresh_secs: int | None = None,
) -> uuid.UUID:
    """Save (or update) a composition by name. Fork = save under a new name. `webhook_url`
    and `active` are a watch's execution metadata (a lens ignores them). `room_id` scopes it
    to a stance; a re-save without one keeps the existing one; a genuine create without one
    gets the named fallback below, never a bare NULL — see ROOM_ID GETS THE SAME TREATMENT.
    `description` = one line of 'when to open this'; `section` = which shelf of the composer
    sidebar (arrive | wall | memory | fleet | engine | casework) — description and
    refresh_secs keep their prior value when omitted on a re-save, same COALESCE-keeps-prior
    contract as always; section and room_id no longer CAN be omitted into NULL at all, on
    either a create or a re-save (see below — both needed the same fix, for the same reason).
    `refresh_secs` (ruling cf9286b2) is how often the watermark-poller should check for this
    lens while it's the one on screen — None (the default) means MANUAL ONLY: a lens goes
    live because someone decided it should, never because it inherited a global tick.

    MUST BE SECTIONED (task #94): neither the MCP save_composition tool nor the HTTP
    /compositions route ever pass `section` — a fresh save through either always arrives here
    with section=None. Without a guard, that upserts section=NULL directly (the UPDATE
    branch's COALESCE only ever protects a RE-save, never a genuine CREATE), and a
    room_id=NULL, section=NULL composition renders nowhere: room=NULL excludes it from every
    room-scoped read, and section=NULL used to have no NOT-NULL backstop either. `_more` is
    already the CLIENT's own fallback shelf label (osiris.js: `c.section||'_more'`) for
    exactly this case — reused here rather than inventing a second sentinel, so an
    uncategorized composition still lands somewhere a reader can find it.

    ROOM_ID GETS THE SAME TREATMENT (ruling 89e67c49): room_id=NULL turned out to have the
    identical "invisible outside the god view" defect section=NULL did (0 of 28 compositions
    visible in the one room in active use have room_id=NULL — confirmed live, task #94).
    Resolved the same way, for the same Postgres reason (below): reuse the prior row's
    room_id on a re-save, or the 'engineer' room BY NAME (not a hardcoded id) on a genuine
    create with none at all — looked up fresh each call since a room's id isn't portable
    across environments the way the string '_more' is. The fallback firing is logged
    (logger.warning), never silent — "a default that reports beats a silent one" (89e67c49):
    a misfiled-but-visible composition is recoverable, an invisible one isn't.

    UNLIKE SECTION, room_id gets NO DB-level NOT NULL constraint. The column carries
    `REFERENCES rooms(id) ON DELETE SET NULL` (migration 0010) specifically so deleting a
    room gracefully orphans its compositions back to unassigned rather than breaking the
    delete; a NOT NULL constraint would turn deleting ANY room that still has compositions
    in it into a hard failure. Room deletion setting room_id to NULL is a real, legitimate,
    DIFFERENT case from 'never assigned one at creation' — only the latter is what this
    function's own resolution closes. If the 'engineer' room itself doesn't exist in this
    environment (every test DB; a fresh install before anyone's created a room), room_id is
    left None rather than fabricating a value that would fail its own foreign key — logged
    either way, so the gap is visible rather than assumed away."""
    if section is None:
        # Postgres validates NOT NULL against the ATTEMPTED insert row even on a path that
        # will end up taking the ON CONFLICT DO UPDATE branch — the UPDATE SET clause's own
        # COALESCE(EXCLUDED.section, compositions.section) below never gets a chance to run
        # if the plain INSERT tuple itself already violates the constraint. So the "keep
        # prior value on omission" resolution has to happen HERE, in Python, for section
        # specifically: reuse the existing row's section on a re-save, or '_more' if there's
        # no prior row at all (a genuine create) — section is never passed as a literal NULL
        # into the query below, in either case.
        prior_section = await pool.fetchval(
            "SELECT section FROM compositions WHERE name=$1", name)
        if prior_section is None:
            logger.warning(
                "save_composition(%r): no section given and none on record — "
                "defaulting to the '_more' shelf", name)
        section = prior_section or "_more"
    if room_id is None:
        prior_room = await pool.fetchval(
            "SELECT room_id FROM compositions WHERE name=$1", name)
        if prior_room is not None:
            room_id = prior_room
        else:
            fallback_room = await pool.fetchval(
                "SELECT id FROM rooms WHERE name='engineer'")
            if fallback_room is not None:
                logger.warning(
                    "save_composition(%r): no room_id given and none on record — "
                    "defaulting to the 'engineer' room (%s)", name, fallback_room)
                room_id = fallback_room
            else:
                logger.warning(
                    "save_composition(%r): no room_id given, none on record, and no "
                    "'engineer' room exists here — leaving room_id unassigned", name)
    return await pool.fetchval(  # type: ignore[no-any-return]
        "INSERT INTO compositions (name, kind, spec, webhook_url, active, room_id, "
        " description, section, refresh_secs) "
        "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) "
        "ON CONFLICT (name) DO UPDATE SET spec=EXCLUDED.spec, kind=EXCLUDED.kind, "
        "  webhook_url=EXCLUDED.webhook_url, active=EXCLUDED.active, "
        "  room_id=COALESCE(EXCLUDED.room_id, compositions.room_id), "
        "  description=COALESCE(EXCLUDED.description, compositions.description), "
        "  section=COALESCE(EXCLUDED.section, compositions.section), "
        "  refresh_secs=COALESCE(EXCLUDED.refresh_secs, compositions.refresh_secs) "
        "RETURNING id",
        name, kind, spec, webhook_url, active, room_id, description, section, refresh_secs,
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
    """Saved compositions. `room_id` scopes to a stance (None = all rooms — the god view).
    `refresh_secs` (ruling cf9286b2) rides along here, not on run_composition's own result —
    it's composition METADATA (same table/row as description/section), read once when the
    sidebar loads rather than re-fetched on every run."""
    return [
        {"id": str(r["id"]), "name": r["name"], "kind": r["kind"], "spec": _coerce(r["spec"]),
         "webhook_url": r["webhook_url"], "active": r["active"],
         "room_id": str(r["room_id"]) if r["room_id"] else None,
         "description": r["description"], "section": r["section"],
         "refresh_secs": r["refresh_secs"]}
        for r in await pool.fetch(
            "SELECT id, name, kind, spec, webhook_url, active, room_id, description, section, "
            " refresh_secs "
            "FROM compositions "
            "WHERE ($1::uuid IS NULL OR room_id=$1) ORDER BY created_at", room_id
        )
    ]


async def object_items(pool: asyncpg.Pool, ids: list[uuid.UUID]) -> list[dict[str, Any]]:
    """Label a result set's objects AND carry their compact properties — in two batch
    queries, not N. The view-switcher needs this: the Graph view uses label/type, the
    Table view shows property columns (sector, date, …) without a per-row fetch.

    Label resolution is `resolve_label` (task #97 workstream 3, ruling 52daab71) — the
    same rule/chain/canonical engine every other consumer now shares, not a fourth
    hand-rolled fallback chain agreeing with the others by coincidence. `display_label`
    is `disambiguate_labels` across THIS result set — the board/table views are exactly
    where the reported bug (three rows truncating to one indistinguishable string) is
    most visible."""
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
        res = resolve_label(o["type"], p, o["canonical"])
        out.append({"id": str(oid), "type": o["type"], "canonical": o["canonical"],
                    "label": res.label, "props": p,
                    **({"subtitle": res.subtitle} if res.subtitle else {})})
    disp = disambiguate_labels([(o["id"], o["label"], o["canonical"]) for o in out])
    for o in out:
        o["display_label"] = disp[o["id"]]
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


def _count_leaves(node: Any) -> int:
    """How many actual rows a nested "data" dict holds — NOT `len(dict)` (top-level KEYS,
    what `run_spec`'s own `count` field already means and keeps meaning; this is the honest
    total `take`/`depth` bound against, the number the ROADMAP-sized-blob problem is
    actually about). A dict recurses (section→arc→owner…); a list is that many rows; a
    scalar is one."""
    if isinstance(node, dict):
        return sum(_count_leaves(v) for v in node.values())
    if isinstance(node, list):
        return len(node)
    return 1


def _bound_items(
    items: Any, *, fields: list[str] | None, take: int | None, depth: int | None,
    offset: int | None = None,
) -> tuple[Any, dict[str, dict[str, int]]]:
    """THE LENS doing its own job (ruling ad19a779, task #64) instead of leaning on
    `budget.fit()`'s blind backstop — a caller who KNOWS they want 3 rows of 2 fields
    should never have to receive 53 full rows and pay the trim after. Same "no silent
    caps" law as `fit()` (same "shown"/"of" shape too, so a reader who's seen one has seen
    both): every path this actually trims is reported, never a quiet drop. Structural dict
    nesting (a `group`'s own section/arc/owner keys) is walked, never filtered by `fields`
    — `fields` only ever prunes a LEAF row's own columns; `depth` collapses everything
    below the requested level to its honest count rather than silently rendering it flat
    (a caller who asked for depth=1 must not be quietly handed depth=3).

    `offset` (task #149, Thoth DM 3847 — "NO take/offset/cursor"): `take` alone can only
    ever show the FIRST N of a list, on every call, forever — there is no way to ask for
    the NEXT N. Measured live: a 603-item open-threads composition, asked for with no
    lens at all, correctly reported `_bounded: shown 37 of 603` (the generic backstop
    DOES announce itself) but offered no way to see items 38-603 short of building ~20
    separate narrower, hand-partitioned compositions (owner-by-owner, keyword-by-keyword)
    — the actual workaround this task's own dispatch named. `offset` skips N before
    taking, so `take=50, offset=50` is page 2 of the same ordering the composition's own
    op-tree already produced (rank/order runs inside the op-tree; this never re-sorts,
    same law `take` alone already held) — one simple, honest counter, not a stateful
    cursor token, because the ordering itself is already stable per spec."""
    dropped: dict[str, dict[str, int]] = {}
    start = offset or 0

    def walk(node: Any, path: tuple[str, ...], remaining_depth: int | None) -> Any:
        if isinstance(node, dict):
            if remaining_depth is not None and remaining_depth <= 0:
                total = _count_leaves(node)
                dropped[".".join(path) or "(root)"] = {"shown": 0, "of": total}
                return {"_count": total}
            next_depth = None if remaining_depth is None else remaining_depth - 1
            return {k: walk(v, (*path, str(k)), next_depth) for k, v in node.items()}
        if isinstance(node, list):
            end = start + take if take is not None else None
            kept = node[start:end]
            if start or (take is not None and len(node) > start + len(kept)):
                entry = {"shown": len(kept), "of": len(node)}
                if start:
                    entry["offset"] = start
                dropped[".".join(path) or "(root)"] = entry
            return [_bound_row(x, fields) for x in kept]
        return node

    return walk(items, (), depth), dropped


def _bound_row(item: Any, fields: list[str] | None) -> Any:
    if fields and isinstance(item, dict):
        return {k: item[k] for k in fields if k in item}
    return item


async def run_spec(
    pool: asyncpg.Pool, spec: dict[str, Any], subject: uuid.UUID | None = None,
    name: str = "(spec)", caller: str | None = None,
    *, fields: list[str] | None = None, take: int | None = None, depth: int | None = None,
    offset: int | None = None,
) -> dict[str, Any]:
    """Evaluate an op-tree and package the Result for the generic renderer. The inline
    composer (W4) runs an EPHEMERAL working spec through here as you edit chips — no save.
    `caller` is who is reading (an agent id, 'operator'/'console' for the human's own
    surfaces, None for anonymous) — the reflection ACL's input (6c18709f), carried to the
    ops on a contextvar so a nested select inherits it without parameter-threading.

    `fields`/`take`/`depth` (ruling ad19a779, task #64 — projection/pagination) bound the
    PACKAGED result before it ever reaches a caller: `fields` keeps only the named columns
    on each leaf row, `take` caps each list to its first N (composition ops already put the
    most relevant rows first — rank/order run inside the op-tree, this never re-sorts),
    `depth` caps how many nested dict levels (a `group`'s own section/arc/owner structure)
    get walked before collapsing to an honest count. None (the default, every existing
    caller) is a complete no-op — untouched items, unchanged shape, byte-identical to before
    this ruling existed.

    `offset` (task #149) skips N items before `take` — `take` alone can only ever show a
    list's FIRST N, forever; `take=50, offset=50` is page 2 of the SAME stable ordering the
    op-tree already produced. A 603-item composition used to force a caller into building
    dozens of hand-partitioned narrower compositions just to see past item 37 — this is the
    plain counter that replaces that workaround."""
    token = _ACL_CALLER.set(caller) if caller is not None else None
    try:
        res = await _eval(pool, spec, subject)
        items = await _package(pool, res)
    finally:
        if token is not None:
            _ACL_CALLER.reset(token)
    count = len(items) if isinstance(items, list | dict) else 1
    out: dict[str, Any] = {"composition": name, "kind": res.kind, "count": count,
                           "items": items, "spec": spec}
    if fields or take is not None or depth is not None or offset is not None:
        out["items"], dropped = _bound_items(items, fields=fields, take=take, depth=depth,
                                             offset=offset)
        if dropped:
            # `_projected`, never `_bounded` — `budget.fit()` (the LATER, generic backstop
            # every MCP tool result passes through, mcp_server.BoundedMCP.call_tool) writes
            # its OWN `_bounded` key if the response is STILL over budget after this; a
            # shared key name would let one silently clobber the other's honest receipt.
            out["_projected"] = {"tool": "run_composition", "note": (
                "you asked for a bounded view — this names exactly what's not shown, same "
                "as an unrequested trim would."), "dropped": dropped}
    return out


async def run_composition(
    pool: asyncpg.Pool, ref: str, subject: uuid.UUID | None = None,
    caller: str | None = None,
    *, fields: list[str] | None = None, take: int | None = None, depth: int | None = None,
    offset: int | None = None,
) -> dict[str, Any]:
    """Execute a saved composition (by name or id), optionally against a subject.
    `caller` = who is reading (the reflection ACL's input — see run_spec). `fields`/`take`/
    `depth`/`offset` — see run_spec; a roadmap-sized composition (task #64's own proof
    case: 61K chars, 53 threads, unbounded) can now be asked for narrow and small in one
    call instead of shipping whole and getting post-processed by hand — and `offset` pages
    past the first `take` instead of forcing a hand-partitioned rebuild per page."""
    spec = await _spec_of(pool, ref)
    if spec is None:
        return {"error": f"no composition {ref!r}"}
    return await run_spec(pool, spec, subject, name=ref, caller=caller,
                          fields=fields, take=take, depth=depth, offset=offset)


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
                "op": "table", "columns": [{"property": "summary"}, {"property": "kind"},
                                           {"property": "is_handoff"}],
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
                                           {"property": "kind"}, {"property": "is_handoff"}],
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


def _roadmap_status_group(status: str) -> dict[str, Any]:
    """arc -> owner over one project's threads at a given status — pure op-tree, no Function
    needed: closing a thread is itself a self_declared act (capture.py/dispose.py), so an
    untouched resolved/retracted thread cannot exist by construction — the echo-filter the
    OPEN section still needs (see `_fn_roadmap_open`) simply does not apply here."""
    return {
        "op": "group", "by": "arc", "body": {
            "op": "group", "by": "owner", "from": {"op": "these"}, "body": {
                "op": "table", "from": {"op": "these"},
                "columns": [{"property": "id"}, {"property": "summary"},
                           {"property": "kind"}]}},
        "from": {"op": "intersect", "sets": [
            {"op": "select", "object_type": "Thread",
             "where": [{"property": "status", "op": "eq", "value": status}]},
            {"op": "traverse", "from": {"op": "subject"}, "direction": "in",
             "link_type": "in_repo", "hops": 1}]},
    }


def _roadmap_open_group() -> dict[str, Any]:
    """arc -> owner over the OPEN bucket — task #60 (the function-output-re-entering-the-
    op-tree follow-on): `roadmap_open` still has to be a Function (the echo-filter is real
    evidence-provenance logic no `select` can express), but its output is no longer a
    dead-end leaf, so the arc/owner nesting is the SAME `group`-by-arc-then-owner op-tree
    `_roadmap_status_group` already uses for resolved/retracted — just sourced from a
    Function instead of a `select`. No `table` step needed at the leaf: `roadmap_open`'s
    own rows already carry exactly the columns a table step would fetch."""
    return {
        "op": "group", "by": "arc", "from": {"op": "function", "name": "roadmap_open"},
        "body": {"op": "group", "by": "owner", "from": {"op": "these"},
                 "body": {"op": "these"}},
    }


# THE MIGRATED ROADMAP (ruling c5b184cd, thread d56e7073/#44 — the composition-abstraction
# READ half, the proof case; task #60 restored `open` to a pure `group`-by-arc-then-owner
# tree too, once a Function's output could re-enter the op-tree). `sections` stays keyed by
# STATUS, not arc, at the TOP level — deliberately (Thoth, msg 1390): arc as the single
# top-level axis across all three statuses would need unioning a Function-sourced set with
# two `select`-sourced ones under one grouping, a bigger, structurally different change
# `group` consuming a function-node doesn't by itself imply. All three sections are now the
# SAME shape one level down — arc -> owner -> threads — `open` via `_roadmap_open_group`,
# `resolved`/`retracted` via `_roadmap_status_group`.
ROADMAP: dict[str, Any] = {
    "op": "sections",
    "sections": [
        {"title": "open", "body": _roadmap_open_group()},
        {"title": "resolved", "body": _roadmap_status_group("resolved")},
        {"title": "retracted", "body": _roadmap_status_group("retracted")},
    ],
}


# THE MIGRATED DOCS (ruling c5b184cd, thread d56e7073/#44) — simpler than roadmap, no
# Function needed at all: `topic` is a real stored property, one `group` level is the whole
# shape. `where: topic present` excludes an untopiced Reference entirely (deliberate,
# unchanged from docs.py's own note: `topic` is exactly what marks the seeded docs canon,
# never a catch-all bucket a plain fleet-wide Reference would fall into). The fixed section
# ORDER (getting-started/concepts/reference/deployment/history) was presentation policy
# living OUTSIDE the op-tree — app.py's /canon route re-sorted the returned dict by hand.
# Ruling d42c543b (Thoth msg 1926/1937): a route special-casing a composition's own output
# is exactly what the ruling refuses — `group`'s new `sequence` param moves the policy INTO
# the op-tree, where every reader of this composition (the route, /ui, a future consumer)
# gets the same order for free, and the route-level re-sort retires with it.
DOCS: dict[str, Any] = {
    "op": "group", "by": "topic",
    "sequence": ("getting-started", "concepts", "reference", "deployment", "history"),
    "from": {"op": "select", "object_type": "Reference",
             "where": [{"property": "topic", "op": "present"}]},
    "body": {"op": "table", "from": {"op": "these"},
             "columns": [{"property": "canonical"}, {"property": "name"},
                        {"property": "vendor"}]},
}


# THE LIVE DESK (ruling c5b184cd, thread d56e7073/#44) — "what's actionable for the operator
# right now," the wedge meant to end the briefs rot. Expressible TODAY with existing ops +
# Functions, no `group` needed — three flat sections. Resolved/stale fall out BY
# CONSTRUCTION: `status=open` in each select excludes them, nothing further to build.
# `decisions_awaiting_a_call` is a Function (fleet_messages isn't the object graph, so it
# can't be a pure op, same class as `desk_decisions`'s own docstring explains). `drift_alarms`
# depends on `severity` actually being stamped — today only `alarm_schema_drift` does.
#
# THE WRITE LEG (same ruling, DM 1374): `owed_to_you`/`drift_alarms` carry a `row_action`
# resolving a real thread — `resolve_thread`. `decisions_awaiting_a_call`'s own action
# (`settle`) is attached directly inside `_fn_desk_decisions` itself, not here: a Function's
# output is Python-native, no `row_action` templating applies to it.
_RESOLVE_ACTION = {"action": "resolve_thread", "args": {"ref": {"property": "id"}}}

LIVE_DESK: dict[str, Any] = {
    "op": "sections",
    "sections": [
        {"title": "owed_to_you", "body": {
            "op": "table", "columns": [{"property": "id"}, {"property": "summary"},
                                       {"property": "kind"}],
            "row_action": _RESOLVE_ACTION,
            "from": {"op": "order", "by": "recency", "dir": "desc", "from": {
                "op": "select", "object_type": "Thread", "where": [
                    {"property": "status", "op": "eq", "value": "open"},
                    {"property": "owner", "op": "eq", "value": "operator"}]}}}},
        {"title": "decisions_awaiting_a_call",
         "body": {"op": "function", "name": "desk_decisions"}},
        {"title": "drift_alarms", "body": {
            "op": "table", "columns": [{"property": "id"}, {"property": "summary"}],
            "row_action": _RESOLVE_ACTION,
            "from": {"op": "order", "by": "recency", "dir": "desc", "from": {
                "op": "select", "object_type": "Thread", "where": [
                    {"property": "status", "op": "eq", "value": "open"},
                    {"property": "severity", "op": "eq", "value": "alarm"}]}}}},
    ],
}


# THE FLEET STRIP (task #71 slice two, gated msg 1894/1897) — the PILOT proving "everything
# becomes a Composition" on the cheapest possible case: zero UI code, no view-type registry,
# no branch in renderWallView. Two Functions because neither leg is expressible as a pure op
# (liveness/seatedness are derived at read time from agent_mounts, not stored graph
# properties on Agent — see _fn_fleet_live_agents's own docstring). Ranked, never the wall
# the plain "fleet" composition already renders (every agent the graph knows, unfiltered).
FLEET_STRIP: dict[str, Any] = {
    "op": "sections",
    "sections": [
        {"title": "pulse", "body": {"op": "function", "name": "fleet_pulse_line"}},
        {"title": "live_agents", "body": {"op": "function", "name": "fleet_live_agents"}},
    ],
}


# THE MAIL OVERVIEW (task #71 consolidation wave 2, ruling d42c543b, msg 1929) — the
# overview-only half of /mail's port. `mail_threads` (registered as a Function, above) is
# NOT saved as its own composition here — it takes `args.box`, and a saved composition with
# a baked-in box would only ever show one fixed mailbox. THE DRILL-IN (task #90, Thoth msg
# 1976/2005): each row's own `row_action` runs mail_threads for THAT row's box via the
# "run:" navigation dispatch — a click switches the board to that box's threads, no manual
# args-input mechanism needed. Closes the gap this comment used to flag as open.
MAIL_OVERVIEW: dict[str, Any] = {
    "op": "function", "name": "mail_overview",
    "row_action": {"action": "run:mail_threads", "args": {"box": {"property": "box"}}},
}


DEFAULT_COMPOSITIONS: dict[str, dict[str, Any]] = {
    "operational-vs-disclosed-geography": GEOGRAPHY_DISCREPANCY,
    # the arrival briefing — a `sections` op-tree, no longer a hand-written Function.
    "briefing": BRIEFING,
    # the SCOPED briefing — orient's per-project bearings, subject = a SoftwareProject (#20).
    "project-briefing": PROJECT_BRIEFING,
    # the migrated roadmap (ruling c5b184cd, thread d56e7073/#44) — subject = a SoftwareProject.
    "roadmap": ROADMAP,
    # the migrated docs canon (ruling c5b184cd, thread d56e7073/#44) — no subject needed.
    "docs": DOCS,
    # the live desk (ruling c5b184cd, thread d56e7073/#44) — no subject needed.
    "live-desk": LIVE_DESK,
    # the fleet strip's migration pilot (task #71 slice two, msg 1894/1897) — not "fleet"
    # (taken: every agent the graph knows, unranked). No subject needed.
    "fleet-strip": FLEET_STRIP,
    # /fleet's full-fidelity port (rung 2, ruling d42c543b, msg 1926/1936) — ADDITIVE, the
    # route stays live beside this for a side-by-side look before anything retires. Neither
    # "fleet" (every agent ever seen, no liveness) nor "fleet-strip" (one project, live+
    # seated only) is this: soul-folding, doors/ancestors (flattened to prose — see
    # _fn_fleet_live's own docstring for what that means and why), the wake ledger, the
    # hourly budget, and the cross-project view all live here. No subject needed.
    "fleet-live": {"op": "function", "name": "fleet_live"},
    # /mail's overview half (consolidation wave 2, ruling d42c543b, msg 1929) — no subject
    # needed. mail_threads stays a Function only (no saved composition): see MAIL_OVERVIEW's
    # own comment for why a fixed-box composition isn't the right shape yet.
    "mail": MAIL_OVERVIEW,
    # /overhead's port (task #91, ruling d42c543b) — no subject needed; the harness-cost +
    # retained-telemetry read-model, one Function, two data sources (see _fn_overhead).
    "overhead": {"op": "function", "name": "overhead"},
    # /desk's landing roster (task #91, ruling d42c543b) — the ROSTER itself stays read-only
    # (counts, not individual debts — see _fn_desk_overview's own docstring). The write side
    # lives on desk_project, walked into with args.project (own docstring: done/not mine/
    # later on a debt, settle on an ask, embedded directly on each row). desk_project stays a
    # Function only, same shape as mail_threads/args.box — no saved composition.
    "desk": {"op": "function", "name": "desk_overview"},
    # the former bespoke read-models, now forkable compositions over named Functions —
    # opinion left engine code (no more hardcoded read-model + bespoke MCP tool per lens).
    "co-investment-ties": {"op": "function", "name": "coinvest"},
    "who-is-this": {"op": "function", "name": "subject_report"},
    "screen-financing-network": {"op": "function", "name": "screen_network"},
    # the dedicated canon view: the project's design memory (Palantir/Notion + own docs),
    # rendered as a sectioned read-model. Run with no subject; `consult_canon(q)` queries it.
    "design-canon": {"op": "function", "name": "canon", "args": {}},
    # the type catalog osiris SHIPS (task #111, thread 26694d10) — schema.py's static
    # manifest, deliberately NOT catalog.py's live accretive one (msg 2099). No subject
    # needed; also the pool-free source docs_compiler.py's REFERENCE.md render reads.
    "reference": {"op": "function", "name": "reference_catalog"},
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
    # task #138/#163's arc: this WAS the enumeration door already, but `name` alone isn't
    # addressable (no canonical/id to act on) and gave no way to tell a real repo from the
    # zero-commit/zero-file registry noise #152 named — `canonical` and `on_disk_path` are
    # both plain object columns / assertion properties already, so this is wiring, not
    # construction (measured live: 60 active SoftwareProjects, most 0 commits/0 files).
    "projects": {
        "op": "order", "by": "last_touched", "dir": "desc",
        "from": {
            "op": "table",
            "from": {"op": "select", "object_type": "SoftwareProject"},
            "columns": [
                {"name": "project", "property": "name"},
                {"name": "canonical", "property": "canonical"},
                {"name": "on_disk_path", "property": "on_disk_path"},
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
    # rung 2 (task #98): the census half of triage-as-a-primitive — types + counts + health,
    # the left pane the operator sketched. BUCKETS (the middle pane, per-type drill) needs
    # args.object_type per call, so it has no static saved composition — reach it via
    # run-spec/{"op":"function","name":"triage","args":{"mode":"buckets","object_type":...}}
    # or the `triage` MCP tool directly, the same ephemeral path mail_threads/desk_project use.
    "type-census": {"op": "function", "name": "triage"},
    # rung 2 (Thoth DM 2835/2917): the four numbers as a standing surface, fleet-wide by
    # default — how much of thread closure is held by structure vs memory. Per-project scope
    # is args.repo (no static saved composition for that, same reason `triage` buckets mode
    # has none — reach it via run-spec or the `closure_health` MCP tool directly).
    "closure-health": {"op": "function", "name": "closure_health"},
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
    "live-desk": ("wall", "what's actionable for the operator right now — owed, decisions, "
                          "drift alarms"),
    "fleet-strip": ("fleet", "live co-agents right now, ranked — not the wall 'fleet' already "
                             "renders"),
    "fleet-live": ("fleet", "the full roster: every project, souls folded by generation, "
                            "doors/ancestors, the wake ledger and hourly budget"),
    "mail": ("fleet", "every mailbox with traffic, busiest-latest first (overview only — "
                      "see mail_threads for one box)"),
    "desk": ("wall", "what you owe each project, oldest first — see desk_project for one "
                     "(overview only — same shape as mail/mail_threads)"),
    "open threads": ("wall", "the raw unresolved list (ungraded — prefer the-wall)"),
    "echoes": ("wall", "the triage pile: untouched miner echoes, oldest first"),
    "decision-log": ("memory", "every decision with its WHY; superseded entries grayed"),
    "design-canon": ("memory", "the design memory — ask it before re-deriving"),
    "reference": ("memory", "the type catalog osiris SHIPS — static and pool-free, never a "
                            "live accretive stub"),
    "docs": ("memory", "the docs canon by topic — getting-started, concepts, reference, "
                       "deployment, history"),
    "recent work": ("memory", "latest commits across the graph"),
    "changelog by area": ("memory", "what changed, grouped by area"),
    "fable-commits": ("memory", "commits authored by fleet sessions"),
    "the composer arc": ("memory", "the composer's own build history"),
    "fleet": ("fleet", "every agent the graph knows"),
    "projects": ("fleet", "all repos by recency of touch"),
    "project": ("fleet", "one repo's brief — focus a repo or pass args.repo"),
    "project-briefing": ("fleet", "a project's scoped briefing (what orient reads)"),
    "portfolio": ("fleet", "the operator's repos as a portfolio"),
    "roadmap": ("fleet", "a project's work map — open/resolved/retracted, arc then owner"),
    "graph-lint": ("engine", "the graph auditing itself — findings, not verdicts"),
    "type-census": ("engine", "every type's health — counts, orphans, thin, median links"),
    "closure-health": ("engine", "the four numbers: how much of thread closure is held by "
                                 "structure vs memory, fleet-wide"),
    "family-consistency": ("engine", "config families that should agree but don't"),
    "family-drift": ("engine", "how config families drift over time"),
    "lap": ("engine", "one object's provenance timeline — how belief formed"),
    "overhead": ("engine", "what the harness itself costs — hidden channels, cache vs "
                          "fresh, reminders, compactions, retained telemetry"),
    "operational-vs-disclosed-geography": ("casework", "where an org operates vs claims"),
    "co-investment-ties": ("casework", "who co-invests with the subject"),
    "who-is-this": ("casework", "the subject's dossier at a glance"),
    "screen-financing-network": ("casework", "the subject's financing network, screened"),
}

# AUTO-REFRESH (ruling cf9286b2): absent = MANUAL ONLY, the default for every composition not
# named here — a lens goes live because someone decided it should, never by inheriting a
# global tick. The ruling named "mail and the fleet strip want seconds; docs, design-canon
# and the decision log want never" explicitly, then Thoth extended it (msg 1977) to
# "fleet-live" on the same reasoning: refresh_secs belongs to a composition whose ANSWER
# GOES STALE, and who is alive right now is the most perishable fact in the graph — a fleet
# roster that must be manually re-run is a fleet roster that lies by default.
#
# 8s, not :8011's 5s copied by habit (that number was picked for an SSE PUSH lane's own
# server-side tick, a continuous connection — it carries no informational weight for a POLL
# interval). Measured instead (watermark.py's own docstring has the full numbers): the
# watermark query itself costs 0.071ms server-side, ~0.25ms round trip — a non-factor at any
# plausible tick rate, even with dozens of open tabs. The real constraint is UX: 8s is fast
# enough that a burst of new mail or a newly-mounted agent surfaces within one ordinary human
# glance, and slow enough to read as meaningfully different from a genuinely real-time push
# surface (:8011's inbox) — a poll dressed up as a stream would be dishonest about what it is.
_COMP_REFRESH_SECS: dict[str, int] = {
    "mail": 8,
    "fleet-strip": 8,
    "fleet-live": 8,
}


async def seed_default_compositions(pool: asyncpg.Pool) -> int:
    for name, spec in DEFAULT_COMPOSITIONS.items():
        section, desc = _COMP_META.get(name, (None, None))
        await save_composition(pool, name, spec, "lens", description=desc, section=section,
                               refresh_secs=_COMP_REFRESH_SECS.get(name))
    # the shelf also reaches saved, non-default lenses it knows by name (agent-authored
    # twins of the defaults) — metadata only, never their spec
    for name, (section, desc) in _COMP_META.items():
        if name not in DEFAULT_COMPOSITIONS:
            await pool.execute(
                "UPDATE compositions SET section=$2, description=$3 "
                "WHERE name=$1 AND section IS NULL", name, section, desc)
    return len(DEFAULT_COMPOSITIONS)
