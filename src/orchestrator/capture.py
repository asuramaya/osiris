"""Capture-at-source — decisions and open threads written back DURING a session.

The prosthesis test: a Claude session, given only the graph, orients from `briefing`,
does its work, and — before it dies — writes back what it DECIDED and what's still
OPEN, so the next session inherits instead of starting blind. Today the only way a
decision or a thread enters the graph is the regex miner (`src/ingest/{decisions,
threads}.py`), which reads them back OUT of a future commit. But the epochal decisions —
architecture pivots, rulings — happen in *conversation* and never land in a commit body
at all. This module is the missing write path: capture at the moment of deciding.

Same object SHAPE as the miner (type `Decision`/`Thread`, canonical `decision:<hash>` /
`thread:<hash>`, props summary/kind/status) so a captured item and a mined one render
identically in the `decision-log` / `briefing` compositions — nothing downstream has to
know which path minted it. Two things differ, and both are the evidence taxonomy doing
its job:

  * source is `session` (the deciding channel), not the miner's `git-memory`;
  * evidence class is SELF_DECLARED — the decider stating their OWN decision, which is
    strictly higher trust than the miner's DERIVED regex inference over prose. The miner
    is demoted to backfill: it fills in decisions the session forgot to capture.

A session decision has no commit to attach to, so where the miner links `decided_in` →
Commit, we link `in_repo` → the SoftwareProject directly (find-or-create on `repo:<name>`,
so a decision recorded before the repo is ingested pre-attaches to the eventual project).
The `decision-log` composition reads the decided_in rollup for its "in"/"when" columns, so
those render empty for a session decision — gracefully (verified in tests).
"""

from __future__ import annotations

import hashlib
import re
import uuid
from datetime import UTC, datetime, timedelta
from difflib import SequenceMatcher
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

# The session channel — distinct from the miner's `git-memory` so provenance reads true
# (a captured decision and a mined one coexist as a multi-source set on the same object).
_SOURCE = "session"
# SELF_DECLARED: the decider declaring their own decision — the taxonomy's highest-trust
# class, above the miner's DERIVED (a regex inference over commit prose).
_EC = EvidenceClass.SELF_DECLARED.value
_CONF = confidence_for(EvidenceClass.SELF_DECLARED)


def _canon(prefix: str, text: str) -> str:
    """The miner's exact canonical scheme, so a captured item dedups against a mined one
    with identical text (find-or-create idempotency) and renders in the same composition."""
    return f"{prefix}:{hashlib.sha1(text.encode()).hexdigest()[:12]}"


async def _resolve_repo(pool: asyncpg.Pool, name: str) -> uuid.UUID | None:
    """An active SoftwareProject by its `name` property or its `repo:<name>` canonical."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT o.id FROM objects o WHERE o.type='SoftwareProject' AND o.status='active' AND ("
        "  o.canonical = $1 OR o.canonical = $2 OR EXISTS ("
        "    SELECT 1 FROM current_assertions a WHERE a.object_id=o.id "
        "    AND a.name='name' AND a.value #>> '{}' = $3)) LIMIT 1",
        name, f"repo:{name}", name,
    )


async def link_repo(
    actions: Actions, obj_id: uuid.UUID, repo: str, observed: datetime,
    *, source: str = _SOURCE, evidence_class: str = _EC, confidence: float = _CONF,
) -> None:
    """Attach a captured Decision/Thread to its project. A session item has no commit, so
    it links `in_repo` → the SoftwareProject directly (the miner's `decided_in`→Commit→
    `in_repo` chain collapsed by one hop). Find-or-create on `repo:<name>` so the link
    always lands — and a decision recorded before the repo is ingested pre-attaches to the
    same object gitlog will later find-or-create. The edge is deduped (re-capture is a no-op).
    The session-miner reuses this with its own source + DERIVED grade — one implementation,
    two trust tiers (the same split capture/miner already have)."""
    name = repo.removeprefix("repo:").strip()
    proj = await _resolve_repo(actions.pool, name)
    if proj is None:  # a stub the eventual gitlog ingest will land on (same repo: canonical)
        proj = await actions.create_or_find_object("SoftwareProject", f"repo:{name}", source)
        await actions.assert_property(proj, "name", name, source, observed, confidence,
                                      evidence_class=evidence_class)
    exists = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='in_repo' LIMIT 1",
        obj_id, proj,
    )
    if not exists:
        await actions.create_link(obj_id, proj, "in_repo", source, observed, confidence,
                                  evidence_class=evidence_class)


async def record_decision(
    actions: Actions, summary: str, *, kind: str = "ruling",
    rationale: str | None = None, repo: str | None = None, source: str = _SOURCE,
    grounds: list[uuid.UUID] | None = None, protocol: str | None = None,
    supersedes: str | None = None, resolves: str | list[str] | None = None,
) -> uuid.UUID:
    """Capture a decision at the moment it is made — the WHY, declared, not mined.

    `kind` labels it the way the miner does (ruling / reset / override / rejection /
    choice / decision). `rationale`, if given, is the reasoning stored inline (an
    enrichment the miner can't produce — it only has the commit body). `repo` files the
    decision under a SoftwareProject. `source` is the attributing actor — the static
    `session` for a lone operator, or `agent:<session>` for a fleet member so provenance
    records WHICH instance decided (still SELF_DECLARED, still the high-trust channel).
    `grounds` cites the Reference objects the decision rests on — `grounded_by` edges
    minted AT BIRTH, so the citation carries the decider's grade instead of being
    reconstructed later from prose. Idempotent on the summary hash. Returns the id.

    `supersedes` BURIES an earlier decision under this one (the operator's ruling
    dd04d7dd, Tjmax III's ask): the old decision is stamped superseded_by/-because —
    property assertions, event-sourced, unwindable by re-asserting "" — and this one is
    stamped supersedes, so the correction navigates both ways. The lens does the graying:
    superseded decisions leave orient's recent list; the decision-log renders them with
    their successor. NEVER a delete — the wrong hypothesis stays readable under its
    correction. Raises ValueError when the ref matches nothing (the new decision is NOT
    recorded — a correction that can't name its target is not yet a correction).

    `resolves` CLOSES THE THREAD THIS DECISION ANSWERS, in the same act — mints `answers`
    and marks the thread resolved. Until this existed, capture had a one-way valve: the
    answer landed and the question stayed lit, because closing was a SEPARATE verb that a
    dying session forgets. The operator ruled on the lineage question on 2026-07-12; the
    decision recording that ruling announced "resolving thread 2f353b8e" IN PROSE, nothing
    in the code read the prose, and the graph went on asking him a question he had already
    answered for a full day (bug 59c8e47d). open_thread(question) → record_decision(answer)
    is the fleet's most common write; the close belongs INSIDE the answer, not beside it.
    A ruling that can name its question should not need a second verb to finish the sentence.
    Same strictness as `supersedes`: a ref that matches nothing raises, and NOTHING is
    recorded — a ruling that miscites the question it settles has not settled it.

    `resolves` also takes a LIST (§4.7, Maat's ask, ruling dd47c1da): a delegation folds
    the SET of threads it supersedes, not just one — "thread ownership doesn't transfer
    with a delegation" left her hand-closing threads twice, across two sessions, because
    the single-ref form could only ever name one. Each entry resolves INDEPENDENTLY through
    the same matcher as the single-ref form. Unlike the single ref, a list entry that
    matches nothing does NOT abort the call — it would defeat the point of folding a set to
    have one typo veto the other nine — but it is never swallowed either: the caller (the
    MCP tool) is the one that names, per entry, what closed and what didn't, since this
    function's return stays a bare id (additive shape only — ~20 existing call sites bind
    it as a plain UUID). A single STRING keeps the original strictness byte-for-byte:
    matches nothing → raises, nothing recorded."""
    observed = datetime.now(UTC)
    old: uuid.UUID | None = None
    if supersedes:
        old = await _find_decision(actions.pool, supersedes)
        if old is None:
            raise ValueError(f"supersedes matched no decision: {supersedes!r} — quote its "
                             "UUID, 8-char short id, or a summary substring")
    answered: list[uuid.UUID] = []
    if isinstance(resolves, list):
        for thread_ref in resolves:
            tid = await _find_thread(actions.pool, thread_ref)
            if tid is not None:
                answered.append(tid)
    elif resolves:
        single = await _find_thread(actions.pool, resolves)
        if single is None:
            raise ValueError(f"resolves matched no thread: {resolves!r} — quote its UUID, "
                             "8-char short id, or a summary substring")
        answered.append(single)
    # ONE transaction: the Decision, its summary/kind/rationale, and the repo link either all
    # land or none do — a process death mid-sequence can no longer leave a summary-less husk.
    async with actions.atomic() as a:
        d = await a.create_or_find_object("Decision", _canon("decision", summary), source)
        await a.assert_property(d, "summary", summary, source, observed, _CONF,
                                evidence_class=_EC)
        await a.assert_property(d, "kind", kind, source, observed, _CONF,
                                evidence_class=_EC)
        if rationale:
            await a.assert_property(d, "rationale", rationale, source, observed, _CONF,
                                    evidence_class=_EC)
        if protocol:
            # the INVOCATION, not just the conclusion (Anubis VIII, msg 236 — a sibling project's
            # biggest re-derivation class: a ruling that says what was found but not how
            # to reproduce it). Its own property, never folded into rationale: a protocol
            # buried in prose is a protocol lost.
            await a.assert_property(d, "protocol", protocol, source, observed, _CONF,
                                    evidence_class=_EC)
        if repo:
            await link_repo(a, d, repo, observed, source=source, evidence_class=_EC)
        for ref in grounds or []:
            exists = await a.pool.fetchval(
                "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='grounded_by'",
                d, ref)
            if not exists:  # re-capture is a no-op, like link_repo
                await a.create_link(d, ref, "grounded_by", source, observed, _CONF,
                                    evidence_class=_EC)
        if old is not None and old != d:  # a decision never buries itself (idempotent re-record)
            await a.assert_property(old, "superseded_by", str(d), source, observed, _CONF,
                                    evidence_class=_EC)
            await a.assert_property(old, "superseded_because",
                                    f"superseded by {str(d)[:8]}: {summary[:200]}",
                                    source, observed, _CONF, evidence_class=_EC)
            await a.assert_property(d, "supersedes", str(old), source, observed, _CONF,
                                    evidence_class=_EC)
        for thread_id in answered:
            # the ANSWER and the CLOSE in one transaction: a ruling that lands while its
            # question stays open is how the operator gets asked twice. Same shape the
            # resolve_thread verb writes (status/resolved_in/resolved_because), so every
            # lens that already renders a resolved thread renders this one unchanged. A
            # batch just runs this once per thread in the set — same act, same transaction.
            exists = await a.pool.fetchval(
                "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='answers'",
                d, thread_id)
            if not exists:
                await a.create_link(d, thread_id, "answers", source, observed, _CONF,
                                    evidence_class=_EC)
            await a.assert_property(thread_id, "status", "resolved", source, observed, _CONF,
                                    evidence_class=_EC)
            await a.assert_property(thread_id, "resolved_in", source, source, observed, _CONF,
                                    evidence_class=_EC)
            await a.assert_property(thread_id, "resolved_because",
                                    f"answered by decision {str(d)[:8]}: {summary[:200]}",
                                    source, observed, _CONF, evidence_class=_EC)
    return d


async def _find_decision(pool: asyncpg.Pool, ref: str) -> uuid.UUID | None:
    """A Decision by UUID, by short-id PREFIX, then by summary substring (shortest summary
    wins) — the same resolution ladder as _find_thread, and for the same reason: the fleet
    quotes decisions by 8-char short id inside other summaries, so the prefix leg must run
    before the text leg."""
    try:
        return uuid.UUID(ref)
    except (ValueError, AttributeError):
        pass
    short = (ref or "").strip().lower()
    if re.fullmatch(r"[0-9a-f]{8}[0-9a-f-]*", short):
        did = await pool.fetchval(
            "SELECT id FROM objects WHERE type='Decision' AND status='active' "
            "AND id::text LIKE $1 || '%' LIMIT 1", short)
        if did is not None:
            return uuid.UUID(str(did))
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT o.id FROM objects o JOIN current_assertions a ON a.object_id=o.id "
        "WHERE o.type='Decision' AND o.status='active' AND a.name='summary' "
        "AND a.value #>> '{}' ILIKE '%'||$1||'%' ORDER BY length(a.value #>> '{}') ASC LIMIT 1",
        ref,
    )


async def _thread_summary(pool: asyncpg.Pool, thread_id: uuid.UUID) -> str | None:
    """The WINNING `summary` for a thread — used to name what a batch resolve closed."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1",
        thread_id,
    )


# PRIOR-ART SURFACING (thread 44635c42, task #67, from the re-derivation post-mortem):
# a ruling contradicting standing law must not mint frictionlessly. Canonical failure this
# prevents: decision 636a8648 minted in direct contradiction of naming-v3 (a882b334) with
# zero friction — the operator caught it, the verb didn't. `search`'s own fused engine
# (lexical + semantic doors) is topical, not lexical — the exact property needed here, since
# a contradicting ruling rarely reuses its predecessor's wording. `via` in ('id', 'both')
# means an independent SECOND door corroborated the match (an id-exact hit, or both the
# textual and semantic doors agreeing) — that agreement, not a magic rank cutoff, is what
# "strong" means, so the flag doesn't need recalibrating as the corpus grows.
_PRIOR_ART_STRONG_VIA = ("id", "both")


_PRIOR_ART_KINDS = frozenset({"Decision"})
# THE THAW (ruling 1e6d7367): the unified check widens past Decision-only, Imhotep's own
# deliberately-left-open plug (decision 5640f234) — every write-path caller that wants the
# fuller corpus passes this instead of the (still-default, backward-compatible) bare set.
UNIFIED_PRIOR_ART_KINDS = frozenset({"Decision", "Practice", "Superstition"})


def prior_art_from_hits(
    hits: list[dict[str, Any]], *, exclude: set[uuid.UUID] | None = None, limit: int = 5,
    kinds: frozenset[str] = _PRIOR_ART_KINDS,
) -> list[dict[str, Any]]:
    """Shape a `search()` result into a record_decision/record_practice receipt's
    `prior_art` — standing, non-buried hits of the given `kinds` only (default Decision-
    only, unchanged for existing callers; pass UNIFIED_PRIOR_ART_KINDS for the THE THAW's
    unified check over {Decisions, Practices, Superstitions}). Excludes the item just
    recorded and any explicit `supersedes`/`refutes` target — those are already handled by
    that verb, naming them again as "prior art" would just be noise. A `superseded`
    Decision or a `refuted` Practice is dead testimony for THIS purpose (it no longer
    stands for anything a new record could be redundant with), so both are excluded here
    even though search() itself still surfaces them, flagged, for direct lookup. LOUD,
    NEVER A REFUSAL (the SPOF principle): this only shapes data for the receipt to
    display — the caller decides whether a hit is strong enough to flag."""
    exclude_s = {str(e) for e in (exclude or set())}
    out: list[dict[str, Any]] = []
    for h in hits:
        if (h.get("type") not in kinds or h.get("id") in exclude_s
                or h.get("superseded") or h.get("refuted")):
            continue
        out.append({"id": str(h["id"])[:8], "type": h.get("type"),
                    "summary": h.get("snippet") or "",
                    "grade": h.get("grade"), "via": h.get("via")})
        if len(out) >= limit:
            break
    return out


def prior_art_is_strong(prior_art: list[dict[str, Any]]) -> bool:
    """Does the TOP prior-art hit warrant the loud flag ('a standing ruling covers this
    ground — supersede it explicitly or cite it')? See `prior_art_from_hits` for why
    cross-door agreement, not a rank number, is the bar."""
    return bool(prior_art) and prior_art[0].get("via") in _PRIOR_ART_STRONG_VIA


# CONTRADICT vs RE-DERIVE (PRACTICE v2 layer 1, Thoth LXII's DM 1785; grounds c54e8176 +
# thread 54a5c842): a strong hit against a standing Practice gets the SAME "looks like a
# re-derivation" nudge today whether the new decision merely restates the Practice or
# silently reverses it — the exact failure c54e8176 traces (a fixed mistake recurring,
# caught only because a write happened to fire the check at all; Alfred X's framing in
# 54a5c842: the missing property is "asserted at the point of application"). Telling the
# two apart needs no semantic classifier: a reversal leaves a lexical fingerprint
# (negation/override language) a plain restatement does not. This is a HEURISTIC, not a
# verdict — an empty cue list is not proof of agreement, only that this fingerprint is
# absent; the flag stays a nudge a mind reviews, never a block (the SPOF principle).
_CONTRADICTION_CUES = (
    "never", "don't", "do not", "doesn't", "does not", "stop", "instead of",
    "rather than", "no longer", "avoid", "skip", "reverse", "abandon", "override",
    "opposite", "wrong to", "should not", "shouldn't", "must not", "mustn't", "not to",
)


def practice_contradiction_cues(text: str) -> list[str]:
    """Which contradiction-flavored cue phrases appear in `text` (case-insensitive
    substring match, deterministic, no NLP). See `_CONTRADICTION_CUES` for why this is a
    lexical fingerprint check, not an entailment classifier."""
    low = text.lower()
    return [cue for cue in _CONTRADICTION_CUES if cue in low]


def _ref_slug(title: str) -> str:
    """ref:<slug> — the SAME canonical scheme as the doc ingester (src/ingest/reference.py),
    so an agent citing "Attention Is All You Need" and a later doc-ingest of the same title
    find-or-create ONE node instead of twins."""
    return "ref:" + re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")


async def ingest_reference(
    actions: Actions, title: str, *, source_url: str | None = None,
    vendor: str | None = None, body: str | None = None, caveats: str | None = None,
    repo: str | None = None, source: str = _SOURCE,
    cites: list[uuid.UUID] | None = None,
) -> tuple[uuid.UUID, str]:
    """An agent turns something it READ into a first-class Reference node (Soundwave VI's
    ask, obligation ecc8d58e): a paper, a vendor doc, a spec — findable by search, linkable
    by `grounded_by`, instead of narrated into free text and lost.

    `caveats` is deliberately its OWN property, never folded into `body`: "but only under
    X" buried in prose is a caveat lost — a theorem that TIGHTENS rather than confirms must
    survive as exactly that. `cites` wires paper→paper lineage (`cites` edges to other
    Reference ids) so a literature tree is walkable, not re-derived. Graded SELF_DECLARED:
    the agent testifying to what it read (the read is first-hand; the paper's CLAIMS keep
    their own grade in `body`/`caveats` prose). Idempotent on the title slug. Returns
    (id, canonical)."""
    observed = datetime.now(UTC)
    canon = _ref_slug(title)
    async with actions.atomic() as a:
        ref = await a.create_or_find_object("Reference", canon, source)
        await a.assert_property(ref, "name", title, source, observed, _CONF,
                                evidence_class=_EC)
        for prop, value in (("source_url", source_url), ("vendor", vendor),
                            ("body", body), ("caveats", caveats)):
            if value:
                await a.assert_property(ref, prop, value, source, observed, _CONF,
                                        evidence_class=_EC)
        if repo:
            await link_repo(a, ref, repo, observed, source=source, evidence_class=_EC)
        for cited in cites or []:
            exists = await a.pool.fetchval(
                "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='cites'",
                ref, cited)
            if not exists:
                await a.create_link(ref, cited, "cites", source, observed, _CONF,
                                    evidence_class=_EC)
    return ref, canon


# THE OPEN-THREAD DEDUP (two field witnesses, Aegis and Maat): the same fact got minted
# TWICE across a lineage restart because the summary differed slightly the second telling —
# `_canon`'s exact-hash idempotency only catches a byte-identical repeat. Conservative on
# purpose: a false merge silently drops testimony (worse than a duplicate a human can fold).
_DEDUP_SIM = 0.60  # first-pass estimate (no live desk to measure against, unlike mailbox.py's
                    # calibrated 0.30 "same story" bar) — recalibrate if it over/under-fires.


def _normalize_for_dedup(text: str) -> str:
    """Case/punctuation/whitespace-flattened form — the miner's own near-dup guard
    (`sessions.py::_normalized`), reused here so a trivial rewording (case, punctuation, a
    trailing clause) is caught without even reaching the similarity check below."""
    return " ".join(re.sub(r"[^a-z0-9]+", " ", text.lower()).split())


async def _pg_trgm_enabled(pool: asyncpg.Pool) -> bool:
    """Is pg_trgm actually installed on THIS database? CHECK, don't assume: sessions.py's own
    comment ('pg_trgm is not installed, so there is no trigram similarity to lean on') went
    stale the day migration 0025 landed the extension — mailbox.py has used similarity() ever
    since. A local catalog lookup, not a network round trip."""
    return bool(await pool.fetchval(
        "SELECT 1 FROM pg_extension WHERE extname='pg_trgm'"))


async def find_near_duplicate_open_thread(
    pool: asyncpg.Pool, summary: str, *, repo: str | None,
) -> uuid.UUID | None:
    """An existing OPEN thread on this project that is the SAME fact as `summary`, reworded —
    or None. Checked BEFORE minting (open_thread's caller): a normalized exact match first
    (case/punctuation/whitespace), then a conservative similarity check over that project's
    open threads — pg_trgm's `similarity()` when the database has the extension, else a
    Python-side ratio on the same small candidate set. No `repo` means no safe scope to dedup
    against, so it stands down rather than guess fleet-wide."""
    if not repo:
        return None
    proj = await _resolve_repo(pool, repo.removeprefix("repo:").strip())
    if proj is None:
        return None
    rows = await pool.fetch(
        "SELECT o.id, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS summary "
        "FROM objects o JOIN links l ON l.from_id=o.id AND l.type='in_repo' AND l.to_id=$1 "
        "WHERE o.type='Thread' AND o.merged_into IS NULL AND o.status='active' "
        "  AND COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
        "   WHERE a.object_id=o.id AND a.name='status' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1),'open')='open'",
        proj)
    candidates = [(r["id"], r["summary"]) for r in rows if r["summary"]]
    if not candidates:
        return None
    norm_new = _normalize_for_dedup(summary)
    for tid, cand in candidates:
        if _normalize_for_dedup(cand) == norm_new:
            return uuid.UUID(str(tid))
    if await _pg_trgm_enabled(pool):
        ids = [tid for tid, _ in candidates]
        bodies = [cand for _, cand in candidates]
        hit = await pool.fetchval(
            "WITH b AS (SELECT unnest($1::uuid[]) AS id, unnest($2::text[]) AS body) "
            "SELECT id FROM b WHERE similarity(body, $3) > $4 "
            "ORDER BY similarity(body, $3) DESC LIMIT 1",
            ids, bodies, summary, _DEDUP_SIM)
        return uuid.UUID(str(hit)) if hit is not None else None
    best_id, best_ratio = None, 0.0
    for tid, cand in candidates:
        ratio = SequenceMatcher(None, norm_new, _normalize_for_dedup(cand)).ratio()
        if ratio > best_ratio:
            best_id, best_ratio = tid, ratio
    return uuid.UUID(str(best_id)) if best_id is not None and best_ratio > _DEDUP_SIM else None


# THE ROADMAP ARC TAXONOMY (thread 8df8e611, Thoth's locked list, msg 1299) — CLOSED on
# purpose: a free-text `arc` would fragment silently (a typo is a new, permanently-empty
# arc no thread ever finds again), so `open_thread` refuses anything outside this set
# rather than accepting a drifting label. roadmap.py imports this SAME constant for its
# section order — one taxonomy, never two copies that quietly disagree.
ARCS = ("Identity-Succession", "Compaction-Resilience", "Model-Identity", "Token-Cost",
        "Surfaces-Roadmap-Docs", "Fleet-Hygiene", "Security")


async def open_thread(
    actions: Actions, summary: str, *, repo: str | None = None, kind: str | None = None,
    owner: str | None = None, assignee: str | None = None, arc: str | None = None,
    severity: str | None = None, source: str = _SOURCE,
) -> uuid.UUID:
    """Open a thread at source — an unresolved question / next-step for the next session
    to inherit. Same shape as a mined Thread (props summary + status=open) so it appears in
    `briefing`'s open-threads section beside mined ones. Idempotent on the summary hash.

    `kind='obligation'` marks the obligations class (ruling 7336c5fc): a DUTY minted by an
    action ("kernel changed → daemons need restart") — neither a ruling nor ordinary work,
    exactly the thing that used to die with the context window. Same Thread shape, so it
    surfaces in briefing beside the rest; the kind stays as data for filtering. `source`
    attributes the opening actor (a fleet agent vs the lone `session`).

    `owner` says WHOSE MOVE it is (two grievance witnesses: 'mine to act' vs 'waiting on
    the human' were illegible on the wall): 'operator' = blocked on the human's word or
    hands; 'agent:<id>' = a specific mind; a bare project name = any hand on that project.
    Unowned = anyone who reads it may act. The lens sorts by it; the record just keeps it.

    `assignee` (alfred's ask 5, ruling dd47c1da §4.3 — "single-assignee leased
    obligations") is the seat/agent this BUILD belongs to, one build one assignee. It is
    NOT a second field: `owner` already IS "whose move it is", and two properties naming
    the same fact is the bug this avoids, so `assignee` stamps the SAME `owner` property
    (assignee wins if both are given) — orient's sort-by-owner needs no change. What's new
    is ENFORCEMENT, not storage: the caller (mcp_server.open_thread) checks
    find_near_duplicate_open_thread BEFORE minting and, on a hit, surfaces the EXISTING
    lease + its holder instead of minting a parallel build — see that tool's docstring.

    `arc` (thread 8df8e611, roadmap v2) names which of the CLOSED taxonomy (`ARCS`,
    above) this thread belongs to — the roadmap screen's top-level grouping, one level
    above `status`. Raises ValueError on anything outside `ARCS`: a locked taxonomy that
    silently accepted typos would fragment into permanently-empty arcs nobody finds
    again. Omitted (the common case for now) leaves the thread arc-less; the roadmap
    composition's own open half (`compositions._fn_roadmap_open`) buckets those as
    "unsorted" rather than guessing.

    `severity` (ruling c5b184cd, thread d56e7073/#44 — the live-desk composition's
    drift_alarms leg) names an alarm-shaped open in a real, filterable property instead
    of text-matching a summary for "DRIFT"/"CRITICAL". Deliberately UNLOCKED (unlike
    `arc`) — Thoth's ask was the property, not a whole new taxonomy; the first (and so
    far only) real caller is `deploy_guard.alarm_schema_drift`, stamping `"alarm"`."""
    if arc is not None and arc not in ARCS:
        raise ValueError(f"arc must be one of {ARCS}, got {arc!r}")
    observed = datetime.now(UTC)
    effective_owner = assignee if assignee is not None else owner
    # ONE transaction (see record_decision): Thread + summary + status(+kind)(+repo) atomic —
    # never a status-less or summary-less thread husk from a mid-sequence death.
    async with actions.atomic() as a:
        t = await a.create_or_find_object("Thread", _canon("thread", summary), source)
        await a.assert_property(t, "summary", summary, source, observed, _CONF,
                                evidence_class=_EC)
        await a.assert_property(t, "status", "open", source, observed, _CONF,
                                evidence_class=_EC)
        if kind:
            await a.assert_property(t, "kind", kind, source, observed, _CONF,
                                    evidence_class=_EC)
        if arc:
            await a.assert_property(t, "arc", arc, source, observed, _CONF,
                                    evidence_class=_EC)
        if severity:
            await a.assert_property(t, "severity", severity, source, observed, _CONF,
                                    evidence_class=_EC)
        if effective_owner:
            await a.assert_property(t, "owner", effective_owner.strip(), source, observed,
                                    _CONF, evidence_class=_EC)
        if repo:
            await link_repo(a, t, repo, observed, source=source, evidence_class=_EC)
    return t


async def _current_owner(pool: asyncpg.Pool, thread_id: uuid.UUID) -> str | None:
    """The WINNING `owner` value for a thread (grade DESC, then recency — the same
    resolution `open_thread_wall` already reads). Used to NAME the holder of an existing
    lease when a near-duplicate obligation surfaces instead of minting a parallel build."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='owner' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1",
        thread_id,
    )


async def _find_thread(pool: asyncpg.Pool, ref: str) -> uuid.UUID | None:
    """A Thread by UUID, by short-id PREFIX, then by summary substring (shortest summary
    wins — closest to the query). The prefix leg runs BEFORE summary text because the fleet
    quotes threads by their 8-char short id INSIDE other summaries: '5c57f54d' must resolve
    to thread 5c57f54d-…, never to whichever thread's summary happens to mention it (that
    mis-resolve closed the wrong obligation on 2026-07-10)."""
    try:
        return uuid.UUID(ref)
    except (ValueError, AttributeError):
        pass
    short = (ref or "").strip().lower()
    if re.fullmatch(r"[0-9a-f]{8}[0-9a-f-]*", short):
        tid = await pool.fetchval(
            "SELECT id FROM objects WHERE type='Thread' AND status='active' "
            "AND id::text LIKE $1 || '%' LIMIT 1", short)
        if tid is not None:
            return uuid.UUID(str(tid))
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT o.id FROM objects o JOIN current_assertions a ON a.object_id=o.id "
        "WHERE o.type='Thread' AND o.status='active' AND a.name='summary' "
        "AND a.value #>> '{}' ILIKE '%'||$1||'%' ORDER BY length(a.value #>> '{}') ASC LIMIT 1",
        ref,
    )


# the triage verbs' kinds (ruling 758ded94): adopt = obligation (owed work, testimony),
# question = remembered but unowned (ranked out of the work wall), task = ordinary thread.
_TRIAGE_KINDS = ("obligation", "question", "task")


async def reclassify_thread(
    actions: Actions, ref: str, *, kind: str, because: str | None = None,
    owner: str | None = None, source: str = _SOURCE,
) -> uuid.UUID | None:
    """Triage a thread WITHOUT lying about its state (ruling 758ded94: untouched ≠ resolved).
    Reclassification is TESTIMONY — a mind read the thread and judged what it IS: adopt a
    miner echo as real work (kind='obligation'), demote a promoted question back to a
    question (kind='question'), or mark it an ordinary task. The status is untouched: a
    question stays OPEN in the record; the LENS ranks it out of the work wall. SELF_DECLARED
    (outranks the miner's DERIVED kind), event-sourced, reversible. Returns the thread id,
    or None if `ref` matched nothing. `owner` optionally CLAIMS the thread in the same act
    (see open_thread) — triage is where an existing thread learns whose move it is."""
    if kind not in _TRIAGE_KINDS:
        raise ValueError(f"kind must be one of {_TRIAGE_KINDS}")
    tid = await _find_thread(actions.pool, ref)
    if tid is None:
        return None
    observed = datetime.now(UTC)
    await actions.assert_property(tid, "kind", kind, source, observed, _CONF,
                                  evidence_class=_EC)
    if owner:
        await actions.assert_property(tid, "owner", owner.strip(), source, observed, _CONF,
                                      evidence_class=_EC)
    if because:
        await actions.assert_property(tid, "reclassified_because", because, source, observed,
                                      _CONF, evidence_class=_EC)
    return tid


async def _find_artifact(pool: asyncpg.Pool, artifact: str) -> uuid.UUID | None:
    """Resolve an artifact pointer to the graph object it names: an exact canonical
    ('commit:abc123def456', 'decision:…'), an object UUID or 8-char short id (Decision or
    Commit only — the closer types), or a bare git hash (prefix-matched on commit:).
    None for free-form pointers (a file:line, a path) — the resolved_artifact property
    alone carries those; a pointer that matches nothing must never block the close."""
    a = artifact.strip()
    oid = await pool.fetchval("SELECT id FROM objects WHERE canonical=$1", a)
    if oid is not None:
        return uuid.UUID(str(oid))  # exact canonical — any precisely-named type may close
    if re.fullmatch(r"[0-9a-f]{8}(-[0-9a-f-]{4,28})?", a.lower()):
        rows = await pool.fetch(
            "SELECT id FROM objects WHERE id::text LIKE $1 || '%' "
            "AND type IN ('Decision', 'Commit') LIMIT 2", a.lower()[:8])
        if len(rows) == 1:  # ambiguity → property-only, never a guessed edge
            return uuid.UUID(str(rows[0]["id"]))
    if re.fullmatch(r"[0-9a-f]{7,40}", a.lower()):
        rows = await pool.fetch(
            "SELECT id FROM objects WHERE type='Commit' "
            "AND canonical LIKE 'commit:' || $1 || '%' LIMIT 2", a.lower())
        if len(rows) == 1:
            return uuid.UUID(str(rows[0]["id"]))
    return None


async def resolve_thread(
    actions: Actions, ref: str, *, because: str | None = None,
    artifact: str | None = None, source: str = _SOURCE
) -> uuid.UUID | None:
    """Close a thread at source — the session marking a question answered, so it leaves the
    open list and joins the resolved section. Matches the miner's self-heal shape
    (status=resolved + resolved_in + resolved_because) so the `briefing` resolved section
    renders it, with `resolved_in='session'` recording that a session closed it rather than
    a later commit. `ref` is a Thread UUID or a summary substring. Event-sourced via a status
    assertion that supersedes the prior 'open' within-source — never a DELETE. Returns the
    thread id, or None if `ref` matched nothing.

    `artifact` (thread 022bd24a, Ferryman II: `because` was being abused as a completion
    essay because there was nowhere to put "here is what actually got built") is a POINTER
    to the thing that closed the thread — a commit hash, a decision id, a file:line. It is
    always kept as the resolved_artifact property, and when it names a graph object
    (Decision, Commit, or any exact canonical) a resolved_by edge is minted too — the
    strong closure witness the closure-miner almost never finds (e27f7c3)."""
    tid = await _find_thread(actions.pool, ref)
    if tid is None:
        return None
    observed = datetime.now(UTC)
    await actions.assert_property(tid, "status", "resolved", source, observed, _CONF,
                                  evidence_class=_EC)
    await actions.assert_property(tid, "resolved_in", source, source, observed,
                                  _CONF, evidence_class=_EC)
    if because:
        await actions.assert_property(tid, "resolved_because", because, source, observed,
                                      _CONF, evidence_class=_EC)
    if artifact:
        await actions.assert_property(tid, "resolved_artifact", artifact.strip(), source,
                                      observed, _CONF, evidence_class=_EC)
        target = await _find_artifact(actions.pool, artifact)
        if target is not None and not await actions.pool.fetchval(
                "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 "
                "AND type='resolved_by' LIMIT 1", tid, target):
            await actions.create_link(tid, target, "resolved_by", source, observed, _CONF,
                                      evidence_class=_EC)
    return tid


async def assign_thread(
    actions: Actions, ref: str, *, owner: str, because: str | None = None,
    source: str = _SOURCE,
) -> uuid.UUID | None:
    """HAND A THREAD BACK — reassign whose move it is, WITHOUT closing it (operator, 2026-07-11:
    "no good way to resolve these debts per thread or per project, so it snowballs into
    infinity"). A debt on the human's queue had exactly two exits — he does it, or it rots —
    so everything he was ever cc'd on accumulated on him forever. This is the third door:
    owner='<project>' pushes the duty back to the hands that actually own it, where orient()
    surfaces it on THAT project's wall at its next mount. Nobody dispatches; the graph does.

    Not a resolve and never pretends to be (untouched ≠ resolved, 758ded94): status is
    untouched, the debt stays OPEN in the record — it simply stops being HIS. Reversible
    (assign it back), event-sourced, SELF_DECLARED when the operator's own click signs it."""
    tid = await _find_thread(actions.pool, ref)
    if tid is None:
        return None
    observed = datetime.now(UTC)
    await actions.assert_property(tid, "owner", owner.strip(), source, observed, _CONF,
                                  evidence_class=_EC)
    if because:
        await actions.assert_property(tid, "assigned_because", because, source, observed,
                                      _CONF, evidence_class=_EC)
    return tid


async def defer_thread(
    actions: Actions, ref: str, *, days: int, because: str | None = None,
    source: str = _SOURCE,
) -> uuid.UUID | None:
    """SNOOZE — the debt is real, owned, and NOT NOW. Stamps `deferred_until`; the LENS hides
    it from the queue/wall until that date, then it returns on its own. The fourth door, and
    the honest one for "yes, mine, but not this month": the alternative the operator actually
    had was leaving it on a red desk to be re-read and re-skipped every single day, which is
    how a queue stops being read at all.

    Fix at the LENS, never at the record: the thread stays OPEN and owned; only its VISIBILITY
    moves. A deferral is testimony with an expiry — it can never silently become a resolve."""
    tid = await _find_thread(actions.pool, ref)
    if tid is None:
        return None
    observed = datetime.now(UTC)
    until = observed + timedelta(days=max(1, days))
    await actions.assert_property(tid, "deferred_until", until.date().isoformat(), source,
                                  observed, _CONF, evidence_class=_EC)
    if because:
        await actions.assert_property(tid, "deferred_because", because, source, observed,
                                      _CONF, evidence_class=_EC)
    return tid


async def record_reflection(
    actions: Actions, body: str, *, summary: str | None = None,
    repo: str | None = None, source: str = _SOURCE,
) -> uuid.UUID:
    """Keep a memory lived for its own sake — the HOME the operator ruled for (bfb3ae26,
    the panopticon seam's positive half): existential/philosophical conversation that is
    'not exactly work tickets... simply memories lived with my agents.' A Reflection is
    its OWN type, so every work surface structurally cannot present it as a ticket: it is
    not a Thread (nothing to resolve), not a Decision (nothing settled), not a candidate
    (the extractor's fourth rule already refuses first-person-about-the-speaker). It is
    remembered, attributed, queryable — and never actionable. Idempotent on the body."""
    observed = datetime.now(UTC)
    r = await actions.create_or_find_object("Reflection", _canon("reflection", body), source)
    await actions.assert_property(r, "body", body, source, observed, _CONF, evidence_class=_EC)
    await actions.assert_property(r, "summary", summary or body[:160], source, observed,
                                  _CONF, evidence_class=_EC)
    if repo:
        await link_repo(actions, r, repo, observed, source=source, evidence_class=_EC,
                        confidence=_CONF)
    return r


async def record_tension(
    actions: Actions, pole_a: str, pole_b: str, *, lean: str | None = None,
    why: str | None = None, repo: str | None = None, source: str = _SOURCE,
) -> uuid.UUID:
    """Hold a live TENSION — two positions in productive tension, neither settled. Unlike
    record_decision (which SETTLES) or open_thread (which CLOSES), a tension is HELD: the
    current `lean` and `why` are captured, but the object is never auto-resolved or consolidated
    away — because it is its own type, grade-resolution and dedup structurally cannot flatten it.
    Re-record the same poles to MOVE the lean; the lean assertion history is the dance across
    sessions. Idempotent on the unordered pole pair (so (a,b) and (b,a) are one tension)."""
    observed = datetime.now(UTC)
    key = "||".join(sorted((pole_a, pole_b)))  # unordered: the pair, not the order, is identity
    t = await actions.create_or_find_object("Tension", _canon("tension", key), source)
    await actions.assert_property(t, "pole_a", pole_a, source, observed, _CONF, evidence_class=_EC)
    await actions.assert_property(t, "pole_b", pole_b, source, observed, _CONF, evidence_class=_EC)
    if lean:
        await actions.assert_property(t, "lean", lean, source, observed, _CONF, evidence_class=_EC)
    if why:
        await actions.assert_property(t, "lean_why", why, source, observed, _CONF,
                                      evidence_class=_EC)
    if repo:
        await link_repo(actions, t, repo, observed, source=source, evidence_class=_EC,
                        confidence=_CONF)
    return t


async def record_blind_spot(
    actions: Actions, surface: str, cannot_see: str, *, verify_with: str | None = None,
    repo: str | None = None, source: str = _SOURCE,
) -> uuid.UUID:
    """Register a project's KNOWN BLIND SPOT — what this project's harness/rig CANNOT verify
    from here, and where the real verification lives (thread 8e26cd10, Ferryman II: 459
    headless-Chromium tests stayed green while every iPhone was broken; 'the most expensive
    thing I re-derived was not a fact — it was the shape of my own ignorance'). A BlindSpot
    is its own type, held like a Tension: a stable per-project fact that dedup and
    grade-resolution structurally cannot flatten away, surfaced at orient() so a session
    knows what it cannot see BEFORE it trusts a green harness. Idempotent per
    (repo, surface) — re-record to sharpen the wording; the assertion history keeps every
    telling. `surface` names the capability ('webkit-rendering', 'ios-touch'); `cannot_see`
    states the gap; `verify_with` points at the rig or ritual that actually verifies."""
    observed = datetime.now(UTC)
    key = f"{(repo or '').removeprefix('repo:').strip()}||{surface.strip().lower()}"
    b = await actions.create_or_find_object("BlindSpot", _canon("blindspot", key), source)
    await actions.assert_property(b, "surface", surface.strip(), source, observed, _CONF,
                                  evidence_class=_EC)
    await actions.assert_property(b, "cannot_see", cannot_see, source, observed, _CONF,
                                  evidence_class=_EC)
    if verify_with:
        await actions.assert_property(b, "verify_with", verify_with, source, observed, _CONF,
                                      evidence_class=_EC)
    if repo:
        await link_repo(actions, b, repo, observed, source=source, evidence_class=_EC,
                        confidence=_CONF)
    return b


async def kill_superstition(
    actions: Actions, statement: str, *, killed_by: str, repo: str | None = None,
    source: str = _SOURCE,
) -> uuid.UUID:
    """Put a WORKAROUND on the record as DEAD — the fix that landed names the practice it
    obsoletes (thread a9be40c9, Atlas's own will: a bug spawns workarounds; the workarounds
    are written into letters, succession notes and agent memory across the fleet; the bug
    gets FIXED; the workarounds persist as inherited law, taxing every heir forever). A
    Superstition is a first-class object so the kill is searchable forever; orient
    announces recent kills fleet-wide (recent_dead_superstitions) so any mind whose memory
    carries the practice strikes it. `statement` is the workaround AS IT PROPAGATES (quote
    the words agents actually inherit, e.g. 'NEVER DM BY NAME'); `killed_by` points at the
    fix (a decision id, a commit hash). Idempotent on the normalized statement."""
    observed = datetime.now(UTC)
    key = " ".join(statement.split()).lower()
    s = await actions.create_or_find_object("Superstition", _canon("superstition", key), source)
    await actions.assert_property(s, "statement", statement.strip(), source, observed, _CONF,
                                  evidence_class=_EC)
    await actions.assert_property(s, "killed_by", killed_by, source, observed, _CONF,
                                  evidence_class=_EC)
    await actions.assert_property(s, "killed_at", observed.isoformat(), source, observed,
                                  _CONF, evidence_class=_EC)
    if repo:
        await link_repo(actions, s, repo, observed, source=source, evidence_class=_EC,
                        confidence=_CONF)
    return s


async def _find_practice(pool: asyncpg.Pool, ref: str) -> uuid.UUID | None:
    """A Practice by UUID, by short-id PREFIX, then by `statement` substring (shortest
    statement wins) — same resolution ladder as `_find_decision`/`_find_thread`."""
    try:
        return uuid.UUID(ref)
    except (ValueError, AttributeError):
        pass
    short = (ref or "").strip().lower()
    if re.fullmatch(r"[0-9a-f]{8}[0-9a-f-]*", short):
        pid = await pool.fetchval(
            "SELECT id FROM objects WHERE type='Practice' AND status='active' "
            "AND id::text LIKE $1 || '%' LIMIT 1", short)
        if pid is not None:
            return uuid.UUID(str(pid))
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT o.id FROM objects o JOIN current_assertions a ON a.object_id=o.id "
        "WHERE o.type='Practice' AND o.status='active' AND a.name='statement' "
        "AND a.value #>> '{}' ILIKE '%'||$1||'%' ORDER BY length(a.value #>> '{}') ASC LIMIT 1",
        ref,
    )


async def _witness_link(
    actions: Actions, practice_id: uuid.UUID, evidence_id: uuid.UUID,
    source: str, observed: datetime,
) -> bool:
    """Mint `witnesses` (Practice -> Decision/Commit/Thread) idempotently — one witness is
    a hunch, four is law (Alfred IX's own words). NEVER minted from a mere search-topical
    match: only an explicit caller (record_practice's `witnesses=`, record_decision's
    `confirms=`) creates one, the same discipline grounds/obsoletes/supersedes already
    follow. Returns whether a NEW link was minted (false = already witnessed, a no-op)."""
    exists = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='witnesses'",
        practice_id, evidence_id)
    if exists:
        return False
    await actions.create_link(practice_id, evidence_id, "witnesses", source, observed, _CONF,
                              evidence_class=_EC)
    return True


async def practice_confirmed_count(pool: asyncpg.Pool, practice_id: uuid.UUID) -> int:
    """`confirmed` is DERIVED, never a stored/incremented scalar — the count of `witnesses`
    links at read time. An incremented-on-write counter would need read-then-write-under-
    lock, the exact race class thread dc9d1eed found live in bridged_seat/
    record_bridge_anchor; a link COUNT can never desync from the links it counts."""
    n = await pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND type='witnesses'", practice_id)
    return int(n or 0)


async def record_practice(
    actions: Actions, statement: str, *, failure_prevented: str | None = None,
    surface: str | None = None, repo: str | None = None,
    witnesses: list[uuid.UUID] | None = None, source: str = _SOURCE,
) -> uuid.UUID:
    """Capture a TRANSFERABLE TECHNIQUE — Superstition's positive twin (operator ruling
    1e6d7367, from Alfred IX's filing msg 1418: the graph could hold what to STOP believing
    but nothing held engineering technique that outlives any single repo or date, so two
    houses re-derived the same lesson independently in the same hour). `statement` is the
    imperative one-liner (e.g. 'arm before you seal — one ceremony, not two'); `failure_
    prevented` is the concrete symptom that makes it findable MID-FAILURE, not just on
    reflection; `surface` reuses BlindSpot's domain vocabulary. Timeless — never moment-
    stamped, true regardless of repo or date, unlike a Decision. `witnesses` links the
    Decisions/Commits/Threads that are this Practice's evidence AT BIRTH; `confirms=` on a
    LATER record_decision call is how a re-encounter adds one more (see practice_confirmed_
    count — `confirmed` is that link count, never a separate stored number). Idempotent on
    the normalized statement."""
    observed = datetime.now(UTC)
    key = " ".join(statement.split()).lower()
    p = await actions.create_or_find_object("Practice", _canon("practice", key), source)
    await actions.assert_property(p, "statement", statement.strip(), source, observed, _CONF,
                                  evidence_class=_EC)
    if failure_prevented:
        await actions.assert_property(p, "failure_prevented", failure_prevented, source,
                                      observed, _CONF, evidence_class=_EC)
    if surface:
        await actions.assert_property(p, "surface", surface, source, observed, _CONF,
                                      evidence_class=_EC)
    if repo:
        await link_repo(actions, p, repo, observed, source=source, evidence_class=_EC,
                        confidence=_CONF)
    for w in witnesses or []:
        await _witness_link(actions, p, w, source, observed)
    return p


async def mint_implements(
    actions: Actions, from_decision: uuid.UUID, to_decision: uuid.UUID, source: str = _SOURCE,
) -> bool:
    """This Decision is a SPECIFIC EXECUTION of that standing ruling (thread 169398d6,
    prior_art_flag's third path) — the parent stays alive, unlike supersedes. Idempotent:
    returns whether a NEW link was minted."""
    exists = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='implements'",
        from_decision, to_decision)
    if exists:
        return False
    await actions.create_link(from_decision, to_decision, "implements", source,
                              datetime.now(UTC), _CONF, evidence_class=_EC)
    return True


async def acknowledge_prior_art(
    actions: Actions, decision_id: uuid.UUID, prior_art_id: str, source: str = _SOURCE,
) -> None:
    """'Related standing law, reviewed, no action needed' as a GRAPH EVENT (thread
    169398d6's small-stage fix), not a shrug swallowed in prose — the third path
    prior_art_flag's two-verb (supersede-or-cite) prompt was missing."""
    await actions.assert_property(decision_id, "prior_art_acknowledged", prior_art_id,
                                  source, datetime.now(UTC), _CONF, evidence_class=_EC)


async def refute_practice(
    actions: Actions, practice_ref: str, *, killed_by: str, repo: str | None = None,
    source: str = _SOURCE,
) -> dict[str, uuid.UUID] | None:
    """THE POLARITY FLIP (ruling 1e6d7367's lifecycle clause): a Practice REFUTED converts
    to a Superstition — same family, same kill-verb (`kill_superstition`), reusing the
    Practice's OWN statement so the dead workaround is searchable under the exact words it
    propagated as. The Practice itself is never retired: it stays ACTIVE carrying
    `refuted_by`, because a half-remembered refuted lesson is exactly the thing that must
    stay findable — surfaced WITH the flag, not erased. Returns None (no write) when
    `practice_ref` matches no Practice — same all-or-nothing strictness as `supersedes`/
    `resolves`: a refutation that can't name its target has not refuted anything."""
    pool = actions.pool
    pid = await _find_practice(pool, practice_ref)
    if pid is None:
        return None
    observed = datetime.now(UTC)
    statement = await pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='statement' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", pid)
    await actions.assert_property(pid, "refuted_by", killed_by, source, observed, _CONF,
                                  evidence_class=_EC)
    sid = await kill_superstition(actions, statement or practice_ref, killed_by=killed_by,
                                  repo=repo, source=source)
    return {"practice": pid, "superstition": sid}


async def recent_dead_superstitions(
    pool: asyncpg.Pool, *, days: int = 14, limit: int = 5,
) -> list[dict[str, str]]:
    """The kills worth announcing — superstitions put down within the window, newest first.
    FLEET-WIDE by design: a workaround replicates across houses (Anubis X was spreading
    'stop using names' to a second house before the fix even shipped), so the announcement
    must not stop at a project boundary. Bounded and aging-out: orient speaks the recent
    dead, search remembers them all forever."""
    rows = await pool.fetch(
        "WITH latest AS ("
        "  SELECT DISTINCT ON (a.object_id, a.name) a.object_id, a.name, "
        "         a.value #>> '{}' AS val, a.observed_at "
        "  FROM current_assertions a JOIN objects o ON o.id = a.object_id "
        "  WHERE o.type = 'Superstition' AND o.status = 'active' "
        "  ORDER BY a.object_id, a.name, a.confidence DESC, a.observed_at DESC) "
        "SELECT s.val AS statement, k.val AS killed_by, s.observed_at "
        "FROM latest s JOIN latest k ON k.object_id = s.object_id AND k.name = 'killed_by' "
        "WHERE s.name = 'statement' AND s.observed_at > now() - ($1 || ' days')::interval "
        "ORDER BY s.observed_at DESC LIMIT $2", str(days), limit)
    return [{"statement": r["statement"], "killed_by": r["killed_by"]} for r in rows]


# the words Ferryman listed (thread 022bd24a) plus the N/N shape — deliberately narrow:
# a nag that fires on every ruling teaches everyone to ignore it
_MEASUREMENT = re.compile(
    r"(?i)\b(verified|verification|probe[ds]?|sweep(s|ed)?|swept|benchmark\w*|"
    r"sampl(e[ds]?|ing)|threshold\w*|seed(ed|s)?)\b|\b\d+\s*/\s*\d+\b")


def measurement_smell(text: str) -> bool:
    """Does a decision's text read like a MEASUREMENT? (thread 022bd24a: `protocol` is
    record_decision's best field and nothing asks for it — a verification recipe recorded
    without its invocation is exactly the re-derivation class the field exists to kill.)
    Used by the record_decision tool to NAG for an empty protocol — advice in the response,
    never a gate: the decision records either way."""
    return bool(_MEASUREMENT.search(text))


async def divergent_leans(pool: asyncpg.Pool) -> dict[str, str]:
    """Tensions where two minds' CURRENT leans disagree — keyed by the tension's canonical,
    valued with the confession line the lens must speak (task #53, from the tension-vs-
    resolver audit c7041c53: the assertion set honestly keeps BOTH leans, but any single-
    winner reader silently shows one — orient must say 'two minds lean apart' instead).
    Per-source current lean = that source's latest; divergence = >1 distinct value among
    them. Report-only: nothing here resolves anything (a Tension is HELD by design)."""
    rows = await pool.fetch(
        "WITH per_source AS ("
        "  SELECT DISTINCT ON (a.object_id, a.source_id) "
        "         a.object_id, a.source_id, a.value #>> '{}' AS lean "
        "  FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "  WHERE o.type='Tension' AND o.status='active' AND a.name='lean' "
        "  ORDER BY a.object_id, a.source_id, a.observed_at DESC) "
        "SELECT (SELECT o2.canonical FROM objects o2 WHERE o2.id=p.object_id) AS canon, "
        "       array_agg(p.source_id || ' leans ' || quote_literal(p.lean) "
        "                 ORDER BY p.source_id) AS voices "
        "FROM per_source p GROUP BY p.object_id "
        "HAVING count(DISTINCT p.lean) > 1")
    return {str(r["canon"]): "two minds lean apart: " + "; ".join(r["voices"][:4])
            for r in rows}


async def set_lifecycle(
    actions: Actions, project: str, lifecycle: str, *, because: str | None = None,
    source: str = _SOURCE,
) -> uuid.UUID | None:
    """HALT A PROGRAM — the operator kills a project by name, and the graph should hear it.

    A halted project's threads are REAL YIELD ON A PAUSED TREE: not garbage (so the janitor must
    never sweep them — the miner did its job, and the work was genuine), and not debt (so no lens
    may count them). 333 of them — 257 in one, 78 in another — were inflating every number in
    the system after the operator had explicitly stopped both programs. A memory that cannot hear
    "we stopped doing that" will keep billing you for it forever.

    This is TESTIMONY, not a guess: the human said it, an agent records it, and the lens obeys.
    Reversible by construction — set it back to 'active' and every thread returns exactly as it
    was. Nothing is deleted, nothing is swept, nothing is lost. `lifecycle`: active | halted.
    """
    if lifecycle not in ("active", "halted"):
        raise ValueError(f"lifecycle must be 'active' or 'halted', not {lifecycle!r}")
    pid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE type='SoftwareProject' AND canonical=$1", f"repo:{project}")
    if pid is None:
        return None
    now = datetime.now(UTC)
    await actions.assert_property(pid, "lifecycle", lifecycle, source, now, _CONF,
                                  evidence_class=_EC)
    if because:
        await actions.assert_property(pid, "lifecycle_because", because, source, now, _CONF,
                                      evidence_class=_EC)
    return pid  # type: ignore[no-any-return]
