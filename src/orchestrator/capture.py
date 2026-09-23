"""Capture-at-source: decisions and open threads written back during a session.

The goal: a session, given only the graph, orients from `briefing`, does its work, and
before it ends, writes back what it DECIDED and what's still OPEN, so the next session
inherits instead of starting blind. Previously the only way a decision or a thread
entered the graph was the regex miner (`src/ingest/{decisions,threads}.py`), which reads
them back out of a future commit. But important decisions, architecture pivots, rulings,
happen in conversation and never land in a commit body at all. This module is the
missing write path: capture at the moment of deciding.

Same object SHAPE as the miner (type `Decision`/`Thread`, canonical `decision:<hash>` /
`thread:<hash>`, props summary/kind/status) so a captured item and a mined one render
identically in the `decision-log` / `briefing` compositions: nothing downstream has to
know which path minted it. Two things differ, and both are the evidence taxonomy doing
its job:

  * source is `session` (the deciding channel), not the miner's `git-memory`;
  * evidence class is SELF_DECLARED: the decider stating their OWN decision, which is
    strictly higher trust than the miner's DERIVED regex inference over prose. The miner
    is demoted to backfill: it fills in decisions the session forgot to capture.

A session decision rarely has a commit to attach to at the moment it's stated: a ruling
usually precedes the work it justifies, so where the miner links `decided_in` -> Commit, we
link `in_repo` -> the SoftwareProject directly (find-or-create on `repo:<name>`, so a
decision recorded before the repo is ingested pre-attaches to the eventual project). The
`decision-log` composition reads the decided_in rollup for its "in"/"when" columns, so those
render empty for a session decision with no cited commit, gracefully (verified in tests).

When a decision IS recorded after the fact, landed, gated, and cited in its own prose
("commit 238b48f"), `record_decision` mints `decided_in` too, straight from that citation
(`_cited_commit_shas`/`_resolve_commit` below), the same edge the miner would eventually
add by reading it back out of the commit body, just without waiting on a mining pass that
never runs over session capture at all.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import uuid
from datetime import UTC, datetime, timedelta
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import asyncpg

from src.actions.core import ActionError, Actions
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

logger = logging.getLogger("osiris.capture")

# The session channel: distinct from the miner's `git-memory` so provenance reads true
# (a captured decision and a mined one coexist as a multi-source set on the same object).
_SOURCE = "session"
# SELF_DECLARED: the decider declaring their own decision, the taxonomy's highest-trust
# class, above the miner's DERIVED (a regex inference over commit prose).
_EC = EvidenceClass.SELF_DECLARED.value
_CONF = confidence_for(EvidenceClass.SELF_DECLARED)

# THE OPERATOR, AS A REAL OBJECT: a Person with the operator role and a canonical the
# literal 'operator' resolves to, so nothing else accretes on a bare string. THE LITERAL
# STRING 'operator' IS UNCHANGED EVERYWHERE ELSE: resolve_owner_seat
# (owner_normalization.py), _OPERATOR_ACTORS (seats.py), and every other reader/writer of
# the bare string keep working exactly as before; this object is an additive identity a
# caller can attach for real provenance (a ruled_by edge), never a replacement for the
# alias. Canonical `person:operator` matches Person's own existing scheme (schema.py:
# "person:", already used for OSINT identity-hub subjects).
_OPERATOR_PERSON_CANONICAL = "person:operator"


async def ensure_operator_person(actions: Actions, source: str = _SOURCE) -> uuid.UUID:
    """Idempotent find-or-create for the operator's own Person object (see the module
    comment above this function). Safe to call from any mint path that wants to attach
    real operator provenance: never mints twice (create_or_find_object's own
    (type, canonical) uniqueness)."""
    pid = await actions.create_or_find_object("Person", _OPERATOR_PERSON_CANONICAL, source)
    await actions.assert_property(pid, "role", "operator", source, datetime.now(UTC),
                                  _CONF, evidence_class=_EC)
    return pid


# THE OTHER HALF OF THE ACTOR/SOURCE DISTINCTION: `ensure_operator_person` above gives
# the operator-attribution family a real target; this gives the module's own bare
# 'session' default one too, an attributionless, automated write with no specific Agent
# or Person behind it. ONE singleton, `system:automated-closure`, distinct from a made-up
# Agent canonical (the old placeholder shape this replaces).
_SYSTEM_SOURCE_CANONICAL = "system:automated-closure"


async def ensure_system_source(actions: Actions, source: str = _SOURCE) -> uuid.UUID:
    """Idempotent find-or-create for the singleton SystemSource object standing in for
    an attributionless/automated write (see the module comment above this function).
    Never mints twice (create_or_find_object's own (type, canonical) uniqueness)."""
    sid = await actions.create_or_find_object("SystemSource", _SYSTEM_SOURCE_CANONICAL,
                                               source)
    await actions.assert_property(sid, "role", "automated-closure-default", source,
                                  datetime.now(UTC), _CONF, evidence_class=_EC)
    return sid


# Most rulings already cite the commit they landed in, in plain prose ("commit
# 238b48f", "Commit: 238b48f."); this is the only thing standing between that text and a
# real `decided_in` edge. Requires the word "commit(s)" immediately before the hex token
# (word boundary through an optional ":"/"#" and whitespace) so it never mistakes a
# decision/thread short id or a UUID fragment quoted nearby (e.g. "decision <hex id>")
# for a commit: those are never preceded by the word "commit".
_COMMIT_CITATION_RE = re.compile(r"\bcommits?\b\s*[:#]?\s*([0-9a-f]{7,40})\b", re.IGNORECASE)


def _cited_commit_shas(*texts: str | None) -> list[str]:
    """Every distinct sha cited as "commit <sha>" across the given texts, in first-seen
    order. Case-normalized to lowercase (git shas are lowercase hex; a citation typed in
    caps should still resolve)."""
    seen: dict[str, None] = {}
    for text in texts:
        if not text:
            continue
        for m in _COMMIT_CITATION_RE.finditer(text):
            seen.setdefault(m.group(1).lower(), None)
    return list(seen)


def _canon(prefix: str, text: str) -> str:
    """The miner's exact canonical scheme, so a captured item dedups against a mined one
    with identical text (find-or-create idempotency) and renders in the same composition."""
    return f"{prefix}:{hashlib.sha1(text.encode()).hexdigest()[:12]}"


def _thread_canon(summary: str, repo: str | None) -> str:
    """`open_thread`'s own identity key, REPO-SCOPED. Live-reproduced defect: two
    `open_thread(SAME summary, repo="A")` / `(..., repo="B")` calls minted ONE Thread
    object, silently in_repo-linked to both, because the bare `_canon("thread",
    summary)` this replaced hashed the summary text alone, a project dimension nothing
    else in the identity carried. `find_near_duplicate_open_thread`'s own fuzzy pre-check
    IS correctly repo-scoped (verified reading it) and its own docstring's claim ("no
    repo means no safe scope to dedup against") was true for THAT function: the defect
    lived one layer down, in the exact-match mint path underneath it, which the docstring
    never described and a project-scoped test never exercised (test_dedup_never_crosses_
    a_project_boundary calls the fuzzy checker directly, never open_thread itself, for
    its second project).

    `repo=None` keeps the old bare-text hash unchanged (an unfiled thread has no scope to
    protect, same law the fuzzy checker already applies), so this only changes behavior
    for the case that was actually broken, and every already-minted `repo=None` Thread's
    canonical still matches. A `repo=` thread minted before this fix will NOT match its
    own old canonical on a repeat call after upgrading: a one-time, unavoidable re-mint on
    next touch, not a silent divergence (the old cross-project sharing was the bug;
    ceasing to reproduce it is the fix). Normalizes the same way `link_repo`/
    `_resolve_repo` do (`repo:` prefix stripped) so `repo="osiris"` and
    `repo="repo:osiris"` still collide onto the same object, exactly as the rest of the
    repo-handling in this module already treats them as the same name."""
    if not repo:
        return _canon("thread", summary)
    return _canon("thread", f"{repo.removeprefix('repo:').strip()}\x00{summary}")


async def _resolve_commit(pool: asyncpg.Pool, sha: str) -> uuid.UUID | None:
    """A cited sha almost never matches a Commit's canonical byte-for-byte: gitlog.py
    stores `commit:<sha[:12]>` (a 12-char prefix) while rulings typically cite git's
    conventional 7-char short form ("commit 238b48f"). Prefix-match instead of
    exact-match: `sha[:12]` bounds the LIKE pattern at the stored canonical's own length,
    so neither a short 7-char citation nor a full 40-char paste ever over- or
    under-shoots it. READ-ONLY, unlike `link_repo`'s repo stub: a repo name is a small,
    guessable, eventually-real set worth pre-attaching to; a mistyped or not-yet-ingested
    sha is not, so silently skipping (never minting a property-less ghost Commit) is the
    deliberate choice here."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT id FROM objects WHERE type='Commit' AND canonical LIKE 'commit:' || $1 || '%' "
        "LIMIT 1",
        sha[:12],
    )


# PROSE-ID -> EDGE (a derivation lane over prose citations): measured 3,170 active osiris
# Decision+Thread objects, 37.5% carry at least one recoverable citation, 2,115
# recoverable edges, zero same-type collisions at this exact 8-hex length across 9,590
# objects. Same qualifier-word discipline as `_COMMIT_CITATION_RE` above: the qualifier
# must sit immediately before the hex token, so a bare 8-hex string quoted nearby for
# some other reason never gets mistaken for a citation (verified with a negative-control
# test). The qualifier word also names which type to resolve against: "ruling"/"decision"
# -> Decision, "obligation"/"thread" -> Thread, so a citation is never guessed across
# type, only ever resolved against what its own prose claimed, or skipped.
_PROSE_ID_QUALIFIER_TYPES = {
    "decision": "Decision", "decisions": "Decision",
    "ruling": "Decision", "rulings": "Decision",
    "thread": "Thread", "threads": "Thread",
    "obligation": "Thread", "obligations": "Thread",
}
_PROSE_ID_CITATION_RE = re.compile(
    r"\b(decisions?|rulings?|threads?|obligations?)\b\s*[:#]?\s*([0-9a-f]{8})\b",
    re.IGNORECASE)


def _cited_object_refs(*texts: str | None) -> list[tuple[str, str]]:
    """Every (claimed type, 8-hex short id) pair cited as "decision <id>"/"ruling <id>"/
    "thread <id>"/"obligation <id>" across the given texts, in first-seen order, deduped
    on the (type, id) pair: the exact same shape `_cited_commit_shas` already proved
    safe, ported rather than re-invented."""
    seen: dict[tuple[str, str], None] = {}
    out: list[tuple[str, str]] = []
    for text in texts:
        if not text:
            continue
        for m in _PROSE_ID_CITATION_RE.finditer(text):
            key = (_PROSE_ID_QUALIFIER_TYPES[m.group(1).lower()], m.group(2).lower())
            if key not in seen:
                seen[key] = None
                out.append(key)
    return out


async def _resolve_cited_object(
    pool: asyncpg.Pool, claimed_type: str, short_id: str,
) -> tuple[uuid.UUID | None, str | None]:
    """Resolve strictly against the type the citation's own qualifier word claimed,
    reusing `_find_decision`/`_find_thread` (one resolver family, not a second
    extraction path beside them), `require_identifier=True` so an 8-hex-shaped citation
    refuses rather than falls through to a fuzzy text match. Matches the UUID PREFIX, NOT
    THE CANONICAL: an earlier pass matched the canonical hash and undercounted 27x before
    this was caught. `_resolve_ref`'s short-id leg already matches `o.id::text LIKE $2 ||
    '%'`, the actual citation scheme in use, so this needed no new SQL at all, only
    reusing the right existing leg.

    Returns `(id, None)` on a clean match. Returns `(None, reason)` on anything else,
    never a guess across type: a claimed-Decision id that resolves to nothing is checked
    against Thread too, so the skip reason names a real type mismatch when that's what
    happened, distinct from a plain not-found."""
    finder = _find_decision if claimed_type == "Decision" else _find_thread
    try:
        hit = await finder(pool, short_id, require_identifier=True)
    except RefAmbiguous:
        return None, f"ambiguous — {short_id} matches more than one {claimed_type}"
    if hit is not None:
        return hit, None
    other_type = "Thread" if claimed_type == "Decision" else "Decision"
    other_finder = _find_thread if claimed_type == "Decision" else _find_decision
    try:
        other_hit = await other_finder(pool, short_id, require_identifier=True)
    except RefAmbiguous:
        other_hit = None
    if other_hit is not None:
        return None, (f"qualifier said {claimed_type} but {short_id} resolves to a "
                      f"{other_type} instead — skipped, never guessed")
    return None, f"{short_id} not found as a {claimed_type} (or any other known type)"


async def _object_source(pool: asyncpg.Pool, obj_id: uuid.UUID) -> str | None:
    """Best-effort proxy for 'who wrote this': the source that asserted the object's
    own `summary` (every Decision/Thread has exactly one). Used only to flag a citation
    as self-referential, to keep that population countable separately from real
    cross-author structure, the same discipline `_EXTENSION_LINK_PENDING_REASON` already
    applies to the hatch. Never load-bearing for resolution itself."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT source_id FROM current_assertions WHERE object_id=$1 AND name='summary' "
        "ORDER BY confidence DESC, observed_at DESC LIMIT 1", obj_id)


async def mint_cites(
    actions: Actions, from_id: uuid.UUID, to_id: uuid.UUID, source: str,
    *, self_referential: bool, origin: str,
) -> bool:
    """This object's own prose named that one: a declaration recorded in text, not a
    similarity guess, so SELF_DECLARED (same tier `noted_in`/`decided_in` already use
    for an author's own citation). Distinct link type from `mint_bears_on`'s own
    `answers` edge (Decision->Thread only, semantically "settled", governed by a
    no-auto-act rule on the tested `bears_on=` kwarg): a bare prose mention is a weaker,
    more general claim than "this speaks to that open question", and the prior-art
    promotion below needs Practice/Superstition targets `answers` was never shaped for.
    Reuses `cites` (Reference->Reference, ingest_reference's own `cites=`) rather than
    inventing new vocabulary, the same word already used for "my own text points at that
    object", broadened to legally connect Decision/Thread on either end. Verified against
    the live graph before widening: every existing `cites` edge is Reference->Reference,
    evidence_class self_declared, empty properties. domain/range is advisory only (no
    reader in this codebase filters on it), so nothing about an existing edge
    reinterprets.

    `self_referential` (an author citing their own earlier work, vs. citing someone
    else's) and `origin` (a PROSE-DERIVED cite, a regex match against free text, and a
    DECLARED one, a caller naming an exact target on purpose via `ingest_reference`'s own
    `cites=` or an explicit `acknowledge_prior_art` confirmation, are different
    confidence shapes even at the same SELF_DECLARED grade, and must stay queryable
    apart, not merely inferable from context) are both recorded on the link's own
    properties: a measured split of 35.5% self-ref, 64.5% cross-author is exactly the
    number `self_referential` keeps honest going forward. Idempotent: returns whether a
    new link was minted; never a self-loop."""
    if from_id == to_id:
        return False
    exists = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='cites'",
        from_id, to_id)
    if exists:
        return False
    await actions.create_link(from_id, to_id, "cites", source, datetime.now(UTC), _CONF,
                              evidence_class=_EC,
                              properties={"self_referential": self_referential,
                                        "origin": origin})
    return True


# LANE 0: measured 92.8% of Decisions and 92.3% of Threads get every link they will ever
# have inside 60 seconds of birth, so nothing in osiris walks back over an
# already-minted, unlinked object and links it. THE RULE, BINARY, NO DIAL: a derived edge
# may be written with no human in the loop IFF the lookup that produces it returns
# exactly one answer. Anything else, zero candidates, two or more, is written as
# nothing, never a low-confidence guess. This keeps a clean line between what is
# derivable (mechanically deterministic) and what is merely guessed.
#
# ONE SHARED KERNEL (one guard, not three differently-shaped guards on one class): every
# orphan-healing lane (Agent-by-session, Decision-by-lineage-root, and whatever the third
# lane turns out to need) resolves its own lane-specific lookup, that part cannot be
# unified without knowing what each lane actually joins on, and hands the resulting
# candidate list here for the one part that IS shared: the cardinality check, the
# mint-or-abstain decision, the evidence tier, and the durable, queryable record of why a
# write did not happen. This guards against a mistake made twice in one day before this
# kernel existed: claiming 219 orphan Agents were derivable by shared session (one
# session held 232 linked agents, not unique) and 203 orphan Decisions by lineage root (7
# of 23 roots span 2-4 projects, not unique). Both times a join confirmed a story and
# nobody asked whether it returned one row or many; this function is that question,
# asked mechanically, every time.
_DERIVE_TIER = EvidenceClass.DIRECT_OBSERVATION
_DERIVE_CONF = confidence_for(_DERIVE_TIER)


async def derive_or_abstain(
    actions: Actions, from_id: uuid.UUID, link_type: str, candidates: list[uuid.UUID],
    source: str, *, why_if_ambiguous: str | None = None, retried: bool = False,
) -> dict[str, Any]:
    """`candidates` is the caller's own lane-specific lookup, already run: this
    function never queries for them itself, so it stays the same one primitive
    regardless of what a lane joins on. Tier is DIRECT_OBSERVATION (0.6), not
    DERIVED (0.4) and not SELF_DECLARED (0.9): a cardinality-1 join over facts the
    graph already asserts is deterministic, not a probabilistic guess (what DERIVED
    means everywhere else in this module, the miner's regex inference over prose);
    but no author typed this specific relationship or chose its link-type word either
    (the same reasoning behind grading spawned_by links as direct_observation, an
    identical precedent set by a mount-derived repo= tier fix). `properties={"origin":
    "derived"}` on every edge this mints, the same `cites` origin=prose|declared marker,
    generalized, so a future census can always tell a mechanically-derived edge from a
    caller-declared or prose-cited one, never merely infer it.

    NO CONFIDENCE PARAMETER EXISTS ON THIS FUNCTION, DELIBERATELY: that enforces the
    binary rule above, not an omission: a caller cannot lower the bar, because there
    is no dial to turn. `len(candidates) == 1` mints (idempotent: checks the link
    doesn't already exist first). Anything else, zero or two-or-more, mints nothing
    and records why as a durable, queryable `derivation_abstained_<link_type>` property
    on `from_id` (same shape `prose_citation_skips`/`unlinked_because` already use,
    namespaced by link_type so a second lane abstaining on the same object under a
    different link_type never clobbers the first lane's own record; assert_property's
    own last-write-wins would otherwise silently lose it): `why_if_ambiguous`, when
    given, names the caller's own reason (e.g. "session holds 232 linked agents, not
    1"); omitted, a generic candidate-count reason is recorded instead, either way,
    never a silent drop. THE CANDIDATE IDS THEMSELVES ARE KEPT, not just the count: an
    abstention is future work, not a dead end. A later pass that revisits an unresolved
    lookup arrives with the shortlist already computed instead of re-deriving it
    against a graph that has moved on, the same hand-off shape `unlinked_because`
    already proved. Recovering a discarded candidate set later is strictly harder
    than keeping it now.

    `retried=True` ("derived at write time" and "derived nine hours later when ingest
    caught up" are different provenance facts a reader may care about) stamps
    `properties["retried"]=True` on the minted edge: the caller's own declaration that
    this call is a re-attempt at a previously zero-candidate abstention (see
    `retryable_abstentions` below for the structurally-safe door onto that population),
    never inferred here.

    A successful mint that lands on an object carrying a live (unresolved)
    `derivation_abstained_<link_type>` property SUPERSEDES it with a `resolved` marker
    (`{"link_type", "resolved": True, "resolved_to": <this to_id>}`); otherwise a
    resolved case would sit in `retryable_abstentions`' own queue forever, the exact
    detector-without-a-door failure this whole lane exists to fix, just moved one step
    over. CROSS-SOURCE, DELIBERATELY (the same trap an earlier backfill_lineage_
    repo_links fix found and fixed for its own mint path, now fixed here at the shared
    root): a retry runs under whatever actor re-derived it, almost never the original
    abstention's own writer, and `assert_property`'s supersession is
    same-source-only: a same-source re-assert would leave the stale, different-source
    row "current" beside the new resolved marker instead of retiring it. This uses
    `supersede_assertion`, the one legitimate cross-source door, against every live row
    found (never just the first). A fresh cardinality-1 success with no prior abstention
    on record (the common case) writes nothing extra: this is a correction to a stale
    record, not a receipt every mint owes.

    Returns `{"minted": bool, "to": uuid|None, "abstained": bool, "reason": str|None,
    "candidate_count": int, "candidates": list[uuid.UUID]}`, the caller's own receipt,
    not durable state by itself (the durable half is the property write on `from_id`
    when abstaining, and the link itself when minting)."""
    if len(candidates) == 1:
        to_id = candidates[0]
        exists = await actions.pool.fetchval(
            "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type=$3",
            from_id, to_id, link_type)
        if not exists:
            props: dict[str, Any] = {"origin": "derived"}
            if retried:
                props["retried"] = True
            await actions.create_link(from_id, to_id, link_type, source, datetime.now(UTC),
                                      _DERIVE_CONF, evidence_class=_DERIVE_TIER.value,
                                      properties=props)
            # CROSS-SOURCE, ON PURPOSE: the live abstention was very likely stamped
            # under a different actor than this mint's own `source`. A retry (this
            # function's own `retried=True` case) runs under whichever actor re-derived
            # it, almost never the original writer. `assert_property`'s own supersession
            # is same-source-only (actions/core.py); re-asserting under `source` would
            # leave the stale, different-source abstention "current" beside the new
            # resolved marker rather than retiring it, a contradiction minted by the very
            # act meant to resolve it. `actions.supersede_assertion` is the one
            # legitimate cross-source retirement door; every live (non-resolved) row is
            # superseded, not just the first: multiple sources can each hold their own
            # abstention on the same object/link_type in principle, and a genuine
            # resolution retires all of them, not an arbitrarily-picked one.
            stale = await actions.pool.fetch(
                "SELECT id FROM assertions WHERE object_id=$1 AND name=$2 "
                "AND NOT (value ? 'resolved') "
                "AND NOT EXISTS (SELECT 1 FROM assertions s WHERE s.supersedes=id)",
                from_id, f"derivation_abstained_{link_type}")
            for s in stale:
                await actions.supersede_assertion(
                    from_id, f"derivation_abstained_{link_type}", s["id"],
                    {"link_type": link_type, "resolved": True, "resolved_to": str(to_id)},
                    source, datetime.now(UTC), _DERIVE_CONF,
                    f"derive_or_abstain minted {link_type}={to_id}, superseding this "
                    "abstention",
                    evidence_class=_DERIVE_TIER.value)
        return {"minted": not exists, "to": to_id, "abstained": False, "reason": None,
               "candidate_count": 1}
    reason = why_if_ambiguous or (
        f"{len(candidates)} candidates for {link_type} — not a unique lookup, "
        "never guessed" if candidates else
        f"no candidate found for {link_type}")
    # property name carries `link_type` (never a bare "derivation_abstained"): assert_
    # property's own last-write-wins semantics mean a second lane abstaining on the same
    # object under a different link_type must not silently clobber the first lane's own
    # abstention record.
    await actions.assert_property(
        from_id, f"derivation_abstained_{link_type}",
        {"link_type": link_type, "candidate_count": len(candidates), "reason": reason,
         "candidates": [str(c) for c in candidates]},
        source, datetime.now(UTC), _DERIVE_CONF, evidence_class=_DERIVE_TIER.value)
    return {"minted": False, "to": None, "abstained": True, "reason": reason,
           "candidate_count": len(candidates), "candidates": list(candidates)}


async def resolve_repo_default(
    pool: asyncpg.Pool, repo: str | None, actor: str, ident_project: str | None,
) -> dict[str, Any]:
    """THE ONE LADDER: every repo= identity-default caller (record_decision, open_thread,
    and the still-deferred ingest_reference / settle() bulk loops) climbs the same three
    rungs, never a caller-specific reinvention. An earlier record_decision-only version
    of this logic is folded back into this shared shape rather than left to drift as the
    first of four differently-worded copies.

    Rung 1: the caller's own explicit `repo=`, untouched, no lookup runs at all.
    Rung 2: `ident_project`, the writing generation's own works_in (what the mounted
    identity already resolved; this function never queries for it itself).
    Rung 3: only when rung 2 found nothing and `actor` is a real lineage (never the bare
    "session" back-compat source, which has no lineage to walk), `lineage_works_in`
    widens the read across every generation sharing this actor's own root. Unanimous
    across the lineage's own works_in -> resolves; anything else (zero, or 2+
    disagreeing) -> this rung also fails, and the candidate set is handed back for the
    caller to pass to `derive_or_abstain` (`record_lineage_abstain`, below) after its own
    object exists: this function mints and asserts nothing itself, same as the original
    inline version never did.

    Returns `{"repo": str|None, "repo_defaulted": bool, "lineage_attempted": bool,
    "lineage_candidates": list[uuid.UUID], "lineage_projects": list[str]}`."""
    if repo:
        return {"repo": repo, "repo_defaulted": False, "lineage_attempted": False,
               "lineage_candidates": [], "lineage_projects": []}
    repo = ident_project
    repo_defaulted = repo is not None
    lineage_attempted = False
    lineage_candidates: list[uuid.UUID] = []
    lineage_projects: list[str] = []
    if repo is None and actor.startswith("agent:"):
        from src.orchestrator.agents import lineage_works_in

        lineage_attempted = True
        lineage = await lineage_works_in(pool, actor)
        if lineage["resolved"] is not None:
            repo = lineage["resolved"]
            repo_defaulted = True
        else:
            lineage_candidates = lineage["candidate_ids"]
            lineage_projects = lineage["projects"]
    return {"repo": repo, "repo_defaulted": repo_defaulted,
           "lineage_attempted": lineage_attempted,
           "lineage_candidates": lineage_candidates, "lineage_projects": lineage_projects}


async def record_lineage_abstain(
    pool: asyncpg.Pool, object_id: uuid.UUID, actor: str,
    lineage_candidates: list[uuid.UUID], lineage_projects: list[str],
) -> dict[str, Any]:
    """The same post-mint abstain-and-record step `resolve_repo_default`'s rung 3 defers
    to, shared so every caller reports it identically. Call only when
    `resolve_repo_default` returned `lineage_attempted=True` and no `repo`. Mints the
    `in_repo` edge iff `lineage_candidates` is a singleton (never happens in practice:
    `resolve_repo_default` would have resolved it already, kept for defensive symmetry
    with `derive_or_abstain`'s own general contract); otherwise records the durable
    `derivation_abstained_in_repo` property, candidate ids kept whole as future work,
    never broken by recency or generation count.

    Returns the same receipt shape `record_decision`'s wrapper already exposed as
    `lineage_repo_derivation`: `{"attempted": True, "minted": bool, "candidates":
    list[str]}`."""
    reason = (
        f"{len(lineage_projects)} distinct projects across this lineage's own "
        f"works_in ({', '.join(lineage_projects)}) — not a unique lookup, never "
        "guessed" if lineage_projects else None)
    abstain = await derive_or_abstain(
        Actions(pool), object_id, "in_repo", lineage_candidates, actor,
        why_if_ambiguous=reason)
    return {"attempted": True, "minted": abstain["minted"],
           "candidates": [str(c) for c in abstain.get("candidates", [])]}


# HISTORICAL BACKFILL: `resolve_repo_default`'s ladder is write-time-only by design, it
# fires once, when a Decision/Thread is minted, and was never going to retroactively
# touch an object that already existed before it deployed. Measured at one point: 468
# pre-existing, zero-live-link Decision/Thread objects predated that ladder landing, 281
# of which `resolve_repo_default` would mint cleanly today if it ever ran against them, a
# missing verb, not a defect in the live safeguard, exactly the same shape two earlier
# backfills each already covered for their own populations. This population shifts as
# other changes land, so re-measure before trusting any prior count; a measurement can be
# invalidated by a later repair.
#
# THE SUPERSEDE QUESTION, ANSWERED EXPLICITLY, TWICE OVER:
# (1) `derive_or_abstain`'s own "a successful mint supersedes a live abstention" step
# (its docstring) fires only when `derive_or_abstain` itself performs the mint, and
# `resolve_repo_default`'s successful path (rung 2 or 3 naming a project) returns a plain
# string, minted via `link_repo` (the same call the live record_decision/open_thread path
# already uses for that exact case), never through `derive_or_abstain`. So the two lanes
# do not interact for free here; this backfill supersedes a live `derivation_abstained_
# in_repo` record itself, right after a `link_repo` mint. (2) That supersession must be
# cross-source, a second and sharper trap found building this: `assert_property`'s own
# supersession is scoped to the same source only (actions/core.py). The original
# abstention was stamped under the object's own writer's actor (or an earlier backfill
# run's actor), never this call's own `actor`, so re-asserting under `actor` would just
# add a second, different-source "current" row that coexists beside the stale one rather
# than retiring it (the same multi-source-coexistence bug an earlier live diagnosis
# found). `actions.supersede_assertion`, the one legitimate cross-source retirement door,
# is used instead, so a case this backfill (or a future pass) once abstained on and later
# resolves does not sit in `retryable_abstentions`' queue forever regardless of which
# actor recorded the original abstention. At last check, the overlap was empty (zero rows
# carried a live `derivation_abstained_in_repo` at all, since that ladder never abstained
# on anything before this backfill existed), but the handling below is unconditional, not
# contingent on that measurement staying true. The abstain path itself (candidates
# 0-or-2+) does go through `derive_or_abstain` directly, so it already gets the shared
# abstention-recording discipline for free.
async def _supersede_stale_in_repo_abstention(
    actions: Actions, object_id: uuid.UUID, repo: str, actor: str, observed: datetime,
    reason_note: str,
) -> None:
    """Retire a live `derivation_abstained_in_repo` record after a successful mint,
    tolerant of a concurrent writer already retiring the same row between this call's own
    read and write. Found live under a real apply run under real fleet load:
    `backfill_lineage_repo_links` and `backfill_lineage_repo_links_at_write_time`
    can both resolve the same leftover object in the same pass. `supersede_assertion`
    refuses a row no longer live (`ActionError: already superseded`), and until this fix
    that crashed the whole apply run partway through, on an ActionError that meant
    'someone else already did the thing you wanted,' not a real failure. Swallowed here
    only for that one message; any other ActionError still propagates. The object ends up
    resolved either way; the loser's own `resolved_to` note is redundant, not lost data,
    since the winning mint's own `link_repo` call already recorded the live `in_repo`
    edge itself."""
    stale = await actions.pool.fetch(
        "SELECT id FROM assertions WHERE object_id=$1 "
        "AND name='derivation_abstained_in_repo' AND NOT (value ? 'resolved') "
        "AND NOT EXISTS (SELECT 1 FROM assertions s WHERE s.supersedes=id)",
        object_id)
    if not stale:
        return
    proj_id = await _resolve_repo(actions.pool, repo)
    for s in stale:
        try:
            await actions.supersede_assertion(
                object_id, "derivation_abstained_in_repo", s["id"],
                {"link_type": "in_repo", "resolved": True, "resolved_to": str(proj_id)},
                actor, observed, _DERIVE_CONF, reason_note, evidence_class=_DERIVE_TIER.value)
        except ActionError as e:
            if "already superseded" not in str(e):
                raise


async def backfill_lineage_repo_links(
    actions: Actions, *, actor: str, dry_run: bool = True, because: str | None = None,
) -> dict[str, Any]:
    """Links every zero-live-link Decision/Thread authored by a real Agent lineage (never
    the bare `session` back-compat source) to its project, via the same two rungs a new
    write already climbs: `resolve_repo_default(repo=None, ident_project=None)`. Rung 1
    (explicit repo=) is moot for an object that already exists without one; rung 2
    (`ident_project`) has no live mount to read for a historical object, so this always
    passes `None` and lets the function fall straight to rung 3 (the author's own current
    lineage-wide works_in, which may have resolved since the object was written).

    A clean rung-3 name mints `in_repo` via `link_repo`, graded DIRECT_OBSERVATION (never
    SELF_DECLARED: nobody typed this repo=, a mechanical lookup recovered it after the
    fact), and explicitly supersedes any live `derivation_abstained_in_repo` record on
    the same object with a `resolved` marker, since `link_repo` (unlike `derive_or_
    abstain`) does not do that on its own (see the module comment above this function).
    Zero or 2+ lineage-wide projects abstains via `derive_or_abstain` directly, never a
    guess, the same binary rule as elsewhere: exactly one answer or nothing written.

    DRY RUN IS THE DEFAULT. `dry_run=False` REQUIRES a non-blank `because`. Idempotent:
    a repeat call finds nothing to scan once an object is linked, and re-abstaining just
    re-asserts the same fact."""
    if not dry_run and not (because or "").strip():
        return {"error": "backfilling without a because is an un-audited repair — cite "
                         "the evidence/ruling that authorizes it"}
    pool = actions.pool
    rows = await pool.fetch(
        "SELECT DISTINCT o.id, o.type, a.source_id FROM objects o "
        "JOIN assertions a ON a.object_id=o.id AND a.name='summary' "
        "WHERE o.type IN ('Decision','Thread') AND o.status='active' "
        "AND a.source_id LIKE 'agent:%' "
        "AND NOT EXISTS (SELECT 1 FROM links l WHERE l.from_id=o.id AND l.type='in_repo' "
        "AND (l.valid_until IS NULL OR l.valid_until > now()))")
    observed = datetime.now(UTC)
    plan: list[dict[str, Any]] = []
    minted = 0
    abstained = 0
    for row in rows:
        result = await resolve_repo_default(pool, None, row["source_id"], None)
        repo = result["repo"]
        if repo is not None:
            entry = {"id": str(row["id"]), "type": row["type"], "verdict": "mint",
                     "to": repo, "source": row["source_id"]}
            minted += 1
            if not dry_run:
                mint_confession = await link_repo(
                    actions, row["id"], repo, observed, source=actor,
                    evidence_class=_DERIVE_TIER.value, confidence=_DERIVE_CONF)
                if mint_confession:
                    entry.update(mint_confession)
                # CROSS-SOURCE, ON PURPOSE: a live abstention was stamped under its OWN
                # writer's actor at the object's own birth (or a prior backfill run under
                # a different actor than this one). `assert_property`'s own supersession
                # is same-source-only (actions/core.py), so retiring a different source's
                # abstention needs `supersede_assertion`, the one legitimate cross-source
                # retirement door, not a second same-source row that would merely coexist
                # beside the stale one (the same precedent an earlier fix established).
                await _supersede_stale_in_repo_abstention(
                    actions, row["id"], repo, actor, observed,
                    f"backfill_lineage_repo_links resolved this object to "
                    f"{repo!r}, superseding the stale abstention")
        else:
            reason = (
                f"{len(result['lineage_projects'])} distinct projects across this "
                f"lineage's own works_in ({', '.join(result['lineage_projects'])}) — not "
                "a unique lookup, never guessed" if result["lineage_projects"] else
                "no project found anywhere across this lineage's own works_in")
            entry = {"id": str(row["id"]), "type": row["type"], "verdict": "abstain",
                     "reason": reason, "candidate_count": len(result["lineage_candidates"]),
                     "source": row["source_id"]}
            abstained += 1
            if not dry_run:
                await derive_or_abstain(actions, row["id"], "in_repo",
                                        result["lineage_candidates"], actor,
                                        why_if_ambiguous=reason)
        plan.append(entry)
    return {"dry_run": dry_run, "scanned": len(rows), "to_mint": minted,
           "to_abstain": abstained, "plan": plan, "because": because if not dry_run else None}


async def backfill_lineage_repo_links_at_write_time(
    actions: Actions, *, actor: str, dry_run: bool = True, because: str | None = None,
) -> dict[str, Any]:
    """PROVENANCE SWEEP, DECISION/THREAD LANE REFINEMENT: the finer rung
    `backfill_lineage_repo_links` itself named as follow-up scope rather than build
    under context pressure. Every object that lane left abstained (its own
    current-unanimous check already claims everything it can) gets one more look,
    this time windowing each lineage's own `works_in` edges to what was actually live at
    the object's own `observed_at` (`lineage_works_in_at`, agents.py) rather than what the
    lineage's live edges say today. A lineage that moved from one project to a second
    after an object was captured is not actually ambiguous about that object, it just
    looks that way to a read with no clock. Measured live at one point against a 177-row
    leftover population: 45 resolve here that the current-unanimous check could not, 1
    more is zero-everywhere, 131 remain genuinely ambiguous even at their own write time.

    Structurally the same shape as `backfill_lineage_repo_links` (same objects/summary
    query, same mint-via-link_repo + cross-source supersede-on-mint, same derive_or_abstain
    abstain path), kept as its own function/commit per a "one resolver, one commit" rule,
    not folded into the existing lane, since the two use genuinely different lookups
    (`lineage_works_in` vs `lineage_works_in_at`) and running this one is only correct to
    try second, after the plain lane has already claimed what it can.

    DRY RUN IS THE DEFAULT. `dry_run=False` REQUIRES a non-blank `because`. Idempotent for
    the same reason the sibling lane is: a repeat call finds nothing left to scan once an
    object is linked, and re-abstaining just re-asserts the same fact."""
    if not dry_run and not (because or "").strip():
        return {"error": "backfilling without a because is an un-audited repair — cite "
                         "the evidence/ruling that authorizes it"}
    from src.orchestrator.agents import lineage_works_in_at

    pool = actions.pool
    rows = await pool.fetch(
        "SELECT DISTINCT o.id, o.type, a.source_id, a.observed_at FROM objects o "
        "JOIN assertions a ON a.object_id=o.id AND a.name='summary' "
        "WHERE o.type IN ('Decision','Thread') AND o.status='active' "
        "AND a.source_id LIKE 'agent:%' "
        "AND NOT EXISTS (SELECT 1 FROM links l WHERE l.from_id=o.id AND l.type='in_repo' "
        "AND (l.valid_until IS NULL OR l.valid_until > now()))")
    observed = datetime.now(UTC)
    plan: list[dict[str, Any]] = []
    minted = 0
    abstained = 0
    for row in rows:
        lineage = await lineage_works_in_at(pool, row["source_id"], row["observed_at"])
        repo = lineage["resolved"]
        if repo is not None:
            entry = {"id": str(row["id"]), "type": row["type"], "verdict": "mint",
                     "to": repo, "source": row["source_id"]}
            minted += 1
            if not dry_run:
                mint_confession = await link_repo(
                    actions, row["id"], repo, observed, source=actor,
                    evidence_class=_DERIVE_TIER.value, confidence=_DERIVE_CONF)
                if mint_confession:
                    entry.update(mint_confession)
                await _supersede_stale_in_repo_abstention(
                    actions, row["id"], repo, actor, observed,
                    f"backfill_lineage_repo_links_at_write_time resolved this object to "
                    f"{repo!r}, superseding the stale abstention")
        else:
            reason = (
                f"{len(lineage['projects'])} distinct projects across this lineage's own "
                f"works_in AT {row['observed_at'].isoformat()} "
                f"({', '.join(lineage['projects'])}) — not a unique lookup, never guessed"
                if lineage["projects"] else
                "no project found anywhere across this lineage's own works_in, even at "
                "the object's own write time")
            entry = {"id": str(row["id"]), "type": row["type"], "verdict": "abstain",
                     "reason": reason, "candidate_count": len(lineage["candidate_ids"]),
                     "source": row["source_id"]}
            abstained += 1
            if not dry_run:
                await derive_or_abstain(actions, row["id"], "in_repo",
                                        lineage["candidate_ids"], actor,
                                        why_if_ambiguous=reason)
        plan.append(entry)
    return {"dry_run": dry_run, "scanned": len(rows), "to_mint": minted,
           "to_abstain": abstained, "plan": plan, "because": because if not dry_run else None}


# LANE 1 (built off Lane 0's derive_or_abstain above): the boot-startup watchdog's own
# UNREVIEWED BOOT alarm Threads (deploy_guard.alarm_unreviewed_boot), minted with no
# context and no mounted caller, so an earlier fix correctly left them unable to satisfy
# a repo= requirement (unlinkable by that one kind, not unlinkable full stop). 178 of 186
# name the exact sha they booted on in their own summary text ("running HEAD '<sha>' was
# never recorded") and that sha already resolves to a real Commit object 95.7% of the
# time (measured, independently re-verified). Not `_cited_commit_shas` (that regex
# requires the literal word "commit(s)" immediately before the hex token, a
# qualifier-word guard against mismatching a decision/thread short id; this alarm's own
# fixed template never uses that word at all).
_BOOT_ALARM_HEAD_RE = re.compile(r"running HEAD '([0-9a-f]{7,40})' was never recorded")


async def backfill_boot_alarm_commit_links(
    actions: Actions, *, actor: str, dry_run: bool = True, because: str | None = None,
) -> dict[str, Any]:
    """Repair verb for Lane 1 (a previously named population): every zero-live-
    link Thread stamped `source_id` in {boot:osiris-mcp, boot:osiris-worker} whose summary
    cites a boot sha, linked `noted_in` its own already-existing Commit object via Lane 0's
    `derive_or_abstain`, never a guess, never a low-confidence edge (the same binary rule
    used elsewhere). A thread whose cited sha resolves to zero or more-than-one
    Commit abstains, durably, via `derive_or_abstain`'s own `derivation_abstained_noted_in`
    record: the candidate set is kept, not just a count, so a later pass inherits
    the shortlist instead of re-deriving it. A thread with no sha in its summary at all (the
    ~8 that print none) abstains the same way, with `why_if_ambiguous` naming that specific
    cause rather than a bare candidate-count reason.

    DRY RUN IS THE DEFAULT. `dry_run=False` REQUIRES a non-blank `because`. Idempotent:
    `derive_or_abstain` checks the link doesn't already exist before minting, and a repeat
    call over an already-abstained thread simply re-asserts the same abstention fact."""
    if not dry_run and not (because or "").strip():
        return {"error": "backfilling without a because is an un-audited repair — cite "
                         "the evidence/ruling that authorizes it"}
    pool = actions.pool
    threads = await pool.fetch(
        "SELECT o.id, o.canonical, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "  AS summary "
        "FROM objects o "
        "WHERE o.type='Thread' AND o.status='active' "
        "AND EXISTS (SELECT 1 FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='summary' AND a.value #>> '{}' LIKE 'UNREVIEWED BOOT:%') "
        "AND EXISTS (SELECT 1 FROM assertions a2 WHERE a2.object_id=o.id "
        "  AND a2.name='summary' "
        "  AND a2.source_id IN ('boot:osiris-mcp', 'boot:osiris-worker')) "
        "AND NOT EXISTS (SELECT 1 FROM links l WHERE l.from_id=o.id "
        "  AND (l.valid_until IS NULL OR l.valid_until > now()))")
    plan: list[dict[str, Any]] = []
    minted = 0
    abstained = 0
    for row in threads:
        m = _BOOT_ALARM_HEAD_RE.search(row["summary"] or "")
        if m is None:
            entry = {"id": str(row["id"]), "canonical": row["canonical"],
                     "verdict": "abstain", "reason": "no HEAD sha in summary"}
            if not dry_run:
                await derive_or_abstain(
                    actions, row["id"], "noted_in", [], actor,
                    why_if_ambiguous="thread's summary contains no HEAD sha to resolve")
            plan.append(entry)
            abstained += 1
            continue
        sha = m.group(1).lower()
        candidates = [r["id"] for r in await pool.fetch(
            "SELECT id FROM objects WHERE type='Commit' "
            "AND canonical LIKE 'commit:' || $1 || '%'", sha[:12])]
        if len(candidates) == 1:
            entry = {"id": str(row["id"]), "canonical": row["canonical"], "verdict": "mint",
                     "to": str(candidates[0]), "sha": sha}
            minted += 1
        else:
            entry = {"id": str(row["id"]), "canonical": row["canonical"],
                     "verdict": "abstain", "sha": sha, "candidate_count": len(candidates)}
            abstained += 1
        if not dry_run:
            await derive_or_abstain(actions, row["id"], "noted_in", candidates, actor)
        plan.append(entry)
    return {"dry_run": dry_run, "scanned": len(threads), "to_mint": minted,
           "to_abstain": abstained, "plan": plan, "because": because if not dry_run else None}


# THE PROVENANCE SWEEP: the graph should be able to repair its own missing links.
# One derive_or_abstain lane per orphan type (no live link of any kind), each resolving
# from that type's own recorded evidence, never a guess. This lane is Agent (239
# measured live at one point): every orphan is is_sidechain=true, registered by the
# miner's disk-reconstruction pass (lineage.register_swarm/sense_swarms). Checked live:
# every real orphan carries only session/is_sidechain/agent_type-shaped properties, no
# `project` at all: an older register_swarm wrote them, before that function's own
# project-stamping + works_in-mint block existed (current register_swarm resolves both
# immediately, so calling it today never reproduces an orphan; see this file's own
# test fixtures, which build the Agent object directly for that reason). The fix
# landing for new writes never touches the historical backlog; this lane is that
# backlog's own repair, using the one piece of evidence those old rows still carry.
# THE SESSION VALUE ITSELF IS THE EVIDENCE: `_session_dirs(root)` walks every real
# `<project-dir>/<session>/subagents/` under ~/.claude/projects, and `_project_of`
# decodes the project name straight from the parent directory, no LLM, no guess, the
# same on-disk fact scan_subagents already reads at write time. A `session` value that
# is a full uuid matches at most one directory by construction; an 8-hex-fragment
# `session` (an older miner run, before scan_subagents carried the full uuid, the
# live population's own shape) can legitimately match session directories under more
# than one project if that fragment was ever reused, exactly the "multi-project
# sidechain" case a prior ruling names, and exactly why this resolves via
# derive_or_abstain's own cardinality rule rather than picking the first/newest match.
async def resolve_agent_orphans(
    actions: Actions, *, root: Path | None = None, actor: str = "provenance-sweep:agent",
    dry_run: bool = True, because: str | None = None,
) -> dict[str, Any]:
    """Links every zero-live-link Agent to its project via `works_in`, resolved from
    the `session` property register_swarm already stamped on it (never re-derived,
    reading the same on-disk session directories that property names). Zero or
    2+ distinct projects abstains via `derive_or_abstain`, candidate ids kept whole.

    THE DUAL-WRITE: an abstention here also asserts `unlinked_because`/
    `unlinked_because_kind`, the same door-side hatch `_enforce_required_links` writes,
    so `adoption_meter`'s hatch count (an unscoped, all-object read of `unlinked_because`,
    not limited to SCOPED_TYPES) sees this sweep's confessions too. `derive_or_abstain`'s
    own `derivation_abstained_works_in` property is a different fact (candidate ids kept,
    namespaced by link_type) and is written regardless; this hatch write is additive,
    fires only on the abstain branch, and is `kind="standalone"`: a sweep confession
    names no pending extension link.

    DRY RUN IS THE DEFAULT. `dry_run=False` REQUIRES a non-blank `because`. Idempotent:
    a repeat call finds nothing to scan once an object is linked or already carries a
    live abstention that `derive_or_abstain` itself dedupes against; `assert_property`'s
    own within-source supersession makes a repeat hatch write equally safe."""
    if not dry_run and not (because or "").strip():
        return {"error": "backfilling without a because is an un-audited repair — cite "
                         "the evidence/ruling that authorizes it"}
    from src.orchestrator.lineage import _project_of, _session_dirs

    root = root or (Path.home() / ".claude" / "projects")
    pool = actions.pool
    rows = await pool.fetch(
        "SELECT o.id, o.canonical, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='session' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "  AS session "
        "FROM objects o WHERE o.type='Agent' AND o.status='active' "
        "AND NOT EXISTS (SELECT 1 FROM links l WHERE (l.from_id=o.id OR l.to_id=o.id) "
        "AND (l.valid_until IS NULL OR l.valid_until > now()))")
    session_dirs = _session_dirs(root)
    plan: list[dict[str, Any]] = []
    minted = 0
    abstained = 0
    for row in rows:
        session = (row["session"] or "").strip()
        reason: str | None = None
        candidate_ids: list[uuid.UUID] = []
        if not session:
            reason = "no session property recorded at all"
        else:
            matches = [d for d in session_dirs if d.name.startswith(session)]
            projects = sorted({p for d in matches if (p := _project_of(d))})
            if not projects:
                reason = (f"session {session!r} matches no on-disk session directory "
                          "under ~/.claude/projects — the tree may have been pruned")
            else:
                for proj in projects:
                    pid = await actions.create_or_find_object(
                        "SoftwareProject", f"repo:{proj}", actor)
                    candidate_ids.append(pid)
                if len(projects) > 1:
                    reason = (f"session {session!r} matches {len(projects)} distinct "
                              f"projects ({', '.join(projects)}) — not a unique lookup, "
                              "never guessed")
        if len(candidate_ids) == 1:
            entry = {"id": str(row["id"]), "canonical": row["canonical"], "verdict": "mint",
                     "to": str(candidate_ids[0]), "session": session}
            minted += 1
        else:
            entry = {"id": str(row["id"]), "canonical": row["canonical"],
                     "verdict": "abstain", "reason": reason, "session": session,
                     "candidate_count": len(candidate_ids)}
            abstained += 1
        if not dry_run:
            await derive_or_abstain(actions, row["id"], "works_in", candidate_ids, actor,
                                    why_if_ambiguous=reason)
            if len(candidate_ids) != 1:
                hatch_observed = datetime.now(UTC)
                await actions.assert_property(
                    row["id"], "unlinked_because", reason or "no candidate resolved",
                    actor, hatch_observed, _CONF, evidence_class=_EC)
                await actions.assert_property(
                    row["id"], "unlinked_because_kind", "standalone", actor,
                    hatch_observed, _CONF, evidence_class=_EC)
        plan.append(entry)
    return {"dry_run": dry_run, "scanned": len(rows), "to_mint": minted,
           "to_abstain": abstained, "plan": plan, "because": because if not dry_run else None}


async def resolve_reference_orphans(
    actions: Actions, *, actor: str = "provenance-sweep:agent",
    dry_run: bool = True, because: str | None = None,
) -> dict[str, Any]:
    """PROVENANCE SWEEP, REFERENCE LANE: links every zero-live-link
    Reference to its project, resolved from its own `topic` property against a real
    project-name prefix. `bootstrap_project` (orchestrator/bootstrap.py) always writes
    `topic=f"{project}-{topic}"` when it calls `ingest_log` for a named project, so a
    topic literally starting with `"<some live SoftwareProject's name>-"` is a mechanical,
    zero-ambiguity signal, never a content guess.

    MEASURED LIVE, AND REPORTED HONESTLY RATHER THAN FORCED: every one of the
    56 real Reference orphans carries a bare topic (`history`, `design`, `ops`, or one
    genuinely test-shaped outlier `sekhmet-bootstrap-smoke-history` whose own prefix
    matches no live project); the project-prefixed population (`heinrich-history`,
    `decepticons-history`, `monsterhouse-history`, ...) was already linked before this
    lane exists (36 of 38 measured live; `resolve_agent_orphans`'s own history shows a
    zero-mint run is not a broken resolver, it is what "we already checked" looks like).
    A bare topic carries no project-identifying signal in its own data at all, `source_id`
    is uniformly the synthetic `"ref:osiris"` default regardless of which project the
    ingest actually served (`ingest_log`'s own docstring names this exact gap), so
    every orphan today abstains, correctly: minting "osiris" off a bare topic would be
    exactly the content-inference guess `derive_or_abstain`'s whole contract refuses.
    This lane still ships (build the resolver, not just what it resolves today): a
    future `ingest_log` call for a real project that forgets `repo=` is caught here, and
    `retry_ambiguous_abstentions`/`retryable_abstentions` already
    retry every abstention this records for free the moment its shape changes.

    DRY RUN IS THE DEFAULT. `dry_run=False` REQUIRES a non-blank `because`. Idempotent:
    a repeat call finds nothing to scan once an object is linked."""
    if not dry_run and not (because or "").strip():
        return {"error": "backfilling without a because is an un-audited repair — cite "
                         "the evidence/ruling that authorizes it"}
    pool = actions.pool
    projects = await pool.fetch(
        "SELECT id, canonical FROM objects WHERE type='SoftwareProject' AND status='active'")
    names = sorted({p["canonical"].removeprefix("repo:") for p in projects})
    rows = await pool.fetch(
        "SELECT o.id, o.canonical, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='topic' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "  AS topic "
        "FROM objects o WHERE o.type='Reference' AND o.status='active' "
        "AND NOT EXISTS (SELECT 1 FROM links l WHERE (l.from_id=o.id OR l.to_id=o.id) "
        "AND (l.valid_until IS NULL OR l.valid_until > now()))")
    plan: list[dict[str, Any]] = []
    minted = 0
    abstained = 0
    for row in rows:
        topic = (row["topic"] or "").strip()
        matching = [n for n in names if topic.startswith(n + "-")]
        candidate_ids: list[uuid.UUID] = []
        reason: str | None = None
        if not matching:
            reason = (f"topic {topic!r} carries no live project's name as a prefix — no "
                      "signal to derive from" if topic else
                      "no topic property recorded at all")
        else:
            for proj in matching:
                pid = await actions.create_or_find_object(
                    "SoftwareProject", f"repo:{proj}", actor)
                candidate_ids.append(pid)
            if len(matching) > 1:
                reason = (f"topic {topic!r} matches {len(matching)} distinct project name "
                          f"prefixes ({', '.join(matching)}) — not a unique lookup, never "
                          "guessed")
        if len(candidate_ids) == 1:
            entry = {"id": str(row["id"]), "canonical": row["canonical"], "verdict": "mint",
                     "to": str(candidate_ids[0]), "topic": topic}
            minted += 1
        else:
            entry = {"id": str(row["id"]), "canonical": row["canonical"],
                     "verdict": "abstain", "reason": reason, "topic": topic,
                     "candidate_count": len(candidate_ids)}
            abstained += 1
        if not dry_run:
            await derive_or_abstain(actions, row["id"], "in_repo", candidate_ids, actor,
                                    why_if_ambiguous=reason)
        plan.append(entry)
    return {"dry_run": dry_run, "scanned": len(rows), "to_mint": minted,
           "to_abstain": abstained, "plan": plan, "because": because if not dry_run else None}


async def resolve_practice_orphans(
    actions: Actions, *, actor: str = "provenance-sweep:agent",
    dry_run: bool = True, because: str | None = None,
) -> dict[str, Any]:
    """PROVENANCE SWEEP, PRACTICE LANE: links every Practice with no
    live `in_repo` edge of its own to its project, resolved from the distinct projects its
    own live `witnesses` edges already name (the decisions that confirm or refute it),
    one hop, never a guess.

    SCOPED TO in_repo, NOT "zero links at all" (unlike the Agent/Reference lanes above): a
    Practice with no `witnesses` edge is barely a Practice (`record_practice`'s own
    contract mints one on `witnesses=`), so the true "zero-link" population would be
    empty by construction; the actual orphan shape here is "has evidence, was never
    itself attached to a project". `witnesses` targets are Decision/Commit/Thread by
    schema (ontology/schema.py) but a live one can also name a Practice (chained evidence,
    measured live); that target simply carries no `in_repo` of its own and contributes no
    candidate, same as a target of any type with no project link yet.

    MEASURED LIVE at one point: 49 zero-in_repo Practices out of 95 total. Walking each
    one's own witnesses set and taking the distinct in_repo projects those targets already
    carry resolves the clean majority (a single project named, sometimes several times over)
    and correctly abstains the rest: some genuinely multi-project (evidence drawn from two
    or more repos), some whose witnesses themselves have no project yet either (zero
    candidates, nothing to derive).

    DRY RUN IS THE DEFAULT. `dry_run=False` REQUIRES a non-blank `because`. Idempotent:
    a repeat call finds nothing to scan once an object is linked."""
    if not dry_run and not (because or "").strip():
        return {"error": "backfilling without a because is an un-audited repair — cite "
                         "the evidence/ruling that authorizes it"}
    pool = actions.pool
    rows = await pool.fetch(
        "SELECT o.id, o.canonical FROM objects o WHERE o.type='Practice' AND o.status='active' "
        "AND NOT EXISTS (SELECT 1 FROM links l WHERE l.from_id=o.id AND l.type='in_repo' "
        "AND (l.valid_until IS NULL OR l.valid_until > now()))")
    plan: list[dict[str, Any]] = []
    minted = 0
    abstained = 0
    for row in rows:
        proj_rows = await pool.fetch(
            "SELECT DISTINCT p.id, p.canonical FROM links w "
            "JOIN links l ON l.from_id=w.to_id AND l.type='in_repo' "
            "  AND (l.valid_until IS NULL OR l.valid_until > now()) "
            "JOIN objects p ON p.id=l.to_id AND p.type='SoftwareProject' "
            "WHERE w.from_id=$1 AND w.type='witnesses' "
            "AND (w.valid_until IS NULL OR w.valid_until > now())", row["id"])
        # DISTINCT p.id in the query above already dedups: one row per project id.
        projects = sorted({p["canonical"].removeprefix("repo:") for p in proj_rows})
        candidate_ids = [p["id"] for p in proj_rows]
        reason: str | None = None
        if len(candidate_ids) != 1:
            reason = (
                f"{len(projects)} distinct projects across this Practice's own witnesses "
                f"({', '.join(projects)}) — not a unique lookup, never guessed"
                if projects else
                "no project found among any of this Practice's own witnesses")
        if len(candidate_ids) == 1:
            entry = {"id": str(row["id"]), "canonical": row["canonical"], "verdict": "mint",
                     "to": str(candidate_ids[0])}
            minted += 1
        else:
            entry = {"id": str(row["id"]), "canonical": row["canonical"],
                     "verdict": "abstain", "reason": reason,
                     "candidate_count": len(candidate_ids)}
            abstained += 1
        if not dry_run:
            await derive_or_abstain(actions, row["id"], "in_repo", candidate_ids, actor,
                                    why_if_ambiguous=reason)
        plan.append(entry)
    return {"dry_run": dry_run, "scanned": len(rows), "to_mint": minted,
           "to_abstain": abstained, "plan": plan, "because": because if not dry_run else None}


async def resolve_superstition_orphans(
    actions: Actions, *, actor: str = "provenance-sweep:agent",
    dry_run: bool = True, because: str | None = None,
) -> dict[str, Any]:
    """PROVENANCE SWEEP, SUPERSTITION LANE: links every
    zero-live-in_repo Superstition to its project, resolved from its own `killed_by`
    property (the decision that killed it), one hop, never a
    guess. `kill_superstition` (this module) already passes the killing call's own `repo`
    straight through when one is given; an orphan Superstition is exactly the case where
    that call had none, same "missing at write time" gap the Decision/Thread lanes above
    close for their own objects. `killed_by` names either a Decision id or a commit hash
    (`kill_superstition`'s own docstring), resolved via the same `_resolve_ref` ladder
    every other identifier-shaped reference in this module uses, `require_identifier=True`
    so a malformed value refuses rather than falls through to a fuzzy text search.

    MEASURED LIVE at one point: all 4 real orphans share one `killed_by` (a single
    Decision, itself already linked to one project), a clean, unambiguous mint for every
    one of them, the "single-candidate, linked" shape this lane's own acceptance criteria
    names.

    DRY RUN IS THE DEFAULT. `dry_run=False` REQUIRES a non-blank `because`. Idempotent:
    a repeat call finds nothing to scan once an object is linked."""
    if not dry_run and not (because or "").strip():
        return {"error": "backfilling without a because is an un-audited repair — cite "
                         "the evidence/ruling that authorizes it"}
    pool = actions.pool
    rows = await pool.fetch(
        "SELECT o.id, o.canonical, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='killed_by' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "  AS killed_by "
        "FROM objects o WHERE o.type='Superstition' AND o.status='active' "
        "AND NOT EXISTS (SELECT 1 FROM links l WHERE l.from_id=o.id AND l.type='in_repo' "
        "AND (l.valid_until IS NULL OR l.valid_until > now()))")
    plan: list[dict[str, Any]] = []
    minted = 0
    abstained = 0
    for row in rows:
        killed_by = (row["killed_by"] or "").strip()
        killer: uuid.UUID | None = None
        if killed_by:
            killer = await _resolve_ref(pool, "Decision", killed_by, text_field="summary",
                                        require_identifier=True)
            if killer is None:
                killer = await _resolve_ref(pool, "Commit", killed_by, text_field="subject",
                                            require_identifier=True)
        candidate_ids: list[uuid.UUID] = []
        reason: str | None = None
        if killer is None:
            reason = (f"killed_by {killed_by!r} does not resolve to any live "
                      "Decision/Commit — no signal to derive from" if killed_by else
                      "no killed_by property recorded at all")
        else:
            proj_rows = await pool.fetch(
                "SELECT DISTINCT p.id, p.canonical FROM links l "
                "JOIN objects p ON p.id=l.to_id AND p.type='SoftwareProject' "
                "WHERE l.from_id=$1 AND l.type='in_repo' "
                "AND (l.valid_until IS NULL OR l.valid_until > now())", killer)
            candidate_ids = [p["id"] for p in proj_rows]
            if len(candidate_ids) > 1:
                names = sorted({p["canonical"].removeprefix("repo:") for p in proj_rows})
                reason = (f"the killing decision/commit itself names {len(names)} distinct "
                          f"projects ({', '.join(names)}) — not a unique lookup, never "
                          "guessed")
            elif not candidate_ids:
                reason = "the killing decision/commit carries no project link of its own yet"
        if len(candidate_ids) == 1:
            entry = {"id": str(row["id"]), "canonical": row["canonical"], "verdict": "mint",
                     "to": str(candidate_ids[0]), "killed_by": killed_by}
            minted += 1
        else:
            entry = {"id": str(row["id"]), "canonical": row["canonical"],
                     "verdict": "abstain", "reason": reason, "killed_by": killed_by,
                     "candidate_count": len(candidate_ids)}
            abstained += 1
        if not dry_run:
            await derive_or_abstain(actions, row["id"], "in_repo", candidate_ids, actor,
                                    why_if_ambiguous=reason)
        plan.append(entry)
    return {"dry_run": dry_run, "scanned": len(rows), "to_mint": minted,
           "to_abstain": abstained, "plan": plan, "because": because if not dry_run else None}


async def resolve_seat_orphans(
    actions: Actions, *, actor: str = "provenance-sweep:seat",
    dry_run: bool = True, because: str | None = None,
) -> dict[str, Any]:
    """THE POST-MINT INVARIANT'S OWN HEARTBEAT HALF:
    claim_name's own in-line check (agents.py) catches a vacant Seat in the same call
    that minted it: bind_holder always runs right after ensure_seat, same call, so that
    check is normally a no-op. This is what catches the one thing it cannot: a process
    that crashed between ensure_seat succeeding and bind_holder ever running, or a Seat
    minted by some other caller (mintseat.py, greatfold.py) that never went through
    claim_name's own binding step at all. Every active, unconfessed Seat with no live
    holder is confessed via the same `capture.confirm_or_confess_link` claim_name's
    in-line check already uses, on the same heartbeat `apply_provenance_sweep_heartbeat`
    already runs every other lane through.

    UNLIKE this module's other orphan lanes (Agent/Reference/Practice/Superstition), this
    is not a derive_or_abstain candidate lookup: a vacant Seat has no ambiguous candidate
    set to resolve from its own properties; it is either held or it is not. So `to_mint`
    stays 0 always here; `to_abstain` counts real confessions, one per Seat this call
    actually wrote a hatch for (a Seat already confessed by an earlier run is excluded by
    the query itself, never re-counted).

    DRY RUN IS THE DEFAULT. `dry_run=False` REQUIRES a non-blank `because`. Idempotent:
    a repeat call finds nothing to scan once a Seat is held or already confessed."""
    if not dry_run and not (because or "").strip():
        return {"error": "backfilling without a because is an un-audited repair — cite "
                         "the evidence/ruling that authorizes it"}
    rows = await actions.pool.fetch(
        "SELECT o.id, o.canonical FROM objects o WHERE o.type='Seat' AND o.status='active' "
        "AND NOT EXISTS (SELECT 1 FROM links l WHERE l.to_id=o.id AND l.type='holds' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())) "
        "AND NOT EXISTS (SELECT 1 FROM current_assertions ca WHERE ca.object_id=o.id "
        "AND (ca.name='unlinked_because' OR "
        "     (ca.name='derivation_abstained_holds' AND NOT (ca.value ? 'resolved'))))")
    plan: list[dict[str, Any]] = []
    confessed = 0
    for row in rows:
        plan.append({"id": str(row["id"]), "canonical": row["canonical"],
                     "verdict": "abstain"})
        if not dry_run:
            wrote = await confirm_or_confess_link(
                actions, row["id"], "holds", direction="to",
                reason="no live holder found by the provenance sweep's heartbeat",
                source=actor, observed=datetime.now(UTC))
            if wrote:
                confessed += 1
        else:
            confessed += 1
    return {"dry_run": dry_run, "scanned": len(rows), "to_mint": 0,
           "to_abstain": confessed, "plan": plan, "because": because if not dry_run else None}


async def resolve_project_orphans(
    actions: Actions, *, actor: str = "provenance-sweep:project",
    dry_run: bool = True, because: str | None = None,
) -> dict[str, Any]:
    """PROVENANCE SWEEP, SOFTWAREPROJECT LANE: SoftwareProject has no single mint door,
    five separate auto-vivifying `create_or_find_object("SoftwareProject", ...)` call
    sites, each a side effect of some other caller resolving its own `repo=` string, so
    unlike this module's other lanes it was never covered by any sweep either. Confesses
    every zero-live-link SoftwareProject.

    NOT a `derive_or_abstain` candidate lookup, SAME SHAPE as `resolve_seat_orphans`
    above: a friendless SoftwareProject has no ambiguous
    candidate set to resolve from its own properties; by construction, if a real
    self-declared `in_repo`/`works_in`/`governs` link to it existed, it would not be
    zero-live-link in the first place. So `to_mint` is always 0 here; `to_abstain` counts
    real confessions, via the same `confirm_or_confess_link` primitive `resolve_seat_
    orphans` uses (real link checked first, then already-confessed, then the dual hatch
    write), one primitive for every confession in this module, once an earlier
    post-mint invariant branch landed it on main. An
    earlier build of this lane wrote the hatch directly because that primitive
    wasn't merged yet; this rebase refactors onto it.

    THE EVIDENCE SEARCH (evidence being the commits, decisions
    or references that name the project) is purely descriptive, never a mint: this scans
    Commit.subject / Decision.summary / Reference.topic for the project's bare name
    (`canonical` minus its `repo:` prefix) so the confession's own `reason` text can
    distinguish "mentioned in prose somewhere, just never linked" from "nothing in the
    graph names this project at all", a genuinely useful triage signal for whoever reads
    the abstention later, but a textual mention is never treated as a candidate to link
    against; asserting a real edge off a prose match would be exactly the guess `derive_
    or_abstain`'s whole contract refuses, and this lane never does it.

    DRY RUN IS THE DEFAULT. `dry_run=False` REQUIRES a non-blank `because`. Idempotent:
    `confirm_or_confess_link` itself skips an already-linked or already-confessed row."""
    if not dry_run and not (because or "").strip():
        return {"error": "backfilling without a because is an un-audited repair — cite "
                         "the evidence/ruling that authorizes it"}
    pool = actions.pool
    rows = await pool.fetch(
        "SELECT o.id, o.canonical FROM objects o WHERE o.type='SoftwareProject' "
        "AND o.status='active' "
        "AND NOT EXISTS (SELECT 1 FROM links l WHERE (l.from_id=o.id OR l.to_id=o.id) "
        "AND (l.valid_until IS NULL OR l.valid_until > now())) "
        "AND NOT EXISTS (SELECT 1 FROM current_assertions ca WHERE ca.object_id=o.id "
        "AND (ca.name='unlinked_because' OR "
        "     (ca.name='derivation_abstained_in_repo' AND NOT (ca.value ? 'resolved'))))")
    plan: list[dict[str, Any]] = []
    confessed = 0
    for row in rows:
        name = row["canonical"].removeprefix("repo:")
        named_by = await pool.fetchval(
            "SELECT count(*) FROM ("
            "  SELECT 1 FROM objects c WHERE c.type='Commit' AND c.status='active' "
            "  AND EXISTS (SELECT 1 FROM current_assertions a WHERE a.object_id=c.id "
            "    AND a.name='subject' AND a.value #>> '{}' ILIKE '%' || $1 || '%') "
            "  UNION ALL "
            "  SELECT 1 FROM objects d WHERE d.type='Decision' AND d.status='active' "
            "  AND EXISTS (SELECT 1 FROM current_assertions a WHERE a.object_id=d.id "
            "    AND a.name='summary' AND a.value #>> '{}' ILIKE '%' || $1 || '%') "
            "  UNION ALL "
            "  SELECT 1 FROM objects r WHERE r.type='Reference' AND r.status='active' "
            "  AND EXISTS (SELECT 1 FROM current_assertions a WHERE a.object_id=r.id "
            "    AND a.name='topic' AND a.value #>> '{}' ILIKE $1 || '%')"
            ") named", name)
        reason = (f"named by {named_by} commit/decision/reference row(s) in prose, but "
                  "none carries a live link to this project" if named_by else
                  "no commit, decision, or reference names this project at all")
        plan.append({"id": str(row["id"]), "canonical": row["canonical"],
                     "verdict": "abstain", "reason": reason, "named_by": named_by})
        if not dry_run:
            wrote = await confirm_or_confess_link(
                actions, row["id"], "in_repo", direction="to", reason=reason,
                source=actor, observed=datetime.now(UTC))
            if wrote:
                confessed += 1
        else:
            confessed += 1
    return {"dry_run": dry_run, "scanned": len(rows), "to_mint": 0,
           "to_abstain": confessed, "plan": plan, "because": because if not dry_run else None}


_PROVENANCE_SWEEP_BECAUSE = (
    "classification_laws_heartbeat: provenance sweep self-heal (wave 15, mail 8840) — "
    "every lane below is cardinality-1-mint-or-abstain via derive_or_abstain, never a "
    "guess, so an unattended cron running it is exactly as safe as a supervised one"
)


async def apply_provenance_sweep_heartbeat(
    actions: Actions, *, actor: str = "cron:classification_laws_heartbeat",
) -> dict[str, Any]:
    """THE PROVENANCE SWEEP'S OWN HEARTBEAT SUB-SWEEP: so that a fresh install self-heals
    with the same unconditional, mechanical, zero-hand-pass
    discipline `classification_laws_heartbeat`'s other siblings already carry, never
    someone re-running a backfill script by hand. Applies every lane built for this
    sweep: Agent (`resolve_agent_orphans`), Decision/Thread (`backfill_lineage_repo_
    links` + its at-write-time sibling), Reference (`resolve_reference_orphans`),
    Practice (`resolve_practice_orphans`), Superstition (`resolve_superstition_orphans`),
    plus the post-mint invariant's own heartbeat half, Seat (`resolve_seat_orphans`),
    and SoftwareProject (`resolve_project_orphans`), for real (`dry_run=False`), each
    independently, under one fixed `because` (this
    module's `_PROVENANCE_SWEEP_BECAUSE`): every lane is cardinality-1-mint-or-abstain
    by construction, so there is nothing here for a human to authorize per-run that
    the lane's own contract doesn't already guarantee.

    ONE LANE'S FAILURE NEVER SINKS ANOTHER'S (same discipline as `classification_laws_
    heartbeat`'s own siblings): each call is individually try/excepted; a DB hiccup on
    one lane is reported under its own key (`"<lane>_error"`) and the sweep continues.
    Idempotent: a lane with nothing left to scan just reports zero scanned/minted, same
    as running it by hand twice."""
    lanes: dict[str, Any] = {}
    because = _PROVENANCE_SWEEP_BECAUSE
    try:
        lanes["agent"] = await resolve_agent_orphans(
            actions, actor=actor, dry_run=False, because=because)
    except Exception as exc:  # a DB hiccup on one lane must not sink the others
        lanes["agent_error"] = repr(exc)
    try:
        lanes["decision_thread"] = await backfill_lineage_repo_links(
            actions, actor=actor, dry_run=False, because=because)
    except Exception as exc:
        lanes["decision_thread_error"] = repr(exc)
    try:
        lanes["decision_thread_at_write_time"] = await backfill_lineage_repo_links_at_write_time(
            actions, actor=actor, dry_run=False, because=because)
    except Exception as exc:
        lanes["decision_thread_at_write_time_error"] = repr(exc)
    try:
        lanes["reference"] = await resolve_reference_orphans(
            actions, actor=actor, dry_run=False, because=because)
    except Exception as exc:
        lanes["reference_error"] = repr(exc)
    try:
        lanes["practice"] = await resolve_practice_orphans(
            actions, actor=actor, dry_run=False, because=because)
    except Exception as exc:
        lanes["practice_error"] = repr(exc)
    try:
        lanes["superstition"] = await resolve_superstition_orphans(
            actions, actor=actor, dry_run=False, because=because)
    except Exception as exc:
        lanes["superstition_error"] = repr(exc)
    try:
        lanes["seat"] = await resolve_seat_orphans(
            actions, actor=actor, dry_run=False, because=because)
    except Exception as exc:
        lanes["seat_error"] = repr(exc)
    try:
        lanes["project"] = await resolve_project_orphans(
            actions, actor=actor, dry_run=False, because=because)
    except Exception as exc:
        lanes["project_error"] = repr(exc)
    return {"lanes": lanes,
           "total_minted": sum(v.get("to_mint", 0) for v in lanes.values()
                               if isinstance(v, dict)),
           "total_abstained": sum(v.get("to_abstain", 0) for v in lanes.values()
                                  if isinstance(v, dict))}


async def _describe(pool: asyncpg.Pool, obj_id: uuid.UUID) -> tuple[str | None, str | None]:
    """Best-effort (type, summary) for a bare id: `summary` is the universal text-field
    name this codebase's own generic listing/describe queries already key on across
    object types (record_decision/open_thread/ingest_reference all write it). Display
    only, never load-bearing: an object with no `summary` assertion (rare, non-authored
    types) returns a None summary, not an error."""
    row = await pool.fetchrow(
        "SELECT o.type, "
        "(SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        " AND a.name='summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "AS summary FROM objects o WHERE o.id=$1", obj_id)
    if row is None:
        return None, None
    return row["type"], row["summary"]


async def abstained_derivations(
    pool: asyncpg.Pool, link_type: str | None = None, *, limit: int = 100,
) -> dict[str, Any]:
    """THE READ SURFACE over derive_or_abstain's own refusals: opens a materialized
    queue with candidate sets already resolved, instead of making a caller write a query
    against a graph that has moved under it. Every namespaced
    `derivation_abstained_<link_type>` property, newest first, with
    `from_id` and every candidate already resolved to its own (type, summary) via
    `_describe`, so a caller reading this gets the shortlist as it stands now, not a bag
    of uuids it has to re-look-up itself against a graph that may have moved since the
    abstention was recorded.

    `link_type=None` returns every lane's abstentions pooled together; passing a specific
    link_type scopes to one namespaced property, exactly ("query every ambiguous
    in_repo" without filtering a soup: the property name is the filter).
    LIVE ABSTENTIONS ONLY: a case `derive_or_abstain` has since resolved (its own
    `resolved` marker) is excluded: reading a resolved case as still-
    abstained would be the exact stale-record failure this lane exists to fix, just
    surfaced through the wrong door. `count` is the true total population for that scope
    (never capped by `limit`, same convention `stale_current_flags` already uses);
    `sample` is bounded by `limit`."""
    name_filter = f"derivation_abstained_{link_type}" if link_type else None
    total = await pool.fetchval(
        "SELECT count(DISTINCT (object_id, name)) FROM current_assertions "
        "WHERE name LIKE 'derivation_abstained_%' AND ($1::text IS NULL OR name = $1) "
        "AND NOT (value ? 'resolved')",
        name_filter)
    rows = await pool.fetch(
        "WITH latest AS ("
        "  SELECT DISTINCT ON (a.object_id, a.name) a.object_id, a.name, a.value, "
        "         a.observed_at, a.source_id "
        "  FROM current_assertions a "
        "  WHERE a.name LIKE 'derivation_abstained_%' AND ($1::text IS NULL OR a.name = $1) "
        "  ORDER BY a.object_id, a.name, a.confidence DESC, a.observed_at DESC) "
        "SELECT * FROM latest WHERE NOT (value ? 'resolved') "
        "ORDER BY observed_at DESC LIMIT $2", name_filter, limit)
    sample: list[dict[str, Any]] = []
    for r in rows:
        value = r["value"]
        from_id = r["object_id"]
        from_type, from_summary = await _describe(pool, from_id)
        candidates: list[dict[str, Any]] = []
        for c in value.get("candidates", []):
            cid = uuid.UUID(c)
            c_type, c_summary = await _describe(pool, cid)
            candidates.append({"id": str(cid)[:8], "type": c_type, "summary": c_summary})
        sample.append({
            "from_id": str(from_id)[:8], "from_type": from_type, "from_summary": from_summary,
            "link_type": value.get("link_type", r["name"].removeprefix("derivation_abstained_")),
            "reason": value.get("reason"), "candidate_count": value.get("candidate_count"),
            "candidates": candidates, "observed_at": r["observed_at"], "source": r["source_id"],
        })
    return {"count": total, "sample": sample}


async def retryable_abstentions(
    pool: asyncpg.Pool, link_type: str | None = None, *, limit: int = 100,
) -> dict[str, Any]:
    """THE STRUCTURALLY-SAFE RETRY DOOR: the SQL itself
    filters to `candidate_count = 0`: a zero-candidate abstention means the lookup found
    nothing yet, which time can change (an ingest that catches up, a deploy that lands),
    so retrying it later is sound. A 2+-candidate abstention means the lookup found a
    genuine ambiguity; time changes nothing about that fact, and a later
    retry that happens to see one candidate is a race, not a resolution: minting from it
    would be the guess the binary rule forbids. That distinction is baked into the WHERE
    clause, not a post-fetch filter a caller (or a future edit) could accidentally widen
    to include the ambiguous population.

    Oldest-abstained first (the longest-waiting cases surface first for a re-attempt).
    Same {count, sample} shape as `abstained_derivations`, already excluding resolved
    cases (a case this function names is by construction never a resolved one, a
    resolved case's value carries no `candidate_count` key at all, since a resolution
    replaces the abstention value entirely). The caller still owns re-running its own
    lane-specific lookup and calling `derive_or_abstain(..., retried=True)`; this
    function only names which objects are safe to re-attempt, never re-attempts them."""
    name_filter = f"derivation_abstained_{link_type}" if link_type else None
    total = await pool.fetchval(
        "SELECT count(DISTINCT (object_id, name)) FROM current_assertions "
        "WHERE name LIKE 'derivation_abstained_%' AND ($1::text IS NULL OR name = $1) "
        "AND (value ? 'candidate_count') AND (value->>'candidate_count')::int = 0",
        name_filter)
    rows = await pool.fetch(
        "WITH latest AS ("
        "  SELECT DISTINCT ON (a.object_id, a.name) a.object_id, a.name, a.value, "
        "         a.observed_at, a.source_id "
        "  FROM current_assertions a "
        "  WHERE a.name LIKE 'derivation_abstained_%' AND ($1::text IS NULL OR a.name = $1) "
        "  ORDER BY a.object_id, a.name, a.confidence DESC, a.observed_at DESC) "
        "SELECT * FROM latest "
        "WHERE (value ? 'candidate_count') AND (value->>'candidate_count')::int = 0 "
        "ORDER BY observed_at ASC LIMIT $2", name_filter, limit)
    sample = []
    for r in rows:
        value = r["value"]
        from_id = r["object_id"]
        from_type, from_summary = await _describe(pool, from_id)
        sample.append({
            "from_id": str(from_id)[:8], "from_type": from_type, "from_summary": from_summary,
            "link_type": value.get("link_type", r["name"].removeprefix("derivation_abstained_")),
            "reason": value.get("reason"), "observed_at": r["observed_at"],
            "source": r["source_id"],
        })
    return {"count": total, "sample": sample}


# THE OTHER HALF OF THE RETRY DOOR: the zero-candidate and ambiguous
# populations must not collapse together; they have different re-scan
# conditions. `retryable_abstentions` above is deliberately scoped to `candidate_count=0`
# only: its own docstring's reasoning ("a later retry that happens to see one candidate
# is a race") is correct for re-running the original lookup from scratch, which is exactly
# what that function's callers do (re-derive candidates fresh, e.g. `resolve_repo_default`
# catching up as ingest lands). It says nothing about a different, safer operation: an
# ambiguous abstention already keeps its full original candidate set, so
# checking whether some of those specific,
# already-recorded candidates have since been formally eliminated (merged, retired,
# invalidated, a real, audited event, never a guess) is not a race with a fresh lookup;
# it is the same question asked elsewhere, re-asked against a candidate set that may have
# shrunk since it was last asked. `retryable_ambiguous_abstentions` is exactly that check,
# LANE-AGNOSTIC (unlike a zero-candidate retry, which needs each lane's own lookup re-run,
# no such thing exists here or is proposed by this lane): it needs only the stored
# candidate ids, never re-derives anything, so one generic write verb
# (`retry_ambiguous_abstentions`, below) can safely serve every lane's ambiguous
# abstentions at once, present or future.
_AMBIGUOUS_SURVIVOR_CTE = (
    "WITH latest AS ("
    "  SELECT DISTINCT ON (a.object_id, a.name) a.object_id, a.name, a.value, "
    "         a.observed_at, a.source_id "
    "  FROM current_assertions a "
    "  WHERE a.name LIKE 'derivation_abstained_%' AND ($1::text IS NULL OR a.name = $1) "
    "  ORDER BY a.object_id, a.name, a.confidence DESC, a.observed_at DESC), "
    "ambiguous AS ("
    "  SELECT *, jsonb_array_length(value->'candidates') AS original_candidate_count "
    "  FROM latest "
    "  WHERE (value ? 'candidate_count') AND (value->>'candidate_count')::int >= 2), "
    "surviving AS ("
    "  SELECT amb.*, ("
    "    SELECT count(*) FROM jsonb_array_elements_text(amb.value->'candidates') cid "
    "    JOIN objects o ON o.id = cid::uuid AND o.status='active'"
    "  ) AS active_count, ("
    "    SELECT o.id FROM jsonb_array_elements_text(amb.value->'candidates') cid "
    "    JOIN objects o ON o.id = cid::uuid AND o.status='active' LIMIT 1"
    "  ) AS survivor_id "
    "  FROM ambiguous amb) "
)


async def _ambiguous_survivor_rows(
    pool: asyncpg.Pool, link_type: str | None, limit: int,
) -> list[asyncpg.Record]:
    """The shared, full-precision fetch both `retryable_ambiguous_abstentions` (display,
    truncated ids) and `retry_ambiguous_abstentions` (write, needs the real uuids) build
    on: one query, never re-derived differently by the two callers."""
    name_filter = f"derivation_abstained_{link_type}" if link_type else None
    return await pool.fetch(  # type: ignore[no-any-return]
        _AMBIGUOUS_SURVIVOR_CTE + "SELECT * FROM surviving WHERE active_count = 1 "
        "ORDER BY observed_at ASC LIMIT $2", name_filter, limit)


async def retryable_ambiguous_abstentions(
    pool: asyncpg.Pool, link_type: str | None = None, *, limit: int = 100,
) -> dict[str, Any]:
    """Every live (non-resolved), 2+-candidate abstention whose original candidate set
    has been reduced, by elimination alone, to exactly one `objects.status='active'`
    survivor, never a fresh re-derivation, only a status recheck of ids the abstention
    itself already recorded, so the safety is structural (SQL-enforced, same discipline
    `retryable_abstentions` uses for its own zero-candidate scope) rather than a promise
    a caller has to keep. A candidate set reduced to zero survivors is a real, different
    fact (every named answer is now gone) but is not included here: nothing to retry-
    mint from it, and conflating it with the one-survivor case would collapse two
    populations that must stay separate; a caller wanting that population reads
    `abstained_derivations`/this function's own `eliminated_to_zero` count and decides
    separately what (if anything) it means for that lane.

    Oldest-abstained first, same {count, sample} shape as `retryable_abstentions` plus
    `surviving_candidate` (the one id this call's own sibling, `retry_ambiguous_
    abstentions`, would mint) and `original_candidate_count` (so a reader can see how
    ambiguous it originally was, not just that it wasn't). `eliminated_to_zero` (top-
    level, alongside `count`) is the true total of the zero-survivor population, for
    visibility only, never folded into `count` or `sample`."""
    name_filter = f"derivation_abstained_{link_type}" if link_type else None
    total = await pool.fetchval(
        _AMBIGUOUS_SURVIVOR_CTE + "SELECT count(*) FROM surviving WHERE active_count = 1",
        name_filter)
    eliminated_to_zero = await pool.fetchval(
        _AMBIGUOUS_SURVIVOR_CTE + "SELECT count(*) FROM surviving WHERE active_count = 0",
        name_filter)
    rows = await _ambiguous_survivor_rows(pool, link_type, limit)
    sample: list[dict[str, Any]] = []
    for r in rows:
        value = r["value"]
        from_id = r["object_id"]
        survivor_id = r["survivor_id"]
        from_type, from_summary = await _describe(pool, from_id)
        survivor_type, survivor_summary = await _describe(pool, survivor_id)
        sample.append({
            "from_id": str(from_id)[:8], "from_type": from_type, "from_summary": from_summary,
            "link_type": value.get("link_type", r["name"].removeprefix("derivation_abstained_")),
            "original_candidate_count": r["original_candidate_count"],
            "surviving_candidate": {"id": str(survivor_id)[:8], "type": survivor_type,
                                    "summary": survivor_summary},
            "observed_at": r["observed_at"], "source": r["source_id"],
        })
    return {"count": total, "eliminated_to_zero": eliminated_to_zero, "sample": sample}


async def retry_ambiguous_abstentions(
    actions: Actions, *, actor: str, dry_run: bool = True, because: str | None = None,
    link_type: str | None = None,
) -> dict[str, Any]:
    """The write half `retryable_ambiguous_abstentions` names but never touches: mints
    the one surviving candidate for every row that function reports, via `derive_or_
    abstain(..., retried=True)`. `len(candidates) == 1` there always mints (the
    surviving set is a singleton by this function's own selection), and its own cross-
    source supersede step retires the stale abstention regardless of which actor
    originally recorded it. LANE-AGNOSTIC BY CONSTRUCTION: no lane-specific lookup is
    re-run, only the stored candidate ids' current status is rechecked, so this one verb
    already covers every present and future lane's ambiguous abstentions, not just this
    house's own current single lane (in_repo).

    DRY RUN IS THE DEFAULT. `dry_run=False` REQUIRES a non-blank `because`. Idempotent:
    a repeat call finds nothing to retry once minted (the abstention is superseded, so
    `retryable_ambiguous_abstentions` no longer names it)."""
    if not dry_run and not (because or "").strip():
        return {"error": "backfilling without a because is an un-audited repair — cite "
                         "the evidence/ruling that authorizes it"}
    rows = await _ambiguous_survivor_rows(actions.pool, link_type, 1_000_000)
    plan: list[dict[str, Any]] = []
    minted = 0
    for r in rows:
        from_id = r["object_id"]
        survivor_id = r["survivor_id"]
        row_link_type = r["value"].get(
            "link_type", r["name"].removeprefix("derivation_abstained_"))
        plan.append({"id": str(from_id), "link_type": row_link_type,
                    "to": str(survivor_id),
                    "original_candidate_count": r["original_candidate_count"]})
        minted += 1
        if not dry_run:
            await derive_or_abstain(actions, from_id, row_link_type, [survivor_id], actor,
                                    retried=True)
    return {"dry_run": dry_run, "scanned": len(rows), "to_mint": minted,
           "plan": plan, "because": because if not dry_run else None}


async def _mint_prose_citations(
    a: Actions, obj_id: uuid.UUID, source: str, *texts: str | None,
) -> list[dict[str, str]]:
    """The shared write-time step both record_decision and open_thread call, inside
    their own atomic block: extract, resolve, mint, same shape `_cited_commit_shas`/
    `_resolve_commit` already established for `decided_in`/`noted_in`. Returns the
    skip log (a hard requirement, mirroring `backfill_decided_in`'s own
    `skipped` field): every citation that did not mint, and exactly why, an
    ambiguous match, a type mismatch, or nothing found at all, never a silent drop."""
    skipped: list[dict[str, str]] = []
    for claimed_type, short_id in _cited_object_refs(*texts):
        target_id, reason = await _resolve_cited_object(a.pool, claimed_type, short_id)
        if target_id is None:
            skipped.append({"ref": f"{claimed_type.lower()} {short_id}",
                           "reason": reason or "unresolved"})
            continue
        # `source` (this call's own param) is who is citing, directly, never re-read
        # from the object's own just-asserted, possibly still-uncommitted summary via
        # `a.pool` (a different connection than this transaction's own bound one would
        # not see it yet). Only the target's source needs a lookup: it is a pre-
        # existing, already-committed object this transaction never wrote.
        target_source = await _object_source(a.pool, target_id)
        await mint_cites(a, obj_id, target_id, source, origin="prose",
                         self_referential=(target_source is not None
                                          and target_source == source))
    return skipped


# Backfill source, distinct from live capture's `_SOURCE` ("session"): the
# same trust tier (SELF_DECLARED, below), just a provenance-traceable marker that this
# particular decided_in edge was minted by the backward pass, not at the decision's own
# birth.
_BACKFILL_SOURCE = "decided_in-backfill"


# THE BACKFILL: the citation scan above runs only inside record_decision, at write
# time, a decision that cites a
# commit the gitlog ingest hasn't reached yet resolves to nothing, and nothing ever
# retries it (a gap that fired in first production use). The resolution was premature,
# not wrong, a race, not an
# ambiguity, so re-running the identical matcher later, once the referent has actually
# arrived, succeeds not because it got smarter but because the world caught up. That is
# what makes a plain backward pass safe: mechanical, idempotent, and re-runnable without
# limit. The other starvation mode (duplication) does not share this property and
# stays gated behind explicit approval; this function never touches it.
async def backfill_decided_in(
    actions: Actions, *, dry_run: bool = False,
) -> dict[str, Any]:
    """A backward pass over every active Decision, minting the `decided_in` edges the live
    path (`record_decision`, above) only ever mints going forward, at record time. Reuses
    `_cited_commit_shas`/`_resolve_commit` unchanged, same regex, same prefix match, same
    silent-skip-on-miss discipline, run backward instead of forward, so a citation gets a
    second chance once its commit has since been ingested.

    Each edge is minted through its own `create_link` call, outside any enclosing
    transaction (unlike `record_decision`'s single atomic block), deliberately: over
    hundreds of Decisions, one giant transaction would hold a lock the whole pass and turn
    any single unexpected error into a full rollback of edges that were each independently
    correct. `create_link` already wraps itself in its own transaction (`Actions._tx`), so
    a mid-pass death leaves every edge minted so far intact: exactly what makes a re-run
    safe: already-linked pairs are skipped (below), never re-minted.

    `dry_run=True` (the default) counts what would mint without writing anything.
    `scripts/backfill_decided_in.py` defaults to this, `--apply` flips it, matching this
    codebase's existing backfill convention (`backfill_seat_bindings.py`).

    Graded SELF_DECLARED, same as the live path (`_EC`): this is not a new inference over
    the prose (that would be DERIVED, the miner's tier): it is the exact same deterministic
    extraction the original decider's own self-declared citation already licensed, delayed
    only by timing. `source` is `_BACKFILL_SOURCE`, distinct from live capture's "session",
    so provenance can tell a backfilled edge from one minted at the decision's own birth,
    without changing its trust tier.

    Returns `{"scanned": N, "minted": N, "already_had": N, "skipped": [...]}`. `skipped`
    names every citation that resolved to nothing (a hard requirement: a
    backfill that reports a mint count and stays silent about what it could not resolve
    repeats a known instrument-dishonesty bug). A skip here means the cited
    commit has never been gitlog-ingested at all (a typo, or a repo the fleet doesn't
    track); the backward pass has no scan-order effect on that (unlike the forward path's
    genuine race), so a skip today stays a skip on every future re-run unless that exact
    commit is later ingested."""
    rows = await actions.pool.fetch(
        "SELECT o.id, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS summary, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='rationale' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS rationale, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='protocol' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS protocol "
        "FROM objects o WHERE o.type='Decision' AND o.status='active' "
        "AND o.merged_into IS NULL"
    )
    observed = datetime.now(UTC)
    minted = 0
    already_had = 0
    skipped: list[dict[str, str]] = []
    for row in rows:
        for sha in _cited_commit_shas(row["summary"], row["rationale"], row["protocol"]):
            commit_id = await _resolve_commit(actions.pool, sha)
            if commit_id is None:
                skipped.append({"decision": str(row["id"]), "sha": sha})
                continue
            exists = await actions.pool.fetchval(
                "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='decided_in'",
                row["id"], commit_id)
            if exists:
                already_had += 1
                continue
            minted += 1
            if not dry_run:
                await actions.create_link(row["id"], commit_id, "decided_in",
                                          _BACKFILL_SOURCE, observed, _CONF,
                                          evidence_class=_EC)
    return {"scanned": len(rows), "minted": minted, "already_had": already_had,
            "skipped": skipped}


# A cross-house find: `link_repo` find-or-created a
# SoftwareProject from any caller-supplied `repo` string, with zero validation: pass
# "ballgem" and it resolves; pass "/home/asuramaya/code/ballgem" and it silently mints a
# second, bogus project, because the caller (and the graph) have no way to tell a project
# name from a path to one. Every legitimate minting path in this codebase (gitlog.py's
# `ingest_repo`, sessions.py's `_repo_from_cwd`, neighborhoods.py's `census_trees`) derives
# the name as a bare directory basename (`Path(...).name`), never a path, never empty,
# never a stray punctuation character standing in for a name that was never resolved. This
# is that shape, positively defined: a boundary that refuses anything that isn't a
# well-formed project ref, rather than trying to cleverly widen to accept more shapes.
# The design constraint: a caller can act on an object it already knows
# about, and cannot discover one it doesn't: a caller that doesn't already know the
# name cannot be allowed to conjure a new project from an arbitrary string.
_REPO_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")


def _validate_repo_name(name: str, raw: str) -> None:
    """Raise ValueError, naming what was wrong, when `name` (the repo string with any
    `repo:` prefix already stripped) is not a well-formed project ref. `raw` is the
    caller's original, unstripped string, quoted in the message so the refusal is legible
    even when the stripped form alone wouldn't explain it (an all-whitespace `raw` strips
    to an empty `name`)."""
    if not _REPO_NAME_RE.fullmatch(name):
        raise ValueError(
            f"repo must be a bare project name, not {raw!r} — pass the project's own name "
            "(e.g. its directory basename), never a filesystem path or a placeholder; "
            "find-or-create refuses anything that isn't a well-formed project ref"
        )


async def _resolve_repo(pool: asyncpg.Pool, name: str) -> uuid.UUID | None:
    """An active SoftwareProject by its `name` property, its `repo:<name>` canonical, or,
    last, a retired canonical (object_aliases, appended when a rename migrated the
    canonical): the alias names the same object, so a hand-typed old
    spelling resolves instead of minting a stub."""
    found = await pool.fetchval(
        "SELECT o.id FROM objects o WHERE o.type='SoftwareProject' AND o.status='active' AND ("
        "  o.canonical = $1 OR o.canonical = $2 OR EXISTS ("
        "    SELECT 1 FROM current_assertions a WHERE a.object_id=o.id "
        "    AND a.name='name' AND a.value #>> '{}' = $3)) LIMIT 1",
        name, f"repo:{name}", name,
    )
    if found is not None:
        return found  # type: ignore[no-any-return]
    return await _resolve_repo_alias(pool, name)


async def _resolve_repo_alias(pool: asyncpg.Pool, name: str) -> uuid.UUID | None:
    """The active SoftwareProject whose retired canonical is `repo:<name>` (object_aliases),
    or None. The read half of "an alias is never a stub"; `_resolve_repo` falls back to it."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT o.id FROM object_aliases al JOIN objects o ON o.id=al.object_id "
        "WHERE al.type='SoftwareProject' AND al.alias=$1 AND o.status='active'",
        f"repo:{name.removeprefix('repo:')}")


async def _resolve_repo_by_remote(pool: asyncpg.Pool, remote_url: str) -> list[uuid.UUID]:
    """Active SoftwareProjects whose current `remote_url` assertion matches, a
    location fallback (census_trees), used only after
    a name lookup (`_resolve_repo`) finds nothing. Never an identity signal on its own: a
    fork whose origin was never repointed shares its parent's remote_url too (a known
    local-git-fork-detection blind spot), which is exactly why a caller here must treat
    more than one hit as ambiguous and refuse rather than pick: this function reports the
    candidate set, it never chooses among them."""
    rows = await pool.fetch(
        "SELECT o.id FROM objects o WHERE o.type='SoftwareProject' AND o.status='active' "
        "AND EXISTS (SELECT 1 FROM current_assertions a WHERE a.object_id=o.id "
        "AND a.name='remote_url' AND a.value #>> '{}' = $1)",
        remote_url,
    )
    return [r["id"] for r in rows]


async def _mint_or_find_repo(
    actions: Actions, repo: str, observed: datetime,
    *, source: str = _SOURCE, evidence_class: str = _EC, confidence: float = _CONF,
) -> uuid.UUID:
    """The validated find-or-create at the center of every legitimate SoftwareProject mint:
    refuses a path-shaped or otherwise malformed `repo`, before any object is
    touched, via `_validate_repo_name`, the single choke point. `link_repo` wraps this
    with its own `in_repo` edge, for the common case of attaching a captured Decision/
    Thread to its project. `census_trees` (neighborhoods.py) has no such edge to attach,
    a disk-discovered repo names only itself, so it calls this directly rather than being
    forced through `link_repo`'s object-linking contract to reach the same guard."""
    name = repo.removeprefix("repo:").strip()
    _validate_repo_name(name, repo)
    proj = await _resolve_repo(actions.pool, name)
    if proj is None:  # a stub the eventual gitlog ingest will land on (same repo: canonical)
        proj = await actions.create_or_find_object("SoftwareProject", f"repo:{name}", source)
        # create_or_find_object is idempotent on canonical, but when `_resolve_repo`'s own
        # (narrower) lookup keeps missing an object that already exists under this exact
        # canonical, this branch runs on
        # every call for it, reasserting the identical `name` every time. Check the
        # current value first so a repeat call against an already-named object writes
        # nothing.
        current_name = await actions.pool.fetchval(
            "SELECT a.value #>> '{}' FROM current_assertions a "
            "WHERE a.object_id=$1 AND a.name='name' AND a.source_id=$2 LIMIT 1",
            proj, source)
        if current_name != name:
            await actions.assert_property(proj, "name", name, source, observed, confidence,
                                          evidence_class=evidence_class)
    return proj


async def link_repo(
    actions: Actions, obj_id: uuid.UUID, repo: str, observed: datetime,
    *, source: str = _SOURCE, evidence_class: str = _EC, confidence: float = _CONF,
) -> dict[str, Any] | None:
    """Attach a captured Decision/Thread to its project. A session item has no commit, so
    it links `in_repo` -> the SoftwareProject directly (the miner's `decided_in`->Commit->
    `in_repo` chain collapsed by one hop). Find-or-create on `repo:<name>` so the link
    always lands, and a decision recorded before the repo is ingested pre-attaches to the
    same object gitlog will later find-or-create. The edge is deduped (re-capture is a no-op).
    The session-miner reuses this with its own source + DERIVED grade, one implementation,
    two trust tiers (the same split capture/miner already have). `repo` must be a bare
    project name (`_validate_repo_name`): a path-shaped or otherwise malformed
    string refuses here, before any object is touched, so a caller can never mint a bogus
    project by accident; this validates first so the failure never depends on transaction
    rollback to stay clean.

    THE MINT CONFESSION: an earlier fix was scoped as a narrower addition rather than
    a broader refuse-not-mint rewrite that had been reverted, since the choke point was
    already correct and the actual root cause was a same-source clobber elsewhere, not
    a capture-path resolution gap. A hand-typed
    `repo=` that finds nothing genuinely new mints silently, same as it always has, this
    just stops it being silent. Returns `{"minted_project": "repo:<name>", "confession":
    "no project named <name> existed; minted"}` when this call minted the object (a
    resolve-check immediately before the find-or-create, good enough for a confession,
    never load-bearing for correctness the way a write-path check would need to be: a
    concurrent racer minting the same name between the two reads produces, at worst, a
    confession on the call that lost the race too, never a wrong object or a missed
    link), else `None`. Every existing caller that ignores this return value keeps
    working exactly as before; the confession is additive, never a behavior change."""
    name = repo.removeprefix("repo:").strip()
    _validate_repo_name(name, repo)  # raises before the resolve-check if malformed
    pre_existing = await _resolve_repo(actions.pool, name) is not None
    proj = await _mint_or_find_repo(actions, repo, observed, source=source,
                                    evidence_class=evidence_class, confidence=confidence)
    exists = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='in_repo' "
        "AND (valid_until IS NULL OR valid_until > now()) LIMIT 1",
        obj_id, proj,
    )
    if not exists:
        await actions.create_link(obj_id, proj, "in_repo", source, observed, confidence,
                                  evidence_class=evidence_class)
    if not pre_existing:
        return {"minted_project": f"repo:{name}",
                "confession": f"no project named {name!r} existed; minted"}
    return None


# THE DECLARE-OR-REFUSE GATE: only the link kinds a door can know about at its own atomic
# commit point are legal here.
# repo=/grounds=/resolves= mint inside record_decision's own `actions.atomic()` block.
# obsoletes=/confirms=/refutes=/implements=/rediscovers=/bears_on=/narrows=/cites= also now
# mint inside that same block, but none of the eight ever joined `kinds_in_scope` below:
# folding where a link mints changed the crash-window guarantee, not this gate's own scope,
# which record_decision's wrapper never widened when the first six landed either. open_
# thread only ever has "repo" in scope: its own `resolves=` closes a different, pre-existing
# thread, after its atomic block.
# "holds"/"works_in" (kind == link_type, no renaming needed) were added for the post-mint
# invariant below: confirm_or_confess_link's own two
# callers, claim_name (Seat) and register_agent (Agent), never go through
# _enforce_required_links itself (they mint across more than one phase, no single
# actions.atomic() block to refuse-and-rollback inside), but they still write
# derivation_abstained_<link_type> through the same _confess_abstention helper, which reads
# this table for every kind it is given. resolve_project_orphans adds a third
# identity entry, "in_repo": "in_repo": its own confirm_or_confess_link call already uses
# "repo"'s target link type directly (a SoftwareProject orphan has no per-door "repo" kind
# of its own to name), so it needs the same "kind == link_type" shape the other two use,
# not the existing "repo"->"in_repo" entry (that one's key is the door-side kind word
# record_decision's callers pass, never the bare link type this lane already has in hand).
#
# DIRECTION: every
# entry above checks an outgoing link, `WHERE from_id=obj_id`, the object being minted
# pointing at its own repo/grounds/holder. "authoring_run" below is the first incoming
# entry: an Artifact does not point at its own producing AgentRun, the run points at the
# artifact (`produced`, run -> artifact), so satisfying this kind means `WHERE
# to_id=obj_id`. Reuses confirm_or_confess_link's own "from"/"to" vocabulary (that
# function already needed both directions for the post-mint invariant above) rather than
# inventing a second one: one direction word, two callers.
_REQUIRED_LINK_KIND_TABLE: dict[str, tuple[str, str]] = {
    "repo": ("in_repo", "from"), "grounds": ("grounded_by", "from"),
    "resolves": ("answers", "from"), "holds": ("holds", "from"),
    "works_in": ("works_in", "from"), "in_repo": ("in_repo", "from"),
    "authoring_run": ("produced", "to"),
}
# A later fold-in caught an in_repo KeyError during a rebase: this used to also carry
# "holds": "holds",
# "works_in": "works_in", "in_repo": "in_repo": three identity entries added by hand, one
# per new confirm_or_confess_link caller, because a caller passing its own real link_type
# as `kind` (rather than one of the three door-side shorthand words above) got a KeyError
# unless someone remembered to add it. Both lookup sites below now fall back to treating
# an unrecognized `kind` as the link_type itself (`.get(kind, kind)`): a caller naming its
# own real link_type directly just works, no hand-added entry, ever again.


async def _confess_abstention(
    a: Actions, obj_id: uuid.UUID, kinds_in_scope: tuple[str, ...], unlinked_because: str,
    source: str, observed: datetime,
) -> None:
    """THE HATCH DUAL-WRITES, widening the gate to
    every object-minting door: `unlinked_because`/`unlinked_because_kind` stay exactly as
    they are, the countable hatch `adoption_meter._hatch_counts` already reads, untouched.
    This also writes a `derivation_abstained_<link_type>` record per link kind in this
    door's own `kinds_in_scope`, in the same shape `derive_or_abstain` already uses (see
    above), so orphan_census/graph_lint's 'orphan' check and any future miner see one
    abstention shape regardless of whether the hand that abstained was a derivation join
    or a caller's own declared reason, never two conventions a reader has to know apart.

    Written unconditionally for every kind in `kinds_in_scope`, not gated on whether this
    type's `required_link_kinds` currently arms that kind, the same principle the
    `unlinked_because` hatch itself already follows a few lines below this call: arming
    enforcement later must never retroactively silence a gap that was already confessed.

    `candidate_count=0, candidates=[]`: this is a declared reason, not an ambiguous lookup
    with a real candidate set; there is nothing to keep for a future miner to re-visit,
    unlike derive_or_abstain's own abstention. Graded SELF_DECLARED (`_CONF`/`_EC`, this
    module's own top-level constants), not derive_or_abstain's DIRECT_OBSERVATION tier:
    a caller confessing a gap is a declaration, not a deterministic join over facts the
    graph already asserts."""
    for kind in kinds_in_scope:
        if kind in _REQUIRED_LINK_KIND_TABLE:
            link_type, _direction = _REQUIRED_LINK_KIND_TABLE[kind]
        else:
            link_type = kind
        await a.assert_property(
            obj_id, f"derivation_abstained_{link_type}",
            {"link_type": link_type, "candidate_count": 0, "reason": unlinked_because,
             "candidates": []},
            source, observed, _CONF, evidence_class=_EC)


# THE POST-MINT INVARIANT: claim_name -> ensure_seat/bind_holder and register_agent
# mint an object across
# more than one phase under an advisory lock (_seat_lock / mint_lock), never one
# actions.atomic() block; the raise-and-rollback gate (_enforce_required_links,
# just above) cannot reach across that gap; there is no single transaction left to roll
# back by the time the natural link would be missing. So this checks instead of refuses:
# the object this mint produced ends up linked or confessed, never a silent third state.
# The heartbeat sub-sweep (post_mint_orphan_sweep, seats.py) is the other half: it
# catches whatever this in-line call missed because the process died between phases,
# before this ever ran.
async def confirm_or_confess_link(
    actions: Actions, obj_id: uuid.UUID, link_type: str, *, direction: str = "from",
    reason: str, source: str, observed: datetime,
) -> bool:
    """Real link checked first, same ordering _enforce_required_links already uses (never
    poison the hatch's own count with a confession that never needed it). `direction`
    picks which side of the link `obj_id` sits on: "from" (this object -> its natural
    target, e.g. Agent -> SoftwareProject via works_in) or "to" (the target points at
    this object, e.g. Agent -> Seat via holds, the Seat is `obj_id`, but it never
    initiates the link itself). UNLIKE _enforce_required_links' own satisfied-check, this
    filters on valid_until: `holds` is routinely invalidated (a vacated seat, a healed
    prior holder) and a dead link must never read as still-satisfied, repo/grounds/
    resolves are close to append-only in practice, which is why that check never needed
    the filter; `holds` cannot make the same assumption.

    Idempotent: an object that already carries a live, non-resolved `unlinked_because` or
    `derivation_abstained_<link_type>` is left alone, never re-confessed on every repeat
    mount or heartbeat tick. `link_type` doubles as `_confess_abstention`'s own `kind` key
    (see _REQUIRED_LINK_KIND_TABLE): callers here always pass the bare link type, not a
    softer abstraction, since these two callers each have exactly one natural link and
    nothing to disambiguate.

    Returns True when this call wrote a fresh confession, False when the object was
    already linked or already confessed: the heartbeat sweep's own receipt counts on
    this to know how many objects it actually touched, vs. how many it merely re-checked."""
    col = "from_id" if direction == "from" else "to_id"
    satisfied = await actions.pool.fetchval(
        f"SELECT 1 FROM links WHERE {col}=$1 AND type=$2 AND evidence_class=$3 "
        "AND (valid_until IS NULL OR valid_until > now()) LIMIT 1",
        obj_id, link_type, EvidenceClass.SELF_DECLARED.value)
    if satisfied:
        return False
    already_confessed = await actions.pool.fetchval(
        "SELECT 1 FROM current_assertions WHERE object_id=$1 AND ("
        "  name = 'unlinked_because' OR "
        "  (name = $2 AND NOT (value ? 'resolved'))"
        ") LIMIT 1",
        obj_id, f"derivation_abstained_{link_type}")
    if already_confessed:
        return False
    await actions.assert_property(obj_id, "unlinked_because", reason, source, observed,
                                  _CONF, evidence_class=_EC)
    await actions.assert_property(obj_id, "unlinked_because_kind", "standalone", source,
                                  observed, _CONF, evidence_class=_EC)
    await _confess_abstention(actions, obj_id, (link_type,), reason, source, observed)
    return True


async def _enforce_required_links(
    a: Actions, obj_id: uuid.UUID, type_name: str, *, kinds_in_scope: tuple[str, ...],
    unlinked_because: str | None, source: str, observed: datetime,
    unlinked_because_kind: str | None = None,
) -> None:
    """Called at the end of a mint's own atomic block, still inside it: a raise here
    triggers the caller's real `conn.transaction()` rollback (Actions.atomic's own
    docstring), so a refusal leaves no orphan object, a genuine refuse-at-door rather
    than a post-hoc alarm. `kinds_in_scope` is this door's own atomically-knowable
    subset (see _REQUIRED_LINK_KIND_TABLE above): a type's declared requirement outside
    that subset is silently not checked by this call (a different door checks it against
    its own scope). `unlinked_because`, when given, is the mandatory countable hatch,
    asserted as a fact on the object in this same transaction, and satisfies the gate
    outright. Otherwise, satisfied only by a SELF_DECLARED-graded link already visible on
    this connection (this call's own writes above included, via the same transaction);
    a DIRECT_OBSERVATION/DERIVED-graded link (e.g. a mount-defaulted repo=) never counts.

    `unlinked_because_kind` (a structural fix, not a better prose match): a separate,
    non-prose property asserted
    alongside `unlinked_because` in the same transaction, `"extension_link_pending"` when
    the caller already knows this write's only requested connectivity is an extension-
    link param (obsoletes=/confirms=/.../cites=) pending mint after this transaction, else
    `"standalone"`. This is the actual discriminator `adoption_meter._hatch_counts` reads
    now, never a re-parse of `unlinked_because`'s own prose text against a reason
    constant whose wording keeps growing a new param name. The caller (record_decision's
    MCP wrapper) already computes this exact boolean at the moment it decides whether to
    substitute `_EXTENSION_LINK_PENDING_REASON` for the prose, passed straight through,
    never re-derived here from the string. Defaults to `"standalone"` when omitted (every
    other caller of this function, open_thread has no extension-link params of its own
    to be pending on), so the metric stays explicit rather than inferred.

    DUAL-WRITE: every hatch
    fire (both branches below) also writes `derivation_abstained_<link_type>` for each kind
    in `kinds_in_scope`, via `_confess_abstention`, see its own docstring. `unlinked_because`
    /`unlinked_because_kind` themselves are unchanged, still the metric adoption_meter
    reads; this is an addition, never a replacement, no wholesale rename.

    DIRECTION: the
    satisfied-check below reads each kind's own direction from `_REQUIRED_LINK_KIND_TABLE`
    ("from": obj_id is the link's source, the shape every kind used before this arc, or
    "to": obj_id is the link's target, e.g. "authoring_run": an Artifact never points at
    its own producing AgentRun, the run points at the artifact). Purely additive: every
    pre-existing kind stays "from", byte-for-byte the same query it always ran."""
    # ONE bound connection for this whole call, catalog read included, not a.pool.
    # object_type/fetchval would acquire a different connection from the same pool while
    # this atomic() caller's own connection is still held open, and under concurrent
    # xdist load with a small pool that is a real deadlock (every atomic() caller
    # blocked needing an (N+1)th connection none of them can ever free), the exact
    # class create_or_find_object's own comment already names, hit live here (found via
    # a hung test_deploy_guard.py boot-check run, not by reasoning). a._read() reuses
    # the bound connection when inside atomic(), same discipline Actions' own read
    # helpers (resolve_object_id, current_values) use, and it also fixes the read-
    # committed-isolation gap noted below: inside an open transaction, a fresh
    # connection would silently miss this same call's own uncommitted writes above.
    async with a._read() as conn:
        # UNCACHED, DELIBERATELY (found live: under full-suite concurrent load,
        # catalog.object_type's process-wide fingerprint cache served a stale empty
        # required_link_kinds for a type this same test had just declared moments
        # earlier, a stale-empty read here doesn't misrender a UI, it silently
        # disables the whole gate. The refuse-check's own correctness must never
        # depend on a cache invalidating in time; read the live property directly.
        raw = await conn.fetchval(
            "SELECT a.value FROM objects o JOIN current_assertions a "
            "ON a.object_id = o.id WHERE o.type = 'Type' "
            "AND o.canonical = $1 AND a.name = 'required_link_kinds' "
            "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1",
            f"type:object:{type_name}")
        required = [k for k in (raw or ()) if k in kinds_in_scope]
        if not required:
            if unlinked_because:
                # THE HATCH IS A DECLARATION, NOT JUST A REFUSAL-AVOIDER: a caller who
                # knows it has no repo, the boot-alarm
                # watchdog, a service-scoped claim with no SoftwareProject to name, should
                # get to say so honestly whether or not this type is currently enforced.
                # Recording it here, unconditionally, means arming required_link_kinds
                # later never retroactively silences a gap that was already confessed.
                await a.assert_property(obj_id, "unlinked_because", unlinked_because,
                                        source, observed, _CONF, evidence_class=_EC)
                await a.assert_property(obj_id, "unlinked_because_kind",
                                        unlinked_because_kind or "standalone", source,
                                        observed, _CONF, evidence_class=_EC)
                await _confess_abstention(a, obj_id, kinds_in_scope, unlinked_because,
                                          source, observed)
            return  # unenforced for this type (the common case in this pass), or
                    # nothing this door can even attest to: not this call's problem
        # REAL LINKS CHECKED FIRST, the hatch only as a fallback: if `unlinked_because`
        # were checked before this, a caller who
        # passed both a real satisfying link and a (possibly machine-set)
        # unlinked_because would take the hatch branch anyway, poisoning the hatch
        # count, the arc's only metric, with writes that never needed it.
        for kind in required:
            if kind in _REQUIRED_LINK_KIND_TABLE:
                link_type, direction = _REQUIRED_LINK_KIND_TABLE[kind]
            else:
                link_type, direction = kind, "from"
            col = "from_id" if direction == "from" else "to_id"
            satisfied = await conn.fetchval(
                f"SELECT 1 FROM links WHERE {col}=$1 AND type=$2 AND evidence_class=$3 "
                "LIMIT 1", obj_id, link_type, EvidenceClass.SELF_DECLARED.value)
            if satisfied:
                return
    if unlinked_because:
        await a.assert_property(obj_id, "unlinked_because", unlinked_because, source,
                                observed, _CONF, evidence_class=_EC)
        await a.assert_property(obj_id, "unlinked_because_kind",
                                unlinked_because_kind or "standalone", source, observed,
                                _CONF, evidence_class=_EC)
        await _confess_abstention(a, obj_id, kinds_in_scope, unlinked_because, source,
                                  observed)
        return
    raise ValueError(
        f"{type_name} refused: none of its required link kinds ({', '.join(required)}) "
        "were declared (a link a caller ASSERTED, not one this server derived/observed) "
        "— link one, or pass unlinked_because=<reason> to record the gap as a countable "
        "fact instead of a silent hole (task #189, decision 7ea187b9).")


async def record_decision(
    actions: Actions, summary: str, *, kind: str = "ruling",
    rationale: str | None = None, repo: str | None = None, source: str = _SOURCE,
    grounds: list[uuid.UUID] | None = None, protocol: str | None = None,
    supersedes: str | None = None, resolves: str | list[str] | None = None,
    repo_evidence_class: str | None = None, unlinked_because: str | None = None,
    implements: uuid.UUID | None = None, confirms: list[uuid.UUID] | None = None,
    rediscovers: list[uuid.UUID] | None = None, bears_on: list[uuid.UUID] | None = None,
    narrows: list[uuid.UUID] | None = None, cites: list[uuid.UUID] | None = None,
    refute_id: uuid.UUID | None = None, obsoletes: list[str] | None = None,
    unlinked_because_kind: str | None = None, operator_authorized: bool = False,
) -> uuid.UUID:
    """Capture a decision at the moment it is made: the why, declared, not mined.

    `operator_authorized` (rulings carry
    authorized_by to the operator Person object; the edge itself was renamed
    `ruled_by` once `authorized_by` turned out already spoken
    for by the work-lineage build's own run's-plan sense): an explicit,
    caller-declared
    act, never inferred from `source`. `source` records who typed this into the graph
    (a fleet agent's own id, or the bare `session` for an unmounted tab); it does not
    distinguish the operator's own words from a seat holder's own scoped judgment call,
    since a human-attended seat (a manager seat the
    operator types directly through) writes under its own mounted agent id, never a
    special sentinel. NO AUTO-DETECT, matching the sibling citation ruling's own "no
    auto-cite, ever" principle: a caller (typically a human-attended seat relaying an
    actual operator ruling) sets this explicitly when, and only when, the decision being
    recorded truly represents the operator's own authority, never guessed from
    `source`/`kind`.

    `kind` labels it the way the miner does (ruling / reset / override / rejection /
    choice / decision). `rationale`, if given, is the reasoning stored inline (an
    enrichment the miner can't produce, it only has the commit body). `repo` files the
    decision under a SoftwareProject. `source` is the attributing actor, the static
    `session` for a lone operator, or `agent:<session>` for a fleet member so provenance
    records which instance decided (still SELF_DECLARED, still the high-trust channel).

    `implements`/`confirms`/`rediscovers`/`bears_on` (closing part
    of a previously named partial-commit gap): pre-resolved ids, minted inside this same atomic
    transaction; the object and every one of these now either all land or none do, a
    crash mid-sequence can no longer leave a Decision with some but not all of what it
    asked for. The caller (mcp_server's wrapper) still owns resolving a bare ref/short-id
    to one of these; this function's own strictness laws (UUID/canonical/short-id only,
    no prose fallback) apply at that resolution step, same as always; a wrong-typed id
    here is the caller's bug, not this function's to catch twice.
    `narrows` joins the same fold,
    a pure link mint, same shape as `rediscovers`. Points from this decision to an
    earlier one whose scope it bounds without refuting or superseding it: the earlier
    ruling's own measurement stays correct within its now-visible limit. Non-burying by
    construction, same guarantee `mint_rediscovers` already proves: no property write on
    either side, no `status` touch, no path that could gray the target out of orient's
    recent list the way `supersedes` deliberately does. See `mint_narrows` below.
    `cites` joins the same
    fold; the door `bears_on` (Decision->Thread only) and `narrows` (bounds scope, the
    wrong relation) both correctly refused: a decision that adds a new facet to an
    earlier one's ongoing finding, neither bounding, refuting, nor independently re-
    deriving it. Reuses the existing `cites` link type (already legal Decision->Decision,
    already carrying the origin=prose|declared split) rather than a new edge; the
    prose-citation miner already mints this exact relation when an author's own
    qualifier-worded prose happens to match; this is the same edge, declared explicitly
    (`origin="declared"`) for a caller who names the target on purpose without the
    qualifier-word convention. `self_referential` computed the same way `_mint_prose_
    citations` already does: the target's own asserted source, compared to this call's.
    `refute_id`/`obsoletes` (closing a previously named partial-commit debt): a
    resolved Practice UUID and the list of
    workaround statements this decision kills, folded into this same atomic block;
    the object, its `refuted_by` stamp, and every dead Superstition either all land or
    none do. The regression an earlier fold-attempt caught (the wrapper's "this
    overturns Practice X" message going silent, or worse, misreading the freshly-
    converted Superstition as a live workaround being revived) was in the search-based
    classification downstream, not in this write itself; the wrapper (mcp_server.py)
    now decides the overturning flag directly from `refute_id`, never from whether its
    own prior-art search still finds the Practice unrefuted, and excludes both
    `refute_id`'s and every `obsoletes` entry's own about-to-exist Superstition from
    that search by canonical lookup so neither can self-collide as a false "reviving a
    dead workaround" hit against the very call that is killing it. `refute_id` is the
    caller's own pre-resolved id (same UUID/canonical/short-id-only strictness as
    `implements`, resolved by the wrapper before this call; a refutation that cannot
    name its target has refused before reaching here, same as before this fold).

    `repo_evidence_class` grades the `in_repo` link only, never the decision itself: a
    caller who typed `repo=` is testifying to it (default, SELF_DECLARED, unchanged).
    A caller who had it defaulted from mount state (an orphan-door fix, the
    MCP wrapper's job, not this function's) never asserted this fact about this object;
    the server observed its own live mount table, which is DIRECT_OBSERVATION (0.6), not
    a ninth-tenths-confident declaration. Landing both paths at SELF_DECLARED (the
    original shape) would launder an inference into a declaration; the next census reads
    the graph as healed while the link is a guess wearing a citation. Pass the class
    explicitly when defaulting; omit it when the caller declared.

    `unlinked_because` is the declare-or-refuse gate's
    mandatory countable hatch: if this type declares required link kinds in the catalog
    and none are satisfied by a SELF_DECLARED
    link (repo=/grounds=/resolves=, a mount-defaulted or otherwise derived link never
    counts), the write refuses unless this is given. When given, it is recorded as a
    fact on the object in the same transaction and the write proceeds; this is the
    metric the whole arc is measured by, so name a real reason, not a placeholder.
    `unlinked_because_kind` is the non-prose companion; pass
    `"extension_link_pending"` when this write's only requested connectivity is an
    extension-link param (obsoletes=/confirms=/.../cites=) pending mint after this
    transaction, else omit it. `adoption_meter._hatch_counts` reads this, never a
    re-parse of `unlinked_because`'s own text; a prose match silently drifts every
    time this constant's wording grows a new param name, this field cannot.

    `grounds` cites the Reference objects the decision rests on, `grounded_by` edges
    minted at birth, so the citation carries the decider's grade instead of being
    reconstructed later from prose. `decided_in` needs no parameter of its own:
    any commit sha already named in `summary`/`rationale`/`protocol` ("commit 238b48f") is
    resolved by prefix against an ingested Commit and linked automatically, silently
    skipped, never guessed, when the sha doesn't (yet) match anything. Idempotent on the
    summary hash, and, when `repo` is
    given, on a near-duplicate reword of it too (fixing a retry-after-
    ambiguous-failure bug: a rejected-but-actually-committed call, retried with the summary
    reworded by one word, minted a duplicate). `find_near_duplicate_decision` runs first; a hit
    reuses that live decision's id instead of minting, exactly as `find_near_duplicate_open_
    thread` does for threads. The decision named by `supersedes` is excluded from that
    lookup: a correction restates its subject by nature, so it
    is the highest-risk case for this guard, not the lowest, see find_near_duplicate_
    decision's own docstring for the failure this exclusion prevents. Returns the id.

    `supersedes` buries an earlier decision under this one: the old decision is stamped
    superseded_by/-because,
    property assertions, event-sourced, unwindable by re-asserting "", and this one is
    stamped supersedes, so the correction navigates both ways. The lens does the graying:
    superseded decisions leave orient's recent list; the decision-log renders them with
    their successor. Never a delete: the wrong hypothesis stays readable under its
    correction. UUID, CANONICAL, OR SHORT-ID ONLY (the same law `resolves`
    follows below): a free-text/prose ref no longer falls through to a summary-substring
    match; burying a decision is an addressing act, and an identifier-shaped-but-wrong
    arg must refuse fleet-wide rather than search for something it merely resembles.
    Raises ValueError when the ref matches nothing (the new decision is not recorded:
    a correction that can't name its target is not yet a correction).

    `resolves` closes the thread this decision answers, in the same act: mints `answers`
    and marks the thread resolved. Until this existed, capture had a one-way valve: the
    answer landed and the question stayed lit, because closing was a separate verb that a
    dying session forgets. A ruling was made on a lineage question; the
    decision recording that ruling announced "resolving thread <id>" in prose, nothing
    in the code read the prose, and the graph went on asking about a question already
    answered for a full day. open_thread(question) -> record_decision(answer)
    is the fleet's most common write; the close belongs inside the answer, not beside it.
    A ruling that can name its question should not need a second verb to finish the sentence.
    Same strictness as `supersedes`: a ref that matches nothing raises, and nothing is
    recorded: a ruling that miscites the question it settles has not settled it.

    UUID, CANONICAL, OR SHORT-ID ONLY (fixing several documented instances of a stray-but-
    valid short id closing the wrong thread; the cure is
    refuse, not widen): unlike `supersedes`/`implements`/`refutes`, `resolves` no longer
    falls through to a free-text summary-substring match; an addressing act that closes
    a thread must name it exactly, never guess from prose. This does not, and cannot,
    catch a valid id naming the wrong thread (no matcher can refuse a syntactically
    correct citation without knowing intent), see the MCP tool's receipt, which now
    surfaces the matched thread's own summary for both the single-ref and list forms, so
    a caller sees what they just closed in the same turn instead of a day later.

    `resolves` also takes a list: a delegation folds
    the set of threads it supersedes, not just one, since "thread ownership doesn't transfer
    with a delegation" left threads hand-closed twice, across two sessions, because
    the single-ref form could only ever name one. Each entry resolves independently through
    the same matcher as the single-ref form. Unlike the single ref, a list entry that
    matches nothing does not abort the call; it would defeat the point of folding a set to
    have one typo veto the other nine, but it is never swallowed either: the caller (the
    MCP tool) is the one that names, per entry, what closed and what didn't, since this
    function's return stays a bare id (additive shape only, ~20 existing call sites bind
    it as a plain UUID). A single string keeps the original strictness byte-for-byte:
    matches nothing → raises, nothing recorded."""
    observed = datetime.now(UTC)
    old: uuid.UUID | None = None
    if supersedes:
        # require_identifier=True (the same law resolves= already follows,
        # below): burying a decision under this one is an addressing act, not a search,
        # an identifier-shaped-but-wrong arg (a bare local task number) must refuse
        # fleet-wide rather than fall through to a prose/summary-substring match.
        old = await _find_decision(actions.pool, supersedes, require_identifier=True)
        if old is None:
            raise ValueError(f"supersedes matched no decision: {supersedes!r} — quote its "
                             "UUID, canonical, or 8-char short id (no longer a prose "
                             "match — an addressing act refuses rather than guesses)")
    answered: list[uuid.UUID] = []
    if isinstance(resolves, list):
        for thread_ref in resolves:
            tid = await _find_thread(actions.pool, thread_ref, require_identifier=True)
            if tid is not None:
                answered.append(tid)
    elif resolves:
        single = await _find_thread(actions.pool, resolves, require_identifier=True)
        if single is None:
            raise ValueError(f"resolves matched no thread: {resolves!r} — quote its UUID, "
                             "canonical, or 8-char short id (a prose/summary match no "
                             "longer resolves here — an addressing act refuses rather "
                             "than guesses)")
        answered.append(single)
    # The near-dup lookup, like `_find_decision`/`_find_thread` just above, reads outside the
    # write transaction, a pre-check, not a locked decision. On a hit, `d` below reuses that
    # live decision instead of minting a duplicate; everything else (kind/rationale/protocol/repo/
    # grounds/supersedes/resolves) still runs exactly as it would for a freshly-minted one:
    # only the object itself is deduped, never the structural side effects a caller depends on.
    # `exclude=old`: `supersedes` names the one decision this call must never dedup onto,
    # see find_near_duplicate_decision's docstring for why a correction is the highest-risk
    # case, not a low one.
    dup = (await find_near_duplicate_decision(actions.pool, summary, repo=repo, exclude=old)
           if repo else None)
    # ONE transaction: the Decision, its summary/kind/rationale, and the repo link either all
    # land or none do: a process death mid-sequence can no longer leave a summary-less husk.
    async with actions.atomic() as a:
        d = dup if dup is not None else await a.create_or_find_object(
            "Decision", _canon("decision", summary), source)
        await a.assert_property(d, "summary", summary, source, observed, _CONF,
                                evidence_class=_EC)
        await a.assert_property(d, "kind", kind, source, observed, _CONF,
                                evidence_class=_EC)
        if rationale:
            await a.assert_property(d, "rationale", rationale, source, observed, _CONF,
                                    evidence_class=_EC)
        if protocol:
            # the invocation, not just the conclusion: a common re-derivation gap is a
            # ruling that says what was found but not how
            # to reproduce it. Its own property, never folded into rationale: a protocol
            # buried in prose is a protocol lost.
            await a.assert_property(d, "protocol", protocol, source, observed, _CONF,
                                    evidence_class=_EC)
        if repo:
            rec = repo_evidence_class or _EC
            mint_confession = await link_repo(a, d, repo, observed, source=source,
                                              evidence_class=rec,
                                              confidence=confidence_for(EvidenceClass(rec)))
            if mint_confession:
                logger.warning("record_decision(repo=%r): %s", repo,
                               mint_confession["confession"])
        for ref in grounds or []:
            exists = await a.pool.fetchval(
                "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='grounded_by'",
                d, ref)
            if not exists:  # re-capture is a no-op, like link_repo
                await a.create_link(d, ref, "grounded_by", source, observed, _CONF,
                                    evidence_class=_EC)
        # Mint `decided_in` from a sha already named in the decider's own prose
        # (summary/rationale/protocol): the same edge the miner writes when it finds the
        # decision the other way around (starting from the Commit), now written at birth
        # too instead of waiting on a mining pass that never runs over session capture.
        for sha in _cited_commit_shas(summary, rationale, protocol):
            commit_id = await _resolve_commit(a.pool, sha)
            if commit_id is None:  # not (yet) ingested, or a typo: skip, never guess
                continue
            exists = await a.pool.fetchval(
                "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='decided_in'",
                d, commit_id)
            if not exists:
                await a.create_link(d, commit_id, "decided_in", source, observed, _CONF,
                                    evidence_class=_EC)
        # PROSE-ID -> EDGE: "ruling <id>"/"decision <id>"/"thread <id>"/"obligation <id>"
        # in this
        # decision's own prose becomes a real `cites` edge, same atomic block. Any
        # citation that could not resolve is recorded as a property (never silently
        # dropped), keeping the false-positive surface reportable, same
        # discipline `unlinked_because`/backfill_decided_in's own `skipped` field use.
        prose_skips = await _mint_prose_citations(a, d, source, summary, rationale, protocol)
        if prose_skips:
            await a.assert_property(d, "prose_citation_skips", prose_skips, source,
                                    observed, _CONF, evidence_class=_EC)
        if old is not None and old != d:  # a decision never buries itself (idempotent re-record)
            await a.assert_property(old, "superseded_by", str(d), source, observed, _CONF,
                                    evidence_class=_EC)
            await a.assert_property(old, "superseded_because",
                                    f"superseded by {str(d)[:8]}: {summary[:200]}",
                                    source, observed, _CONF, evidence_class=_EC)
            await a.assert_property(d, "supersedes", str(old), source, observed, _CONF,
                                    evidence_class=_EC)
        for thread_id in answered:
            # the answer and the close in one transaction: a ruling that lands while its
            # question stays open is how a resolved question gets asked twice. Same shape the
            # resolve_thread verb writes (status/resolved_in/resolved_because), so every
            # lens that already renders a resolved thread renders this one unchanged. A
            # batch just runs this once per thread in the set: same act, same transaction.
            exists = await a.pool.fetchval(
                "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='answers'",
                d, thread_id)
            if not exists:
                await a.create_link(d, thread_id, "answers", source, observed, _CONF,
                                    evidence_class=_EC)
            # SINGULAR, same as resolve_thread's own writes above: an answering decision
            # is the same workflow-transition act, so it must
            # collapse every open witness too, not just its own source.
            await a.assert_singular_property(thread_id, "status", "resolved", source, observed,
                                             _CONF, evidence_class=_EC)
            await a.assert_singular_property(thread_id, "resolved_in", source, source, observed,
                                             _CONF, evidence_class=_EC)
            await a.assert_singular_property(thread_id, "resolved_because",
                                             f"answered by decision {str(d)[:8]}: {summary[:200]}",
                                             source, observed, _CONF, evidence_class=_EC)
        # All eight extension-link params now
        # mint in this same transaction as the object itself: pure link/property
        # mints, no side-object creation that anything else this call's own search
        # step reads for the first six; `refute_id`/`obsoletes` do create side objects
        # (a converted-or-fresh Superstition each) but the downstream classification
        # that used to depend on search timing has moved to a direct refute_id check
        # in the wrapper (see this function's own docstring); the atomicity fix and
        # the classification fix are separable, and only the classification one was
        # ever the actual regression risk. Each mint_*/kill_superstition helper below
        # takes a generic Actions and never opens its own atomic() block, so passing
        # `a` (this transaction's own bound Actions) participates correctly, no
        # nested-transaction conflict, verified by reading each one before relying on
        # it here.
        if implements is not None:
            await mint_implements(a, d, implements, source)
        for pid in confirms or []:
            await _witness_link(a, pid, d, source, observed)
        for rdid in rediscovers or []:
            await mint_rediscovers(a, d, rdid, source)
        for bid in bears_on or []:
            await mint_bears_on(a, d, bid, source)
        for nid in narrows or []:
            await mint_narrows(a, d, nid, source)
        for cid in cites or []:
            target_source = await _object_source(a.pool, cid)
            await mint_cites(a, d, cid, source, origin="declared",
                             self_referential=(target_source is not None
                                              and target_source == source))
        if operator_authorized:
            # RULINGS CARRY ruled_by TO THE OPERATOR: see this function's own
            # docstring on `operator_authorized`
            # for why this is an explicit param, never inferred from `source`. Named
            # `ruled_by`, not `authorized_by`, since the work-lineage build
            # already spoke for `authorized_by` as a run's own plan/objective
            # edge; two senses on one edge name is exactly what that principle
            # forbids, so this one was split off under its own name.
            #
            # AUTHORITY BY CHARTER: when this decision names a `repo`,
            # `ruled_by` targets the specific operator Person whose charter covers it,
            # falling back to the singleton `person:operator` only when no
            # operator's charter covers this repo (an operator-authorized decision must
            # never end up with no ruled_by edge just because charter data is
            # incomplete). No `repo` at all: unchanged, always the singleton, there is
            # no project here to scope against.
            operator_id = None
            if repo:
                from src.orchestrator.charter import resolve_operator_authority

                check = await resolve_operator_authority(a.pool, source, project=repo)
                operator_id = check["person_id"] if check["authorized"] else None
            if operator_id is None:
                operator_id = await ensure_operator_person(a, source=source)
            await a.create_link(d, operator_id, "ruled_by", source, observed,
                                _CONF, evidence_class=_EC)
        if refute_id is not None:
            # THE POLARITY FLIP, folded (see refute_practice, now this block's own
            # inline duplicate, kept identical, just sharing `a`/`d`/`observed` instead of
            # opening its own atomic() and receiving killed_by from outside): the
            # Practice stays active, flagged, never retired.
            refuted_statement = await a.pool.fetchval(
                "SELECT val.value #>> '{}' FROM current_assertions val "
                "WHERE val.object_id=$1 AND val.name='statement' "
                "ORDER BY val.confidence DESC, val.observed_at DESC LIMIT 1", refute_id)
            await a.assert_property(refute_id, "refuted_by", str(d), source, observed,
                                    _CONF, evidence_class=_EC)
            await kill_superstition(a, refuted_statement or str(refute_id),
                                    killed_by=str(d), repo=repo, source=source)
        for statement in obsoletes or []:
            if statement and statement.strip():
                await kill_superstition(a, statement, killed_by=str(d), repo=repo,
                                        source=source)
        await _enforce_required_links(
            a, d, "Decision", kinds_in_scope=("repo", "grounds", "resolves"),
            unlinked_because=unlinked_because, source=source, observed=observed,
            unlinked_because_kind=unlinked_because_kind)
    return d


class RefAmbiguous(Exception):
    """Raised by `_resolve_ref` when a short-id prefix genuinely matches more than one
    live object, the one case an earlier fix actually needs a real disambiguation
    list for (an exact canonical match can never be ambiguous: `objects` carries a
    UNIQUE(type, canonical) constraint). `.candidates` is the real, capped list, never
    picked-for-you via an arbitrary LIMIT 1. Orchestrator-level callers that already
    treat a failed ref resolution as an all-or-nothing refusal (record_decision's
    resolves/supersedes, which already raises ValueError on a plain miss) let this
    propagate unchanged: an ambiguous ref deserves a louder failure than a silent guess,
    not a quieter one. MCP tool wrappers that want to render the list to a human catch it
    directly."""

    def __init__(self, ref: str, type_: str, candidates: list[dict[str, str]]) -> None:
        self.ref = ref
        self.type_ = type_
        self.candidates = candidates
        super().__init__(
            f"{ref!r} matches {len(candidates)} {type_} objects by short-id prefix — "
            "quote more characters, or the full UUID, to disambiguate")


async def _resolve_ref(
    pool: asyncpg.Pool, type_: str, ref: str, *, text_field: str,
    require_identifier: bool = False,
) -> uuid.UUID | None:
    """The shared resolution ladder every `_find_*` helper in this module is built on,
    fixing an earlier near-miss: a ref that looks like an
    identifier must resolve deterministically or refuse; it must never silently fall
    through to a fuzzy text search, because a hex-looking string can coincidentally
    substring-match a completely different object's text field (exactly what happened: a
    bare canonical suffix substring-matched a bug-report thread that merely quoted it).

    Ladder: (1) a full UUID, exact `id` match. (2) this type's own canonical scheme
    (`_canon`'s `<type>:<12hex>`, with or without the caller supplying the `type:` prefix),
    an exact `canonical` match, never ambiguous by the UNIQUE constraint. (3) a
    short-id prefix (the pre-existing 8+ hex/dash convention, unchanged), exactly one
    hit resolves it; zero hits refuses (returns None; does not fall through to (4), a
    deliberate behavior change from before this fix, where id-shaped input that missed
    the prefix leg still got a free-text search); two or more hits raises
    `RefAmbiguous` with the real candidates instead of an arbitrary LIMIT 1 pick. Only
    when `ref` matches neither the canonical shape nor the short-id shape at all does
    this fall through to (4), the pre-existing fuzzy `ILIKE` substring match (shortest
    match wins); genuinely free-text queries are exactly as forgiving as before.

    `require_identifier=True` (a search feature
    wired into an addressing path should refuse rather than widen) removes step (4)
    entirely for a call path that closes something rather than merely reading it;
    record_decision's `resolves=` is the one caller that opts in. This does not catch a
    valid-but-wrong short id (several documented instances of a real 8-hex id,
    naming the wrong thread, resolved exactly as designed, since no matcher can refuse a
    syntactically-correct citation without knowing intent); it closes a different,
    genuinely live exposure the docstring's own contract still names today: a bare prose
    phrase silently falling through to ILIKE and closing whatever thread it happens to
    substring-match."""
    try:
        full = uuid.UUID(ref)
    except (ValueError, AttributeError):
        full = None
    if full is not None:
        # EXISTENCE + TYPE CHECKED, NOT ASSUMED: this leg
        # used to return a syntactically-valid UUID unconditionally: a real id belonging
        # to the wrong type, or no object at all, resolved exactly like a genuine hit,
        # unlike every other rung of this same ladder (canonical/short-id both refuse on
        # no match). A caller passed a Decision's own id where a Thread was expected and
        # got silent success instead of the refusal this function's own docstring promises.
        exists = await pool.fetchval(
            "SELECT 1 FROM objects WHERE id=$1 AND type=$2 AND status='active'",
            full, type_)
        return full if exists else None
    raw = (ref or "").strip().lower()
    canon_prefix = f"{type_.lower()}:"
    hex_part = raw[len(canon_prefix):] if raw.startswith(canon_prefix) else raw
    if re.fullmatch(r"[0-9a-f]{12}", hex_part):
        cid = await pool.fetchval(
            "SELECT id FROM objects WHERE type=$1 AND status='active' AND canonical=$2",
            type_, f"{canon_prefix}{hex_part}")
        if cid is not None:
            return uuid.UUID(str(cid))
    if re.fullmatch(r"[0-9a-f]{8}[0-9a-f-]*", raw):
        # ONE ROW PER OBJECT, NEVER PER SOURCE (the fourth sighting of the
        # winning_props/"stuck-open-threads" bug class): a plain
        # `LEFT JOIN current_assertions ON object_id=o.id AND name=$3` fans out to one row
        # per source when 2+ agents have each asserted/reasserted the same text_field (a
        # later triage touch re-stating an identical summary, say), a real, singular
        # object then double-counts as two joined rows and spuriously raises
        # RefAmbiguous. The correlated subquery picks one winning row per o.id (the
        # established tiebreak used elsewhere), so multiplicity in current_assertions can
        # never inflate the object count this ambiguity check is actually testing.
        rows = await pool.fetch(
            "SELECT o.id, "
            "  (SELECT a.value #>> '{}' FROM current_assertions a "
            "   WHERE a.object_id=o.id AND a.name=$3 "
            "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS text "
            "FROM objects o "
            "WHERE o.type=$1 AND o.status='active' AND o.id::text LIKE $2 || '%' LIMIT 6",
            type_, raw, text_field)
        if len(rows) == 1:
            return uuid.UUID(str(rows[0]["id"]))
        if len(rows) > 1:
            raise RefAmbiguous(ref, type_, [
                {"id": str(r["id"]), text_field: r["text"]} for r in rows])
        return None  # id-shaped but matched nothing anywhere: refuse, never fall through
    if require_identifier:
        return None  # not identifier-shaped at all: refuse rather than free-text match
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT o.id FROM objects o JOIN current_assertions a ON a.object_id=o.id "
        "WHERE o.type=$1 AND o.status='active' AND a.name=$3 "
        "AND a.value #>> '{}' ILIKE '%'||$2||'%' ORDER BY length(a.value #>> '{}') ASC LIMIT 1",
        type_, ref, text_field,
    )


async def _find_decision(
    pool: asyncpg.Pool, ref: str, *, require_identifier: bool = False,
) -> uuid.UUID | None:
    """A Decision by UUID, by canonical, by short-id prefix, then by summary substring
    (shortest summary wins), see `_resolve_ref` for the full ladder and why it refuses
    rather than guesses on identifier-shaped input. `require_identifier=True` drops the
    summary-substring leg, the same opt-in `_find_thread` already exposes, for a call path
    that closes the record it names rather than merely reading it (ack_handoff)."""
    return await _resolve_ref(pool, "Decision", ref, text_field="summary",
                              require_identifier=require_identifier)


async def _decision_snapshot(pool: asyncpg.Pool, decision_id: uuid.UUID) -> dict[str, str | None]:
    """The current summary/rationale for a Decision, read before a near-duplicate reuse
    overwrites them, so the MCP wrapper's receipt can show a caller what a false-positive
    dedup hit is about to erase (fixing a case where record_decision's near-dup
    guard silently merged two distinct rulings that shared a boilerplate summary template,
    with no signal in either receipt)."""
    row = await pool.fetchrow(
        "SELECT "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "   AND a.name='summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS summary, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "   AND a.name='rationale' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS rationale",
        decision_id)
    return {"summary": row["summary"], "rationale": row["rationale"]}


async def verify_ruling(
    pool: asyncpg.Pool, ruling_ref: str, *, write_name: str,
) -> dict[str, Any]:
    """THE RULING-CITATION DOOR: a worker write
    normally gated to the operator (or a seat's own manager, where that escape exists,
    `charter_for`'s own shape) may instead cite an operator's own standing ruling and
    act under that ruling's authority, provided the ruling actually says so. Three
    checks, in order, each naming exactly what failed: (1) `ruling_ref` resolves to a
    real Decision (`_find_decision`'s own by-uuid/canonical/short-id/summary-substring
    ladder, never a guess); (2) that decision's own `kind` property reads 'ruling', not
    'decision' or anything else, an ordinary decision is not standing authority to act
    on, only a ruling is; (3) the ruling's own summary or rationale text actually names
    `write_name` (a case-insensitive substring match), a ruling about something else
    entirely cannot silently authorize an unrelated write just because a caller cited
    it. Returns `{"ok": True, "ruling_id", "summary"}` on success, `{"ok": False,
    "error"}` naming which of the three failed otherwise, a caller passes the error
    straight through as its own refusal, never re-derives the reason."""
    did = await _find_decision(pool, ruling_ref)
    if did is None:
        return {"ok": False,
                "error": f"no such decision: {ruling_ref!r} — a ruling citation must "
                        "resolve to a real Decision"}
    kind = await pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='kind' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", did)
    if kind != "ruling":
        return {"ok": False,
                "error": f"{ruling_ref!r} resolves to a decision of kind {kind!r}, not "
                        "a ruling — only a ruling is standing authority to act under"}
    snap = await _decision_snapshot(pool, did)
    text = f"{snap.get('summary') or ''} {snap.get('rationale') or ''}".lower()
    if write_name.lower() not in text:
        return {"ok": False,
                "error": f"the ruling at {ruling_ref!r} does not name {write_name!r} in "
                        "its own summary or rationale — citing a ruling to act under "
                        "operator authority requires the ruling's own text to actually "
                        "authorize THIS write, never inferred from context"}
    return {"ok": True, "ruling_id": did, "summary": snap.get("summary")}


async def _thread_summary(pool: asyncpg.Pool, thread_id: uuid.UUID) -> str | None:
    """The winning `summary` for a thread, used to name what a batch resolve closed."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1",
        thread_id,
    )


# PRIOR-ART SURFACING, from a re-derivation post-mortem:
# a ruling contradicting standing law must not mint frictionlessly. Canonical failure this
# prevents: a decision minted in direct contradiction of an earlier naming decision with
# zero friction; a human caught it, the verb didn't. `search`'s own fused engine
# (lexical + semantic doors) is topical, not lexical, the exact property needed here, since
# a contradicting ruling rarely reuses its predecessor's wording. `via` in ('id', 'both')
# means an independent second door corroborated the match (an id-exact hit, or both the
# textual and semantic doors agreeing); that agreement, not a magic rank cutoff, is what
# "strong" means, so the flag doesn't need recalibrating as the corpus grows.
_PRIOR_ART_STRONG_VIA = ("id", "both")


_PRIOR_ART_KINDS = frozenset({"Decision"})
# THE THAW: the unified check widens past Decision-only, a deliberately-left-open plug
# from an earlier decision: every write-path caller that wants the
# fuller corpus passes this instead of the (still-default, backward-compatible) bare set.
# Added because a fresh decision
# that topically matches an open board row should surface the row, unprompted, the same way
# a matching Practice or standing Decision already does, see _open_obligation_thread_ids
# for the extra status/kind filter this Thread inclusion needs (a resolved/retracted or
# non-board Thread showing up as "prior art" would be noise, not the routing nudge this
# exists for).
UNIFIED_PRIOR_ART_KINDS = frozenset({"Decision", "Practice", "Superstition", "Thread"})


async def _open_obligation_thread_ids(
    pool: asyncpg.Pool, thread_ids: list[uuid.UUID], *, repo: str | None = None,
) -> set[uuid.UUID]:
    """Which of these Thread ids are the Thread shape UNIFIED_PRIOR_ART_KINDS' widening
    means to surface. Call before prior_art_from_hits (which truncates `id` to an 8-char
    short id and so can no longer query precisely) on the raw search() hits, not after.

    TWO ADMISSION PATHS, measured not guessed:
    (1) status='open' AND kind='obligation', the modern convention, admitted regardless
    of repo. (2) status='open' AND NO `kind` property at all AND `repo` is given AND the
    Thread shares that same `in_repo` project, an older row predating the convention,
    admitted only within its own project's scope.

    REPO SCOPE, NOT KIND'S BARE ABSENCE, is the discriminator, because a kindless Thread's
    absence-of-kind says nothing about whether it is a board row at all: measured fleet-
    wide, 73% of the 92 open, kind-less Threads live outside this project's own graph,
    real, legitimate work for other projects. Admitting those as "prior
    art" on an unrelated ruling would be noise dressed as coverage. Within this project alone the
    same population reads 24-of-25 clean: the same field, kind's mere absence, means two
    different things depending on which project you ask it in, and repo scope is what
    separates them.

    `repo=None` admits nothing under path (2), never, by design, an implicit fleet-wide
    fallback: an absent repo must never silently become
    fleet-wide, that is the exact hole this check exists to close. A record_decision
    call with no `repo` gets only the modern-convention path, same as before this change.

    One batched query for (1) over the caller's candidate ids (in practice at most ~15,
    search()'s own limit); a second batched query for (2), only when `repo` is given and
    at least one kindless-but-open candidate remains, never a per-row round trip, and a
    no-op (both queries skipped) when no Thread hit is present."""
    if not thread_ids:
        return set()
    rows = await pool.fetch(
        "SELECT DISTINCT ON (object_id, name) object_id, name, value #>> '{}' AS v "
        "FROM current_assertions WHERE object_id = ANY($1::uuid[]) "
        "AND name IN ('status', 'kind') "
        "ORDER BY object_id, name, confidence DESC, observed_at DESC",
        thread_ids)
    by_id: dict[uuid.UUID, dict[str, str]] = {}
    for r in rows:
        by_id.setdefault(r["object_id"], {})[r["name"]] = r["v"]
    kept = {tid for tid, props in by_id.items()
            if props.get("status", "open") == "open" and props.get("kind") == "obligation"}
    if repo:
        kindless_open = [tid for tid, props in by_id.items()
                         if tid not in kept and props.get("status", "open") == "open"
                         and "kind" not in props]
        # _resolve_repo, not a raw canonical string match: the same resolver link_repo
        # uses to attach a Thread to its project in the first place, so this admits by
        # the identical identity link_repo would have written, not a re-derived guess.
        proj = await _resolve_repo(pool, repo) if kindless_open else None
        if proj is not None:
            same_repo = await pool.fetch(
                "SELECT from_id FROM links WHERE from_id = ANY($1::uuid[]) "
                "AND to_id=$2 AND type='in_repo' "
                "AND (valid_until IS NULL OR valid_until > now())",
                kindless_open, proj)
            kept |= {r["from_id"] for r in same_repo}
    return kept


def prior_art_from_hits(
    hits: list[dict[str, Any]], *, exclude: set[uuid.UUID] | None = None, limit: int = 5,
    kinds: frozenset[str] = _PRIOR_ART_KINDS,
) -> list[dict[str, Any]]:
    """Shape a `search()` result into a record_decision/record_practice receipt's
    `prior_art`, standing, non-buried hits of the given `kinds` only (default Decision-
    only, unchanged for existing callers; pass UNIFIED_PRIOR_ART_KINDS for the unified
    check over {Decisions, Practices, Superstitions}). Excludes the item just
    recorded and any explicit `supersedes`/`refutes` target, those are already handled by
    that verb, naming them again as "prior art" would just be noise. A `superseded`
    Decision or a `refuted` Practice is dead testimony for this purpose (it no longer
    stands for anything a new record could be redundant with), so both are excluded here
    even though search() itself still surfaces them, flagged, for direct lookup. LOUD,
    NEVER A REFUSAL (the SPOF principle): this only shapes data for the receipt to
    display; the caller decides whether a hit is strong enough to flag.

    TYPE-PARTITIONED, ONE RESERVED SLOT: 87 Practices vs 6,353
    Decisions means a Practice essentially never survives a plain rank-order truncation
    to `limit`; realistic-language queries surfaced zero Practices in the population
    at one measurement. A raw top-K over `hits` fills every slot from the 73:1-larger
    Decision population before a Practice's own rank is ever reached. Fix is ranking,
    never authorship: when 'Practice' is in `kinds`, the last slot is reserved for the
    single best-ranked qualifying Practice (in `hits`' own order, wherever it actually
    sits) if one exists and isn't already among the first `limit - 1` picks; everything
    else keeps plain rank order untouched, so a caller with Practice excluded from
    `kinds` (record_decision's default) sees no behavior change at all."""
    exclude_s = {str(e) for e in (exclude or set())}

    def _qualifies(h: dict[str, Any]) -> bool:
        return (h.get("type") in kinds and h.get("id") not in exclude_s
                and not h.get("superseded") and not h.get("refuted"))

    def _shape(h: dict[str, Any]) -> dict[str, Any]:
        return {"id": str(h["id"])[:8], "type": h.get("type"),
                "summary": h.get("snippet") or "", "grade": h.get("grade"),
                "via": h.get("via")}

    reserve_practice = "Practice" in kinds and limit > 0
    fill_limit = (limit - 1) if reserve_practice else limit

    out: list[dict[str, Any]] = []
    best_practice: dict[str, Any] | None = None
    for h in hits:
        if not _qualifies(h):
            continue
        if reserve_practice and h.get("type") == "Practice" and best_practice is None:
            best_practice = h
        if len(out) < fill_limit:
            out.append(_shape(h))
    if best_practice is not None and not any(o["type"] == "Practice" for o in out):
        if len(out) >= limit:
            out.pop()
        out.append(_shape(best_practice))
    return out


def prior_art_is_strong(prior_art: list[dict[str, Any]]) -> bool:
    """Does the top prior-art hit warrant the loud flag ('a standing ruling covers this
    ground, supersede it explicitly or cite it')? See `prior_art_from_hits` for why
    cross-door agreement, not a rank number, is the bar."""
    return bool(prior_art) and prior_art[0].get("via") in _PRIOR_ART_STRONG_VIA


async def property_prior_art(
    pool: asyncpg.Pool, *, subject_canonical: str, field: str, new_value: str,
    because: str = "", actor: str,
) -> dict[str, Any]:
    """Generalizes record_decision's own prior-art guard (search()-based, LOUD, NEVER
    REFUSES, the SPOF principle) from a decision write to a property write (a sibling
    investigation into "two
    records of one truth with no reconciler", one instance being a standing Decision
    versus a later property write that silently contradicts it).

    THIS DOES NOT SOLVE THE HARD PROBLEM (proven live: an operator-authorized write and an
    ordinary agent's own judgment call are bit-for-bit identical at the assertion layer,
    same evidence_class, same confidence, source_id is just the calling agent, no
    structural edge to any authorizing Decision). It cannot know which property writes are
    operator-set, so it does not try to refuse a contradiction; it only ensures the
    writer sees whatever standing Decision already discusses this exact ground before the
    write lands. That is the whole fix: earlier writers acted in good faith on missing
    information, not bad faith on visible information.

    Decision-kind hits only (the default `prior_art_from_hits` kind set): a standing
    ruling is the thing worth surfacing here, not a Practice or Superstition. Fail-open:
    a search hiccup must never block the write it is advising on, same discipline
    record_decision's own guard already holds. Returns `{}` when nothing rises to a
    strong hit (weak/coincidental matches are not worth the noise on every property
    write); merge the result into the caller's own receipt dict; a real hit adds
    `prior_art` + `prior_art_flag`, the same keys record_decision's own receipt uses, so
    a caller already familiar with that shape reads this one identically."""
    from src.orchestrator import compositions as comp

    query = f"{subject_canonical} {field} {new_value} {because}".strip()[:300]
    try:
        search_out = await comp.run_spec(
            pool, {"op": "function", "name": "search",
                   "args": {"q": query, "limit": 15, "caller": actor}},
            None, name="search", caller=actor)
        prior = prior_art_from_hits(search_out["items"]["hits"])
    except Exception:  # noqa: BLE001 - never block the write on a search-side failure
        prior = []
    if not prior or not prior_art_is_strong(prior):
        return {}
    top = prior[0]
    return {
        "prior_art": prior,
        "prior_art_flag": (
            f"a standing ruling ({top['id']}) may already cover {subject_canonical}'s "
            f"{field!r} — read it before this value stands as the final word"),
    }


# CONTRADICT vs RE-DERIVE: a strong hit against a standing Practice gets the same "looks
# like a re-derivation" nudge today whether the new decision merely restates the Practice
# or silently reverses it. That is the exact failure this traces: a fixed mistake
# recurring, caught only because a write happened to fire the check at all, when the
# missing property is "asserted at the point of application." Telling the two apart
# needs no semantic classifier: a reversal leaves a lexical fingerprint (negation/override
# language) a plain restatement does not. This is a heuristic, not a verdict; an empty cue
# list is not proof of agreement, only that this fingerprint is absent. The flag stays a
# nudge for a human to review, never a block (fail open rather than gate on a single check).
_CONTRADICTION_CUES = (
    "never", "don't", "do not", "doesn't", "does not", "stop", "instead of",
    "rather than", "no longer", "avoid", "skip", "reverse", "abandon", "override",
    "opposite", "wrong to", "should not", "shouldn't", "must not", "mustn't", "not to",
)

# WORD-BOUNDARY MATCHING: a gap that was previously identified but never fixed, and was
# confirmed live when "stop" matched as a raw substring inside the unrelated tool name
# "stopslop", a measurement across real data that found it firing for real, not just
# latent. A plain `cue in text` check is blind to word boundaries; `\b...\b` requires a
# non-word character (or string edge) on both sides, so "stop" no longer matches inside
# "stopslop"/"backstop" while still matching a genuine standalone occurrence. This is a
# strict narrowing: every text this used to flag via a genuine standalone word/phrase
# still flags; only substring-inside-a-longer-word collisions stop matching. Precompiled
# once, not per call, since this runs once per sentence of every turn's tail text as well
# as at every record_decision, and multi-word cues ("rather than") need no special
# casing since \b anchors the whole phrase's own edges.
_CUE_PATTERNS = tuple(
    (cue, re.compile(r"\b" + re.escape(cue) + r"\b")) for cue in _CONTRADICTION_CUES
)


def practice_contradiction_cues(text: str) -> list[str]:
    """Which contradiction-flavored cue phrases appear in `text` as a genuine standalone
    word/phrase (case-insensitive, word-boundary-anchored, deterministic, no NLP). See
    `_CONTRADICTION_CUES`/`_CUE_PATTERNS` for why this is a lexical fingerprint check, not
    an entailment classifier, and why it requires word boundaries."""
    low = text.lower()
    return [cue for cue, pattern in _CUE_PATTERNS if pattern.search(low)]


def _ref_slug(title: str) -> str:
    """ref:<slug>: the same canonical scheme as the doc ingester (src/ingest/reference.py),
    so an agent citing "Attention Is All You Need" and a later doc-ingest of the same title
    find-or-create one node instead of duplicates."""
    return "ref:" + re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")


async def ingest_reference(
    actions: Actions, title: str, *, source_url: str | None = None,
    vendor: str | None = None, body: str | None = None, caveats: str | None = None,
    repo: str | None = None, source: str = _SOURCE,
    cites: list[uuid.UUID] | None = None,
    repo_evidence_class: str | None = None,
    unlinked_because: str | None = None, unlinked_because_kind: str | None = None,
) -> tuple[uuid.UUID, str]:
    """An agent turns something it read into a first-class Reference node: a paper, a
    vendor doc, a spec, findable by search, linkable
    by `grounded_by`, instead of narrated into free text and lost.

    `caveats` is deliberately its own property, never folded into `body`: "but only under
    X" buried in prose is a caveat lost, a theorem that tightens rather than confirms must
    survive as exactly that. `cites` wires paper->paper lineage (`cites` edges to other
    Reference ids) so a literature tree is walkable, not re-derived. Graded SELF_DECLARED:
    the agent testifying to what it read (the read is first-hand; the paper's claims keep
    their own grade in `body`/`caveats` prose). Idempotent on the title slug.

    `repo_evidence_class` grades the `in_repo` link only, same rule as record_decision's
    own parameter of the same name: SELF_DECLARED (default) when the caller typed `repo=`,
    DIRECT_OBSERVATION when the MCP wrapper defaulted it from the caller's own mount state
    rather than the caller asserting it about this specific Reference.

    `unlinked_because`/`unlinked_because_kind` (widening the declare-or-refuse gate to
    this door): same shape record_decision/
    open_thread already use, a real `in_repo` link satisfies the gate outright,
    `unlinked_because` is the mandatory countable hatch otherwise, and refusing without
    either raises before this Reference ever lands (`_enforce_required_links`, in scope
    for `("repo",)` only, the sole link kind this door mints inside its own atomic
    block; `cites` is deliberately not in scope here, a root Reference legitimately
    cites nothing).

    Returns (id, canonical)."""
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
            rec = repo_evidence_class or _EC
            mint_confession = await link_repo(a, ref, repo, observed, source=source,
                                              evidence_class=rec,
                                              confidence=confidence_for(EvidenceClass(rec)))
            if mint_confession:
                logger.warning("ingest_reference(repo=%r): %s", repo,
                               mint_confession["confession"])
        for cited in cites or []:
            exists = await a.pool.fetchval(
                "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='cites'",
                ref, cited)
            if not exists:
                # `origin: "declared"`: the caller named this exact target on purpose, same
                # confidence shape as mint_cites' own "declared" origin, kept
                # explicitly queryable apart from a prose-derived cite, never merely
                # inferable from the absence of a marker on this codebase's own
                # pre-existing (Reference->Reference, unmarked) cites edges.
                await a.create_link(ref, cited, "cites", source, observed, _CONF,
                                    evidence_class=_EC,
                                    properties={"origin": "declared"})
        await _enforce_required_links(
            a, ref, "Reference", kinds_in_scope=("repo",),
            unlinked_because=unlinked_because, source=source, observed=observed,
            unlinked_because_kind=unlinked_because_kind)
    return ref, canon


# THE NEAR-DUPLICATE DEDUP: the same fact gets minted twice across a retry or a lineage
# restart because the summary differed slightly the second telling; `_canon`'s exact-hash
# idempotency only catches a byte-identical repeat. One measured case: a record_decision
# call came back rejected after it had already committed server-side, and the natural
# retry minted a duplicate because the two summaries differed by one word, requiring a
# manual merge afterward. Shared by `find_near_duplicate_open_thread` (threads) and
# `find_near_duplicate_decision` (decisions, below): one algorithm, one threshold, two mint
# sites. Conservative on purpose: a false merge silently drops testimony, which is worse
# than a duplicate a human can fold.
_DEDUP_SIM = 0.60  # first-pass estimate (no live baseline to calibrate against yet, unlike
                    # the similar 0.30 "same story" bar used elsewhere); recalibrate if it
                    # over/under-fires.


def _normalize_for_dedup(text: str) -> str:
    """Case/punctuation/whitespace-flattened form: the miner's own near-dup guard
    (`sessions.py::_normalized`), reused here so a trivial rewording (case, punctuation, a
    trailing clause) is caught without even reaching the similarity check below."""
    return " ".join(re.sub(r"[^a-z0-9]+", " ", text.lower()).split())


# Recurring scaffolding a Decision's own summary opens with: measured live against the
# real corpus, 363 of 7,498 decisions (4.8%) open with "STATE OF THE BOARD", the single
# largest cluster, each one different substance riding an identical fixed header that
# would otherwise dominate find_near_duplicate_decision's own trigram/ratio comparison
# between two decisions that share nothing else. Scoped to decisions only
# (find_near_duplicate_open_thread never carries this convention) and kept short on
# purpose: the measured, unambiguous cases, not a speculative taxonomy of every phrase
# ever typed.
_DEDUP_BOILERPLATE_PREFIXES = (
    "state of the board",
    "operator ruling",
    "correction to my",
    "correction to the",
)


def _strip_dedup_boilerplate(text: str) -> str:
    """Drop one recognized boilerplate opener (case-insensitive, plus any immediately
    following punctuation/dash/colon) from the front of `text`, never mid-summary, where
    the same words are real content, not scaffolding. Used only as the similarity
    comparison's own working copy; the stored summary is never touched."""
    stripped = text.lstrip()
    lowered = stripped.lower()
    for prefix in _DEDUP_BOILERPLATE_PREFIXES:
        if lowered.startswith(prefix):
            return stripped[len(prefix):].lstrip(" \t—-:,.")
    return text


async def _pg_trgm_enabled(pool: asyncpg.Pool) -> bool:
    """Is pg_trgm actually installed on this database? Check, don't assume: an earlier
    comment ('pg_trgm is not installed, so there is no trigram similarity to lean on') went
    stale once a migration landed the extension; mailbox.py has used similarity() ever
    since. A local catalog lookup, not a network round trip."""
    return bool(await pool.fetchval(
        "SELECT 1 FROM pg_extension WHERE extname='pg_trgm'"))


async def find_near_duplicate_open_thread(
    pool: asyncpg.Pool, summary: str, *, repo: str | None,
) -> uuid.UUID | None:
    """An existing open thread on this project that is the same fact as `summary`, reworded,
    or None. Checked before minting (open_thread's caller): a normalized exact match first
    (case/punctuation/whitespace), then a conservative similarity check over that project's
    open threads, pg_trgm's `similarity()` when the database has the extension, else a
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
        "AND (l.valid_until IS NULL OR l.valid_until > now()) "
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


async def find_near_duplicate_decision(
    pool: asyncpg.Pool, summary: str, *, repo: str | None, exclude: uuid.UUID | None = None,
) -> uuid.UUID | None:
    """An existing live Decision on this project that is the same ruling as `summary`,
    reworded, or None. `record_decision` checks this before minting (mirrors
    `find_near_duplicate_open_thread`'s shape exactly, same normalized-exact-then-similarity
    cascade, same `_DEDUP_SIM` bar): a normalized exact match first, then a conservative
    similarity check over that project's live decisions, pg_trgm when available, else the
    Python-side ratio fallback. `repo`-scoped only, like the thread guard, no safe scope to
    dedup against fleet-wide, so it stands down when `repo` is absent. Live excludes a
    decision that has since been superseded (a buried ruling is a different fact now, the
    correction, same exclusion shape as the thread guard's 'resolved is never a target').
    Unlike the thread guard, this is not the whole defense: `record_decision` still runs
    `supersedes`/`resolves` in full regardless of a hit, so a structural side effect a
    retry depends on is never swallowed by the dedup, only the object itself is reused.

    BOILERPLATE STRIPPED FIRST: `_strip_dedup_boilerplate` drops a recognized scaffolding
    opener (measured live: "STATE OF THE BOARD" alone opens 4.8% of the whole corpus) from
    both `summary` and each candidate's own text before either comparison runs, so two
    decisions sharing only a convention's own fixed header, never their actual substance,
    no longer merge. The disclosed-merge receipt this function's own hit still drives
    (`record_decision`'s own "reused an existing decision" wording) is untouched; this
    only changes what counts as similar enough to reach it.

    `exclude`: the decision named by this call's own `supersedes` must never itself be a
    dedup candidate. A correction restates its subject by nature, that is what makes it a
    correction, so it is systematically more similar to the ruling it corrects than an
    average pair of decisions, not less. The highest-stakes write this guard touches is
    exactly the one most likely to misfire. Without this, `dup` could resolve to the very
    decision `supersedes` names, `record_decision` would set `d = dup = old`, and its
    existing "never buries itself" guard (`old != d`) would then silently skip the burial:
    the correction's words land on the old object, superseded_by never gets asserted, and
    the wrong ruling ends up wearing the right one's words. A stated intent ("supersede
    this one") outranks any similarity score, so the caller resolves `old` first and
    passes it here: an exclusion, not a heuristic."""
    if not repo:
        return None
    proj = await _resolve_repo(pool, repo.removeprefix("repo:").strip())
    if proj is None:
        return None
    exclude_clause = " AND o.id <> $2" if exclude is not None else ""
    params = (proj, exclude) if exclude is not None else (proj,)
    rows = await pool.fetch(
        "SELECT o.id, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS summary "
        "FROM objects o JOIN links l ON l.from_id=o.id AND l.type='in_repo' AND l.to_id=$1 "
        "AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "WHERE o.type='Decision' AND o.merged_into IS NULL AND o.status='active' "
        "  AND COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
        "   WHERE a.object_id=o.id AND a.name='superseded_by' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1),'')=''" + exclude_clause,
        *params)
    candidates = [(r["id"], r["summary"]) for r in rows if r["summary"]]
    if not candidates:
        return None
    # boilerplate stripped from the comparison's own working copy only, the stored
    # `summary`/`cand` strings are never touched, only what
    # normalize/similarity below actually compares.
    stripped_new = _strip_dedup_boilerplate(summary)
    norm_new = _normalize_for_dedup(stripped_new)
    for did, cand in candidates:
        if _normalize_for_dedup(_strip_dedup_boilerplate(cand)) == norm_new:
            return uuid.UUID(str(did))
    if await _pg_trgm_enabled(pool):
        ids = [did for did, _ in candidates]
        bodies = [_strip_dedup_boilerplate(cand) for _, cand in candidates]
        hit = await pool.fetchval(
            "WITH b AS (SELECT unnest($1::uuid[]) AS id, unnest($2::text[]) AS body) "
            "SELECT id FROM b WHERE similarity(body, $3) > $4 "
            "ORDER BY similarity(body, $3) DESC LIMIT 1",
            ids, bodies, stripped_new, _DEDUP_SIM)
        return uuid.UUID(str(hit)) if hit is not None else None
    best_id, best_ratio = None, 0.0
    for did, cand in candidates:
        ratio = SequenceMatcher(
            None, norm_new, _normalize_for_dedup(_strip_dedup_boilerplate(cand))).ratio()
        if ratio > best_ratio:
            best_id, best_ratio = did, ratio
    return uuid.UUID(str(best_id)) if best_id is not None and best_ratio > _DEDUP_SIM else None


# THE ROADMAP ARC TAXONOMY, a locked list, closed on
# purpose: a free-text `arc` would fragment silently (a typo is a new, permanently-empty
# arc no thread ever finds again), so `open_thread` refuses anything outside this set
# rather than accepting a drifting label. roadmap.py imports this same constant for its
# section order, one taxonomy, never two copies that quietly disagree.
ARCS = ("Identity-Succession", "Compaction-Resilience", "Model-Identity", "Token-Cost",
        "Surfaces-Roadmap-Docs", "Fleet-Hygiene", "Security", "Graph-Engineering")

# ONE LINE PER ARC: several independent "the taxonomy is ambiguous" findings turned out to
# trace to the same absence, that NONE of the seven ever carried a definition anywhere, so
# an undocumented boundary read as ambiguous even where the taxonomy itself was fine.
# Written by re-reading the actual specimens that forced each boundary, not guessed: the
# two threads that dual-fit Identity-Succession/Compaction-Resilience (both a specific
# succession's own board-state/handoff note) resolve cleanly once Identity-Succession is
# drawn as that event's own record and Compaction-Resilience as the general mechanism any
# session leans on, not one lineage's instance of it. See ARC_DEFINITIONS's own two entries
# below for the drawn line. Never validated or persisted (ARCS itself stays the only closed
# set); this is reference text for a human choosing an arc, not a second schema.
ARC_DEFINITIONS: dict[str, str] = {
    "Identity-Succession": (
        "An AGENT or SEAT's own identity crossing a generation — minting, lineage, "
        "charter, handles, and the board-state/handoff note a SPECIFIC succession event "
        "produces. NOT a SoftwareProject's identity (dedup, case-collision, fork "
        "detection) — that has no arc yet, a named gap, not this one's job to cover."
    ),
    "Compaction-Resilience": (
        "The GENERAL mechanism that lets ANY session survive losing its context window — "
        "the offload ritual, resumability, transcript/session persistence infrastructure. "
        "NOT one particular lineage's own handoff note (that's Identity-Succession); this "
        "is the machinery, not an instance of using it."
    ),
    "Model-Identity": (
        "Which MODEL an agent is actually running as, and the harness silently swapping "
        "or degrading it. NOT the rest of a seat's pin file — house/seat/project belong "
        "to Identity-Succession; this is the model field and its precedence alone."
    ),
    "Token-Cost": (
        "Spend and budget — what a session or the fleet actually burns, including the "
        "unpriced-subscription-lane gap where no local meter can see the true number."
    ),
    "Surfaces-Roadmap-Docs": (
        "The fleet's own outward-facing text — CLI/MCP vocabulary alignment, docs, and "
        "the roadmap/board rendering itself, not the underlying work those surfaces show."
    ),
    "Fleet-Hygiene": (
        "Tool/ledger/graph reliability bugs — a verb that silently drops data, a lint "
        "check, a stale-obligation sweep. The machinery's own correctness, not what it "
        "was used to build."
    ),
    "Security": (
        "Vulnerabilities, credential handling, and PII/secret exposure. Rare by design "
        "in an internal coordination tool, not proven dead weight — no evidence either "
        "way yet (decision 42433f6e/608b0e14)."
    ),
    "Graph-Engineering": (
        "First-class work-lineage node/edge types (AgentRun, Artifact, Evaluation, "
        "Metric; produced/derived_from/evaluated_by/revises) and the write- and "
        "read-invariants that keep every output traceable to its run, plan, source and "
        "evaluator. NOT the knowledge-lineage machinery itself (Decision/Thread/"
        "supersedes — ordinary graph work) and NOT a lint check's own correctness bug "
        "once these types exist (that's Fleet-Hygiene)."
    ),
}
for _arc_name in ARCS:
    assert _arc_name in ARC_DEFINITIONS, f"{_arc_name!r} has no ARC_DEFINITIONS entry"


def arc_definition(arc: str) -> str | None:
    """The one-line boundary for `arc`, or None for anything outside ARCS (never guessed)."""
    return ARC_DEFINITIONS.get(arc)


# THE ONE UNSORTED SENTINEL (measured at 7.2% fleet-wide): the same shape as
# offices._CHARTER_UNDECLARED, never persisted (ARCS stays a closed taxonomy; this is not
# a legal `arc` value and never becomes one), a receipt-only honest echo so a caller who
# left `arc` unset sees that choice on every mint rather than it silently vanishing, the
# same way a fresh seat's missing charter now reads UNDECLARED instead of nothing at all.
# Leads with "unsorted" deliberately: `_fn_roadmap_open` (compositions.py) already buckets
# an arc-less thread under that exact word on the read side, so a caller who sees this in
# their own receipt and later finds it grouped "unsorted" on the roadmap recognizes the
# same fact stated twice, not two different ones.
_ARC_UNSORTED = "unsorted — arc was left unset (capture.ARCS names the taxonomy)"

# THE REPO GATE: ARCS is an osiris-coordination taxonomy, not a general one. 506 of 661
# fleet-wide arc-null threads trace to 37 distinct non-osiris projects whose work
# genuinely has no home in these seven names, and the only reader of `arc` anywhere in
# this codebase is osiris's own roadmap composition. Rather than build a
# taxonomy-per-project mechanism nobody has asked for, `arc` becomes legal to set only
# when the thread's own project resolves to osiris itself, making the code say what the
# world already does, additively: never a refusal, never a strip of the 219 threads that
# already carry an arc off-scope; a caller who passes one anyway is told why, not turned
# away.
#
# UNSPECIFIED REPO READS AS IN-SCOPE, NOT OUT (found live before shipping, not guessed):
# deploy_guard.py's two boot alarms and task_sync.py's tier2 mints call capture.open_thread
# directly with arc="Fleet-Hygiene" and no `repo` at all. These are exactly the "two
# hardcoded automated callers" that an earlier arc-adoption decision named as working at
# 100%. They are osiris's own machinery, never called by any other project's code, and
# never threaded a repo string before this gate existed either. Treating an absent repo as
# out of scope would have silently zeroed their arc, a real regression a naive read of
# "legal only when repo=osiris" would have shipped. The actual case this gate exists for
# (a live agent explicitly filing to their own non-osiris project) always arrives with a
# resolved repo, either explicit or defaulted from the caller's own mount identity
# (mcp_server.open_thread's `ident.project` fill), so gating only on a repo that resolves
# to something other than osiris catches the real case without breaking the
# unnamed-caller one.
async def arc_in_scope(pool: asyncpg.Pool, repo: str | None) -> bool:
    """True when `repo` is unspecified (an internal osiris caller that never threads a
    project string) or names osiris itself. False only when `repo` resolves to a
    different, real project, or to nothing at all under a named string, the actual case
    this gate exists for. The literal string "osiris" short-circuits before any DB
    resolution: `open_thread(repo="osiris", ...)` mints the osiris SoftwareProject itself
    via `link_repo` only after this gate runs, so resolving-first would find no such
    project yet on the very first call, a chicken-and-egg false negative a name check
    sidesteps entirely."""
    if not repo:
        return True
    name = repo.removeprefix("repo:").strip()
    if name == "osiris":
        return True
    proj = await _resolve_repo(pool, name)
    if proj is None:
        return False
    osiris_id = await _resolve_repo(pool, "osiris")
    return osiris_id is not None and proj == osiris_id


async def arc_in_scope_for_thread(pool: asyncpg.Pool, thread_id: uuid.UUID) -> bool:
    """The same gate as `arc_in_scope`, for an already-existing thread (reclassify_thread's
    own door): true when the thread carries no in_repo edge at all (same permissive default
    as an unspecified `repo`, since a thread deploy_guard/task_sync minted never got one
    either) or carries one to osiris itself. False when it has an in_repo edge to a real,
    different project, including the case where osiris itself was never minted, since a
    thread that is filed somewhere cannot then match a project that doesn't exist."""
    repo_ids = await pool.fetch(
        "SELECT DISTINCT l.to_id FROM links l WHERE l.from_id=$1 AND l.type='in_repo' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())",
        thread_id)
    if not repo_ids:
        return True
    osiris_id = await _resolve_repo(pool, "osiris")
    if osiris_id is None:
        return False
    return any(r["to_id"] == osiris_id for r in repo_ids)


def _arc_out_of_scope_note(label: str) -> str:
    """The receipt-only sentinel for an out-of-scope `arc`, same shape as _ARC_UNSORTED
    and offices._CHARTER_UNDECLARED: never persisted, never a refusal, always visible."""
    return (f"osiris-scoped — {label} is not the osiris project, so this thread will not "
            "carry an arc (capture.ARCS names osiris's own roadmap taxonomy only)")


DEFAULT_STALE_AFTER_DAYS = 14


async def open_thread(
    actions: Actions, summary: str, *, repo: str | None = None, kind: str | None = None,
    owner: str | None = None, assignee: str | None = None, arc: str | None = None,
    severity: str | None = None, resolves: str | list[str] | None = None,
    branch: str | None = None, files_touched: list[str] | None = None,
    source: str = _SOURCE, repo_evidence_class: str | None = None,
    unlinked_because: str | None = None, stale_after_days: int | None = None,
) -> uuid.UUID:
    """Open a thread at source: an unresolved question or next step for the next session
    to inherit. Same shape as a mined Thread (props summary + status=open) so it appears in
    `briefing`'s open-threads section beside mined ones. Idempotent on the summary hash.

    `unlinked_because` is the declare-or-refuse gate's countable hatch, same contract as
    record_decision's own parameter: if Thread declares required link kinds and this
    call's own `repo=` (the only kind this door can know about at its own atomic commit)
    doesn't satisfy it at SELF_DECLARED grade, the write refuses unless this is given; when
    given, it's recorded as a fact in the same transaction and the write proceeds.

    `repo_evidence_class` grades the `in_repo` link only, same rule as record_decision's
    own parameter of the same name: SELF_DECLARED (default) when the caller typed `repo=`,
    DIRECT_OBSERVATION when the MCP wrapper defaulted it from the caller's own mount state.

    `kind='obligation'` marks the obligations class: a duty minted by an action ("kernel
    changed, daemons need restart"), neither a ruling nor ordinary work, exactly the thing
    that used to die with the context window. Same Thread shape, so it surfaces in
    briefing beside the rest; the kind stays as data for filtering. `source` attributes
    the opening actor (a fleet agent vs. the lone `session`).

    `owner` says whose move it is (two distinct needs, "mine to act" vs. "waiting on the
    human", were previously illegible on the board): 'operator' = blocked on the human's
    word or hands; 'agent:<id>' = a specific agent; a bare project name = any hand on that
    project. Unowned = anyone who reads it may act. The lens sorts by it; the record just
    keeps it. Except for `kind='obligation'`: an unowned duty (not a general thread, see
    below) defaults to the caller's own seat, never refuses, never picks silently. This
    follows from a population read that found over a thousand open obligations fleet-wide
    carrying no owner at all.

    `assignee` ("single-assignee leased obligations") is the seat/agent this build belongs
    to, one build one assignee. It is not a second field: `owner` already is "whose move
    it is", and two properties naming the same fact is the bug this avoids, so `assignee`
    stamps the same `owner` property (assignee wins if both are given); orient's
    sort-by-owner needs no change. What's new is enforcement, not storage: the caller
    (mcp_server.open_thread) checks find_near_duplicate_open_thread before minting and, on
    a hit, surfaces the existing lease and its holder instead of minting a parallel build,
    see that tool's docstring.

    `arc` names which of the closed taxonomy (`ARCS`, above) this thread belongs to, the
    roadmap screen's top-level grouping, one level above `status`. Osiris-scoped: legal to
    set only when `repo` resolves to the osiris project itself, raises ValueError on
    anything outside `ARCS` for an osiris thread (a locked taxonomy that silently accepted
    typos would fragment into permanently-empty arcs nobody finds again), but for any
    other project a supplied `arc` is silently dropped rather than validated or refused
    (arc has no legal home outside osiris, so a caller is told why, never turned away).
    Omitted (the common case) leaves the thread arc-less; the roadmap composition's own
    open half (`compositions._fn_roadmap_open`) buckets those as "unsorted" rather than
    guessing.

    `severity` names an alarm-shaped open in a real, filterable property instead of
    text-matching a summary for "DRIFT"/"CRITICAL". Deliberately unlocked (unlike `arc`):
    the ask was for the property, not a whole new taxonomy; the first (and so far only)
    real caller is `deploy_guard.alarm_schema_drift`, stamping `"alarm"`.

    `resolves` closes a predecessor thread this new one supersedes, in the same call. This
    fixes a diagnosed gap: record_decision's `supersedes` gets exercised every reign on the
    Decision side, but nothing analogous ever ran on the Thread side, so a lineage's own
    board-state/handoff threads accumulate forever, each superseded in practice (a
    successor opened their own) but none ever marked so in the graph. A successor opening
    their own board-state note passes their ancestor's own board-state thread here, same
    shape record_decision's `resolves` already uses (UUID, canonical, or 8-char short id
    only, an addressing act that refuses rather than guesses; the list form resolves each
    entry independently, a miss is skipped not fatal, matching record_decision's own list
    behavior). Reuses resolve_thread's own existing artifact-resolution path rather than
    re-implementing it (Thread is already a valid artifact target, covering a
    sibling-thread-closure shape), no new edge type, no new machinery: the new thread's
    own id becomes the resolved_by witness on whatever it supersedes. Runs after the new
    thread's own creation transaction commits (resolving a different, already-existing
    object is not part of this thread's own atomic write).

    `branch`/`files_touched` mark held work, the one real gap identified among the several
    a review named; the others turned out already-solved or never schema-shaped. No new
    type: the same generic obligation Thread that a conditional-acceptance review already
    proved sufficient (content-capacity was never the problem), carrying the git branch
    and the repo-relative files this build touches so a later reader, or `open_thread`'s
    own collision check below, can find it by file overlap instead of only by already
    suspecting it exists.

    `stale_after_days` applies to `kind='obligation'` only, same scoping as the
    owner-default above: a general thread has no aging law (this house's own standing
    choice, unowned/ageless both being legitimate for a plain question), but a duty
    carries a window past which it surfaces on its owner's own next Stop as a named ask,
    never a DM, never a cron; that is `obligation_hygiene.py`'s separate
    idle-since-touched axis, unrelated to this age-since-opened one. Defaults to
    `DEFAULT_STALE_AFTER_DAYS` (14), overridable per call for a duty that is known to run
    longer or shorter; stamped once, at birth, as an absolute `stale_after` timestamp
    (`observed + the window`) rather than a duration, so every later reader compares
    against `now()` with no re-derivation."""
    if arc is not None:
        if await arc_in_scope(actions.pool, repo):
            if arc not in ARCS:
                raise ValueError(f"arc must be one of {ARCS}, got {arc!r}")
        else:
            arc = None  # out-of-scope: dropped, never refused; see mcp_server's receipt
    observed = datetime.now(UTC)
    effective_owner = assignee if assignee is not None else owner
    if kind == "obligation" and not effective_owner and source != "session":
        # DEFAULT, NEVER REFUSE: a population read found 1,057 open obligations
        # fleet-wide carrying no owner at all. An obligation is a minted duty, not a
        # general thread; a general thread's own null owner is a valid, intentional state
        # (this function's own docstring above: "unowned = anyone who reads it may act"),
        # but a duty that drifts unowned is invisible to the operator queue. A refusal
        # here would block real work at the exact moment someone is trying to record a
        # duty; a silent pick would hide a wrong guess for a week. This house's standing
        # shape applies: the write always proceeds, nothing is refused, nothing is
        # silently chosen; the caller (mcp_server.open_thread) reads back whether a
        # default landed via `_current_owner` and names it in the receipt. Stamped as the
        # caller's own seat id now, not its bare handle; the stored value must itself
        # already satisfy the ownership law, not just look plausible to a human reader.
        from src.orchestrator.seats import held_seat

        seat = await held_seat(actions.pool, source)
        if seat and seat.get("seat_id"):
            effective_owner = seat["seat_id"]
    to_resolve: list[uuid.UUID] = []
    if isinstance(resolves, list):
        for ref in resolves:
            tid = await _find_thread(actions.pool, ref, require_identifier=True)
            if tid is not None:
                to_resolve.append(tid)
    elif resolves:
        single = await _find_thread(actions.pool, resolves, require_identifier=True)
        if single is None:
            raise ValueError(f"resolves matched no thread: {resolves!r} — quote its UUID, "
                             "canonical, or 8-char short id (no prose match — an "
                             "addressing act refuses rather than guesses)")
        to_resolve.append(single)
    # ONE transaction (see record_decision): Thread + summary + status(+kind)(+repo) atomic,
    # never a status-less or summary-less thread husk from a mid-sequence death.
    async with actions.atomic() as a:
        t = await a.create_or_find_object("Thread", _thread_canon(summary, repo), source)
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
        if kind == "obligation":
            window_days = (stale_after_days if stale_after_days is not None
                           else DEFAULT_STALE_AFTER_DAYS)
            stale_after = observed + timedelta(days=window_days)
            await a.assert_property(t, "stale_after", stale_after.isoformat(), source,
                                    observed, _CONF, evidence_class=_EC)
        if branch:
            await a.assert_property(t, "branch", branch, source, observed, _CONF,
                                    evidence_class=_EC)
        if files_touched:
            await a.assert_property(t, "files_touched", files_touched, source, observed,
                                    _CONF, evidence_class=_EC)
        # noted_in FROM THE OPENER'S OWN PROSE (the same mechanism record_decision uses for
        # decided_in, ported here since this door never had it): a thread whose summary
        # already names a commit ("commit 238b48f broke the gate") is the same self-declared
        # shape as a decision naming one, but `decided_in`'s schema domain is Decision-only
        # (schema.py:399); `noted_in` (Thread -> Commit, schema.py:372) is the type the
        # session-miner already uses for this exact edge shape (ingest/threads.py:143),
        # now written at birth too instead of waiting on a mining pass. Only `summary` is
        # scanned; open_thread has no rationale/protocol field to also check.
        for sha in _cited_commit_shas(summary):
            commit_id = await _resolve_commit(a.pool, sha)
            if commit_id is None:  # not (yet) ingested, or a typo: skip, never guess
                continue
            exists = await a.pool.fetchval(
                "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='noted_in'",
                t, commit_id)
            if not exists:
                await a.create_link(t, commit_id, "noted_in", source, observed, _CONF,
                                    evidence_class=_EC)
        # PROSE-ID -> EDGE (same mechanism record_decision uses, above): a Thread's
        # own summary is its only text field to scan.
        prose_skips = await _mint_prose_citations(a, t, source, summary)
        if prose_skips:
            await a.assert_property(t, "prose_citation_skips", prose_skips, source,
                                    observed, _CONF, evidence_class=_EC)
        if repo:
            rec = repo_evidence_class or _EC
            mint_confession = await link_repo(a, t, repo, observed, source=source,
                                              evidence_class=rec,
                                              confidence=confidence_for(EvidenceClass(rec)))
            if mint_confession:
                logger.warning("open_thread(repo=%r): %s", repo,
                               mint_confession["confession"])
        await _enforce_required_links(
            a, t, "Thread", kinds_in_scope=("repo",),
            unlinked_because=unlinked_because, source=source, observed=observed)
    for old_tid in to_resolve:
        if old_tid == t:
            continue  # never resolve yourself (idempotent re-open onto the same summary hash)
        await resolve_thread(
            actions, str(old_tid),
            because=f"superseded by this lineage's own successor note: {summary[:200]}",
            artifact=str(t), source=source)
    return t


async def open_held_work(
    pool: asyncpg.Pool, *, repo: str | None = None,
) -> list[dict[str, Any]]:
    """Every open held-work Thread: `open_thread(..., branch=..., files_touched=...)`'s
    own written shape. `repo` scopes to one project's `in_repo` edge, same discipline as
    `find_near_duplicate_open_thread`; omitted, this is fleet-wide (a branch's files can
    collide across a repo boundary only if the same repo is meant, so the common caller
    passes `repo`). Each row: `id` (short), `summary`, `branch`, `files_touched` (list,
    possibly empty if the thread predates this field or never carried it, never guessed),
    `owner`. Read-only; never gates anything, same posture as `open_held_work`'s own
    callers (a courtesy at mint time, a listing at mount time), never a refusal path."""
    proj = await _resolve_repo(pool, repo.removeprefix("repo:").strip()) if repo else None
    if repo and proj is None:
        return []
    repo_clause = " AND EXISTS (SELECT 1 FROM links l WHERE l.from_id=o.id " \
                 "AND l.type='in_repo' AND l.to_id=$1 " \
                 "AND (l.valid_until IS NULL OR l.valid_until > now()))" if proj is not None else ""
    params = (proj,) if proj is not None else ()
    rows = await pool.fetch(
        "SELECT o.id, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS summary, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='branch' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS branch, "
        " (SELECT a.value FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='files_touched' ORDER BY a.confidence DESC, a.observed_at DESC "
        "   LIMIT 1) AS files_touched, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='owner' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS owner "
        "FROM objects o "
        "WHERE o.type='Thread' AND o.merged_into IS NULL AND o.status='active' "
        "  AND COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
        "   WHERE a.object_id=o.id AND a.name='status' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1),'open')='open' "
        "  AND EXISTS (SELECT 1 FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='branch')" + repo_clause,
        *params)
    out = []
    for r in rows:
        files = r["files_touched"]
        if isinstance(files, str):
            files = json.loads(files)
        out.append({"id": str(r["id"])[:8], "summary": r["summary"] or "",
                    "branch": r["branch"] or "", "files_touched": files or [],
                    "owner": r["owner"]})
    return out


def held_work_overlap(
    files: list[str], candidates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Pure, no IO: which `candidates` (each an `open_held_work()` row) touch at least one
    of `files`. The actual collision check; everything above this just supplies the rows.
    Never blocks, never refuses; a caller decides what to do with what it finds (a
    fleet-wide check that can false-positive must never refuse-to-serve)."""
    wanted = set(files)
    return [c for c in candidates if wanted & set(c.get("files_touched") or [])]


async def _current_owner(pool: asyncpg.Pool, thread_id: uuid.UUID) -> str | None:
    """The winning `owner` value for a thread (grade DESC, then recency, the same
    resolution `open_thread_wall` already reads). Used to name the holder of an existing
    lease when a near-duplicate obligation surfaces instead of minting a parallel build."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='owner' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1",
        thread_id,
    )


async def _thread_named_properties(
    pool: asyncpg.Pool, thread_id: uuid.UUID, names: tuple[str, ...],
) -> dict[str, str]:
    """The winning value of each named property on a thread, present only where one
    exists. The read side discarded_on_noop() needs to compare against a caller's
    supplied fields on open_thread's own dedup branch."""
    rows = await pool.fetch(
        "SELECT a.name, a.value #>> '{}' AS val FROM current_assertions a "
        "WHERE a.object_id=$1 AND a.name = ANY($2::text[]) "
        "ORDER BY a.confidence DESC, a.observed_at DESC", thread_id, list(names))
    out: dict[str, str] = {}
    for r in rows:
        out.setdefault(r["name"], r["val"])  # first row per name is the winner (ORDER BY)
    return out


def discarded_on_noop(supplied: dict[str, Any], existing: dict[str, Any]) -> dict[str, Any]:
    """The write-boundary counterpart to the idempotent-write receipt problem: on an
    idempotent/already-exists early return, which of the caller's other supplied fields
    would have changed the record had the write actually run? `supplied` is pre-filtered
    to the fields the caller actually passed (never a default-means-unset sentinel like
    None); this returns the subset whose value differs from, or is simply absent in,
    `existing`. Empty when every supplied field already matches (a genuine no-op, no
    warning earned) or when the caller supplied nothing beyond the identity key.

    One rule for every early-return write path in this house, not a second hand-rolled
    diff invented per module: two known specimens (open_thread's `deduped: true` silently
    dropping arc/kind/owner/branch/files_touched; write_pin_additions' `written: False`
    unable to say whether a skipped key's value actually matched what was proposed) are
    the same defect in two modules, and fixing them independently is exactly how this
    fleet ended up with two disagreeing liveness authorities, a lesson this rule exists
    to not repeat a third time. Never a refusal: the caller still gets the existing
    record; this only names what of their own argument was thrown away, so they can act
    on knowing rather than discover it later by re-counting the population by hand."""
    return {k: v for k, v in supplied.items() if existing.get(k) != v}


async def _thread_resolved_in(pool: asyncpg.Pool, thread_id: uuid.UUID) -> str | None:
    """Whether a thread was already resolved before the current call: `resolved_in` is
    stamped both by `resolve_thread` itself and by record_decision's own `resolves=`
    mechanism (capture.py:450, the same shape, deliberately). Purely informational: the
    MCP tool's receipt uses this to tell a caller plainly when their call landed on an
    already-resolved thread, rather than looking identical to a fresh close. It does not
    gate or skip anything; resolve_thread always writes regardless of what this reads."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='resolved_in' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1",
        thread_id,
    )


async def _find_thread(
    pool: asyncpg.Pool, ref: str, *, require_identifier: bool = False,
) -> uuid.UUID | None:
    """A Thread by UUID, by canonical (`thread:<12hex>`, with or without the prefix), by
    short-id prefix, then by summary substring (shortest summary wins, closest to the
    query). The prefix leg runs before summary text because the fleet quotes threads by
    their 8-char short id inside other summaries: '5c57f54d' must resolve to thread
    5c57f54d-..., never to whichever thread's summary happens to mention it (that
    mis-resolve closed the wrong obligation once already). And an identifier-shaped ref
    that matches neither the canonical nor the short-id leg refuses outright rather than
    falling through to that same substring text (a near-miss where a bare canonical
    suffix silently matched a bug-report thread that merely quoted it).
    `require_identifier=True` drops the summary-substring leg entirely; record_decision's
    `resolves=` opts in, since it closes the thread it names rather than merely reading
    it. See `_resolve_ref` for the full ladder and rationale."""
    return await _resolve_ref(pool, "Thread", ref, text_field="summary",
                              require_identifier=require_identifier)


# the triage verbs' kinds: adopt = obligation (owed work, testimony),
# question = remembered but unowned (ranked out of the work wall), task = ordinary thread.
_TRIAGE_KINDS = ("obligation", "question", "task")


async def reclassify_thread(
    actions: Actions, ref: str, *, kind: str, because: str | None = None,
    owner: str | None = None, arc: str | None = None, source: str = _SOURCE,
) -> uuid.UUID | None:
    """Triage a thread without lying about its state (untouched does not mean resolved).
    Reclassification is testimony: a human read the thread and judged what it is: adopt a
    miner echo as real work (kind='obligation'), demote a promoted question back to a
    question (kind='question'), or mark it an ordinary task. The status is untouched: a
    question stays open in the record; the lens ranks it out of the work wall.
    SELF_DECLARED (outranks the miner's DERIVED kind), event-sourced, reversible. Returns
    the thread id, or None if `ref` matched nothing. `owner` optionally claims the thread
    in the same act (see open_thread); triage is where an existing thread learns whose
    move it is.

    `arc` closes a backfill gap: `open_thread`'s own `arc` param only ever writes on a
    genuinely new thread; its near-duplicate collision path
    (`find_near_duplicate_open_thread`) returns the existing id and `deduped: "true"`
    without ever calling this module's own write block, so re-calling `open_thread` with
    the same summary text plus an `arc` value is a silent no-op on an already-open thread.
    This was discovered live: 17 attempted stamps, zero landed, caught by checking the
    record afterward rather than trusting the receipt. This was the missing verb, not a
    filing gap: `reclassify_thread` already exists for exactly this shape (judging an
    existing thread's own metadata after the fact) and `arc` is a closed taxonomy exactly
    like `kind`, so it gets the same validate-then-assert treatment rather than new
    machinery. Osiris-scoped, same law as `open_thread`'s own `arc`: legal to set only
    when the thread already carries an in_repo edge to osiris itself, dropped, never
    refused, for any other project's thread."""
    if kind not in _TRIAGE_KINDS:
        raise ValueError(f"kind must be one of {_TRIAGE_KINDS}")
    tid = await _find_thread(actions.pool, ref)
    if tid is None:
        return None
    if arc is not None:
        if await arc_in_scope_for_thread(actions.pool, tid):
            if arc not in ARCS:
                raise ValueError(f"arc must be one of {ARCS}, got {arc!r}")
        else:
            arc = None  # out-of-scope: dropped, never refused; see mcp_server's receipt
    observed = datetime.now(UTC)
    await actions.assert_property(tid, "kind", kind, source, observed, _CONF,
                                  evidence_class=_EC)
    if owner:
        await actions.assert_property(tid, "owner", owner.strip(), source, observed, _CONF,
                                      evidence_class=_EC)
    if arc:
        await actions.assert_property(tid, "arc", arc, source, observed, _CONF,
                                      evidence_class=_EC)
    if because:
        await actions.assert_property(tid, "reclassified_because", because, source, observed,
                                      _CONF, evidence_class=_EC)
    return tid


# The artifact resolver's own closer-type allowlist (Decision, Commit, Thread, Tension,
# Practice, see _find_artifact's docstring for why each is/isn't here), lowercased and
# colon-suffixed once so both the short-id and commit-hash branches strip the same
# recognized set the same way, never a second, drifting copy of this list.
_ARTIFACT_TYPE_PREFIXES = tuple(
    f"{t.lower()}:" for t in ("decision", "commit", "thread", "tension", "practice"))


def _split_repo_hash_artifact(a: str) -> tuple[str, str] | None:
    """Split a `repo:<name>@<hash>` artifact pointer into (repo name, lowercased hash), or
    None if `a` isn't shaped like one, in which case `_find_artifact` falls through to its
    other branches unchanged. A bare hash already resolves against every ingested repo's
    Commit objects with no project scoping at all (verified by reading `_find_artifact`'s
    SQL below and `_resolve_commit` one function up: neither ever joins or filters on
    SoftwareProject), so a fix landed in one project closing a thread in a different
    project via a bare hash was never actually blocked by this resolver. What was missing
    is this explicit disambiguating shape: when a short hash collides across two or more
    repos' Commits, a bare hash still correctly refuses (ambiguity means property-only,
    unchanged, deliberately, see `_find_artifact`'s own note on that), and until now a
    caller had no way to say "no, this repo's commit" short of pasting the full 40-char
    sha. `repo:<name>` reuses `_resolve_repo` (the same canonical-or-name resolver
    `link_repo`/census_trees/every other repo-scoping call site in this module already
    uses, no second resolver); the Commit search then joins `in_repo` (Commit --in_repo-->
    SoftwareProject, the same edge shape gitlog.py mints and `_fn_project` in
    compositions.py already scopes Commits by) to that one project. Case-insensitive on
    the `repo:` literal only, matching this module's own `repo.removeprefix('repo:')`
    convention elsewhere; the hash half is lowercased since every stored Commit canonical
    is lowercase hex."""
    if not a.lower().startswith("repo:") or "@" not in a:
        return None
    repo_part, _, hash_part = a[len("repo:"):].partition("@")
    repo_part = repo_part.strip()
    hash_part = hash_part.strip().lower()
    if repo_part and re.fullmatch(r"[0-9a-f]{7,40}", hash_part):
        return repo_part, hash_part
    return None


def _strip_recognized_artifact_prefix(lowered: str) -> str:
    """Strip a "type:" prefix from an already-lowercased artifact pointer, but only when
    it names one of _find_artifact's own closer types. A caller who copies a receipt's
    exact "type:short-id" shape, a very plausible thing to type since receipts constantly
    show the real 12-hex canonical in that exact shape, used to get refused by every
    branch below, because "decision:" contains non-hex letters that break a hex-only
    fullmatch at the 4th character. Mirrors the one technique _resolve_ref already uses
    one function away (its own `canon_prefix`/`hex_part` split) rather than inventing a
    second copy, one shape, one guard.

    An unrecognized prefix ("banana:3d504086") is deliberately left unstripped: it still
    fails the hex fullmatch below and this function still refuses, exactly as before this
    fix. Silently accepting an unknown prefix's tail as if it were the whole pointer would
    risk a coincidental hex-collision with an unrelated object, worse than the plain
    refusal a caller already gets today for a pointer this resolver doesn't recognize."""
    for prefix in _ARTIFACT_TYPE_PREFIXES:
        if lowered.startswith(prefix):
            return lowered[len(prefix):]
    return lowered


async def _find_artifact(pool: asyncpg.Pool, artifact: str) -> uuid.UUID | None:
    """Resolve an artifact pointer to the graph object it names: an exact canonical
    ('commit:abc123def456', 'decision:...', 'thread:...'), an object UUID or 8-char short
    id (Decision, Commit, Thread, Tension, or Practice, the closer types, widened to
    include Thread since a fold/merge into a sibling thread is a legitimate closure this
    resolver used to have no shape for at all, and widened to Tension/Practice after a
    closure-backfill review of unresolvable rows found real citations of exactly these
    types that this allowlist was simply missing), a bare git hash (prefix-matched on
    commit:, Commit only, searched across every ingested repo's Commits with no project
    scoping at all, since a thread reference never looks like a hash, so that branch is
    unchanged), or `repo:<name>@<hash>` (the explicit disambiguator for when a bare hash's
    short prefix collides across two or more repos' Commits; see
    `_split_repo_hash_artifact`'s own docstring for the full story, including the finding
    that a bare hash closing a thread in a different project than the one the commit
    landed in already worked before this format existed). Deliberately not widened to
    Agent: an Agent's short code is never a prefix of its own `id` (`id` is an unrelated
    random UUID, the short code lives only in `canonical`), so adding Agent here would
    match nothing, ever; and even if it matched, an Agent is not what closed a thread
    (`closed_by` already exists for that shape). None for free-form pointers (a file:line,
    a path); the resolved_artifact property alone carries those, and a pointer that
    matches nothing must never block the close.

    PREFIX VS NO-PREFIX (corrected from an earlier, too-broad "short id vs full UUID"
    framing): a caller who types the short form of a canonical-shaped citation
    ("decision:3d504086", 8 hex chars, "type:"-prefixed) used to fail every branch below
    and silently fall back to the weak closed_by edge; a bare id with no prefix
    ("3d504086") always worked. Both the short-id and commit-hash branches now strip a
    recognized type prefix (see `_strip_recognized_artifact_prefix`) before their own hex
    fullmatch; an unrecognized prefix is left alone and still refuses, same as before this
    fix."""
    a = artifact.strip()
    oid = await pool.fetchval("SELECT id FROM objects WHERE canonical=$1", a)
    if oid is not None:
        return uuid.UUID(str(oid))  # exact canonical: any precisely-named type may close
    split = _split_repo_hash_artifact(a)
    if split is not None:
        repo_name, hash_part = split
        proj_id = await _resolve_repo(pool, repo_name)
        if proj_id is None:  # named repo doesn't resolve: clean refusal, no guessing
            return None
        rows = await pool.fetch(
            "SELECT c.id FROM objects c JOIN links l ON l.from_id=c.id AND l.type='in_repo' "
            "AND l.to_id=$2 WHERE c.type='Commit' AND c.canonical LIKE 'commit:' || $1 || '%' "
            "LIMIT 2", hash_part, proj_id)
        return uuid.UUID(str(rows[0]["id"])) if len(rows) == 1 else None
    hex_part = _strip_recognized_artifact_prefix(a.lower())
    if re.fullmatch(r"[0-9a-f]{8}(-[0-9a-f-]{4,28})?", hex_part):
        rows = await pool.fetch(
            "SELECT id FROM objects WHERE id::text LIKE $1 || '%' "
            "AND type IN ('Decision', 'Commit', 'Thread', 'Tension', 'Practice') LIMIT 2",
            hex_part[:8])
        if len(rows) == 1:  # ambiguity means property-only, never a guessed edge
            return uuid.UUID(str(rows[0]["id"]))
    if re.fullmatch(r"[0-9a-f]{7,40}", hex_part):
        rows = await pool.fetch(
            "SELECT id FROM objects WHERE type='Commit' "
            "AND canonical LIKE 'commit:' || $1 || '%' LIMIT 2", hex_part)
        if len(rows) == 1:
            return uuid.UUID(str(rows[0]["id"]))
    return None


async def resolve_thread(
    actions: Actions, ref: str, *, because: str | None = None,
    artifact: str | None = None, source: str = _SOURCE
) -> uuid.UUID | None:
    """Close a thread at source: the session marking a question answered, so it leaves the
    open list and joins the resolved section. Matches the miner's self-heal shape
    (status=resolved + resolved_in + resolved_because) so the `briefing` resolved section
    renders it, with `resolved_in='session'` recording that a session closed it rather than
    a later commit. `ref` is a Thread UUID or a summary substring. Event-sourced via a status
    assertion that supersedes the prior 'open' within-source, never a delete. Returns the
    thread id, or None if `ref` matched nothing.

    `artifact` (added because `because` was being abused as a completion essay, since there
    was nowhere to put "here is what actually got built") is a pointer to the thing that
    closed the thread: a commit hash, a decision id, a file:line, or `repo:<name>@<hash>`
    when a bare hash would collide across two or more repos' Commits. It is always kept as
    the resolved_artifact property, and when it names a graph object (Decision, Commit, or
    any exact canonical) a resolved_by edge is minted too, the strong closure witness the
    closure-miner almost never finds. See `_find_artifact`'s own docstring for the full set
    of recognized shapes, including the finding that a bare commit hash was never
    project-scoped: a fix landed in one project could already close a thread in another via
    a plain hash with no ambiguity; `repo:<name>@<hash>` only exists for the ambiguous case.

    Phase 1a (a prior measurement found 78% of closures left no traversable trace, because
    resolved_by only fires when `artifact` names a graph object): every closure now mints
    exactly one closure edge, resolved_by when the artifact resolves to a Commit/Decision
    (unchanged), else closed_by to the resolving agent (`source`), whether `artifact` was
    unresolvable free text or absent entirely. A weak edge that always exists beats a
    strong one that exists a fifth of the time; the read path can traverse the weak one
    and cannot traverse absence.

    RE-RESOLVING IS ALLOWED, NOT REFUSED, ON PURPOSE: `_find_thread` matches on identity
    only, never status. A second call on an already-resolved thread is not a mistake to
    guard against; Phase 1a's own multi-witness design depends on it
    (test_two_strong_edges_still_report_strong: record_decision's `resolves=` closes a
    thread with only an `answers` edge; a later resolve_thread(artifact=...) call naming
    the real closing commit/decision is how the strong `resolved_by` witness gets attached
    after the fact, and must not be refused). `because`/`resolved_artifact` follow the same
    latest-write-wins model as every other property this kernel writes: the second call's
    text becomes the new current value, the first is not lost (still readable via the
    non-current assertion rows, `recall`'s own decision/thread addenda pattern), never a
    claim that past reasoning survives at the current read. `resolved_by`/`closed_by`
    edges, unlike the property, accumulate per distinct target (check-then-create) rather
    than replacing, so a thread can carry more than one closure witness. The MCP tool's
    receipt names when a call landed on an already-resolved thread, so a caller is told
    plainly rather than left to assume this was the first close."""
    tid = await _find_thread(actions.pool, ref)
    if tid is None:
        return None
    observed = datetime.now(UTC)
    # SINGULAR: a resolve is a workflow transition, not a corroborating witness. It must
    # supersede every open source's status, not just its own, or the thread reads
    # simultaneously open-and-resolved (a shape once found affecting hundreds of
    # obligations at once). open_thread's own writes stay plain assert_property,
    # unchanged, so multi-source alarm coexistence (deploy_guard's two boot services) is
    # untouched.
    await actions.assert_singular_property(tid, "status", "resolved", source, observed,
                                           _CONF, evidence_class=_EC)
    await actions.assert_singular_property(tid, "resolved_in", source, source, observed,
                                           _CONF, evidence_class=_EC)
    if because:
        await actions.assert_singular_property(tid, "resolved_because", because, source,
                                               observed, _CONF, evidence_class=_EC)
    target = None
    if artifact:
        await actions.assert_property(tid, "resolved_artifact", artifact.strip(), source,
                                      observed, _CONF, evidence_class=_EC)
        target = await _find_artifact(actions.pool, artifact)
        if target is not None and not await actions.pool.fetchval(
                "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 "
                "AND type='resolved_by' LIMIT 1", tid, target):
            await actions.create_link(tid, target, "resolved_by", source, observed, _CONF,
                                      evidence_class=_EC)
    if target is None:
        await _mint_closed_by(actions, tid, source, observed)
    return tid


async def resolve_threads_bulk(
    actions: Actions, refs: list[str], *, because: str, artifact: str | None = None,
    dry_run: bool = True, source: str = _SOURCE,
) -> dict[str, Any]:
    """Batch-close obligations, built as a parameter on the existing single-ref
    `resolve_thread` rather than a new verb: every ref closes under the same shared
    `because`/`artifact`, each write still its own independent compensating event (never
    one collapsed write) via a plain per-ref call to `resolve_thread` below.

    REFUSE-WHOLE-RUN, same posture as `_retire_handoff_backlog` / backfill_thread_status_
    collapse.py: if any ref fails to resolve to exactly one real thread (unmatched, or two
    refs in the same call resolving to the same thread, a caller-side collision, not a
    graph ambiguity), nothing is written and the refusal names every offending ref. A
    batch close with an unmeasured error rate risks closing threads nobody actually
    reviewed; refusing outright beats silently skipping the bad rows and closing the rest.

    `because` is mandatory here (unlike the single-ref door, where it's optional): a
    batch close with no shared, recorded reason is exactly the "closing 1000+ obligations
    that are mostly stale" failure mode this guards against.

    `dry_run=True` (hard default, mirrors every other bulk primitive in this codebase)
    previews every match (id, summary, whether already resolved) and writes nothing; pass
    `dry_run=False` to actually close. `_find_thread(..., require_identifier=True)` is
    used for every ref (record_decision's own `resolves=` ladder, not the looser
    summary-substring read `resolve_thread`'s single-ref door still allows): a batch
    closes what it names, same discipline as resolves=, never a loose match."""
    if not refs:
        return {"ok": False, "reason": "empty ref list — nothing to do"}
    if not because or not because.strip():
        return {"ok": False, "reason": "because is mandatory for a batch close"}
    resolved: dict[str, uuid.UUID | None] = {}
    for ref in refs:
        resolved[ref] = await _find_thread(actions.pool, ref, require_identifier=True)
    unresolved = [r for r, tid in resolved.items() if tid is None]
    seen: dict[uuid.UUID, str] = {}
    duplicates: list[dict[str, str]] = []
    for r, tid in resolved.items():
        if tid is None:
            continue
        if tid in seen:
            duplicates.append({"first": seen[tid], "also": r})
        else:
            seen[tid] = r
    if unresolved or duplicates:
        return {
            "ok": False,
            "reason": "refusing the whole batch — not every ref resolved to exactly one "
                      "distinct thread; same posture as _retire_handoff_backlog, nothing "
                      "written",
            "unresolved": unresolved,
            "duplicates": duplicates,
        }
    matched: dict[str, uuid.UUID] = {r: t for r, t in resolved.items() if t is not None}
    previews = []
    for ref, tid in matched.items():
        summary = await actions.pool.fetchval(
            "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
            "AND a.name='summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1",
            tid)
        already = await _thread_resolved_in(actions.pool, tid) is not None
        previews.append({
            "ref": ref, "id": str(tid)[:8], "summary": summary,
            "already_resolved": already,
        })
    if dry_run:
        return {"ok": True, "dry_run": True, "would_close": previews, "count": len(previews)}
    closed = []
    for tid in matched.values():
        await resolve_thread(actions, str(tid), because=because, artifact=artifact,
                             source=source)
        closed.append(str(tid)[:8])
    return {
        "ok": True, "dry_run": False, "closed": closed, "count": len(closed),
        "because": because,
    }


async def _mint_closed_by(
    actions: Actions, tid: uuid.UUID, source: str, observed: datetime
) -> None:
    """The Phase 1a fallback edge: who closed a thread, minted whenever resolved_by did
    not land for this closure. `source` resolves to a real object of the right type (a
    prior "stand for now" deferral on the Actor/Source type distinction has since been
    resolved): a real `agent:<id>` string finds the Agent object mount() already created,
    unchanged, the common case; a member of the operator-attribution family
    (`_OPERATOR_ACTORS`, seats.py, 'operator', 'analyst:operator', 'console', deliberately
    treated as one notion everywhere else in this codebase too, so this does not invent a
    second) resolves to the real operator Person object (`ensure_operator_person`); the
    module's own bare 'session' default resolves to the singleton SystemSource
    (`ensure_system_source`). No source string mints a placeholder Agent under a non-Agent
    canonical any more, every closer is a real object of a real type, and no closer is
    ever left with nothing to point at. Idempotent per (thread, closer) pair, same
    check-then-create shape as resolved_by."""
    from src.orchestrator.seats import _OPERATOR_ACTORS

    if source in _OPERATOR_ACTORS:
        closer = await ensure_operator_person(actions, source)
    elif source == _SOURCE:
        closer = await ensure_system_source(actions, source)
    else:
        closer = await actions.create_or_find_object("Agent", source, source)
    if not await actions.pool.fetchval(
            "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 "
            "AND type='closed_by' LIMIT 1", tid, closer):
        await actions.create_link(tid, closer, "closed_by", source, observed, _CONF,
                                  evidence_class=_EC)


_CLOSED_BY_PLACEHOLDER_CANONICALS = ("session", "analyst:operator")


async def backfill_closed_by_real_sources(
    actions: Actions, *, actor: str, dry_run: bool = True, because: str | None = None,
) -> dict[str, Any]:
    """THE COMPENSATING FOLD: re-points every existing `closed_by` edge still targeting
    one of the two placeholder Agent objects `_mint_closed_by` used to mint (canonical
    literally 'session' or 'analyst:operator', never `agent:`-prefixed, so `merge()`
    cannot touch them: it dispatches on the ref's own string form, agent:/seat:/else, and
    refuses a cross-type pairing outright; these placeholders are Agent-typed but their
    correct target is Person or SystemSource, a different type either way) at the real
    object `_mint_closed_by` would mint today for that same edge's own `source_id`.

    ATTRIBUTION SURVIVES THE FOLD (same mechanism as projects.py's
    `_move_project_estate`): the re-pointed edge keeps the original edge's own source_id/
    confidence/evidence_class; `actor` is passed only to `invalidate_link`'s and
    `create_link`'s own separate `actor=` kwarg, never used to overwrite what the closure
    itself was attributed to. Once every closed_by edge off a placeholder is moved, the
    placeholder is `retire_object`'d (status flip via a compensating event, never a
    delete), never before every edge off it is moved, so a placeholder is never retired
    while still load-bearing.

    DRY RUN IS THE DEFAULT. `dry_run=False` requires a non-blank `because`. Idempotent: a
    repeat call finds no placeholder-targeted closed_by edges left once the fold is done."""
    if not dry_run and not (because or "").strip():
        return {"error": "backfilling without a because is an un-audited repair — cite "
                         "the evidence/ruling that authorizes it"}
    from src.orchestrator.seats import _OPERATOR_ACTORS

    pool = actions.pool
    rows = await pool.fetch(
        "SELECT l.from_id AS thread_id, l.to_id AS placeholder_id, "
        "  ph.canonical AS placeholder_canonical, l.source_id, l.confidence, "
        "  l.evidence_class, l.first_seen "
        "FROM links l JOIN objects ph ON ph.id=l.to_id "
        "WHERE l.type='closed_by' AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "AND ph.type='Agent' AND ph.canonical = ANY($1::text[])",
        list(_CLOSED_BY_PLACEHOLDER_CANONICALS))
    observed = datetime.now(UTC)
    plan: list[dict[str, Any]] = []
    by_placeholder: dict[uuid.UUID, list[dict[str, Any]]] = {}
    for row in rows:
        source = row["source_id"]
        if source in _OPERATOR_ACTORS:
            target_kind = "Person"
        elif source == _SOURCE:
            target_kind = "SystemSource"
        else:
            # a placeholder minted under 'session'/'analyst:operator' whose own edge
            # source_id is neither, e.g. a caller passed source= explicitly as one of
            # the two placeholder canonicals themselves. Genuinely ambiguous which real
            # object this closure should now attribute to; never guessed.
            plan.append({"thread": str(row["thread_id"])[:8],
                        "placeholder": row["placeholder_canonical"],
                        "verdict": "abstain",
                        "reason": f"edge source_id {source!r} is neither the operator-"
                                  "attribution family nor the module default — cannot "
                                  "map to a real target without guessing"})
            continue
        entry = {"thread": str(row["thread_id"])[:8], "placeholder": row["placeholder_canonical"],
                 "source": source, "verdict": "repoint", "to_type": target_kind}
        plan.append(entry)
        if not dry_run:
            new_target = (await ensure_operator_person(actions, source) if target_kind == "Person"
                         else await ensure_system_source(actions, source))
            await actions.create_link(row["thread_id"], new_target, "closed_by", source,
                                      row["first_seen"] or observed, row["confidence"],
                                      evidence_class=row["evidence_class"])
            await actions.invalidate_link(row["thread_id"], row["placeholder_id"], "closed_by",
                                          actor, observed,
                                          reason="backfill_closed_by_real_sources: re-pointed "
                                                 "off a placeholder Agent to a real object")
            by_placeholder.setdefault(row["placeholder_id"], []).append(entry)
    retired: list[str] = []
    if not dry_run:
        for placeholder_id in by_placeholder:
            remaining = await pool.fetchval(
                "SELECT 1 FROM links WHERE to_id=$1 AND type='closed_by' "
                "AND (valid_until IS NULL OR valid_until > now()) LIMIT 1", placeholder_id)
            if remaining:
                continue
            canonical = await pool.fetchval(
                "SELECT canonical FROM objects WHERE id=$1", placeholder_id)
            from src.orchestrator.agents import retire_agent
            result = await retire_agent(actions, agent_id=canonical, actor=actor,
                                        because=f"{because} (backfill_closed_by_real_sources: "
                                                "every closed_by edge off this placeholder has "
                                                "been re-pointed to a real object)")
            if not result.get("error"):
                retired.append(canonical)
    return {"dry_run": dry_run, "scanned": len(rows), "plan": plan,
           "retired": retired if not dry_run else None,
           "because": because if not dry_run else None}


async def backfill_operator_charter(
    actions: Actions, *, actor: str, dry_run: bool = True, because: str | None = None,
) -> dict[str, Any]:
    """THE OPERATOR CHARTER BACKFILL ("authority by charter"): mints a `governs` link
    from `person:operator` (via `ensure_operator_person`) to every currently-active
    SoftwareProject it doesn't already govern. This is what makes "the single operator
    today is chartered over every project, so behavior does not change" literally true,
    the moment `charter_for`'s and `record_decision`'s own operator checks start
    consulting the charter instead of a bare literal.

    DRY RUN IS THE DEFAULT. `dry_run=False` requires a non-blank `because`. Idempotent:
    a repeat call finds no active SoftwareProject left uncovered.

    Never runs against production on its own: this is a one-time, deliberate act, held
    for an explicit `dry_run=False` call naming why, same discipline every backfill in
    this file already keeps."""
    if not dry_run and not (because or "").strip():
        return {"error": "backfilling without a because is an un-audited repair — cite "
                         "the evidence/ruling that authorizes it"}
    from src.orchestrator.charter import operator_charter_of

    pool = actions.pool
    person_id = await ensure_operator_person(actions, source=actor)
    already = set(await operator_charter_of(pool, _OPERATOR_PERSON_CANONICAL))
    rows = await pool.fetch(
        "SELECT id, canonical FROM objects WHERE type='SoftwareProject' AND status='active'")
    to_add = sorted(
        (r["canonical"].removeprefix("repo:"), r["id"]) for r in rows
        if r["canonical"].removeprefix("repo:") not in already)
    plan = [{"repo": name, "verdict": "mint"} for name, _oid in to_add]
    minted: list[str] = []
    if not dry_run:
        observed = datetime.now(UTC)
        for name, proj_id in to_add:
            await actions.create_link(person_id, proj_id, "governs", actor, observed,
                                      _CONF, evidence_class=_EC)
            minted.append(name)
    return {"dry_run": dry_run, "already_chartered": sorted(already), "scanned": len(rows),
           "plan": plan, "minted": minted if not dry_run else None,
           "because": because if not dry_run else None}


async def assign_thread(
    actions: Actions, ref: str, *, owner: str, because: str | None = None,
    source: str = _SOURCE,
) -> uuid.UUID | None:
    """HAND A THREAD BACK: reassign whose move it is, without closing it. Without this,
    a debt sitting in the operator's queue had exactly two exits: they do it, or it rots,
    so anything they were ever cc'd on accumulated on them forever with no way to resolve
    those debts per thread or per project. This is the third door: owner='<project>'
    pushes the duty back to the hands that actually own it, where orient() surfaces it on
    that project's wall at its next mount. Nobody dispatches; the graph does.

    Not a resolve and never pretends to be (untouched does not mean resolved): status is
    untouched, the debt stays open in the record, it simply stops being theirs. Reversible
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
    """SNOOZE: the debt is real, owned, and not now. Stamps `deferred_until`; the lens
    hides it from the queue/wall until that date, then it returns on its own. This is the
    honest option for "yes, mine, but not this month": the alternative was leaving it on
    the queue to be re-read and re-skipped every single day, which is how a queue stops
    being read at all.

    Fix at the lens, never at the record: the thread stays open and owned; only its
    visibility moves. A deferral is testimony with an expiry, it can never silently
    become a resolve."""
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
    """Keep a memory lived for its own sake: a home for existential/philosophical
    conversation that is not a work ticket, simply memories worth keeping. A Reflection
    is its own type, so every work surface structurally cannot present it as a ticket: it
    is not a Thread (nothing to resolve), not a Decision (nothing settled), not a
    candidate (the extractor's fourth rule already refuses first-person-about-the-speaker).
    It is remembered, attributed, queryable, and never actionable. Idempotent on the
    body."""
    observed = datetime.now(UTC)
    r = await actions.create_or_find_object("Reflection", _canon("reflection", body), source)
    await actions.assert_property(r, "body", body, source, observed, _CONF, evidence_class=_EC)
    await actions.assert_property(r, "summary", summary or body[:160], source, observed,
                                  _CONF, evidence_class=_EC)
    if repo:
        mint_confession = await link_repo(actions, r, repo, observed, source=source,
                                          evidence_class=_EC, confidence=_CONF)
        if mint_confession:
            logger.warning("record_reflection(repo=%r): %s", repo,
                           mint_confession["confession"])
    return r


async def record_tension(
    actions: Actions, pole_a: str, pole_b: str, *, lean: str | None = None,
    why: str | None = None, repo: str | None = None, source: str = _SOURCE,
) -> uuid.UUID:
    """Hold a live tension: two positions in productive tension, neither settled. Unlike
    record_decision (which settles) or open_thread (which closes), a tension is held: the
    current `lean` and `why` are captured, but the object is never auto-resolved or
    consolidated away, because it is its own type, so grade-resolution and dedup
    structurally cannot flatten it. Re-record the same poles to move the lean; the lean
    assertion history is the record of how it shifted across sessions. Idempotent on the
    unordered pole pair (so (a,b) and (b,a) are one tension)."""
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
        mint_confession = await link_repo(actions, t, repo, observed, source=source,
                                          evidence_class=_EC, confidence=_CONF)
        if mint_confession:
            logger.warning("record_tension(repo=%r): %s", repo,
                           mint_confession["confession"])
    return t


async def record_blind_spot(
    actions: Actions, surface: str, cannot_see: str, *, verify_with: str | None = None,
    repo: str | None = None, source: str = _SOURCE,
) -> uuid.UUID:
    """Register a project's known blind spot: what this project's harness/rig cannot
    verify from here, and where the real verification lives. This traces back to a
    concrete failure: 459 headless-Chromium tests stayed green while every iPhone was
    broken, because the most expensive thing to rediscover was not a fact but the shape
    of the harness's own blind spot. A BlindSpot is its own type, held like a Tension: a
    stable per-project fact that dedup and grade-resolution structurally cannot flatten
    away, surfaced at orient() so a session knows what it cannot see before it trusts a
    green harness. Idempotent per (repo, surface), re-record to sharpen the wording; the
    assertion history keeps every telling. `surface` names the capability
    ('webkit-rendering', 'ios-touch'); `cannot_see` states the gap; `verify_with` points
    at the rig or ritual that actually verifies."""
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
        mint_confession = await link_repo(actions, b, repo, observed, source=source,
                                          evidence_class=_EC, confidence=_CONF)
        if mint_confession:
            logger.warning("record_blind_spot(repo=%r): %s", repo,
                           mint_confession["confession"])
    return b


# every hook surface this files an alarm for: the same strings a health reader
# (src/orchestrator/smoke.py's whisper_health) looks these objects up by
HOOK_ALARM_SURFACES: tuple[str, ...] = (
    "whisper/automount", "hook/session-end", "hook/precompact", "hook/stophook",
)
_HOOK_ALARM_VERIFY_WITH = ("check the server log (journalctl -u osiris-mcp, tag "
                          "'osiris.whisper' for the whisper route) for the full traceback")


async def record_hook_failure(actions: Actions, *, surface: str, cannot_see: str) -> None:
    """File a session-lifecycle hook's failure into the same channel `record_blind_spot`
    already gives every other unverifiable-from-here gap. This traces back to a real
    incident where the SessionStart whisper returned a 500 on the majority of arrivals,
    silently swallowed by a fail-open path. A hook mid-failure cannot investigate itself,
    only confess; this is that confession. `surface` should be one of
    `HOOK_ALARM_SURFACES`, so `whisper_health`'s read side finds it; a caller passing
    something else still records, just outside that reader's known set. Rate-limited by
    construction, not a counter: `record_blind_spot` is idempotent per (repo, surface), so
    a hundred identical failures collapse onto the same graph object (its assertion
    history keeps every telling, which is what `whisper_health` counts), never a hundred
    new rows. Never raises: this runs inside an already-failing `except` block in every
    caller; a second failure here must stay silent, never mask or replace the first."""
    try:
        await record_blind_spot(actions, surface, cannot_see,
                                verify_with=_HOOK_ALARM_VERIFY_WITH, repo="osiris")
    except Exception:  # noqa: BLE001 - an alarm that itself fails must stay silent, never loud
        pass


# The same blind-spot channel HOOK_ALARM_SURFACES already uses, for another
# silent-forever failure found alongside it: embed_pass's own
# `except Exception: _log.warning(...)` swallowed every semantic-embedding load failure
# into a log line nobody watches. `smoke.embed_health` is this surface's own read side,
# same shape as `whisper_health`.
EMBED_ALARM_SURFACE = "embed/model2vec-load"
_EMBED_ALARM_VERIFY_WITH = ("journalctl --user -u osiris-worker | grep -i embed_pass — the "
                            "cron logs the real exception on every failed tick too")


async def record_embed_load_failure(actions: Actions, *, cannot_see: str) -> None:
    """File a semantic-embedding load failure (a sticky latch closing, or any other
    embed_backfill exception) into the blind-spot channel: `embed_pass`'s own generic
    `except Exception` used to log-and-return-0 with nothing else watching. Rate-limited
    by construction, same as `record_hook_failure`: idempotent per (repo, surface), so a
    hundred identical cron ticks collapse onto one graph object (the assertion history
    keeps every telling, which is what a health reader counts). Never raises: called from
    inside an already-failing cron tick; a second failure here must stay silent, never
    mask or replace the first."""
    try:
        await record_blind_spot(actions, EMBED_ALARM_SURFACE, cannot_see,
                                verify_with=_EMBED_ALARM_VERIFY_WITH, repo="osiris")
    except Exception:  # noqa: BLE001 - an alarm that itself fails must stay silent, never loud
        pass


async def kill_superstition(
    actions: Actions, statement: str, *, killed_by: str, repo: str | None = None,
    source: str = _SOURCE,
) -> uuid.UUID:
    """Put a workaround on the record as dead: the fix that landed names the practice it
    obsoletes. The recurring pattern this guards against: a bug spawns workarounds, the
    workarounds get written into notes, succession notes, and agent memory across the
    fleet, the bug gets fixed, and the workarounds persist as inherited law, taxing every
    heir forever. A Superstition is a first-class object so the kill is searchable
    forever; orient announces recent kills fleet-wide (recent_dead_superstitions) so any
    agent whose memory carries the practice strikes it. `statement` is the workaround as
    it propagates (quote the words agents actually inherit, e.g. 'NEVER DM BY NAME');
    `killed_by` points at the fix (a decision id, a commit hash). Idempotent on the
    normalized statement.

    THE CORRECTIVE ANALOG: killed_by used to be a plain property with no
    reverse-queryable edge, so a Decision's own record had nothing to show for what it
    killed. When killed_by resolves to a graph object (same resolver as Thread's
    resolved_by, _find_artifact) a killed_by link is minted too, idempotent per
    (superstition, target); the property always carries the raw pointer regardless of
    whether it resolved, same two-tier shape as resolve_thread's
    resolved_artifact/resolved_by."""
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
        mint_confession = await link_repo(actions, s, repo, observed, source=source,
                                          evidence_class=_EC, confidence=_CONF)
        if mint_confession:
            logger.warning("kill_superstition(repo=%r): %s", repo,
                           mint_confession["confession"])
    target = await _find_artifact(actions.pool, killed_by)
    if target is not None and not await actions.pool.fetchval(
            "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='killed_by' LIMIT 1",
            s, target):
        await actions.create_link(s, target, "killed_by", source, observed, _CONF,
                                  evidence_class=_EC)
    return s


async def _find_practice(
    pool: asyncpg.Pool, ref: str, *, require_identifier: bool = False,
) -> uuid.UUID | None:
    """A Practice by UUID, by canonical, by short-id prefix, then by `statement`
    substring (shortest statement wins), same resolution ladder as
    `_find_decision`/`_find_thread`; see `_resolve_ref`. `require_identifier=True` drops
    the statement-substring leg, the same opt-in `_find_decision`/`_find_thread` expose,
    for a call path that converts or links the record it names rather than merely
    reading it (`refutes=`/`confirms=` on record_decision: an identifier-shaped arg like
    a bare local task number must refuse fleet-wide, not search for it)."""
    return await _resolve_ref(pool, "Practice", ref, text_field="statement",
                              require_identifier=require_identifier)


async def _witness_link(
    actions: Actions, practice_id: uuid.UUID, evidence_id: uuid.UUID,
    source: str, observed: datetime,
) -> bool:
    """Mint `witnesses` (Practice -> Decision/Commit/Thread) idempotently: one witness is
    a hunch, four is a pattern. Never minted from a mere search-topical match: only an
    explicit caller (record_practice's `witnesses=`, record_decision's `confirms=`)
    creates one, the same discipline grounds/obsoletes/supersedes already follow. Returns
    whether a new link was minted (false = already witnessed, a no-op)."""
    exists = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='witnesses'",
        practice_id, evidence_id)
    if exists:
        return False
    await actions.create_link(practice_id, evidence_id, "witnesses", source, observed, _CONF,
                              evidence_class=_EC)
    return True


async def practice_confirmed_count(pool: asyncpg.Pool, practice_id: uuid.UUID) -> int:
    """`confirmed` is derived, never a stored/incremented scalar: the count of
    `witnesses` links at read time. An incremented-on-write counter would need
    read-then-write-under-lock, the exact race class found live once already in
    bridged_seat/record_bridge_anchor; a link count can never desync from the links it
    counts."""
    n = await pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND type='witnesses'", practice_id)
    return int(n or 0)


async def record_practice(
    actions: Actions, statement: str, *, failure_prevented: str | None = None,
    surface: str | None = None, repo: str | None = None,
    witnesses: list[uuid.UUID] | None = None, source: str = _SOURCE,
    unlinked_because: str | None = None, unlinked_because_kind: str | None = None,
) -> uuid.UUID:
    """Capture a transferable technique, Superstition's positive twin: the graph could
    hold what to stop believing but nothing held engineering technique that outlives any
    single repo or date, so independent teams could re-derive the same lesson without
    ever finding each other's version. `statement` is the imperative one-liner (e.g.
    'arm before you seal, one step, not two'); `failure_prevented` is the concrete
    symptom that makes it findable mid-failure, not just on reflection; `surface` reuses
    BlindSpot's domain vocabulary. Timeless: never moment-stamped, true regardless of
    repo or date, unlike a Decision. `witnesses` links the Decisions/Commits/Threads that
    are this Practice's evidence at birth; `confirms=` on a later record_decision call is
    how a re-encounter adds one more (see practice_confirmed_count, `confirmed` is that
    link count, never a separate stored number). Idempotent on the normalized statement.

    `unlinked_because`/`unlinked_because_kind` widens the declare-or-refuse gate to this
    door: a real `in_repo` link satisfies the gate, `unlinked_because` is the hatch
    otherwise, `_enforce_required_links` in scope for `("repo",)` only. A Practice is
    deliberately repo-agnostic (timeless, may span every project a lesson applies to), so
    this scope stays inert (no refusal) unless and until this type's own
    `required_link_kinds` is armed to include `"repo"`; wiring it now only establishes the
    same convention every other capture-a-fact door already carries, never a new
    requirement sprung on an existing caller."""
    observed = datetime.now(UTC)
    key = " ".join(statement.split()).lower()
    async with actions.atomic() as a:
        p = await a.create_or_find_object("Practice", _canon("practice", key), source)
        await a.assert_property(p, "statement", statement.strip(), source, observed, _CONF,
                                evidence_class=_EC)
        if failure_prevented:
            await a.assert_property(p, "failure_prevented", failure_prevented, source,
                                    observed, _CONF, evidence_class=_EC)
        if surface:
            await a.assert_property(p, "surface", surface, source, observed, _CONF,
                                    evidence_class=_EC)
        if repo:
            mint_confession = await link_repo(a, p, repo, observed, source=source,
                                              evidence_class=_EC, confidence=_CONF)
            if mint_confession:
                logger.warning("record_practice(repo=%r): %s", repo,
                               mint_confession["confession"])
        for w in witnesses or []:
            await _witness_link(a, p, w, source, observed)
        await _enforce_required_links(
            a, p, "Practice", kinds_in_scope=("repo",),
            unlinked_because=unlinked_because, source=source, observed=observed,
            unlinked_because_kind=unlinked_because_kind)
    return p


# WORK-LINEAGE (Graph-Engineering arc): item 1 of 3, the types/edges exist and
# are mintable. Items 2 (the incoming-direction declare-or-refuse gate for
# artifact-has-authoring-run-plus-version) and 3 (traceability_census + graph_lint
# 'untraceable-output' + the weekly desk line) are separate, later commits.
#
# ensure_agent_run RETIRED: the AgentRun pointer it lazily minted folded into Agent,
# since the session object is the Agent generation. record_artifact's own
# `authoring_run` param below now resolves an existing Agent generation directly
# (_resolve_ref, never a mint) instead of lazily minting a second pointer object for
# the same session.


async def ensure_artifact(actions: Actions, key: str, source: str = _SOURCE) -> uuid.UUID:
    """Find-or-mint an Artifact: a build/deploy/document output Commit does not
    already cover (Commit stays its own type, never aliased). `key` is the caller's
    own identifying string (a build id, deploy tag, or report path), canonicalized as
    artifact:<key>."""
    return await actions.create_or_find_object("Artifact", f"artifact:{key}", source)


async def mint_produced(
    actions: Actions, run_id: uuid.UUID, output_id: uuid.UUID, source: str = _SOURCE,
) -> bool:
    """The run's own output, the traceability invariant's run leg. Idempotent: returns
    whether a new link was minted."""
    exists = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='produced'",
        run_id, output_id)
    if exists:
        return False
    await actions.create_link(run_id, output_id, "produced", source, datetime.now(UTC),
                              _CONF, evidence_class=_EC)
    return True


async def mint_derived_from(
    actions: Actions, artifact_id: uuid.UUID, source_id: uuid.UUID, source: str = _SOURCE,
) -> bool:
    """An Artifact's own source material, the traceability invariant's source leg.
    Artifact-to-source only (never a run's plan/objective, that's `mint_authorized_by`
    below). Idempotent: returns whether a new link was minted."""
    exists = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='derived_from'",
        artifact_id, source_id)
    if exists:
        return False
    await actions.create_link(artifact_id, source_id, "derived_from", source,
                              datetime.now(UTC), _CONF, evidence_class=_EC)
    return True


async def mint_authorized_by(
    actions: Actions, run_id: uuid.UUID, plan_id: uuid.UUID, source: str = _SOURCE,
) -> bool:
    """The run's own authorizing Decision or Thread, the traceability invariant's
    plan/objective leg (a deliberate split over overloading derived_from). Idempotent:
    returns whether a new link was minted."""
    exists = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='authorized_by'",
        run_id, plan_id)
    if exists:
        return False
    await actions.create_link(run_id, plan_id, "authorized_by", source, datetime.now(UTC),
                              _CONF, evidence_class=_EC)
    return True


async def mint_evaluated_by(
    actions: Actions, subject_id: uuid.UUID, evaluation_id: uuid.UUID, source: str = _SOURCE,
) -> bool:
    """An Agent generation/Artifact pointing at the Evaluation that judged it, the
    traceability invariant's evaluator leg. Idempotent: returns whether a new link was
    minted."""
    exists = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='evaluated_by'",
        subject_id, evaluation_id)
    if exists:
        return False
    await actions.create_link(subject_id, evaluation_id, "evaluated_by", source,
                              datetime.now(UTC), _CONF, evidence_class=_EC)
    return True


async def mint_revises(
    actions: Actions, new_artifact_id: uuid.UUID, old_artifact_id: uuid.UUID,
    source: str = _SOURCE,
) -> bool:
    """A later Artifact version supersedes an earlier one, the version DAG, same
    self-referential shape as Commit's own `follows` edge. Idempotent: returns whether
    a new link was minted."""
    exists = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='revises'",
        new_artifact_id, old_artifact_id)
    if exists:
        return False
    await actions.create_link(new_artifact_id, old_artifact_id, "revises", source,
                              datetime.now(UTC), _CONF, evidence_class=_EC)
    return True


async def record_evaluation(
    actions: Actions, rubric: str, *, verdict: str | None = None,
    subject: uuid.UUID | None = None, value: float | int | str | None = None,
    unit: str | None = None, measured_at: datetime | None = None,
    source: str = _SOURCE,
) -> uuid.UUID:
    """Capture a verdict against an Agent generation or Artifact: a test suite result,
    a code review finding, a gate_hook pass/fail, none of which were minted as objects
    before this Graph-Engineering arc (they lived only as commit-message prose).
    Distinct from Practice's own `witnesses` links (record_decision(confirms=...)/
    record_practice(witnesses=...)): that mechanism answers 'is this rule still true',
    this answers 'did this run/artifact pass'.

    `rubric` (which standard/check was applied) is mandatory and non-blank, refused
    at the door (ValueError, never a silent default): an Evaluation with no named
    rubric is unverifiable prose wearing a graph object's shape. This is a plain
    property validation, not the declare-or-refuse link-kind gate `_enforce_required_
    links` implements elsewhere (that machinery is a separate, later commit, item 2 of
    this same arc).

    `value`/`unit`/`measured_at` are Metric's own shape, stored as properties on this
    same object (a deliberate choice over an earlier draft design's separate Metric
    node); there is no Metric ObjectType.

    `subject`, when given, mints the `evaluated_by` edge from the Agent generation/
    Artifact being judged to this Evaluation (the traceability invariant's own evaluator
    leg) in the same transaction. Omitted, the Evaluation still mints; a caller who
    evaluates before the subject exists can link it later via `mint_evaluated_by`.

    EACH CALL MINTS A FRESH OBJECT, DELIBERATELY NOT IDEMPOTENT ON `rubric` ALONE
    (unlike Practice's own statement-keyed dedup): the same rubric run twice against
    the same subject is two distinct verdicts (a re-run after a fix), not one fact
    re-asserted; the canonical id embeds a fresh random component so two calls never
    collide."""
    if not rubric or not rubric.strip():
        raise ValueError(
            "Evaluation refused: rubric is mandatory (Graph-Engineering arc, thread "
            "7f547426) — name the standard/check being applied, never a blank verdict.")
    observed = measured_at or datetime.now(UTC)
    canon = _canon("evaluation", f"{subject}:{rubric}:{uuid.uuid4().hex}")
    async with actions.atomic() as a:
        e = await a.create_or_find_object("Evaluation", canon, source)
        await a.assert_property(e, "rubric", rubric.strip(), source, observed, _CONF,
                                evidence_class=_EC)
        if verdict:
            await a.assert_property(e, "verdict", verdict, source, observed, _CONF,
                                    evidence_class=_EC)
        if value is not None:
            await a.assert_property(e, "value", value, source, observed, _CONF,
                                    evidence_class=_EC)
        if unit:
            await a.assert_property(e, "unit", unit, source, observed, _CONF,
                                    evidence_class=_EC)
        if measured_at:
            await a.assert_property(e, "measured_at", measured_at.isoformat(), source,
                                    observed, _CONF, evidence_class=_EC)
        if subject is not None:
            await a.create_link(subject, e, "evaluated_by", source, observed, _CONF,
                                evidence_class=_EC)
    return e


async def record_artifact(
    actions: Actions, key: str, *, authoring_run: str | None = None,
    source: str = _SOURCE, unlinked_because: str | None = None,
) -> uuid.UUID:
    """Mint an Artifact: a build/deploy/document output Commit does not already cover
    (Graph-Engineering arc, item 2/3). Refuses (or confesses) at the door unless it
    carries its authoring Agent generation's own `produced` edge
    (artifact-has-authoring-run-plus-version) via `_enforce_required_links`' new
    incoming direction (`"authoring_run"` above), the same declare-or-refuse
    discipline record_decision/open_thread/ingest_reference/record_practice already
    use, extended for the first time to a kind this door can only ever see as a link
    pointing at it, never one it asserts itself.

    `authoring_run`, when given, is a reference (UUID/short-id/canonical) to an
    existing Agent generation, resolved via `_resolve_ref`, `require_identifier=True`
    (an addressing path, never a fuzzy text search), refusing if it doesn't resolve.
    The session object is the Agent generation; there is no longer a separate AgentRun
    pointer to lazily mint, so the run this Artifact is attributed to must already be
    a real, existing generation, not a string this door would otherwise have to take
    on faith. Resolved before the atomic block (a plain read against `actions.pool`,
    never `a.pool` inside `atomic()`, the same connection-exhaustion risk
    `_enforce_required_links`' own comment already documents for a nested pool
    acquisition). Omitted, the gate falls straight to its `unlinked_because` hatch or
    refuses, the same two-branch shape every other door already has.

    "PLUS-VERSION": a first version legitimately has no predecessor; `revises` is
    never itself gated here, only ever optional (an outgoing revises edge to its
    predecessor, or none if it's the first version). Version-ness is expressed by
    absence, not a second positive requirement; a caller who does have a predecessor
    links it separately via `mint_revises` after this call returns (the predecessor's
    own id is not knowable to this door in general, it mints after the predecessor,
    not necessarily in the same breath).

    Artifact's own `required_link_kinds` is unarmed by default (the same "dark until a
    real caller arms it" convention Practice's own gate already follows): this door's
    gate machinery is real and load-bearing the moment the type catalog's
    `required_link_kinds` for Artifact includes `"authoring_run"`, but until then
    every Artifact mints freely, same as before this arc existed."""
    observed = datetime.now(UTC)
    canon = f"artifact:{key}"
    run_id: uuid.UUID | None = None
    if authoring_run:
        run_id = await _resolve_ref(actions.pool, "Agent", authoring_run,
                                    text_field="name", require_identifier=True)
        if run_id is None:
            raise ValueError(f"record_artifact refused: authoring_run={authoring_run!r} "
                              "does not resolve to a real, existing Agent generation")
    async with actions.atomic() as a:
        art = await a.create_or_find_object("Artifact", canon, source)
        if run_id is not None:
            await a.create_link(run_id, art, "produced", source, observed, _CONF,
                                evidence_class=_EC)
        await _enforce_required_links(
            a, art, "Artifact", kinds_in_scope=("authoring_run",),
            unlinked_because=unlinked_because, source=source, observed=observed)
    return art


# THE CITATION SHAPE: the graph never mints a turn for having happened, everything
# said stays in the soul store (alembic/0050_soul_store.py), verbatim and
# hash-chained; a citation is an explicit `cites` edge from a Decision/Thread/
# Evaluation to an Agent generation, carrying line_idx/line_hash/said_at as edge
# properties, never auto-minted from prose (that stays `mint_cites`'s own separate,
# untouched lane, which never targets an Agent, verified live: every existing caller
# of `mint_cites`/`_mint_prose_citations` only ever resolves References/Decisions/
# Threads by id, never an Agent canonical scheme).
async def _verify_transcript_line(
    pool: asyncpg.Pool, harness: str, anchor_sid: str, line_idx: int,
) -> dict[str, Any]:
    """Re-derive one `soul_lines` row's own chain link locally: two rows, O(1), never
    a full walk from genesis (that's `SoulStore.rematerialize`'s own job, a different
    question). Verifies (a) this row's own `line_hash` really is
    sha256(prev_hash_bytes + raw_line_bytes) (`soul_store._chain_hash`, reused not
    reinvented, one derivation of the hash, not two that could quietly drift), and
    (b) its `prev_hash` matches the immediately preceding line's own `line_hash`, so a
    gap or a substituted predecessor breaks the chain locally even without re-walking
    to line 0. Returns `{"verified": bool, "raw_line": str, "line_hash": str,
    "said_at": datetime, "reason": str | None}`. `said_at` is the store's own
    `ingested_at` (this stage stores raw bytes only, no semantic parse of an embedded
    per-line timestamp; this is the store's own observation time, not a claim about
    when the words were first typed)."""
    from src.ingest.soul_store import _chain_hash
    row = await pool.fetchrow(
        "SELECT raw_line, line_hash, prev_hash, ingested_at FROM soul_lines "
        "WHERE harness=$1 AND anchor_sid=$2 AND line_idx=$3", harness, anchor_sid,
        line_idx)
    if row is None:
        return {"verified": False, "reason": f"no soul_lines row at harness={harness!r}, "
                f"anchor_sid={anchor_sid!r}, line_idx={line_idx}"}
    raw_line = bytes(row["raw_line"])
    if line_idx > 0:
        prior_hash = await pool.fetchval(
            "SELECT line_hash FROM soul_lines WHERE harness=$1 AND anchor_sid=$2 "
            "AND line_idx=$3", harness, anchor_sid, line_idx - 1)
        if prior_hash is None:
            return {"verified": False, "reason": f"chain broken — no row at line_idx "
                    f"{line_idx - 1}, this line's own prev_hash cannot be checked"}
        if row["prev_hash"] != prior_hash:
            return {"verified": False, "reason": "chain broken — prev_hash does not "
                    "match the preceding line's own hash (tampered or a gap)"}
    elif row["prev_hash"] is not None:
        return {"verified": False, "reason": "chain broken — line 0 must carry a null "
                "prev_hash"}
    if _chain_hash(row["prev_hash"], raw_line) != row["line_hash"]:
        return {"verified": False, "reason": "chain broken — stored line_hash does not "
                "match this line's own content (tampered or corrupted)"}
    return {"verified": True, "raw_line": raw_line.decode("utf-8", errors="replace"),
            "line_hash": row["line_hash"], "said_at": row["ingested_at"], "reason": None}


async def mint_transcript_citation(
    actions: Actions, from_id: uuid.UUID, agent_ref: str, line_idx: int, because: str,
    source: str = _SOURCE, *, harness: str = "claude-code",
) -> dict[str, Any]:
    """An explicit citation of one line of an Agent generation's own transcript: a
    `cites` edge from `from_id` (a Decision, Thread, or Evaluation) to the Agent
    `agent_ref` resolves to, carrying `line_idx`/`line_hash`/`said_at` as edge
    properties, a chain of custody verifiable against the soul store's own hash chain.

    NO AUTO-CITE, EVER: `because` is mandatory and non-blank, since a citation is a
    deliberate act with a stated reason, never inferred from prose. Distinct from
    `mint_cites`'s own prose-derived lane, never reused for this purpose and never
    widened to reach it.

    NEVER TARGETS A HUMAN NODE: `agent_ref` must resolve (via `_resolve_ref`,
    `require_identifier=True`, an addressing path, never a fuzzy text search) to a
    real, active Agent object. The literal 'operator' string, a Seat, a Person, or
    anything else refuses exactly like an unresolved ref; this function only ever
    knows how to check "is this a real Agent", which is sufficient: nothing that
    represents a human is ever typed Agent in this graph."""
    if not because or not because.strip():
        raise ValueError("mint_transcript_citation refused: `because` is mandatory and "
                          "non-blank — a citation is a deliberate act with a reason, "
                          "never inferred from prose")
    agent_id = await _resolve_ref(actions.pool, "Agent", agent_ref, text_field="name",
                                  require_identifier=True)
    if agent_id is None:
        raise ValueError(f"mint_transcript_citation refused: {agent_ref!r} does not "
                          "resolve to a real Agent generation — a citation never "
                          "targets a human/operator node or anything else")
    session = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a "
        "WHERE a.object_id=$1 AND a.name='session'", agent_id)
    if not session:
        raise ValueError(f"mint_transcript_citation refused: Agent {agent_ref!r} "
                          "carries no `session` property — nothing to cite")
    verification = await _verify_transcript_line(actions.pool, harness, str(session),
                                                 line_idx)
    if not verification["verified"]:
        raise ValueError(f"mint_transcript_citation refused: {verification['reason']}")
    observed = datetime.now(UTC)
    await actions.create_link(
        from_id, agent_id, "cites", source, observed, _CONF, evidence_class=_EC,
        properties={"line_idx": line_idx, "line_hash": verification["line_hash"],
                    "said_at": verification["said_at"].isoformat(), "because": because,
                    "origin": "declared"})
    return {"agent_id": str(agent_id), "line_idx": line_idx,
            "line_hash": verification["line_hash"]}


async def read_transcript_citation(
    pool: asyncpg.Pool, from_id: uuid.UUID, agent_ref: str, *,
    harness: str = "claude-code",
) -> dict[str, Any]:
    """The verified read-back for an existing transcript citation: finds the live
    `cites` edge from `from_id` to the Agent `agent_ref` resolves to, re-verifies its
    stored `line_hash` against the soul store's own chain (never trusting the edge
    property alone, since a tampered edge property would otherwise silently "verify"
    against itself), and returns the actual cited line. Refuses loudly on a missing
    edge, an unresolved `agent_ref`, or a hash mismatch, never a stale/wrong line
    returned silently."""
    agent_id = await _resolve_ref(pool, "Agent", agent_ref, text_field="name",
                                  require_identifier=True)
    if agent_id is None:
        raise ValueError(f"read_transcript_citation refused: {agent_ref!r} does not "
                          "resolve to a real Agent generation")
    edge = await pool.fetchrow(
        "SELECT properties FROM links WHERE from_id=$1 AND to_id=$2 AND type='cites' "
        "AND (valid_until IS NULL OR valid_until > now()) LIMIT 1", from_id, agent_id)
    if edge is None:
        raise ValueError(f"read_transcript_citation refused: no live citation from "
                          f"{from_id} to {agent_ref!r}")
    from src.orchestrator.dossier import _jsonb
    props = _jsonb(edge["properties"])
    line_idx = props.get("line_idx")
    if line_idx is None:
        raise ValueError("read_transcript_citation refused: the citation edge carries "
                          "no line_idx — not a transcript citation")
    session = await pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a "
        "WHERE a.object_id=$1 AND a.name='session'", agent_id)
    if not session:
        raise ValueError(f"read_transcript_citation refused: Agent {agent_ref!r} "
                          "carries no `session` property")
    verification = await _verify_transcript_line(pool, harness, str(session), line_idx)
    if not verification["verified"]:
        raise ValueError(f"read_transcript_citation refused: {verification['reason']} "
                          "— the cited line no longer verifies against the soul "
                          "store's own chain")
    if verification["line_hash"] != props.get("line_hash"):
        raise ValueError("read_transcript_citation refused: the citation's own "
                          "recorded line_hash does not match the store's current "
                          "line_hash — tampering or a stale citation")
    return {"line_idx": line_idx, "raw_line": verification["raw_line"],
            "line_hash": verification["line_hash"], "said_at": props.get("said_at")}


async def mint_implements(
    actions: Actions, from_decision: uuid.UUID, to_decision: uuid.UUID, source: str = _SOURCE,
) -> bool:
    """This Decision is a specific execution of that standing ruling (prior_art_flag's
    third path), the parent stays alive, unlike supersedes. Idempotent: returns whether
    a new link was minted."""
    exists = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='implements'",
        from_decision, to_decision)
    if exists:
        return False
    await actions.create_link(from_decision, to_decision, "implements", source,
                              datetime.now(UTC), _CONF, evidence_class=_EC)
    return True


async def mint_rediscovers(
    actions: Actions, from_decision: uuid.UUID, to_decision: uuid.UUID, source: str = _SOURCE,
) -> bool:
    """This (later) Decision independently arrived at a finding an earlier one already
    recorded (two prior decisions were each rediscovered a week after they were first
    named, and nothing in the graph could say so). Points from the later finding to the
    earlier one it re-derives.

    BURIES NEITHER SIDE: the earlier decision keeps its own standing untouched (no
    superseded_by, no graying in orient's recent list), unlike `supersedes`; and unlike
    `implements`, the later decision is not a specific execution of the earlier one's
    plan, it is an independent arrival at the same conclusion. Distinct from a
    near-duplicate reword (`find_near_duplicate_decision` already merges those at write
    time, silently, into one object): a rediscovery's wording differs, that is exactly
    why the prior-art guard's lexical/semantic match can miss it, while the finding is
    the same.

    Idempotent: returns whether a new link was minted.

    WHAT THIS DOES NOT DO: it records a rediscovery after the fact; it does not prevent
    one. Catching a rediscovery before it is written down is a retrieval-quality question,
    deliberately left separate and unbuilt: the same prior-art search that should have
    surfaced the earlier finding for the later one returned five hits and missed it."""
    exists = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='rediscovers'",
        from_decision, to_decision)
    if exists:
        return False
    await actions.create_link(from_decision, to_decision, "rediscovers", source,
                              datetime.now(UTC), _CONF, evidence_class=_EC)
    return True


async def mint_narrows(
    actions: Actions, from_decision: uuid.UUID, to_decision: uuid.UUID, source: str = _SOURCE,
) -> bool:
    """This (later) Decision bounds the scope of an earlier one without refuting or
    superseding it: the target's own measurement stays correct within its now-visible
    limit. Points from the bounding decision to the one it bounds, same direction as
    `rediscovers`.

    NON-BURYING BY CONSTRUCTION, the entire design constraint (the exact opposite of
    `supersedes`): this function writes only the link. No `superseded_by`/`superseded_
    because` property, no status touch, on either side; nothing here can gray the
    target out of orient's recent list or the decision-log's live section, structurally,
    the same guarantee `mint_rediscovers` already proves for its own edge. A `narrows`
    implementation that could bury its target would be `supersedes` wearing a new name.

    `recall()` surfaces inbound `narrows` edges on the bounded decision as `narrowed_by`
    (same shape Thread's `bears_on_from` already proves); the edge existing in the
    links table is not the deliverable by itself, a reader who finds the bounded ruling
    must find the bound too, or nothing was fixed. Idempotent: returns whether a new
    link was minted."""
    exists = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='narrows'",
        from_decision, to_decision)
    if exists:
        return False
    await actions.create_link(from_decision, to_decision, "narrows", source,
                              datetime.now(UTC), _CONF, evidence_class=_EC)
    return True


async def mint_bears_on(
    actions: Actions, decision_id: uuid.UUID, thread_id: uuid.UUID, source: str = _SOURCE,
) -> bool:
    """Route a fresh Decision back to the stale board row it speaks to, without closing
    it. Mints the identical `answers` edge record_decision(resolves=...) mints, same link
    type, same dedup-checked existence check, through a route that is by construction
    incapable of acting on the row: this function touches only the `links` table, never
    `status` or any other property, and is called from the MCP wrapper only, never
    threaded through record_decision's own atomic transaction the way
    `resolves`/`supersedes` are. That separation is deliberate, not an oversight, guided
    by a standing no-auto-act rule: every specimen worth having was found by a human
    reading and judging, and a verb that acts on a row is how the correct ones get lost
    along with the wrong ones. `resolves=` stays the close-and-cite verb for a ruling
    that settles its question; this is the cite-only verb for a finding that merely
    speaks to one, a stale row's text going wrong, an already-answered measurement
    nobody routed back, anything short of "and therefore this row is done."

    Idempotent: returns whether a new link was minted. THE RECEIPT-HONESTY LAW: an
    already-linked pair must never render as a bare, indistinguishable success. The MCP
    wrapper reports this boolean per thread, same shape rediscovers/confirms already use
    (`new_link`), so a caller can tell "your citation landed" from "already linked,
    nothing needed" in the same turn."""
    exists = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='answers'",
        decision_id, thread_id)
    if exists:
        return False
    await actions.create_link(decision_id, thread_id, "answers", source,
                              datetime.now(UTC), _CONF, evidence_class=_EC)
    return True


async def thread_answering_decisions(
    pool: asyncpg.Pool, thread_ids: list[uuid.UUID],
) -> dict[uuid.UUID, list[dict[str, str]]]:
    """Every Decision genuinely answering each of `thread_ids`, the shared read-back,
    extracted here so `recall()`'s own single-object `bears_on_from` and
    `obligation_hygiene`'s own stale-row nudge read the identical query instead of two
    copies free to drift. Batched (one query, not N) since the hygiene sweep's own
    caller runs over every open obligation Thread at once.

    TWO EDGE TYPES, BOTH A REAL RESOLVE, NEITHER TEXT-DERIVED: `answers`
    (`mint_bears_on`'s edge, and `record_decision(resolves=)`'s own same-transaction
    mint, Decision -> Thread) unioned with `resolved_by` edges whose target is a
    Decision (`resolve_thread(artifact=<decision>)`, Thread -> Decision, the other door
    that closes a thread with a decision pointer, which used to be invisible here).
    Read-side widening, never a second minting path: `resolved_by` already existed for
    every such closure, live and historical alike, so widening the read closes the gap
    with no backfill needed and no risk to `thread_closure_status`'s own separate
    mutual-exclusivity assumption about the two edge types, that view stays untouched.

    Live edges only (valid_until IS NULL): an unmerge/retraction/heal must not go on
    citing a row. A thread with none is simply absent from the returned dict, never a
    present-but-empty list, so `dict.get(tid, [])` reads correctly either way. A pair
    carrying both edge types (rare: a decision resolved it and was separately cited as
    the closing artifact) counts once, never twice."""
    if not thread_ids:
        return {}
    rows = await pool.fetch(
        "SELECT DISTINCT thread_id, id, summary, created_at FROM ("
        "  SELECT l.to_id AS thread_id, d.id, d.created_at, "
        "   (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=d.id "
        "    AND a.name='summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "    AS summary "
        "  FROM links l JOIN objects d ON d.id=l.from_id AND d.type='Decision' "
        "  WHERE l.to_id = ANY($1) AND l.type='answers' AND l.valid_until IS NULL "
        "  UNION ALL "
        "  SELECT l.from_id AS thread_id, d.id, d.created_at, "
        "   (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=d.id "
        "    AND a.name='summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "    AS summary "
        "  FROM links l JOIN objects d ON d.id=l.to_id AND d.type='Decision' "
        "  WHERE l.from_id = ANY($1) AND l.type='resolved_by' AND l.valid_until IS NULL "
        ") both_doors ORDER BY thread_id, created_at", thread_ids)
    out: dict[uuid.UUID, list[dict[str, str]]] = {}
    for r in rows:
        out.setdefault(r["thread_id"], []).append(
            {"id": str(r["id"])[:8], "summary": (r["summary"] or "")[:160]})
    return out


async def acknowledge_prior_art(
    actions: Actions, decision_id: uuid.UUID, prior_art_id: str, source: str = _SOURCE,
) -> None:
    """'Related standing law, reviewed, no action needed' as a graph event, not a shrug
    swallowed in prose: the third path prior_art_flag's two-verb (supersede-or-cite)
    prompt was missing.

    PROMOTED FROM A STRING TO A REAL EDGE: the system computed relatedness, surfaced it,
    the author confirmed it, and that confirmation is now `mint_cites`'s own edge (never
    self_referential; acknowledging your own prior art isn't a citation of someone
    else's work), same SELF_DECLARED grade as prose-id citations, same one mechanism.
    `prior_art_acknowledged` stays written too; an existing reader of that property
    keeps working unchanged, the edge is additive."""
    observed = datetime.now(UTC)
    await actions.assert_property(decision_id, "prior_art_acknowledged", prior_art_id,
                                  source, observed, _CONF, evidence_class=_EC)
    try:
        target_id = uuid.UUID(prior_art_id)
    except ValueError:
        return  # not UUID-shaped: the property write above still landed, nothing to link
    await mint_cites(actions, decision_id, target_id, source, origin="declared",
                     self_referential=False)


async def refute_practice(
    actions: Actions, practice_ref: str, *, killed_by: str, repo: str | None = None,
    source: str = _SOURCE,
) -> dict[str, uuid.UUID] | None:
    """THE POLARITY FLIP: a Practice refuted converts to a Superstition, same family,
    same kill-verb (`kill_superstition`), reusing the Practice's own statement so the
    dead workaround is searchable under the exact words it propagated as. The Practice
    itself is never retired: it stays active carrying `refuted_by`, because a
    half-remembered refuted lesson is exactly the thing that must stay findable,
    surfaced with the flag, not erased. Returns None (no write) when `practice_ref`
    matches no Practice, same all-or-nothing strictness as `supersedes`/`resolves`: a
    refutation that can't name its target has not refuted anything.

    THE CORRECTIVE ANALOG: `refuted_by` used to be a plain property, same gap
    `kill_superstition` had, so a Decision's own record had nothing to show for what it
    refuted. When `killed_by` resolves to a graph object (`_find_artifact`, same
    resolver Thread's resolved_by uses) a `refuted_by` link is minted on the Practice
    too, idempotent per (practice, target); the chained `kill_superstition` call mints
    its own `killed_by` link on the fresh Superstition in the same pass. The property
    always carries the raw pointer regardless of resolution."""
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
    target = await _find_artifact(pool, killed_by)
    if target is not None and not await pool.fetchval(
            "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='refuted_by' LIMIT 1",
            pid, target):
        await actions.create_link(pid, target, "refuted_by", source, observed, _CONF,
                                  evidence_class=_EC)
    sid = await kill_superstition(actions, statement or practice_ref, killed_by=killed_by,
                                  repo=repo, source=source)
    return {"practice": pid, "superstition": sid}


async def amend_practice(
    actions: Actions, ref: str, amendment: str, *, source: str = _SOURCE,
) -> uuid.UUID | None:
    """Narrow or correct a live practice's guidance as understanding develops, without
    changing its id, its `statement`, or its witness/confirmed count. This is the same
    shape as `amend_decision` for a Decision, following the same rule: follow the
    existing amendment mechanism rather than re-deciding it. `statement` is
    record_practice's own idempotency key (its normalized text is what "the same lesson"
    means to every future caller); mutating it here would silently redefine that key out
    from under anyone who re-encounters the original wording and expects record_practice
    to find, not twin, it, the exact risk amend_decision's own design already avoids for
    `summary`. So this can only add a new, independently-current property, never touch
    `statement`/`witnesses`/anything already on the object, same mechanism as
    `annotate_thread`/`amend_decision` (`_append_property_name`).

    A Practice's own live read surface is `practices()`, every caller actually uses it,
    so this verb's amendments are wired into that composition directly
    (`_fn_practices`), the same reasoning `recall()` now applies to a Decision's own
    addenda (`recall()` no longer leaves them write-only, see recall.py's own
    docstring). That is the whole point of narrowing a practice's text in place: a
    reader who calls `practices()` must see it, not go hunting through `lap()`'s raw
    provenance timeline for an `amendment:` assertion.

    Refuses (raises ValueError, naming `refute_practice` by name) when `ref` names a
    Practice already refuted (carries `refuted_by`): a dead lesson does not grow new
    guidance; a practice that needs killing is `refute_practice`'s job, not this one's.

    Returns the practice id, or None if `ref` matched nothing (same convention as
    `amend_decision`/`resolve_thread`). Raises ValueError on a blank amendment."""
    amendment = amendment.strip()
    if not amendment:
        raise ValueError("amendment must not be blank — an empty addition is not testimony")
    pid = await _find_practice(actions.pool, ref)
    if pid is None:
        return None
    refuted_by = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='refuted_by' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1",
        pid,
    )
    if refuted_by:
        raise ValueError(
            f"practice {ref!r} is already refuted (killed_by {str(refuted_by)[:8]}) — a "
            "dead lesson does not grow new guidance; amend_practice only ever adds to a "
            "practice still standing")
    observed = datetime.now(UTC)
    await actions.assert_property(pid, _append_property_name("amendment"), amendment, source,
                                  observed, _CONF, evidence_class=_EC)
    return pid


async def practice_amendments(
    pool: asyncpg.Pool, practice_id: uuid.UUID,
) -> list[dict[str, Any]]:
    """Every amendment `amend_practice` has added to this practice, oldest first, see
    `thread_notes`/`decision_addenda` for why each one is independently current.
    `practices()` reads these too (folded into its own composition); this is the
    standalone form for direct lookup/testing."""
    rows = await pool.fetch(
        "SELECT a.value #>> '{}' AS amendment, a.source_id AS source, a.observed_at, "
        "a.confidence FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name LIKE 'amendment:%' ORDER BY a.observed_at ASC",
        practice_id,
    )
    return [{"amendment": r["amendment"], "source": r["source"], "observed_at": r["observed_at"],
             "confidence": float(r["confidence"])} for r in rows]


async def recent_dead_superstitions(
    pool: asyncpg.Pool, *, days: int = 14, limit: int = 5,
) -> list[dict[str, str]]:
    """The kills worth announcing: superstitions put down within the window, newest
    first. Fleet-wide by design: a workaround replicates across teams (one lesson was
    spreading to a second team before the fix even shipped), so the announcement must
    not stop at a project boundary. Bounded and aging-out: orient speaks the recent
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


# the words identified for this pattern plus the N/N shape; deliberately narrow:
# a nag that fires on every ruling teaches everyone to ignore it
_MEASUREMENT = re.compile(
    r"(?i)\b(verified|verification|probe[ds]?|sweep(s|ed)?|swept|benchmark\w*|"
    r"sampl(e[ds]?|ing)|threshold\w*|seed(ed|s)?)\b|\b\d+\s*/\s*\d+\b")


def measurement_smell(text: str) -> bool:
    """Does a decision's text read like a measurement? `protocol` is record_decision's
    best field and nothing asks for it: a verification recipe recorded without its
    invocation is exactly the re-derivation class the field exists to kill. Used by the
    record_decision tool to nag for an empty protocol, advice in the response, never a
    gate: the decision records either way."""
    return bool(_MEASUREMENT.search(text))


# THE DISPATCH VERSION: measurement_smell's own vocabulary does not transfer to send()'s
# traffic, checked empirically against three real specimens used as the acceptance test:
# none of the three flat, wrong premises trip a single word in _MEASUREMENT. Dispatch
# prose fails a different way: not "claims a measurement with no protocol", but "asserts
# a mechanism/behavior fact with no hedge acknowledging it might be wrong". A citation
# alone does not save it: one specimen named a test file by exact line and was still
# wrong, because the citation was never re-read for what it actually proved. Deliberately
# narrow, same law _MEASUREMENT's own comment states: a nag that fires on every dispatch
# teaches everyone to ignore it, verified against seven plausible false-positive shapes
# (a genuine hedge, three plain status reports, an authorization citation) before this
# shipped, none fire.
_UNHEDGED_ASSERTION = re.compile(
    r"(?i)\b(excludes|includes|by design|as-is|is what|is the same|"
    r"would (not )?have|reads (live|before|after)|runs (before|after)|"
    r"the check (only|never)|is (not )?the (cause|blocker|gap|hole)|"
    r"never (reads|checks|touches)|only (reads|checks|ever)|untested|"
    r"no (regression )?test)\b")
# FRESH-VERIFICATION IS A HEDGE TOO: an early calibration pass found three of four test
# specimens were self-corrections ("I grepped and found four live callers, not zero")
# that still fired, because the original vocabulary only recognized hedging by
# uncertainty (i think/might/haven't checked), never hedging by fresh re-check (i
# grepped/i checked and found X). The nag's own design note already says "if you
# re-read the thing you're describing this turn, say so", and a caller who names the
# exact re-check they just performed is doing that; the vocabulary just didn't
# recognize the shape. Checked empirically against all four live specimens before
# shipping: the confirmed true positive (praising a find, no re-check language of its
# own) still fires; all three self-correction messages (each containing "I grepped")
# stop firing.
_HEDGE = re.compile(
    r"(?i)\b(i think|i believe|might|may not|maybe|probably|possibly|"
    r"not (fully |100% )?(sure|certain|confirmed|verified)|"
    r"haven'?t (checked|verified|confirmed|read|re-?read)|"
    r"worth (checking|verifying|a (second|closer) look)|"
    r"reproduce (it )?yourself|double-?check|unverified|assuming|"
    r"as far as i (know|can tell)|uncertain|caveat|"
    r"i (just )?(went and )?(grepped|checked|re-?ran|re-?checked))\b")


def unhedged_assertion_smell(text: str) -> bool:
    """Does dispatch text read like a flat, uncited-in-spirit claim about code/system
    behavior with no hedge acknowledging it might be wrong? measurement_smell's own
    sibling, built for send() rather than record_decision, same shape (a lexical
    detector, never a self-report: the sender's own confidence is exactly what produced
    all three specimens this was built against), different vocabulary, because the
    failure it targets is different. Used by send()'s wrapper to nag, never gate, exactly
    as measurement_smell already does; the message sends either way."""
    return bool(_UNHEDGED_ASSERTION.search(text)) and not _HEDGE.search(text)


async def divergent_leans(pool: asyncpg.Pool) -> dict[str, str]:
    """Tensions where two agents' current leans disagree, keyed by the tension's
    canonical, valued with the line the lens must speak. This traces to an earlier audit
    that found the assertion set honestly keeps both leans, but any single-winner reader
    silently shows one, so orient must say 'two minds lean apart' instead. Per-source
    current lean = that source's latest; divergence = more than one distinct value among
    them. Report-only: nothing here resolves anything (a Tension is held by design)."""
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
    """HALT A PROGRAM: the operator stops a project by name, and the graph should hear it.

    A halted project's threads are real yield on a paused effort: not garbage (so the
    janitor must never sweep them, the miner did its job, and the work was genuine), and
    not debt (so no lens may count them). 333 of them, 257 in one project and 78 in
    another, were inflating every number in the system after the operator had explicitly
    stopped both programs. A memory that cannot hear "we stopped doing that" will keep
    billing you for it forever.

    This is testimony, not a guess: the human said it, an agent records it, and the lens
    obeys. Reversible by construction: set it back to 'active' and every thread returns
    exactly as it was. Nothing is deleted, nothing is swept, nothing is lost. `lifecycle`:
    active | halted.
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


# LANE 2, MAKE INCREMENTAL CAPTURE POSSIBLE: neither `resolve_thread` (closes on call
# regardless of intent) nor `record_decision` (write-once-plus-supersede, mint fresh, or
# bury under a correction) lets a session add to a durable object without replacing or
# closing it. Both force batch-at-the-end capture, which is exactly what dies at a
# context-window seam. Two verbs below, one law: append, never overwrite.
#
# Every addition carries its own source/observed_at/grade, the same metadata every
# assertion in this module already carries, under a property name that can never collide
# with an earlier append (`_append_property_name`, just below). That matters because the
# ordinary mechanism every other property write here relies on, `assert_property`'s
# within-source supersession, would otherwise silently bury an earlier note/addendum from
# `current_assertions` the instant two calls happened to share a name, precisely the loss
# this lane exists to close. So unlike every other mint in this file (`_canon`, hashed on
# content for idempotency, a repeat is the same fact, fold it), an append is keyed on
# nothing but its own identity: a repeat is new testimony, never a duplicate to fold away.
#
# AND NEITHER IS A SECOND SUPERSEDE: amendment is not correction. `record_decision(
# supersedes=...)` already exists for "the earlier reasoning was wrong" and stays the
# only door for that; `amend_decision` structurally cannot touch
# `summary`/`rationale`/anything already on the object, it can only add, and refuses
# outright, naming supersede by name, the moment its target is no longer live.
def _append_property_name(prefix: str) -> str:
    """A property name that can never collide with an earlier append under the same
    prefix, see the banner above for why a content hash (this module's usual idempotency
    key) would be the wrong choice here."""
    return f"{prefix}:{uuid.uuid4().hex[:12]}"


async def annotate_thread(
    actions: Actions, ref: str, note: str, *, corrected_summary: str | None = None,
    because: str | None = None, source: str = _SOURCE,
) -> uuid.UUID | None:
    """Add to a thread's record without closing it (`resolve_thread` closes;
    `assign_thread` hands off; `defer_thread` snoozes; this one just adds). `status` is
    never touched: an annotated thread stays exactly as open, or resolved, or deferred,
    as it was before the call. This is addition, not a state transition, and it fills a
    real gap: today `resolve_thread` is the only verb that writes to an existing thread,
    and it closes on call regardless of intent, so anything short of a full close gets
    forced into batch-at-the-end capture, exactly what a dying session drops.

    Carries the same source/observed_at/grade every assertion in this module already
    does, stamped under a property name that can never collide with an earlier append
    (`_append_property_name`), since a genuine within-source supersede here would
    silently bury an earlier note from `current_assertions`, the loss this verb exists
    to prevent. Read the whole record back, in the order it was understood, with
    `thread_notes`.

    `corrected_summary` (optional: "let annotate carry corrected_summary in the same
    call"): one call fixes the headline instead of requiring a caller to already know
    `correct_thread_summary` is a second, separate verb, closing an affordance gap where
    the obvious action pointed at the wrong tool by default. Writes through the same
    shared helper `correct_thread_summary` itself calls (`_write_corrected_summary`),
    never a second copy of that property-write. `because` rides beside it, same meaning
    as `correct_thread_summary`'s own `because`, ignored (never written) when
    `corrected_summary` is not given.

    This is not `resolve_thread`'s `because` used alone: without `corrected_summary`,
    annotate_thread still has no parameter that can change `summary`/`status`/any
    existing property, it can only add. A caller who means "the earlier understanding
    was wrong" and does not pass `corrected_summary` wants a different verb entirely;
    nothing here revises anything unless that parameter is given.

    Returns the thread id, or None if `ref` matched nothing (same convention as
    `resolve_thread`/`assign_thread`/`defer_thread`). Raises ValueError on a blank note,
    an empty addition is not testimony."""
    note = note.strip()
    if not note:
        raise ValueError("note must not be blank — an empty addition is not testimony")
    tid = await _find_thread(actions.pool, ref)
    if tid is None:
        return None
    observed = datetime.now(UTC)
    await actions.assert_property(tid, _append_property_name("note"), note, source, observed,
                                  _CONF, evidence_class=_EC)
    if corrected_summary is not None:
        await _write_corrected_summary(
            actions, tid, corrected_summary, because=because, source=source,
            observed=observed)
    # A TOUCH RENEWS THE WINDOW: the stop hook's stale-obligation gate names "annotate,
    # resolve, or reclassify" as the three touches that carry a stale duty, but only ever
    # read `stale_after`, so an honest dated note left the row exactly as stale, and the
    # block recurred every session until the owner reclassified 25 obligations wholesale
    # just to silence it. Annotating an obligation that carries a window now re-stamps
    # `stale_after` = now + the default window, so a touch is a touch on every surface.
    # Nothing else changes: status, kind, summary stay.
    has_window = await actions.pool.fetchval(
        "SELECT 1 FROM current_assertions WHERE object_id=$1 AND name='stale_after' "
        "AND is_current LIMIT 1", tid)
    if has_window:
        renewed = observed + timedelta(days=DEFAULT_STALE_AFTER_DAYS)
        await actions.assert_property(tid, "stale_after", renewed.isoformat(), source,
                                      observed, _CONF, evidence_class=_EC)
    return tid


async def thread_notes(pool: asyncpg.Pool, thread_id: uuid.UUID) -> list[dict[str, Any]]:
    """Every annotation `annotate_thread` has added to this thread, oldest first, the
    order it was understood in, which is often the finding itself. Reads
    `current_assertions` directly rather than going through a single-winner resolver
    like `_thread_summary`/`_current_owner`: each note's property name is unique
    (`_append_property_name`), so every one of them is independently "current", there
    is no winner to pick among them."""
    rows = await pool.fetch(
        "SELECT a.value #>> '{}' AS note, a.source_id AS source, a.observed_at, "
        "a.confidence FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name LIKE 'note:%' ORDER BY a.observed_at ASC",
        thread_id,
    )
    return [{"note": r["note"], "source": r["source"], "observed_at": r["observed_at"],
             "confidence": float(r["confidence"])} for r in rows]


async def open_or_annotate_persisting_alarm(
    actions: Actions, summary: str, *, kind: str, source: str,
    arc: str | None = None, severity: str | None = None, owner: str | None = None,
    unlinked_because: str | None = None,
) -> str:
    """The shared mint-or-annotate door for a periodic, source-not-a-human alarm/audit
    re-run on the same persisting condition, promoted here from deploy_guard.py once
    tree_ingest.py needed the identical shape a second time, never a second copy of the
    logic. The same guard `agents._report_half_healed_phantom` carries: every caller of
    this shape shares a stable summary text (deliberately keeping a volatile detail like
    `service`/age/watermark out of it, so `open_thread`'s own dedup converges), re-run on
    every boot/deploy/heartbeat tick. `open_thread` is idempotent on the summary hash, it
    finds the same Thread object regardless of current status and unconditionally
    re-asserts status='open', so a caller that skipped this guard and called
    `open_thread` directly on every tick would silently override a human's own resolve
    the next tick that still sees the identical condition. This was found live, in
    deploy_guard.py's own callers, after a widened status-regression check caught it on
    the half-heal detector.

    If the thread this summary would resolve to already reads status='resolved', this
    annotates it with the still-present sighting instead of calling `open_thread` at
    all: the condition being real stays on the record, but re-opening a thread a human
    already closed is not an automated sweep's call. A never-seen-before or still-open
    thread behaves exactly as a bare `open_thread` call always has."""
    canon = _thread_canon(summary, None)
    current_status = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM objects o JOIN current_assertions a "
        "ON a.object_id=o.id WHERE o.canonical=$1 AND o.type='Thread' AND a.name='status' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", canon)
    if current_status == "resolved":
        tid = await annotate_thread(
            actions, canon,
            f"still present at {datetime.now(UTC).isoformat()}: this alarm's own "
            "condition has not cleared. Resolved once already — re-opening it is a "
            "human's call, not this sweep's.",
            source=source)
        return str(tid) if tid is not None else canon
    return str(await open_thread(
        actions, summary, kind=kind, arc=arc, severity=severity, owner=owner,
        source=source, unlinked_because=unlinked_because))


# THE CONTESTED-SUMMARY LAW: a thread's own `summary` can be proven false by a later
# note, yet nothing anywhere marks the disagreement before a reader decides whether to
# open it. A false headline survived a week, through a stale-sweep nudge and a full
# obligation crunch, because two annotations discharged the hygiene duty without ever
# fixing it. A thread is contested when its newest `note:*` annotation post-dates the
# last summary touch (`corrected_summary` if one exists, else the original `summary`).
#
# ONE SHARED SQL FRAGMENT, never re-derived per surface (the exact failure being fixed,
# one level up: five near-identical COALESCE copies already exist for "which summary
# wins" alone, no_regrow.py, obligation_hygiene.py, compositions.py, digest.py,
# mcp_server.py's threads(), and a sixth hand-rolled boolean would be the same mistake
# at a higher stakes table). Every consumer (threads(render='text'), the backlog band,
# orient's wall, the stale nudge, no_regrow's own exclusion, the fleet audit) imports
# this rather than writing its own. Assumes the calling query aliases the Thread object
# as `o`, the same convention every COALESCE fragment above already assumes.
LAST_SUMMARY_TOUCH_SQL = (
    "COALESCE("
    "(SELECT a.observed_at FROM current_assertions a WHERE a.object_id=o.id "
    " AND a.name='corrected_summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1), "
    "(SELECT a.observed_at FROM current_assertions a WHERE a.object_id=o.id "
    " AND a.name='summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1))"
)
CONTESTED_SQL = (
    "EXISTS (SELECT 1 FROM current_assertions n WHERE n.object_id=o.id "
    "AND n.name LIKE 'note:%' AND n.observed_at > " + LAST_SUMMARY_TOUCH_SQL + ")"
)


async def correct_thread_summary(
    actions: Actions, ref: str, corrected_summary: str, *, because: str | None = None,
    source: str = _SOURCE,
) -> uuid.UUID | None:
    """The verb `annotate_thread` names and refuses to be (its own docstring: "a caller
    who means the earlier understanding was wrong wants a different verb entirely"),
    this is that verb.

    THE PROBLEM MEASURED, NOT ASSUMED: a Thread's own `summary` can never be re-asserted
    in place. `open_thread` mints on `_canon("thread", summary)`, so the original
    summary text is the object's own identity/dedup key; re-asserting it under a
    changed value would not correct the thread, it would silently stop finding it (a
    caller who now supplies the corrected text mints a twin instead of updating the
    original, the exact failure this verb exists to prevent). `annotate_thread`'s
    `_append_property_name` pattern (many independently-current notes, by design, see
    its own docstring) is the wrong shape here too: a correction is not one more
    coexisting note, it is the new headline, there should be exactly one live answer to
    "what does this thread's summary currently say," with the prior wording kept as
    queryable history, not a growing pile of undated candidates.

    THE FIX: `corrected_summary` is an ordinary property, not an appended one, the same
    supersession machinery every other property on this graph already uses
    (assert_property's own within-source supersede). Calling this again re-asserts it:
    the new text wins in `current_assertions`, the old one survives as non-current,
    queryable history exactly the way `summary`/`status`/everything else already works,
    no new mechanism, no twin, `summary` itself untouched (still the object's own
    identity, still what a caller matches against). `because` (optional) rides the same
    pattern as a second property, `corrected_because`, why the headline changed, not
    just that it did.

    ONE HOP, NOT SIX: `recall()`'s existing flat-dump already returns every current
    property by name with no special-casing, `corrected_summary` (and
    `corrected_because`) appear there for free, sitting right beside the untouched
    original `summary`, in the same call. No change to recall.py was needed or made:
    this is exactly why a plain property was the right shape and
    `_append_property_name` (recall.py's note/addendum branch, deliberately excluded
    from the flat dump) would have been the wrong one, a reader gets original and
    correction in one recall(ref), not a second lookup.

    Returns the thread id, or None if `ref` matched nothing (same convention as
    `resolve_thread`/`annotate_thread`). Raises ValueError on a blank corrected_summary."""
    tid = await _find_thread(actions.pool, ref)
    if tid is None:
        return None
    await _write_corrected_summary(
        actions, tid, corrected_summary, because=because, source=source,
        observed=datetime.now(UTC))
    return tid


async def _write_corrected_summary(
    actions: Actions, tid: uuid.UUID, corrected_summary: str, *, because: str | None,
    source: str, observed: datetime,
) -> None:
    """The property-write `correct_thread_summary` and `annotate_thread`'s own
    `corrected_summary=` param both share, one write, never a second copy drifting from
    the first: two callers writing "the same fix" slightly differently is how a
    headline survives a correction. Raises ValueError on a blank corrected_summary, an
    empty correction is not testimony."""
    corrected_summary = corrected_summary.strip()
    if not corrected_summary:
        raise ValueError(
            "corrected_summary must not be blank — an empty correction is not testimony")
    await actions.assert_property(tid, "corrected_summary", corrected_summary, source, observed,
                                  _CONF, evidence_class=_EC)
    if because:
        await actions.assert_property(tid, "corrected_because", because.strip(), source,
                                      observed, _CONF, evidence_class=_EC)


async def amend_decision(
    actions: Actions, ref: str, addendum: str, *, source: str = _SOURCE,
) -> uuid.UUID | None:
    """Append reasoning to a live decision as understanding develops, without
    superseding it. `record_decision` is write-once-plus-supersede, mint fresh, or bury
    under a correction, and there was no third option for "more of the same ruling's
    own reasoning, added later." `summary` is never touched here (it is the addressable
    handle callers dedup and short-id-match against, mutating it under a reader is how
    dedup problems start), and neither is `rationale`/`kind`/anything else already on
    the object; amend_decision structurally has no parameter that could touch them, it
    can only add a new, independent property, same law and same mechanism as
    `annotate_thread` (`_append_property_name`), see that verb's docstring for why a
    content hash would be the wrong key here.

    Refuses (raises ValueError, naming supersede by name) when `ref` resolves to a decision
    that is already superseded: a dead ruling does not grow new reasoning, amending it would
    either misattribute fresh testimony to a ruling no longer in force, or quietly do
    supersede's job without supersede's bookkeeping (the two-way superseded_by/supersedes
    navigation). A correction belongs on `record_decision(supersedes=...)`; this verb only
    ever adds to a ruling still standing.

    Returns the decision id, or None if `ref` matched nothing (same convention as
    `resolve_thread`). Raises ValueError on a blank addendum."""
    addendum = addendum.strip()
    if not addendum:
        raise ValueError("addendum must not be blank — an empty addition is not testimony")
    did = await _find_decision(actions.pool, ref)
    if did is None:
        return None
    superseded_by = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='superseded_by' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1",
        did,
    )
    if superseded_by:
        raise ValueError(
            f"decision {ref!r} is already superseded by {str(superseded_by)[:8]} — amend "
            "the successor, or use record_decision(supersedes=...) if you mean a correction; "
            "amend_decision only ever adds to a ruling still standing")
    observed = datetime.now(UTC)
    await actions.assert_property(did, _append_property_name("addendum"), addendum, source,
                                  observed, _CONF, evidence_class=_EC)
    return did


async def decision_addenda(pool: asyncpg.Pool, decision_id: uuid.UUID) -> list[dict[str, Any]]:
    """Every addendum `amend_decision` has added to this decision, oldest first, see
    `thread_notes` for why each one is independently current."""
    rows = await pool.fetch(
        "SELECT a.value #>> '{}' AS addendum, a.source_id AS source, a.observed_at, "
        "a.confidence FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name LIKE 'addendum:%' ORDER BY a.observed_at ASC",
        decision_id,
    )
    return [{"addendum": r["addendum"], "source": r["source"], "observed_at": r["observed_at"],
             "confidence": float(r["confidence"])} for r in rows]
