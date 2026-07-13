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
    supersedes: str | None = None, resolves: str | None = None,
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
    recorded — a ruling that miscites the question it settles has not settled it."""
    observed = datetime.now(UTC)
    old: uuid.UUID | None = None
    if supersedes:
        old = await _find_decision(actions.pool, supersedes)
        if old is None:
            raise ValueError(f"supersedes matched no decision: {supersedes!r} — quote its "
                             "UUID, 8-char short id, or a summary substring")
    answered: uuid.UUID | None = None
    if resolves:
        answered = await _find_thread(actions.pool, resolves)
        if answered is None:
            raise ValueError(f"resolves matched no thread: {resolves!r} — quote its UUID, "
                             "8-char short id, or a summary substring")
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
        if answered is not None:
            # the ANSWER and the CLOSE in one transaction: a ruling that lands while its
            # question stays open is how the operator gets asked twice. Same shape the
            # resolve_thread verb writes (status/resolved_in/resolved_because), so every
            # lens that already renders a resolved thread renders this one unchanged.
            exists = await a.pool.fetchval(
                "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='answers'",
                d, answered)
            if not exists:
                await a.create_link(d, answered, "answers", source, observed, _CONF,
                                    evidence_class=_EC)
            await a.assert_property(answered, "status", "resolved", source, observed, _CONF,
                                    evidence_class=_EC)
            await a.assert_property(answered, "resolved_in", source, source, observed, _CONF,
                                    evidence_class=_EC)
            await a.assert_property(answered, "resolved_because",
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


async def open_thread(
    actions: Actions, summary: str, *, repo: str | None = None, kind: str | None = None,
    owner: str | None = None, source: str = _SOURCE,
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
    Unowned = anyone who reads it may act. The lens sorts by it; the record just keeps it."""
    observed = datetime.now(UTC)
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
        if owner:
            await a.assert_property(t, "owner", owner.strip(), source, observed, _CONF,
                                    evidence_class=_EC)
        if repo:
            await link_repo(a, t, repo, observed, source=source, evidence_class=_EC)
    return t


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


async def resolve_thread(
    actions: Actions, ref: str, *, because: str | None = None, source: str = _SOURCE
) -> uuid.UUID | None:
    """Close a thread at source — the session marking a question answered, so it leaves the
    open list and joins the resolved section. Matches the miner's self-heal shape
    (status=resolved + resolved_in + resolved_because) so the `briefing` resolved section
    renders it, with `resolved_in='session'` recording that a session closed it rather than
    a later commit. `ref` is a Thread UUID or a summary substring. Event-sourced via a status
    assertion that supersedes the prior 'open' within-source — never a DELETE. Returns the
    thread id, or None if `ref` matched nothing."""
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
