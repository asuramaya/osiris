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
import uuid
from datetime import UTC, datetime

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
) -> uuid.UUID:
    """Capture a decision at the moment it is made — the WHY, declared, not mined.

    `kind` labels it the way the miner does (ruling / reset / override / rejection /
    choice / decision). `rationale`, if given, is the reasoning stored inline (an
    enrichment the miner can't produce — it only has the commit body). `repo` files the
    decision under a SoftwareProject. `source` is the attributing actor — the static
    `session` for a lone operator, or `agent:<session>` for a fleet member so provenance
    records WHICH instance decided (still SELF_DECLARED, still the high-trust channel).
    Idempotent on the summary hash. Returns the id."""
    observed = datetime.now(UTC)
    d = await actions.create_or_find_object("Decision", _canon("decision", summary), source)
    await actions.assert_property(d, "summary", summary, source, observed, _CONF,
                                  evidence_class=_EC)
    await actions.assert_property(d, "kind", kind, source, observed, _CONF,
                                  evidence_class=_EC)
    if rationale:
        await actions.assert_property(d, "rationale", rationale, source, observed, _CONF,
                                      evidence_class=_EC)
    if repo:
        await link_repo(actions, d, repo, observed, source=source, evidence_class=_EC)
    return d


async def open_thread(
    actions: Actions, summary: str, *, repo: str | None = None, kind: str | None = None,
    source: str = _SOURCE,
) -> uuid.UUID:
    """Open a thread at source — an unresolved question / next-step for the next session
    to inherit. Same shape as a mined Thread (props summary + status=open) so it appears in
    `briefing`'s open-threads section beside mined ones. Idempotent on the summary hash.

    `kind='obligation'` marks the obligations class (ruling 7336c5fc): a DUTY minted by an
    action ("kernel changed → daemons need restart") — neither a ruling nor ordinary work,
    exactly the thing that used to die with the context window. Same Thread shape, so it
    surfaces in briefing beside the rest; the kind stays as data for filtering. `source`
    attributes the opening actor (a fleet agent vs the lone `session`)."""
    observed = datetime.now(UTC)
    t = await actions.create_or_find_object("Thread", _canon("thread", summary), source)
    await actions.assert_property(t, "summary", summary, source, observed, _CONF,
                                  evidence_class=_EC)
    await actions.assert_property(t, "status", "open", source, observed, _CONF,
                                  evidence_class=_EC)
    if kind:
        await actions.assert_property(t, "kind", kind, source, observed, _CONF,
                                      evidence_class=_EC)
    if repo:
        await link_repo(actions, t, repo, observed, source=source, evidence_class=_EC)
    return t


async def _find_thread(pool: asyncpg.Pool, ref: str) -> uuid.UUID | None:
    """A Thread by UUID, or by summary substring (shortest summary wins — closest to the
    query)."""
    try:
        return uuid.UUID(ref)
    except (ValueError, AttributeError):
        pass
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT o.id FROM objects o JOIN current_assertions a ON a.object_id=o.id "
        "WHERE o.type='Thread' AND o.status='active' AND a.name='summary' "
        "AND a.value #>> '{}' ILIKE '%'||$1||'%' ORDER BY length(a.value #>> '{}') ASC LIMIT 1",
        ref,
    )


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
