"""THE FIRST MINER (wave 16, decision 4d622aee, operator 2026-09-10: "why do they all
need separate miners? if a miner abstract has the same job over different types, it
becomes more manageable"; "yes, build and wire it" — thread 8dfcd4b1, which supersedes
every earlier miner design). ONE generic abstention miner, never N miners per type:
lanes are DATA (object type, required link type, candidate-pool signal, text fields to
read), not code. Each call to `abstention_miner_tick` picks the NEXT object, in
round-robin rotation ACROSS lanes (so no lane's own backlog starves another), that
carries a live `derivation_abstained_<link_type>` record with no already-outstanding
Proposal against it, reads that lane's own text fields, and calls `propose()`
(proposals.py, wave 15 item 1) with AT MOST ONE candidate — or writes nothing at all.
Never a link write: miners remain last resort. Budget, the 30-day acceptance-rate
throttle, the 7-day zero-acceptance stop, and the DERIVED-tier confidence cap all
already exist in `propose()` (wave 15 items 3-4), reused unchanged here, keyed by
(miner='abstention', owner).

STARTING LANES, MEASURED LIVE (decision 4d622aee, 2026-09-10): Decision (79 orphans),
Thread (5), Practice (8), Reference (56) — each targets `in_repo`, the SAME project-
membership link `resolve_agent_orphans`/`resolve_reference_orphans` (capture.py) already
try to derive mechanically and abstain on when they can't. This lane's own candidate
signal is a project-name MENTION anywhere in the object's own text fields — a real, if
crude, textual read, deliberately DIFFERENT from the mechanical sweep's own strict
prefix match on ONE named property (`session`/`topic`), so a miner tick is not simply
re-running the exact check that already gave up. Agent is registered with an EMPTY
pool — "never guessing from names" (operator's own words, mail 9130) — so it proposes
nothing, ever, until a real signal is designed for it.

THE LANE SIGNAL (Thoth ruling, mail 9847, decision 2406c9c5, 2026-09-11): the FIRST
DAY's own telemetry (decision 47c24d12) measured the original "exactly one project
mentioned" gate against real next-in-queue text and found it almost never true — 6, 8,
0, 0 candidates — so real text was either silent or ambiguous, never singular.
`_dominant_project` replaces it: the project mentioned most often wins when it leads the
runner-up by `_DOMINANCE_FACTOR` (2x); a non-dominant or silent read falls back to the
object's own author's `works_in` project (`_author_works_in`, via the `produced` edge —
Decision/Thread only, this schema declares no author edge for Practice/Reference);
neither firing means abstain. Still exactly one candidate into `propose()`, and budget/
throttle/confidence cap stay exactly as they were — this ruling only ever touches which
signal picks the candidate, never propose()'s own law. Each proposal's own `candidate`
carries a `signal` key ("dominance"/"author_tiebreak") so the lane telemetry
(digest.py's `_proposal_telemetry`) can say which one produced it.

`guarded_miner_tick` (proposals.py, wave 15 item 4) is the failure-receipt-first wrapper
every miner tick runs inside; this module supplies the tick body, never calls
`guarded_miner_tick` itself — that is the caller's (the heartbeat wiring's) own job, so
this module stays independently testable without the alarm-thread machinery."""
from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.orchestrator.monitor import get_cursor, set_cursor
from src.orchestrator.proposals import propose

_MINER = "abstention"
_ACTOR = "miner:abstention"
_CURSOR_KEY = "abstention_miner:lane_index"


@dataclass(frozen=True)
class _Lane:
    object_type: str
    link_type: str
    text_fields: tuple[str, ...]
    has_pool: bool = True


_LANES: tuple[_Lane, ...] = (
    _Lane("Decision", "in_repo", ("summary", "rationale")),
    _Lane("Thread", "in_repo", ("summary",)),
    _Lane("Practice", "in_repo", ("statement",)),
    _Lane("Reference", "in_repo", ("topic",)),
    _Lane("Agent", "works_in", (), has_pool=False),
)


async def _next_lane_index(pool: asyncpg.Pool) -> int:
    """Round-robin state, persisted in the generic cursor store (watermarks table, the
    SAME door digest.py's own operator watermark uses) — never in-process memory, since
    a tick can run from any process and must pick up where the last one left off."""
    raw = await get_cursor(pool, _CURSOR_KEY)
    idx = int(raw) if raw and raw.isdigit() else 0
    return idx % len(_LANES)


async def _advance_lane_index(pool: asyncpg.Pool, idx: int) -> None:
    await set_cursor(pool, _CURSOR_KEY, str((idx + 1) % len(_LANES)))


async def _next_abstained_object(pool: asyncpg.Pool, lane: _Lane) -> uuid.UUID | None:
    """The OLDEST active object of this lane's own type carrying a LIVE (unresolved)
    `derivation_abstained_<link_type>` record — the exact predicate `propose()`'s own
    last-resort law already requires (proposals.py's `_live_abstention_exists`, mirrored
    here as a set-scan rather than a single-object check) — that does NOT already carry
    an outstanding (`status='proposed'`, not expired for this check's purpose) Proposal
    against the same (from_id, link_type) pair. Without this second exclusion, every tick
    that lands on an exhausted budget or a slow-to-judge owner would just re-propose the
    same top-of-queue object forever instead of ever reaching the next one."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT o.id FROM objects o "
        "WHERE o.type=$1 AND o.status='active' "
        "AND EXISTS (SELECT 1 FROM current_assertions ca WHERE ca.object_id=o.id "
        "  AND ca.name=$2 AND NOT (ca.value ? 'resolved')) "
        "AND NOT EXISTS ("
        "  SELECT 1 FROM objects pr WHERE pr.type='Proposal' "
        "  AND EXISTS (SELECT 1 FROM current_assertions ep WHERE ep.object_id=pr.id "
        "    AND ep.name='evidence_pointer' AND ep.value ->> 'from_id' = o.id::text "
        "    AND ep.value ->> 'link_type' = $3) "
        "  AND EXISTS (SELECT 1 FROM current_assertions st WHERE st.object_id=pr.id "
        "    AND st.name='status' AND st.value #>> '{}' = 'proposed')) "
        "ORDER BY o.created_at ASC LIMIT 1",
        lane.object_type, f"derivation_abstained_{lane.link_type}", lane.link_type)


_WORD_RE_CACHE: dict[str, re.Pattern[str]] = {}
_DOMINANCE_FACTOR = 2


async def _mention_counts(
    pool: asyncpg.Pool, text_fields: tuple[str, ...], obj_id: uuid.UUID,
) -> tuple[dict[uuid.UUID, int], str]:
    """Read `text_fields` off `obj_id`'s own CURRENT properties, concatenate, and count
    every LIVE SoftwareProject's own whole-word, case-insensitive MENTION COUNT in that
    text — word-boundary-anchored so a short project name never matches inside an
    unrelated longer word (e.g. "os" inside "cosmos"). Returns ({project_id: count, ...}
    with only projects mentioned at least once, joined_text) — the text is handed back
    too so a caller/receipt can show what the miner actually read, never a black box."""
    parts = []
    for field in text_fields:
        val = await pool.fetchval(
            "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
            "AND a.name=$2 ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1",
            obj_id, field)
        if val:
            parts.append(val)
    text = " ".join(parts)
    if not text.strip():
        return {}, text
    projects = await pool.fetch(
        "SELECT id, canonical FROM objects WHERE type='SoftwareProject' AND status='active'")
    counts: dict[uuid.UUID, int] = {}
    for p in projects:
        name = p["canonical"].removeprefix("repo:").strip()
        if not name:
            continue
        pattern = _WORD_RE_CACHE.get(name)
        if pattern is None:
            pattern = re.compile(rf"\b{re.escape(name)}\b", re.IGNORECASE)
            _WORD_RE_CACHE[name] = pattern
        n = len(pattern.findall(text))
        if n:
            counts[p["id"]] = n
    return counts, text


async def _author_works_in(pool: asyncpg.Pool, obj_id: uuid.UUID) -> uuid.UUID | None:
    """The tie-break signal (Thoth ruling, mail 9847, decision 2406c9c5): `obj_id`'s own
    author — the Agent generation that `produced` it (capture.py's traceability edge,
    the only authorship edge this schema declares onto Decision/Thread; Practice/
    Reference have none, so this always returns None for those lanes) — and that
    author's own current `works_in` project. None when the object has no author, or the
    author has no live works_in project; either way the caller abstains."""
    author_id = await pool.fetchval(
        "SELECT from_id FROM links WHERE to_id=$1 AND type='produced' "
        "AND valid_until IS NULL LIMIT 1", obj_id)
    if author_id is None:
        return None
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT l.to_id FROM links l JOIN objects p ON p.id=l.to_id "
        "WHERE l.from_id=$1 AND l.type='works_in' AND l.valid_until IS NULL "
        "AND p.status='active' ORDER BY l.created_at DESC LIMIT 1", author_id)


async def _dominant_project(
    pool: asyncpg.Pool, text_fields: tuple[str, ...], obj_id: uuid.UUID,
) -> tuple[uuid.UUID | None, str, str]:
    """THE LANE SIGNAL (Thoth ruling, mail 9847, decision 2406c9c5, replacing the old
    "exactly one mention" gate that real text almost never satisfied — decision 47c24d12
    measured 6/8/0/0 candidates against real next-in-queue objects in every lane):
    the project mentioned most often in `obj_id`'s own text fields, accepted ONLY when it
    leads the runner-up by at least `_DOMINANCE_FACTOR`x (a lone mention still counts,
    since the runner-up is then 0); tied or non-dominant falls back to the object's own
    author's `works_in` project; neither signal firing means abstain. Returns
    (candidate_id_or_None, joined_text, signal) where `signal` is "dominance",
    "author_tiebreak", or "none" — named so the miner's own receipt and the lane
    telemetry can say which produced (or failed to produce) a proposal, never leave that
    invisible."""
    counts, text = await _mention_counts(pool, text_fields, obj_id)
    if counts:
        ranked = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
        top_id, top_n = ranked[0]
        runner_n = ranked[1][1] if len(ranked) > 1 else 0
        if top_n >= _DOMINANCE_FACTOR * runner_n:
            return top_id, text, "dominance"
    author_project = await _author_works_in(pool, obj_id)
    if author_project is not None:
        return author_project, text, "author_tiebreak"
    return None, text, "none"


def _lanes_off(settings: Any) -> set[str]:
    raw = getattr(settings, "osiris_abstention_miner_lanes_off", "") or ""
    return {p.strip() for p in re.split(r"[,\s]+", raw) if p.strip()}


async def abstention_miner_tick(actions: Actions) -> dict[str, Any]:
    """One tick: advance the round-robin, look at the next lane, propose at most one
    candidate against its next eligible abstention, or do nothing. Never raises past a
    normal `{"action": ...}` receipt — `guarded_miner_tick` (the caller's own wrapper) is
    where a genuine exception becomes a durable failure receipt; this function's own
    business-as-usual "found nothing"/"too many candidates" outcomes are not failures.

    Reads `settings_with_overlay` (THE SETTINGS MENU piece 1, thread f4498ab304e4),
    not bare `get_settings()` — `miner.abstention.enabled` is registered with
    `effect='immediate'`, so a write through the settings door takes hold on the very
    next tick, no restart needed."""
    from src.orchestrator.settings_service import settings_with_overlay

    pool = actions.pool
    settings = await settings_with_overlay(pool)
    if not getattr(settings, "osiris_abstention_miner_enabled", True):
        return {"action": "dark", "reason": "osiris_abstention_miner_enabled is False"}
    idx = await _next_lane_index(pool)
    lane = _LANES[idx]
    await _advance_lane_index(pool, idx)
    if lane.object_type in _lanes_off(settings):
        return {"lane": lane.object_type, "action": "skipped",
                "reason": "silenced via osiris_abstention_miner_lanes_off"}
    if not lane.has_pool:
        return {"lane": lane.object_type, "action": "skipped",
                "reason": "empty candidate pool by design — never guessing from names"}
    obj_id = await _next_abstained_object(pool, lane)
    if obj_id is None:
        return {"lane": lane.object_type, "action": "none",
                "reason": "no eligible abstention this tick"}
    candidate_id, text, signal = await _dominant_project(pool, lane.text_fields, obj_id)
    if candidate_id is None:
        return {"lane": lane.object_type, "object": str(obj_id), "action": "skipped",
                "reason": "no project dominates the text and no author to tie-break",
                "text_read": text[:200]}
    out = await propose(
        actions, from_id=obj_id, link_type=lane.link_type,
        candidate={"kind": "link", "from_id": str(obj_id), "to_id": str(candidate_id),
                  "link_type": lane.link_type, "signal": signal},
        confidence=0.4, owner="operator", miner=_MINER, actor=_ACTOR)
    action = "refused" if "error" in out else "proposed"
    return {"lane": lane.object_type, "object": str(obj_id), "action": action, **out}
