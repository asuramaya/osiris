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
from src.config.settings import get_settings
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


async def _project_mentioned_in_text(
    pool: asyncpg.Pool, text_fields: tuple[str, ...], obj_id: uuid.UUID,
) -> tuple[list[uuid.UUID], str]:
    """Read `text_fields` off `obj_id`'s own CURRENT properties, concatenate, and find
    every LIVE SoftwareProject whose own name appears as a whole-word, case-insensitive
    mention in that text — word-boundary-anchored so a short project name never matches
    inside an unrelated longer word (e.g. "os" inside "cosmos"). Returns
    (candidate_ids, joined_text) — the text is handed back too so a caller/receipt can
    show what the miner actually read, never a black box."""
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
        return [], text
    projects = await pool.fetch(
        "SELECT id, canonical FROM objects WHERE type='SoftwareProject' AND status='active'")
    hits: list[uuid.UUID] = []
    for p in projects:
        name = p["canonical"].removeprefix("repo:").strip()
        if not name:
            continue
        pattern = _WORD_RE_CACHE.get(name)
        if pattern is None:
            pattern = re.compile(rf"\b{re.escape(name)}\b", re.IGNORECASE)
            _WORD_RE_CACHE[name] = pattern
        if pattern.search(text):
            hits.append(p["id"])
    return hits, text


def _lanes_off(settings: Any) -> set[str]:
    raw = getattr(settings, "osiris_abstention_miner_lanes_off", "") or ""
    return {p.strip() for p in re.split(r"[,\s]+", raw) if p.strip()}


async def abstention_miner_tick(actions: Actions) -> dict[str, Any]:
    """One tick: advance the round-robin, look at the next lane, propose at most one
    candidate against its next eligible abstention, or do nothing. Never raises past a
    normal `{"action": ...}` receipt — `guarded_miner_tick` (the caller's own wrapper) is
    where a genuine exception becomes a durable failure receipt; this function's own
    business-as-usual "found nothing"/"too many candidates" outcomes are not failures."""
    settings = get_settings()
    pool = actions.pool
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
    candidates, text = await _project_mentioned_in_text(pool, lane.text_fields, obj_id)
    if len(candidates) != 1:
        return {"lane": lane.object_type, "object": str(obj_id), "action": "skipped",
                "reason": f"{len(candidates)} candidate(s) mentioned in text, need exactly 1",
                "text_read": text[:200]}
    out = await propose(
        actions, from_id=obj_id, link_type=lane.link_type,
        candidate={"kind": "link", "from_id": str(obj_id), "to_id": str(candidates[0]),
                  "link_type": lane.link_type},
        confidence=0.4, owner="operator", miner=_MINER, actor=_ACTOR)
    action = "refused" if "error" in out else "proposed"
    return {"lane": lane.object_type, "object": str(obj_id), "action": action, **out}
