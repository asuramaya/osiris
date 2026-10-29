"""THE FIRST MINER (decision recorded 2026-09-10, prompted by an operator observation
that separate miners per type were becoming hard to manage, and a request to build one
generic miner and wire it in, which supersedes every earlier miner design). ONE generic
abstention miner, never N miners per type: lanes are DATA (object type, required link
type, candidate-pool signal, text fields to read), not code. Each call to
`abstention_miner_tick` picks the NEXT object, in round-robin rotation ACROSS lanes (so
no lane's own backlog starves another), that carries a live
`derivation_abstained_<link_type>` record with no already-outstanding Proposal against
it, reads that lane's own text fields, and calls `propose()` (proposals.py) with AT MOST
ONE candidate, or writes nothing at all. Never a link write: miners remain last resort.
Budget, the 30-day acceptance-rate throttle, the 7-day zero-acceptance stop, and the
DERIVED-tier confidence cap all already exist in `propose()`, reused unchanged here,
keyed by (miner='abstention', owner).

STARTING LANES, MEASURED LIVE (2026-09-10): Decision (79 orphans),
Thread (5), Practice (8), Reference (56); each targets `in_repo`, the SAME project-
membership link `resolve_agent_orphans`/`resolve_reference_orphans` (capture.py) already
try to derive mechanically and abstain on when they can't. This lane's own candidate
signal is a project-name MENTION anywhere in the object's own text fields: a real, if
crude, textual read, deliberately DIFFERENT from the mechanical sweep's own strict
prefix match on ONE named property (`session`/`topic`), so a miner tick is not simply
re-running the exact check that already gave up. Agent is registered with an EMPTY pool,
by design never guessing a project from a name alone, so it proposes nothing, ever, until
a real signal is designed for it.

THE STOPLIST (2026-10-29): a project whose name is also a seat or agent name, a person's
name or an ordinary word counts only when it is mentioned the way a repository is (`repo:x`,
a path ending `/x`, "x repo"); the bare word there is the author or the sentence (live: a
thread that said it was filed elsewhere was matched to the project named for its author).
Decided at read time against the live seats and people (`_ambiguous_names`), nothing listed
by hand but a small generic word list.

THE LANE SIGNAL (a later ruling, 2026-09-11): the FIRST DAY's own telemetry measured the
original "exactly one project mentioned" gate against real next-in-queue text and found
it almost never true (6, 8, 0, 0 candidates), so real text was either silent or
ambiguous, never singular. `_dominant_project` replaces it: the project mentioned most
often wins when it leads the runner-up by `_DOMINANCE_FACTOR` (2x); a non-dominant or
silent read falls back to the object's own author's `works_in` project
(`_author_works_in`, via the `produced` edge, Decision/Thread only, this schema declares
no author edge for Practice/Reference); neither firing means abstain. Still exactly one
candidate into `propose()`, and budget/throttle/confidence cap stay exactly as they
were: this ruling only ever touches which signal picks the candidate, never propose()'s
own rule. Each proposal's own `candidate` carries a `signal` key
("dominance"/"author_tiebreak") so the lane telemetry (digest.py's
`_proposal_telemetry`) can say which one produced it.

`guarded_miner_tick` (proposals.py) is the failure-reporting-first wrapper every miner
tick runs inside; this module supplies the tick body, never calls `guarded_miner_tick`
itself: that is the caller's (the heartbeat wiring's) own job, so this module stays
independently testable without the alarm-thread machinery."""
from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.orchestrator.monitor import get_cursor, set_cursor
from src.orchestrator.proposals import propose

_MINER = "abstention"
_ACTOR = "miner:abstention"
_CURSOR_KEY = "abstention_miner:lane_index"
_OUTCOMES_KEY_PREFIX = "abstention_miner:outcomes:"
_POSITION_KEY_PREFIX = "abstention_miner:position:"
_WALK_LIMIT = 25     # abstentions a single tick may step past before it stops


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


def _rotation_after(idx: int) -> int:
    """The first lane at or after `idx` (wrapping) that has a candidate pool. A lane with an
    empty pool never acts, so a tick spent on it is a tick lost for the others."""
    for step in range(len(_LANES)):
        k = (idx + step) % len(_LANES)
        if _LANES[k].has_pool:
            return k
    return idx % len(_LANES)


async def _next_lane_index(pool: asyncpg.Pool) -> int:
    """Round-robin state, persisted in the generic cursor store (watermarks table, the
    SAME call digest.py's own operator watermark uses), never in-process memory, since
    a tick can run from any process and must pick up where the last one left off. Lanes
    without a candidate pool are not in the rotation."""
    raw = await get_cursor(pool, _CURSOR_KEY)
    idx = int(raw) if raw and raw.isdigit() else 0
    return _rotation_after(idx % len(_LANES))


async def _advance_lane_index(pool: asyncpg.Pool, idx: int) -> None:
    await set_cursor(pool, _CURSOR_KEY, str(_rotation_after((idx + 1) % len(_LANES))))


_ELIGIBLE_SQL = (
    "FROM objects o "
    "WHERE o.type=$1 AND o.status='active' "
    "AND EXISTS (SELECT 1 FROM current_assertions ca WHERE ca.object_id=o.id "
    "  AND ca.name=$2 AND NOT (ca.value ? 'resolved')) "
    "AND NOT EXISTS ("
    "  SELECT 1 FROM objects pr WHERE pr.type='Proposal' "
    "  AND EXISTS (SELECT 1 FROM current_assertions ep WHERE ep.object_id=pr.id "
    "    AND ep.name='evidence_pointer' AND ep.value ->> 'from_id' = o.id::text "
    "    AND ep.value ->> 'link_type' = $3) "
    "  AND EXISTS (SELECT 1 FROM current_assertions st WHERE st.object_id=pr.id "
    "    AND st.name='status' AND st.value #>> '{}' = 'proposed')) ")


def _position_key(lane: _Lane) -> str:
    return f"{_POSITION_KEY_PREFIX}{lane.object_type}"


async def _next_abstained_object(
    pool: asyncpg.Pool, lane: _Lane, *, after: uuid.UUID | None = None,
) -> uuid.UUID | None:
    """The next active object of this lane's own type carrying a LIVE (unresolved)
    `derivation_abstained_<link_type>` record, the exact predicate `propose()`'s own
    last-resort rule already requires (proposals.py's `_live_abstention_exists`, mirrored
    here as a set-scan rather than a single-object check), that does NOT already carry
    an outstanding (`status='proposed'`) Proposal against the same (from_id, link_type)
    pair; an expired one is swept to 'expired' (proposals.py `sweep_expired`) first, so it
    stops counting.

    OLDEST FIRST, AFTER THE LAST ONE EXAMINED: the lane keeps a position (the object its
    last counted tick looked at, in the cursor store) and the pick is the oldest eligible
    object strictly after it, ordered by (created_at, id) so the order is total; when none
    is left it wraps to the oldest again. Without a position an object the miner abstains
    on is the oldest eligible one on every later tick too, so the lane never reaches the
    objects behind it (measured live: the first object of every lane abstained, 779 more
    waiting). `after` names the position explicitly (a tick walking forward holds it in
    memory); without it the stored position is read. The position is advanced by
    `_advance_position` and nothing is written on the objects themselves."""
    args = (lane.object_type, f"derivation_abstained_{lane.link_type}", lane.link_type)
    order = "ORDER BY o.created_at ASC, o.id ASC LIMIT 1"
    if after is None:
        stored = await get_cursor(pool, _position_key(lane))
        try:
            after = uuid.UUID(stored) if stored else None
        except ValueError:
            after = None
    if after is not None:
        found: uuid.UUID | None = await pool.fetchval(
            "SELECT o.id " + _ELIGIBLE_SQL
            + "AND (o.created_at, o.id) > (SELECT created_at, id FROM objects WHERE id=$4) "
            + order, *args, after)
        if found is not None:
            return found
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT o.id " + _ELIGIBLE_SQL + order, *args)


async def _advance_position(pool: asyncpg.Pool, lane: _Lane, obj_id: uuid.UUID) -> None:
    """Remember that `obj_id` was the lane's last examined object."""
    await set_cursor(pool, _position_key(lane), str(obj_id))


_WORD_RE_CACHE: dict[str, re.Pattern[str]] = {}
_DOMINANCE_FACTOR = 2


# Plain words a repository could be named for, so that the word in a sentence is not evidence of
# the repository. A SMALL generic list, never repo names: whether a project name collides with
# one is decided at read time, against the live project list.
_ORDINARY_WORDS = frozenset((
    "api app apps base bridge build cloud code common config console core data demo desk "
    "docs engine example framework gateway hub internships kernel lab library link main model "
    "monitor neo network node pipeline platform plugin portal probe project prototype proxy "
    "recon registry relay runtime sandbox scanner server service shared signal site spec "
    "starter store stream studio sync system test tests tool tools toolkit tracker utils "
    "vector web worker workspace").split())


async def _ambiguous_names(pool: asyncpg.Pool, project_names: list[str]) -> set[str]:
    """The lower-case project names whose bare word is NOT evidence of the project: it is also
    a seat's or an agent's own name or handle, a person's name (the local part of a `dev:` or
    `person:` identity), or an ordinary word. Read live, so a new seat that shares a project's
    name is covered the day it is claimed, with nothing listed by hand."""
    lowered = [n.lower() for n in project_names]
    found = {n for n in lowered if n in _ORDINARY_WORDS}
    for row in await pool.fetch(
            "SELECT DISTINCT lower(a.value #>> '{}') AS n FROM current_assertions a "
            "JOIN objects o ON o.id=a.object_id "
            "WHERE o.type IN ('Seat','Agent') AND a.name IN ('name','handle') "
            "AND lower(a.value #>> '{}') = ANY($1::text[])", lowered):
        found.add(row["n"])
    for row in await pool.fetch(
            "SELECT DISTINCT lower(split_part(split_part(canonical, ':', 2), '@', 1)) AS n "
            "FROM objects WHERE canonical ~ '^(dev|person):' AND status='active'"):
        if row["n"] in lowered:
            found.add(row["n"])
    return found


_STRICT_RE_CACHE: dict[str, re.Pattern[str]] = {}


def _strict_pattern(name: str) -> re.Pattern[str]:
    """A mention that is shaped like a repository: `repo:name`, a path ending in `/name`, or
    "name repo". The only kind that counts for a name that is also a person, a seat or a word.
    A home directory (`/home/name`, `/Users/name`) or a GitHub owner (`github.com/name`) is
    the person, not a repository."""
    pattern = _STRICT_RE_CACHE.get(name)
    if pattern is None:
        e = re.escape(name)
        pattern = re.compile(
            rf"(?:repo:|(?<!home)(?<!Users)(?<!github\.com)[/\\]){e}\b"
            rf"|\b{e}(?:'s)?[ -](?:repo|repository|codebase)\b", re.IGNORECASE)
        _STRICT_RE_CACHE[name] = pattern
    return pattern


async def _project_patterns(
    pool: asyncpg.Pool,
) -> list[tuple[uuid.UUID, re.Pattern[str], str]]:
    """Every LIVE SoftwareProject's name pattern: whole-word and case-insensitive,
    word-boundary-anchored so a short project name never matches inside an unrelated longer
    word (e.g. "os" inside "cosmos"). A name that is also a seat, an agent, a person or an
    ordinary word (`_ambiguous_names`) gets the repo-shaped pattern instead, since the bare
    word there is far more often the author or the sentence than the repository."""
    rows = [(p["id"], p["canonical"].removeprefix("repo:").strip()) for p in await pool.fetch(
        "SELECT id, canonical FROM objects WHERE type='SoftwareProject' AND status='active'")]
    rows = [(pid, name) for pid, name in rows if name]
    ambiguous = await _ambiguous_names(pool, [name for _, name in rows])
    out: list[tuple[uuid.UUID, re.Pattern[str], str]] = []
    for pid, name in rows:
        if name.lower() in ambiguous:
            out.append((pid, _strict_pattern(name), name.lower()))
            continue
        pattern = _WORD_RE_CACHE.get(name)
        if pattern is None:
            pattern = re.compile(rf"\b{re.escape(name)}\b", re.IGNORECASE)
            _WORD_RE_CACHE[name] = pattern
        out.append((pid, pattern, name.lower()))
    return out


def _count_mentions(
    text: str, patterns: list[tuple[uuid.UUID, re.Pattern[str], str]],
) -> dict[uuid.UUID, int]:
    counts: dict[uuid.UUID, int] = {}
    low = text.lower()
    for pid, pattern, name in patterns:
        if name not in low:      # cheap exact pre-check: a name that is not there cannot match
            continue
        n = len(pattern.findall(text))
        if n:
            counts[pid] = n
    return counts


def _pick_dominant(counts: dict[uuid.UUID, int]) -> uuid.UUID | None:
    """The project mentioned most often when it leads the runner-up by `_DOMINANCE_FACTOR`x."""
    if not counts:
        return None
    ranked = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
    top_id, top_n = ranked[0]
    runner_n = ranked[1][1] if len(ranked) > 1 else 0
    return top_id if top_n >= _DOMINANCE_FACTOR * runner_n else None


async def _mention_counts(
    pool: asyncpg.Pool, text_fields: tuple[str, ...], obj_id: uuid.UUID,
    patterns: list[tuple[uuid.UUID, re.Pattern[str], str]] | None = None,
) -> tuple[dict[uuid.UUID, int], str]:
    """Read `text_fields` off `obj_id`'s own CURRENT properties, concatenate, and count
    every LIVE SoftwareProject's own MENTION COUNT in that text (see `_project_patterns`).
    Returns ({project_id: count, ...} with only projects mentioned at least once,
    joined_text); the text is handed back too so a caller or log record can show what the
    miner actually read, never a black box."""
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
    return _count_mentions(text, patterns or await _project_patterns(pool)), text


async def _author_works_in(pool: asyncpg.Pool, obj_id: uuid.UUID) -> uuid.UUID | None:
    """The tie-break signal (from the same later ruling): `obj_id`'s own
    author, the Agent generation that `produced` it (capture.py's traceability edge,
    the only authorship edge this schema declares onto Decision/Thread; Practice/
    Reference have none, so this always returns None for those lanes), and that
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
    patterns: list[tuple[uuid.UUID, re.Pattern[str], str]] | None = None,
) -> tuple[uuid.UUID | None, str, str]:
    """THE LANE SIGNAL (from the later ruling, replacing the old
    "exactly one mention" gate that real text almost never satisfied: telemetry
    measured 6/8/0/0 candidates against real next-in-queue objects in every lane):
    the project mentioned most often in `obj_id`'s own text fields, accepted ONLY when it
    leads the runner-up by at least `_DOMINANCE_FACTOR`x (a lone mention still counts,
    since the runner-up is then 0); tied or non-dominant falls back to the object's own
    author's `works_in` project; neither signal firing means abstain. Returns
    (candidate_id_or_None, joined_text, signal) where `signal` is "dominance",
    "author_tiebreak", or "none", named so the miner's own outcome record and the lane
    telemetry can say which produced (or failed to produce) a proposal, never leave that
    invisible."""
    counts, text = await _mention_counts(pool, text_fields, obj_id, patterns)
    top = _pick_dominant(counts)
    if top is not None:
        return top, text, "dominance"
    author_project = await _author_works_in(pool, obj_id)
    if author_project is not None:
        return author_project, text, "author_tiebreak"
    return None, text, "none"


def _lanes_off(settings: Any) -> set[str]:
    raw = getattr(settings, "osiris_abstention_miner_lanes_off", "") or ""
    return {p.strip() for p in re.split(r"[,\s]+", raw) if p.strip()}


async def _tick(actions: Actions) -> dict[str, Any]:
    """One tick: advance the round-robin, look at the next lane, step past up to `_WALK_LIMIT`
    abstentions (one position write at the end, nothing on the objects) to the first object a
    project can be proposed for, and propose at most one candidate, or do nothing. Never
    raises past a normal `{"action": ...}` result; `guarded_miner_tick` (the caller's own
    wrapper) is where a genuine exception becomes a durable failure record; this function's own
    business-as-usual "found nothing"/"too many candidates" outcomes are not failures.

    Reads `settings_with_overlay` (part of the settings-overlay design), not bare
    `get_settings()`: `miner.abstention.enabled` is registered with
    `effect='immediate'`, so a write through the settings entry point takes hold on the
    very next tick, no restart needed."""
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
                "reason": "empty candidate pool by design: never guessing from names"}
    patterns = await _project_patterns(pool)
    position = await _next_abstained_object(pool, lane)
    if position is None:
        return {"lane": lane.object_type, "action": "none",
                "reason": "no eligible abstention this tick"}
    examined: list[uuid.UUID] = []
    obj_id: uuid.UUID | None = position
    last_text = ""
    while obj_id is not None and obj_id not in examined and len(examined) < _WALK_LIMIT:
        candidate_id, last_text, signal = await _dominant_project(
            pool, lane.text_fields, obj_id, patterns)
        if candidate_id is None:        # examined and left alone: step past it
            examined.append(obj_id)
            obj_id = await _next_abstained_object(pool, lane, after=obj_id)
            continue
        out = await propose(
            actions, from_id=obj_id, link_type=lane.link_type,
            candidate={"kind": "link", "from_id": str(obj_id), "to_id": str(candidate_id),
                      "link_type": lane.link_type, "signal": signal},
            confidence=0.4, owner="operator", miner=_MINER, actor=_ACTOR)
        action = "refused" if "error" in out else "proposed"
        if action == "proposed":
            examined.append(obj_id)
        # a refusal (budget spent, throttled) keeps the object: it has not been examined away
        if examined:
            await _advance_position(pool, lane, examined[-1])
        return {"lane": lane.object_type, "object": str(obj_id), "action": action,
                "examined": len(examined), **out}
    await _advance_position(pool, lane, examined[-1])
    return {"lane": lane.object_type, "object": str(examined[-1]), "action": "skipped",
            "reason": "no project dominates the text and no author to tie-break",
            "examined": len(examined), "text_read": last_text[:200]}


async def abstention_miner_tick(actions: Actions) -> dict[str, Any]:
    """One tick (see `_tick`), after moving Proposals past their date to 'expired' so they
    stop hiding their objects, and with its outcome counted for the day (`_note_outcome`),
    because a tick that skipped or found nothing used to leave no trace at all."""
    from src.orchestrator.proposals import sweep_expired

    await sweep_expired(actions)
    out = await _tick(actions)
    if out.get("action") != "dark":
        await _note_outcome(actions.pool, str(out.get("lane", "")), str(out.get("action", "")))
    return out


def _outcome_key(day: str) -> str:
    return f"{_OUTCOMES_KEY_PREFIX}{day}"


async def _note_outcome(pool: asyncpg.Pool, lane: str, action: str) -> None:
    """Add one to today's `<lane>:<action>` counter. One JSON object per UTC day in the
    cursor store, bumped by a single atomic upsert (no read-modify-write), so two ticks can
    never lose a count."""
    day = datetime.now(UTC).date().isoformat()
    name = f"{lane}:{action}"
    await pool.execute(
        "INSERT INTO watermarks (key, cursor, updated_at) "
        "VALUES ($1, jsonb_build_object($2::text, 1)::text, now()) "
        "ON CONFLICT (key) DO UPDATE SET updated_at=now(), cursor=jsonb_set("
        "  coalesce(watermarks.cursor::jsonb, '{}'::jsonb), ARRAY[$2::text], "
        "  to_jsonb(coalesce((watermarks.cursor::jsonb ->> $2::text)::int, 0) + 1))::text",
        _outcome_key(day), name)


_MENTION_CLASSES = ("0", "1", "2+")


def _top_mentions(counts: dict[uuid.UUID, int], names: dict[uuid.UUID, str]) -> dict[str, int]:
    top = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[:3]
    return {names.get(k, str(k)): v for k, v in top}


def _mention_class(counts: dict[uuid.UUID, int]) -> str:
    """How many DIFFERENT projects the text names: none, one, or two and more."""
    return _MENTION_CLASSES[min(len(counts), 2)]


async def _eligible_in_pick_order(
    pool: asyncpg.Pool, lane: _Lane, limit: int,
) -> list[asyncpg.Record]:
    """The next `limit` eligible objects in the order ticks will take them: after the lane's
    position, then wrapping to the start (an object never twice)."""
    args = (lane.object_type, f"derivation_abstained_{lane.link_type}", lane.link_type)
    order = "ORDER BY o.created_at ASC, o.id ASC LIMIT "
    rows: list[asyncpg.Record] = []
    after = await get_cursor(pool, _position_key(lane))
    try:
        after_id = uuid.UUID(after) if after else None
    except ValueError:
        after_id = None
    if after_id is not None:
        rows = list(await pool.fetch(
            "SELECT o.id, o.created_at " + _ELIGIBLE_SQL
            + "AND (o.created_at, o.id) > (SELECT created_at, id FROM objects WHERE id=$4) "
            + order + "$5", *args, after_id, limit))
    if len(rows) < limit:
        seen = {r["id"] for r in rows}
        wrapped = await pool.fetch(
            "SELECT o.id, o.created_at " + _ELIGIBLE_SQL + order + "$4", *args, limit)
        rows += [r for r in wrapped if r["id"] not in seen][: limit - len(rows)]
    return rows


async def _lane_scan(
    pool: asyncpg.Pool, lane: _Lane, patterns: list[tuple[uuid.UUID, re.Pattern[str], str]],
) -> list[dict[str, Any]]:
    """Every eligible object of the lane with the text the miner would read, its mention
    counts and whether its author works in a project: three queries for the whole lane, not
    three per object. (A multi-author object may differ from `_author_works_in`, which takes
    the first producer; the aggregate is for judging the rule, the picks use the real one.)"""
    fields = ", ".join(
        f"(SELECT a.value #>> '{{}}' FROM current_assertions a WHERE a.object_id=o.id "
        f"AND a.name=${4 + k} ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1)"
        for k in range(len(lane.text_fields)))
    rows = await pool.fetch(
        f"SELECT o.id, o.created_at, ARRAY[{fields}] AS parts " + _ELIGIBLE_SQL
        + "ORDER BY o.created_at ASC, o.id ASC",
        lane.object_type, f"derivation_abstained_{lane.link_type}", lane.link_type,
        *lane.text_fields)
    ids = [r["id"] for r in rows]
    with_author = {r["obj"] for r in await pool.fetch(
        "SELECT DISTINCT l.to_id AS obj FROM links l JOIN links w ON w.from_id=l.from_id "
        "AND w.type='works_in' AND w.valid_until IS NULL "
        "JOIN objects p ON p.id=w.to_id AND p.status='active' "
        "WHERE l.type='produced' AND l.valid_until IS NULL AND l.to_id = ANY($1::uuid[])",
        ids)}
    out: list[dict[str, Any]] = []
    for r in rows:
        text = " ".join(x for x in r["parts"] if x)
        counts = _count_mentions(text, patterns) if text.strip() else {}
        dominant = _pick_dominant(counts)
        out.append({"id": r["id"], "created": r["created_at"], "text": text, "counts": counts,
                    "dominant": dominant, "has_author": r["id"] in with_author})
    return out


async def abstention_survey(
    pool: asyncpg.Pool, *, per_lane: int = 5, days: int = 7, class2_per_lane: int = 10,
    excerpts: bool = False,
) -> dict[str, Any]:
    """READ-ONLY. Why the miner proposes so little, in one glance, for the digest.

    Per lane that has a candidate pool: how many abstentions are eligible; the next
    `per_lane` of them in the order ticks take them (after the lane's position, oldest
    first) with the mention class (0, 1 or 2+ different projects named), the signal the
    current rule would pick, and the project it picks or why it abstains; the class of ALL
    eligible objects and what the rule would do with each (dominance, author tie-break or
    abstain); and `class2_per_lane` evenly spaced objects that name two or more projects, the
    hard cases the dominance rule is judged on (with `excerpts`, the first 240 characters of
    the text the rule read). Plus the tick outcomes counted per day for the last `days` days,
    and how many objects are hidden behind a still-outstanding Proposal.
    Writes nothing and calls no model."""
    names = {r["id"]: r["canonical"].removeprefix("repo:") for r in await pool.fetch(
        "SELECT id, canonical FROM objects WHERE type='SoftwareProject' AND status='active'")}
    patterns = await _project_patterns(pool)
    lanes: list[dict[str, Any]] = []
    for lane in _LANES:
        if not lane.has_pool:
            continue
        scan = await _lane_scan(pool, lane, patterns)
        by_id = {x["id"]: x for x in scan}
        position = await get_cursor(pool, _position_key(lane))
        nxt: list[dict[str, Any]] = []
        for r in await _eligible_in_pick_order(pool, lane, per_lane):
            x = by_id.get(r["id"])
            if x is None:
                continue
            picked, signal = x["dominant"], "dominance"
            if picked is None and x["has_author"]:
                picked, signal = await _author_works_in(pool, r["id"]), "author_tiebreak"
            if picked is None:
                signal = "none"
            if picked is not None:
                reason = None
            elif x["counts"]:
                reason = "mentions are split and there is no author project"
            else:
                reason = "no project is mentioned and there is no author project"
            nxt.append({
                "object": str(r["id"]), "created": r["created_at"].date().isoformat(),
                "mention_class": _mention_class(x["counts"]),
                "mentions": _top_mentions(x["counts"], names),
                "signal": signal, "picks": names.get(picked) if picked else None,
                "abstain_reason": reason})
        classes = {c: 0 for c in _MENTION_CLASSES}
        fate = {"dominance": 0, "author_tiebreak": 0, "abstain": 0}
        two_plus: list[dict[str, Any]] = []
        for x in scan:
            classes[_mention_class(x["counts"])] += 1
            if x["dominant"] is not None:
                fate["dominance"] += 1
            elif x["has_author"]:
                fate["author_tiebreak"] += 1
            else:
                fate["abstain"] += 1
            if len(x["counts"]) >= 2:
                two_plus.append(x)
        step = max(1, len(two_plus) // class2_per_lane) if class2_per_lane else 1
        samples: list[dict[str, Any]] = []
        for x in two_plus[::step][:class2_per_lane]:
            row: dict[str, Any] = {
                "object": str(x["id"]), "created": x["created"].date().isoformat(),
                "mentions": _top_mentions(x["counts"], names),
                "signal": "dominance" if x["dominant"] is not None else (
                    "author_tiebreak" if x["has_author"] else "none"),
                "picks": names.get(x["dominant"]) if x["dominant"] is not None else None}
            if excerpts:
                row["excerpt"] = x["text"][:240]
            samples.append(row)
        lanes.append({
            "lane": f"{lane.object_type}:{lane.link_type}", "eligible": len(scan),
            "position": position,
            "first_abstains": bool(nxt) and nxt[0]["picks"] is None,
            "next": nxt,
            "next_abstaining": sum(1 for n in nxt if n["picks"] is None),
            "classes": classes, "if_taken": fate, "class2_samples": samples})
    today = datetime.now(UTC).date()
    outcomes: dict[str, dict[str, int]] = {}
    for back in range(days):
        day = (today - timedelta(days=back)).isoformat()
        raw = await get_cursor(pool, _outcome_key(day))
        if raw:
            try:
                outcomes[day] = {str(k): int(v) for k, v in json.loads(raw).items()}
            except (ValueError, TypeError, AttributeError):
                continue
    totals: dict[str, int] = {}
    for per_day in outcomes.values():
        for k, v in per_day.items():
            totals[k] = totals.get(k, 0) + v
    outstanding = int(await pool.fetchval(
        "SELECT count(*) FROM objects pr WHERE pr.type='Proposal' AND EXISTS ("
        "SELECT 1 FROM current_assertions st WHERE st.object_id=pr.id AND st.name='status' "
        "AND st.value #>> '{}' = 'proposed')") or 0)
    return {"lanes": lanes, "outcomes_by_day": outcomes, "outcomes_total": totals,
            "outcomes_window_days": days, "proposals_outstanding": outstanding}
