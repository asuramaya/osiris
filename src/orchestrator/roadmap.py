"""roadmap(project) — open Threads/obligations AND their resolved/retracted history, one flat
tracker grouped arc→status→owner (thread 521ae613a6f4 / d56e7073 / 8df8e611; v2, Thoth's
go msg 1299 — `arc` finally exists on `open_thread()`, locked taxonomy in capture.ARCS).

COMPOSES rather than reinvents the open half: `open_thread_wall` (compositions.py) already
gives the real, echo-filtered wall for a project — an obligation mechanically inferred but
never touched by a mind is excluded (ruling 61c1b20d, "a guess does not get a week"), and its
unfiled-owner fallback (thread 4ffe0eb9, a thread opened without `repo=` whose OWNER still
names this project) rides along for free. `rank_open_threads` orders that wall
obligations-first, then by whose move it is (mine-to-act / another mind's / waiting-on-the-
human) — reused verbatim for the open bucket's internal order; that ranking IS "orient's
ranking" this verb was asked to reuse. `arc` is fetched SEPARATELY here (by short-id, a
batch lookup keyed on the same 8-char ids the wall already returns) rather than widening
`open_thread_wall` itself — that function is shared with orient()'s own wall, which has no
need of arc, and a self-contained lookup here means no risk of a collision on a file other
work may be touching.

The RESOLVED/RETRACTED half is new relative to orient/the wall (both open-only by design) —
a roadmap that only ever shows what's still outstanding isn't tracking anything. No
echo-filtering is needed for it: resolving or retracting a thread is itself a self_declared
act (`capture.py`/`dispose.py`), so an untouched resolved thread cannot exist by construction.
Deliberately simpler than the open half, flagged not hidden: it does NOT replicate the
unfiled-owner fallback (a thread closed without ever having been filed to this project by
`repo=` or by its owner's project — a narrow edge the open wall's own history shows is rare).

A thread with no `arc` (every thread minted before this build, and any future omission)
buckets under the literal "unsorted" label — LAST in the arc order, never invented, never
silently dropped. Purely a READ over whatever the graph already holds — mints nothing,
asserts nothing."""
from __future__ import annotations

from typing import Any

import asyncpg

from src.orchestrator.capture import ARCS

_STATUS_ORDER = ("open", "resolved", "retracted")
_UNSORTED = "unsorted"
_ARC_ORDER = (*ARCS, _UNSORTED)
_DONE_LIMIT = 200


async def _done_threads(pool: asyncpg.Pool, proj: Any) -> list[dict[str, Any]]:
    """Recently resolved/retracted threads filed to this project — recency-desc, capped."""
    rows = await pool.fetch(
        "SELECT o.id, o.created_at, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS summary, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='kind' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS kind, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='owner' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS owner, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='arc' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS arc, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS status "
        "FROM objects o JOIN links l ON l.from_id=o.id AND l.type='in_repo' AND l.to_id=$1 "
        "WHERE o.type='Thread' AND o.merged_into IS NULL AND o.status='active' "
        "  AND COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
        "   WHERE a.object_id=o.id AND a.name='status' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1),'open') "
        "   IN ('resolved','retracted') "
        "ORDER BY o.created_at DESC LIMIT $2", proj, _DONE_LIMIT)
    return [dict(r) for r in rows if r["summary"]]


async def _arc_map(pool: asyncpg.Pool, short_ids: list[str]) -> dict[str, str]:
    """arc values for a batch of OPEN threads, keyed by the 8-char short id `open_thread_wall`
    already hands out — a self-contained lookup, never widening the shared wall function."""
    if not short_ids:
        return {}
    rows = await pool.fetch(
        "SELECT substring(o.id::text, 1, 8) AS short_id, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='arc' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS arc "
        "FROM objects o WHERE substring(o.id::text, 1, 8) = ANY($1::text[])", short_ids)
    return {r["short_id"]: r["arc"] for r in rows if r["arc"]}


def _owner_groups(items: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Group already-ordered items by owner, preserving each item's own relative order —
    an unowned item groups under the literal 'unowned' label (never silently dropped)."""
    groups: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        groups.setdefault(item.get("owner") or "unowned", []).append(item)
    return groups


def _status_groups(
    items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Group already-ordered items by status (fixed order), each status broken down by owner."""
    by_status: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        by_status.setdefault(item["status"], []).append(item)
    return [
        {"status": s, "owners": [
            {"owner": o, "threads": ts} for o, ts in _owner_groups(by_status[s]).items()]}
        for s in _STATUS_ORDER if s in by_status
    ]


async def roadmap(
    pool: asyncpg.Pool, project: str, *, me: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    """Every Thread/obligation for `project`, open and recently-closed, grouped
    arc→status→owner. `me` is the ranking identity set (an agent id + its project, or
    `{'operator'}`) — the same contract `rank_open_threads` already takes; it orders the
    'open' status group obligations-first, then whose move it is. `resolved`/`retracted`
    groups order by recency only (no whose-move question once a thread is closed). A
    thread with no declared `arc` buckets under "unsorted", always last. Refuses on an
    unknown project rather than a silent empty result."""
    from src.orchestrator.compositions import open_thread_wall, rank_open_threads

    proj = await pool.fetchval(
        "SELECT id FROM objects WHERE type='SoftwareProject' AND canonical=$1",
        f"repo:{project}")
    if proj is None:
        return {"error": f"no such project: {project!r}"}

    wall, _echoes = await open_thread_wall(pool, proj)
    open_ranked, _more = rank_open_threads(wall, me)
    arcs_by_id = await _arc_map(pool, [t["id"] for t in open_ranked])
    done = await _done_threads(pool, proj)

    all_items: list[dict[str, Any]] = [
        {**t, "status": "open", "arc": arcs_by_id.get(t["id"], _UNSORTED)}
        for t in open_ranked
    ]
    for r in done:
        all_items.append({
            "id": str(r["id"])[:8], "summary": r["summary"], "owner": r["owner"],
            "status": r["status"] or "open", "arc": r["arc"] or _UNSORTED,
            **({"kind": r["kind"]} if r["kind"] else {}),
        })

    by_arc: dict[str, list[dict[str, Any]]] = {}
    for item in all_items:
        by_arc.setdefault(item["arc"], []).append(item)

    return {
        "project": project,
        "arcs": [
            {"arc": a, "statuses": _status_groups(by_arc[a])}
            for a in _ARC_ORDER if a in by_arc
        ],
        "note": "v2: arc→status→owner (thread 8df8e611) — 'unsorted' is every thread minted "
                "before arc existed, or any future omission; never guessed, never dropped",
    }
