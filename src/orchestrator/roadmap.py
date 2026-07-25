"""roadmap(project) — open Threads/obligations AND their resolved/retracted history, one flat
tracker grouped status→owner (thread 521ae613a6f4 / d56e7073, the composition-renderer
readiness call; Thoth's go, msg 1227: v1 status→owner only, NO arc — no Thread has ever
carried one, and it's the clean v2 once `open_thread()` grows the property and a backfill
tags the existing 263, tracked on thread 8df8e611).

COMPOSES rather than reinvents the open half: `open_thread_wall` (compositions.py) already
gives the real, echo-filtered wall for a project — an obligation mechanically inferred but
never touched by a mind is excluded (ruling 61c1b20d, "a guess does not get a week"), and its
unfiled-owner fallback (thread 4ffe0eb9, a thread opened without `repo=` whose OWNER still
names this project) rides along for free. `rank_open_threads` orders that wall
obligations-first, then by whose move it is (mine-to-act / another mind's / waiting-on-the-
human) — reused verbatim for the open bucket's internal order; that ranking IS "orient's
ranking" this verb was asked to reuse.

The RESOLVED/RETRACTED half is new here — orient and the wall are open-only by design, and a
roadmap that only ever shows what's still outstanding isn't tracking anything. No
echo-filtering is needed for it: resolving or retracting a thread is itself a self_declared
act (`capture.py`/`dispose.py`), so an untouched resolved thread cannot exist by construction.
Deliberately simpler than the open half, flagged not hidden: it does NOT replicate the
unfiled-owner fallback (a thread closed without ever having been filed to this project by
`repo=` or by its owner's project — a narrow edge the open wall's own history shows is rare).

Purely a READ over whatever the graph already holds — mints nothing, asserts nothing."""
from __future__ import annotations

from typing import Any

import asyncpg

_STATUS_ORDER = ("open", "resolved", "retracted")
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


def _owner_groups(items: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Group already-ordered items by owner, preserving each item's own relative order —
    an unowned item groups under the literal 'unowned' label (never silently dropped)."""
    groups: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        groups.setdefault(item.get("owner") or "unowned", []).append(item)
    return groups


async def roadmap(
    pool: asyncpg.Pool, project: str, *, me: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    """Every Thread/obligation for `project`, open and recently-closed, grouped status→owner.
    `me` is the ranking identity set (an agent id + its project, or `{'operator'}`) — the same
    contract `rank_open_threads` already takes; it orders the 'open' status group
    obligations-first, then whose move it is. `resolved`/`retracted` groups order by recency
    only (no whose-move question once a thread is closed). Refuses on an unknown project
    rather than a silent empty result."""
    from src.orchestrator.compositions import open_thread_wall, rank_open_threads

    proj = await pool.fetchval(
        "SELECT id FROM objects WHERE type='SoftwareProject' AND canonical=$1",
        f"repo:{project}")
    if proj is None:
        return {"error": f"no such project: {project!r}"}

    wall, _echoes = await open_thread_wall(pool, proj)
    open_ranked, _more = rank_open_threads(wall, me)
    done = await _done_threads(pool, proj)

    by_status: dict[str, dict[str, list[dict[str, Any]]]] = {}
    if open_ranked:
        by_status["open"] = _owner_groups(open_ranked)
    for status in ("resolved", "retracted"):
        rows = [
            {"id": str(r["id"])[:8], "summary": r["summary"], "owner": r["owner"],
             **({"kind": r["kind"]} if r["kind"] else {})}
            for r in done if (r["status"] or "open") == status
        ]
        if rows:
            by_status[status] = _owner_groups(rows)

    return {
        "project": project,
        "statuses": [
            {"status": s, "owners": [
                {"owner": o, "threads": items} for o, items in by_status[s].items()]}
            for s in _STATUS_ORDER if s in by_status
        ],
        "note": "v1: status→owner only, no `arc` yet (thread 8df8e611) — no Thread has ever "
                "carried one; add it to open_thread() once ready, then re-scope this verb",
    }
