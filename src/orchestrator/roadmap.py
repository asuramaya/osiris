"""roadmap — arc/owner grouping helpers for the "open" half of the roadmap composition
(ruling c5b184cd, thread d56e7073/#44: the composition-abstraction READ half).

The bespoke `roadmap(project)` read-model that used to live here RETIRED when the roadmap
screen migrated to a real composition (`compositions.ROADMAP`, `/roadmap` route) — the
resolved/retracted half is now a pure `group`-by-arc-then-owner op-tree, needing no Python
at all. The OPEN half stays Python because it needs `open_thread_wall`'s echo-filter (real
evidence-provenance domain logic no `select` can express) and Functions are leaves in this
architecture (their output can't feed a further `group`), so the arc/owner nesting for the
open bucket happens here too — see `compositions._fn_roadmap_open`, this module's one
caller. Kept as its own small module (one-verb-one-module) rather than folded into
compositions.py itself, matching `discrepancy.py`'s own precedent for a Function's private
helpers."""
from __future__ import annotations

from typing import Any


async def _arc_map(pool: Any, short_ids: list[str]) -> dict[str, str]:
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
