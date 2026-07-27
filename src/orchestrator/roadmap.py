"""roadmap — the arc lookup for the "open" half of the roadmap composition (ruling
c5b184cd, thread d56e7073/#44: the composition-abstraction READ half).

The bespoke `roadmap(project)` read-model that used to live here RETIRED when the roadmap
screen migrated to a real composition (`compositions.ROADMAP`, `/roadmap` route). All three
sections — open, resolved, retracted — are now a pure `group`-by-arc-then-owner op-tree
(task #60, the function-output-re-entering-the-op-tree follow-on, let the open bucket join
resolved/retracted's shape once a Function's output could feed a further `group`). The OPEN
bucket still needs one piece of real Python: `open_thread_wall`'s echo-filter (evidence-
provenance domain logic no `select` can express) and the arc lookup below, both called from
`compositions._fn_roadmap_open`, this module's one remaining caller. Kept as its own small
module (one-verb-one-module) rather than folded into compositions.py itself, matching
`discrepancy.py`'s own precedent for a Function's private helpers."""
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
