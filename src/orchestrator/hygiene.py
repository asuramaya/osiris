"""Pattern hygiene (DESIGN §11) — keep the pattern layer from becoming a graveyard.

Two cheap rules over the objects windowed helpers emit:
  * promote a Campaign from draft once it has >= N ObservedData edges (until then
    it's unpublished/hidden — flagged via a 'lifecycle' assertion, not a status);
  * archive a pattern whose newest ObservedData edge is older than a max age
    (its activity window has gone cold).
Both run after a window tick (or on a cron). Archival is an event-sourced status
change so snapshots stay correct.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from src.actions.core import Actions

_PATTERN_TYPES = ("Campaign", "AttackPattern", "IntrusionSet", "ThreatActor")


async def promote_campaigns(actions: Actions, *, min_observed: int = 3) -> int:
    """Publish draft Campaigns that have accumulated enough evidence. Returns
    the number newly published."""
    rows = await actions.pool.fetch(
        "SELECT c.id FROM objects c "
        "WHERE c.type='Campaign' AND c.status='active' "
        "  AND (SELECT count(*) FROM links l JOIN objects o ON o.id=l.to_id "
        "       WHERE l.from_id=c.id AND l.type='has_observation' AND o.type='ObservedData') "
        "      >= $1 "
        "  AND NOT EXISTS (SELECT 1 FROM current_assertions a "
        "       WHERE a.object_id=c.id AND a.name='lifecycle' AND a.value #>> '{}' = 'published')",
        min_observed,
    )
    now = datetime.now(UTC)
    for r in rows:
        await actions.assert_property(
            r["id"], "lifecycle", "published", "hygiene", now, 1.0
        )
    return len(rows)


async def archive_stale_patterns(
    actions: Actions, *, now: datetime, max_age: timedelta
) -> int:
    """Archive patterns whose newest ObservedData edge is older than max_age."""
    cutoff = now - max_age
    rows = await actions.pool.fetch(
        "SELECT p.id FROM objects p "
        "WHERE p.type = ANY($1::text[]) AND p.status='active' "
        "  AND EXISTS (SELECT 1 FROM links l WHERE l.from_id=p.id AND l.type='has_observation') "
        "  AND (SELECT max(l.last_seen) FROM links l WHERE l.from_id=p.id "
        "       AND l.type='has_observation') < $2",
        list(_PATTERN_TYPES),
        cutoff,
    )
    for r in rows:
        await actions.set_status(r["id"], "archived", "stale: no recent observations", "hygiene")
    return len(rows)
