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

import uuid
from datetime import UTC, datetime, timedelta

from src.actions.core import Actions
from src.ontology.canonicalize import canonicalize
from src.ontology.resolution import (
    ensure_person_hub,
    find_footprint_merge_candidates,
    find_person_merge_candidates,
)

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


async def converge_identities(
    actions: Actions, *, case_id: uuid.UUID | None = None
) -> dict[str, int]:
    """Footprint identity convergence (idempotent, safe to re-run): queue merge
    candidates for the review tray, and assemble Person hubs from STRONG signals
    (an Account's listed email matching a known Email; Accounts sharing a rel=me
    source page). Scoped to one case's objects when case_id is given. Never
    auto-merges Persons (ruling #3). Returns {"candidates": n, "hubs": m}."""
    pool = actions.pool
    queued = await find_footprint_merge_candidates(pool)
    queued += await find_person_merge_candidates(pool)

    case_objs: set[uuid.UUID] | None = None
    if case_id is not None:
        case_objs = {
            r["object_id"]
            for r in await pool.fetch(
                "SELECT object_id FROM case_objects WHERE case_id=$1", case_id
            )
        }

    def in_scope(object_id: uuid.UUID) -> bool:
        return case_objs is None or object_id in case_objs

    hubs = 0
    # hub from bio-email: an Account whose listed email is a known Email object
    for r in await pool.fetch(
        "SELECT a.object_id AS account_id, a.value #>> '{}' AS email "
        "FROM current_assertions a "
        "JOIN objects o ON o.id=a.object_id AND o.type='Account' "
        "WHERE a.name='email'"
    ):
        if not in_scope(r["account_id"]) or not r["email"]:
            continue
        canon = canonicalize("Email", r["email"])
        email_id = await pool.fetchval(
            "SELECT id FROM objects WHERE type='Email' AND canonical=$1", canon
        )
        if email_id is None:
            continue
        await ensure_person_hub(
            actions, key=canon, account_ids=[r["account_id"]],
            email_value=canon, email_id=email_id, case_id=case_id,
        )
        hubs += 1

    # hub from rel=me: Accounts that are rel=me targets of the same page are one identity
    by_src: dict[tuple[uuid.UUID, str], list[uuid.UUID]] = {}
    for r in await pool.fetch(
        "SELECT l.from_id AS src, s.canonical AS src_canon, l.to_id AS account_id "
        "FROM links l "
        "JOIN objects o ON o.id=l.to_id AND o.type='Account' "
        "JOIN objects s ON s.id=l.from_id "
        "WHERE l.type='rel_me'"
    ):
        if not in_scope(r["account_id"]):
            continue
        by_src.setdefault((r["src"], r["src_canon"]), []).append(r["account_id"])
    for (_src, src_canon), accts in by_src.items():
        await ensure_person_hub(actions, key=src_canon, account_ids=accts, case_id=case_id)
        hubs += 1

    return {"candidates": queued, "hubs": hubs}


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
