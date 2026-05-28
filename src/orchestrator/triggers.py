"""Trigger matching — the read side of the manifest projection.

Given an emitted event and the object it concerns, return the helper ids whose
projected triggers fire. Pure DB read; the durable outbox relay + Arq dispatch
that actually *runs* matched helpers (and budget gating) arrive in Phase 3.
"""

from __future__ import annotations

import uuid
from typing import Any

import asyncpg


async def matching_helpers(
    pool: asyncpg.Pool,
    event: str,
    object_type: str,
    object_properties: set[str] | None = None,
    *,
    case_id: uuid.UUID | None = None,
) -> list[str]:
    """Helpers whose projected trigger fires for this event+object. A case can
    veto a helper via cases.trigger_overrides = {helper_id: false} (#5 per-case
    enable/disable) without touching the global manifest projection."""
    object_properties = object_properties or set()
    overrides: dict[str, bool] = {}
    async with pool.acquire() as conn:
        if case_id is not None:
            overrides = await conn.fetchval(
                "SELECT trigger_overrides FROM cases WHERE id=$1", case_id
            ) or {}
        rows = await conn.fetch(
            "SELECT helper_id, match FROM triggers "
            "WHERE enabled AND on_event = $1 AND match->>'type' = $2",
            event,
            object_type,
        )
    out: list[str] = []
    for row in rows:
        if overrides.get(row["helper_id"]) is False:
            continue  # explicitly disabled for this case
        match: dict[str, Any] = row["match"]
        required = set(match.get("requires_properties", []))
        if required <= object_properties:
            out.append(row["helper_id"])
    return out
