"""Trigger matching — the read side of the manifest projection.

Given an emitted event and the object it concerns, return the helper ids whose
projected triggers fire. Pure DB read; the durable outbox relay + Arq dispatch
that actually *runs* matched helpers (and budget gating) arrive in Phase 3.
"""

from __future__ import annotations

from typing import Any

import asyncpg


async def matching_helpers(
    pool: asyncpg.Pool,
    event: str,
    object_type: str,
    object_properties: set[str] | None = None,
) -> list[str]:
    object_properties = object_properties or set()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT helper_id, match FROM triggers "
            "WHERE enabled AND on_event = $1 AND match->>'type' = $2",
            event,
            object_type,
        )
    out: list[str] = []
    for row in rows:
        match: dict[str, Any] = row["match"]
        required = set(match.get("requires_properties", []))
        if required <= object_properties:
            out.append(row["helper_id"])
    return out
