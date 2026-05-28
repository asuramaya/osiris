"""Intake front door (DESIGN §6 on_input, minus classify).

Canonicalizes a raw observable for its type, finds-or-creates the object, and
preserves the as-observed value as an assertion (canonicalization is lossy).
Deterministic types auto-merge here: two raw forms with the same canonical key
resolve to one object via the UNIQUE(type, canonical) constraint.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from src.actions.core import Actions
from src.ontology.canonicalize import canonicalize


async def intake(
    actions: Actions,
    type_: str,
    raw: str,
    actor: str,
    case_id: uuid.UUID | None = None,
    *,
    hop_distance: int = 0,
) -> uuid.UUID:
    canonical = canonicalize(type_, raw)
    object_id = await actions.create_or_find_object(
        type_, canonical, actor, case_id, hop_distance=hop_distance
    )
    if raw != canonical:
        # keep the original form as evidence of how it was seen
        await actions.assert_property(
            object_id, "observed_value", raw, actor, datetime.now(UTC), 1.0, case_id=case_id
        )
    return object_id
