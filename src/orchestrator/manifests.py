"""Helper manifests: load YAML, validate, and project the trigger table.

Ruling #5: triggers are a *pure projection* of manifests — never hand-edited.
`project_triggers` rebuilds the `triggers` table from the loaded manifest set,
preserving only the analyst's per-trigger `enabled` flag across rebuilds. A
helper that needs no properties triggers on `object_created`; one that requires
properties triggers on `property_added` (gated on those properties existing).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import asyncpg
import yaml
from pydantic import BaseModel, Field


class Consumes(BaseModel):
    type: str
    requires_properties: list[str] = Field(default_factory=list)


class Emit(BaseModel):
    type: str | None = None
    link_type: str | None = None
    confidence_floor: float = 0.0


class Rate(BaseModel):
    per_origin_rps: float = 1.0
    per_origin_concurrent: int = 1
    jitter_ms: tuple[int, int] = (0, 0)


class Manifest(BaseModel):
    id: str
    name: str
    description: str = ""
    consumes: Consumes
    emits: list[Emit] = Field(default_factory=list)
    tier: Literal["open", "fragile", "gated", "manual"] = "open"
    origin: str = "multi"
    rate: Rate = Field(default_factory=Rate)
    parser: str
    cache_ttl: int = 3600
    windowing: dict[str, Any] | None = None
    enabled: bool = True


def load_manifests(directory: str | Path) -> dict[str, Manifest]:
    """Load and validate every helpers/*.yaml into {helper_id: Manifest}."""
    out: dict[str, Manifest] = {}
    for path in sorted(Path(directory).glob("*.yaml")):
        data = yaml.safe_load(path.read_text())
        manifest = Manifest.model_validate(data)
        if manifest.id in out:
            raise ValueError(f"duplicate helper id {manifest.id!r} ({path})")
        out[manifest.id] = manifest
    return out


def _trigger_for(manifest: Manifest) -> tuple[str, dict[str, Any]]:
    """(on_event, match) projected from a manifest's consume signature."""
    if manifest.consumes.requires_properties:
        return "property_added", {
            "type": manifest.consumes.type,
            "requires_properties": manifest.consumes.requires_properties,
        }
    return "object_created", {"type": manifest.consumes.type}


async def project_triggers(pool: asyncpg.Pool, manifests: dict[str, Manifest]) -> int:
    """Rebuild the triggers table from manifests; keep prior `enabled` flags.
    Returns the number of triggers written."""
    async with pool.acquire() as conn, conn.transaction():
        prior = {
            r["helper_id"]: r["enabled"]
            for r in await conn.fetch("SELECT helper_id, enabled FROM triggers")
        }
        await conn.execute("TRUNCATE triggers RESTART IDENTITY")
        n = 0
        for manifest in manifests.values():
            on_event, match = _trigger_for(manifest)
            await conn.execute(
                "INSERT INTO triggers (on_event, match, helper_id, enabled) VALUES ($1,$2,$3,$4)",
                on_event,
                match,
                manifest.id,
                prior.get(manifest.id, manifest.enabled),
            )
            n += 1
        return n
