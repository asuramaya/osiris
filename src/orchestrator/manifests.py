"""Helper manifests: load YAML, validate, and project the trigger table.

Triggers are a *pure projection* of manifests: never hand-edited.
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


class Template(BaseModel):
    url: str | None = None          # e.g. "https://t.me/s/{object.canonical}"
    method: str = "GET"
    body: str | None = None


class Manifest(BaseModel):
    id: str
    name: str
    description: str = ""
    consumes: Consumes
    emits: list[Emit] = Field(default_factory=list)
    tier: Literal["open", "fragile", "gated", "manual", "suggest"] = "open"
    origin: str = "multi"
    rate: Rate = Field(default_factory=Rate)
    parser: str
    template: Template | None = None
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
    Returns the number of triggers written.

    NO ACCESS EXCLUSIVE LOCK (from a live incident): this used to
    `TRUNCATE triggers RESTART IDENTITY`, which needs ACCESS EXCLUSIVE, and a concurrent
    pg_dump's own (much weaker) lock on this table was enough to block that TRUNCATE,
    which in turn blocked console startup (this runs in the ASGI lifespan) behind a
    backup that had nothing to do with it. DELETE + upsert-by-`helper_id` (migration
    0068's own unique constraint) needs only ROW EXCLUSIVE, which a pg_dump's read
    never contests. `enabled` is preserved exactly as before: an UPDATE (an existing
    helper_id) never touches that column at all, so the analyst's own flag survives;
    a fresh INSERT (a helper_id absent from the table right now) uses the manifest's
    own default, same as a helper reappearing after removal always did. `SET LOCAL
    lock_timeout` is the last line of defense: some OTHER exclusive-lock holder this
    function doesn't anticipate must never hang the ASGI app forever; the caller (the
    lifespan startup path) catches the resulting `LockNotAvailableError` and leaves the
    previous projection in place rather than failing to bind."""
    async with pool.acquire() as conn, conn.transaction():
        await conn.execute("SET LOCAL lock_timeout = '2s'")
        ids = list(manifests.keys())
        await conn.execute(
            "DELETE FROM triggers WHERE NOT (helper_id = ANY($1::text[]))", ids)
        n = 0
        for manifest in manifests.values():
            on_event, match = _trigger_for(manifest)
            await conn.execute(
                "INSERT INTO triggers (on_event, match, helper_id, enabled) "
                "VALUES ($1,$2,$3,$4) "
                "ON CONFLICT (helper_id) DO UPDATE SET "
                "  on_event=excluded.on_event, match=excluded.match",
                on_event,
                match,
                manifest.id,
                manifest.enabled,
            )
            n += 1
        return n
