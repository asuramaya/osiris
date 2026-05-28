"""Export ontology objects as a STIX 2.1 bundle.

Read-only (no mutation, so it queries the pool directly). Reconstructs each SDO
from its `stix_meta` property overlaid with the promoted fields, and re-emits
links among the exported set as STIX relationship (SRO) objects. Custom non-STIX
object types are wrapped as `observed-data` per the day-one ruling.
"""

from __future__ import annotations

import uuid
from typing import Any

import asyncpg

from src.ontology import stix


async def subgraph_from(
    pool: asyncpg.Pool, root_id: uuid.UUID, *, hops: int = 1
) -> set[uuid.UUID]:
    """BFS over links out to `hops` — the basis of an entity 'dossier' export."""
    seen: set[uuid.UUID] = {root_id}
    frontier: set[uuid.UUID] = {root_id}
    async with pool.acquire() as conn:
        for _ in range(hops):
            if not frontier:
                break
            rows = await conn.fetch(
                "SELECT to_id AS n FROM links WHERE from_id = ANY($1::uuid[]) "
                "UNION SELECT from_id AS n FROM links WHERE to_id = ANY($1::uuid[])",
                list(frontier),
            )
            nxt = {r["n"] for r in rows} - seen
            seen |= nxt
            frontier = nxt
    return seen


async def _current_props(conn: Any, object_id: uuid.UUID) -> dict[str, Any]:
    """Highest-confidence current value per property name (point-of-use selection)."""
    rows = await conn.fetch(
        "SELECT DISTINCT ON (name) name, value FROM current_assertions "
        "WHERE object_id = $1 ORDER BY name, confidence DESC, observed_at DESC",
        object_id,
    )
    return {r["name"]: r["value"] for r in rows}


def _build_sdo(stix_type: str, stix_id: str, props: dict[str, Any]) -> dict[str, Any]:
    meta = props.get("stix_meta") or {}
    sdo: dict[str, Any] = {
        "type": stix_type,
        "spec_version": meta.get("spec_version", "2.1"),
        "id": stix_id,
    }
    sdo.update({k: meta[k] for k in stix.META_FIELDS if k in meta})
    for fname in stix.PROMOTED_FIELDS:
        if fname in props:
            sdo[fname] = props[fname]
    return sdo


async def export_objects(pool: asyncpg.Pool, object_ids: set[uuid.UUID]) -> dict[str, Any]:
    """Build a STIX 2.1 bundle from the given objects plus the links among them."""
    objects_out: list[dict[str, Any]] = []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, type, canonical, status FROM objects WHERE id = ANY($1::uuid[])",
            list(object_ids),
        )
        for row in rows:
            if row["status"] == "merged":
                continue  # merged-away objects resolve to their winner; don't double-emit
            props = await _current_props(conn, row["id"])
            stix_type = stix.OBJECT_TYPE_TO_STIX.get(row["type"])
            if stix_type is not None and str(row["canonical"]).startswith(f"{stix_type}--"):
                objects_out.append(_build_sdo(stix_type, row["canonical"], props))
            else:
                # custom/non-STIX object -> wrap as observed-data
                objects_out.append(_wrap_observed_data(row, props))

        # relationships among the exported set only
        link_rows = await conn.fetch(
            "SELECT l.from_id, l.to_id, l.type, l.properties, "
            "       f.canonical AS src, t.canonical AS tgt "
            "FROM links l "
            "JOIN objects f ON f.id = l.from_id "
            "JOIN objects t ON t.id = l.to_id "
            "WHERE l.from_id = ANY($1::uuid[]) AND l.to_id = ANY($1::uuid[]) "
            "  AND l.type <> 'same_as'",
            list(object_ids),
        )
        for lr in link_rows:
            rel_id = (lr["properties"] or {}).get("stix_id") or f"relationship--{uuid.uuid4()}"
            objects_out.append(
                {
                    "type": "relationship",
                    "spec_version": "2.1",
                    "id": rel_id,
                    "relationship_type": lr["type"],
                    "source_ref": lr["src"],
                    "target_ref": lr["tgt"],
                }
            )

    return {"type": "bundle", "id": f"bundle--{uuid.uuid4()}", "objects": objects_out}


def _wrap_observed_data(row: Any, props: dict[str, Any]) -> dict[str, Any]:
    """Custom object types (Vehicle/Vessel/TelegramChannel/...) -> STIX observed-data."""
    meta = props.get("stix_meta") or {}
    return {
        "type": "observed-data",
        "spec_version": "2.1",
        "id": f"observed-data--{uuid.uuid4()}",
        "created": meta.get("created"),
        "modified": meta.get("modified"),
        "x_osiris_type": row["type"],
        "x_osiris_canonical": row["canonical"],
        "x_osiris_properties": {k: v for k, v in props.items() if k != "stix_meta"},
    }
