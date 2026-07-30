"""Entity dossier — the 'who is this?' read model for a FEDERATED entity.

`frontier.subject_report` answers 'who is this?' for a crawled *footprint*: it
buckets identity fragments by confidence tier (verified / corroborated / speculative).
That lens is wrong for an entity ingested from an open base (OpenSanctions, EDGAR,
Wikidata): every fact is AUTHORITATIVE_API, so the tiers collapse and the substance —
the ownership / family / director *network* — never surfaces.

This is the complementary read model: given an object, return its identity
properties (multi-source aware) plus its relationships grouped by direction and type,
each endpoint NAMED. It's what the graph view renders as a node's neighborhood, but as
structured data the dossier panel and the brief can consume directly.
"""

from __future__ import annotations

import uuid
from typing import Any

import asyncpg

from src.ontology.labels import LABEL_CHAIN, resolve_label

# Identity bookkeeping links (merge plumbing) are not part of the entity's network.
_HIDDEN_LINK_TYPES = ("same_as", "not_same_as")


async def _label_props(
    pool: asyncpg.Pool, ids: list[uuid.UUID]
) -> dict[uuid.UUID, dict[str, Any]]:
    """Winning value per LABEL_CHAIN property, batched across `ids` in one query — the
    same shape compositions.py's `_attach_labels` uses, so a dossier with many
    neighbors costs one extra query, not N."""
    if not ids:
        return {}
    rows = await pool.fetch(
        "SELECT DISTINCT ON (object_id, name) object_id, name, value #>> '{}' AS v "
        "FROM current_assertions WHERE object_id = ANY($1::uuid[]) AND name = ANY($2::text[]) "
        "ORDER BY object_id, name, confidence DESC NULLS LAST, observed_at DESC",
        ids, list(LABEL_CHAIN))
    out: dict[uuid.UUID, dict[str, Any]] = {}
    for r in rows:
        out.setdefault(r["object_id"], {})[r["name"]] = r["v"]
    return out


async def entity_dossier(pool: asyncpg.Pool, object_id: uuid.UUID) -> dict[str, Any]:
    """Identity properties + named relationship network for one entity. Returns {}
    if the object does not exist (the endpoint maps that to 404).

    Task #97 workstream 3 (ruling 52daab71): both this entity's own `name` and every
    neighbor's name used to check ONLY the `name` property — an entity/neighbor whose
    real identity lives in title/summary/statement/surface/handle (a Practice, a
    BlindSpot, an unclaimed Agent) rendered its raw canonical hash here even though the
    graph/table views of the SAME object already resolved it correctly. Both now share
    `resolve_label`, the one canonical answer every other consumer uses."""
    obj = await pool.fetchrow(
        "SELECT id, type, canonical, status FROM objects WHERE id=$1", object_id
    )
    if obj is None:
        return {}

    own_props = (await _label_props(pool, [object_id])).get(object_id, {})
    name = resolve_label(obj["type"], own_props, obj["canonical"]).label

    # properties as the multi-source set: one entry per property name, carrying each
    # source's value + how it was obtained (evidence_class) + confidence.
    prop_rows = await pool.fetch(
        "SELECT name, value #>> '{}' AS value, source_id, evidence_class, confidence "
        "FROM current_assertions "
        "WHERE object_id=$1 AND name NOT IN ('name','tag') "
        "ORDER BY name, confidence DESC NULLS LAST",
        object_id,
    )
    properties: dict[str, dict[str, Any]] = {}
    for r in prop_rows:
        entry = properties.setdefault(r["name"], {"name": r["name"], "values": []})
        entry["values"].append({
            "value": r["value"],
            "source": r["source_id"],
            "evidence_class": r["evidence_class"],
            "confidence": r["confidence"],
        })

    # relationships, both directions, neighbor labelled and typed. Repeated edges
    # (same direction, type, neighbor) are collapsed: a duplicated link carries no
    # extra information.
    seen: set[tuple[str, str, uuid.UUID]] = set()
    raw_rels: list[dict[str, Any]] = []
    nbr_ids: list[uuid.UUID] = []
    for direction, end, other in (("out", "from_id", "to_id"), ("in", "to_id", "from_id")):
        rows = await pool.fetch(
            f"SELECT l.type, l.{other} AS nbr, l.evidence_class, l.source_id, "
            f"       n.type AS nbr_type, n.canonical AS nbr_canon "
            f"FROM links l JOIN objects n ON n.id=l.{other} "
            f"WHERE l.{end}=$1 AND l.type <> ALL($2::text[])",
            object_id,
            list(_HIDDEN_LINK_TYPES),
        )
        for r in rows:
            key = (direction, r["type"], r["nbr"])
            if key in seen:
                continue
            seen.add(key)
            nbr_ids.append(r["nbr"])
            raw_rels.append({"direction": direction, "type": r["type"], "nbr": r["nbr"],
                             "nbr_type": r["nbr_type"], "nbr_canon": r["nbr_canon"],
                             "evidence_class": r["evidence_class"], "source": r["source_id"]})

    nbr_props = await _label_props(pool, nbr_ids)
    rels = [
        {
            "direction": r["direction"],
            "type": r["type"],
            "neighbor": {
                "id": str(r["nbr"]),
                "name": resolve_label(r["nbr_type"], nbr_props.get(r["nbr"], {}),
                                      r["nbr_canon"]).label,
                "type": r["nbr_type"],
            },
            "evidence_class": r["evidence_class"],
            "source": r["source"],
        }
        for r in raw_rels
    ]

    return {
        "id": str(object_id),
        "type": obj["type"],
        "canonical": obj["canonical"],
        "status": obj["status"],
        "name": name,
        "properties": list(properties.values()),
        "relationships": rels,
    }
