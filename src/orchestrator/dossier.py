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

# Identity bookkeeping links (merge plumbing) are not part of the entity's network.
_HIDDEN_LINK_TYPES = ("same_as", "not_same_as")


async def entity_dossier(pool: asyncpg.Pool, object_id: uuid.UUID) -> dict[str, Any]:
    """Identity properties + named relationship network for one entity. Returns {}
    if the object does not exist (the endpoint maps that to 404)."""
    obj = await pool.fetchrow(
        "SELECT id, type, canonical, status FROM objects WHERE id=$1", object_id
    )
    if obj is None:
        return {}

    name = await pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions "
        "WHERE object_id=$1 AND name='name' "
        "ORDER BY confidence DESC NULLS LAST, observed_at DESC LIMIT 1",
        object_id,
    )

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

    # relationships, both directions, neighbor labelled (name, else canonical — the
    # same fallback the graph view uses) and typed. Repeated edges (same direction,
    # type, neighbor) are collapsed: a duplicated link carries no extra information.
    _name = (
        "(SELECT value #>> '{}' FROM current_assertions a "
        " WHERE a.object_id=n.id AND a.name='name' "
        " ORDER BY confidence DESC NULLS LAST, observed_at DESC LIMIT 1)"
    )
    seen: set[tuple[str, str, uuid.UUID]] = set()
    rels: list[dict[str, Any]] = []
    for direction, end, other in (("out", "from_id", "to_id"), ("in", "to_id", "from_id")):
        rows = await pool.fetch(
            f"SELECT l.type, l.{other} AS nbr, l.evidence_class, l.source_id, "
            f"       n.type AS nbr_type, n.canonical AS nbr_canon, {_name} AS nbr_name "
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
            rels.append({
                "direction": direction,
                "type": r["type"],
                "neighbor": {
                    "id": str(r["nbr"]),
                    "name": r["nbr_name"] or r["nbr_canon"],
                    "type": r["nbr_type"],
                },
                "evidence_class": r["evidence_class"],
                "source": r["source_id"],
            })

    return {
        "id": str(object_id),
        "type": obj["type"],
        "canonical": obj["canonical"],
        "status": obj["status"],
        "name": name,
        "properties": list(properties.values()),
        "relationships": rels,
    }
