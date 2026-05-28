"""Ingest a STIX 2.1 bundle into the ontology via the Actions layer.

Two passes: SDOs first (build a stix_id -> object_id map), then relationships
(resolve source/target refs against that map). All mutation goes through Actions
so every object/property/link is audited and cascades through the outbox — the
ATT&CK seed is just another source asserting facts.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from src.actions.core import Actions
from src.ontology import stix

# ATT&CK reference data is authoritative, not a probabilistic OSINT finding.
_CONFIDENCE = 1.0


@dataclass
class IngestReport:
    objects: int = 0
    links: int = 0
    skipped: int = 0
    dangling_refs: int = 0
    stix_id_to_object: dict[str, uuid.UUID] = field(default_factory=dict)


def _observed_at(stix_obj: dict[str, Any]) -> datetime:
    """Authoritative clock = the STIX `modified` (fallback `created`)."""
    raw = stix_obj.get("modified") or stix_obj.get("created")
    if not raw:
        return datetime.now(UTC)
    # STIX timestamps are RFC3339 with a trailing Z.
    return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))


async def ingest_bundle(
    actions: Actions,
    bundle: dict[str, Any],
    *,
    case_id: uuid.UUID | None = None,
    source_id: str = stix.SOURCE_ID,
) -> IngestReport:
    report = IngestReport()
    objects = bundle.get("objects", [])
    relationships: list[dict[str, Any]] = []

    # --- pass 1: SDOs ---------------------------------------------------
    for obj in objects:
        stix_type = obj.get("type")
        if stix_type == "relationship":
            relationships.append(obj)
            continue
        if stix_type in stix.SKIP_STIX_TYPES:
            report.skipped += 1
            continue

        stix_id = obj["id"]
        obj_type = stix.object_type_for(stix_type)
        observed_at = _observed_at(obj)

        object_id = await actions.create_or_find_object(
            obj_type, stix_id, source_id, case_id
        )
        report.stix_id_to_object[stix_id] = object_id
        report.objects += 1

        for fname in stix.PROMOTED_FIELDS:
            if fname in obj:
                await actions.assert_property(
                    object_id, fname, obj[fname], source_id, observed_at, _CONFIDENCE,
                    case_id=case_id,
                )
        ext_id = stix.mitre_external_id(obj)
        if ext_id is not None:
            await actions.assert_property(
                object_id, "external_id", ext_id, source_id, observed_at, _CONFIDENCE,
                case_id=case_id,
            )
        meta = {k: obj[k] for k in stix.META_FIELDS if k in obj}
        await actions.assert_property(
            object_id, "stix_meta", meta, source_id, observed_at, _CONFIDENCE, case_id=case_id
        )

    # --- pass 2: relationships -> links ---------------------------------
    for rel in relationships:
        src = report.stix_id_to_object.get(rel.get("source_ref", ""))
        tgt = report.stix_id_to_object.get(rel.get("target_ref", ""))
        if src is None or tgt is None:
            report.dangling_refs += 1
            continue
        await actions.create_link(
            src,
            tgt,
            rel["relationship_type"],
            source_id,
            _observed_at(rel),
            _CONFIDENCE,
            properties={"stix_id": rel["id"]},
            case_id=case_id,
        )
        report.links += 1

    return report
