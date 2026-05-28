"""STIX 2.1 <-> ontology mapping.

STIX 2.1 is our seed vocabulary (DESIGN §11/§15). Decisions in force:
  * canonical = the STIX id (deterministic, interop-stable); the MITRE handle
    (G0032, T1566) is an indexed `external_id` property.
  * Pattern SDOs map to first-class object types; STIX relationships (SROs)
    map to links carrying their hyphenated STIX `relationship_type` verbatim.
  * Custom non-STIX objects (Vehicle/Vessel/TelegramChannel) export wrapped as
    `observed-data` — handled in export.py, not here.
"""

from __future__ import annotations

from typing import Any

# STIX SDO `type` -> ontology object type. Unknown STIX types fall through and
# are stored under their raw STIX type (dynamic ontology — no enum to migrate).
STIX_TYPE_TO_OBJECT: dict[str, str] = {
    "attack-pattern": "AttackPattern",
    "intrusion-set": "IntrusionSet",
    "campaign": "Campaign",
    "threat-actor": "ThreatActor",
    "tool": "Tool",
    "malware": "Malware",
    "course-of-action": "CourseOfAction",
    "indicator": "Indicator",
    "observed-data": "ObservedData",
    "identity": "Identity",
    "x-mitre-tactic": "Tactic",
}

OBJECT_TYPE_TO_STIX: dict[str, str] = {v: k for k, v in STIX_TYPE_TO_OBJECT.items()}

# STIX meta/container types that are not ontology objects and carry no graph edges.
SKIP_STIX_TYPES: frozenset[str] = frozenset(
    {"marking-definition", "x-mitre-matrix", "x-mitre-collection"}
)

# Ontology-meaningful SDO fields promoted to individual assertions. Everything
# else needed to reconstruct a faithful bundle rides in the `stix_meta` property.
PROMOTED_FIELDS: tuple[str, ...] = (
    "name",
    "description",
    "aliases",
    "kill_chain_phases",
)

# Fields captured wholesale for faithful round-trip export.
META_FIELDS: tuple[str, ...] = (
    "spec_version",
    "created",
    "modified",
    "created_by_ref",
    "object_marking_refs",
    "external_references",
    "revoked",
    "x_mitre_platforms",
    "x_mitre_version",
    "x_mitre_is_subtechnique",
    "is_family",
)

SOURCE_ID = "mitre-attack"


def object_type_for(stix_type: str) -> str:
    """Map a STIX SDO type to an ontology type, preserving unknowns verbatim."""
    return STIX_TYPE_TO_OBJECT.get(stix_type, stix_type)


def mitre_external_id(stix_obj: dict[str, Any]) -> str | None:
    """The MITRE handle (G0032 / T1566 / S0002) from external_references, if any."""
    for ref in stix_obj.get("external_references", []):
        if ref.get("source_name") == SOURCE_ID and "external_id" in ref:
            return str(ref["external_id"])
    return None
