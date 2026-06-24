"""Evidence-class taxonomy — the single source of truth for confidence.

Before this module, every parser invented its own confidence number (0.4 → 0.99)
with no shared meaning, so "noise" was baked into the graph as fake-precise facts
and nothing downstream could reason about *why* a node was believed. Here we grade
each fact by HOW it was obtained, and derive its confidence from that class. Parsers
stop writing numbers; they declare an `EvidenceClass`. The class is persisted (on the
assertion / link) so the crawl frontier and the subject report can read it.

CORROBORATED is never assigned by a parser — it is computed at read time when ≥2
independent sources agree (see `orchestrator/frontier.py`). Storing it would go stale
the moment a third source lands.
"""

from __future__ import annotations

from typing import Any

from src.parsers.base import EvidenceClass, LinkSpec, ObjectSpec, TargetRef

# Base confidence each class projects onto its assertion/link. The CHECK(0..1)
# constraint, the ER scorers, and the report views still read `confidence`, so it
# stays a real column — but it is now a *projection* of the class, not a guess.
BASE_CONFIDENCE: dict[EvidenceClass, float] = {
    EvidenceClass.SELF_DECLARED: 0.9,
    EvidenceClass.AUTHORITATIVE_API: 0.85,
    EvidenceClass.CORROBORATED: 0.8,
    EvidenceClass.DIRECT_OBSERVATION: 0.6,
    EvidenceClass.DERIVED: 0.4,
    EvidenceClass.CO_OCCURRENCE: 0.35,
}

# Strength order for "what is the strongest reason this node is here" (frontier).
# Higher = stronger. CORROBORATED ranks above a single authoritative source.
_STRENGTH: dict[EvidenceClass, int] = {
    EvidenceClass.CO_OCCURRENCE: 0,
    EvidenceClass.DERIVED: 1,
    EvidenceClass.DIRECT_OBSERVATION: 2,
    EvidenceClass.AUTHORITATIVE_API: 3,
    EvidenceClass.SELF_DECLARED: 4,
    EvidenceClass.CORROBORATED: 5,
}

# A parser-assigned class in this set is too weak, on its own, to justify expanding
# the node it lands on (the frontier keeps such nodes as leaves until corroborated).
SPECULATIVE: frozenset[EvidenceClass] = frozenset(
    {EvidenceClass.CO_OCCURRENCE, EvidenceClass.DERIVED}
)

# Anchor-grade: strong enough on its own to make a node expandable.
ANCHOR_GRADE: frozenset[EvidenceClass] = frozenset(
    {EvidenceClass.SELF_DECLARED, EvidenceClass.AUTHORITATIVE_API}
)


def confidence_for(ec: EvidenceClass) -> float:
    return BASE_CONFIDENCE[ec]


def strength(ec: EvidenceClass) -> int:
    return _STRENGTH[ec]


def is_speculative(ec: EvidenceClass) -> bool:
    return ec in SPECULATIVE


def is_anchor_grade(ec: EvidenceClass) -> bool:
    return ec in ANCHOR_GRADE


def emit(
    type: str,
    canonical: str,
    evidence_class: EvidenceClass,
    *,
    properties: dict[str, Any] | None = None,
    property_classes: dict[str, EvidenceClass] | None = None,
    evidence: dict[str, Any] | None = None,
) -> ObjectSpec:
    """Build an ObjectSpec whose confidence is derived from `evidence_class`.

    Most objects are one coherent thing, so all their properties inherit the
    object's class. `property_classes` is the escape hatch when a single object
    carries facts of differing provenance (the runner grades each property by its
    own class, falling back to the object default)."""
    return ObjectSpec(
        type=type,
        canonical=canonical,
        properties=properties or {},
        confidence=confidence_for(evidence_class),
        evidence_class=evidence_class,
        property_classes=property_classes or {},
        evidence=evidence,
    )


def link(
    from_ref: TargetRef,
    to_ref: TargetRef,
    type: str,
    evidence_class: EvidenceClass,
) -> LinkSpec:
    """Build a LinkSpec whose confidence is derived from `evidence_class`. The
    link's class is the frontier's primary signal ('why is this node here?')."""
    return LinkSpec(
        from_ref=from_ref,
        to_ref=to_ref,
        type=type,
        confidence=confidence_for(evidence_class),
        evidence_class=evidence_class,
    )
