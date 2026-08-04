"""Evidence-class taxonomy — pure unit tests (no DB)."""
from __future__ import annotations

from src.parsers.base import EvidenceClass, TargetRef
from src.parsers.evidence import (
    BASE_CONFIDENCE,
    confidence_for,
    emit,
    is_anchor_grade,
    is_speculative,
    link,
    strength,
)


def test_every_class_has_a_confidence_and_strength() -> None:
    for ec in EvidenceClass:
        assert 0.0 <= confidence_for(ec) <= 1.0
        assert isinstance(strength(ec), int)
    # the map is exhaustive
    assert set(BASE_CONFIDENCE) == set(EvidenceClass)


def test_confidence_ordering_matches_trust() -> None:
    # self-declared / authoritative outrank observation, which outranks guesses
    assert confidence_for(EvidenceClass.SELF_DECLARED) > confidence_for(
        EvidenceClass.DIRECT_OBSERVATION
    )
    assert confidence_for(EvidenceClass.DIRECT_OBSERVATION) > confidence_for(
        EvidenceClass.CO_OCCURRENCE
    )
    # corroboration is strongest of all (read-time promotion)
    assert strength(EvidenceClass.CORROBORATED) == max(strength(e) for e in EvidenceClass)


def test_speculative_vs_anchor_partition() -> None:
    assert is_speculative(EvidenceClass.CO_OCCURRENCE)
    assert is_speculative(EvidenceClass.DERIVED)
    assert not is_speculative(EvidenceClass.SELF_DECLARED)
    assert is_anchor_grade(EvidenceClass.SELF_DECLARED)
    assert is_anchor_grade(EvidenceClass.AUTHORITATIVE_API)
    # direct observation is neither a guess nor an anchor — it's the middle tier
    assert not is_speculative(EvidenceClass.DIRECT_OBSERVATION)
    assert not is_anchor_grade(EvidenceClass.DIRECT_OBSERVATION)


def test_emit_derives_confidence_from_class() -> None:
    spec = emit(
        "Account",
        "github:asuramaya",
        EvidenceClass.AUTHORITATIVE_API,
        properties={"name": "Priya"},
    )
    assert spec.confidence == confidence_for(EvidenceClass.AUTHORITATIVE_API)
    assert spec.evidence_class is EvidenceClass.AUTHORITATIVE_API
    assert spec.properties == {"name": "Priya"}


def test_emit_supports_per_property_classes() -> None:
    # one object whose email co-occurs but whose handle is authoritative
    spec = emit(
        "Account",
        "x:foo",
        EvidenceClass.AUTHORITATIVE_API,
        properties={"handle": "foo", "maybe_email": "a@b.com"},
        property_classes={"maybe_email": EvidenceClass.CO_OCCURRENCE},
    )
    assert spec.property_classes["maybe_email"] is EvidenceClass.CO_OCCURRENCE
    # the object default still applies to ungraded properties
    assert spec.evidence_class is EvidenceClass.AUTHORITATIVE_API


def test_link_carries_class_and_confidence() -> None:
    lk = link(
        TargetRef(input=True), TargetRef(ref="x:foo"), "co_occurs", EvidenceClass.CO_OCCURRENCE
    )
    assert lk.type == "co_occurs"
    assert lk.evidence_class is EvidenceClass.CO_OCCURRENCE
    assert lk.confidence == confidence_for(EvidenceClass.CO_OCCURRENCE)
