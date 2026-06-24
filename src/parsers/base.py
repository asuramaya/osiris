"""Parser contract.

A parser is a pure function: (raw source response, input object) -> ParseResult.
It never touches the DB or Actions — it only describes what should exist. The
runner resolves the plan against the graph and applies it through Actions, so
parsers stay trivially testable and carry no business side-effects.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any, Protocol


class EvidenceClass(StrEnum):
    """How a fact was obtained — the source of truth for confidence. Defined here
    (the contract module) so ObjectSpec/LinkSpec can carry it without a cycle;
    `parsers/evidence.py` owns the class→confidence map and the emit/link helpers.
    CORROBORATED is read-time only (computed when ≥2 independent sources agree)."""

    SELF_DECLARED = "self_declared"
    AUTHORITATIVE_API = "authoritative_api"
    DIRECT_OBSERVATION = "direct_observation"
    CO_OCCURRENCE = "co_occurrence"
    DERIVED = "derived"
    CORROBORATED = "corroborated"


@dataclass(frozen=True)
class InputObject:
    """The graph object a helper was dispatched on (its trigger input)."""

    id: str
    type: str
    canonical: str
    properties: dict[str, Any] = field(default_factory=dict)


@dataclass
class ObjectSpec:
    """An object the parser wants to exist, with its asserted properties.

    `evidence` (if set) is the raw artifact bytes-as-dict for this object; the
    runner content-addresses it and stamps evidence_uri/sha256 on its assertions.
    """

    type: str
    canonical: str
    properties: dict[str, Any] = field(default_factory=dict)
    confidence: float = 0.9
    evidence: dict[str, Any] | None = None
    # How this object/its properties were obtained. `evidence_class` is the object
    # default; `property_classes` overrides it per-property when one object carries
    # facts of differing provenance. None = legacy/unknown (runner falls back to
    # `confidence` and writes no class).
    evidence_class: EvidenceClass | None = None
    property_classes: dict[str, EvidenceClass] = field(default_factory=dict)


@dataclass
class TargetRef:
    """How the runner should locate a link endpoint. Exactly one mode is set."""

    input: bool = False             # the consumed input object
    ref: str | None = None          # canonical of an ObjectSpec in this result
    external_id: str | None = None  # ATT&CK handle reverse-lookup (G0032/T1566)
    attack_name: str | None = None  # resolve existing object by name/alias


@dataclass
class LinkSpec:
    from_ref: TargetRef
    to_ref: TargetRef
    type: str
    confidence: float = 0.7
    # Why this edge exists — the frontier's primary "is this node an anchor?" signal.
    evidence_class: EvidenceClass | None = None


@dataclass
class ParseResult:
    objects: list[ObjectSpec] = field(default_factory=list)
    links: list[LinkSpec] = field(default_factory=list)
    observed_at: datetime | None = None


class Parser(Protocol):
    def __call__(self, response: dict[str, Any], input_object: InputObject) -> ParseResult: ...
