"""Parser contract.

A parser is a pure function: (raw source response, input object) -> ParseResult.
It never touches the DB or Actions — it only describes what should exist. The
runner resolves the plan against the graph and applies it through Actions, so
parsers stay trivially testable and carry no business side-effects.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol


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


@dataclass
class ParseResult:
    objects: list[ObjectSpec] = field(default_factory=list)
    links: list[LinkSpec] = field(default_factory=list)
    observed_at: datetime | None = None


class Parser(Protocol):
    def __call__(self, response: dict[str, Any], input_object: InputObject) -> ParseResult: ...
