"""Email -> candidate Username pivot.

A bare email rarely has a public search footprint, but the handle behind it
does. Deriving Username objects from the local part lets the cascade pivot into
username search/dorking + osint4all username sources — the productive path.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.parsers.base import InputObject, LinkSpec, ObjectSpec, ParseResult, TargetRef


def parse_email_handles(response: dict[str, Any], input_object: InputObject) -> ParseResult:
    result = ParseResult(observed_at=datetime.now(UTC))
    local = input_object.canonical.split("@", 1)[0].lower()

    variants: list[str] = []
    for v in (local, local.replace(".", ""), local.replace(".", "_"), local.split(".", 1)[0]):
        if v and v not in variants:
            variants.append(v)

    for handle in variants:
        result.objects.append(
            ObjectSpec(
                type="Username",
                canonical=handle,
                confidence=0.5,  # candidate — same local part, not proven the same person
                properties={"derived_from": input_object.canonical},
            )
        )
        result.links.append(
            LinkSpec(TargetRef(input=True), TargetRef(ref=handle), "derived_handle", 0.5)
        )
    return result
