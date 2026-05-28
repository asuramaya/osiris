"""crt.sh parser: certificate records -> subdomain Domain objects + links.

Consumes a Domain, emits child Domain objects (one per distinct subdomain seen
in CT logs) linked parent --has_subdomain--> child. This is the canonical
cascade test: each new Domain re-triggers crt.sh, and only the hop-distance
budget stops the recursion (DESIGN §6) — not queue emptiness.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.parsers.base import InputObject, LinkSpec, ObjectSpec, ParseResult, TargetRef


def _canonical_domain(name: str) -> str:
    return name.strip().lower().rstrip(".").lstrip("*.")


def parse_crtsh(response: dict[str, Any], input_object: InputObject) -> ParseResult:
    result = ParseResult(observed_at=datetime.now(UTC))
    parent = _canonical_domain(input_object.canonical)
    seen: set[str] = set()

    for cert in response.get("certs", []):
        # name_value may hold several names separated by newlines
        for raw in str(cert.get("name_value", "")).splitlines():
            sub = _canonical_domain(raw)
            if not sub or sub == parent or sub in seen:
                continue
            if not sub.endswith(f".{parent}"):
                continue  # only true subdomains of the queried apex
            seen.add(sub)
            result.objects.append(
                ObjectSpec(
                    type="Domain",
                    canonical=sub,
                    confidence=0.9,
                    properties={"discovered_via": "crtsh", "apex": parent},
                )
            )
            result.links.append(
                LinkSpec(
                    from_ref=TargetRef(input=True),
                    to_ref=TargetRef(ref=sub),
                    type="has_subdomain",
                    confidence=0.9,
                )
            )
    return result
