"""Parser for Wayback CDX results — archived URLs as direct observations."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.parsers.base import EvidenceClass, InputObject, ParseResult, TargetRef
from src.parsers.evidence import emit, link

_CAP = 40  # bound how many archived URLs one domain injects


def parse_wayback(response: dict[str, Any], input_object: InputObject) -> ParseResult:
    result = ParseResult(observed_at=datetime.now(UTC))
    seen: set[str] = set()
    for row in response.get("snapshots", []):
        if not row:
            continue
        url = row[0]
        if (not url or url in seen or url == input_object.canonical
                or not url.startswith(("http://", "https://"))):
            continue
        seen.add(url)
        # the page provably existed (the archive holds a capture) — a real observation.
        result.objects.append(
            emit("URL", url, EvidenceClass.DIRECT_OBSERVATION, properties={"archived": True})
        )
        result.links.append(
            link(TargetRef(input=True), TargetRef(ref=url), "archived_snapshot",
                 EvidenceClass.DIRECT_OBSERVATION)
        )
        if len(seen) >= _CAP:
            break
    return result
