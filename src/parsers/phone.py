"""Phone metadata parser — enrich a Phone and seed format-variant search.

Two moves:
  * assert the offline-derived metadata (country/region/carrier/line type) back
    onto the Phone object (an ObjectSpec matching the input dedupes to it);
  * emit the human-formatted variants (national / international) as Phrase
    objects so the search-dorking helper hunts the number in the formats people
    actually paste it in — that's the keyless path that connects a number to the
    accounts, listings and directory pages bearing it.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.parsers.base import InputObject, LinkSpec, ObjectSpec, ParseResult, TargetRef


def parse_phone_meta(response: dict[str, Any], input_object: InputObject) -> ParseResult:
    result = ParseResult(observed_at=datetime.now(UTC))
    if not response.get("valid"):
        return result

    # assert metadata back onto the Phone (find-or-create dedupes to the input)
    result.objects.append(
        ObjectSpec(
            type="Phone",
            canonical=input_object.canonical,
            confidence=0.95,
            properties={
                "country": response.get("country"),
                "region": response.get("region"),
                "carrier": response.get("carrier"),
                "line_type": response.get("line_type"),
                "e164": response.get("e164"),
                "international": response.get("international"),
                "national": response.get("national"),
            },
            evidence=response,
        )
    )

    # seed format-variant search: dorking the bare E.164 misses pages that show
    # the number formatted, so push the human formats through as Phrases.
    seen: set[str] = set()
    for fmt in (response.get("national"), response.get("international")):
        if not fmt or fmt in seen:
            continue
        seen.add(fmt)
        result.objects.append(
            ObjectSpec(type="Phrase", canonical=fmt, confidence=0.6)
        )
        result.links.append(
            LinkSpec(TargetRef(input=True), TargetRef(ref=fmt), "search_variant", 0.6)
        )
    return result
