"""Parser for SearXNG dork-search results.

Pulls aggregated search hits into the graph: one ObservedData per run (the raw
result set as evidence) and a URL object per unique hit, linked from the seed
selector via `appears_in`. A hit surfaced by a credential/leak-intent dork is
tagged so the analyst can triage; promotion of the page contents themselves is
a follow-up (federation/co-browse).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.parsers.base import InputObject, LinkSpec, ObjectSpec, ParseResult, TargetRef

_LEAK_HINTS = ("filetype:env", "filetype:sql", "pastebin", "ghostbin", "rentry", "paste.ee")


def parse_searxng_results(response: dict[str, Any], input_object: InputObject) -> ParseResult:
    result = ParseResult(observed_at=datetime.now(UTC))
    selector = input_object.canonical
    dork_results = response.get("dork_results", [])

    # one ObservedData per search run — the evidence behind everything below
    n_hits = sum(len(d.get("results", [])) for d in dork_results)
    snapshot = f"search:{selector}"
    result.objects.append(
        ObjectSpec(
            type="ObservedData",
            canonical=snapshot,
            confidence=0.95,
            properties={"source": "searxng", "selector": selector, "hit_count": n_hits,
                        "dorks": [d.get("query") for d in dork_results]},
            evidence=response,
        )
    )
    result.links.append(
        LinkSpec(TargetRef(input=True), TargetRef(ref=snapshot), "has_observation", 0.95)
    )

    seen: set[str] = set()
    for dork in dork_results:
        leaky = any(h in (dork.get("query") or "") for h in _LEAK_HINTS)
        for hit in dork.get("results", []):
            url = hit.get("url")
            if not url or url in seen:
                continue
            seen.add(url)
            result.objects.append(
                ObjectSpec(
                    type="URL",
                    canonical=url,
                    confidence=0.6,
                    properties={
                        "title": hit.get("title"),
                        "snippet": hit.get("content"),
                        "engine": hit.get("engine"),
                        "lead": "credential/leak" if leaky else None,
                    },
                )
            )
            result.links.append(
                LinkSpec(TargetRef(input=True), TargetRef(ref=url), "appears_in", 0.6)
            )
    return result
