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
from src.parsers.snippets import extract_selectors

_LEAK_HINTS = ("filetype:env", "filetype:sql", "pastebin", "ghostbin", "rentry", "paste.ee")

# snippet co-occurrence is weak evidence — emit mined selectors low so they don't
# pollute high-trust views or auto-merge, but still get crawled by the cascade.
_MINED_CONFIDENCE = 0.4
_MINED_CAP = 50  # bound how many mined selectors one search run injects


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
    # mined selectors deduped across the whole run (type, canonical) -> emitted once
    mined: dict[tuple[str, str], None] = {}
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
            # mine the hit's title+snippet for selectors that co-occur with the seed
            blob = f"{hit.get('title') or ''} {hit.get('content') or ''}"
            for pair in extract_selectors(blob):
                mined.setdefault(pair, None)

    # emit mined selectors (low confidence; linked co_occurs) so the cascade crawls
    # them. Skip the seed itself and any URL already surfaced as a direct hit.
    for (type_, canon) in list(mined)[:_MINED_CAP]:
        if canon == selector or (type_ == "URL" and canon in seen):
            continue
        result.objects.append(ObjectSpec(type=type_, canonical=canon, confidence=_MINED_CONFIDENCE))
        result.links.append(
            LinkSpec(TargetRef(input=True), TargetRef(ref=canon), "co_occurs", _MINED_CONFIDENCE)
        )
    return result
