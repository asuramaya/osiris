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

from src.parsers.base import EvidenceClass, InputObject, ParseResult, TargetRef
from src.parsers.evidence import emit, link
from src.parsers.snippets import extract_selectors

_LEAK_HINTS = ("filetype:env", "filetype:sql", "pastebin", "ghostbin", "rentry", "paste.ee")

_MINED_CAP = 50  # bound how many mined selectors one search run injects


def parse_searxng_results(response: dict[str, Any], input_object: InputObject) -> ParseResult:
    result = ParseResult(observed_at=datetime.now(UTC))
    selector = input_object.canonical
    dork_results = response.get("dork_results", [])

    # one ObservedData per search run — the faithful record behind everything below
    n_hits = sum(len(d.get("results", [])) for d in dork_results)
    snapshot = f"search:{selector}"
    result.objects.append(
        emit(
            "ObservedData",
            snapshot,
            EvidenceClass.AUTHORITATIVE_API,
            properties={"source": "searxng", "selector": selector, "hit_count": n_hits,
                        "dorks": [d.get("query") for d in dork_results]},
            evidence=response,
        )
    )
    result.links.append(
        link(TargetRef(input=True), TargetRef(ref=snapshot), "has_observation",
             EvidenceClass.AUTHORITATIVE_API)
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
            # a real URL the search returned — a direct observation.
            result.objects.append(
                emit(
                    "URL",
                    url,
                    EvidenceClass.DIRECT_OBSERVATION,
                    properties={
                        "title": hit.get("title"),
                        "snippet": hit.get("content"),
                        "engine": hit.get("engine"),
                        "lead": "credential/leak" if leaky else None,
                    },
                )
            )
            result.links.append(
                link(TargetRef(input=True), TargetRef(ref=url), "appears_in",
                     EvidenceClass.DIRECT_OBSERVATION)
            )
            # mine the hit's title+snippet for selectors that co-occur with the seed
            blob = f"{hit.get('title') or ''} {hit.get('content') or ''}"
            for pair in extract_selectors(blob):
                mined.setdefault(pair, None)

    # mined selectors are mere co-occurrence in a snippet — speculative. The frontier
    # keeps them as leaves (won't crawl them) until a second source corroborates.
    for (type_, canon) in list(mined)[:_MINED_CAP]:
        if canon == selector or (type_ == "URL" and canon in seen):
            continue
        result.objects.append(emit(type_, canon, EvidenceClass.CO_OCCURRENCE))
        result.links.append(
            link(TargetRef(input=True), TargetRef(ref=canon), "co_occurs",
                 EvidenceClass.CO_OCCURRENCE)
        )
    return result
