"""SearXNG meta-search connector + dork-search helpers (DESIGN §3/§7/§8).

SearXNG is the self-hosted meta-search that fronts many engines without keys or
CAPTCHAs — the engine for the dork-aggregation collection path. The connector
expands a selector into dorks (dorks.py), runs the top few against the local
SearXNG JSON API, and returns the aggregated hits for the parser to pull in.

OSIRIS_SEARXNG_URL points at the instance (default http://127.0.0.1:8888).
"""

from __future__ import annotations

import os
from typing import Any

import httpx

from src.orchestrator.dorks import generate_dorks
from src.orchestrator.manifests import Manifest
from src.parsers.base import InputObject

_DEFAULT_URL = "http://127.0.0.1:8888"
_DORKS_PER_RUN = 3
_RESULTS_PER_DORK = 6

# selector types we route through search-engine dorking
_SEARCHABLE = ("Email", "Domain", "Username", "IPv4", "Person", "Phrase")


async def _searx_query(client: httpx.AsyncClient, base: str, query: str) -> list[dict[str, Any]]:
    resp = await client.get(
        f"{base}/search",
        params={"q": query, "format": "json", "safesearch": "0"},
        headers={"Accept": "application/json"},
    )
    resp.raise_for_status()
    data: dict[str, Any] = resp.json()
    return list(data.get("results", []))[:_RESULTS_PER_DORK]


async def searxng_search(input_object: InputObject, *, timeout_s: float = 25.0) -> dict[str, Any]:
    """Run the selector's top dorks through SearXNG and aggregate the hits."""
    base = os.environ.get("OSIRIS_SEARXNG_URL", _DEFAULT_URL).rstrip("/")
    dorks = generate_dorks(input_object.type, input_object.canonical, limit=_DORKS_PER_RUN)
    out: list[dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=timeout_s) as client:
        for q in dorks:
            out.append({"query": q, "results": await _searx_query(client, base, q)})
    return {"selector": input_object.canonical, "dork_results": out}


def search_manifests() -> dict[str, Manifest]:
    """One open-tier search helper per searchable type (local SearXNG is keyless
    and bot-tolerant, so it routes as a normal server worker)."""
    out: dict[str, Manifest] = {}
    for type_ in _SEARCHABLE:
        m = Manifest.model_validate(
            {
                "id": f"searxng_{type_.lower()}",
                "name": f"Search-engine dorking ({type_})",
                "description": f"Dork this {type_} across search engines via SearXNG and "
                               "pull the result pages in as evidence.",
                "consumes": {"type": type_},
                "tier": "open",
                "origin": "searxng",
                "parser": "searxng_results",
                "cache_ttl": 3600,
            }
        )
        out[m.id] = m
    return out
