from __future__ import annotations

import uuid

from src.connectors.searxng import search_manifests
from src.orchestrator.dorks import generate_dorks
from src.parsers.base import InputObject
from src.parsers.searxng import parse_searxng_results


def test_generate_dorks_substitutes_and_ranks() -> None:
    d = generate_dorks("Email", "priya@kowalski.dev", limit=3)
    assert d[0] == '"priya@kowalski.dev"'
    assert any("filetype:env" in q for q in d)
    assert len(d) == 3
    assert generate_dorks("Domain", "asuramaya.com")[0] == "site:asuramaya.com"


def test_search_manifests_cover_selectors() -> None:
    ms = search_manifests()
    types = {m.consumes.type for m in ms.values()}
    assert {"Email", "Domain", "Username"} <= types
    assert all(m.tier == "open" and m.parser == "searxng_results" for m in ms.values())


def test_parse_pulls_results_into_graph() -> None:
    inp = InputObject(id=str(uuid.uuid4()), type="Email", canonical="priya@kowalski.dev")
    response = {
        "selector": "priya@kowalski.dev",
        "dork_results": [
            {"query": '"priya@kowalski.dev"', "results": [
                {"url": "https://example.com/a", "title": "A", "content": "...", "engine": "ddg"},
                {"url": "https://example.com/b", "title": "B", "content": "...", "engine": "ddg"},
            ]},
            {"query": '"priya@kowalski.dev" filetype:env', "results": [
                {"url": "https://paste.ee/x", "title": "leak", "content": "env", "engine": "brave"},
                {"url": "https://example.com/a", "title": "A", "content": "dup", "engine": "ddg"},
            ]},
        ],
    }
    result = parse_searxng_results(response, inp)
    urls = [o for o in result.objects if o.type == "URL"]
    obs = [o for o in result.objects if o.type == "ObservedData"]
    assert len(obs) == 1 and obs[0].properties["hit_count"] == 4
    assert {u.canonical for u in urls} == {
        "https://example.com/a", "https://example.com/b", "https://paste.ee/x"
    }  # deduped across dorks
    leak = next(u for u in urls if u.canonical == "https://paste.ee/x")
    assert leak.properties["lead"] == "credential/leak"
    # every URL is linked back to the seed
    assert sum(1 for link in result.links if link.type == "appears_in") == 3
