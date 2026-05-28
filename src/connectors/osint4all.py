"""osint4all integration — turn the curated source directory into helpers.

osint4all (DESIGN §17, "the input universe") is a catalog of OSINT sources keyed
by selector type. Because the architecture is manifest-driven, integrating it =
generating one helper manifest per source. Almost all are link-outs an analyst
opens (not autonomous parsers), so they import as tier=suggest: pasting a
selector + Expand surfaces the applicable sources in the handoff tray, where the
analyst co-browses and promotes findings.

A built-in SEED makes the capability work out of the box; `import_startme` turns
a start.me board export (the real osint4all) into the same manifest shape.
"""

from __future__ import annotations

import re
from typing import Any

from src.orchestrator.manifests import Manifest

# A small representative slice of osint4all, keyed by ontology type.
# url uses {object.canonical}, which the dispatcher renders per object.
SEED: dict[str, list[tuple[str, str]]] = {
    "Email": [
        ("EmailRep", "https://emailrep.io/{object.canonical}"),
        ("Have I Been Pwned", "https://haveibeenpwned.com/account/{object.canonical}"),
    ],
    "Domain": [
        ("ViewDNS reverse-IP", "https://viewdns.info/reverseip/?host={object.canonical}"),
        ("URLScan search", "https://urlscan.io/search/#{object.canonical}"),
    ],
    "Username": [
        ("WhatsMyName", "https://whatsmyname.app/?q={object.canonical}"),
        ("Sherlock targets", "https://www.google.com/search?q=%22{object.canonical}%22"),
    ],
    "IPv4": [
        ("AbuseIPDB", "https://www.abuseipdb.com/check/{object.canonical}"),
        ("Shodan host", "https://www.shodan.io/host/{object.canonical}"),
    ],
    "TelegramChannel": [
        ("tgstat", "https://tgstat.com/channel/{object.canonical}"),
    ],
    "Phrase": [
        ("Google dork", "https://www.google.com/search?q={object.canonical}"),
    ],
}


def _slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def _manifest(type_: str, name: str, url: str) -> Manifest:
    return Manifest.model_validate(
        {
            "id": f"osint4all_{type_.lower()}_{_slug(name)}",
            "name": f"{name} ({type_})",
            "description": f"Open {name} for this {type_} in your browser (osint4all).",
            "consumes": {"type": type_},
            "tier": "suggest",
            "origin": _slug(name),
            "parser": "suggest_noop",
            "template": {"url": url},
            "cache_ttl": 0,
        }
    )


def suggest_manifests(seed: dict[str, list[tuple[str, str]]] | None = None) -> dict[str, Manifest]:
    """Generate the suggest-tier manifest set from the seed (or a custom map)."""
    out: dict[str, Manifest] = {}
    for type_, sources in (seed or SEED).items():
        for name, url in sources:
            m = _manifest(type_, name, url)
            out[m.id] = m
    return out


# group name (start.me column / tag) -> ontology type
_GROUP_TO_TYPE = {
    "email": "Email", "domain": "Domain", "ip": "IPv4", "username": "Username",
    "telegram": "TelegramChannel", "people": "Person",
}


def import_startme(board: dict[str, Any]) -> dict[str, Manifest]:
    """Turn a start.me board export into suggest manifests. Maps each bookmark's
    group/tags to an ontology type; bookmarks whose group we can't map are
    skipped. (The real osint4all integration path — feed it the JSON export.)"""
    seed: dict[str, list[tuple[str, str]]] = {}
    for group in board.get("groups", []):
        gname = str(group.get("title", "")).lower()
        type_ = next((t for k, t in _GROUP_TO_TYPE.items() if k in gname), None)
        if type_ is None:
            continue
        for bm in group.get("bookmarks", []):
            url, title = bm.get("url"), bm.get("title")
            if url and title:
                seed.setdefault(type_, []).append((title, url))
    return suggest_manifests(seed)
