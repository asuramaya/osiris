"""Web-page parser — extract identity signals from fetched HTML (stdlib only).

From a page the operator already reached via search, pull:
  * rel="me" links — the IndieWeb identity-verification standard; a strong
    same-identity signal (Mastodon, personal sites, GitHub profiles use it);
  * mailto: addresses — contact emails;
  * profile-shaped <a href> links — Accounts on known platforms;
  * <title> — a human label for the URL node.
Uses the stdlib html.parser (no bs4/lxml dependency). Links attach to the
fetched URL (the input), preserving provenance.
"""

from __future__ import annotations

from datetime import UTC, datetime
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin

from src.ontology.canonicalize import canonicalize
from src.parsers.accounts import _PROFILE_PATTERNS
from src.parsers.base import InputObject, LinkSpec, ObjectSpec, ParseResult, TargetRef


class _Extractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.anchors: list[tuple[str, str]] = []  # (href, rel-tokens)
        self.title: str | None = None
        self._in_title = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        d = {k: (v or "") for k, v in attrs}
        if tag == "a" and d.get("href"):
            self.anchors.append((d["href"], d.get("rel", "").lower()))
        elif tag == "title":
            self._in_title = True

    def handle_endtag(self, tag: str) -> None:
        if tag == "title":
            self._in_title = False

    def handle_data(self, data: str) -> None:
        if self._in_title and self.title is None and data.strip():
            self.title = data.strip()[:200]


def _account_ref(url: str) -> str | None:
    for platform, pattern in _PROFILE_PATTERNS:
        m = pattern.match(url)
        if m:
            return f"{platform}:{m.group(1)}"
    return None


def parse_webpage(response: dict[str, Any], input_object: InputObject) -> ParseResult:
    result = ParseResult(observed_at=datetime.now(UTC))
    if not response.get("fetched"):
        return result
    base = response.get("url") or input_object.canonical
    p = _Extractor()
    p.feed(response.get("html") or "")

    if p.title:
        # label the fetched URL itself (find-or-create dedupes to the input)
        result.objects.append(
            ObjectSpec(type="URL", canonical=input_object.canonical, confidence=0.6,
                       properties={"page_title": p.title})
        )

    emitted: set[str] = set()

    def emit(type_: str, canon: str, confidence: float, link: str) -> None:
        if not canon or canon in emitted:
            return
        emitted.add(canon)
        result.objects.append(ObjectSpec(type=type_, canonical=canon, confidence=confidence))
        result.links.append(LinkSpec(TargetRef(input=True), TargetRef(ref=canon), link, confidence))

    for href, rel in p.anchors:
        if href.startswith("mailto:"):
            addr = href[len("mailto:"):].split("?")[0].strip()
            if "@" in addr:
                emit("Email", canonicalize("Email", addr), 0.7, "has_email")
            continue
        resolved = urljoin(base, href)
        if not resolved.startswith(("http://", "https://")):
            continue
        is_me = "me" in rel.split()
        acct = _account_ref(resolved)
        if is_me:
            # rel=me is a strong identity link — emit the Account if profile-shaped,
            # else the URL itself, at high confidence.
            if acct:
                emit("Account", acct, 0.8, "rel_me")
            else:
                emit("URL", resolved, 0.8, "rel_me")
        elif acct:
            emit("Account", acct, 0.5, "is_profile")
        # non-profile, non-rel=me outbound links are not emitted (too noisy)
    return result
