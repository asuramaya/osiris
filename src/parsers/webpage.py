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
from src.parsers.accounts import profile_account
from src.parsers.base import EvidenceClass, InputObject, ParseResult, TargetRef
from src.parsers.evidence import emit, link


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
    match = profile_account(url)
    return f"{match[0]}:{match[1]}" if match is not None else None


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
            emit("URL", input_object.canonical, EvidenceClass.DIRECT_OBSERVATION,
                 properties={"page_title": p.title})
        )

    emitted: set[str] = set()

    def add(type_: str, canon: str, link_type: str, ec: EvidenceClass) -> None:
        if not canon or canon in emitted:
            return
        emitted.add(canon)
        result.objects.append(emit(type_, canon, ec))
        result.links.append(link(TargetRef(input=True), TargetRef(ref=canon), link_type, ec))

    for href, rel in p.anchors:
        if href.startswith("mailto:"):
            addr = href[len("mailto:"):].split("?")[0].strip()
            if "@" in addr:
                # an explicit contact address on the page — self-declared.
                add("Email", canonicalize("Email", addr), "has_email", EvidenceClass.SELF_DECLARED)
            continue
        resolved = urljoin(base, href)
        if not resolved.startswith(("http://", "https://")):
            continue
        is_me = "me" in rel.split()
        acct = _account_ref(resolved)
        if is_me:
            # rel=me is the IndieWeb identity-verification standard — self-declared.
            if acct:
                add("Account", acct, "rel_me", EvidenceClass.SELF_DECLARED)
            else:
                add("URL", resolved, "rel_me", EvidenceClass.SELF_DECLARED)
        elif acct:
            # a plain profile link observed on the page (not rel=me): real but a
            # weaker identity signal — DIRECT_OBSERVATION, not an anchor.
            add("Account", acct, "is_profile", EvidenceClass.DIRECT_OBSERVATION)
        # non-profile, non-rel=me outbound links are not emitted (too noisy)
    return result
