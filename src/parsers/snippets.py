"""Selector extraction from free text (search snippets / page text).

Unlike classify() (which types a WHOLE trimmed string), these patterns *find*
selectors embedded in prose — the emails, @handles, phone numbers and links that
co-occur with a seed in a search result. Co-occurrence is weak evidence, so the
caller emits these at low confidence; the value is that the cascade then crawls
them. Conservative by design: usernames only as @handles, phones only as
digit-dense runs, to keep snippet noise out of the graph.
"""

from __future__ import annotations

import re

from src.ontology.canonicalize import canonicalize

_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
_URL_RE = re.compile(r"https?://[^\s\"'<>)\]]+", re.I)
_HANDLE_RE = re.compile(r"(?<![\w/@.])@([A-Za-z0-9_]{3,30})(?![\w.])")
_PHONE_RE = re.compile(r"(?<![\w.])(\+?\d[\d\s().\-]{6,}\d)(?![\w])")

_TRAILING = ".,);:'\"]}>"


def extract_selectors(text: str) -> list[tuple[str, str]]:
    """Mine (type, canonical) selector pairs from free text. Deduped, order-stable.

    Types emitted: Email, URL, Username (from @handles), Phone (7–15 digits).
    Each canonical is normalized via canonicalize() so it dedupes against objects
    materialized elsewhere.
    """
    if not text:
        return []
    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    def add(type_: str, raw: str) -> None:
        canon = canonicalize(type_, raw)
        key = (type_, canon)
        if canon and key not in seen:
            seen.add(key)
            out.append(key)

    for m in _EMAIL_RE.findall(text):
        add("Email", m)
    for m in _URL_RE.findall(text):
        add("URL", m.rstrip(_TRAILING))
    for handle in _HANDLE_RE.findall(text):
        add("Username", handle)
    for run in _PHONE_RE.findall(text):
        if 7 <= sum(c.isdigit() for c in run) <= 15:
            add("Phone", run)
    return out
