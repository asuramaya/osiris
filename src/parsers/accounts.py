"""Parsers that turn footprint hits into typed Account objects.

`parse_username_accounts`: username-enumeration results -> Account per platform.
`parse_url_accounts`: recognize profile-URL patterns in a discovered URL and
derive the Account it represents, so the graph organizes the footprint by
platform/identity rather than a flat list of links.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any

from src.parsers.base import InputObject, LinkSpec, ObjectSpec, ParseResult, TargetRef


def parse_username_accounts(response: dict[str, Any], input_object: InputObject) -> ParseResult:
    result = ParseResult(observed_at=datetime.now(UTC))
    for acc in response.get("accounts", []):
        canon = f"{acc['platform']}:{acc['username']}"
        result.objects.append(
            ObjectSpec(
                type="Account",
                canonical=canon,
                confidence=0.7,
                properties={"platform": acc["platform"], "handle": acc["username"],
                            "url": acc.get("url")},
            )
        )
        result.links.append(
            LinkSpec(TargetRef(input=True), TargetRef(ref=canon), "has_account", 0.7)
        )
    return result


# profile-URL patterns -> (platform, handle). Single-segment profile paths only.
_PROFILE_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("github", re.compile(r"https?://github\.com/([A-Za-z0-9-]{1,39})/?$")),
    ("gitlab", re.compile(r"https?://gitlab\.com/([A-Za-z0-9_.-]+)/?$")),
    ("linkedin", re.compile(r"https?://(?:www\.)?linkedin\.com/in/([A-Za-z0-9-]+)/?")),
    ("twitter", re.compile(r"https?://(?:www\.)?(?:twitter|x)\.com/([A-Za-z0-9_]{1,15})/?$")),
    ("youtube", re.compile(r"https?://(?:www\.)?youtube\.com/@([A-Za-z0-9_.-]+)/?")),
    ("reddit", re.compile(r"https?://(?:www\.)?reddit\.com/user/([A-Za-z0-9_-]+)/?")),
    ("instagram", re.compile(r"https?://(?:www\.)?instagram\.com/([A-Za-z0-9_.]+)/?$")),
    ("telegram", re.compile(r"https?://t\.me/([A-Za-z0-9_]{4,32})/?$")),
    ("medium", re.compile(r"https?://medium\.com/@([A-Za-z0-9_.-]+)/?")),
    ("facebook", re.compile(r"https?://(?:www\.)?facebook\.com/([A-Za-z0-9.]{5,})/?$")),
    ("tiktok", re.compile(r"https?://(?:www\.)?tiktok\.com/@([A-Za-z0-9_.]+)/?")),
    ("mastodon", re.compile(r"https?://[A-Za-z0-9.-]+/@([A-Za-z0-9_]+)/?$")),
    ("soundcloud", re.compile(r"https?://(?:www\.)?soundcloud\.com/([A-Za-z0-9_-]+)/?$")),
    ("about.me", re.compile(r"https?://about\.me/([A-Za-z0-9_.-]+)/?$")),
    ("lobsters", re.compile(r"https?://lobste\.rs/~([A-Za-z0-9_.-]+)/?$")),
    ("tumblr", re.compile(r"https?://([A-Za-z0-9-]+)\.tumblr\.com/?$")),
    ("lastfm", re.compile(r"https?://(?:www\.)?last\.fm/user/([A-Za-z0-9_-]+)/?")),
    ("pinterest", re.compile(r"https?://(?:www\.)?pinterest\.com/([A-Za-z0-9_]+)/?$")),
    ("keybase", re.compile(r"https?://keybase\.io/([A-Za-z0-9_]+)/?$")),
]


def parse_url_accounts(response: dict[str, Any], input_object: InputObject) -> ParseResult:
    result = ParseResult(observed_at=datetime.now(UTC))
    url = input_object.canonical
    for platform, pattern in _PROFILE_PATTERNS:
        m = pattern.match(url)
        if not m:
            continue
        handle = m.group(1)
        canon = f"{platform}:{handle}"
        result.objects.append(
            ObjectSpec(
                type="Account",
                canonical=canon,
                confidence=0.7,
                properties={"platform": platform, "handle": handle, "url": url},
            )
        )
        result.links.append(
            LinkSpec(TargetRef(input=True), TargetRef(ref=canon), "is_profile", 0.7)
        )
        break  # one profile per URL
    return result
