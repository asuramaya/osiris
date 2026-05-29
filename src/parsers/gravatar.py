"""Gravatar profile parser — email -> Person + linked Accounts."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.parsers.base import InputObject, LinkSpec, ObjectSpec, ParseResult, TargetRef


def parse_gravatar(response: dict[str, Any], input_object: InputObject) -> ParseResult:
    result = ParseResult(observed_at=datetime.now(UTC))
    if not response.get("found") or not response.get("entry"):
        return result
    entry = response["entry"][0]
    digest = response.get("hash", "")

    person = f"gravatar:{digest}"
    result.objects.append(
        ObjectSpec(
            type="Person",
            canonical=person,
            confidence=0.8,
            properties={
                "name": entry.get("displayName") or (entry.get("name") or {}).get("formatted"),
                "location": entry.get("currentLocation"),
                "about": entry.get("aboutMe"),
            },
            evidence=entry,
        )
    )
    result.links.append(
        LinkSpec(TargetRef(input=True), TargetRef(ref=person), "has_profile", 0.8)
    )

    for acc in entry.get("accounts", []):
        handle = acc.get("username") or acc.get("display") or acc.get("shortname")
        platform = acc.get("shortname", "account")
        if not handle:
            continue
        canon = f"{platform}:{handle}"
        result.objects.append(
            ObjectSpec(
                type="Account",
                canonical=canon,
                confidence=0.7,
                properties={"platform": platform, "handle": handle, "url": acc.get("url")},
            )
        )
        result.links.append(
            LinkSpec(TargetRef(ref=person), TargetRef(ref=canon), "has_account", 0.7)
        )
    return result
