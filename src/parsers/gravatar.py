"""Gravatar profile parser — email -> Person + linked Accounts."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.parsers.base import EvidenceClass, InputObject, ParseResult, TargetRef
from src.parsers.evidence import emit, link


def parse_gravatar(response: dict[str, Any], input_object: InputObject) -> ParseResult:
    result = ParseResult(observed_at=datetime.now(UTC))
    if not response.get("found") or not response.get("entry"):
        return result
    entry = response["entry"][0]
    digest = response.get("hash", "")

    # the Gravatar profile is keyed to the email hash — an authoritative match.
    person = f"gravatar:{digest}"
    result.objects.append(
        emit(
            "Person",
            person,
            EvidenceClass.AUTHORITATIVE_API,
            properties={
                "name": entry.get("displayName") or (entry.get("name") or {}).get("formatted"),
                "location": entry.get("currentLocation"),
                "about": entry.get("aboutMe"),
            },
            evidence=entry,
        )
    )
    result.links.append(
        link(TargetRef(input=True), TargetRef(ref=person), "has_profile",
             EvidenceClass.AUTHORITATIVE_API)
    )

    # accounts the user listed on their Gravatar — self-declared social links.
    for acc in entry.get("accounts", []):
        handle = acc.get("username") or acc.get("display") or acc.get("shortname")
        platform = acc.get("shortname", "account")
        if not handle:
            continue
        canon = f"{platform}:{handle}"
        result.objects.append(
            emit("Account", canon, EvidenceClass.SELF_DECLARED,
                 properties={"platform": platform, "handle": handle, "url": acc.get("url")})
        )
        result.links.append(
            link(TargetRef(ref=person), TargetRef(ref=canon), "has_account",
                 EvidenceClass.SELF_DECLARED)
        )
    return result
