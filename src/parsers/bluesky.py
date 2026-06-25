"""Parser for Bluesky — account + bio-declared links."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.parsers.accounts import profile_account
from src.parsers.base import EvidenceClass, InputObject, ParseResult, TargetRef
from src.parsers.evidence import emit, link
from src.parsers.snippets import extract_selectors


def parse_bluesky(response: dict[str, Any], input_object: InputObject) -> ParseResult:
    result = ParseResult(observed_at=datetime.now(UTC))
    seen: set[str] = set()

    def add(type_: str, canon: str, link_type: str, ec: EvidenceClass) -> None:
        if not canon or canon in seen or canon == input_object.canonical:
            return
        seen.add(canon)
        result.objects.append(emit(type_, canon, ec))
        result.links.append(link(TargetRef(input=True), TargetRef(ref=canon), link_type, ec))

    seed = input_object.canonical.split(":", 1)[-1].lower()
    prof = response.get("profile")
    if isinstance(prof, dict) and prof.get("handle"):
        handle = prof["handle"]
        result.objects.append(
            emit("Account", f"bluesky:{handle}", EvidenceClass.DIRECT_OBSERVATION,
                 properties={"platform": "bluesky", "handle": handle,
                             "display": prof.get("displayName"),
                             "description": prof.get("description")})
        )
        # same-handle resolution is a weak identity claim (could be a namesake)
        result.links.append(
            link(TargetRef(input=True), TargetRef(ref=f"bluesky:{handle}"), "has_account",
                 EvidenceClass.CO_OCCURRENCE)
        )
        seen.add(f"bluesky:{handle}")
        for type_, canon in extract_selectors(prof.get("description") or ""):
            if type_ == "URL":
                acct = profile_account(canon)
                if acct:
                    add("Account", f"{acct[0]}:{acct[1]}", "declares", EvidenceClass.CO_OCCURRENCE)
            elif type_ == "Email":
                add("Email", canon, "has_email", EvidenceClass.CO_OCCURRENCE)

    # exact-handle search hits (other actors whose handle prefix matches the seed)
    for actor in response.get("candidates") or []:
        h = actor.get("handle", "")
        if h and h.split(".")[0].lower() == seed:
            add("Account", f"bluesky:{h}", "has_account", EvidenceClass.CO_OCCURRENCE)
    return result
