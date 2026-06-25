"""Parser for deeper-GitHub: GPG-key emails (self-declared) + gist-mined selectors."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.ontology.canonicalize import canonicalize
from src.parsers.accounts import profile_account
from src.parsers.base import EvidenceClass, InputObject, ParseResult, TargetRef
from src.parsers.evidence import emit, link
from src.parsers.snippets import extract_selectors


def parse_github_social(response: dict[str, Any], input_object: InputObject) -> ParseResult:
    result = ParseResult(observed_at=datetime.now(UTC))
    if not response.get("found"):
        return result

    seen: set[str] = set()

    def add(type_: str, canon: str, link_type: str, ec: EvidenceClass) -> None:
        if not canon or canon in seen or canon == input_object.canonical:
            return
        seen.add(canon)
        result.objects.append(emit(type_, canon, ec))
        result.links.append(link(TargetRef(input=True), TargetRef(ref=canon), link_type, ec))

    # GPG-key identities — cryptographically self-declared emails
    for email in dict.fromkeys(response.get("emails") or []):
        if "@" in email and "noreply" not in email:
            add("Email", canonicalize("Email", email), "gpg_identity", EvidenceClass.SELF_DECLARED)

    # gists are the user's own content — mine for links/emails (observed in their gists)
    blob = " ".join(response.get("gists") or [])
    for type_, canon in extract_selectors(blob):
        if type_ == "URL":
            acct = profile_account(canon)
            if acct:
                add("Account", f"{acct[0]}:{acct[1]}", "declares", EvidenceClass.DIRECT_OBSERVATION)
        elif type_ == "Email":
            add("Email", canon, "has_email", EvidenceClass.DIRECT_OBSERVATION)
    return result
