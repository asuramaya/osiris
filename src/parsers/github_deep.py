"""Deep GitHub parser — turn the anchor pivot into typed footprint objects.

From the confirmed github Account, emit:
  * declared profile accounts from the README + profile fields (LinkedIn,
    SoundCloud, Twitter, ...) — `declares`, high confidence (self-asserted);
  * owned domains/sites from repo homepages + blog — `has_url`;
  * the real email(s) the user commits with — `committed_as`, the strongest
    email→account tie in the whole footprint (they authored as it).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse

from src.ontology.canonicalize import canonicalize
from src.parsers.accounts import profile_account
from src.parsers.base import EvidenceClass, InputObject, ParseResult, TargetRef
from src.parsers.evidence import emit, link
from src.parsers.snippets import extract_selectors


def parse_github_deep(response: dict[str, Any], input_object: InputObject) -> ParseResult:
    result = ParseResult(observed_at=datetime.now(UTC))
    if not response.get("found"):
        return result
    account = input_object.canonical  # github:<login>
    prof = response.get("profile") or {}

    # refresh the github Account's own properties (dedupes to the input object)
    result.objects.append(
        emit("Account", account, EvidenceClass.AUTHORITATIVE_API,
             properties={"platform": "github", "handle": response.get("login"),
                         "name": prof.get("name"), "bio": prof.get("bio"),
                         "company": prof.get("company"), "location": prof.get("location")},
             evidence=response)
    )

    emitted: set[str] = set()

    def add(type_: str, canon: str, link_type: str, ec: EvidenceClass) -> None:
        # everything mined here is self-declared by the user (README links, profile
        # fields, repo homepages, commit authorship) — the anchor-and-pivot payoff.
        if not canon or canon in emitted or canon == account:
            return
        emitted.add(canon)
        result.objects.append(emit(type_, canon, ec))
        result.links.append(link(TargetRef(input=True), TargetRef(ref=canon), link_type, ec))

    # declared accounts/emails mined from the README (self-asserted = high trust)
    for type_, canon in extract_selectors(response.get("readme") or ""):
        if type_ == "URL":
            acct = profile_account(canon)
            if acct:
                add("Account", f"{acct[0]}:{acct[1]}", "declares", EvidenceClass.SELF_DECLARED)
        elif type_ == "Email":
            add("Email", canon, "has_email", EvidenceClass.SELF_DECLARED)

    # twitter handle from the profile field
    tw = (prof.get("twitter_username") or "").strip().lstrip("@")
    if tw:
        add("Account", f"twitter:{tw}", "declares", EvidenceClass.SELF_DECLARED)

    # owned sites: repo homepages + profile blog -> URL (+ Domain for the host)
    sites = list(response.get("homepages") or [])
    if prof.get("blog"):
        sites.append(prof["blog"])
    for raw in sites:
        url = raw if str(raw).startswith(("http://", "https://")) else f"https://{raw}"
        add("URL", url, "has_url", EvidenceClass.SELF_DECLARED)
        host = urlparse(url).hostname
        if host:
            add("Domain", canonicalize("Domain", host), "has_url", EvidenceClass.SELF_DECLARED)

    # the real committing email(s) — strongest tie (the user authored commits as it)
    for email, _n in (response.get("commit_emails") or {}).items():
        if "@" not in email or "noreply" in email:
            continue  # skip github noreply placeholders; keep real addresses
        add("Email", canonicalize("Email", email), "committed_as", EvidenceClass.SELF_DECLARED)
    return result
