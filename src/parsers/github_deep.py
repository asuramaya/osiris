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
from src.parsers.base import InputObject, LinkSpec, ObjectSpec, ParseResult, TargetRef
from src.parsers.snippets import extract_selectors


def parse_github_deep(response: dict[str, Any], input_object: InputObject) -> ParseResult:
    result = ParseResult(observed_at=datetime.now(UTC))
    if not response.get("found"):
        return result
    account = input_object.canonical  # github:<login>
    prof = response.get("profile") or {}

    # refresh the github Account's own properties (dedupes to the input object)
    result.objects.append(
        ObjectSpec(type="Account", canonical=account, confidence=0.85,
                   properties={"platform": "github", "handle": response.get("login"),
                               "name": prof.get("name"), "bio": prof.get("bio"),
                               "company": prof.get("company"), "location": prof.get("location")},
                   evidence=response)
    )

    emitted: set[str] = set()

    def emit(type_: str, canon: str, conf: float, link: str) -> None:
        if not canon or canon in emitted or canon == account:
            return
        emitted.add(canon)
        result.objects.append(ObjectSpec(type=type_, canonical=canon, confidence=conf))
        result.links.append(LinkSpec(TargetRef(input=True), TargetRef(ref=canon), link, conf))

    # declared accounts/emails mined from the README (self-asserted = high trust)
    for type_, canon in extract_selectors(response.get("readme") or ""):
        if type_ == "URL":
            acct = profile_account(canon)
            if acct:
                emit("Account", f"{acct[0]}:{acct[1]}", 0.85, "declares")
        elif type_ == "Email":
            emit("Email", canon, 0.8, "has_email")

    # twitter handle from the profile field
    tw = (prof.get("twitter_username") or "").strip().lstrip("@")
    if tw:
        emit("Account", f"twitter:{tw}", 0.85, "declares")

    # owned sites: repo homepages + profile blog -> URL (+ Domain for the host)
    sites = list(response.get("homepages") or [])
    if prof.get("blog"):
        sites.append(prof["blog"])
    for raw in sites:
        url = raw if str(raw).startswith(("http://", "https://")) else f"https://{raw}"
        emit("URL", url, 0.7, "has_url")
        host = urlparse(url).hostname
        if host:
            emit("Domain", canonicalize("Domain", host), 0.7, "has_url")

    # the real committing email(s) — strongest tie (the user authored commits as it)
    for email, _n in (response.get("commit_emails") or {}).items():
        if "@" not in email or "noreply" in email:
            continue  # skip github noreply placeholders; keep real addresses
        emit("Email", canonicalize("Email", email), 0.9, "committed_as")
    return result
