"""GitHub user parser — public profile fields into the footprint graph.

Emits the GitHub Account (is_profile from the Username, with name/company/bio/
location), a contact Email if the profile lists one, the blog/website URL, and
a co-occurring Twitter Account. Person-hub formation is deliberately left to the
convergence step (resolution.py) so hub creation stays centralized.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.ontology.canonicalize import canonicalize
from src.parsers.base import EvidenceClass, InputObject, ParseResult, TargetRef
from src.parsers.evidence import emit, link


def parse_github_user(response: dict[str, Any], input_object: InputObject) -> ParseResult:
    result = ParseResult(observed_at=datetime.now(UTC))
    if not response.get("found"):
        return result
    user = response.get("user") or {}
    login = user.get("login")
    if not login:
        return result

    # the account identity is vouched by the GitHub API; the email/blog/twitter it
    # carries are fields the user declared on their profile.
    account = f"github:{login}"
    result.objects.append(
        emit(
            "Account",
            account,
            EvidenceClass.AUTHORITATIVE_API,
            properties={
                "platform": "github",
                "handle": login,
                "name": user.get("name"),
                "company": user.get("company"),
                "bio": user.get("bio"),
                "location": user.get("location"),
                "url": user.get("html_url"),
            },
            evidence=user,
        )
    )
    result.links.append(
        link(TargetRef(input=True), TargetRef(ref=account), "is_profile",
             EvidenceClass.AUTHORITATIVE_API)
    )

    email = user.get("email")
    if email and "@" in email:
        canon = canonicalize("Email", email)
        result.objects.append(emit("Email", canon, EvidenceClass.SELF_DECLARED))
        result.links.append(
            link(TargetRef(ref=account), TargetRef(ref=canon), "has_email",
                 EvidenceClass.SELF_DECLARED)
        )

    blog = (user.get("blog") or "").strip()
    if blog:
        url = blog if blog.startswith(("http://", "https://")) else f"https://{blog}"
        result.objects.append(emit("URL", url, EvidenceClass.SELF_DECLARED))
        result.links.append(
            link(TargetRef(ref=account), TargetRef(ref=url), "has_url",
                 EvidenceClass.SELF_DECLARED)
        )

    twitter = (user.get("twitter_username") or "").strip().lstrip("@")
    if twitter:
        tacc = f"twitter:{twitter}"
        result.objects.append(
            emit("Account", tacc, EvidenceClass.SELF_DECLARED,
                 properties={"platform": "twitter", "handle": twitter})
        )
        result.links.append(
            link(TargetRef(ref=account), TargetRef(ref=tacc), "co_occurs",
                 EvidenceClass.SELF_DECLARED)
        )
    return result
