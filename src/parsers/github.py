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
from src.parsers.base import InputObject, LinkSpec, ObjectSpec, ParseResult, TargetRef


def parse_github_user(response: dict[str, Any], input_object: InputObject) -> ParseResult:
    result = ParseResult(observed_at=datetime.now(UTC))
    if not response.get("found"):
        return result
    user = response.get("user") or {}
    login = user.get("login")
    if not login:
        return result

    account = f"github:{login}"
    result.objects.append(
        ObjectSpec(
            type="Account",
            canonical=account,
            confidence=0.8,
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
        LinkSpec(TargetRef(input=True), TargetRef(ref=account), "is_profile", 0.8)
    )

    email = user.get("email")
    if email and "@" in email:
        canon = canonicalize("Email", email)
        result.objects.append(ObjectSpec(type="Email", canonical=canon, confidence=0.7))
        result.links.append(
            LinkSpec(TargetRef(ref=account), TargetRef(ref=canon), "has_email", 0.7)
        )

    blog = (user.get("blog") or "").strip()
    if blog:
        url = blog if blog.startswith(("http://", "https://")) else f"https://{blog}"
        result.objects.append(ObjectSpec(type="URL", canonical=url, confidence=0.6))
        result.links.append(
            LinkSpec(TargetRef(ref=account), TargetRef(ref=url), "has_url", 0.6)
        )

    twitter = (user.get("twitter_username") or "").strip().lstrip("@")
    if twitter:
        tacc = f"twitter:{twitter}"
        result.objects.append(
            ObjectSpec(type="Account", canonical=tacc, confidence=0.6,
                       properties={"platform": "twitter", "handle": twitter})
        )
        result.links.append(
            LinkSpec(TargetRef(ref=account), TargetRef(ref=tacc), "co_occurs", 0.6)
        )
    return result
