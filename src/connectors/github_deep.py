"""Deep GitHub enrichment — the anchor-and-pivot collector.

Given a CONFIRMED github Account (not a username guess), mine the sources the
person actually controls, where links are self-declared and authoritative:
  * profile README  -> declared social/contact links (LinkedIn, SoundCloud, ...);
  * repo homepages   -> owned domains/sites;
  * commit authorship-> the real email(s) the user commits with (filtered to the
    target's own commits via the API `author=` param, so collaborator emails in
    forked repos are never pulled in).
Keyless by default (~60 req/hr unauth); honours an optional operator GITHUB_TOKEN
(the operator's own creds, 5000 req/hr) to lift the cap. Calls are bounded
(profile + readme + repos + commits for the top-N repos) and cached long.
"""

from __future__ import annotations

import os
from typing import Any
from urllib.parse import quote

import httpx

from src.parsers.base import InputObject

_API = "https://api.github.com"
_MAX_REPOS_FOR_COMMITS = 8  # bound commit-mining calls (one API call per repo)


def _headers() -> dict[str, str]:
    h = {"Accept": "application/vnd.github+json", "User-Agent": "osiris-osint"}
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


async def _get_json(client: httpx.AsyncClient, url: str) -> Any:
    try:
        r = await client.get(url, headers=_headers())
    except httpx.HTTPError:
        return None
    if r.status_code != 200:
        return None
    return r.json()


async def _get_text(client: httpx.AsyncClient, url: str) -> str | None:
    try:
        r = await client.get(url, headers={"User-Agent": "osiris-osint"})
    except httpx.HTTPError:
        return None
    return r.text if r.status_code == 200 else None


async def fetch_github_deep(
    input_object: InputObject, *, timeout_s: float = 20.0
) -> dict[str, Any]:
    canon = input_object.canonical
    if not canon.startswith("github:"):
        return {"found": False, "reason": "not-github-account"}
    login = canon.split(":", 1)[1].strip()
    if not login:
        return {"found": False}
    enc = quote(login, safe="")

    async with httpx.AsyncClient(timeout=timeout_s, follow_redirects=True) as client:
        profile = await _get_json(client, f"{_API}/users/{enc}")
        if not isinstance(profile, dict) or not profile.get("login"):
            return {"found": False, "login": login}

        # profile README (github.com/<login>/<login>) — main, then master
        readme = None
        for branch in ("main", "master"):
            readme = await _get_text(
                client, f"https://raw.githubusercontent.com/{enc}/{enc}/{branch}/README.md"
            )
            if readme:
                break

        repos = await _get_json(client, f"{_API}/users/{enc}/repos?per_page=100&sort=pushed")
        repos = repos if isinstance(repos, list) else []
        homepages = [r["homepage"] for r in repos if r.get("homepage")]

        # commit emails authored BY this user (own emails only)
        emails: dict[str, int] = {}
        for repo in repos[:_MAX_REPOS_FOR_COMMITS]:
            name = repo.get("name")
            if not name:
                continue
            commits = await _get_json(
                client,
                f"{_API}/repos/{enc}/{quote(name, safe='')}/commits"
                f"?author={enc}&per_page=100",
            )
            if not isinstance(commits, list):
                continue
            for cmt in commits:
                email = ((cmt.get("commit") or {}).get("author") or {}).get("email")
                if email:
                    emails[email] = emails.get(email, 0) + 1

    return {
        "found": True,
        "login": profile.get("login"),
        "profile": {
            "name": profile.get("name"), "bio": profile.get("bio"),
            "blog": profile.get("blog"), "company": profile.get("company"),
            "location": profile.get("location"), "email": profile.get("email"),
            "twitter_username": profile.get("twitter_username"),
            "html_url": profile.get("html_url"),
        },
        "readme": readme or "",
        "homepages": homepages,
        "commit_emails": emails,
    }
