"""Keyless GitHub user enrichment via the public api.github.com.

A Username is the footprint's connective tissue; GitHub's public user endpoint
returns real-name / company / blog / twitter / email / location / bio with NO
key (unauthenticated, ~60 req/hr — kept in budget by a long cache_ttl and low
rps in the manifest). 404 → no such user; 403/429 → rate-limited (graceful, no
raise, so a momentary cap doesn't storm the cascade with failed runs).
"""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

import httpx

from src.parsers.base import InputObject

_API = "https://api.github.com/users/"
_HEADERS = {"Accept": "application/vnd.github+json", "User-Agent": "osiris-osint"}


async def fetch_github_user(
    input_object: InputObject, *, timeout_s: float = 12.0
) -> dict[str, Any]:
    login = input_object.canonical.strip()
    async with httpx.AsyncClient(timeout=timeout_s, follow_redirects=True) as client:
        try:
            resp = await client.get(f"{_API}{quote(login, safe='')}", headers=_HEADERS)
        except httpx.HTTPError:
            return {"found": False, "login": login}
    if resp.status_code == 404:
        return {"found": False, "login": login}
    if resp.status_code in (403, 429):
        return {"found": False, "rate_limited": True, "login": login}
    if resp.status_code != 200:
        return {"found": False, "login": login, "status": resp.status_code}
    return {"found": True, "user": resp.json()}
