"""Username enumeration (Sherlock / WhatsMyName-style footprint crawler).

Given a Username, probe a curated set of sites for an account with that handle
and emit the ones that exist as Account objects. This is the core "map my whole
digital footprint" move — usernames are the connective tissue across platforms.

Sites are curated for clean detection (a 404 means absent, or a known absent
marker in the body). Keyless; each probe egresses directly. Bounded concurrency.
"""

from __future__ import annotations

import asyncio
from typing import Any

import httpx

# Sites whose absence signal is VERIFIED reliable (existing -> 200; missing ->
# 404 or another >=400). Sites that soft-200 a missing user (pypi) or redirect
# everything to a login page (replit), or 403 everything (npm/reddit without
# auth), are excluded — they produce false positives/negatives. A production
# build would drive the WhatsMyName data file (per-site detection rules) here.
SITES: list[dict[str, Any]] = [
    {"platform": "github", "url": "https://github.com/{u}", "require_status": 200},
    {"platform": "gitlab", "url": "https://gitlab.com/{u}", "require_status": 200},
    {"platform": "keybase", "url": "https://keybase.io/{u}", "require_status": 200},
    {"platform": "dev.to", "url": "https://dev.to/{u}", "require_status": 200},
    {"platform": "gumroad", "url": "https://{u}.gumroad.com", "require_status": 200},
    {"platform": "hackernews", "url": "https://news.ycombinator.com/user?id={u}",
     "require_status": 200, "absent_text": "No such user."},
]
_UA = {"User-Agent": "Mozilla/5.0 (compatible; osiris-osint)"}


async def _probe(
    client: httpx.AsyncClient, site: dict[str, Any], username: str
) -> dict[str, Any] | None:
    url = site["url"].replace("{u}", username)
    try:
        resp = await client.get(url, headers=_UA)
    except httpx.HTTPError:
        return None
    # exists iff the site's success status AND no "absent" marker in the body
    if resp.status_code != site["require_status"]:
        return None
    if "absent_text" in site and site["absent_text"] in resp.text:
        return None
    return {"platform": site["platform"], "url": str(resp.url), "username": username}


async def enumerate_username(
    input_object: Any, *, timeout_s: float = 8.0, concurrency: int = 8
) -> dict[str, Any]:
    username = input_object.canonical
    sem = asyncio.Semaphore(concurrency)

    async def guarded(client: httpx.AsyncClient, site: dict[str, Any]) -> dict[str, Any] | None:
        async with sem:
            return await _probe(client, site, username)

    async with httpx.AsyncClient(timeout=timeout_s, follow_redirects=True) as client:
        results = await asyncio.gather(*[guarded(client, s) for s in SITES])
    return {"username": username, "accounts": [r for r in results if r]}
