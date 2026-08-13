"""Username enumeration (Sherlock / WhatsMyName-style footprint crawler).

Given a Username, probe a curated set of sites for an account with that handle
and emit the ones that exist as Account objects. This is the core "map my whole
digital footprint" move — usernames are the connective tissue across platforms.

Sites are curated for clean detection (a 404 means absent, or a known absent
marker in the body). Keyless; each probe egresses directly. Bounded concurrency.
"""

from __future__ import annotations

import asyncio
from typing import Any, Literal, TypedDict

import httpx


class ProbeResult(TypedDict):
    """A tri-state, always returned — never a bare Optional (60bc15db: a network fault
    and a confirmed-absent user must not collapse into the same None). `status` is
    'found' (an account, `url` set), 'absent' (checked, genuinely no account), or
    'inconclusive' (the site could not be checked — `reason` says why, e.g. a timeout
    or transport error; NOT evidence either way)."""

    platform: str
    status: Literal["found", "absent", "inconclusive"]
    url: str | None
    username: str
    reason: str | None

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
    # Social/community platforms whose absence signal was LIVE-VERIFIED clean
    # (exists -> 200, missing -> 404) and that serve without keys or bot walls.
    {"platform": "lobsters", "url": "https://lobste.rs/~{u}", "require_status": 200},
    {"platform": "about.me", "url": "https://about.me/{u}", "require_status": 200},
    {"platform": "tumblr", "url": "https://{u}.tumblr.com", "require_status": 200},
    {"platform": "mastodon.social", "url": "https://mastodon.social/@{u}",
     "require_status": 200},
    {"platform": "lastfm", "url": "https://www.last.fm/user/{u}", "require_status": 200},
    {"platform": "soundcloud", "url": "https://soundcloud.com/{u}", "require_status": 200},
]
_UA = {"User-Agent": "Mozilla/5.0 (compatible; osiris-osint)"}


def _absent(platform: str, username: str) -> ProbeResult:
    return {"platform": platform, "status": "absent", "url": None,
            "username": username, "reason": None}


def _inconclusive(platform: str, username: str, reason: str) -> ProbeResult:
    return {"platform": platform, "status": "inconclusive", "url": None,
            "username": username, "reason": reason}


async def _probe(
    client: httpx.AsyncClient, site: dict[str, Any], username: str
) -> ProbeResult:
    url = site["url"].replace("{u}", username)
    try:
        resp = await client.get(url, headers=_UA)
    except httpx.HTTPError as e:
        return _inconclusive(site["platform"], username, f"{type(e).__name__}: {e}")
    # exists iff the site's success status AND no "absent" marker in the body
    if resp.status_code != site["require_status"]:
        return _absent(site["platform"], username)
    if "absent_text" in site and site["absent_text"] in resp.text:
        return _absent(site["platform"], username)
    return {"platform": site["platform"], "status": "found", "url": str(resp.url),
            "username": username, "reason": None}


async def enumerate_username(
    input_object: Any, *, timeout_s: float = 8.0, concurrency: int = 8
) -> dict[str, Any]:
    username = input_object.canonical
    sem = asyncio.Semaphore(concurrency)

    async def guarded(client: httpx.AsyncClient, site: dict[str, Any]) -> ProbeResult:
        async with sem:
            return await _probe(client, site, username)

    async with httpx.AsyncClient(timeout=timeout_s, follow_redirects=True) as client:
        results = await asyncio.gather(*[guarded(client, s) for s in SITES])
    accounts = [{"platform": r["platform"], "url": r["url"], "username": r["username"]}
                for r in results if r["status"] == "found"]
    # ALWAYS present, even empty — an omitted key here would be the exact 60bc15db shape
    # this fix exists to remove: "nothing inconclusive" and "never checked" must not
    # look alike to a caller who only reads for the key's presence.
    inconclusive = [{"platform": r["platform"], "reason": r["reason"]}
                     for r in results if r["status"] == "inconclusive"]
    return {"username": username, "accounts": accounts, "inconclusive": inconclusive}
