"""Keyless web-page fetch connector — the "follow the link" step.

GETs a discovered URL server-side (no key, no browser), so the parser can mine
the page itself for identity signals (rel=me links, mailto:, profile links).
Defensive by construction: http(s) only, text/html only, an incremental byte
cap so a stray binary/huge page can't blow up memory, and centralized challenge
detection — a bot wall raises ChallengeDetected and the cascade suspends the run
to a human (we never solve or evade).
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

import httpx

from src.orchestrator.challenges import ChallengeDetected, detect
from src.parsers.base import InputObject

# a real browser UA: many CDN-fronted personal sites (Cloudflare etc.) serve an
# empty/stub body to obvious bots but the full page to a browser UA. We're reading
# public pages the operator already reached via search, not evading auth.
_UA = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


async def fetch_webpage(
    input_object: InputObject, *, timeout_s: float = 15.0, max_bytes: int = 1_500_000
) -> dict[str, Any]:
    url = input_object.canonical
    if urlparse(url).scheme not in ("http", "https"):
        return {"fetched": False, "reason": "non-http", "url": url}

    async with httpx.AsyncClient(timeout=timeout_s, follow_redirects=True) as client:
        try:
            async with client.stream("GET", url, headers=_UA) as resp:
                ctype = resp.headers.get("content-type", "")
                if "html" not in ctype.lower():
                    return {"fetched": False, "reason": "not-html",
                            "url": str(resp.url), "content_type": ctype}
                buf = bytearray()
                async for chunk in resp.aiter_bytes():
                    buf.extend(chunk)
                    if len(buf) > max_bytes:
                        break  # cap: never read an unbounded body into memory
                body = bytes(buf[:max_bytes]).decode(resp.encoding or "utf-8", errors="replace")
        except httpx.HTTPError as exc:
            return {"fetched": False, "reason": type(exc).__name__, "url": url}

    challenge = detect(status_code=resp.status_code, headers=dict(resp.headers), body=body)
    if challenge is not None:
        raise ChallengeDetected(challenge, url=str(resp.url))
    return {"fetched": True, "url": str(resp.url), "status": resp.status_code, "html": body}
