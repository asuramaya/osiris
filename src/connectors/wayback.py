"""Wayback Machine CDX connector — keyless historical footprint.

The Internet Archive's CDX server is free and unauthenticated. Pointed at a domain
the subject owns, it returns every path it ever archived — old profile pages,
deleted content, prior versions of a site — which url_fetch then mines for links the
live site no longer shows. This is the single richest keyless source of a person's
*past* footprint, and Osiris wasn't touching it.
"""

from __future__ import annotations

from typing import Any

import httpx

from src.parsers.base import InputObject

_CDX = "https://web.archive.org/cdx/search/cdx"


async def fetch_wayback(
    input_object: InputObject, *, timeout_s: float = 25.0, limit: int = 250
) -> dict[str, Any]:
    target = input_object.canonical
    # matchType is explicit, so DON'T also append a wildcard (double-spec returns nothing):
    # domain => the host and all its subdomains/paths; prefix => everything under a URL.
    if input_object.type == "Domain":
        params = {"url": target, "matchType": "domain"}
    elif input_object.type == "URL":
        params = {"url": target, "matchType": "prefix"}
    else:
        return {"snapshots": []}
    params.update({
        "output": "json", "collapse": "urlkey",
        "fl": "original,timestamp,statuscode", "limit": str(limit),
    })
    try:
        async with httpx.AsyncClient(timeout=timeout_s, follow_redirects=True) as client:
            r = await client.get(_CDX, params=params, headers={"User-Agent": "osiris-osint"})
    except httpx.HTTPError:
        return {"snapshots": []}
    if r.status_code != 200:
        return {"snapshots": []}
    try:
        rows = r.json()
    except ValueError:
        return {"snapshots": []}
    if rows and rows[0] and rows[0][0] == "original":
        rows = rows[1:]  # drop the header row
    return {"snapshots": rows}
