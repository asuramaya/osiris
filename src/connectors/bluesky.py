"""Bluesky connector — keyless open-protocol social.

Unlike Twitter/Instagram (antibot), the AT Protocol AppView is fully public: no key,
no auth. getProfile resolves a handle; searchActors finds candidates by term. This is
real social presence reachable under the keyless/no-antibot constraint.
"""

from __future__ import annotations

from typing import Any

import httpx

from src.parsers.base import InputObject

_PUB = "https://public.api.bsky.app/xrpc"
_UA = {"User-Agent": "osiris-osint"}


async def fetch_bluesky(input_object: InputObject, *, timeout_s: float = 15.0) -> dict[str, Any]:
    handle = input_object.canonical
    if input_object.type == "Account" and handle.startswith("bluesky:"):
        handle = handle.split(":", 1)[1]
    out: dict[str, Any] = {"profile": None, "candidates": []}
    async with httpx.AsyncClient(timeout=timeout_s, follow_redirects=True) as client:
        for actor in (f"{handle}.bsky.social", handle):
            try:
                r = await client.get(
                    f"{_PUB}/app.bsky.actor.getProfile", params={"actor": actor}, headers=_UA
                )
            except httpx.HTTPError:
                continue
            if r.status_code == 200:
                out["profile"] = r.json()
                break
        try:
            r = await client.get(
                f"{_PUB}/app.bsky.actor.searchActors",
                params={"q": handle, "limit": "10"}, headers=_UA,
            )
            if r.status_code == 200:
                out["candidates"] = r.json().get("actors", [])
        except httpx.HTTPError:
            pass
    return out
