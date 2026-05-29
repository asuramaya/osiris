"""Gravatar profile lookup (keyless email enrichment).

A Gravatar profile is keyed by md5(lowercased email) and exposes a public JSON
profile (display name, location, linked social accounts) when one exists — one
of the few things a bare email reliably resolves to without auth. 404 just means
no public profile.
"""

from __future__ import annotations

import hashlib
from typing import Any

import httpx

from src.parsers.base import InputObject


async def fetch_gravatar(input_object: InputObject, *, timeout_s: float = 15.0) -> dict[str, Any]:
    email = input_object.canonical.strip().lower()
    digest = hashlib.md5(email.encode()).hexdigest()  # noqa: S324 - Gravatar's key scheme
    url = f"https://en.gravatar.com/{digest}.json"
    async with httpx.AsyncClient(timeout=timeout_s, follow_redirects=True) as client:
        resp = await client.get(url, headers={"User-Agent": "osiris-osint"})
    if resp.status_code != 200:
        return {"found": False, "hash": digest}
    try:
        data: dict[str, Any] = resp.json()
    except ValueError:
        return {"found": False, "hash": digest}
    return {"found": True, "hash": digest, "entry": data.get("entry", [])}
