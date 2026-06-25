"""Deeper GitHub connector — the keyless data github_user/github_deep skip.

Two more keyless surfaces on a confirmed github Account:
  * GPG keys (`/users/{u}/gpg_keys`) — each carries the email identities the user
    signs commits with: cryptographic, self-declared, often emails not on the
    profile;
  * Gists (`/users/{u}/gists`) — descriptions + filenames the user wrote, mined for
    links/emails the profile doesn't list.
Same 60/hr unauth budget as the other github helpers; honours GITHUB_TOKEN.
"""

from __future__ import annotations

import os
from typing import Any
from urllib.parse import quote

import httpx

from src.parsers.base import InputObject

_API = "https://api.github.com"


def _headers() -> dict[str, str]:
    h = {"Accept": "application/vnd.github+json", "User-Agent": "osiris-osint"}
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


async def fetch_github_social(
    input_object: InputObject, *, timeout_s: float = 20.0
) -> dict[str, Any]:
    canon = input_object.canonical
    if not canon.startswith("github:"):
        return {"found": False}
    login = canon.split(":", 1)[1].strip()
    if not login:
        return {"found": False}
    enc = quote(login, safe="")
    emails: list[str] = []
    gists: list[str] = []
    async with httpx.AsyncClient(timeout=timeout_s, follow_redirects=True) as client:
        try:
            r = await client.get(f"{_API}/users/{enc}/gpg_keys", headers=_headers())
            if r.status_code == 200:
                for key in r.json():
                    for ident in key.get("emails") or []:
                        if ident.get("email"):
                            emails.append(ident["email"])
        except httpx.HTTPError:
            pass
        try:
            r = await client.get(f"{_API}/users/{enc}/gists?per_page=100", headers=_headers())
            if r.status_code == 200:
                for g in r.json():
                    desc = g.get("description") or ""
                    files = " ".join((g.get("files") or {}).keys())
                    gists.append(f"{desc} {files} {g.get('html_url', '')}")
        except httpx.HTTPError:
            pass
    return {"found": True, "login": login, "emails": emails, "gists": gists}
