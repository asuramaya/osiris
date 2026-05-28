"""crt.sh Certificate Transparency connector (keyless).

Domain -> CT log certificate records, from which we harvest subdomains. crt.sh
is notoriously slow and flaky, which is exactly why it sits behind the router's
cache + rate limit + timeout. Keyless; egresses directly from this box.
"""

from __future__ import annotations

from typing import Any

import httpx

from src.parsers.base import InputObject

_URL = "https://crt.sh/"


async def fetch_crtsh(input_object: InputObject, *, timeout_s: float = 25.0) -> dict[str, Any]:
    domain = input_object.canonical
    params = {"q": f"%.{domain}", "output": "json"}
    async with httpx.AsyncClient(timeout=timeout_s) as client:
        resp = await client.get(_URL, params=params)
        resp.raise_for_status()
        # crt.sh returns a bare JSON array; wrap it so parsers see a dict.
        return {"domain": domain, "certs": resp.json()}
