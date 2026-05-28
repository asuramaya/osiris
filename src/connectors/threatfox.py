"""ThreatFox (abuse.ch) connector.

NOTE: abuse.ch now requires a free Auth-Key on all API calls (ThreatFox/URLhaus/
MalwareBazaar) — the "keyless ThreatFox" assumption in DESIGN §3/§14 no longer
holds. Set THREATFOX_AUTH_KEY in the env; calls without it return Unauthorized.
The hermetic tests use recorded fixtures and never hit the network.
"""

from __future__ import annotations

import os
from typing import Any

import httpx

_API = "https://threatfox-api.abuse.ch/api/v1/"


async def search_malware_iocs(
    search_term: str, *, limit: int = 50, timeout_s: float = 30.0
) -> dict[str, Any]:
    """Fetch IOCs associated with a malware family/name."""
    auth_key = os.environ.get("THREATFOX_AUTH_KEY", "")
    headers = {"Auth-Key": auth_key} if auth_key else {}
    body = {"query": "malwareinfo", "malware": search_term, "limit": limit}
    async with httpx.AsyncClient(timeout=timeout_s) as client:
        resp = await client.post(_API, json=body, headers=headers)
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
        return data
