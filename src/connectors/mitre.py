"""Fetch MITRE ATT&CK STIX bundles (the §11 day-one seed).

Network-gated: used by the ingest CLI and ad-hoc, never in the hermetic test
suite (which runs against tests/fixtures/). Defaults to the Enterprise domain.
"""

from __future__ import annotations

from typing import Any

import httpx

ATTACK_DOMAINS = {
    "enterprise": "enterprise-attack/enterprise-attack.json",
    "mobile": "mobile-attack/mobile-attack.json",
    "ics": "ics-attack/ics-attack.json",
}
_BASE = "https://raw.githubusercontent.com/mitre/cti/master/"


async def fetch_attack_bundle(
    domain: str = "enterprise", *, timeout_s: float = 120.0
) -> dict[str, Any]:
    if domain not in ATTACK_DOMAINS:
        raise ValueError(f"unknown ATT&CK domain {domain!r}; choose from {sorted(ATTACK_DOMAINS)}")
    url = _BASE + ATTACK_DOMAINS[domain]
    async with httpx.AsyncClient(timeout=timeout_s, follow_redirects=True) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
        return data
