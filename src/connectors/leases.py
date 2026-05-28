"""Cookie-lease subsystem (DESIGN §9 mode 2, Phase 5).

The analyst solves a challenge once in their real browser; we capture the
resulting session cookies (e.g. cf_clearance) and reuse them for short-TTL
server-side requests. Threat model turned feature: the lease is bound to
(IP, UA) — a stolen blob is useless off-IP. On THIS single-box deployment the
orchestrator and the analyst's Chrome egress from the same IP, so the IP-bound
lease validates for server-side reuse — the happy path, not the exception.

Cookies are encrypted at rest with Fernet; the key comes from the OS keyring
(ruling #10), falling back to a 0600 key file on headless boxes with no Secret
Service. OSIRIS_LEASE_KEY overrides both (used by tests).
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import asyncpg
import httpx
from cryptography.fernet import Fernet

from src.orchestrator.challenges import ChallengeDetected, detect

_KEYRING_SERVICE = "osiris"
_KEYRING_USER = "lease-encryption-key"


def get_lease_key() -> bytes:
    """Resolve the Fernet key: explicit env override -> OS keyring -> key file."""
    env = os.environ.get("OSIRIS_LEASE_KEY")
    if env:
        return env.encode()
    try:
        import keyring  # local import: backend probing can be slow / fail on servers

        existing = keyring.get_password(_KEYRING_SERVICE, _KEYRING_USER)
        if existing is None:
            existing = Fernet.generate_key().decode()
            keyring.set_password(_KEYRING_SERVICE, _KEYRING_USER, existing)
        return existing.encode()
    except Exception:
        # headless / no Secret Service -> protected key file (systemd-creds-style)
        path = Path(
            os.environ.get("OSIRIS_LEASE_KEY_FILE", "~/.config/osiris/lease.key")
        ).expanduser()
        if path.exists():
            return path.read_bytes()
        path.parent.mkdir(parents=True, exist_ok=True)
        key = Fernet.generate_key()
        path.write_bytes(key)
        path.chmod(0o600)
        return key


@dataclass
class Lease:
    id: int
    origin: str
    cookies: list[dict[str, Any]]
    ua: str
    bound_ip: str | None
    expires_at: datetime


class LeaseStore:
    def __init__(self, pool: asyncpg.Pool, key: bytes | None = None) -> None:
        self.pool = pool
        self.fernet = Fernet(key or get_lease_key())

    async def capture(
        self,
        origin: str,
        cookies: list[dict[str, Any]],
        ua: str,
        *,
        bound_ip: str | None,
        ttl_seconds: int,
        issued_by: str,
    ) -> int:
        token = self.fernet.encrypt(json.dumps(cookies).encode()).decode()
        lease_id: int = await self.pool.fetchval(
            "INSERT INTO cookie_leases (origin, cookie_blob, ua, bound_ip, expires_at, issued_by) "
            "VALUES ($1,$2,$3,$4, now() + make_interval(secs => $5), $6) RETURNING id",
            origin,
            {"ct": token},
            ua,
            bound_ip,
            ttl_seconds,
            issued_by,
        )
        return lease_id

    async def get(self, origin: str) -> Lease | None:
        row = await self.pool.fetchrow(
            "SELECT * FROM cookie_leases WHERE origin=$1 AND expires_at > now() "
            "ORDER BY expires_at DESC LIMIT 1",
            origin,
        )
        if row is None:
            return None
        cookies = json.loads(self.fernet.decrypt(row["cookie_blob"]["ct"].encode()))
        return Lease(
            id=row["id"],
            origin=row["origin"],
            cookies=cookies,
            ua=row["ua"],
            bound_ip=str(row["bound_ip"]) if row["bound_ip"] is not None else None,
            expires_at=row["expires_at"],
        )


def valid_for_server_egress(lease: Lease, current_ip: str) -> bool:
    """A lease is reusable server-side only from the IP it was bound to (DESIGN §9).
    On the single-box deployment that's trivially true — same egress IP."""
    return lease.bound_ip is not None and lease.bound_ip == current_ip


async def fetch_with_lease(
    url: str, lease: Lease, *, timeout_s: float = 30.0, detect_challenge: bool = True
) -> dict[str, Any]:
    """Server-side GET reusing the leased session (cookies + UA). Raises
    ChallengeDetected if the lease didn't get us past the wall — caller re-hands off."""
    cookies = {c["name"]: c["value"] for c in lease.cookies}
    headers = {"User-Agent": lease.ua} if lease.ua else {}
    async with httpx.AsyncClient(timeout=timeout_s, follow_redirects=True) as client:
        resp = await client.get(url, cookies=cookies, headers=headers)
    body = resp.text
    if detect_challenge:
        challenge = detect(status_code=resp.status_code, headers=dict(resp.headers), body=body)
        if challenge is not None:
            raise ChallengeDetected(challenge, url=url)
    return {"url": url, "status": resp.status_code, "html": body, "headers": dict(resp.headers)}


def origin_of(url: str) -> str:
    """Scheme://host[:port] — the lease scoping key."""
    parsed = httpx.URL(url)
    return f"{parsed.scheme}://{parsed.host}" + (f":{parsed.port}" if parsed.port else "")


__all__ = [
    "Lease",
    "LeaseStore",
    "fetch_with_lease",
    "get_lease_key",
    "origin_of",
    "valid_for_server_egress",
]
