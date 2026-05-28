from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from cryptography.fernet import Fernet
from src.actions.core import Actions
from src.connectors.leases import (
    Lease,
    LeaseStore,
    fetch_with_lease,
    origin_of,
    valid_for_server_egress,
)
from src.orchestrator.challenges import ChallengeDetected

KEY = Fernet.generate_key()


async def test_capture_get_roundtrip_and_encryption_at_rest(actions: Actions) -> None:
    store = LeaseStore(actions.pool, KEY)
    cookies = [{"name": "cf_clearance", "value": "sekret", "domain": ".x.com", "path": "/"}]
    lid = await store.capture(
        "https://x.com", cookies, "UA/1.0", bound_ip="127.0.0.1",
        ttl_seconds=900, issued_by="analyst:test",
    )
    lease = await store.get("https://x.com")
    assert lease is not None
    assert lease.cookies[0]["value"] == "sekret"
    assert lease.ua == "UA/1.0"
    assert lease.bound_ip == "127.0.0.1"
    # the blob at rest is ciphertext — plaintext cookie value never hits the DB
    raw = await actions.pool.fetchval("SELECT cookie_blob FROM cookie_leases WHERE id=$1", lid)
    assert "sekret" not in str(raw)


async def test_ip_binding_gates_server_reuse() -> None:
    lease = Lease(
        id=1, origin="https://x.com", cookies=[], ua="UA",
        bound_ip="203.0.113.7", expires_at=datetime.now(UTC) + timedelta(hours=1),
    )
    assert valid_for_server_egress(lease, "203.0.113.7") is True   # same egress IP
    assert valid_for_server_egress(lease, "203.0.113.8") is False  # off-IP -> void


async def test_expired_lease_is_not_returned(actions: Actions) -> None:
    store = LeaseStore(actions.pool, KEY)
    await store.capture(
        "https://y.com", [{"name": "a", "value": "b"}], "UA",
        bound_ip="127.0.0.1", ttl_seconds=-1, issued_by="analyst:test",
    )
    assert await store.get("https://y.com") is None


async def test_fetch_with_lease_reuses_session(actions: Actions, local_site: str) -> None:
    store = LeaseStore(actions.pool, KEY)
    cookies = [{"name": "session", "value": "secret123", "domain": "127.0.0.1", "path": "/"}]
    await store.capture(
        origin_of(local_site), cookies, "UA", bound_ip="127.0.0.1",
        ttl_seconds=900, issued_by="analyst:test",
    )
    lease = await store.get(origin_of(local_site))
    assert lease is not None
    res = await fetch_with_lease(f"{local_site}/protected", lease)
    assert "topsecret" in res["html"]  # the leased cookie got us past the wall


async def test_fetch_without_valid_cookie_detects_challenge(local_site: str) -> None:
    empty = Lease(
        id=1, origin=origin_of(local_site), cookies=[], ua="UA",
        bound_ip="127.0.0.1", expires_at=datetime.now(UTC) + timedelta(hours=1),
    )
    with pytest.raises(ChallengeDetected):
        await fetch_with_lease(f"{local_site}/protected", empty)
