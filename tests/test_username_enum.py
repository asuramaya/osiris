"""_probe's tri-state (60bc15db, specimen #6 of decision 01e0c69a): a network fault and a
confirmed-absent user must not collapse into the same bare None."""

from __future__ import annotations

from typing import Any

import httpx
import pytest
from src.connectors import username_enum
from src.connectors.username_enum import _probe, enumerate_username

_SITE = {"platform": "github", "url": "https://github.com/{u}", "require_status": 200}
_SITE_WITH_ABSENT_TEXT = {
    "platform": "hackernews", "url": "https://news.ycombinator.com/user?id={u}",
    "require_status": 200, "absent_text": "No such user.",
}


class _Input:
    def __init__(self, canonical: str) -> None:
        self.canonical = canonical


def _client(handler: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler), timeout=5.0)


async def test_probe_found_on_the_configured_success_status() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="profile")

    async with _client(handler) as client:
        r = await _probe(client, _SITE, "asuramaya")
    assert r == {"platform": "github", "status": "found",
                 "url": "https://github.com/asuramaya", "username": "asuramaya", "reason": None}


async def test_probe_absent_on_a_non_matching_status() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    async with _client(handler) as client:
        r = await _probe(client, _SITE, "nosuchuser")
    assert r["status"] == "absent"
    assert r["url"] is None
    assert r["reason"] is None


async def test_probe_absent_on_an_absent_text_marker() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="Oops! No such user.")

    async with _client(handler) as client:
        r = await _probe(client, _SITE_WITH_ABSENT_TEXT, "nosuchuser")
    assert r["status"] == "absent"


async def test_probe_inconclusive_on_a_transport_fault_not_absent() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("Connection refused")

    async with _client(handler) as client:
        r = await _probe(client, _SITE, "asuramaya")
    assert r["status"] == "inconclusive"
    assert r["url"] is None
    assert r["reason"] is not None and "ConnectError" in r["reason"]


async def test_enumerate_username_never_folds_inconclusive_into_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # one site found, one absent, one inconclusive -- the aggregate must keep all three
    # legible rather than the old shape (found vs. "everything else silently dropped").
    results = {
        "github": {"platform": "github", "status": "found",
                   "url": "https://github.com/asuramaya", "username": "asuramaya",
                   "reason": None},
        "gitlab": {"platform": "gitlab", "status": "absent", "url": None,
                   "username": "asuramaya", "reason": None},
        "keybase": {"platform": "keybase", "status": "inconclusive", "url": None,
                    "username": "asuramaya", "reason": "ConnectError: refused"},
    }

    async def fake_probe(client: Any, site: dict[str, Any], username: str) -> Any:
        return results[site["platform"]]

    monkeypatch.setattr(username_enum, "_probe", fake_probe)
    monkeypatch.setattr(
        username_enum, "SITES",
        [{"platform": p, "url": "https://x/{u}", "require_status": 200} for p in results])

    out = await enumerate_username(_Input("asuramaya"))
    assert out["accounts"] == [
        {"platform": "github", "url": "https://github.com/asuramaya", "username": "asuramaya"}]
    assert out["inconclusive"] == [{"platform": "keybase", "reason": "ConnectError: refused"}]


async def test_enumerate_username_carries_an_empty_inconclusive_list_when_all_resolved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # NEVER omitted, even when empty -- an omitted key here is the exact 60bc15db shape
    # this fix removes: "nothing inconclusive" and "never checked" must not look alike.
    async def fake_probe(client: Any, site: dict[str, Any], username: str) -> Any:
        return {"platform": site["platform"], "status": "absent", "url": None,
                "username": username, "reason": None}

    monkeypatch.setattr(username_enum, "_probe", fake_probe)
    out = await enumerate_username(_Input("nobody"))
    assert out["accounts"] == []
    assert out["inconclusive"] == []
