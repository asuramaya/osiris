from __future__ import annotations

import uuid
from collections.abc import AsyncIterator

import httpx
import pytest_asyncio
from src.actions.core import Actions
from src.api.app import create_app
from src.ontology.classify import classify


def test_classify_common_selectors() -> None:
    assert classify("Alice@Example.com") == "Email"
    assert classify("8.8.8.8") == "IPv4"
    assert classify("2001:db8::1") == "IPv6"
    assert classify("evil.kp") == "Domain"
    assert classify("https://t.me/dprk_news") == "URL"
    assert classify("@dprk_news") == "TelegramChannel"
    _sha = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
    assert classify(_sha) == "FileHash"
    assert classify("1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2") == "BTCAddress"
    assert classify("+1 415 555 0100") == "Phone"
    assert classify("lazarus group attribution") == "Phrase"  # free text -> dorking seed


@pytest_asyncio.fixture
async def client(actions: Actions) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(actions.pool)
    app.state.pool = actions.pool
    app.state.manifests = {}
    app.state.connectors = {}

    class _Redis:  # the front-door endpoints under test don't touch redis
        async def aclose(self) -> None: ...

    app.state.redis = _Redis()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def test_create_case_and_paste_anything(
    client: httpx.AsyncClient, actions: Actions
) -> None:
    # operator creates a case from the UI
    r = await client.post("/cases", json={"name": "Lazarus hunt"})
    assert r.status_code == 200
    cid = r.json()["id"]

    # operator pastes a raw email — classified + created, no type given
    r = await client.post(f"/cases/{cid}/intake", json={"raw": "John.Doe@Gmail.com"})
    body = r.json()
    assert body["type"] == "Email"
    oid = uuid.UUID(body["object_id"])

    # it's in the case, canonicalized, with the raw form preserved
    obj = (await client.get(f"/objects/{oid}")).json()
    assert obj["canonical"] == "johndoe@gmail.com"
    scoped = (await client.get(f"/objects?case_id={cid}")).json()
    assert oid in {uuid.UUID(o["id"]) for o in scoped}


async def test_intake_respects_explicit_type(
    client: httpx.AsyncClient, actions: Actions
) -> None:
    cid = (await client.post("/cases", json={"name": "c"})).json()["id"]
    r = await client.post(
        f"/cases/{cid}/intake", json={"raw": "Lazarus Group", "type": "IntrusionSet"}
    )
    assert r.json()["type"] == "IntrusionSet"
