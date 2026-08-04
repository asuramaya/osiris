"""'This is me' — the developer unifies their own identity across repos.

The resolver catches dev Persons that share a name/handle, but a real name and a handle
(priya ↔ asuramaya) share no deterministic key — that's a human assertion. The first
claim designates a canonical `self`; a later claim merges that identity into it.
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime

import httpx
import pytest_asyncio
from src.actions.core import Actions
from src.api.app import create_app

NOW = datetime(2026, 7, 2, tzinfo=UTC)


@pytest_asyncio.fixture
async def client(actions: Actions) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(actions.pool)
    app.state.pool = actions.pool
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def _dev(actions: Actions, email: str, name: str) -> str:
    o = await actions.create_or_find_object("Person", f"dev:{email}", "gitlog")
    await actions.assert_property(o, "name", name, "gitlog", NOW, 0.9)
    await actions.assert_property(o, "email", email, "gitlog", NOW, 0.9)
    return str(o)


async def test_first_claim_designates_then_a_later_claim_merges(
    client: httpx.AsyncClient, actions: Actions
) -> None:
    asura = await _dev(actions, "dakota.jm@gmail.com", "asuramaya")
    # same person, no shared key
    priya = await _dev(actions, "priya.kowalski42@gmail.com", "priya")

    # first claim → this Person becomes the canonical self
    r1 = (await client.post(f"/objects/{asura}/claim-identity")).json()
    assert r1["action"] == "designated" and r1["self"] == asura

    # a later claim → merge that identity INTO the self (winner = the self)
    r2 = (await client.post(f"/objects/{priya}/claim-identity")).json()
    assert r2["action"] == "merged" and r2["self"] == asura and r2["merged"] == priya

    p = actions.pool
    # priya is now merged into asuramaya (event-sourced projection), asuramaya stays active
    assert await p.fetchval("SELECT status FROM objects WHERE id=$1", priya) == "merged"
    assert await p.fetchval("SELECT merged_into FROM objects WHERE id=$1", priya) == \
        await p.fetchval("SELECT id FROM objects WHERE id=$1", asura)
    assert await p.fetchval("SELECT status FROM objects WHERE id=$1", asura) == "active"

    # re-claiming the self is a no-op
    r3 = (await client.post(f"/objects/{asura}/claim-identity")).json()
    assert r3["action"] == "already-self"


async def test_only_a_person_can_be_claimed(
    client: httpx.AsyncClient, actions: Actions
) -> None:
    c = await actions.create_or_find_object("Commit", "commit:abc", "gitlog")
    resp = await client.post(f"/objects/{c}/claim-identity")
    assert resp.status_code == 400
