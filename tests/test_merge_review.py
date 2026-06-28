"""Merge review — resolve in place. The tray lists labelled candidate pairs (no raw
uuids), and an analyst confirms or rejects each through the Actions layer."""
from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime

import httpx
import pytest_asyncio
from src.actions.core import Actions
from src.api.app import create_app
from src.ontology.resolution import find_cross_base_candidates

NOW = datetime(2026, 6, 27, tzinfo=UTC)


@pytest_asyncio.fixture
async def client(actions: Actions) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(actions.pool)
    app.state.pool = actions.pool
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def test_tray_lists_labelled_pairs_then_resolves_in_place(
    client: httpx.AsyncClient, actions: Actions
) -> None:
    # the same org across two bases (different provenance, names differ by legal form)
    a = await actions.create_or_find_object("Organization", "cik:1", "edgar")
    await actions.assert_property(a, "name", "Acme Holdings Inc", "edgar", NOW, 0.85)
    b = await actions.create_or_find_object("Organization", "lei:XYZ", "gleif")
    await actions.assert_property(b, "name", "Acme Holdings", "gleif", NOW, 0.85)
    assert await find_cross_base_candidates(actions.pool) >= 1

    tray = (await client.get("/merge-candidates")).json()
    assert tray and tray[0]["a_type"] == "Organization"
    assert "Acme" in tray[0]["a_label"] and "Acme" in tray[0]["b_label"]  # named, not uuids

    cid = tray[0]["id"]
    r = await client.post(f"/merge-candidates/{cid}/resolve", json={"decision": "merged"})
    assert r.json()["resolved"] == "merged"

    statuses = {
        row["status"]
        for row in await actions.pool.fetch(
            "SELECT status FROM objects WHERE id = ANY($1::uuid[])", [a, b]
        )
    }
    assert "merged" in statuses  # one side merged into the other
    assert (await client.get("/merge-candidates")).json() == []  # tray cleared


async def test_reject_records_negative_memory(
    client: httpx.AsyncClient, actions: Actions
) -> None:
    a = await actions.create_or_find_object("Organization", "cik:2", "edgar")
    await actions.assert_property(a, "name", "Globex Corp", "edgar", NOW, 0.85)
    b = await actions.create_or_find_object("Organization", "lei:GLBX", "gleif")
    await actions.assert_property(b, "name", "Globex", "gleif", NOW, 0.85)
    await find_cross_base_candidates(actions.pool)
    cid = (await client.get("/merge-candidates")).json()[0]["id"]

    await client.post(f"/merge-candidates/{cid}/resolve", json={"decision": "rejected"})
    # both objects stay active; a not_same_as edge is recorded (negative memory)
    active = await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE id = ANY($1::uuid[]) AND status='active'", [a, b]
    )
    assert active == 2
    assert await actions.pool.fetchval("SELECT count(*) FROM links WHERE type='not_same_as'") == 2
