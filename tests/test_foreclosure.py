"""Harris County foreclosure vertical — watcher, object-level beat matching, and the
leads feed read-model. The broker beat (old-venture, done right): a notice → a graded
Property node → a sourced lead, with a beat that fires only on what it cares about.
"""
from __future__ import annotations

import uuid
from collections.abc import AsyncIterator

import httpx
import pytest_asyncio
from src.actions.core import Actions
from src.api.app import create_app
from src.ingest.harris_foreclosure import (
    SAMPLE_NOTICES,
    demo_fetch,
    make_harris_foreclosure_watcher,
)
from src.orchestrator.monitor import GraphEvent, get_cursor, matches, tick


def _ev(**kw: object) -> GraphEvent:
    base: dict[str, object] = dict(
        outbox_id=1, event_type="object_created", object_id=uuid.uuid4(), case_id=None,
        object_type="Property", canonical="harris-notice:DEMO-0001", payload={},
        value=None, props={},
    )
    base.update(kw)
    return GraphEvent(**base)  # type: ignore[arg-type]


# --- object-level beat matching (the where-clause) --------------------------

def test_beat_where_clause_conjunction() -> None:
    ev = _ev(props={"zip": "77084", "opening_bid": "248000",
                    "address": "18330 Olive Leaf Dr"})
    # zip AND under-300k -> a match
    assert matches({"object_type": "Property", "where": [
        {"property": "zip", "op": "eq", "value": "77084"},
        {"property": "opening_bid", "op": "lt", "value": 300000}]}, ev) is not None
    # the price ceiling excludes it
    too_pricey = {"where": [{"property": "opening_bid", "op": "lt", "value": 200000}]}
    assert matches(too_pricey, ev) is None
    # case-insensitive substring on the address
    assert matches({"where": [{"property": "address", "op": "contains",
                               "value": "olive leaf"}]}, ev) is not None
    # a missing property fails closed
    assert matches({"where": [{"property": "ghost", "op": "eq", "value": "x"}]}, ev) is None


# --- the watcher ------------------------------------------------------------

async def test_watcher_ingests_notices_as_graded_property_objects(actions: Actions) -> None:
    watcher = make_harris_foreclosure_watcher(fetch=demo_fetch)
    n = await tick(actions, "harris", watcher)
    assert n == len(SAMPLE_NOTICES)
    row = await actions.pool.fetchrow(
        "SELECT a.value #>> '{}' AS v, a.evidence_class AS ec FROM current_assertions a "
        "JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical='harris-notice:DEMO-0001' AND a.name='owner'"
    )
    assert "Rivera" in row["v"]
    assert row["ec"] == "authoritative_api"  # a county record is authoritative
    assert await get_cursor(actions.pool, "source:harris") == "2026-06-26"


async def test_watcher_only_emits_newer_and_is_quiet_with_no_news(actions: Actions) -> None:
    watcher = make_harris_foreclosure_watcher(fetch=demo_fetch)
    assert await tick(actions, "h", watcher) == len(SAMPLE_NOTICES)
    assert await tick(actions, "h", watcher) == 0  # same feed, nothing newer filed


# --- the leads feed + demo seed (through the API) ---------------------------

@pytest_asyncio.fixture
async def client(actions: Actions) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(actions.pool)
    app.state.pool = actions.pool
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


def _prop(card: dict, name: str) -> object:
    return next(f["value"] for f in card["properties"] if f["name"] == name)


async def test_demo_loads_the_generic_watch_feed(client: httpx.AsyncClient) -> None:
    """The demo loader ingests notices + ensures one watch; the GENERIC /matches feed
    renders them as type-driven sourced cards — the surface names no vertical."""
    seed = (await client.post("/demo/foreclosure-seed")).json()
    assert seed["ingested"] == 8
    cards = (await client.get("/matches", params={"subscription_id": seed["watch_id"]})).json()
    assert len(cards) == 8

    olive = next(c for c in cards if "Olive Leaf" in c["title"])
    assert olive["type"] == "Property"            # rendered by type, not as "a lead"
    assert "18330 Olive Leaf" in olive["title"]   # title = the address (the object's name)
    assert _prop(olive, "owner").startswith("DEMO")
    # the provenance block is the whole point — generic, true to the source
    assert olive["provenance"]["source_label"] == "Harris County Clerk"
    assert olive["provenance"]["how"] == "authoritative record"
    assert olive["provenance"]["demo"] is True
    assert round(olive["provenance"]["confidence"], 2) == 0.85  # PG real is float4


async def test_a_narrower_watch_matches_fewer(client: httpx.AsyncClient) -> None:
    await client.post("/demo/foreclosure-seed")  # 8 Property objects in the graph
    sub = (await client.post("/subscriptions", json={"name": "cheap", "criteria": {
        "object_type": "Property",
        "where": [{"property": "opening_bid", "op": "lt", "value": 200000}]}})).json()
    cards = (await client.get("/matches", params={"subscription_id": sub["id"]})).json()
    assert len(cards) == 3  # the three sub-$200k notices
    assert all(float(_prop(c, "opening_bid")) < 200000 for c in cards)


async def test_object_view_carries_per_property_provenance(client: httpx.AsyncClient) -> None:
    """The shared Object view (object.html) renders /objects/{id}; each fact must carry
    how it was obtained + a source label — the unifying atom both surfaces open into."""
    seed = (await client.post("/demo/foreclosure-seed")).json()
    cards = (await client.get("/matches", params={"subscription_id": seed["watch_id"]})).json()
    oid = cards[0]["object_id"]
    obj = (await client.get(f"/objects/{oid}")).json()
    assert obj["type"] == "Property"
    bid = next(p for p in obj["properties"] if p["name"] == "opening_bid")
    assert bid["how"] == "authoritative record"
    assert bid["source_label"] == "Harris County Clerk"
    assert bid["evidence_class"] == "authoritative_api"


async def test_a_saved_beat_fires_a_scoped_alert(client: httpx.AsyncClient) -> None:
    sub = (await client.post("/subscriptions", json={"name": "cheap", "criteria": {
        "event_types": ["object_created"], "object_type": "Property",
        "where": [{"property": "opening_bid", "op": "lt", "value": 200000}]}})).json()
    await client.post("/demo/foreclosure-seed")  # tick + evaluate (prospective)
    alerts = (await client.get("/alerts", params={"subscription_id": sub["id"]})).json()
    assert len(alerts) == 3  # exactly the sub-$200k notices fired for THIS beat
