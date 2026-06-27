"""Harris County foreclosure vertical — watcher, object-level beat matching, and the
leads feed read-model. The broker beat (ForeScan, done right): a notice → a graded
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


async def test_seed_populates_the_sourced_leads_feed(client: httpx.AsyncClient) -> None:
    seed = (await client.post("/demo/foreclosure-seed")).json()
    assert seed["ingested"] == 8 and seed["leads_total"] == 8

    leads = (await client.get("/leads")).json()
    assert len(leads) == 8
    top = leads[0]  # newest filed first
    assert top["filed_date"] == "2026-06-26"
    assert top["demo"] == "true"
    # every lead carries its provenance (the whole point)
    assert top["provenance"]["source_label"] == "Harris County Clerk"
    assert top["provenance"]["how"] == "authoritative county record"
    assert round(top["provenance"]["confidence"], 2) == 0.85  # PG real is float4 (approx)


async def test_feed_filters_narrow_to_a_beat(client: httpx.AsyncClient) -> None:
    await client.post("/demo/foreclosure-seed")
    z = (await client.get("/leads", params={"zip": "77084"})).json()
    assert z and all(x["zip"] == "77084" for x in z)
    cheap = (await client.get("/leads", params={"max_bid": 200000})).json()
    assert cheap and all(float(x["opening_bid"]) <= 200000 for x in cheap)
    q = (await client.get("/leads", params={"q": "cypress"})).json()
    assert q and all("cypress" in x["address"].lower() for x in q)


async def test_saved_beat_fires_an_alert_on_a_matching_new_notice(
    client: httpx.AsyncClient
) -> None:
    # a beat: sub-$200k foreclosures anywhere
    await client.post("/subscriptions", json={
        "name": "cheap", "criteria": {
            "event_types": ["object_created"], "object_type": "Property",
            "where": [{"property": "opening_bid", "op": "lt", "value": 200000}]}})
    seed = (await client.post("/demo/foreclosure-seed")).json()
    assert seed["alerts_fired"] == 3  # the three sub-$200k notices in the demo set
    alerts = (await client.get("/alerts")).json()
    assert len(alerts) == 3
