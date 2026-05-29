from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest_asyncio
import redis.asyncio as aioredis
from src.actions.core import Actions
from src.api.app import create_app
from src.ontology.resolution import find_person_merge_candidates, review_tray
from src.orchestrator.budgets import BudgetLedger
from src.orchestrator.handoff import suspend
from src.orchestrator.manifests import load_manifests

HELPERS = Path(__file__).parent.parent / "helpers"
NOW = datetime(2026, 5, 28, tzinfo=UTC)


@pytest_asyncio.fixture
async def client(
    actions: Actions, redis_client: aioredis.Redis
) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(actions.pool)
    app.state.pool = actions.pool
    app.state.manifests = load_manifests(HELPERS)
    app.state.connectors = {}
    app.state.redis = redis_client
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def test_resolve_merge_candidate_via_api(
    client: httpx.AsyncClient, actions: Actions, case_id: str
) -> None:
    cid = uuid.UUID(case_id)
    for canon in ("pa", "pb"):
        oid = await actions.create_or_find_object("Person", canon, "analyst:test", cid)
        await actions.assert_property(
            oid, "email", "k@dprk.example", "analyst:test", NOW, 0.9, case_id=cid
        )
    await find_person_merge_candidates(actions.pool)
    cand = (await client.get("/merge-candidates")).json()[0]

    r = await client.post(f"/merge-candidates/{cand['id']}/resolve", json={"decision": "merged"})
    assert r.status_code == 200
    assert await review_tray(actions.pool) == []  # off the tray
    rows = await actions.pool.fetch("SELECT status FROM objects WHERE type='Person'")
    assert sorted(x["status"] for x in rows) == ["active", "merged"]


async def test_mark_subject_builds_identity_hub(
    client: httpx.AsyncClient, actions: Actions, case_id: str
) -> None:
    cid = uuid.UUID(case_id)
    acc = await actions.create_or_find_object("Account", "github:asuramaya", "analyst:test", cid)

    r = await client.post(f"/objects/{acc}/subject", json={"case_id": case_id})
    assert r.status_code == 200
    hub_id = uuid.UUID(r.json()["hub"])

    # a per-case subject Person hub was minted and linked to the account
    hub = await actions.pool.fetchrow("SELECT canonical FROM objects WHERE id=$1", hub_id)
    assert hub["canonical"] == f"subject:{case_id}"
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND to_id=$2 AND type='has_account'",
        hub_id, acc,
    ) == 1
    # both the account and the hub are tagged 'subject'
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM current_assertions WHERE object_id=$1 AND name='tag'"
        " AND value->>'tag' = 'subject'", acc,
    ) >= 1
    # idempotent: re-marking doesn't duplicate the has_account link
    await client.post(f"/objects/{acc}/subject", json={"case_id": case_id})
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND type='has_account'", hub_id
    ) == 1


async def test_handoff_lifecycle_via_api(
    client: httpx.AsyncClient, actions: Actions, redis_client: aioredis.Redis
) -> None:
    cid = uuid.UUID(
        str(await actions.pool.fetchval(
            "INSERT INTO cases (name, owner, budgets) VALUES ('h','a',$1) RETURNING id",
            {"max_human_handoffs": 5},
        ))
    )
    chan = await actions.create_or_find_object("TelegramChannel", "dprk_news", "a", cid)
    manifest = load_manifests(HELPERS)["telegram_channel_profile"]
    ledger = BudgetLedger(actions.pool, redis_client)
    hid = await suspend(actions, ledger, manifest, chan, cid, url="https://t.me/s/dprk_news",
                        challenge_kind=None)

    # tray shows it
    tray = (await client.get(f"/cases/{cid}/tray")).json()
    assert len(tray) == 1 and tray[0]["id"] == hid

    await client.post(f"/handoffs/{hid}/open")
    r = await client.post(f"/handoffs/{hid}/postback",
                          json={"result": {"title": "DPRK News", "subscribers": 9001}})
    assert r.status_code == 200 and r.json()["properties"] >= 2
    assert (await client.get(f"/cases/{cid}/tray")).json() == []  # resolved


async def test_node_management(client: httpx.AsyncClient, actions: Actions, case_id: str) -> None:
    cid = uuid.UUID(case_id)
    a = await actions.create_or_find_object("Domain", "n1.kp", "analyst:test", cid)
    b = await actions.create_or_find_object("Domain", "n2.kp", "analyst:test", cid)

    # add a property + a tag through the API (audited via Actions)
    await client.post(f"/objects/{a}/properties", json={"name": "note", "value": "seed"})
    await client.post(f"/objects/{a}/tags", json={"tag": "of-interest"})
    obj = (await client.get(f"/objects/{a}")).json()
    names = {p["name"] for p in obj["properties"]}
    assert "note" in names and "tag" in names

    # manual link
    r = await client.post("/links", json={"from_id": str(a), "to_id": str(b), "type": "related_to"})
    assert r.json()["link_id"] > 0
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND type='related_to'", a
    ) == 1

    # archive removes it from the case object list
    await client.post(f"/objects/{a}/archive")
    listed = {o["canonical"] for o in (await client.get(f"/objects?case_id={cid}")).json()}
    assert "n1.kp" not in listed and "n2.kp" in listed


async def test_case_management(client: httpx.AsyncClient, actions: Actions) -> None:
    cid = (await client.post("/cases", json={"name": "old name"})).json()["id"]
    await client.patch(f"/cases/{cid}", json={"name": "Lazarus dossier",
                                               "budgets": {"max_hop_distance": None}})
    got = (await client.get(f"/cases/{cid}")).json()
    assert got["name"] == "Lazarus dossier"
    assert got["budgets"]["max_hop_distance"] is None  # unbounded depth

    await client.post(f"/cases/{cid}/archive")
    ids = {c["id"] for c in (await client.get("/cases")).json()}
    assert cid not in ids  # archived cases drop off the list


async def test_case_stats(client: httpx.AsyncClient, actions: Actions) -> None:
    cid = uuid.UUID(
        str(await actions.pool.fetchval(
            "INSERT INTO cases (name, owner) VALUES ('s','a') RETURNING id"
        ))
    )
    await actions.create_or_find_object("Domain", "a.kp", "a", cid)
    await actions.create_or_find_object("Domain", "b.kp", "a", cid)
    await actions.create_or_find_object("Malware", "mw", "a", cid)

    stats = (await client.get(f"/cases/{cid}/stats")).json()
    assert stats["by_type"]["Domain"] == 2
    assert stats["total"] == 3
    assert stats["pending_handoffs"] == 0
