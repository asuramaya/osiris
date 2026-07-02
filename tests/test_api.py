from __future__ import annotations

import json
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest_asyncio
from src.actions.core import Actions
from src.api.app import create_app
from src.ontology.ingest import ingest_bundle

DPRK_BUNDLE = Path(__file__).parent / "fixtures" / "dprk_attack_bundle.json"
HELPERS = Path(__file__).parent.parent / "helpers"
LAZARUS = "intrusion-set--c93fccb1-e8e8-42cf-ae33-2ad1d183913a"
APPLEJEUS = "malware--6a0ef5d4-fc7c-4dda-85d7-592e4dbdc5d9"


@pytest_asyncio.fixture
async def client(actions: Actions) -> AsyncIterator[httpx.AsyncClient]:
    from src.orchestrator.manifests import load_manifests

    app = create_app(actions.pool)
    app.state.pool = actions.pool
    app.state.manifests = load_manifests(HELPERS)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def _seed(actions: Actions) -> uuid.UUID:
    cid = await actions.pool.fetchval(
        "INSERT INTO cases (name, owner) VALUES ('api','analyst:test') RETURNING id"
    )
    await ingest_bundle(actions, json.loads(DPRK_BUNDLE.read_text()), case_id=uuid.UUID(str(cid)))
    return uuid.UUID(str(cid))


async def test_health(client: httpx.AsyncClient) -> None:
    r = await client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


async def test_list_and_get_object(client: httpx.AsyncClient, actions: Actions) -> None:
    await _seed(actions)
    r = await client.get("/objects", params={"type": "IntrusionSet"})
    assert r.status_code == 200
    canonicals = {o["canonical"] for o in r.json()}
    assert LAZARUS in canonicals

    oid = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical=$1", LAZARUS)
    r = await client.get(f"/objects/{oid}")
    assert r.status_code == 200
    body = r.json()
    assert body["type"] == "IntrusionSet"
    names = {p["value"] for p in body["properties"] if p["name"] == "name"}
    assert "Lazarus Group" in names


async def test_objects_search_is_word_order_proof(
    client: httpx.AsyncClient, actions: Actions
) -> None:
    from src.orchestrator.capture import record_decision

    # summary carries some tokens, rationale others — so a query can span both properties
    d = await record_decision(
        actions, "the atomic claim uses a partial unique index",
        rationale="idempotent retry-safe dedup on active statuses",
    )

    async def ids(q: str) -> set[str]:
        r = await client.get("/objects", params={"q": q})
        assert r.status_code == 200
        return {o["id"] for o in r.json()}

    assert str(d) in await ids("claim atomic")        # reordered, both in summary
    assert str(d) in await ids("idempotent claim")    # cross-property AND (rationale + summary)
    assert str(d) in await ids("index atomic dedup")  # non-adjacent, both properties
    assert str(d) not in await ids("claim nonexistenttoken")  # every token must hit
    assert str(d) in await ids("idempotent")          # single-token behaviour unchanged


async def test_object_graph(client: httpx.AsyncClient, actions: Actions) -> None:
    await _seed(actions)
    oid = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical=$1", LAZARUS)
    r = await client.get(f"/objects/{oid}/graph", params={"hops": 1})
    g = r.json()
    types = {n["type"] for n in g["nodes"]}
    assert "IntrusionSet" in types and "Malware" in types
    assert any(e["type"] == "uses" for e in g["edges"])


async def test_available_helpers_from_manifest_registry(
    client: httpx.AsyncClient, actions: Actions
) -> None:
    await _seed(actions)
    applejeus = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical=$1", APPLEJEUS)
    r = await client.get(f"/objects/{applejeus}/helpers")
    assert "threatfox_malware_iocs" in {h["id"] for h in r.json()}


async def test_object_404(client: httpx.AsyncClient) -> None:
    r = await client.get(f"/objects/{uuid.uuid4()}")
    assert r.status_code == 404


async def test_list_cases_with_counts(client: httpx.AsyncClient, actions: Actions) -> None:
    cid = await _seed(actions)
    cases = (await client.get("/cases")).json()
    mine = next(c for c in cases if c["id"] == str(cid))
    assert mine["name"] == "api"
    assert mine["object_count"] >= 11  # the ingested ATT&CK objects


async def test_objects_scoped_to_case(client: httpx.AsyncClient, actions: Actions) -> None:
    cid = await _seed(actions)
    # an object in a *different* case must not appear when scoping to this one
    other = uuid.UUID(
        str(await actions.pool.fetchval(
            "INSERT INTO cases (name, owner) VALUES ('other','x') RETURNING id"
        ))
    )
    await actions.create_or_find_object("Domain", "elsewhere.test", "x", other)
    scoped = (await client.get(f"/objects?case_id={cid}")).json()
    canonicals = {o["canonical"] for o in scoped}
    assert LAZARUS in canonicals
    assert "elsewhere.test" not in canonicals


async def test_snapshot_time_travel(client: httpx.AsyncClient, actions: Actions) -> None:
    cid = uuid.UUID(
        str(
            await actions.pool.fetchval(
                "INSERT INTO cases (name, owner) VALUES ('snap','analyst:test') RETURNING id"
            )
        )
    )
    a = await actions.create_or_find_object("Domain", "early.test", "analyst:test", cid)
    cutoff = await actions.pool.fetchval("SELECT created_at FROM objects WHERE id=$1", a)
    await actions.create_or_find_object("Domain", "late.test", "analyst:test", cid)

    r = await client.get(f"/cases/{cid}/snapshot", params={"at": cutoff.isoformat()})
    canonicals = {o["canonical"] for o in r.json()["objects"]}
    assert "early.test" in canonicals
    assert "late.test" not in canonicals  # created after the cutoff


async def test_merge_candidates_endpoint(client: httpx.AsyncClient, actions: Actions) -> None:
    from src.ontology.resolution import find_person_merge_candidates

    cid = uuid.UUID(
        str(
            await actions.pool.fetchval(
                "INSERT INTO cases (name, owner) VALUES ('mc','analyst:test') RETURNING id"
            )
        )
    )
    for canon in ("pa", "pb"):
        oid = await actions.create_or_find_object("Person", canon, "analyst:test", cid)
        await actions.assert_property(
            oid, "email", "shared@x.com", "analyst:test", datetime.now(UTC), 0.9, case_id=cid
        )
    await find_person_merge_candidates(actions.pool)
    r = await client.get("/merge-candidates")
    assert len(r.json()) == 1
