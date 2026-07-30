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


async def test_membrane_route_is_retired(client: httpx.AsyncClient) -> None:
    """THE INBOX (task #71, ruling 0b3dd431) replaced /membrane as :8011's front door —
    locking in the retirement as intentional, not an accidental regression. render_membrane
    itself stays in src/api/membrane.py, unrouted, for one deploy cycle (Thoth's own
    instruction, msg 1811) — this only proves the ROUTE is gone, not the module."""
    r = await client.get("/membrane")
    assert r.status_code == 404


async def test_inbox_route_wires_the_real_app_live(client: httpx.AsyncClient) -> None:
    """The one live-route test for THE INBOX at the real create_app() level (pure
    builder/render coverage lives in test_inbox_blocks.py/test_inbox_catalog.py/
    test_inbox_app.py) — this is the wiring check those can't cover on their own: the
    router is actually include_router()'d, and the static mount actually serves the
    vendored assets, from the SAME app real deploys boot."""
    r = await client.get("/")
    assert r.status_code == 200
    assert "<!doctype html>" in r.text.lower()
    css = await client.get("/static/app.css")
    assert css.status_code == 200
    js = await client.get("/static/datastar.js")
    assert js.status_code == 200


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


async def test_get_object_resolves_name_via_the_full_chain(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    """Task #97 workstream 3 (client half): osiris.js's objectDetail rendered the
    inspector's title from a raw `name`-property scan — a Practice (statement/
    failure_prevented/surface, no name) showed its canonical hash there even though
    every list view of the SAME object already resolved it correctly. GET /objects/{id}
    now carries a top-level `name` via resolve_label, same as /objects and the dossier."""
    p = await actions.create_or_find_object("Practice", "practice:gettest", "test")
    await actions.assert_property(p, "statement", "measure it yourself, not from memory",
                                  "test", datetime.now(UTC), 0.9)
    r = await client.get(f"/objects/{p}")
    assert r.status_code == 200
    assert r.json()["name"] == "measure it yourself, not from memory"


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


async def test_objects_list_resolves_labels_via_the_full_chain_and_disambiguates(
    client: httpx.AsyncClient, actions: Actions,
) -> None:
    """Task #97 workstream 3: /objects' own `name` used to be a raw SQL COALESCE
    (name/title/summary/subject/canonical only) — a Practice fell straight to its
    canonical hash. Now resolve_label + disambiguate_labels, same as every other
    consumer, with a `display_label` field alongside."""
    p = await actions.create_or_find_object("Practice", "practice:apitest", "test")
    await actions.assert_property(p, "statement", "an api-level practice test", "test",
                                  datetime.now(UTC), 0.9)
    r = await client.get("/objects", params={"type": "Practice"})
    assert r.status_code == 200
    row = next(o for o in r.json() if o["canonical"] == "practice:apitest")
    assert row["name"] == "an api-level practice test"
    assert row["display_label"]


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


async def test_object_card_title_uses_resolve_label_not_name_only(actions: Actions) -> None:
    """Task #97 workstream 3: _object_card (the watch/subscription card-preview
    endpoint) used to check ONLY the `name` property for its title — a Practice
    (statement/failure_prevented/surface, no name) rendered its raw canonical hash.
    Now shares the same resolve_label every other consumer does."""
    from src.api.app import _object_card

    p = await actions.create_or_find_object("Practice", "practice:cardtest", "test")
    await actions.assert_property(p, "statement", "measure it yourself", "test",
                                  datetime.now(UTC), 0.9)
    card = await _object_card(actions.pool, p)
    assert card is not None
    assert card["title"] == "measure it yourself"
    # the statement still appears in the generic properties list too (unchanged
    # behavior for any field besides `name`, which alone gets excluded)
    assert any(pr["name"] == "statement" for pr in card["properties"])
