from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest_asyncio
from src.actions.core import Actions
from src.api.app import create_app
from src.orchestrator.federation import federated_query, promote, to_preview
from src.orchestrator.manifests import load_manifests
from src.parsers.base import InputObject

HELPERS = Path(__file__).parent.parent / "helpers"
CRT = load_manifests(HELPERS)["crtsh_subdomains"]


async def _fake_crtsh(input_object: InputObject) -> dict:
    d = input_object.canonical
    return {"domain": d, "certs": [{"name_value": f"mail.{d}\nvpn.{d}\nwww.{d}"}]}


async def _domain(actions: Actions, cid: uuid.UUID, canonical: str) -> InputObject:
    oid = await actions.create_or_find_object("Domain", canonical, "analyst:test", cid)
    return InputObject(id=str(oid), type="Domain", canonical=canonical)


async def test_federated_query_does_not_persist(actions: Actions, case_id: str) -> None:
    cid = uuid.UUID(case_id)
    inp = await _domain(actions, cid, "corp.kp")
    result = await federated_query(_fake_crtsh, "crtsh_subdomains", inp)

    # preview has the three subdomains...
    canons = {o.canonical for o in result.objects}
    assert canons == {"mail.corp.kp", "vpn.corp.kp", "www.corp.kp"}
    # ...but nothing besides the seed domain was written to the graph
    assert await actions.pool.fetchval("SELECT count(*) FROM objects WHERE type='Domain'") == 1
    preview = to_preview(result)
    assert len(preview["objects"]) == 3 and len(preview["links"]) == 3


async def test_promote_materializes_only_selected(actions: Actions, case_id: str) -> None:
    cid = uuid.UUID(case_id)
    inp = await _domain(actions, cid, "corp.kp")
    result = await federated_query(_fake_crtsh, "crtsh_subdomains", inp)

    counts = await promote(
        actions, result, source_id="crtsh_subdomains", input_object=inp,
        case_id=cid, selected=["mail.corp.kp"],
    )
    assert counts["objects"] == 1
    domains = {
        r["canonical"]
        for r in await actions.pool.fetch("SELECT canonical FROM objects WHERE type='Domain'")
    }
    assert domains == {"corp.kp", "mail.corp.kp"}  # only the chosen one materialized
    # promotion is provenance-attributed via a manual helper_run
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM helper_runs WHERE tier='manual' AND status='done'"
    ) == 1


@pytest_asyncio.fixture
async def client(actions: Actions) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(actions.pool)
    app.state.pool = actions.pool
    app.state.manifests = {"crtsh_subdomains": CRT}
    app.state.connectors = {"crtsh_subdomains": _fake_crtsh}
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def test_federate_then_promote_via_api(
    client: httpx.AsyncClient, actions: Actions, case_id: str
) -> None:
    cid = uuid.UUID(case_id)
    oid = await actions.create_or_find_object("Domain", "corp.kp", "analyst:test", cid)

    r = await client.post(
        "/federate", json={"helper_id": "crtsh_subdomains", "object_id": str(oid)}
    )
    assert r.status_code == 200
    assert {o["canonical"] for o in r.json()["objects"]} == {
        "mail.corp.kp", "vpn.corp.kp", "www.corp.kp"
    }
    # still nothing persisted from the federate call
    assert await actions.pool.fetchval("SELECT count(*) FROM objects WHERE type='Domain'") == 1

    r = await client.post(
        "/promote",
        json={
            "helper_id": "crtsh_subdomains",
            "object_id": str(oid),
            "case_id": str(cid),
            "selected": ["vpn.corp.kp", "www.corp.kp"],
        },
    )
    assert r.status_code == 200
    assert r.json()["objects"] == 2
    domains = {
        r["canonical"]
        for r in await actions.pool.fetch("SELECT canonical FROM objects WHERE type='Domain'")
    }
    assert domains == {"corp.kp", "vpn.corp.kp", "www.corp.kp"}
