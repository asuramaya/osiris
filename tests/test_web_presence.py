"""The base->crawl bridge: a federated entity's website becomes a crawlable seed."""
from __future__ import annotations

import uuid

from src.actions.core import Actions
from src.ingest.opensanctions import ingest_ftm
from src.orchestrator.enrich import seed_web_presence
from src.orchestrator.frontier import is_expandable


async def test_seed_web_presence_mints_crawlable_anchor(actions: Actions) -> None:
    cid = await actions.pool.fetchval(
        "INSERT INTO cases (name, owner) VALUES ('c','analyst') RETURNING id"
    )
    cid = uuid.UUID(str(cid))
    # a federated org with a website (the FtM loader stamps it AUTHORITATIVE_API)
    await ingest_ftm(actions, [{"id": "O1", "schema": "Company", "properties": {
        "name": ["Voltara Devices LLC"], "website": ["http://voltara.example/"]}}], case_id=cid)
    org = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='O1'")

    counts = await seed_web_presence(actions, org, case_id=cid)
    assert counts == {"urls": 1, "domains": 1}

    url = await actions.pool.fetchrow(
        "SELECT o.id, co.hop_distance FROM objects o "
        "JOIN case_objects co ON co.object_id=o.id AND co.case_id=$1 "
        "WHERE o.type='URL' AND o.canonical='http://voltara.example'", cid)
    assert url is not None
    assert url["hop_distance"] == 1  # one hop past the (hop-0) entity

    dom = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE type='Domain' AND canonical='voltara.example'")
    assert dom is not None

    # links are authoritative (the base declared the site) and point from the entity
    klass = await actions.pool.fetchval(
        "SELECT evidence_class FROM links WHERE from_id=$1 AND to_id=$2 AND type='has_url'",
        org, url["id"])
    assert klass == "authoritative_api"

    # the crux: anchor-grade inbound link => the frontier lets the cascade crawl it
    assert await is_expandable(actions.pool, cid, url["id"]) is True
    assert await is_expandable(actions.pool, cid, dom) is True


async def test_seed_web_presence_no_website_is_noop(actions: Actions) -> None:
    org = await actions.create_or_find_object("Organization", "O2", "analyst")
    assert await seed_web_presence(actions, org) == {"urls": 0, "domains": 0}
