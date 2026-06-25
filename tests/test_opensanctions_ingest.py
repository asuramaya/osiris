from __future__ import annotations

from src.actions.core import Actions
from src.ingest.opensanctions import ingest_ftm, parse_jsonl

_FTM = [
    {"id": "P1", "schema": "Person",
     "properties": {"name": ["Kim Jong Un"], "country": ["kp"], "topics": ["sanction"]}},
    {"id": "O1", "schema": "Company",
     "properties": {"name": ["Bureau 39"], "country": ["kp"]}},
    {"id": "R1", "schema": "Directorship",
     "properties": {"director": ["P1"], "organization": ["O1"]}},
    {"id": "P2", "schema": "Person", "properties": {"name": ["Relative X"]}},
    {"id": "R2", "schema": "Family", "properties": {"person": ["P1"], "relative": ["P2"]}},
    {"id": "R3", "schema": "Ownership",  # asset not in slice -> stubbed, edge still forms
     "properties": {"owner": ["P1"], "asset": ["MISSING"]}},
]


async def test_ingest_ftm_loads_entities_and_relationships(actions: Actions) -> None:
    counts = await ingest_ftm(actions, _FTM)
    assert counts["objects"] == 3  # P1, O1, P2 — relationship-entities are not nodes
    assert counts["stubs"] == 1    # MISSING (the absent ownership asset) is stubbed
    assert counts["links"] == 3    # directs, family, owns — the absent endpoint is bridged
    # the absent endpoint became a typed stub (Organization, per the ownership role)
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE canonical='MISSING' AND type='Organization'"
    ) == 1

    pid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE type='Person' AND canonical='P1'"
    )
    assert pid is not None
    name = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name='name'", pid
    )
    assert name == "Kim Jong Un"
    # authoritative-dataset provenance is stamped on the assertion
    ec = await actions.pool.fetchval(
        "SELECT evidence_class FROM current_assertions WHERE object_id=$1 AND name='name'", pid
    )
    assert ec == "authoritative_api"

    assert await actions.pool.fetchval("SELECT count(*) FROM links WHERE type='directs'") == 1
    assert await actions.pool.fetchval("SELECT count(*) FROM links WHERE type='family'") == 1
    assert await actions.pool.fetchval("SELECT count(*) FROM links WHERE type='owns'") == 1


def test_parse_jsonl_skips_blank_and_bad_lines() -> None:
    text = '{"id":"A","schema":"Person"}\n\n  \nnot-json\n{"id":"B","schema":"Company"}\n'
    rows = list(parse_jsonl(text))
    assert [r["id"] for r in rows] == ["A", "B"]
