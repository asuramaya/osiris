"""OrgBook BC ingest: BC corporate registry -> verified Organization nodes.

Hermetic — the network is the `search_topics` seam; these drive the pure parser and the
materializer with a fixture topic (the real OrgBook response shape), never calling BC.
"""
from __future__ import annotations

from src.actions.core import Actions
from src.ingest.orgbook import ingest_topics, parse_topic

# the real OrgBook v4 /search/topic shape (trimmed)
_TOPIC = {
    "source_id": "BC1110898",
    "names": [
        {"text": "BRILLIANT PHOENIX CAPITAL MANAGEMENT INC.", "type": "entity_name"},
        {"text": "725078695", "type": "business_number"},
    ],
    "attributes": [
        {"type": "registration_date", "value": "2017-03-13T21:04:26+00:00"},
        {"type": "entity_status", "value": "ACT"},
        {"type": "entity_type", "value": "BC"},
        {"type": "home_jurisdiction", "value": "BC"},
    ],
}


def test_parse_topic_extracts_registration_record() -> None:
    p = parse_topic(_TOPIC)
    assert p["source_id"] == "BC1110898"
    assert p["name"] == "BRILLIANT PHOENIX CAPITAL MANAGEMENT INC."
    assert p["business_number"] == "725078695"
    assert p["status"] == "active"          # ACT -> readable
    assert p["entity_type"] == "BC"
    assert p["jurisdiction"] == "BC"
    assert p["registration_date"] == "2017-03-13"   # date only


def test_parse_topic_uses_registered_jurisdiction_fallback() -> None:
    # an extraprovincial LP carries registered_jurisdiction, not home_jurisdiction
    lp = {
        "source_id": "XP0893803",
        "names": [{"text": "BRILLIANT PHOENIX NEURALINK LIMITED PARTNERSHIP",
                   "type": "entity_name"}],
        "attributes": [{"type": "registered_jurisdiction", "value": "BC"},
                       {"type": "entity_type", "value": "XP"}],
    }
    assert parse_topic(lp)["jurisdiction"] == "BC"


async def test_ingest_materializes_bc_registration(actions: Actions) -> None:
    counts = await ingest_topics(actions, [parse_topic(_TOPIC)])
    assert counts["objects"] == 1

    oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE type='Organization' AND canonical='bc-reg:BC1110898'"
    )
    assert oid is not None
    name = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name='name'", oid
    )
    assert name == "BRILLIANT PHOENIX CAPITAL MANAGEMENT INC."
    # the registration is graded as authoritative
    klass = await actions.pool.fetchval(
        "SELECT evidence_class FROM current_assertions WHERE object_id=$1 AND name='name'", oid
    )
    assert klass == "authoritative_api"
    bn = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
        "AND name='business_number'", oid
    )
    assert bn == "725078695"


async def test_ingest_skips_topics_without_legal_name(actions: Actions) -> None:
    # a topic whose only name is a bare business number is not a usable entity
    junk = {"source_id": "X", "names": [{"text": "999", "type": "business_number"}],
            "attributes": []}
    assert (await ingest_topics(actions, [parse_topic(junk)]))["objects"] == 0
