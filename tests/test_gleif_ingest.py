"""GLEIF ingest: LEI records -> Organizations + ownership parents; shared-LEI fusion."""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.ingest.gleif import ingest_lei, parse_lei_record
from src.ontology.resolution import find_cross_base_candidates, resolve_cross_base

NOW = datetime(2026, 6, 26, tzinfo=UTC)

# the real GLEIF lei-record shape (trimmed)
_RECORD = {
    "attributes": {
        "lei": "54930043XZGB27CTOV49",
        "entity": {
            "legalName": {"name": "TESLA, INC."},
            "jurisdiction": "US-TX", "status": "ACTIVE",
            "legalAddress": {"city": "Dallas", "country": "US"},
        },
        "registration": {"status": "ISSUED"},
    }
}


def test_parse_lei_record() -> None:
    p = parse_lei_record(_RECORD)
    assert p["lei"] == "54930043XZGB27CTOV49"
    assert p["name"] == "TESLA, INC."
    assert p["jurisdiction"] == "US-TX"
    assert p["status"] == "ACTIVE"
    assert p["registration_status"] == "ISSUED"
    assert p["country"] == "US"


async def test_ingest_lei_with_parent(actions: Actions) -> None:
    rec = parse_lei_record(_RECORD)
    rec["parents"] = [
        {"lei": "ABCDEF0000000000PARENT", "name": "TESLA HOLDINGS", "kind": "ultimate-parent"},
    ]
    counts = await ingest_lei(actions, [rec])
    assert counts["objects"] == 1 and counts["links"] == 1

    oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='lei:54930043XZGB27CTOV49'")
    assert oid is not None
    lei = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name='lei'", oid)
    assert lei == "54930043XZGB27CTOV49"
    # the ultimate-parent link points to the parent LEI org
    parent = await actions.pool.fetchval(
        "SELECT to_id FROM links WHERE from_id=$1 AND type='ultimate_parent'", oid)
    assert parent is not None


async def test_shared_lei_is_deterministic_cross_base_signal(actions: Actions) -> None:
    # GLEIF mints the company by LEI; another base (edgar) has it by CIK but carries the
    # SAME lei property -> a shared-LEI candidate at 0.95, stronger than a name match.
    await ingest_lei(actions, [parse_lei_record(_RECORD)])
    gleif_id = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='lei:54930043XZGB27CTOV49'")

    ed = await actions.create_or_find_object("Organization", "cik:0001318605", "edgar")
    await actions.assert_property(ed, "name", "Tesla, Inc.", "edgar", NOW, 0.85)
    await actions.assert_property(ed, "lei", "54930043XZGB27CTOV49", "edgar", NOW, 0.85)

    await find_cross_base_candidates(actions.pool)
    merged = await resolve_cross_base(actions)
    assert merged >= 1
    # the two fuse into one entity
    assert await actions.resolve_object_id(gleif_id) == await actions.resolve_object_id(ed)
