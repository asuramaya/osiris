from __future__ import annotations

from src.actions.core import Actions
from src.ingest.wikidata import ingest_entities, parse_entities


def _val(t: str, value: object) -> dict[str, object]:
    return {"mainsnak": {"snaktype": "value", "datavalue": {"type": t, "value": value}}}


# Q1 is a Person stub already in the graph (an OpenSanctions endpoint); Q2 is named
# in the same batch; Q3 is referenced by a relationship claim but absent -> stubbed.
_ENT = {
    "Q1": {
        "id": "Q1",
        "labels": {"en": {"language": "en", "value": "Kim Jong Un"}},
        "descriptions": {"en": {"language": "en", "value": "Supreme Leader of North Korea"}},
        "claims": {
            "P31": [_val("wikibase-entityid", {"id": "Q5"})],
            "P569": [_val("time", {"time": "+1984-01-08T00:00:00Z", "precision": 11})],
            "P26": [_val("wikibase-entityid", {"id": "Q2"})],   # spouse (in batch)
            "P40": [_val("wikibase-entityid", {"id": "Q3"})],   # child (absent -> stub)
        },
    },
    "Q2": {
        "id": "Q2",
        "labels": {"en": {"language": "en", "value": "Ri Sol-ju"}},
        "claims": {"P31": [_val("wikibase-entityid", {"id": "Q5"})]},
    },
}


async def test_enriches_stub_in_place_with_class(actions: Actions) -> None:
    # an OpenSanctions-style bare Person stub keyed by the Wikidata id
    stub = await actions.create_or_find_object("Person", "Q1", "opensanctions")

    counts = await ingest_entities(actions, _ENT, relationships=True)

    assert counts["enriched"] == 2          # Q1 + Q2 got properties
    assert counts["links"] == 2             # spouse + child
    assert counts["endpoints"] == 1         # Q3 stubbed (referenced, not in batch)

    # the stub was enriched IN PLACE — same object id, no duplicate
    same = await actions.create_or_find_object("Person", "Q1", "opensanctions")
    assert same == stub
    name = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name='name'", stub
    )
    assert name == "Kim Jong Un"
    ec = await actions.pool.fetchval(
        "SELECT evidence_class FROM current_assertions WHERE object_id=$1 AND name='name'", stub
    )
    assert ec == "authoritative_api"
    bd = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions "
        "WHERE object_id=$1 AND name='birthDate'", stub
    )
    assert bd == "1984-01-08"

    # the absent child endpoint became a typed Person stub the edge points at
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE canonical='Q3' AND type='Person'"
    ) == 1
    assert await actions.pool.fetchval("SELECT count(*) FROM links WHERE type='spouse'") == 1
    assert await actions.pool.fetchval("SELECT count(*) FROM links WHERE type='family'") == 1


async def test_properties_only_skips_relationships(actions: Actions) -> None:
    counts = await ingest_entities(actions, _ENT, relationships=False)
    assert counts["links"] == 0
    assert counts["endpoints"] == 0
    assert counts["enriched"] == 2


async def test_ingest_mints_official_social_accounts(actions: Actions) -> None:
    ent = {"Q1": {
        "id": "Q1",
        "labels": {"en": {"language": "en", "value": "Neuralink"}},
        "claims": {
            "P2002": [_val("string", "neuralink")],          # twitter/X
            "P2013": [_val("string", "neuralinkcorporation")],  # facebook
            "P4264": [_val("string", "neuralink")],          # linkedin
        },
    }}
    counts = await ingest_entities(actions, ent)
    assert counts["accounts"] == 3
    org = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='Q1'")
    accts = {
        r["canonical"] for r in await actions.pool.fetch(
            "SELECT o.canonical FROM links l JOIN objects o ON o.id=l.to_id "
            "WHERE l.from_id=$1 AND l.type='has_account'", org)
    }
    assert accts == {"twitter:neuralink", "facebook:neuralinkcorporation", "linkedin:neuralink"}
    klass = await actions.pool.fetchval(
        "SELECT evidence_class FROM links WHERE from_id=$1 AND type='has_account' LIMIT 1", org)
    assert klass == "authoritative_api"


def test_parse_entities_drops_missing() -> None:
    data = {"entities": {
        "Q1": {"id": "Q1", "labels": {}},
        "Q999": {"id": "Q999", "missing": ""},
    }}
    parsed = parse_entities(data)
    assert set(parsed) == {"Q1"}
