from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest
from src.actions.core import Actions
from src.ontology import stix
from src.ontology.export import export_objects, subgraph_from
from src.ontology.ingest import ingest_bundle

FIXTURE = Path(__file__).parent / "fixtures" / "dprk_attack_bundle.json"
LAZARUS = "intrusion-set--c93fccb1-e8e8-42cf-ae33-2ad1d183913a"


@pytest.fixture
def bundle() -> dict:
    return json.loads(FIXTURE.read_text())


def _by_type(objs: list[dict]) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    for o in objs:
        out.setdefault(o["type"], []).append(o)
    return out


async def test_ingest_counts_and_skips(actions: Actions, case_id: str, bundle: dict) -> None:
    report = await ingest_bundle(actions, bundle, case_id=case_id)
    # 11 SDOs (4 intrusion-sets, 3 malware, 1 tool, 3 attack-patterns), 9 relationships,
    # 1 marking-definition skipped, no dangling refs.
    assert report.objects == 11
    assert report.links == 9
    assert report.skipped == 1
    assert report.dangling_refs == 0


async def test_ingest_is_idempotent(actions: Actions, case_id: str, bundle: dict) -> None:
    await ingest_bundle(actions, bundle, case_id=case_id)
    await ingest_bundle(actions, bundle, case_id=case_id)  # re-ingest the seed
    assert await actions.pool.fetchval("SELECT count(*) FROM objects") == 11
    # canonical = STIX id makes re-ingest a no-op create; one create event per object.
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM object_events WHERE event_type='create'"
    ) == 11


async def test_external_id_reverse_lookup(actions: Actions, case_id: str, bundle: dict) -> None:
    await ingest_bundle(actions, bundle, case_id=case_id)
    # helpers link OSINT findings to ATT&CK objects by handle (G0032) -> object
    row = await actions.pool.fetchrow(
        "SELECT object_id FROM current_assertions WHERE name='external_id' "
        "AND value #>> '{}' = 'G0032'"
    )
    obj = await actions.pool.fetchrow("SELECT canonical, type FROM objects WHERE id=$1",
                                      row["object_id"])
    assert obj["canonical"] == LAZARUS
    assert obj["type"] == "IntrusionSet"


async def test_dprk_ttp_graph(actions: Actions, case_id: str, bundle: dict) -> None:
    """The product question: what does North Korea (Lazarus) use?"""
    await ingest_bundle(actions, bundle, case_id=case_id)
    lazarus = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1", LAZARUS
    )
    used = await actions.pool.fetch(
        "SELECT o.type, o.canonical FROM links l JOIN objects o ON o.id = l.to_id "
        "WHERE l.from_id=$1 AND l.type='uses'",
        lazarus,
    )
    used_types = sorted(r["type"] for r in used)
    # Lazarus uses 3 things in the fixture: 3 malware + 1 attack-pattern? -> 2 malware + AppleJeus
    assert used_types == ["AttackPattern", "Malware", "Malware", "Malware"]
    assert len(used) == 4


async def test_roundtrip_semantic_equivalence(
    actions: Actions, case_id: str, bundle: dict
) -> None:
    await ingest_bundle(actions, bundle, case_id=case_id)
    all_ids = {
        r["id"]
        for r in await actions.pool.fetch("SELECT id FROM objects")
    }
    out = await export_objects(actions.pool, all_ids)

    src = _by_type([o for o in bundle["objects"] if o["type"] not in stix.SKIP_STIX_TYPES])
    dst = _by_type(out["objects"])

    # every input SDO id survives with type + name + external_id intact
    src_sdos = {o["id"]: o for t, lst in src.items() if t != "relationship" for o in lst}
    dst_sdos = {o["id"]: o for t, lst in dst.items() if t != "relationship" for o in lst}
    assert set(src_sdos) == set(dst_sdos)
    for sid, s in src_sdos.items():
        d = dst_sdos[sid]
        assert d["type"] == s["type"]
        assert d.get("name") == s.get("name")
        assert stix.mitre_external_id(d) == stix.mitre_external_id(s)

    # every input relationship (source, type, target) survives
    def rel_set(objs: list[dict]) -> set[tuple[str, str, str]]:
        return {
            (o["source_ref"], o["relationship_type"], o["target_ref"])
            for o in objs
            if o["type"] == "relationship"
        }

    assert rel_set(bundle["objects"]) == rel_set(out["objects"])
    # exported bundle is well-formed STIX 2.1
    assert out["type"] == "bundle"
    assert out["id"].startswith("bundle--")


async def test_export_lazarus_dossier(actions: Actions, case_id: str, bundle: dict) -> None:
    """Subgraph export = input 'North Korea/Lazarus' -> bundle of its TTPs out."""
    await ingest_bundle(actions, bundle, case_id=case_id)
    lazarus = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical=$1", LAZARUS)
    ids = await subgraph_from(actions.pool, uuid.UUID(str(lazarus)), hops=1)
    out = await export_objects(actions.pool, ids)

    names = {o.get("name") for o in out["objects"] if o["type"] != "relationship"}
    assert "Lazarus Group" in names
    assert {"FALLCHILL", "BLINDINGCAN", "AppleJeus", "Spearphishing Attachment"} <= names
    # APT38 (not directly linked to Lazarus in the fixture) is NOT pulled in at 1 hop
    assert "APT38" not in names
