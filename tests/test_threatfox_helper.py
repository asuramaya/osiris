from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest
from src.actions.core import Actions
from src.ontology.export import export_objects, subgraph_from
from src.ontology.ingest import ingest_bundle
from src.orchestrator.manifests import load_manifests
from src.orchestrator.runner import HelperRunError, claim_run, run_helper
from src.parsers.base import InputObject
from src.parsers.threatfox import parse_threatfox_iocs

FIXTURES = Path(__file__).parent / "fixtures"
HELPERS_DIR = Path(__file__).parent.parent / "helpers"
DPRK_BUNDLE = FIXTURES / "dprk_attack_bundle.json"
TF_RESPONSE = FIXTURES / "threatfox_applejeus.json"
APPLEJEUS = "malware--6a0ef5d4-fc7c-4dda-85d7-592e4dbdc5d9"


@pytest.fixture
def tf_response() -> dict:
    return json.loads(TF_RESPONSE.read_text())


def _applejeus_input(obj_id: uuid.UUID) -> InputObject:
    return InputObject(id=str(obj_id), type="Malware", canonical=APPLEJEUS,
                       properties={"name": "AppleJeus"})


# --- parser is pure: no DB needed ------------------------------------------

def test_parser_emits_indicators_and_links(tf_response: dict) -> None:
    inp = _applejeus_input(uuid.uuid4())
    result = parse_threatfox_iocs(tf_response, inp)
    # 2 IOCs -> 2 Indicator + 2 ObservedData
    assert sum(o.type == "Indicator" for o in result.objects) == 2
    assert sum(o.type == "ObservedData" for o in result.objects) == 2
    link_types = sorted(link.type for link in result.links)
    # per IOC: indicates(->malware) + based-on; first IOC also indicates(->T1566)
    assert link_types == ["based-on", "based-on", "indicates", "indicates", "indicates"]
    # the raw record rides along as evidence on ObservedData
    obs = next(o for o in result.objects if o.type == "ObservedData")
    assert obs.evidence is not None


def test_parser_ignores_error_response() -> None:
    inp = _applejeus_input(uuid.uuid4())
    assert parse_threatfox_iocs({"query_status": "error"}, inp).objects == []


# --- full run against the graph --------------------------------------------

async def _seed_and_run(actions: Actions, case_id: str, tf_response: dict) -> uuid.UUID:
    bundle = json.loads(DPRK_BUNDLE.read_text())
    await ingest_bundle(actions, bundle, case_id=case_id)
    applejeus_id = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1", APPLEJEUS
    )
    manifest = load_manifests(HELPERS_DIR)["threatfox_malware_iocs"]
    await run_helper(actions, manifest, tf_response, _applejeus_input(applejeus_id),
                     uuid.UUID(case_id))
    return uuid.UUID(str(applejeus_id))


async def test_ioc_to_ttp_chain(
    actions: Actions, case_id: str, tf_response: dict, monkeypatch, tmp_path
) -> None:
    monkeypatch.setenv("OSIRIS_ARTIFACT_DIR", str(tmp_path))
    applejeus = await _seed_and_run(actions, case_id, tf_response)

    # Indicators + ObservedData materialized
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Indicator'"
    ) == 2
    # both IOCs 'indicate' the AppleJeus malware (the consumed object)
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE to_id=$1 AND type='indicates'", applejeus
    ) == 2
    # the attack.T1566-tagged IOC links directly to the Phishing technique
    phishing = await actions.pool.fetchval(
        "SELECT object_id FROM current_assertions WHERE name='external_id' "
        "AND value #>> '{}' = 'T1566'"
    )
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE to_id=$1 AND type='indicates'", phishing
    ) == 1

    # CONVERGENCE: a fresh OSINT IOC is now reachable from Lazarus's TTP picture
    # (Indicator -> AppleJeus <- uses -- Lazarus). Prove the path exists.
    lazarus = await actions.pool.fetchval(
        "SELECT object_id FROM current_assertions WHERE name='external_id' "
        "AND value #>> '{}' = 'G0032'"
    )
    reachable = await actions.pool.fetchval(
        "SELECT count(*) > 0 FROM links uses JOIN links ind ON ind.to_id = uses.to_id "
        "WHERE uses.from_id=$1 AND uses.type='uses' AND ind.type='indicates'",
        lazarus,
    )
    assert reachable is True


async def test_evidence_is_content_addressed(
    actions: Actions, case_id: str, tf_response: dict, monkeypatch, tmp_path
) -> None:
    monkeypatch.setenv("OSIRIS_ARTIFACT_DIR", str(tmp_path))
    await _seed_and_run(actions, case_id, tf_response)
    rows = await actions.pool.fetch(
        "SELECT evidence_uri, evidence_sha256 FROM assertions "
        "WHERE source_id='threatfox_malware_iocs' AND evidence_sha256 IS NOT NULL"
    )
    assert rows
    for r in rows:
        assert len(r["evidence_sha256"]) == 64
        path = r["evidence_uri"].removeprefix("file://")
        assert Path(path).exists()


async def test_export_ioc_subgraph_as_stix(
    actions: Actions, case_id: str, tf_response: dict, monkeypatch, tmp_path
) -> None:
    monkeypatch.setenv("OSIRIS_ARTIFACT_DIR", str(tmp_path))
    await _seed_and_run(actions, case_id, tf_response)
    ind = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE type='Indicator' AND canonical LIKE 'ioc:ip:port%'"
    )
    ids = await subgraph_from(actions.pool, uuid.UUID(str(ind)), hops=1)
    bundle = await export_objects(actions.pool, ids)
    by_type: dict[str, int] = {}
    for o in bundle["objects"]:
        by_type[o["type"]] = by_type.get(o["type"], 0) + 1
    # the IOC's neighbourhood: itself + AppleJeus (malware) + its raw observed-data
    # + the Phishing attack-pattern, all valid STIX 2.1
    assert by_type.get("malware", 0) >= 1
    assert by_type.get("attack-pattern", 0) >= 1
    assert all(o["spec_version"] == "2.1" for o in bundle["objects"])


async def test_claim_is_exclusive_while_running(actions: Actions, case_id: str) -> None:
    obj = await actions.create_or_find_object("Malware", "m-claim", "analyst:test", case_id)
    first = await claim_run(actions, "threatfox_malware_iocs", obj, uuid.UUID(case_id), "open")
    assert first is not None
    # second claim while the first is still 'running' is refused by the partial index
    second = await claim_run(actions, "threatfox_malware_iocs", obj, uuid.UUID(case_id), "open")
    assert second is None


async def test_double_run_raises(
    actions: Actions, case_id: str, tf_response: dict, monkeypatch, tmp_path
) -> None:
    monkeypatch.setenv("OSIRIS_ARTIFACT_DIR", str(tmp_path))
    bundle = json.loads(DPRK_BUNDLE.read_text())
    await ingest_bundle(actions, bundle, case_id=case_id)
    applejeus_id = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1", APPLEJEUS
    )
    manifest = load_manifests(HELPERS_DIR)["threatfox_malware_iocs"]
    inp = _applejeus_input(applejeus_id)
    await run_helper(actions, manifest, tf_response, inp, uuid.UUID(case_id))
    # first run finished (status=done) -> a *new* run is allowed again
    await run_helper(actions, manifest, tf_response, inp, uuid.UUID(case_id))
    # but a manually-held running claim blocks run_helper
    await claim_run(actions, manifest.id, applejeus_id, uuid.UUID(case_id), "open")
    with pytest.raises(HelperRunError):
        await run_helper(actions, manifest, tf_response, inp, uuid.UUID(case_id))
