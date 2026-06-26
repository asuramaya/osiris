"""ClinicalTrials.gov ingest: trial + sponsor + sites + investigators."""
from __future__ import annotations

from src.actions.core import Actions
from src.ingest.clinicaltrials import ingest_study, parse_study

_STUDY = {
    "protocolSection": {
        "identificationModule": {"nctId": "NCT06429735", "briefTitle": "PRIME BCI Study"},
        "statusModule": {"overallStatus": "RECRUITING",
                         "startDateStruct": {"date": "2024-01"}},
        "designModule": {"enrollmentInfo": {"count": 15}, "phases": ["NA"]},
        "sponsorCollaboratorsModule": {"leadSponsor": {"name": "Neuralink Corp"}},
        "contactsLocationsModule": {
            "overallOfficials": [
                {"name": "Francisco Ponce, MD", "role": "PRINCIPAL_INVESTIGATOR",
                 "affiliation": "Barrow Neurological Institute"},
            ],
            "locations": [
                {"facility": "Barrow Neurological Institute", "city": "Phoenix",
                 "state": "Arizona", "country": "United States"},
                {"facility": "University of Miami", "city": "Miami",
                 "state": "Florida", "country": "United States"},
            ],
        },
    },
}


def test_parse_study_flattens_facts() -> None:
    d = parse_study(_STUDY)
    assert d["nct"] == "NCT06429735"
    assert d["status"] == "RECRUITING"
    assert d["why_stopped"] is None          # honest: no stop reason
    assert d["has_results"] is False         # no posted adverse events/deaths
    assert d["enrollment"] == 15
    assert {loc["facility"] for loc in d["locations"]} == {
        "Barrow Neurological Institute", "University of Miami"}


async def test_ingest_study_materializes_trial_sites_investigators(actions: Actions) -> None:
    counts = await ingest_study(actions, parse_study(_STUDY))
    assert counts["trials"] == 1
    assert counts["sites"] == 2
    assert counts["investigators"] == 1

    trial = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE type='ClinicalTrial' AND canonical='nct:NCT06429735'"
    )
    assert trial is not None
    status = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name='status'", trial
    )
    assert status == "RECRUITING"

    # the sponsor runs it; sites + the surgeon hang off it
    sponsor = await actions.pool.fetchval(
        "SELECT from_id FROM links WHERE to_id=$1 AND type='sponsors'", trial
    )
    sp_name = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name='name'", sponsor
    )
    assert sp_name == "Neuralink Corp"

    sites = {
        r["nm"]
        for r in await actions.pool.fetch(
            "SELECT (SELECT value #>> '{}' FROM current_assertions a "
            "        WHERE a.object_id=l.to_id AND a.name='name' LIMIT 1) AS nm "
            "FROM links l WHERE l.from_id=$1 AND l.type='site'", trial)
    }
    assert sites == {"Barrow Neurological Institute", "University of Miami"}

    inv = await actions.pool.fetchval(
        "SELECT (SELECT value #>> '{}' FROM current_assertions a "
        "        WHERE a.object_id=l.to_id AND a.name='name' LIMIT 1) "
        "FROM links l WHERE l.from_id=$1 AND l.type='investigator' LIMIT 1", trial
    )
    assert inv == "Francisco Ponce, MD"
