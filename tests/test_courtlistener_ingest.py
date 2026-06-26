"""CourtListener ingest: court cases linked to a subject by party status."""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.ingest.courtlistener import ingest_cases, parse_case

NOW = datetime(2026, 6, 26, tzinfo=UTC)

_RESULT = {
    "docket_id": 65749974, "caseName": "SEC v. Scam Coin LLC",
    "court": "District Court, S.D.N.Y.", "court_id": "nysd", "dateFiled": "2024-03-01",
    "docketNumber": "24-cv-001", "assignedTo": "Jane Judge", "suitNature": "Securities Fraud",
    "party": ["Securities and Exchange Commission", "Scam Coin LLC"],
    "attorney": ["A. Lawyer"], "firm": ["Big Law LLP"],
    "docket_absolute_url": "/docket/65749974/sec-v-scam-coin/",
}


def test_parse_case_extracts_parties_and_metadata() -> None:
    p = parse_case(_RESULT)
    assert p["key"] == "65749974"
    assert p["case_name"] == "SEC v. Scam Coin LLC"
    assert p["nature"] == "Securities Fraud"
    assert p["judge"] == "Jane Judge"
    assert "Scam Coin LLC" in p["parties"]
    assert p["url"].endswith("/docket/65749974/sec-v-scam-coin/")


async def test_ingest_links_subject_as_party(actions: Actions) -> None:
    subject = await actions.create_or_find_object("Organization", "company:scam coin", "x")
    await actions.assert_property(subject, "name", "Scam Coin LLC", "x", NOW, 0.85)

    counts = await ingest_cases(
        actions, [parse_case(_RESULT)], subject_id=subject, subject_name="Scam Coin LLC"
    )
    assert counts == {"cases": 1, "links": 1}

    case = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE type='CourtCase' AND canonical='courtlistener:65749974'"
    )
    assert case is not None
    nature = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name='nature'", case
    )
    assert nature == "Securities Fraud"
    # the subject IS a named party -> the litigation link is DIRECT_OBSERVATION, not co_occ
    klass = await actions.pool.fetchval(
        "SELECT evidence_class FROM links WHERE from_id=$1 AND to_id=$2 AND type='litigation'",
        subject, case,
    )
    assert klass == "direct_observation"
