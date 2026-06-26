"""Form D ingest: the buried private-placement layer (issuer + officers + amounts)."""
from __future__ import annotations

from src.actions.core import Actions
from src.ingest.edgar_formd import (
    _target_company,
    ingest_form_d,
    link_feeders,
    link_spv_targets,
    parse_form_d,
)


def _formd(cik: str, name: str, sold: str) -> str:
    return f"""<?xml version="1.0"?><edgarSubmission>
      <primaryIssuer><cik>{cik}</cik><entityName>{name}</entityName>
        <issuerAddress><stateOrCountry>CA</stateOrCountry></issuerAddress></primaryIssuer>
      <offeringData><totalAmountSold>{sold}</totalAmountSold></offeringData>
    </edgarSubmission>"""

# a minimal Form D shaped like the real Neuralink Corp filing
_XML = """<?xml version="1.0"?>
<edgarSubmission>
  <primaryIssuer>
    <cik>0001708503</cik>
    <entityName>Neuralink Corp.</entityName>
    <issuerAddress><city>Fremont</city><stateOrCountry>CA</stateOrCountry></issuerAddress>
  </primaryIssuer>
  <relatedPersonsList>
    <relatedPersonInfo>
      <relatedPersonName><firstName>Elon</firstName><lastName>Musk</lastName></relatedPersonName>
      <relatedPersonAddress><city>Fremont</city><stateOrCountry>CA</stateOrCountry></relatedPersonAddress>
      <relatedPersonRelationshipList>
        <relationship>Executive Officer</relationship>
      </relatedPersonRelationshipList>
    </relatedPersonInfo>
    <relatedPersonInfo>
      <relatedPersonName><firstName>Jared</firstName><lastName>Birchall</lastName></relatedPersonName>
      <relatedPersonAddress><city>Fremont</city><stateOrCountry>CA</stateOrCountry></relatedPersonAddress>
      <relatedPersonRelationshipList>
        <relationship>Executive Officer</relationship><relationship>Director</relationship>
      </relatedPersonRelationshipList>
    </relatedPersonInfo>
  </relatedPersonsList>
  <offeringData>
    <totalOfferingAmount>280274981</totalOfferingAmount>
    <totalAmountSold>280274981</totalAmountSold>
    <minimumInvestmentAccepted>14995</minimumInvestmentAccepted>
    <totalNumberAlreadyInvested>24</totalNumberAlreadyInvested>
  </offeringData>
</edgarSubmission>
"""


def test_parse_form_d_extracts_issuer_persons_offering() -> None:
    d = parse_form_d(_XML)
    assert d["issuer"] == "Neuralink Corp."
    assert d["cik"] == "0001708503"
    assert d["state"] == "CA"
    assert d["offering"]["amount_raised"] == "280274981"
    assert d["offering"]["investors"] == "24"
    names = {p["name"] for p in d["persons"]}
    assert names == {"Elon Musk", "Jared Birchall"}
    birchall = next(p for p in d["persons"] if p["name"] == "Jared Birchall")
    assert birchall["relationships"] == ["Executive Officer", "Director"]


async def test_ingest_form_d_materializes_issuer_and_officers(actions: Actions) -> None:
    counts = await ingest_form_d(actions, parse_form_d(_XML))
    assert counts["issuers"] == 1
    assert counts["persons"] == 2

    issuer = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE type='Organization' AND canonical='cik:0001708503'"
    )
    assert issuer is not None
    raised = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name='amount_raised'",
        issuer,
    )
    assert raised == "280274981"

    # officers/directors are linked from the issuer, named, AUTHORITATIVE_API
    officers = {
        r["nm"]: r["ltype"]
        for r in await actions.pool.fetch(
            "SELECT l.type AS ltype, "
            "  (SELECT value #>> '{}' FROM current_assertions a "
            "   WHERE a.object_id=l.to_id AND a.name='name' LIMIT 1) AS nm "
            "FROM links l WHERE l.from_id=$1 AND l.type IN ('officer','director')",
            issuer,
        )
    }
    assert officers["Elon Musk"] == "officer"
    assert officers["Jared Birchall"] in ("officer", "director")


async def test_link_feeders_connects_spvs_to_core(actions: Actions) -> None:
    # the core company out-raises its feeders; SPVs reference its name
    core = await ingest_form_d(actions, parse_form_d(_formd("1", "Neuralink Corp.", "280000000")))
    spv1 = await ingest_form_d(actions, parse_form_d(_formd("2", "MAV Neuralink, LP", "1320596")))
    spv2 = await ingest_form_d(actions, parse_form_d(_formd("3", "DPV Neuralink I LLC", "3466740")))
    # an unrelated issuer in the batch must NOT be linked
    other = await ingest_form_d(actions, parse_form_d(_formd("4", "Acme Robotics Inc", "5000")))

    issuers = [
        {"id": r["issuer_id"], "name": r["name"], "amount": r["amount"]}
        for r in (core, spv1, spv2, other)
    ]
    core_id, feeders = await link_feeders(actions, issuers, "Neuralink")
    assert core_id == core["issuer_id"]
    assert feeders == 2  # the two SPVs, not Acme

    # the funnel is reachable from the core: SPVs raises_for it, speculative-graded
    rows = await actions.pool.fetch(
        "SELECT from_id, evidence_class FROM links WHERE to_id=$1 AND type='raises_for'",
        core["issuer_id"],
    )
    assert {r["from_id"] for r in rows} == {spv1["issuer_id"], spv2["issuer_id"]}
    assert all(r["evidence_class"] == "co_occurrence" for r in rows)


def test_target_company_parses_portfolio_co() -> None:
    assert _target_company("Anthropic SPV2 Emerging Global a Series of CGF2021 LLC") == "Anthropic"
    assert _target_company("Databricks MAV Alternate Fund I, LP") == "Databricks"
    assert _target_company("Cohere-MAV Alternate Fund I") == "Cohere"
    assert _target_company("Atom Computing Jan 2026 a Series of CGF2021 LLC") == "Atom Computing"
    assert _target_company("MAV Groq Alternate Fund") == "Groq"     # leading operator skipped
    assert _target_company("BP Neuralink LP") == "Neuralink"
    assert _target_company("a Series of CGF2021 LLC") is None       # all boilerplate
    assert _target_company("AC-0215 Gaingels Fund I") is None       # code prefix, no real name
    assert _target_company("CC SX III a Series of X") is None       # short codes only


async def test_link_spv_targets_builds_coinvestment(actions: Actions) -> None:
    # two SPVs from different operators funding the SAME company -> a shared target node
    a = await ingest_form_d(actions, parse_form_d(_formd("10", "Anthropic SPV a Series X", "100")))
    b = await ingest_form_d(actions, parse_form_d(_formd("11", "Anthropic MAV Alt Fund", "200")))
    issuers = [{"id": r["issuer_id"], "name": r["name"], "amount": r["amount"]} for r in (a, b)]

    assert await link_spv_targets(actions, issuers) == 2
    target = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE type='Organization' AND canonical='company:anthropic'"
    )
    assert target is not None
    funders = await actions.pool.fetch(
        "SELECT from_id FROM links WHERE to_id=$1 AND type='raises_for'", target
    )
    assert {r["from_id"] for r in funders} == {a["issuer_id"], b["issuer_id"]}
