"""Form D ingest: the buried private-placement layer (issuer + officers + amounts)."""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.ingest.edgar_formd import ingest_form_d, parse_form_d

NOW = datetime(2026, 6, 26, tzinfo=UTC)

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


async def test_link_funnel_wires_spvs_to_core(actions: Actions) -> None:
    from src.ingest.edgar_formd import link_funnel

    # the operating company (largest raise) + two feeder SPVs + a cross-base duplicate
    core = await actions.create_or_find_object("Organization", "cik:0001708503", "edgar")
    await actions.assert_property(core, "name", "Neuralink Corp.", "edgar", NOW, 0.85)
    await actions.assert_property(core, "amount_raised", "280274981", "edgar", NOW, 0.85)
    for canon, nm in (("cik:1", "MAV Neuralink, LP"), ("cik:2", "TWE Neuralink SPV, LLC")):
        spv = await actions.create_or_find_object("Organization", canon, "edgar")
        await actions.assert_property(spv, "name", nm, "edgar", NOW, 0.85)
    dup = await actions.create_or_find_object("Organization", "Q29043471", "wikidata")
    await actions.assert_property(dup, "name", "Neuralink", "wikidata", NOW, 0.85)

    out = await link_funnel(actions, "Neuralink")
    assert out["core"] == "Neuralink Corp."
    assert out["spv_links"] == 2  # the two SPVs; the same-named Wikidata dup is skipped

    # the funnel points SPV -> core, DERIVED (a name inference, not a crawl seed)
    links = await actions.pool.fetch(
        "SELECT from_id, evidence_class FROM links WHERE to_id=$1 AND type='raises_for'", core)
    assert len(links) == 2
    assert all(r["evidence_class"] == "derived" for r in links)
    assert dup not in {r["from_id"] for r in links}

    # idempotent
    assert (await link_funnel(actions, "Neuralink"))["spv_links"] == 0
