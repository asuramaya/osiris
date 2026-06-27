"""Footprint discrepancy: operational reach the disclosed home doesn't cover."""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator.discrepancy import country_of, footprint_discrepancy

NOW = datetime(2026, 6, 26, tzinfo=UTC)


def test_country_of_handles_formats() -> None:
    assert country_of("Dallas, Texas, United States") == "United States"
    assert country_of("Abu Dhabi, United Arab Emirates") == "United Arab Emirates"
    assert country_of("Toronto, Ontario, Canada") == "Canada"
    assert country_of("Austin, TX") == "United States"   # US postal code
    assert country_of("Jakarta, K8") == "Indonesia"      # EDGAR foreign code
    assert country_of("CA") == "United States"
    # an UNMAPPED EDGAR code is not a country — must not leak as a false reach claim
    assert country_of("L3") is None
    assert country_of("Somewhere, X0") is None


async def test_discrepancy_flags_foreign_operational_reach(actions: Actions) -> None:
    # a US-disclosed company...
    co = await actions.create_or_find_object("Organization", "cik:0000001", "edgar")
    await actions.assert_property(co, "name", "Acme Neuro", "edgar", NOW, 0.85)
    await actions.assert_property(co, "incorporation_state", "CA", "edgar", NOW, 0.85)

    # ...running a trial at a site in the UAE (2 hops: co -> trial -> site)
    src = "clinicaltrials"
    trial = await actions.create_or_find_object("ClinicalTrial", "nct:1", src)
    await actions.create_link(co, trial, "sponsors", src, NOW, 0.85)
    site = await actions.create_or_find_object("Organization", "ctgov-org:ccad", src)
    await actions.assert_property(site, "name", "Cleveland Clinic Abu Dhabi", src, NOW, 0.85)
    await actions.assert_property(
        site, "location", "Abu Dhabi, United Arab Emirates", src, NOW, 0.85)
    await actions.create_link(trial, site, "site", src, NOW, 0.85)

    # ...and a domestic site, which must NOT be flagged
    us_site = await actions.create_or_find_object("Organization", "ctgov-org:miami", src)
    await actions.assert_property(us_site, "name", "University of Miami", src, NOW, 0.85)
    await actions.assert_property(
        us_site, "location", "Miami, Florida, United States", src, NOW, 0.85)
    await actions.create_link(trial, us_site, "site", src, NOW, 0.85)

    d = await footprint_discrepancy(actions.pool, co)
    assert d["home"] == ["United States"]
    assert set(d["operational_countries"]) == {"United States", "United Arab Emirates"}
    flagged = {x["country"] for x in d["discrepancies"]}
    assert flagged == {"United Arab Emirates"}  # the foreign reach only
    uae = next(x for x in d["discrepancies"] if x["country"] == "United Arab Emirates")
    assert uae["reach"][0]["name"] == "Cleveland Clinic Abu Dhabi"
