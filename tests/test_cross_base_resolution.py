"""Resolving the same org across federated bases (Wikidata <-> EDGAR) by name."""
from __future__ import annotations

from datetime import UTC, datetime

import pytest
from src.actions.core import Actions
from src.ontology.resolution import (
    find_cross_base_candidates,
    normalize_org_name,
    reclassify_mistyped_entities,
    resolve_cross_base,
    review_tray,
)

NOW = datetime(2026, 6, 26, tzinfo=UTC)


def test_normalize_strips_legal_form() -> None:
    assert normalize_org_name("Tesla, Inc.") == "tesla"
    assert normalize_org_name("Tesla") == "tesla"
    assert normalize_org_name("ELECTRA PRO LLC") == "electra pro"
    assert normalize_org_name("Joint Stock Company GAUSS") != ""  # not emptied to nothing


async def _org(actions: Actions, canonical: str, name: str, source: str) -> str:
    oid = await actions.create_or_find_object("Organization", canonical, source)
    await actions.assert_property(oid, "name", name, source, NOW, 0.85)
    return oid


async def test_cross_base_org_match_queues_candidate(actions: Actions) -> None:
    # the same company from two bases, names differing only by legal form
    wd = await _org(actions, "Q478214", "Tesla", "wikidata")
    ed = await _org(actions, "cik:0001318605", "Tesla, Inc.", "edgar")

    queued = await find_cross_base_candidates(actions.pool)
    assert queued == 1

    pair = next(t for t in await review_tray(actions.pool) if wd in (t["a_id"], t["b_id"]))
    assert {pair["a_id"], pair["b_id"]} == {wd, ed}
    assert pair["score"] == pytest.approx(0.6)
    assert any("cross-base" in s for s in pair["reasons"]["signals"])

    # idempotent
    assert await find_cross_base_candidates(actions.pool) == 0


async def test_same_base_namesakes_not_matched(actions: Actions) -> None:
    # two orgs from the SAME base sharing a name are a within-base dup (the ingest's
    # job), not a cross-base resolution — they must not be queued against each other.
    await _org(actions, "cik:1", "Tesla, Inc.", "edgar")
    await _org(actions, "cik:2", "Tesla Corp.", "edgar")
    assert await find_cross_base_candidates(actions.pool) == 0


async def test_resolve_cross_base_merges_distinctive_cluster(actions: Actions) -> None:
    from src.ontology.resolution import find_cross_base_candidates, resolve_cross_base

    # the same company fragmented across three bases (distinct sources -> candidates)
    comp = await _org(actions, "company:neuralink", "Neuralink", "edgar")
    cik = await _org(actions, "cik:0001708503", "Neuralink Corp.", "edgar")
    # give cik a second source so company:/cik: differ in provenance
    await actions.assert_property(cik, "topics", "private", "edgar", NOW, 0.85)
    wiki = await _org(actions, "Q29043471", "Neuralink", "wikidata")

    await find_cross_base_candidates(actions.pool)
    merged = await resolve_cross_base(actions)
    assert merged >= 2  # company: and Q fold into the CIK-keyed winner

    # everything now resolves to the CIK (most authoritative canonical)
    assert await actions.resolve_object_id(comp) == cik
    assert await actions.resolve_object_id(wiki) == cik


async def test_reclassify_heals_mistyped_person_and_enables_cross_base(
    actions: Actions,
) -> None:
    # a GP entity mis-ingested as a Person (the Form D defect), an officer-style link to
    # it, and the SAME entity correctly typed as an Org from another base (BC registry)
    fake_person = await actions.create_or_find_object(
        "Person", "sec-person:n/a brilliant phoenix gp inc.", "edgar")
    await actions.assert_property(
        fake_person, "name", "n/a Brilliant Phoenix GP Inc.", "edgar", NOW, 0.85)
    spv = await _org(actions, "cik:99", "BP Neuralink LP", "edgar")
    await actions.create_link(spv, fake_person, "director", "edgar", NOW, 0.85)
    bc = await _org(actions, "bc-reg:A0127997", "BRILLIANT PHOENIX GP INC.", "orgbook")

    # repair: the fake person re-types into an Organization
    assert await reclassify_mistyped_entities(actions) == 1
    org = await actions.create_or_find_object(
        "Organization", "sec-org:brilliant phoenix gp inc.", "reclassify")
    assert await actions.resolve_object_id(fake_person) == org  # merged into the Org
    # the director link now resolves to the Org, not a fake person
    assert await actions.pool.fetchval(
        "SELECT type FROM objects WHERE id=$1", org) == "Organization"

    # and now the EDGAR-side Org cross-base-resolves with the BC registry entity
    await find_cross_base_candidates(actions.pool)
    merged = await resolve_cross_base(actions)
    assert merged >= 1
    assert await actions.resolve_object_id(org) == await actions.resolve_object_id(bc)


async def test_resolve_cross_base_skips_short_names(actions: Actions) -> None:
    from src.ontology.resolution import find_cross_base_candidates, resolve_cross_base

    a = await _org(actions, "company:abc", "ABC", "edgar")  # 3 chars -> too short/common
    await _org(actions, "Q1", "ABC", "wikidata")
    await find_cross_base_candidates(actions.pool)
    assert await resolve_cross_base(actions) == 0
    assert await actions.resolve_object_id(a) == a  # untouched
