"""Resolving the same org across federated bases (Wikidata <-> EDGAR) by name."""
from __future__ import annotations

from datetime import UTC, datetime

import pytest
from src.actions.core import Actions
from src.ontology.resolution import (
    find_cross_base_candidates,
    normalize_org_name,
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
