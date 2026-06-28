"""Compositions — the composer's primitive. The key claim: an opinionated read-model
(`discrepancy`) is just a composition of neutral ops, so opinion leaves the engine and
becomes a saved, forkable spec the user owns. Also proves the ops and persistence.
"""
from __future__ import annotations

from datetime import UTC, datetime

import pytest
from src.actions.core import Actions
from src.orchestrator.compositions import (
    DEFAULT_COMPOSITIONS,
    list_compositions,
    run_composition,
    save_composition,
    seed_default_compositions,
)
from src.orchestrator.discrepancy import footprint_discrepancy

NOW = datetime(2026, 6, 27, tzinfo=UTC)


async def _scenario(actions: Actions):
    """A US-disclosed company running a trial at a UAE site (2 hops) + a US site."""
    co = await actions.create_or_find_object("Organization", "cik:1", "edgar")
    await actions.assert_property(co, "name", "Acme Neuro", "edgar", NOW, 0.85)
    await actions.assert_property(co, "incorporation_state", "CA", "edgar", NOW, 0.85)
    src = "clinicaltrials"
    trial = await actions.create_or_find_object("ClinicalTrial", "nct:1", src)
    await actions.create_link(co, trial, "sponsors", src, NOW, 0.85)
    uae = await actions.create_or_find_object("Organization", "ctgov-org:ccad", src)
    await actions.assert_property(uae, "location", "Abu Dhabi, United Arab Emirates",
                                  src, NOW, 0.85)
    await actions.create_link(trial, uae, "site", src, NOW, 0.85)
    us = await actions.create_or_find_object("Organization", "ctgov-org:miami", src)
    await actions.assert_property(us, "location", "Miami, Florida, United States", src, NOW, 0.85)
    await actions.create_link(trial, us, "site", src, NOW, 0.85)
    return co


# --- the headline: discrepancy IS a composition -----------------------------

async def test_discrepancy_is_just_a_composition(actions: Actions) -> None:
    co = await _scenario(actions)
    await seed_default_compositions(actions.pool)

    res = await run_composition(actions.pool, "operational-vs-disclosed-geography", co)
    # the composition surfaces the foreign operational country — the SAME signal the
    # hardcoded read-model produced, now a forkable spec instead of engine code.
    assert res["kind"] == "values"
    assert res["items"] == ["United Arab Emirates"]

    # equivalence with the (now-vestigial) engine read-model
    legacy = await footprint_discrepancy(actions.pool, co)
    assert set(res["items"]) == {x["country"] for x in legacy["discrepancies"]}


# --- the neutral ops --------------------------------------------------------

async def test_select_op(actions: Actions) -> None:
    await _scenario(actions)
    spec = {"op": "select", "object_type": "ClinicalTrial"}
    res = await run_composition(
        actions.pool, await _save(actions, "trials", spec)
    )
    assert res["kind"] == "objects" and res["count"] == 1


async def test_traverse_then_collect(actions: Actions) -> None:
    co = await _scenario(actions)
    spec = {"op": "collect", "transform": "country", "properties": ["location"],
            "from": {"op": "traverse", "from": {"op": "subject"}, "hops": 2}}
    res = await run_composition(actions.pool, await _save(actions, "geo", spec), co)
    assert set(res["items"]) == {"United States", "United Arab Emirates"}


# --- P1 ops: union / intersect / aggregate / order / take -------------------

async def test_intersect_neighbourhood_with_type(actions: Actions) -> None:
    """'Organizations within 2 hops of the subject' = intersect(neighbourhood, orgs).
    The trial (a ClinicalTrial) and the subject itself fall out — set algebra, no join."""
    co = await _scenario(actions)
    spec = {"op": "intersect", "sets": [
        {"op": "traverse", "from": {"op": "subject"}, "hops": 2},
        {"op": "select", "object_type": "Organization"}]}
    res = await run_composition(actions.pool, await _save(actions, "orgs-near", spec), co)
    assert res["kind"] == "objects" and res["count"] == 2  # uae + us


async def test_union_dedups(actions: Actions) -> None:
    await _scenario(actions)
    spec = {"op": "union", "sets": [
        {"op": "select", "object_type": "Organization"},
        {"op": "select", "object_type": "ClinicalTrial"}]}
    res = await run_composition(actions.pool, await _save(actions, "everything", spec))
    assert res["count"] == 4  # 3 orgs + 1 trial, no double-count


async def _filings(actions: Actions) -> None:
    for cik, sector, amt in [("10", "ai", "100"), ("11", "ai", "300"), ("12", "bio", "50")]:
        o = await actions.create_or_find_object("Organization", f"cik:{cik}", "edgar")
        await actions.assert_property(o, "sector", sector, "edgar", NOW, 0.85)
        await actions.assert_property(o, "amount", amt, "edgar", NOW, 0.85)


async def test_aggregate_order_take(actions: Actions) -> None:
    """count per sector → order desc → take top 1: 'ai' wins with 2 (Palantir groupBy)."""
    await _filings(actions)
    spec = {"op": "take", "n": 1, "from": {
        "op": "order", "dir": "desc", "from": {
            "op": "aggregate", "group_by": ["sector"], "metric": {"type": "count"},
            "from": {"op": "select", "object_type": "Organization"}}}}
    res = await run_composition(actions.pool, await _save(actions, "top-sector", spec))
    assert res["kind"] == "rows" and res["count"] == 1
    assert res["items"][0]["group"]["sector"] == "ai"
    assert res["items"][0]["metric"] == 2


async def test_aggregate_sum_over_field(actions: Actions) -> None:
    await _filings(actions)
    spec = {"op": "aggregate", "group_by": ["sector"],
            "metric": {"type": "sum", "field": "amount"},
            "from": {"op": "select", "object_type": "Organization"}}
    res = await run_composition(actions.pool, await _save(actions, "sum-amt", spec))
    by = {r["group"]["sector"]: r["metric"] for r in res["items"]}
    assert by["ai"] == 400.0 and by["bio"] == 50.0


async def test_aggregate_dimension_cap(actions: Actions) -> None:
    """Palantir's ≤3-dimension cap is enforced — a 4-dim aggregate is rejected."""
    await _filings(actions)
    spec = {"op": "aggregate", "group_by": ["a", "b", "c", "d"], "metric": {"type": "count"},
            "from": {"op": "select", "object_type": "Organization"}}
    with pytest.raises(ValueError, match="dimension"):
        await run_composition(actions.pool, await _save(actions, "too-wide", spec))


# --- persistence / forkability ----------------------------------------------

async def test_save_run_fork_roundtrip(actions: Actions) -> None:
    spec = {"op": "select", "object_type": "Organization"}
    await save_composition(actions.pool, "all-orgs", spec)
    # fork = save the same spec under a new name; both coexist
    await save_composition(actions.pool, "my-orgs", spec)
    names = {c["name"] for c in await list_compositions(actions.pool)}
    assert {"all-orgs", "my-orgs"} <= names
    # update-by-name (not a dup)
    await save_composition(actions.pool, "all-orgs", {"op": "select", "object_type": "Person"})
    rows = [c for c in await list_compositions(actions.pool) if c["name"] == "all-orgs"]
    assert len(rows) == 1 and rows[0]["spec"]["object_type"] == "Person"


async def test_default_compositions_seeded(actions: Actions) -> None:
    n = await seed_default_compositions(actions.pool)
    assert n == len(DEFAULT_COMPOSITIONS)
    names = {c["name"] for c in await list_compositions(actions.pool)}
    assert "operational-vs-disclosed-geography" in names


async def _save(actions: Actions, name: str, spec: dict) -> str:
    await save_composition(actions.pool, name, spec)
    return name
