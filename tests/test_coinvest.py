"""Co-investment ties + company consolidation."""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.ontology.resolution import consolidate_companies
from src.orchestrator.coinvest import coinvestment_ties

NOW = datetime(2026, 6, 26, tzinfo=UTC)


async def _spv(actions: Actions, spv_canon: str, operator_canon: str, target_id: str) -> None:
    """An SPV run by an operator, raising for a target company."""
    spv = await actions.create_or_find_object("Organization", spv_canon, "edgar")
    op = await actions.create_or_find_object("Person", operator_canon, "edgar")
    await actions.assert_property(op, "name", operator_canon.split(":")[-1], "edgar", NOW, 0.85)
    await actions.create_link(spv, op, "officer", "edgar", NOW, 0.85)
    await actions.create_link(spv, target_id, "raises_for", "edgar", NOW, 0.35)


async def test_coinvestment_ranks_shared_operators(actions: Actions) -> None:
    neuralink = await actions.create_or_find_object("Organization", "company:neuralink", "edgar")
    await actions.assert_property(neuralink, "name", "Neuralink", "edgar", NOW, 0.85)
    openai = await actions.create_or_find_object("Organization", "company:openai", "edgar")
    await actions.assert_property(openai, "name", "OpenAI", "edgar", NOW, 0.85)
    anthropic = await actions.create_or_find_object("Organization", "company:anthropic", "edgar")
    await actions.assert_property(anthropic, "name", "Anthropic", "edgar", NOW, 0.85)

    # operator "sydecar" funds Neuralink + OpenAI + Anthropic; "mav" funds Neuralink + OpenAI
    await _spv(actions, "org:s1", "sec-person:sydecar", neuralink)
    await _spv(actions, "org:s2", "sec-person:sydecar", openai)
    await _spv(actions, "org:s3", "sec-person:sydecar", anthropic)
    await _spv(actions, "org:m1", "sec-person:mav", neuralink)
    await _spv(actions, "org:m2", "sec-person:mav", openai)

    ties = await coinvestment_ties(actions.pool, neuralink)
    # OpenAI shares 2 operators with Neuralink; Anthropic shares 1 -> OpenAI ranks first
    assert ties[0]["company"] == "OpenAI"
    assert ties[0]["shared_operators"] == 2
    assert set(ties[0]["operators"]) == {"sydecar", "mav"}
    assert ties[1]["company"] == "Anthropic"
    assert ties[1]["shared_operators"] == 1


async def test_consolidate_companies_merges_prefix_variants(actions: Actions) -> None:
    base = await actions.create_or_find_object("Organization", "company:crusoe", "edgar")
    await actions.assert_property(base, "name", "Crusoe", "edgar", NOW, 0.35)
    variant = await actions.create_or_find_object(
        "Organization", "company:crusoe green meadow", "edgar")
    await actions.assert_property(variant, "name", "Crusoe Green Meadow", "edgar", NOW, 0.35)
    unrelated = await actions.create_or_find_object(
        "Organization", "company:standard transformers", "edgar")
    await actions.assert_property(unrelated, "name", "Standard Transformers", "edgar", NOW, 0.35)

    merged = await consolidate_companies(actions)
    assert merged == 1  # the variant folds into the base; the unrelated one is untouched

    assert await actions.resolve_object_id(variant) == base       # variant -> base
    assert await actions.resolve_object_id(unrelated) == unrelated  # untouched
