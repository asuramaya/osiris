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

    # thesis sponsor "Alpha" funds Neuralink + OpenAI + Anthropic; "Beta" funds Neuralink + OpenAI
    await _spv(actions, "org:s1", "sec-person:Alpha Ventures", neuralink)
    await _spv(actions, "org:s2", "sec-person:Alpha Ventures", openai)
    await _spv(actions, "org:s3", "sec-person:Alpha Ventures", anthropic)
    await _spv(actions, "org:m1", "sec-person:Beta Capital", neuralink)
    await _spv(actions, "org:m2", "sec-person:Beta Capital", openai)

    ties = await coinvestment_ties(actions.pool, neuralink)
    # OpenAI shares 2 operators with Neuralink; Anthropic shares 1 -> OpenAI ranks first
    assert ties[0]["company"] == "OpenAI"
    assert ties[0]["shared_operators"] == 2
    assert set(ties[0]["operators"]) == {"Alpha Ventures", "Beta Capital"}
    assert ties[1]["company"] == "Anthropic"
    assert ties[1]["shared_operators"] == 1


async def test_coinvestment_filters_admin_platform_by_name(actions: Actions) -> None:
    target = await actions.create_or_find_object("Organization", "company:target", "edgar")
    await actions.assert_property(target, "name", "Target Co", "edgar", NOW, 0.85)
    dentist = await actions.create_or_find_object("Organization", "company:dentist", "edgar")
    await actions.assert_property(dentist, "name", "Dentologie", "edgar", NOW, 0.85)

    # a fund-admin platform (Sydecar) signs SPVs for BOTH — must NOT count as a tie
    await _spv(actions, "org:p1", "sec-person:LLC Sydecar", target)
    await _spv(actions, "org:p2", "sec-person:LLC Sydecar", dentist)

    ties = await coinvestment_ties(actions.pool, target)
    assert ties == []  # the only shared "operator" is a platform -> no real tie


async def test_coinvestment_filters_serial_admin_by_degree(actions: Actions) -> None:
    target = await actions.create_or_find_object("Organization", "company:t2", "edgar")
    await actions.assert_property(target, "name", "Target Two", "edgar", NOW, 0.85)
    peer = await actions.create_or_find_object("Organization", "company:peer", "edgar")
    await actions.assert_property(peer, "name", "Peer Co", "edgar", NOW, 0.85)

    # a serial-admin signatory (not name-matched) wired into 14 distinct companies
    await _spv(actions, "org:sa-t", "sec-person:Jane Signatory", target)
    await _spv(actions, "org:sa-p", "sec-person:Jane Signatory", peer)
    for i in range(13):
        co = await actions.create_or_find_object("Organization", f"company:misc{i}", "edgar")
        await actions.assert_property(co, "name", f"Misc {i}", "edgar", NOW, 0.85)
        await _spv(actions, f"org:sa{i}", "sec-person:Jane Signatory", co)
    # a genuine thesis sponsor wired into just target + peer
    await _spv(actions, "org:th-t", "sec-person:Real Sponsor", target)
    await _spv(actions, "org:th-p", "sec-person:Real Sponsor", peer)

    ties = await coinvestment_ties(actions.pool, target)
    by_co = {t["company"]: t for t in ties}
    # Peer shows via the thesis sponsor ONLY (degree-14 signatory excluded), so the 13
    # misc companies the signatory also touched do NOT appear as ties.
    assert by_co["Peer Co"]["shared_operators"] == 1
    assert by_co["Peer Co"]["operators"] == ["Real Sponsor"]
    assert not any(c.startswith("Misc ") for c in by_co)


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
