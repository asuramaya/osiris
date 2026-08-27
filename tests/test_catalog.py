"""THE CATALOG STORE (task #97 workstream 1) — Type as a first-class graph object.
Every test proves ONE contract: idempotent stub-on-miss, keep-prior-on-omission, the
bootstrap axiom, warn/strict enforcement, and the fingerprint cache actually seeing a
fresh write. Nothing here touches conftest.py's shared fixtures — each test seeds
exactly what it needs.
"""
from __future__ import annotations

import pytest
from src.actions.core import Actions
from src.ontology import catalog
from src.ontology.catalog import (
    TypeRecord,
    UnknownTypeError,
    categories,
    check_link_type,
    check_object_type,
    ensure_type,
    full_catalog,
    get_type,
    is_known_link_type,
    is_known_object_type,
    link_type,
    object_type,
    seed_catalog,
    set_strict,
)


@pytest.fixture(autouse=True)
def _clear_cache() -> None:
    catalog._cache.clear()
    catalog._usage_cache.clear()


async def test_ensure_type_mints_a_bare_stub(actions: Actions) -> None:
    rec = await ensure_type(actions, name="Widget", kind="object", actor="test")
    assert rec.name == "Widget" and rec.kind == "object"
    assert rec.category == () and rec.description == ""
    again = await get_type(actions.pool, "Widget", "object")
    assert again == rec


async def test_ensure_type_is_idempotent(actions: Actions) -> None:
    a = await ensure_type(actions, name="Gadget", kind="object", actor="test",
                          description="a thing")
    b = await ensure_type(actions, name="Gadget", kind="object", actor="test",
                          description="a thing")
    assert a == b
    st = await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Type' AND canonical='type:object:Gadget'")
    assert st == 1


async def test_ensure_type_keeps_prior_value_on_omission(actions: Actions) -> None:
    """The section/room_id discipline (89e67c49), generalized: a bare call must never
    blank an already-rich Type's fields."""
    await ensure_type(actions, name="Rich", kind="object", actor="human",
                      description="carefully written", category=["Software"],
                      label_field="handle")
    # Seshat's accretion call site — bare, on a type someone already described
    stub_call = await ensure_type(actions, name="Rich", kind="object", actor="accretion")
    assert stub_call.description == "carefully written"
    assert stub_call.category == ("Software",)
    assert stub_call.label_field == "handle"


async def test_ensure_type_link_kind_round_trips_domain_range(actions: Actions) -> None:
    rec = await ensure_type(actions, name="orbits", kind="link", actor="test",
                            description="A orbits B", domain=["Planet"], range=["Star"])
    assert rec.domain == ("Planet",) and rec.range == ("Star",)
    fetched = await link_type(actions.pool, "orbits")
    assert fetched.description == "A orbits B"


async def test_ensure_type_object_kind_round_trips_required_link_kinds(
    actions: Actions,
) -> None:
    """task #189, decision 7ea187b9 — the declare-or-refuse gate's own declaration
    surface, same shape as domain/range's round-trip above."""
    rec = await ensure_type(actions, name="GatedThing", kind="object", actor="test",
                            required_link_kinds=["repo", "grounds"])
    assert rec.required_link_kinds == ("repo", "grounds")
    fetched = await object_type(actions.pool, "GatedThing")
    assert fetched.required_link_kinds == ("repo", "grounds")


async def test_object_type_falls_back_to_default_when_unknown(actions: Actions) -> None:
    rec = await object_type(actions.pool, "NeverDeclared")
    assert rec.name == "Unknown" and rec.category == ("Other",)


async def test_link_type_falls_back_to_default_when_unknown(actions: Actions) -> None:
    rec = await link_type(actions.pool, "never_declared")
    assert rec.name == "unknown"


async def test_is_known_reports_true_only_after_ensure_type(actions: Actions) -> None:
    assert not await is_known_object_type(actions.pool, "Sprocket")
    await ensure_type(actions, name="Sprocket", kind="object", actor="test")
    assert await is_known_object_type(actions.pool, "Sprocket")
    assert not await is_known_link_type(actions.pool, "Sprocket")


async def test_bootstrap_axiom_type_is_always_valid_even_unseeded(actions: Actions) -> None:
    """check_object_type("Type") must never consult the (possibly empty) catalog —
    the one axiom the self-describing meta-schema needs."""
    set_strict(True)
    try:
        await check_object_type(actions.pool, "Type")  # must not raise, catalog is empty
    finally:
        set_strict(False)


async def test_check_object_type_warns_by_default_never_raises(
    actions: Actions, caplog: pytest.LogCaptureFixture,
) -> None:
    set_strict(False)
    with caplog.at_level("WARNING"):
        await check_object_type(actions.pool, "Ghost")
    assert any("Ghost" in r.message for r in caplog.records)


async def test_check_object_type_raises_in_strict_mode_for_undeclared(
    actions: Actions,
) -> None:
    set_strict(True)
    try:
        with pytest.raises(UnknownTypeError):
            await check_object_type(actions.pool, "Ghost")
    finally:
        set_strict(False)


async def test_check_object_type_passes_silently_once_declared(actions: Actions) -> None:
    await ensure_type(actions, name="Declared", kind="object", actor="test")
    set_strict(True)
    try:
        await check_object_type(actions.pool, "Declared")  # must not raise
    finally:
        set_strict(False)


async def test_check_object_type_accretes_when_actions_and_actor_given(
    actions: Actions,
) -> None:
    """Task #97 workstream 2: the accretion hook — check_object_type mints a bare stub
    instead of warning once an Actions instance and an actor are available."""
    set_strict(False)
    try:
        await check_object_type(actions.pool, "AutoWidget", actions=actions, actor="accretor")
    finally:
        set_strict(True)
    assert await is_known_object_type(actions.pool, "AutoWidget")
    rec = await object_type(actions.pool, "AutoWidget")
    assert rec.name == "AutoWidget" and rec.description == ""


async def test_check_object_type_strict_mode_wins_over_accretion(actions: Actions) -> None:
    """Strict mode must never be silently bypassed by supplying actions/actor — it is
    CI's hard-failure net, and accretion swallowing that defeats its whole purpose."""
    with pytest.raises(UnknownTypeError):
        await check_object_type(actions.pool, "ShouldRaiseToo", actions=actions,
                                actor="accretor")
    assert not await is_known_object_type(actions.pool, "ShouldRaiseToo")


async def test_check_object_type_still_warns_without_actions_or_actor(
    actions: Actions, caplog: pytest.LogCaptureFixture,
) -> None:
    """The original warn-only contract is unchanged for a caller with no Actions/actor
    to accrete through."""
    set_strict(False)
    try:
        with caplog.at_level("WARNING"):
            await check_object_type(actions.pool, "StillJustAWarning")
    finally:
        set_strict(True)
    assert any("StillJustAWarning" in r.message for r in caplog.records)
    assert not await is_known_object_type(actions.pool, "StillJustAWarning")


async def test_check_link_type_accretes_when_actions_and_actor_given(actions: Actions) -> None:
    set_strict(False)
    try:
        await check_link_type(actions.pool, "auto_relates_to", actions=actions,
                              actor="accretor")
    finally:
        set_strict(True)
    assert await is_known_link_type(actions.pool, "auto_relates_to")


async def test_check_link_type_same_contract(actions: Actions) -> None:
    set_strict(True)
    try:
        with pytest.raises(UnknownTypeError):
            await check_link_type(actions.pool, "no_such_link")
    finally:
        set_strict(False)
    await ensure_type(actions, name="rel", kind="link", actor="test")
    set_strict(True)
    try:
        await check_link_type(actions.pool, "rel")  # must not raise
    finally:
        set_strict(False)


async def test_categories_dedupes_across_types(actions: Actions) -> None:
    await ensure_type(actions, name="A", kind="object", actor="test", category=["X", "Y"])
    await ensure_type(actions, name="B", kind="object", actor="test", category=["Y", "Z"])
    cats = await categories(actions.pool)
    # a subset check, not equality: the catalog is deliberately session-persistent
    # (task #97 — Type rows survive the per-test reset), so the seeded 96 entries'
    # own categories are also present by the time this test runs
    assert {"X", "Y", "Z"} <= set(cats)


async def test_full_catalog_shape(actions: Actions) -> None:
    await ensure_type(actions, name="Foo", kind="object", actor="test",
                      color="#fff", shape="ellipse", description="d", category=["C"])
    await ensure_type(actions, name="rel", kind="link", actor="test", description="d2")
    c = await full_catalog(actions.pool)
    assert {"object_types", "link_types", "categories"} <= c.keys()
    foo = next(t for t in c["object_types"] if t["name"] == "Foo")
    assert foo["color"] == "#fff" and foo["category"] == ["C"]
    assert any(lt["name"] == "rel" for lt in c["link_types"])


async def test_full_catalog_ranks_object_types_by_live_instance_count(
    actions: Actions,
) -> None:
    """Task #121 (ruling a4bd555c): RELEVANCE OBSERVED, NOT DECLARED — the catalog
    ranks by live usage in THIS graph, never a declared category/domain tag."""
    await ensure_type(actions, name="PopularKind", kind="object", actor="test")
    await ensure_type(actions, name="RareKind", kind="object", actor="test")
    for i in range(5):
        await actions.create_or_find_object("PopularKind", f"popular:{i}", "test")
    await actions.create_or_find_object("RareKind", "rare:0", "test")

    c = await full_catalog(actions.pool)
    popular = next(t for t in c["object_types"] if t["name"] == "PopularKind")
    rare = next(t for t in c["object_types"] if t["name"] == "RareKind")
    assert popular["count"] == 5
    assert rare["count"] == 1
    names = [t["name"] for t in c["object_types"]]
    assert names.index("PopularKind") < names.index("RareKind")


async def test_full_catalog_keeps_a_zero_instance_type_present_but_ranked_last(
    actions: Actions,
) -> None:
    """COMPLETE AT THE RECORD (ruling a4bd555c, the operator's refusal of Thoth's
    retire-the-unused-types instinct): a type with zero live instances in THIS graph
    is never trimmed — it still ships, just last in the observed-relevance order."""
    await ensure_type(actions, name="UsedKind", kind="object", actor="test")
    await ensure_type(actions, name="UnusedKind", kind="object", actor="test")
    await actions.create_or_find_object("UsedKind", "used:0", "test")

    c = await full_catalog(actions.pool)
    names = {t["name"] for t in c["object_types"]}
    assert "UnusedKind" in names
    unused = next(t for t in c["object_types"] if t["name"] == "UnusedKind")
    assert unused["count"] == 0
    order = [t["name"] for t in c["object_types"]]
    assert order.index("UsedKind") < order.index("UnusedKind")


async def test_full_catalog_object_count_excludes_a_merged_away_object(
    actions: Actions,
) -> None:
    """A merge doesn't delete the loser (status='merged', never gone) but it must not
    keep inflating its type's apparent relevance — the same status='active' discipline
    used everywhere else in this codebase."""
    await ensure_type(actions, name="MergeCountKind", kind="object", actor="test")
    winner = await actions.create_or_find_object("MergeCountKind", "mc:winner", "test")
    loser = await actions.create_or_find_object("MergeCountKind", "mc:loser", "test")
    await actions.merge_objects(winner, loser, "test merge", "test")

    c = await full_catalog(actions.pool)
    rec = next(t for t in c["object_types"] if t["name"] == "MergeCountKind")
    assert rec["count"] == 1  # the merged-away loser doesn't count


async def test_full_catalog_link_count_excludes_an_invalidated_link(
    actions: Actions,
) -> None:
    """Mirrors dossier.py's own valid_until discipline: a healed (invalidated, never
    deleted) link must not keep counting toward its type's relevance."""
    from datetime import UTC, datetime

    await ensure_type(actions, name="LinkCountKind", kind="object", actor="test")
    await ensure_type(actions, name="live_count_rel", kind="link", actor="test")
    a = await actions.create_or_find_object("LinkCountKind", "lc:a", "test")
    b = await actions.create_or_find_object("LinkCountKind", "lc:b", "test")
    now = datetime.now(UTC)
    await actions.create_link(a, b, "live_count_rel", "test", now, 0.9)
    await actions.invalidate_link(a, b, "live_count_rel", "test", now)

    c = await full_catalog(actions.pool)
    rel = next(t for t in c["link_types"] if t["name"] == "live_count_rel")
    assert rel["count"] == 0  # the one link that ever existed is now invalidated


async def test_usage_count_cache_does_not_see_a_fresh_write_within_the_ttl(
    actions: Actions,
) -> None:
    """Deliberately the OPPOSITE contract from the Type catalog's own fingerprint gate
    (test_cache_sees_a_fresh_write_immediately_same_process below) — a usage count
    backs a RANKING, not a validation check, so trading a few seconds of staleness for
    one fewer query per full_catalog call is the intended tradeoff, not a bug."""
    await ensure_type(actions, name="TtlKind", kind="object", actor="test")
    first = await full_catalog(actions.pool)
    before = next(t for t in first["object_types"] if t["name"] == "TtlKind")["count"]
    await actions.create_or_find_object("TtlKind", "ttl:0", "test")
    second = await full_catalog(actions.pool)
    after = next(t for t in second["object_types"] if t["name"] == "TtlKind")["count"]
    assert after == before  # still cached — the write hasn't crossed the TTL yet


async def test_usage_count_cache_refreshes_once_cleared(actions: Actions) -> None:
    await ensure_type(actions, name="RefreshKind", kind="object", actor="test")
    await full_catalog(actions.pool)  # prime the cache at count=0
    await actions.create_or_find_object("RefreshKind", "refresh:0", "test")
    catalog._usage_cache.clear()
    c = await full_catalog(actions.pool)
    rec = next(t for t in c["object_types"] if t["name"] == "RefreshKind")
    assert rec["count"] == 1


async def test_cache_sees_a_fresh_write_immediately_same_process(actions: Actions) -> None:
    """The fingerprint check, not a stale TTL window, is what a same-process caller
    must see — mint, then read, with no cache priming in between."""
    assert await object_type(actions.pool, "JustMinted") == catalog._DEFAULT_OBJECT
    await ensure_type(actions, name="JustMinted", kind="object", actor="test",
                      description="fresh")
    rec = await object_type(actions.pool, "JustMinted")
    assert rec.description == "fresh"


async def test_seed_catalog_migrates_every_declared_entry(actions: Actions) -> None:
    from src.ontology import schema
    counts = await seed_catalog(actions)
    assert counts["object_types"] == len(schema._OBJECT_TYPES)
    assert counts["link_types"] == len(schema._LINK_TYPES)
    org = await object_type(actions.pool, "Organization")
    assert org.color == "#4493f8" and "cik:" in org.schemes and org.category == ("Entity",)
    link = await link_type(actions.pool, "controlled_by")
    assert link.domain == ("CryptoAddress",)


async def test_seed_catalog_is_idempotent(actions: Actions) -> None:
    first = await seed_catalog(actions)
    n_before = await actions.pool.fetchval("SELECT count(*) FROM objects WHERE type='Type'")
    second = await seed_catalog(actions)
    n_after = await actions.pool.fetchval("SELECT count(*) FROM objects WHERE type='Type'")
    assert first == second
    # not a total-row assertion (the catalog is deliberately session-persistent, so
    # other tests' ad-hoc types may already share the table) — idempotency means a
    # repeat seed adds NOTHING, whatever the starting count was
    assert n_before == n_after


async def test_type_record_is_frozen_and_comparable() -> None:
    a = TypeRecord(name="X", kind="object", category=("A",))
    b = TypeRecord(name="X", kind="object", category=("A",))
    assert a == b
    with pytest.raises(AttributeError):
        a.name = "Y"  # type: ignore[misc]
