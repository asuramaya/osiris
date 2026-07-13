"""THE GARDEN (operator, 2026-07-11): "we dont hardcode, we build primitives. in this case, a
fanout and organizing per project helps across the entire stack. think about neighborhoods and
bundling. think about the garden of eden, and each project is a tree with fruits".

The graph is the garden, a SoftwareProject is a TREE, and anything hanging off it by `in_repo`
is FRUIT. `bundle` fans ANY object set out into its trees; `neighborhood` is a real dimension of
`select`, so a tree's row drills straight back into exactly its fruit. Both are type-blind: the
same two primitives bundle threads, commits, files or decisions.
"""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator.compositions import run_spec
from src.orchestrator.neighborhoods import neighborhoods_of

NOW = datetime(2026, 7, 11, tzinfo=UTC)


async def _garden(actions: Actions) -> None:
    """Two trees with fruit, and one rootless object hanging in the void."""
    for repo, threads, commits in (("osiris", 3, 1), ("sibling-three", 2, 0)):
        proj = await actions.create_or_find_object("SoftwareProject", f"repo:{repo}", "session")
        await actions.assert_property(proj, "name", repo, "session", NOW, 0.9)
        for i in range(threads):
            t = await actions.create_or_find_object("Thread", f"thread:{repo}-{i}", "session")
            await actions.assert_property(t, "summary", f"{repo} duty {i}", "session", NOW, 0.9)
            await actions.assert_property(t, "status", "open", "session", NOW, 0.9)
            await actions.create_link(t, proj, "in_repo", "session", NOW, 0.9)
        for i in range(commits):
            c = await actions.create_or_find_object("Commit", f"commit:{repo}-{i}", "session")
            await actions.create_link(c, proj, "in_repo", "session", NOW, 0.9)
    orphan = await actions.create_or_find_object("Thread", "thread:rootless", "session")
    await actions.assert_property(orphan, "summary", "hangs from nothing", "session", NOW, 0.9)
    await actions.assert_property(orphan, "status", "open", "session", NOW, 0.9)


async def test_fanout_collapses_any_set_into_its_trees(actions: Actions) -> None:
    """The 974-row scroll becomes a garden you can see: one row per tree, heaviest first, and
    the rootless fruit gets its own honest pile instead of being silently dropped."""
    await _garden(actions)
    out = await run_spec(actions.pool, {
        "op": "bundle", "by": "neighborhood",
        "from": {"op": "select", "object_type": "Thread",
                 "where": [{"property": "status", "op": "eq", "value": "open"}]}})
    assert out["kind"] == "rows"
    rows = out["items"]
    assert [r["group"]["neighborhood"] for r in rows] == ["osiris", "sibling-three", "(no tree)"]
    assert [r["metric"] for r in rows] == [3, 2, 1]        # ranked by weight, no ORDER needed
    assert rows[0]["id"] is not None                       # carries the tree's id, so it walks
    assert rows[-1]["id"] is None                          # the void has no coordinates


async def test_a_tree_walks_back_into_exactly_its_fruit(actions: Actions) -> None:
    """`neighborhood` is a DIMENSION of select (an in_repo EDGE, not an assertion) — which is
    what lets a bundled row drill back with the console's ORDINARY drill, no bespoke path:
    select Thread where neighborhood=osiris."""
    await _garden(actions)
    walk = await run_spec(actions.pool, {
        "op": "select", "object_type": "Thread",
        "where": [{"property": "neighborhood", "op": "eq", "value": "osiris"}]})
    assert walk["count"] == 3
    assert all("osiris duty" in o["label"] for o in walk["items"])
    # the rootless fruit is reachable too, and is never silently swept into a real tree
    void = await run_spec(actions.pool, {
        "op": "select", "object_type": "Thread",
        "where": [{"property": "neighborhood", "op": "eq", "value": "(no tree)"}]})
    assert [o["label"] for o in void["items"]] == ["hangs from nothing"]


async def test_the_garden_is_type_blind(actions: Actions) -> None:
    """Nothing in the primitive knows what a Thread is. Bundle commits — or the WHOLE graph,
    every type at once — and the same trees come back. That is what makes it a primitive and
    not a fourth hand-rolled group-by."""
    await _garden(actions)
    commits = await run_spec(actions.pool, {
        "op": "bundle", "from": {"op": "select", "object_type": "Commit"}})
    assert [(r["group"]["neighborhood"], r["metric"]) for r in commits["items"]] == [("osiris", 1)]
    everything = await run_spec(actions.pool, {"op": "bundle", "from": {"op": "select"}})
    trees = {r["group"]["neighborhood"]: r["metric"] for r in everything["items"]}
    assert trees["osiris"] == 4 and trees["sibling-three"] == 2   # threads AND commits, one call


async def test_neighborhoods_of_is_one_query_and_names_the_tree(actions: Actions) -> None:
    """The shared primitive the whole stack now leans on — the desk roster, the wall's rollup
    and the composer's fanout all ask this one question instead of each re-deriving it."""
    await _garden(actions)
    ids = [r["id"] for r in await actions.pool.fetch(
        "SELECT id FROM objects WHERE type='Thread' AND status='active'")]
    hoods = await neighborhoods_of(actions.pool, ids)
    assert sorted({h["name"] for h in hoods.values()}) == ["osiris", "sibling-three"]
    assert len(hoods) == len(ids) - 1                     # the rootless one is absent, not faked
    assert all(h["id"] for h in hoods.values())           # every tree carries coordinates
