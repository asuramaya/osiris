"""THE GRAPH VISUALIZER + NAVIGABLE SPACE, THE SERVER piece A, the DECLUMP FIX,
THE READING LAYER, and THE LEGIBILITY PASS: the layout heartbeat places every object under a
rank-based sunflower rule (never a hash-into-a-fixed-circle), nudged by a bounded
intra-project SEMANTIC-only relax that ends with a hard minimum-separation pass;
project centers come from a weighted force layout over the contracted project graph,
never a rank; within a project, placement is by semantic adjacency (connected inner
disc vs zero-semantic-edge outer halo), never a type ring."""
from __future__ import annotations

import math
import resource
import time
import uuid
from datetime import UTC, datetime

import numpy as np
from src.actions.core import Actions
from src.orchestrator.graph_layout import (
    _LAYOUT_VERSION_PROP,
    _MIN_SEPARATION,
    LAST_DECLUMP_WORK,
    _declump,
    _hub_ids,
    _intra_project_neighbors,
    _neighbors_of,
    _place_projects,
    _project_and_type,
    _relax_projects,
    _release_layout_lock,
    _try_acquire_layout_lock,
    layout_batch,
    positions_for,
    project_center,
    relax,
    run_layout_migrate,
    unplaced_batch,
)

# --- pure placement rule: deterministic, idempotent, never overlapping ---------------


def test_relax_positions_every_unplaced_node() -> None:
    ids = [uuid.uuid4() for _ in range(12)]
    neighbors = {ids[i]: {ids[(i + 1) % 12], ids[(i - 1) % 12]} for i in range(12)}
    out = relax(ids, neighbors, {}, iterations=20)
    assert set(out.keys()) == set(ids)
    for x, y in out.values():
        assert math.isfinite(x) and math.isfinite(y)


def test_relax_is_deterministic_for_the_same_seed() -> None:
    ids = [uuid.uuid4() for _ in range(8)]
    neighbors = {ids[i]: {ids[(i + 1) % 8]} for i in range(8)}
    a = relax(ids, neighbors, {}, iterations=15, seed=7)
    b = relax(ids, neighbors, {}, iterations=15, seed=7)
    assert a == b


def test_relax_respects_fixed_anchors_never_moving_them() -> None:
    center, anchor = uuid.uuid4(), uuid.uuid4()
    out = relax([center], {center: {anchor}}, {anchor: (500.0, 500.0)}, iterations=40)
    x, y = out[center]
    # pulled toward the anchor, not left near the random seed origin
    assert x > 50 and y > 50


def test_relax_pulls_unconnected_nodes_apart_not_together() -> None:
    a, b = uuid.uuid4(), uuid.uuid4()
    out = relax([a, b], {a: set(), b: set()}, {}, iterations=30)
    dist = math.dist(out[a], out[b])
    assert dist > 10  # repulsion, not a coincidental overlap


def test_relax_with_init_starts_from_the_given_base_not_a_random_seed() -> None:
    lone = uuid.uuid4()
    out = relax([lone], {lone: set()}, {}, iterations=0, init={lone: (123.0, 456.0)})
    assert out[lone] == (123.0, 456.0)


def test_project_center_is_a_pure_function_of_rank_alone() -> None:
    assert project_center(3) == project_center(3)
    assert project_center(3) != project_center(4)


def test_project_center_ranks_never_collide_across_a_realistic_range() -> None:
    """The declump fix's whole point at the project level: no two ranks in a
    realistic range (dozens of real projects) land on the same point, or anywhere
    near it relative to the spacing constant."""
    points = [project_center(r) for r in range(60)]
    for i in range(len(points)):
        for j in range(i + 1, len(points)):
            assert math.dist(points[i], points[j]) > 100


def test_intra_project_neighbors_drops_cross_project_edges() -> None:
    a, b, c = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    proj_x, proj_y = uuid.uuid4(), uuid.uuid4()
    neighbors = {a: {b, c}}
    proj_type: dict[uuid.UUID, tuple[uuid.UUID | None, str]] = {
        a: (proj_x, "Thread"), b: (proj_x, "Thread"), c: (proj_y, "Thread")}
    out = _intra_project_neighbors([a], neighbors, proj_type)
    assert out[a] == {b}


# --- _declump: the hard minimum-separation pass -------------------


def test_declump_separates_two_coincident_points() -> None:
    import numpy as np

    ids = [uuid.uuid4(), uuid.uuid4()]
    pos = np.array([[10.0, 10.0], [10.0, 10.0]])
    out = _declump(pos, np.zeros((0, 2)), ids)
    assert math.dist(out[0], out[1]) >= _MIN_SEPARATION - 1e-6


def test_declump_leaves_already_separated_points_alone() -> None:
    import numpy as np

    ids = [uuid.uuid4(), uuid.uuid4()]
    pos = np.array([[0.0, 0.0], [1000.0, 1000.0]])
    out = _declump(pos, np.zeros((0, 2)), ids)
    assert out[0].tolist() == [0.0, 0.0]
    assert out[1].tolist() == [1000.0, 1000.0]


def test_declump_pushes_a_node_away_from_a_fixed_anchor() -> None:
    import numpy as np

    ids = [uuid.uuid4()]
    pos = np.array([[5.0, 5.0]])
    anchors = np.array([[5.0, 5.0]])
    out = _declump(pos, anchors, ids)
    assert math.dist(out[0], anchors[0]) >= _MIN_SEPARATION - 1e-6


def test_declump_60000_random_points_completes_fast_with_bounded_memory() -> None:
    """THE PHYSICS LAYOUT OOM: the OLD form built a full (n,n,2)
    pairwise array -- 40 GB at n=50,087, kernel-confirmed OOM kill. The KD-tree
    rewrite (THE DECLUMP REWRITE) must handle a
    real-scale population (60,000, comfortably over the 50,087 that actually killed
    the process) in bounded MEMORY and a bounded, DETERMINISTIC amount of WORK --
    this is the acceptance test named in that same dispatch.

    A wall-clock assertion on a shared box is not a correctness signal (this
    house's own standing lesson: a box under four concurrent xdist suites is not
    the same machine twice) -- `elapsed` is printed for a human reading the run,
    never asserted on. The real regression guard is `LAST_DECLUMP_WORK`: the
    total pairs resolved across every iteration must stay within a generous,
    population-scaled ceiling -- a real bug (a quadratic blowup) trips it
    regardless of how fast or slow the box happens to be that run.

    NOTE ON CONVERGENCE (measured against the retired grid-hash version before
    replacing it): this exact seeded population does NOT fully settle within the
    30-iteration budget under EITHER implementation -- ~23,100 pairs still
    violate `min_sep` at the end either way (23,117 old, 23,075 new; the tiny
    difference is float-order noise, not a behavior change). Summed simultaneous
    per-iteration displacement (any point with several violating neighbours at
    once gets the SUM of several independent half-deficit pushes) can create new
    marginal violations as fast as it resolves old ones on a population this
    dense relative to `min_sep` -- iterating to a hard zero was never this
    function's own guarantee, only a bounded deterministic correction. So this
    test does NOT assert `iterations_run` against a ceiling -- what changed is
    the WORK each iteration costs (one C-level KD-tree pass vs. a Python-level
    grid walk), not how many iterations the population needs."""
    rng = np.random.default_rng(42)
    n = 60_000
    # spread over a 6000x6000 area -- dense enough that real declump work happens
    # (not the trivial "nothing overlaps" case), nowhere near the degenerate
    # all-coincident case _memory_guard exists to catch separately
    pos = rng.uniform(-3000.0, 3000.0, size=(n, 2))
    ids = [uuid.uuid4() for _ in range(n)]

    before_rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    start = time.monotonic()
    out = _declump(pos, np.zeros((0, 2)), ids, min_sep=_MIN_SEPARATION)
    elapsed = time.monotonic() - start
    after_rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

    print(f"declump over 60,000 points: {elapsed:.2f}s, "
          f"{LAST_DECLUMP_WORK['pairs_resolved']} pairs resolved over "
          f"{LAST_DECLUMP_WORK['iterations_run']} iterations")

    # a generous population-scaled ceiling -- catches a real oscillation/blowup,
    # not a tight bound on this exact seed's own pair count
    assert LAST_DECLUMP_WORK["pairs_resolved"] < n * 20

    growth_mb = (after_rss_kb - before_rss_kb) / 1024
    assert growth_mb < 500, f"peak RSS grew {growth_mb:.1f} MB, over the 500 MB budget"
    assert out.shape == (n, 2)


def test_declump_one_dense_cluster_of_6000_coincident_points_stays_fast() -> None:
    """THE DENSE-CELL FIX: a live specimen on THE PHYSICS
    LAYOUT's real migration -- one grid cell held 6,131 post-FR points (many
    container-only siblings pulled to the same weak-gravity target with no semantic
    edge differentiating them), 634 million candidate pair checks on the FIRST
    declump iteration alone in the old per-pair Python loop, climbing past 6 GB
    before it was stopped by hand. This reproduces the SHAPE of that population
    directly (one cluster, not a spread scatter -- the 60,000-point test above
    covers the sparse case, this covers the dense one) and must both finish fast
    and actually separate every point to the floor."""
    n = 6000
    ids = [uuid.uuid4() for _ in range(n)]
    rng = np.random.default_rng(7)
    # ALL points within a tiny radius of one spot -- one dense grid cell, not many
    pos = rng.uniform(-0.5, 0.5, size=(n, 2))

    start = time.monotonic()
    out = _declump(pos, np.zeros((0, 2)), ids, min_sep=_MIN_SEPARATION)
    elapsed = time.monotonic() - start

    assert elapsed < 30.0, f"declump over one 6,000-point dense cluster took {elapsed:.1f}s"
    # spot-check a real sample of pairs, not all 18M -- every checked pair meets the floor
    sample = rng.integers(0, n, size=(500, 2))
    for a, b in sample:
        if a == b:
            continue
        assert math.dist(out[a], out[b]) >= _MIN_SEPARATION - 1e-6


def test_relax_never_leaves_two_strongly_attracted_nodes_stacked() -> None:
    """The exact regression once observed: strong mutual attraction (many
    shared edges, tight ideal length) used to be able to collapse two nodes onto
    (almost) the same point; the declump pass now guarantees it can't."""
    a, b = uuid.uuid4(), uuid.uuid4()
    out = relax([a, b], {a: {b}, b: {a}}, {}, iterations=50,
                init={a: (0.0, 0.0), b: (0.01, 0.0)})
    assert math.dist(out[a], out[b]) >= _MIN_SEPARATION - 1e-6


# --- DB-backed: unplaced_batch / layout_batch / positions_for -------------------------


async def test_unplaced_batch_finds_objects_with_no_layout_marker_yet(actions: Actions) -> None:
    oid = await actions.create_or_find_object("Thread", "thread:gl-unpos", "test")
    batch = await unplaced_batch(actions)
    assert oid in batch


async def test_layout_batch_stamps_graph_x_graph_y_and_the_version_marker(
    actions: Actions,
) -> None:
    a = await actions.create_or_find_object("Thread", "thread:gl-a", "test")
    b = await actions.create_or_find_object("Thread", "thread:gl-b", "test")
    await actions.create_link(a, b, "cites", "test", datetime.now(UTC), 1.0)

    placed = await layout_batch(actions, limit=1000)
    assert placed >= 2

    positions = await positions_for(actions, [a, b])
    assert a in positions and b in positions
    assert all(math.isfinite(v) for v in (*positions[a], *positions[b]))

    marker = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name=$2",
        a, _LAYOUT_VERSION_PROP)
    assert marker is not None


async def test_layout_batch_never_repositions_an_already_positioned_object(
    actions: Actions,
) -> None:
    a = await actions.create_or_find_object("Thread", "thread:gl-stable", "test")
    await layout_batch(actions, limit=1000)
    first = (await positions_for(actions, [a]))[a]

    b = await actions.create_or_find_object("Thread", "thread:gl-stable-friend", "test")
    await actions.create_link(a, b, "cites", "test", datetime.now(UTC), 1.0)
    await layout_batch(actions, limit=1000)
    second = (await positions_for(actions, [a]))[a]
    assert first == second


async def test_layout_batch_returns_zero_when_the_graph_is_fully_positioned(
    actions: Actions,
) -> None:
    await actions.create_or_find_object("Thread", "thread:gl-only", "test")
    n1 = await layout_batch(actions, limit=1000)
    assert n1 >= 1
    n2 = await layout_batch(actions, limit=1000)
    assert n2 == 0


async def test_layout_batch_with_no_explicit_limit_reads_the_settings_table(
    actions: Actions,
) -> None:
    """`layout.batch_size` genuinely reads live -- not the env-
    overlay path, which only covers effect='immediate' keys."""
    from src.orchestrator.settings_service import write_setting

    await actions.create_or_find_object("Thread", "thread:gl-settings-a", "test")
    await actions.create_or_find_object("Thread", "thread:gl-settings-b", "test")
    await write_setting(actions.pool, "layout.batch_size", 1, actor="analyst:operator")

    n = await layout_batch(actions)  # no explicit limit -- must read the table
    assert n == 1  # capped to the stored batch_size regardless of total population


# --- THE MIGRATION DOOR --------------------------------------------


async def test_layout_lock_round_trips(actions: Actions) -> None:
    async with actions.pool.acquire() as conn:
        assert await _try_acquire_layout_lock(conn) is True
        # a SECOND session (a fresh connection) can't also acquire it
        async with actions.pool.acquire() as conn2:
            assert await _try_acquire_layout_lock(conn2) is False
        await _release_layout_lock(conn)
        # released -- a fresh session can now acquire it
        async with actions.pool.acquire() as conn3:
            assert await _try_acquire_layout_lock(conn3) is True
            await _release_layout_lock(conn3)


async def test_run_layout_migrate_places_everything_and_yields_a_receipt_per_batch(
    actions: Actions,
) -> None:
    """Never assumes the DB fixture is empty (migrations/fixtures may already seed
    real objects) -- proves the ACTUAL acceptance shape: drains to quiescence, the
    receipts' own running total matches, and every object THIS test created ends up
    positioned."""
    ids = []
    for i in range(3):
        oid = await actions.create_or_find_object("Thread", f"thread:gl-migrate-{i}", "test")
        ids.append(oid)

    receipts = [r async for r in run_layout_migrate(actions, limit=1000)]
    assert receipts[-1]["placed"] == 0  # drained to quiescence
    assert receipts[-1]["total_placed"] == sum(r["placed"] for r in receipts[:-1])
    assert await unplaced_batch(actions) == []
    positions = await positions_for(actions, ids)
    assert set(positions.keys()) == set(ids)


async def test_run_layout_migrate_refuses_while_the_lock_is_held(
    actions: Actions,
) -> None:
    await actions.create_or_find_object("Thread", "thread:gl-migrate-locked", "test")
    async with actions.pool.acquire() as holder:
        assert await _try_acquire_layout_lock(holder) is True
        try:
            receipts = [r async for r in run_layout_migrate(actions)]
            assert receipts == [{
                "error": "the layout heartbeat (or another migrate run) currently "
                        "holds the layout lock -- try again shortly"}]
        finally:
            await _release_layout_lock(holder)


async def test_layout_batch_keeps_two_projects_separated(actions: Actions) -> None:
    now = datetime.now(UTC)
    proj_a = await actions.create_or_find_object("SoftwareProject", "repo:gl-proj-a", "test")
    proj_b = await actions.create_or_find_object("SoftwareProject", "repo:gl-proj-b", "test")
    a = await actions.create_or_find_object("Thread", "thread:gl-in-a", "test")
    b = await actions.create_or_find_object("Thread", "thread:gl-in-b", "test")
    await actions.create_link(a, proj_a, "in_repo", "test", now, 1.0)
    await actions.create_link(b, proj_b, "in_repo", "test", now, 1.0)

    await layout_batch(actions, limit=1000)
    pos = await positions_for(actions, [a, b])
    assert math.dist(pos[a], pos[b]) > 10


async def test_layout_batch_separates_many_objects_of_the_same_type_in_one_project(
    actions: Actions,
) -> None:
    """THE REGRESSION ITSELF, proven at DB scale: a (project, type) group well past
    what a single fixed-radius ring could hold apart now places every member with
    real minimum separation, never a stack."""
    now = datetime.now(UTC)
    proj = await actions.create_or_find_object(
        "SoftwareProject", "repo:gl-crowded-project", "test")
    ids = []
    for i in range(40):
        oid = await actions.create_or_find_object("Thread", f"thread:gl-crowd-{i}", "test")
        await actions.create_link(oid, proj, "in_repo", "test", now, 1.0)
        ids.append(oid)

    placed = 0
    while True:
        n = await layout_batch(actions, limit=1000)
        placed += n
        if n == 0:
            break

    pos = await positions_for(actions, ids)
    points = [pos[oid] for oid in ids]
    for i in range(len(points)):
        for j in range(i + 1, len(points)):
            assert math.dist(points[i], points[j]) >= _MIN_SEPARATION - 1e-6


async def test_layout_batch_migrates_an_object_placed_under_a_prior_version(
    actions: Actions,
) -> None:
    """An object carrying graph_x/graph_y from an older layout (missing today's version
    marker) is swept up by the very next tick and re-placed under the current scheme --
    the mechanism a version bump relies on for its one-time migration pass."""
    from src.orchestrator import graph_layout as gl

    oid = await actions.create_or_find_object("Thread", "thread:gl-old-scheme", "test")
    now = datetime.now(UTC)
    await actions.assert_property(oid, "graph_x", -999.0, gl.GRAPH_LAYOUT_SOURCE, now, 0.9)
    await actions.assert_property(oid, "graph_y", -999.0, gl.GRAPH_LAYOUT_SOURCE, now, 0.9)

    assert oid in await unplaced_batch(actions)
    n = await layout_batch(actions, limit=1000)
    assert n >= 1
    pos = (await positions_for(actions, [oid]))[oid]
    assert pos != (-999.0, -999.0)
    assert oid not in await unplaced_batch(actions)


# --- THE READING LAYER ---------------------------


async def test_neighbors_of_semantic_only_drops_structural_edges(actions: Actions) -> None:
    now = datetime.now(UTC)
    proj = await actions.create_or_find_object("SoftwareProject", "repo:gl-rl-proj", "test")
    a = await actions.create_or_find_object("Thread", "thread:gl-rl-a", "test")
    b = await actions.create_or_find_object("Thread", "thread:gl-rl-b", "test")
    await actions.create_link(a, proj, "in_repo", "test", now, 1.0)  # structural
    await actions.create_link(a, b, "cites", "test", now, 1.0)  # semantic

    all_neighbors = await _neighbors_of(actions, [a])
    semantic_neighbors = await _neighbors_of(actions, [a], semantic_only=True)
    assert proj in all_neighbors[a] and b in all_neighbors[a]
    assert proj not in semantic_neighbors[a]
    assert b in semantic_neighbors[a]


async def test_layout_batch_container_only_members_seed_near_their_container(
    actions: Actions,
) -> None:
    """THE PHYSICS LAYOUT (item 6): a member with ONLY a container
    edge (in_repo) and no semantic edge of its own now seeds at its container's own
    centroid -- reversing THE READING LAYER's old "structural edges never attract"
    rule for this one subset (container is a real, if weak, gravity source in the
    new ruling). `relax`'s own iterative attraction still never pulls on a
    structural/container edge (test_neighbors_of_semantic_only_drops_structural_edges
    covers that lower-level invariant directly) -- this only checks the seed."""
    now = datetime.now(UTC)
    proj = await actions.create_or_find_object("SoftwareProject", "repo:gl-rl-hub", "test")
    members = []
    for i in range(6):
        oid = await actions.create_or_find_object("Thread", f"thread:gl-rl-member-{i}", "test")
        await actions.create_link(oid, proj, "in_repo", "test", now, 1.0)
        members.append(oid)

    while await layout_batch(actions, limit=1000) > 0:
        pass

    proj_pos = (await positions_for(actions, [proj]))[proj]
    member_pos = await positions_for(actions, members)
    # a real minimum-separation floor among siblings (_MIN_SEPARATION) means N
    # container-only siblings can't ALL sit within a few units of one point past a
    # handful of them, and a few bounded relax/declump iterations nudge them further
    # still -- 500 is not a tight bound, it's a clear order-of-magnitude line below
    # the OLD flat ~6000-unit halo offset this same scenario used to produce
    for oid in members:
        assert math.dist(proj_pos, member_pos[oid]) < 500


async def test_place_projects_pulls_a_linked_project_closer_than_an_unlinked_one(
    actions: Actions,
) -> None:
    """THE READING LAYER's own acceptance line: no two linked projects' centers
    farther apart than an unlinked pair with similar radii."""
    now = datetime.now(UTC)
    hub = await actions.create_or_find_object("SoftwareProject", "repo:gl-rl-hub-p", "test")
    linked = await actions.create_or_find_object("SoftwareProject", "repo:gl-rl-linked-p",
                                                  "test")
    unlinked = await actions.create_or_find_object(
        "SoftwareProject", "repo:gl-rl-unlinked-p", "test")

    hub_member = await actions.create_or_find_object("Thread", "thread:gl-rl-hub-m", "test")
    linked_member = await actions.create_or_find_object(
        "Thread", "thread:gl-rl-linked-m", "test")
    unlinked_member = await actions.create_or_find_object(
        "Thread", "thread:gl-rl-unlinked-m", "test")
    await actions.create_link(hub_member, hub, "in_repo", "test", now, 1.0)
    await actions.create_link(linked_member, linked, "in_repo", "test", now, 1.0)
    await actions.create_link(unlinked_member, unlinked, "in_repo", "test", now, 1.0)
    # a real cross-project reference: hub <-> linked, nothing to unlinked
    await actions.create_link(hub_member, linked_member, "cites", "test", now, 1.0)

    while await layout_batch(actions, limit=1000) > 0:
        pass

    pos = await positions_for(actions, [hub, linked, unlinked])
    dist_linked = math.dist(pos[hub], pos[linked])
    dist_unlinked = math.dist(pos[hub], pos[unlinked])
    assert dist_linked < dist_unlinked


async def test_place_projects_positions_every_unplaced_project(actions: Actions) -> None:
    a = await actions.create_or_find_object("SoftwareProject", "repo:gl-rl-direct-a", "test")
    b = await actions.create_or_find_object("SoftwareProject", "repo:gl-rl-direct-b", "test")
    out = await _place_projects(actions, [a, b])
    assert set(out.keys()) == {a, b}
    for x, y in out.values():
        assert math.isfinite(x) and math.isfinite(y)


async def test_place_projects_never_overlaps_two_projects_min_spacing(
    actions: Actions,
) -> None:
    from src.orchestrator.graph_layout import _PROJECT_GUTTER, _PROJECT_SPACING_K

    a_id = uuid.uuid4()
    b_id = uuid.uuid4()
    counts = {a_id: 100, b_id: 100}
    out = _relax_projects([a_id, b_id], {}, counts, {})
    dist = math.dist(out[a_id], out[b_id])
    min_dist = _PROJECT_SPACING_K * math.sqrt(counts[a_id] + counts[b_id] + 2) + _PROJECT_GUTTER
    assert dist >= min_dist - 1e-6


async def test_hub_ids_finds_a_structural_high_degree_object(actions: Actions) -> None:
    from src.orchestrator.graph_layout import _HUB_DEGREE_THRESHOLD

    hub = await actions.create_or_find_object("Person", "principal:gl-rl-hub-person", "test")
    now = datetime.now(UTC)
    for i in range(_HUB_DEGREE_THRESHOLD + 1):
        agent = await actions.create_or_find_object("Agent", f"agent:gl-rl-hub-{i}", "test")
        await actions.create_link(agent, hub, "acts_for", "test", now, 1.0)

    found = await _hub_ids(actions, [hub])
    assert hub in found


async def test_hub_ids_excludes_a_high_degree_membership_container(
    actions: Actions,
) -> None:
    """THE LONG EDGES RULING tip (d): a
    SoftwareProject with high structural degree (every member's own in_repo
    edge counts toward it) must NEVER be classified as a hub -- pulling its
    own identity vertex into the hub zone would break every one of its
    members' in_repo edges at once. `acts_for`'s own Person target (the test
    above) stays a hub unchanged -- only membership containers are excluded."""
    from src.orchestrator.graph_layout import _HUB_DEGREE_THRESHOLD

    proj = await actions.create_or_find_object(
        "SoftwareProject", "repo:gl-rl-container-hub", "test")
    now = datetime.now(UTC)
    for i in range(_HUB_DEGREE_THRESHOLD + 1):
        member = await actions.create_or_find_object(
            "Thread", f"thread:gl-rl-container-hub-{i}", "test")
        await actions.create_link(member, proj, "in_repo", "test", now, 1.0)

    found = await _hub_ids(actions, [proj])
    assert proj not in found


async def test_hub_ids_excludes_an_ordinary_low_degree_object(actions: Actions) -> None:
    oid = await actions.create_or_find_object("Thread", "thread:gl-rl-not-a-hub", "test")
    found = await _hub_ids(actions, [oid])
    assert oid not in found


async def test_layout_batch_still_places_a_structural_hub(
    actions: Actions,
) -> None:
    """THE PHYSICS LAYOUT (item 6) dropped explicit hub-rank-0
    pinning from layout_batch's own INCREMENTAL new-object path -- that guarantee
    now lives only in graph_physics._physics_positions' own hub-recentering step
    for the one-shot migration (test_physics_positions_recenter_hubs_to_the_center_
    of_mass in test_graph_physics.py). This test only confirms the incremental path
    still gives a high-structural-degree object a real, finite position -- no
    crash, no NaN -- rather than asserting a rank-0 guarantee this path no longer
    makes."""
    from src.orchestrator.graph_layout import _HUB_DEGREE_THRESHOLD

    now = datetime.now(UTC)
    hub = await actions.create_or_find_object("Person", "principal:gl-rl-pin-hub", "test")
    for i in range(_HUB_DEGREE_THRESHOLD + 1):
        agent = await actions.create_or_find_object("Agent", f"agent:gl-rl-pin-{i}", "test")
        await actions.create_link(agent, hub, "acts_for", "test", now, 1.0)

    while await layout_batch(actions, limit=1000) > 0:
        pass

    x, y = (await positions_for(actions, [hub]))[hub]
    assert math.isfinite(x) and math.isfinite(y)


async def test_project_and_type_falls_back_to_the_project_assertion(
    actions: Actions,
) -> None:
    """THE MEMBERSHIP UNION FIX: a member with
    NO in_repo link but a `project` assertion naming the repo must still resolve
    to that project's own object id -- the incremental heartbeat's own placement
    door, not just the physics migration's."""
    proj = await actions.create_or_find_object(
        "SoftwareProject", "repo:gl-union", "test")
    member = await actions.create_or_find_object("Thread", "thread:gl-union-member", "test")
    now = datetime.now(UTC)
    await actions.assert_property(member, "project", "gl-union", "test", now, 1.0)

    out = await _project_and_type(actions, [member])
    project_id, obj_type = out[member]
    assert project_id == proj
    assert obj_type == "Thread"


async def test_layout_batch_clusters_semantically_connected_objects_closer_than_halo(
    actions: Actions,
) -> None:
    """THE LEGIBILITY PASS's own acceptance shape: within one project, an object with
    a real semantic edge sits closer to the project center than a same-project object
    with no semantic edge at all."""
    now = datetime.now(UTC)
    proj = await actions.create_or_find_object("SoftwareProject", "repo:gl-lp-cluster", "test")
    connected = await actions.create_or_find_object("Thread", "thread:gl-lp-cl-a", "test")
    connected_friend = await actions.create_or_find_object(
        "Thread", "thread:gl-lp-cl-b", "test")
    halo = await actions.create_or_find_object("Thread", "thread:gl-lp-cl-halo", "test")
    for oid in (connected, connected_friend, halo):
        await actions.create_link(oid, proj, "in_repo", "test", now, 1.0)
    await actions.create_link(connected, connected_friend, "cites", "test", now, 1.0)

    while await layout_batch(actions, limit=1000) > 0:
        pass

    proj_pos = (await positions_for(actions, [proj]))[proj]
    pos = await positions_for(actions, [connected, halo])
    assert math.dist(proj_pos, pos[connected]) < math.dist(proj_pos, pos[halo])
