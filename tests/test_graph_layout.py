"""THE GRAPH VISUALIZER, WAVE B item 1 (operator dispatch, wave 15, thread 8839): the
incremental server-side layout heartbeat feeds -- positions stored as graph_x/graph_y
assertions, one bounded local relaxation per tick."""
from __future__ import annotations

import math
import uuid

from src.actions.core import Actions
from src.orchestrator.graph_layout import (
    layout_batch,
    positions_for,
    relax,
    unpositioned_batch,
)


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


async def test_unpositioned_batch_finds_objects_with_no_graph_x_yet(actions: Actions) -> None:
    oid = await actions.create_or_find_object("Thread", "thread:gl-unpos", "test")
    batch = await unpositioned_batch(actions)
    assert oid in batch


async def test_layout_batch_stamps_graph_x_and_graph_y(actions: Actions) -> None:
    a = await actions.create_or_find_object("Thread", "thread:gl-a", "test")
    b = await actions.create_or_find_object("Thread", "thread:gl-b", "test")
    from datetime import UTC, datetime
    await actions.create_link(a, b, "cites", "test", datetime.now(UTC), 1.0)

    placed = await layout_batch(actions, limit=1000)
    assert placed >= 2

    positions = await positions_for(actions, [a, b])
    assert a in positions and b in positions
    assert all(math.isfinite(v) for v in (*positions[a], *positions[b]))


async def test_layout_batch_never_repositions_an_already_positioned_object(
    actions: Actions,
) -> None:
    a = await actions.create_or_find_object("Thread", "thread:gl-stable", "test")
    await layout_batch(actions, limit=1000)
    first = (await positions_for(actions, [a]))[a]

    b = await actions.create_or_find_object("Thread", "thread:gl-stable-friend", "test")
    from datetime import UTC, datetime
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


async def test_layout_batch_anchors_a_new_neighbor_near_its_already_placed_neighbor(
    actions: Actions,
) -> None:
    from datetime import UTC, datetime

    hub = await actions.create_or_find_object("Thread", "thread:gl-hub", "test")
    await layout_batch(actions, limit=1000)
    hub_pos = (await positions_for(actions, [hub]))[hub]

    leaf = await actions.create_or_find_object("Thread", "thread:gl-leaf", "test")
    await actions.create_link(hub, leaf, "cites", "test", datetime.now(UTC), 1.0)
    await layout_batch(actions, limit=1000)
    leaf_pos = (await positions_for(actions, [leaf]))[leaf]

    # relaxed against a real anchor, not a stray seed on the far side of the plane --
    # the ideal edge length is 60, so a settled neighbor should land within a modest
    # multiple of that, not hundreds of units away.
    assert math.dist(hub_pos, leaf_pos) < 400
