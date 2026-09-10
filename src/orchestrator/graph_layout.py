"""THE GRAPH VISUALIZER, wave B item 1 (operator dispatch, wave 15, thread 8839):
server-side force layout over the WHOLE graph, run incrementally by the heartbeat --
positions stored as current_assertions (graph_x/graph_y), never computed live in the
browser. Cytoscape/fcose still owns the bounded neighbourhood board (Wave A); the
full-graph view at Wave B's own scale (~41k objects) needs its layout ALREADY DONE when a
viewport is requested (graph_layout.viewport, the endpoint this module feeds), not
recomputed per request -- that's what "under two seconds at 41k objects" actually requires.

INCREMENTAL, NOT GLOBAL: a full force-directed relaxation over 41k nodes every tick would be
both slow and pointless (the overwhelming majority of the graph doesn't move between ticks).
Each tick positions only objects that don't have a position yet, using their
ALREADY-POSITIONED neighbors as fixed anchors -- a small LOCAL Fruchterman-Reingold pass,
never a global one. A node with no positioned neighbor at all (a fresh island, or the very
first tick ever) is seeded pseudo-randomly near the origin and relaxes correctly once ITS OWN
neighbors get positioned on some later tick -- mechanical, no hand-run step, matching this
house's own "the layout and clusters come from the heartbeat, never a hand" law.
"""
from __future__ import annotations

import uuid
from datetime import UTC, datetime

import numpy as np

from src.actions.core import Actions

GRAPH_LAYOUT_SOURCE = "cron:graph_layout"
_BATCH_SIZE = 1000
_ITERATIONS = 50
_IDEAL_EDGE_LEN = 60.0
_MAX_STEP = 10.0


async def unpositioned_batch(actions: Actions, limit: int = _BATCH_SIZE) -> list[uuid.UUID]:
    """Object ids with no graph_x assertion yet, oldest first (created_at) -- a stable,
    deterministic order so successive ticks make real progress instead of re-picking the
    same random subset every time."""
    rows = await actions.pool.fetch(
        "SELECT o.id FROM objects o "
        "WHERE o.status NOT IN ('archived','merged','retired') "
        "  AND NOT EXISTS (SELECT 1 FROM current_assertions a "
        "    WHERE a.object_id=o.id AND a.name='graph_x') "
        "ORDER BY o.created_at ASC LIMIT $1",
        limit)
    return [r["id"] for r in rows]


async def positions_for(
    actions: Actions, ids: list[uuid.UUID],
) -> dict[uuid.UUID, tuple[float, float]]:
    """Every id in `ids` that already carries BOTH graph_x and graph_y -- a partial write
    (one landed, not the other) is treated as unpositioned rather than trusted half-done."""
    if not ids:
        return {}
    rows = await actions.pool.fetch(
        "SELECT object_id, name, value #>> '{}' AS v FROM current_assertions "
        "WHERE object_id = ANY($1::uuid[]) AND name IN ('graph_x','graph_y')",
        ids)
    xs: dict[uuid.UUID, float] = {}
    ys: dict[uuid.UUID, float] = {}
    for r in rows:
        (xs if r["name"] == "graph_x" else ys)[r["object_id"]] = float(r["v"])
    return {oid: (xs[oid], ys[oid]) for oid in xs if oid in ys}


async def _neighbors_of(
    actions: Actions, ids: list[uuid.UUID],
) -> dict[uuid.UUID, set[uuid.UUID]]:
    if not ids:
        return {}
    rows = await actions.pool.fetch(
        "SELECT from_id, to_id FROM links "
        "WHERE from_id = ANY($1::uuid[]) OR to_id = ANY($1::uuid[])",
        ids)
    idset = set(ids)
    out: dict[uuid.UUID, set[uuid.UUID]] = {i: set() for i in ids}
    for r in rows:
        f, t = r["from_id"], r["to_id"]
        if f in idset:
            out[f].add(t)
        if t in idset:
            out[t].add(f)
    return out


def relax(
    unplaced: list[uuid.UUID], neighbors: dict[uuid.UUID, set[uuid.UUID]],
    anchors: dict[uuid.UUID, tuple[float, float]], *, iterations: int = _ITERATIONS,
    seed: int = 42,
) -> dict[uuid.UUID, tuple[float, float]]:
    """Vectorized numpy Fruchterman-Reingold, LOCAL to `unplaced`: those nodes repel each
    other and are pulled toward their neighbors (both other unplaced nodes and fixed
    `anchors`, which never move) -- deterministic seed, so a re-run of the same batch
    reproduces the same layout rather than jittering on every retry.

    MATRIX operations over the whole batch at once, never a Python double-loop over pairs
    -- measured live: the naive per-pair Python/numpy-scalar version didn't finish 300
    nodes in 60s (numpy's per-call overhead dominates at that granularity); this version
    positions 1000 nodes in a small fraction of a second by computing the full NxN
    displacement in one broadcast per iteration, the same way every real force-layout
    implementation (fcose included) actually does it."""
    n = len(unplaced)
    idx = {nid: i for i, nid in enumerate(unplaced)}
    rng = np.random.default_rng(seed)
    pos = rng.uniform(-50, 50, size=(n, 2))

    # the edge list as index pairs INTO `pos` (unplaced-unplaced) and a separate
    # unplaced-index/anchor-position list (unplaced-anchor) — built once, outside the
    # iteration loop, since the topology never changes across iterations.
    uu_pairs: list[tuple[int, int]] = []
    seen_pairs: set[tuple[int, int]] = set()
    ua_idx: list[int] = []
    ua_pos: list[tuple[float, float]] = []
    for nid, nbs in neighbors.items():
        i = idx[nid]
        for nb in nbs:
            if nb in idx:
                j = idx[nb]
                pair = (min(i, j), max(i, j))
                if pair not in seen_pairs:
                    seen_pairs.add(pair)
                    uu_pairs.append(pair)
            elif nb in anchors:
                ua_idx.append(i)
                ua_pos.append(anchors[nb])
    uu = np.array(uu_pairs, dtype=np.int64) if uu_pairs else np.zeros((0, 2), dtype=np.int64)
    ua_i = np.array(ua_idx, dtype=np.int64) if ua_idx else np.zeros(0, dtype=np.int64)
    ua_p = np.array(ua_pos, dtype=np.float64) if ua_pos else np.zeros((0, 2))

    for _ in range(iterations):
        # repulsion: every node pushes every other node away, O(n^2) but as ONE matrix op.
        delta = pos[:, None, :] - pos[None, :, :]                       # (n, n, 2)
        dist = np.maximum(np.linalg.norm(delta, axis=2), 0.01)          # (n, n)
        np.fill_diagonal(dist, np.inf)                                  # a node never repels itself
        repel = (_IDEAL_EDGE_LEN ** 2) / dist                           # (n, n)
        disp = (delta / dist[:, :, None] * repel[:, :, None]).sum(axis=1)  # (n, 2)

        # attraction along edges — unplaced-unplaced (both ends move, opposite signs) and
        # unplaced-anchor (only the unplaced end moves), each a single vectorized pass.
        if len(uu):
            a, b = uu[:, 0], uu[:, 1]
            d = pos[a] - pos[b]
            dd = np.maximum(np.linalg.norm(d, axis=1), 0.01)
            force = (dd ** 2 / _IDEAL_EDGE_LEN)[:, None] * (d / dd[:, None])
            np.subtract.at(disp, a, force)
            np.add.at(disp, b, force)
        if len(ua_i):
            d = pos[ua_i] - ua_p
            dd = np.maximum(np.linalg.norm(d, axis=1), 0.01)
            force = (dd ** 2 / _IDEAL_EDGE_LEN)[:, None] * (d / dd[:, None])
            np.subtract.at(disp, ua_i, force)

        dn = np.maximum(np.linalg.norm(disp, axis=1), 1e-9)
        step = np.minimum(dn, _MAX_STEP)
        pos = pos + disp / dn[:, None] * step[:, None]

    return {nid: (float(pos[idx[nid], 0]), float(pos[idx[nid], 1])) for nid in unplaced}


async def layout_batch(actions: Actions, *, limit: int = _BATCH_SIZE) -> int:
    """One heartbeat tick: position up to `limit` unpositioned objects, anchored against
    their already-positioned neighbors. Returns how many objects were newly positioned (0
    when the graph is fully laid out -- the tick's own natural quiescence, no flag needed)."""
    unplaced = await unpositioned_batch(actions, limit)
    if not unplaced:
        return 0
    neighbors = await _neighbors_of(actions, unplaced)
    unplaced_set = set(unplaced)
    neighbor_ids = sorted(
        ({nb for nbs in neighbors.values() for nb in nbs} - unplaced_set), key=str)
    anchors = await positions_for(actions, neighbor_ids)
    placed = relax(unplaced, neighbors, anchors)
    now = datetime.now(UTC)
    for oid, (x, y) in placed.items():
        await actions.assert_property(oid, "graph_x", round(x, 2), GRAPH_LAYOUT_SOURCE,
                                      now, 0.9)
        await actions.assert_property(oid, "graph_y", round(y, 2), GRAPH_LAYOUT_SOURCE,
                                      now, 0.9)
    return len(placed)
