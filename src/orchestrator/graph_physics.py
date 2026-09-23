"""THE PHYSICS LAYOUT: a wholesale replacement of graph_layout.py's sunflower/
declump placement scheme with a real force simulation over the WHOLE active graph,
run once per migration (never per-tick). Semantic edges are springs (weight by type,
degree-normalised so a hub's own many springs don't each pull at full strength).
CONTAINER edges (src.ontology.link_classes.CONTAINER_LINK_TYPES: in_repo, works_in,
acts_for, spawned_by, holds, member_of) are NOT springs, but a flat, weak,
non-degree-normalised pull toward the container, so a member of several containers
settles near their weighted mean and a container's own final position is simply
wherever that pull leaves it (no separate centroid-computation step: the same one
force simulation does both, since a node connected only to its own members is, by
construction, pulled toward their mean). Every OTHER structural type
(dispatch/governance/authorship) stays excluded from the graph entirely, unchanged
from the read-layout scheme.

NESTED COMMUNITIES: a project with more than `_COMMUNITY_MIN_MEMBERS` active members
gets its own internal semantic subgraph run through Leiden community detection
(igraph's built-in `community_leiden`, no separate leidenalg dependency needed); a
member landing in a real (>=3-member) community loses its direct member->project
container edge in favour of member->community and community->project weak edges, so
the community reads as its own sub-cluster (a super cluster reads as districts).
Communities are SYNTHETIC vertices with no `object_id`, present only to shape the one
shared force simulation, stripped before anything is ever written back.

HUBS: reuses graph_layout.py's own `_hub_ids` (structural-degree >= threshold)
unchanged as "universal hub", rather than inventing a second, narrower
cross-project-breadth metric: the existing measured threshold already selects the
unambiguous cases (principal Persons, the biggest projects) that count as "anything
over threshold". A hub's OWN organic FR position is overridden as a final,
disclosed step: as of v8 (THE HUB ZONE, `_HUB_ZONE_ID`) this is its own extra
level-1 vertex with its own radius budget, run through the SAME
`_separate_extents` pass as every real project, never the raw centroid-of-everything
an earlier "snap to the mean, jitter by 1 unit" scheme used. That scheme's own
1-unit jitter radius had no separation guarantee against whatever real content
happened to already occupy that centroid, and the first real hierarchical migration
attempt hit exactly that: eleven hubs squeezed into a 2-unit disc that collided with
a dense project sitting at the same point, caught by `_verify_min_separation`
rather than shipped silently.

SEEDED, DETERMINISTIC: every real vertex's FR starting position is
`graph_layout._sunflower_point` keyed on its own stable creation-order rank (the
`object_ids` query's own `ORDER BY created_at, id`), so a re-run over the same
population lands on the same layout, matching every earlier layout version's own
determinism guarantee. Synthetic community vertices seed at a small deterministic
jitter around the origin (structural scaffolding only, no meaning of their own).

ENDS WITH THE SAME DECLUMP FLOOR (`graph_layout._declump`) every earlier version
used: FR's own repulsion approaches but never guarantees a minimum separation
within a bounded iteration count, this is the deterministic correction that does.
A prior out-of-memory incident (kernel-confirmed: anon-rss 26.3 GB, process killed)
traced to `_declump`'s OLD form, which built a full (n,n,2) pairwise array over the
WHOLE population: 40 GB at n=50,087, since this migration passes every active object
at once (never 1000 at a time the way the heartbeat's own incremental batches do).
Fixed in graph_layout.py itself (a spatial-hash grid, `_grid_cells`/
`_neighbor_cell_indices`, cell size = min_sep, no O(n^2) memory anywhere) so both
this migration and the heartbeat share the fix. `run_physics_migrate` ALSO guards
its own remaining quadratic-shaped steps against `layout.physics_max_bytes`
(default 2 GB) before running them, and logs peak RSS on its final result: a
belt-and-suspenders check against a future reintroduction, not because anything
left here still allocates that way today.

WRITE PATH: reuses `graph_layout._bulk_assert_positions` unchanged (graph_x/graph_y/
graph_layout_v as ordinary property assertions, GRAPH_LAYOUT_SOURCE the sole writer),
chunked to keep any one multi-row statement a bounded size.

A GENUINELY DIFFERENT EXECUTION SHAPE FROM `run_layout_migrate`: that function loops
`layout_batch` (bounded per-tick batches, the SAME algorithm the cron heartbeat uses
for incremental new-object placement) to quiescence. This migration is a single
global computation over the WHOLE population in one pass: springs pull across the
entire graph, not just within a batch, so it cannot be sliced into independent
1000-object batches the way the old sunflower scheme could. `run_physics_migrate`
below is therefore its own function, not a variant of `run_layout_migrate`, but
shares the SAME advisory lock (`graph_layout._LAYOUT_LOCK_KEY`) so it and the
routine cron heartbeat (or a `run_layout_migrate` catch-up run) can never race each
other. It is held for this function's ENTIRE run, not released between steps, since
a heartbeat tick placing even one "new" object mid-computation with the OLD
incremental algorithm would need undoing, not just racing.

ONGOING INCREMENTAL PLACEMENT (item 6, "heartbeat for new objects") is NOT in this
module: it is graph_layout.layout_batch's own placement rule for `unplaced_regular`,
updated in that module to seed a new object at its container's (or already-placed
neighbours') own centroid and run a few bounded local relax iterations with
everything else pinned, instead of the old sunflower disc. See that module's own
docstring for the detail; kept there rather than here since it reuses `relax()`'s
existing local-batch machinery almost unchanged.

HIERARCHICAL PHYSICS (v8): the FLAT whole-graph FR above (v7) ran clean (50,317
placed, 202 MB, 253 s, no OOM) but FAILED the layout's own acceptance the moment
real positions were measured: nn p50 2.8 against a 15-unit floor, every project's
members spread to ~1,000 units while project centroids sat only 82-224 units apart.
The root cause was never a declump bug: `_declump`'s 30-iteration cap simply cannot
converge when a whole graph's worth of projects are allowed to overlap by
construction; the fix is to STOP them overlapping, never to push the convergence
budget higher. Two-level scheme:

LEVEL 1, the project-contracted graph: one vertex per project with >=1 active
member, edges an aggregated cross-project semantic link count
(`_cross_project_edges`), FR over that small graph (`_level1_layout`), then
`_separate_extents`: an extent-aware repulsion pass (each project vertex carries
its own radius budget `_level1_radius`, R_p = k*sqrt(N_p)*spacing) that pushes
every pair of project DISCS apart until their centroid distance is at least
R_a + R_b + a gutter, the exact same deterministic-correction shape `_declump`
itself uses, just keyed on a per-vertex radius instead of one shared floor. This is
what actually stops the "one white blob": no two projects' own member clouds can
ever occupy the same world-space region once this pass has run.

LEVEL 2, one project at a time: `_level2_raw_layout_for_project` reuses
`_build_physics_graph` UNCHANGED, scoped to just that project's own members
(semantic springs, district/community gravity, the collapsed-container fix's
1/member-count container weight). The project's own vertex is included as an
ordinary participant, so its FINAL FR position (not the origin) is what the whole
subgraph gets recentred on, then a LOCAL `_declump` pass makes this raw layout
already floor-respecting in its own unscaled units, computed BEFORE level 1 runs
so level 1's own `_separate_extents` can use each project's REAL extent
(`_level2_extent`) rather than `_level1_radius`'s nominal guess. `_level2_finalize`
then only ever SCALES UP (never down) to fill the nominal disc when there's room,
translated onto the level-1 centroid. This is THE RESCALE COMPRESSION FIX (live
specimen, first real migration attempt: two members landed 0.14-0.93 units apart
after the OLD scheme's single downward percentile-based rescale compressed an
already-tight FR cluster, dense core plus a few far outliers driving up the
95th-percentile radius, straight past the floor). Same measure-the-real-thing
pattern `_hub_zone_radius` already uses for THE HUB ZONE, generalised to every
project.

CROSS-PROJECT BRIDGING (item 3): a member with a live semantic edge to a member of a
DIFFERENT project gets a small post-hoc nudge (`_apply_bridge_nudges`) toward that
other project's own level-1 centroid, so a bridging member settles nearer the facing
edge of its own project's disc. Disclosed simplification: folding this directly into
level 2's own FR would mean merging two very different coordinate scales (a project's
own local spread vs. the whole graph's level-1 extent) inside one force integration;
a bounded post-hoc nudge sidesteps that without needing a third coordinate system.

UNFILED OBJECTS (item 4): `_place_unfiled`. One with a live semantic link to an
already-placed object lands at the mean position of those neighbours; one with NONE
scatters as a proper 2D Gaussian (Box-Muller polar form, not a fixed-radius ring)
centred on the whole placed cloud's own centroid, std set from that cloud's own
spread, so density falls off smoothly outward instead of forming the "ring spike"
measurement flagged on the v7 layout.

VERIFIED DECLUMP (item 2, then PROPORTIONAL): the final global declump is
`_declump_until_converged_or_budget`: small chunks, re-measured after each,
stopping once the worst deficit clears `_PHYSICS_DECLUMP_CONVERGED_RATIO` or
`layout.physics_declump_budget_secs` wall-clock is spent, whichever first,
followed by `_verify_min_separation`, which raises `DeclumpVerificationFailed`
loudly only when the worst pair found is under `_PHYSICS_VERIFY_FAIL_RATIO * min_sep`.
It started as a fixed epsilon below the hard floor; a live measurement on one
attempt (worst pair 13.566 of 15, a convergence residual rather than a collapse,
and effectively invisible) showed a fixed-epsilon floor treated a narrow residual
the same as a genuine collapse. `_declump`'s own per-call iteration cap is silent
about non-convergence (it just stops), which is exactly how v7's nn p50 2.8 went
unnoticed until it was measured live: the hierarchical layout is designed so
declump only ever does bounded local cleanup at reasonable density and should
almost always converge comfortably before the proportional line; this pairing
(converge-or-budget, then a proportional check) is the tripwire for "it genuinely
didn't," not a bare epsilon that flags ordinary residual noise as a failure.
"""
from __future__ import annotations

import math
import random
import resource
import time
import uuid
from collections import defaultdict
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import asyncpg
import igraph as ig
import numpy as np

from src.actions.core import Actions
from src.ontology.link_classes import CONTAINER_LINK_TYPES, STRUCTURAL_LINK_TYPES
from src.orchestrator.graph_layout import (
    _MEMBERSHIP_CONTAINER_LINK_TYPES,
    _MIN_SEPARATION,
    _bulk_assert_positions,
    _declump,
    _grid_cells,
    _hash01,
    _hub_ids,
    _neighbor_cell_indices,
    _release_layout_lock,
    _sunflower_point,
    _try_acquire_layout_lock,
    positions_for,
)
from src.orchestrator.project_identity import resolve_merge_survivors

_CONTAINER_SPRING_WEIGHT = 0.05  # flat, NOT degree-normalised: "weak gravity," never
                                # a real spring; small enough that a member's own
                                # semantic springs (typically >= a few tenths after
                                # degree normalisation) still dominate its final
                                # resting position, container pull only matters when a
                                # member has few or no semantic edges of its own.
_COMMUNITY_MIN_MEMBERS = 500  # a project this size or smaller reads fine as one
                              # cluster; above it, internal structure ("districts")
                              # becomes worth drawing.
_COMMUNITY_MIN_SIZE = 3  # a detected community below this size is noise, not a real
                         # district: its members fold back to a direct project edge
                         # rather than mint a near-empty synthetic vertex.
_SEED_SPACING = 15.0  # same NODE_SPACING scale graph_layout.py's own declump uses,
                      # for a numerically comparable starting scatter.
_SEMANTIC_TYPE_WEIGHT: dict[str, float] = {}  # hook for future per-type tuning
_DEFAULT_SEMANTIC_WEIGHT = 1.0  # every semantic type shares this today: "weight by
                                # type" is a real mechanism (the dict above), just
                                # empty pending real per-type importance data; degree
                                # normalisation is the only differentiating signal now.
_PHYSICS_LAYOUT_VERSION = 9  # graph_layout._LAYOUT_VERSION must match this, bumped
                             # together so the incremental heartbeat and this one-shot
                             # migration always agree on what "current" means.
_DEFAULT_PHYSICS_MAX_BYTES = 2_000_000_000  # layout.physics_max_bytes' own default;
                                            # see _memory_guard's own docstring for
                                            # what this actually checks.

# HIERARCHICAL PHYSICS (v8) -----------------------------------------------------
_LEVEL1_RADIUS_K = 0.75  # R_p = k * sqrt(N_p) * _NODE_SPACING: empirically sized so
                         # a project's own members, hex-packed at the min-sep floor,
                         # roughly fill a disc of this radius (area argument: N*s^2 ~=
                         # pi*R^2*0.9 packing factor -> k ~= 0.6; 0.75 leaves FR's own
                         # non-uniform spread (denser center, sparser fringe) headroom
                         # without pushing the acceptance's nn p50 [15,25] band high).
_LEVEL1_GUTTER = 3 * _MIN_SEPARATION  # extra clearance beyond R_a+R_b between any two
                                      # project discs: the acceptance line only
                                      # requires >= R_a+R_b; a real gutter keeps a
                                      # bridging member's own facing-edge nudge (below)
                                      # from ever pushing it into the NEXT project.
_INTRA_PROJECT_GUTTER = _MIN_SEPARATION  # THE INTRA-PROJECT GUTTER FIX (live
                                         # specimen: the first real v8 write measured
                                         # as a 163k-unit bounding box with 0.32
                                         # same-project 5-NN purity: not compact, not
                                         # separated, fully interleaved.
                                         # `_level2_raw_layout_for_project`'s own
                                         # recursive community split reused
                                         # `_level1_layout` with the SAME
                                         # `_LEVEL1_GUTTER` a whole-graph PROJECT pair
                                         # needs: for a large project split into
                                         # dozens of communities, that many pairwise
                                         # 45-unit-plus clearances compounds into a
                                         # sprawling archipelago (a giant project's
                                         # own real extent measured in THOUSANDS of
                                         # units, not the nominal formula's hundreds),
                                         # whose own internal gaps are large enough
                                         # for an entire SMALLER project to nest
                                         # inside geometrically, even though every
                                         # pairwise project-centroid distance still
                                         # satisfies R_a+R_b+gutter on paper.
                                         # `_separate_extents` only ever pushes
                                         # CENTROIDS apart, it has no notion of a
                                         # porous shape. Communities within ONE
                                         # project don't need PROJECT-scale clearance
                                         # from each other (they are still meant to
                                         # read as one project, just with internal
                                         # districts): min_sep alone keeps them
                                         # visually distinct without ballooning the
                                         # project's own overall footprint.
_LEVEL1_FR_ITERATIONS = 500  # a small graph (one vertex per project): generous
                             # iteration budget costs nothing at this vertex count.
_LEVEL1_SEPARATION_ITERATIONS = 300  # bounded like `_declump`'s own cap; separating
                                     # one pair can nudge another back together, this
                                     # many passes is enough to converge in practice
                                     # for a fleet-scale project count.
_LEVEL1_SEED_SPACING = 200.0  # starting scatter scale for the level-1 FR seed:
                              # `_separate_extents` corrects the real spacing
                              # regardless, this only needs to be roughly the right
                              # order of magnitude so FR's own repulsion has room.
_LEVEL2_FR_ITERATIONS = 200  # one project's own members only: converges faster
                             # than the old whole-graph v7 pass at the same iteration
                             # count, since there's no longer a 50,000-vertex graph
                             # to relax in a single FR call.
_CROSS_PROJECT_BRIDGE_NUDGE = 0.15  # fraction of the remaining distance a bridging
                                    # member is nudged toward the OTHER project's own
                                    # centroid (item 3): small enough that the
                                    # member stays inside its own project's disc
                                    # (bounded by `_LEVEL1_GUTTER`'s own clearance),
                                    # large enough to visibly favour the facing edge.
_UNFILED_FOG_MIN_STD = 5 * _MIN_SEPARATION  # floor for the density-falloff Gaussian's
                                            # own spread when nothing has been placed
                                            # yet to measure a real cloud from (an
                                            # empty-graph edge case, never the live
                                            # population).
_PHYSICS_VERIFY_FAIL_RATIO = 0.75  # PROPORTIONAL VERIFICATION:
                                   # `_verify_min_separation` refuses only when the
                                   # worst pair is under this fraction of min_sep.
                                   # A live measurement on one attempt (worst pair
                                   # 13.566 of 15, a convergence residual rather
                                   # than a collapse, effectively invisible) showed
                                   # a fixed-epsilon floor (the OLD
                                   # `_MIN_SEP_EPSILON` scheme) treated a narrow
                                   # residual miss the same as a genuine collapse
                                   # (three earlier specimens, all under 1 unit
                                   # apart, comfortably still fail well inside this
                                   # line).
_PHYSICS_DECLUMP_CONVERGED_RATIO = 0.05  # `_declump_until_converged_or_budget`
                                         # stops iterating once the worst deficit
                                         # from min_sep clears this fraction (i.e.
                                         # the worst pair is within 95% of the
                                         # floor): comfortably inside
                                         # `_PHYSICS_VERIFY_FAIL_RATIO`'s own 0.75
                                         # line, so a converged run essentially
                                         # never trips verification.
_PHYSICS_DECLUMP_CHUNK = 30  # `_declump_until_converged_or_budget`'s own
                             # re-measurement granularity: re-checks the worst
                             # pairwise distance after this many `_declump`
                             # iterations rather than after every single one
                             # (cheap either way, but `_worst_pair_distance`'s own
                             # grid scan is O(n), no need to pay it every step).
_DEFAULT_PHYSICS_DECLUMP_BUDGET_SECS = 120  # layout.physics_declump_budget_secs'
                                            # own default: the wall-clock ceiling
                                            # `_declump_until_converged_or_budget`
                                            # respects regardless of convergence.
_PHYSICS_DECLUMP_ITERATIONS = 150  # started at 30 (graph_layout._declump's own
                                  # default), doubled to 60 after a live flake in a
                                  # full-suite serial gate: a hermetic 3-object
                                  # all-unfiled population's Gaussian fog
                                  # occasionally started a genuinely slow-converging
                                  # small-N configuration, left a pair 14.24 units
                                  # apart under the floor. THE RECURSIVE HIERARCHY
                                  # FIX (a later migration attempt) then fixed the
                                  # ~12,000-member project's own O(n)-scale
                                  # convergence problem STRUCTURALLY, not by more
                                  # iterations (measured: 7m24s at 200 iterations
                                  # for that ONE project alone, still failed); once
                                  # that landed, the remaining failures were genuine
                                  # borderline convergence gaps on ordinary-sized
                                  # populations (measured across repeat
                                  # --verify-only runs against the live population:
                                  # 14.08 and 14.462 of 15 units, both narrow
                                  # misses, not the multi-unit gaps a structural bug
                                  # produces). 150 verified clean on three
                                  # consecutive live --verify-only runs
                                  # (~2m50s-2m59s wall clock each, well inside the
                                  # 10-minute acceptance): headroom for residual
                                  # per-run variance (the live population itself
                                  # shifts between runs), not a sign the algorithm
                                  # needs doubling everywhere: every OTHER
                                  # `_declump` caller (the incremental heartbeat)
                                  # keeps its own plain 30-iteration default. USED
                                  # BY the intermediate passes only (each project's
                                  # own raw layout, each community's own raw
                                  # layout, the post-nudge re-settle): the FINAL
                                  # global pass moved to
                                  # `_declump_until_converged_or_budget`, which no
                                  # longer takes a fixed count.
_VERIFY_MAX_CANDIDATES = 2000  # a cell-pair candidate count above this is treated as
                               # an outright verification failure rather than paying
                               # for the full pairwise check: this many points
                               # sharing a min-sep neighbourhood already means
                               # declump did not converge; see
                               # `_verify_min_separation`'s own docstring.
_HUB_ZONE_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")  # sentinel level-1
                            # vertex for THE HUB ZONE (live specimen, first real
                            # migration attempt: hubs snapped to the raw
                            # centroid-of-everything with only a 1-unit jitter
                            # collided with a dense project's own disc sitting right
                            # at that centroid; DeclumpVerificationFailed caught it,
                            # a pair 1.664 units apart under the 15-unit floor). A
                            # fixed low-valued UUID, not drawn from the same uuid4
                            # generator that mints every real object id, so a
                            # collision is not just unlikely, it needs one of this
                            # system's own object ids to have been minted OUTSIDE
                            # `uuid.uuid4()`. Giving the hub cluster its OWN radius
                            # budget and running it through the SAME
                            # `_separate_extents` pass as every real project is what
                            # actually guarantees it never lands inside one again.


async def _active_object_ids(actions: Actions) -> list[uuid.UUID]:
    """Every active object's id, oldest-created first. This ORDER is itself the
    stable creation-order rank the deterministic FR seed keys on (index into this
    list), the same convention every earlier layout version used."""
    rows = await actions.pool.fetch(
        "SELECT id FROM objects WHERE status NOT IN ('archived','merged','retired') "
        "ORDER BY created_at, id")
    return [r["id"] for r in rows]


async def _live_link_rows(actions: Actions) -> list[asyncpg.Record]:
    return await actions.pool.fetch(  # type: ignore[no-any-return]
        "SELECT from_id, to_id, type FROM links "
        "WHERE valid_until IS NULL OR valid_until > now()")


async def _project_membership(actions: Actions) -> dict[uuid.UUID, uuid.UUID]:
    """object_id -> project_id, UNION of a LIVE in_repo link (DISTINCT ON the
    object, lowest link id wins a rare multi-project membership) and a `project`
    assertion mapped to its repo object by canonical `repo:<name>` (membership is
    in_repo UNION the project assertion). THE MEMBERSHIP UNION FIX (live specimen,
    a follow-up measurement after the first real v8 write): 16,226 objects carried
    a `project` assertion with ZERO carrying an in_repo link at the same time
    (threads/decisions/messages minted with a project but never actually linked
    in_repo), and the OLD in_repo-only query laid every one of them out as unfiled
    fog. in_repo wins when an object somehow carries both and they disagree (the
    assertion is a `dict.setdefault` fallback, never an override).

    MEMBERSHIP FOLLOWS A MERGE: either source can name a SoftwareProject that has
    since been folded into a survivor (`status='merged'`, `merged_into` set): an
    in_repo link minted before the fold, or a `project` assertion whose bare name
    still resolves to the now-merged object's own canonical, neither one rewritten
    by the fold itself (resolve-on-read, same doctrine as every other merged_into
    reader in this codebase). Both project_id columns above are resolved through
    `resolve_merge_survivors` before the union so a member never lands in a
    district that no longer draws: an id the resolver can't place (a broken/cyclic
    chain) passes through unresolved rather than being dropped from membership
    entirely."""
    rows = await actions.pool.fetch(
        "SELECT DISTINCT ON (l.from_id) l.from_id AS object_id, l.to_id AS project_id "
        "FROM links l WHERE l.type='in_repo' "
        "  AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "ORDER BY l.from_id, l.id")
    membership = {r["object_id"]: r["project_id"] for r in rows}

    assertion_rows = await actions.pool.fetch(
        "SELECT a.object_id, p.id AS project_id "
        "FROM current_assertions a "
        "JOIN objects p ON p.type='SoftwareProject' "
        "  AND p.canonical = 'repo:' || (a.value #>> '{}') "
        "WHERE a.name='project'")
    for r in assertion_rows:
        membership.setdefault(r["object_id"], r["project_id"])

    survivors = await resolve_merge_survivors(actions.pool, set(membership.values()))
    for oid, pid in membership.items():
        membership[oid] = survivors.get(pid, pid)
    return membership


def _semantic_weight(link_type: str, deg_u: int, deg_v: int) -> float:
    base = _SEMANTIC_TYPE_WEIGHT.get(link_type, _DEFAULT_SEMANTIC_WEIGHT)
    return base / math.sqrt(max(1, deg_u) * max(1, deg_v))


def _detect_communities(
    link_rows: list[asyncpg.Record],
    membership: dict[uuid.UUID, uuid.UUID],
    active: set[uuid.UUID],
) -> dict[uuid.UUID, tuple[uuid.UUID, int]]:
    """object_id -> (project_id, community_local_id) for every member of a real
    (>= `_COMMUNITY_MIN_SIZE`) detected community in a project over
    `_COMMUNITY_MIN_MEMBERS`: Leiden over that project's OWN internal semantic
    subgraph only (an edge strictly between two members of the same project, and not
    a container/structural type). A project at or under the threshold, or a member in
    a community too small to be a real district, is simply absent from the returned
    dict; the caller treats that as "attach directly to the project," no special
    casing needed on this function's own side.

    THE LEIDEN SEED FIX (bounding-box compactness follow-up, measured live):
    igraph's `community_leiden` draws from Python's own `random` module by
    default (its own documented behaviour, no wiring needed) with whatever state
    that module happens to be in. Unseeded, so the SAME population could and did
    measure a 4x-different bounding box between two live `--verify-only` runs of
    identical code, purely from a different community partition landing each
    time. Reseeding `random` from `pid` right before each project's own Leiden
    call (a project's community structure never depends on any OTHER project's,
    so a per-project seed can't leak cross-project correlation) makes every
    project's own partition, and therefore every downstream
    real_extent/bounding-box/purity number, reproduce identically run to run: the
    same "SEEDED, DETERMINISTIC" guarantee `_seed_positions` already gives the FR
    starting layout."""
    by_project: dict[uuid.UUID, list[uuid.UUID]] = defaultdict(list)
    for oid, pid in membership.items():
        if oid in active:
            by_project[pid].append(oid)

    out: dict[uuid.UUID, tuple[uuid.UUID, int]] = {}
    for pid, members in by_project.items():
        if len(members) <= _COMMUNITY_MIN_MEMBERS:
            continue
        local_idx = {oid: i for i, oid in enumerate(members)}
        edges: list[tuple[int, int]] = []
        for r in link_rows:
            f, t, lt = r["from_id"], r["to_id"], r["type"]
            if f not in local_idx or t not in local_idx or lt in STRUCTURAL_LINK_TYPES:
                continue
            edges.append((local_idx[f], local_idx[t]))
        sub = ig.Graph()
        sub.add_vertices(len(members))
        sub.add_edges(edges)
        random.seed(pid.int)
        clustering = sub.community_leiden(objective_function="modularity", n_iterations=2)
        sizes = clustering.sizes()
        for oid in members:
            comm = clustering.membership[local_idx[oid]]
            if sizes[comm] >= _COMMUNITY_MIN_SIZE:
                out[oid] = (pid, comm)
    return out


def _build_physics_graph(
    object_ids: list[uuid.UUID],
    link_rows: list[asyncpg.Record],
    communities: dict[uuid.UUID, tuple[uuid.UUID, int]],
    membership: dict[uuid.UUID, uuid.UUID] | None = None,
) -> tuple[ig.Graph, list[uuid.UUID | None]]:
    """The one shared graph every force in this layout acts on: real active objects
    (index-aligned to `object_ids`) plus one synthetic vertex per real (project,
    community) pair found by `_detect_communities` (appended after, no `object_id` of
    their own; `vertex_ids[i] is None` marks a synthetic row). Returns the graph and
    a vertex-index -> object_id-or-None list the caller strips synthetic rows with
    after layout.

    `membership`, when given (THE MEMBERSHIP UNION FIX): a member whose project
    comes ONLY from the `project` assertion (no in_repo link at all) gets NO
    container edge from the `link_rows` loop below, since that loop only ever
    reads real links; without one, that member is an isolated vertex FR has no
    reason to pull toward its own project at all. After the real-link container
    edges are built, every id in `object_ids` still missing one gets a synthetic
    edge straight to its own `membership` target (community-routed the same way
    a real in_repo edge would be, when one applies): the fallback a real link
    never needed."""
    idx = {oid: i for i, oid in enumerate(object_ids)}

    # total live-link degree per real object (ANY type): the same "weight" concept
    # graph_stream.py's own node weight array already computes, reused here as the
    # semantic-spring degree-normalisation denominator.
    degree = [0] * len(object_ids)
    for r in link_rows:
        f, t = r["from_id"], r["to_id"]
        if f in idx and t in idx:
            degree[idx[f]] += 1
            degree[idx[t]] += 1

    community_vertex: dict[tuple[uuid.UUID, int], int] = {}
    vertex_ids: list[uuid.UUID | None] = list(object_ids)
    for key in sorted(set(communities.values()), key=lambda k: (str(k[0]), k[1])):
        community_vertex[key] = len(vertex_ids)
        vertex_ids.append(None)

    # THE COLLAPSED-CONTAINER FIX: a flat container weight pulled EVERY member of
    # a shared container toward the exact same point with equal strength
    # regardless of how many siblings it had. For a container with thousands of
    # members and few or no semantic edges to differentiate them, that isn't
    # "weak gravity" in aggregate, it's a landslide (a live specimen: 6,131
    # points collapsed into one post-FR grid cell). The SAME edge weight
    # (_CONTAINER_SPRING_WEIGHT) is now divided by the container's own live
    # member count (1/N, not 1/sqrt(N): measured live, a 1,000-member test
    # container still landed 264 points in one post-FR cell at 1/sqrt(N), well
    # over the ~50-point acceptance line; 1/N brings it comfortably under), so a
    # container with N members pulls each one at 1/N strength. Sibling repulsion
    # (which every vertex exerts on every other regardless of edges) then
    # actually wins for a large container, letting members spread out around it
    # instead of collapsing onto it. Counted by FINAL destination (`dst_i`,
    # already resolved to a synthetic community vertex where one applies) so a
    # member routed through a district's own vertex is counted against THAT
    # vertex's member count, not the whole project's.
    container_edges: list[tuple[int, int]] = []
    dst_member_counts: dict[int, int] = defaultdict(int)
    for r in link_rows:
        f, t, lt = r["from_id"], r["to_id"], r["type"]
        if f not in idx or t not in idx or lt not in CONTAINER_LINK_TYPES:
            continue
        i, j = idx[f], idx[t]
        comm = communities.get(f)
        reroute = lt == "in_repo" and comm is not None and comm[0] == t
        dst_i = community_vertex[comm] if reroute and comm is not None else j
        container_edges.append((i, dst_i))
        dst_member_counts[dst_i] += 1

    if membership:
        has_edge = {i for i, _dst in container_edges}
        for oid in object_ids:
            i = idx[oid]
            if i in has_edge:
                continue
            target = membership.get(oid)
            if target is None or target not in idx or target == oid:
                continue
            comm = communities.get(oid)
            reroute = comm is not None and comm[0] == target
            dst_i = community_vertex[comm] if reroute and comm is not None else idx[target]
            container_edges.append((i, dst_i))
            dst_member_counts[dst_i] += 1

    edges: list[tuple[int, int]] = []
    weights: list[float] = []
    for r in link_rows:
        f, t, lt = r["from_id"], r["to_id"], r["type"]
        if f not in idx or t not in idx or lt in CONTAINER_LINK_TYPES:
            continue
        if lt not in STRUCTURAL_LINK_TYPES:
            i, j = idx[f], idx[t]
            edges.append((i, j))
            weights.append(_semantic_weight(lt, degree[i], degree[j]))

    for i, dst_i in container_edges:
        w = _CONTAINER_SPRING_WEIGHT / max(1, dst_member_counts[dst_i])
        edges.append((i, dst_i))
        weights.append(w)

    for (_pid, _comm), cvi in community_vertex.items():
        pi = idx.get(_pid)
        if pi is not None:
            w = _CONTAINER_SPRING_WEIGHT / max(1, dst_member_counts.get(cvi, 1))
            edges.append((cvi, pi))
            weights.append(w)

    g = ig.Graph()
    g.add_vertices(len(vertex_ids))
    g.add_edges(edges)
    g.es["weight"] = weights
    return g, vertex_ids


def _seed_positions(vertex_ids: list[uuid.UUID | None]) -> np.ndarray:
    """Deterministic FR starting scatter: a real object seeds at
    `_sunflower_point` keyed on its own stable creation-order rank (its index in
    `object_ids`, i.e. its position in this same list before synthetic rows were
    appended); a synthetic community vertex seeds at a small deterministic jitter
    around the origin (structural scaffolding only)."""
    out = np.zeros((len(vertex_ids), 2))
    for i, oid in enumerate(vertex_ids):
        if oid is not None:
            out[i] = _sunflower_point(i, _SEED_SPACING)
        else:
            angle = _hash01(f"community-seed:{i}") * 2 * math.pi
            out[i] = (10.0 * math.cos(angle), 10.0 * math.sin(angle))
    return out


class MemoryBudgetExceeded(Exception):
    """Raised by `_memory_guard` when the POST-FR positions it's handed would need
    more than `layout.physics_max_bytes` on a hypothetical quadratic fallback;
    see that function's own docstring for why it checks post-FR, not the seed."""


def _level1_radius(n_members: int) -> float:
    """R_p = k * sqrt(N_p) * spacing (item 1 of the hierarchical physics scheme):
    a project's own radius budget in the level-1 contracted layout."""
    return max(_MIN_SEPARATION, _LEVEL1_RADIUS_K * math.sqrt(max(1, n_members)) * _MIN_SEPARATION)


def _cross_project_edges(
    link_rows: list[asyncpg.Record], membership: dict[uuid.UUID, uuid.UUID],
    project_ids: set[uuid.UUID],
) -> dict[tuple[uuid.UUID, uuid.UUID], float]:
    """One aggregated weighted edge per unordered (project_a, project_b) pair,
    counting every live semantic (non-container, non-structural) link between a
    member of one and a member of the other: level 1's own spring weights, and
    the same population `_apply_bridge_nudges` walks again for item 3's per-member
    nudge."""
    counts: dict[tuple[uuid.UUID, uuid.UUID], int] = defaultdict(int)
    for r in link_rows:
        f, t, lt = r["from_id"], r["to_id"], r["type"]
        if lt in CONTAINER_LINK_TYPES or lt in STRUCTURAL_LINK_TYPES:
            continue
        pf, pt = membership.get(f), membership.get(t)
        if pf is None or pt is None or pf == pt or pf not in project_ids or pt not in project_ids:
            continue
        key = (pf, pt) if str(pf) <= str(pt) else (pt, pf)
        counts[key] += 1
    return {k: float(v) for k, v in counts.items()}


def _separate_extents(
    pos: np.ndarray, radii: np.ndarray, *,
    gutter: float = _LEVEL1_GUTTER, iterations: int = _LEVEL1_SEPARATION_ITERATIONS,
) -> np.ndarray:
    """Push every pair of discs (a centroid plus its own radius) apart until their
    centroid distance is at least the sum of their radii plus `gutter`: level 1's
    own extent-aware repulsion (item 1 of the hierarchical physics scheme), the same
    deterministic push-by-the-deficit shape `graph_layout._declump` uses for a
    shared floor, keyed here on each vertex's own radius instead. THIS is what stops
    two projects' member clouds from ever occupying the same world-space region.
    FR's own repulsion alone (as v7 showed) only ever approaches separation, never
    guarantees it, and a whole project's own hundreds-of-units spread makes that gap
    catastrophic rather than cosmetic."""
    n = len(pos)
    if n < 2:
        return pos
    pos = pos.copy()
    for _ in range(iterations):
        diff = pos[:, None, :] - pos[None, :, :]
        dist = np.sqrt((diff ** 2).sum(axis=-1))
        want = radii[:, None] + radii[None, :] + gutter
        np.fill_diagonal(dist, np.inf)
        violation = want - dist
        np.fill_diagonal(violation, 0.0)  # never -inf: that would multiply against a
                                          # 0 direction vector below and raise an
                                          # "invalid value" warning for a value
                                          # `mask` was already going to discard.
        if np.all(violation <= 1e-6):
            break
        mask = violation > 0
        safe_dist = np.where(dist > 1e-9, dist, 1e-9)
        direction = diff / safe_dist[..., None]
        push = np.where(mask[..., None], direction * (violation[..., None] / 2), 0.0)
        pos = pos + push.sum(axis=1)
    return pos


# THE COMPACT ARRANGEMENT (v9) ---------------------------------------------------
# Replaces the FR-then-separate-extents combine `_separate_extents`/the old
# `_level1_layout` body used with SEED (a stress layout) + ANCHOR (Procrustes to the
# previous run) + PACK (front-chain circle packing); see `_level1_layout`'s own
# docstring below for the full shape and why this is what actually shrinks the
# bounding box rather than pushing an already-sprawled FR result further apart.
# `_separate_extents` itself is left in place, still directly unit-tested, as a
# reusable primitive: not deleted, just no longer this function's own compaction
# step.


def _tangent_candidates(
    ax: float, ay: float, ar: float, bx: float, by: float, br: float, r: float,
) -> list[tuple[float, float]]:
    """Both points where a circle of radius `r` sits externally tangent to circle
    a=(ax,ay,ar) AND circle b=(bx,by,br): the two-circle intersection of radii
    (ar+r) and (br+r) centred at a and b. Empty when the two required circles don't
    intersect (a and b too far apart, or too close, for any tangent-to-both circle
    of this radius to exist); the caller tries every adjacent frontier pair, so an
    empty result here just means this particular pair isn't a valid placement."""
    dx, dy = bx - ax, by - ay
    d2 = dx * dx + dy * dy
    if d2 < 1e-12:
        return []
    d = math.sqrt(d2)
    ra2, rb2 = ar + r, br + r
    along = (d2 + ra2 * ra2 - rb2 * rb2) / (2 * d)
    h2 = ra2 * ra2 - along * along
    if h2 < 0:
        return []
    h = math.sqrt(h2)
    ex, ey = dx / d, dy / d
    mx, my = ax + ex * along, ay + ey * along
    if h < 1e-9:
        return [(mx, my)]
    return [(mx - ey * h, my + ex * h), (mx + ey * h, my - ex * h)]


def _overlaps_any(
    cx: float, cy: float, r: float, pos: dict[Any, np.ndarray], radii: dict[Any, float],
    *, exclude: set[Any],
) -> bool:
    for k, p in pos.items():
        if k in exclude:
            continue
        dx, dy = p[0] - cx, p[1] - cy
        min_d = radii[k] + r
        if dx * dx + dy * dy < min_d * min_d - 1e-6:
            return True
    return False


def _pack_siblings(
    order: list[Any], radii: dict[Any, float], *, gutter: float = 0.0,
) -> dict[Any, np.ndarray]:
    """PACK: front-chain circle packing (Wang et al. CHI 2006; d3-hierarchy's own
    `pack.siblings`). A DISCLOSED SIMPLIFICATION, an explicitly authorized fallback
    ("d3-style packSiblings with a fixed insertion order") for when full
    proximity-preserving overlap removal is too much for one pass. d3's own
    algorithm prunes circles from the frontier as later ones enclose them, for
    O(n log n) total; this version keeps every placed circle on the frontier and
    tries EVERY consecutive frontier pair for each new circle, O(n) candidates
    per insertion x O(n) overlap check each = O(n^2) per insertion, O(n^3) total:
    comfortably fast at this function's own scale (a project or community
    population, never the 51k-object graph itself; see the module's own
    acceptance result for the measured wall-clock this run).

    `order` fixes insertion order: packSiblings has no notion of a target
    position, only order, so `_pack_order` (giant first, then a nearest-neighbour
    walk over the SEED+ANCHOR positions) is what lets "related districts sit near
    each other" survive into a from-scratch pack. `gutter` inflates every radius by
    half its own value before packing (so two tangent circles land `gutter` apart,
    not touching); returned positions are keyed on the TRUE (uninflated) radii the
    caller already has, only the packing math ever sees the inflated ones.

    Deterministic: candidate selection ties break on `str(id)` via `_pack_order`'s
    own tie-break, never on dict/set iteration order."""
    if not order:
        return {}
    inflated = {k: radii[k] + gutter / 2 for k in order}
    if len(order) == 1:
        return {order[0]: np.zeros(2)}
    a, b = order[0], order[1]
    pos: dict[Any, np.ndarray] = {a: np.array([0.0, 0.0]),
                                   b: np.array([inflated[a] + inflated[b], 0.0])}
    frontier = [a, b]
    if len(order) == 2:
        return pos
    for k in order[2:]:
        rk = inflated[k]
        best: tuple[int, tuple[float, float]] | None = None
        best_d: float | None = None
        n = len(frontier)
        for i in range(n):
            fa, fb = frontier[i], frontier[(i + 1) % n]
            ax, ay = pos[fa]
            bx, by = pos[fb]
            for cx, cy in _tangent_candidates(
                    ax, ay, inflated[fa], bx, by, inflated[fb], rk):
                if _overlaps_any(cx, cy, rk, pos, inflated, exclude={fa, fb}):
                    continue
                d = cx * cx + cy * cy
                if best_d is None or d < best_d:
                    best_d = d
                    best = (i, (cx, cy))
        if best is None:
            # DEGENERATE FALLBACK (should not occur for well-formed positive
            # radii; no live population has hit this): place tangent to the
            # single frontier circle farthest from the origin, along its own
            # outward ray, rather than raise mid-migration.
            far = max(frontier, key=lambda f: float(np.linalg.norm(pos[f])) + inflated[f])
            fx, fy = pos[far]
            ang = math.atan2(fy, fx) if (fx or fy) else 0.0
            pos[k] = np.array([fx + math.cos(ang) * (inflated[far] + rk),
                                fy + math.sin(ang) * (inflated[far] + rk)])
            frontier.append(k)
            continue
        i, (cx, cy) = best
        pos[k] = np.array([cx, cy])
        frontier.insert(i + 1, k)
    return pos


def _procrustes_transform(
    new_pts: np.ndarray, old_pts: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """ANCHOR's own alignment step: Kabsch rotation (no scale, no reflection) that
    best-fits `new_pts` onto `old_pts` (same row order, both (n,2), n>=2).
    Returns (R, new_centroid, old_centroid) rather than the aligned points
    themselves, so `_apply_procrustes` can apply the SAME transform to vertices
    that were never part of the fit (a project new since the last run rides along
    under its own neighbours' alignment). Reflection is explicitly excluded (the
    det-sign correction below): a mirrored map would be as stable numerically
    but would flip every reader's mental map, which is exactly what ANCHOR exists
    to prevent."""
    if len(new_pts) < 2:
        return np.eye(2), np.zeros(2), np.zeros(2)
    new_c = new_pts.mean(axis=0)
    old_c = old_pts.mean(axis=0)
    a = new_pts - new_c
    b = old_pts - old_c
    h = a.T @ b
    u, _, vt = np.linalg.svd(h)
    det = np.linalg.det(vt.T @ u.T)
    d = 1.0 if det >= 0 else -1.0
    correction = np.diag([1.0, d])
    r = vt.T @ correction @ u.T
    return r, new_c, old_c


def _apply_procrustes(
    pts: np.ndarray, r: np.ndarray, new_c: np.ndarray, old_c: np.ndarray,
) -> np.ndarray:
    return np.asarray((r @ (pts - new_c).T).T + old_c)


def _stress_seed_layout(
    ids: list[uuid.UUID], cross_edges: dict[tuple[uuid.UUID, uuid.UUID], float],
) -> dict[uuid.UUID, np.ndarray]:
    """SEED: Kamada-Kawai stress majorization (igraph's own `layout_kamada_kawai`,
    no new dependency) over the small project-or-community contracted graph,
    replacing the v8 FR seed. Stress layout minimises |geometric distance - graph
    distance| directly, so cross-linked vertices land near each other with no
    separate "separation" pass fighting an already-sprawled result (see
    `_level1_layout`'s own docstring for why this is the actual compactness fix,
    not `_pack_siblings` alone: PACK only ever removes overlap; SEED is what
    decides who ends up ADJACENT once it's removed). An edgeless population (no
    cross edges at all) has nothing for stress majorization to minimise; the
    deterministic sunflower seed IS the layout in that case, same degenerate
    handling FR always had."""
    idx = {pid: i for i, pid in enumerate(ids)}
    g = ig.Graph()
    g.add_vertices(len(ids))
    edges: list[tuple[int, int]] = []
    weights: list[float] = []
    for (a, b), w in cross_edges.items():
        if a in idx and b in idx:
            edges.append((idx[a], idx[b]))
            weights.append(w)
    g.add_edges(edges)
    seed_pts = np.array([_sunflower_point(i, _LEVEL1_SEED_SPACING) for i in range(len(ids))])
    if g.ecount() == 0:
        return {pid: seed_pts[i] for i, pid in enumerate(ids)}
    coords = g.layout_kamada_kawai(
        weights=weights, seed=seed_pts.tolist(), maxiter=_LEVEL1_FR_ITERATIONS)
    pos = np.array(coords.coords)
    return {pid: pos[i] for i, pid in enumerate(ids)}


def _pack_order(
    ids: list[uuid.UUID], radii: dict[uuid.UUID, float], seed: dict[uuid.UUID, np.ndarray],
    cross_edges: dict[tuple[uuid.UUID, uuid.UUID], float] | None = None,
) -> list[uuid.UUID]:
    """GIANT FIRST (the authorized fallback's own wording), then a greedy walk:
    from the current vertex, prefer its STRONGEST still-unplaced graph neighbour
    (`cross_edges`, descending weight) when one exists, else fall back to the
    nearest still-unplaced vertex by SEED(+ANCHOR) position. `_pack_siblings`
    itself has no notion of a target position, only order, so this walk is the
    ENTIRE mechanism by which "related districts sit near each other" survives
    into a from-scratch pack: direct graph adjacency first, since it is a
    stronger, unambiguous signal than SEED's own geometry (a disconnected or
    near-degenerate component, e.g. an isolated singleton project with no
    cross-links at all, gives kamada_kawai nothing to optimise, and two such
    isolated vertices can land at a coincidentally similar SEED distance from a
    genuinely linked pair; a real graph edge never has that ambiguity).
    Deterministic: every tie breaks on `str(id)`, never on set/dict iteration
    order."""
    neighbours: dict[uuid.UUID, list[tuple[float, uuid.UUID]]] = defaultdict(list)
    if cross_edges:
        for (x, y), w in cross_edges.items():
            if x in radii and y in radii:
                neighbours[x].append((w, y))
                neighbours[y].append((w, x))
        for lst in neighbours.values():
            lst.sort(key=lambda wv: (-wv[0], str(wv[1])))

    remaining = set(ids)
    giant = max(ids, key=lambda i: (radii[i], str(i)))
    order = [giant]
    remaining.discard(giant)
    cur = giant
    while remaining:
        nxt = next((v for _, v in neighbours.get(cur, ()) if v in remaining), None)
        if nxt is None:
            nxt = min(remaining, key=lambda i: (
                float(np.linalg.norm(seed[i] - seed[cur])), str(i)))
        order.append(nxt)
        remaining.discard(nxt)
        cur = nxt
    return order


def _level1_layout(
    project_ids: list[uuid.UUID], radii: dict[uuid.UUID, float],
    cross_edges: dict[tuple[uuid.UUID, uuid.UUID], float], *,
    gutter: float = _LEVEL1_GUTTER,
    anchor: dict[uuid.UUID, np.ndarray] | None = None,
) -> dict[uuid.UUID, np.ndarray]:
    """THE COMPACT ARRANGEMENT (v9): one vertex per project (or, one level
    deeper, per community), SEED+ANCHOR+PACK. See
    `_stress_seed_layout`/`_procrustes_transform`+`_apply_procrustes`/
    `_pack_order`+`_pack_siblings`'s own docstrings for each step. Replaces v8's
    FR-then-`_separate_extents` combine (module docstring's HIERARCHICAL PHYSICS
    section, now superseded by this one): FR sprawls with no compactness target of
    its own, and pushing an already-sprawled result apart only ever grows the
    sprawl further (measured live, a 65-90k bounding box against a ~25k target).
    SEED (stress majorization) picks who's adjacent; PACK (circle packing) removes
    overlap WITHOUT re-sprawling, since packing is compaction by construction
    (circles nest, they never get pushed further apart than tangent).

    `gutter` defaults to the whole-graph project-vs-project clearance but is
    overridden much smaller (THE INTRA-PROJECT GUTTER FIX) when this same
    function is reused one level deeper for a project's own communities; see
    `_level2_raw_layout_for_project`'s own docstring for why. `anchor`, when
    given, names a PREVIOUS run's own position for zero or more of these
    vertices (read from `graph_x`/`graph_y` for real project objects; `None` at
    the community level today, since no synthetic community vertex position is
    persisted yet, a disclosed limitation, see run_physics_migrate's own
    docstring). With two or more anchored vertices, the whole SEED layout is
    Procrustes-aligned onto the anchor's own frame before packing, so a rerun
    moves as little as the data allows (stability, not just compactness)."""
    if not project_ids:
        return {}
    if len(project_ids) == 1:
        return {project_ids[0]: np.zeros(2)}
    seed = _stress_seed_layout(project_ids, cross_edges)
    if anchor:
        common = [pid for pid in project_ids if pid in anchor]
        if len(common) >= 2:
            new_pts = np.array([seed[pid] for pid in common])
            old_pts = np.array([anchor[pid] for pid in common])
            r, new_c, old_c = _procrustes_transform(new_pts, old_pts)
            for pid in project_ids:
                seed[pid] = _apply_procrustes(seed[pid][None, :], r, new_c, old_c)[0]
    order = _pack_order(project_ids, radii, seed, cross_edges)
    return _pack_siblings(order, radii, gutter=gutter)


_NOISE_COMMUNITY_KEY = -1  # local-community-id sentinel for "no real community for
                           # THIS project": Leiden's own local ids start at 0, so
                           # -1 never collides with a real one.


def _level2_flat_raw_layout(
    pid: uuid.UUID, members: list[uuid.UUID], link_rows: list[asyncpg.Record],
    communities: dict[uuid.UUID, tuple[uuid.UUID, int]],
    membership: dict[uuid.UUID, uuid.UUID] | None = None,
) -> dict[uuid.UUID, np.ndarray]:
    """One (project or community) population's own internal FR pass, RAW: reuses
    `_build_physics_graph` UNCHANGED (semantic springs, district/community gravity,
    THE COLLAPSED-CONTAINER FIX's 1/member-count container weight), scoped to just
    `[pid, *members]` so container and semantic edges outside this population are
    dropped by that function's own `idx` membership check. `pid`'s own vertex is an
    ordinary participant, so its FINAL FR position, not the origin, is what the
    whole subgraph recentres on (`pos[0]` since `pid` is always first in
    `object_ids`), exactly mirroring how v7's flat layout let a container's own
    position be "wherever the pull leaves it" (when called for a community bucket
    from `_level2_raw_layout_for_project`, `pid` is still the PROJECT's own id:
    every bucket recentres on the same project vertex, which is fine since only the
    bucket's own MEMBER positions are ever read back out). Finished with a LOCAL
    `_declump` pass at the full `_MIN_SEPARATION` floor (cheap at this scale). THE
    RESCALE COMPRESSION FIX (live specimen, first real migration attempt: two
    members landed 0.14-0.93 units apart): a single global percentile-based
    downward rescale in the OLD scheme could compress an already-tight FR cluster
    (dense core, a few far outliers driving up the 95th-percentile radius)
    straight past the floor. Ending HERE, in raw/unscaled units, before any
    caller ever shrinks anything, is what actually prevents that; see
    `_level2_finalize`'s own docstring for why its own scale factor is never
    allowed below 1.0."""
    object_ids = [pid, *members]
    g, vertex_ids = _build_physics_graph(object_ids, link_rows, communities, membership)
    seed = _seed_positions(vertex_ids)
    coords = g.layout_fruchterman_reingold(
        weights=g.es["weight"] if g.ecount() else None,
        niter=_LEVEL2_FR_ITERATIONS, seed=seed.tolist(), grid=True)
    pos = np.array(coords.coords)[: len(object_ids)]
    pos = pos - pos[0]
    member_pos = pos[1:]
    if len(member_pos) == 0:
        return {}
    if len(member_pos) > 1:
        member_pos = _declump(
            member_pos, np.zeros((0, 2)), members, min_sep=_MIN_SEPARATION,
            iterations=_PHYSICS_DECLUMP_ITERATIONS)
    return {oid: member_pos[i] for i, oid in enumerate(members)}


def _project_community_buckets(
    pid: uuid.UUID, members: list[uuid.UUID],
    communities: dict[uuid.UUID, tuple[uuid.UUID, int]],
) -> dict[int, list[uuid.UUID]]:
    """This project's own members, bucketed by detected community-local-id
    (creation order preserved within each bucket). A member with no real
    community for THIS project (small project, or a genuinely edgeless member)
    collects under `_NOISE_COMMUNITY_KEY`."""
    buckets: dict[int, list[uuid.UUID]] = defaultdict(list)
    for oid in members:
        c = communities.get(oid)
        key = c[1] if c is not None and c[0] == pid else _NOISE_COMMUNITY_KEY
        buckets[key].append(oid)
    return dict(buckets)


def _community_vertex_id(pid: uuid.UUID, community_key: int) -> uuid.UUID:
    """A deterministic SYNTHETIC uuid (never a real object id: `uuid5` over a
    namespace derived from `pid` itself, so two different projects' own community
    #0 never collide) letting `_level1_layout`'s already-generic (radius,
    cross-edge) machinery get reused UNCHANGED one level deeper, for a project's
    own communities exactly the way it's used for the whole graph's own projects."""
    return uuid.uuid5(pid, f"community:{community_key}")


def _intra_project_community_edges(
    link_rows: list[asyncpg.Record], oid_community_vertex: dict[uuid.UUID, uuid.UUID],
) -> dict[tuple[uuid.UUID, uuid.UUID], float]:
    """One aggregated weighted edge per unordered (community_vertex_a,
    community_vertex_b) pair, counting every live semantic link between a member of
    one and a member of the other: the SAME aggregation `_cross_project_edges`
    does for the whole graph's own projects, one level deeper."""
    counts: dict[tuple[uuid.UUID, uuid.UUID], int] = defaultdict(int)
    for r in link_rows:
        f, t, lt = r["from_id"], r["to_id"], r["type"]
        if lt in CONTAINER_LINK_TYPES or lt in STRUCTURAL_LINK_TYPES:
            continue
        cf, ct = oid_community_vertex.get(f), oid_community_vertex.get(t)
        if cf is None or ct is None or cf == ct:
            continue
        key = (cf, ct) if str(cf) <= str(ct) else (ct, cf)
        counts[key] += 1
    return {k: float(v) for k, v in counts.items()}


def _level2_raw_layout_for_project(
    pid: uuid.UUID, members: list[uuid.UUID], link_rows: list[asyncpg.Record],
    communities: dict[uuid.UUID, tuple[uuid.UUID, int]],
    membership: dict[uuid.UUID, uuid.UUID] | None = None,
) -> dict[uuid.UUID, np.ndarray]:
    """RECURSIVE HIERARCHY: a project with roughly 12,000 members (nearly 7x the
    next-biggest) kept failing `_verify_min_separation` no matter how high
    `_PHYSICS_DECLUMP_ITERATIONS` went (measured: even 200 iterations, 7m24s wall
    clock for this ONE project alone, still failed). `_level2_flat_raw_layout`'s
    single FR-plus-declump pass over the WHOLE project doesn't scale to this
    population any better than the old whole-graph flat pass did, for the exact
    same reason: declump does bounded LOCAL cleanup, so it cannot fix
    macro-structure FR left too dense. The fix is the same one that fixed the
    whole graph: stop letting sub-populations overlap by construction, one level
    deeper.

    A project with more than one real community bucket
    (`_project_community_buckets`) gets its own MINI level-1/level-2 split: each
    community's own members get `_level2_flat_raw_layout`'d independently (a
    population of hundreds, not thousands), then `_level2_extent` measures each
    community's own REAL spread, `_level1_layout` (reused UNCHANGED, via synthetic
    `_community_vertex_id`s) lays the communities out with extent-aware separation
    so they can never overlap, and each community's own raw layout is scaled UP
    ONLY (never down, the same `_level2_finalize` guarantee) onto its own centroid.
    A project with zero or one bucket (below `_COMMUNITY_MIN_MEMBERS`, or a small
    project whose members never formed a real Leiden community) falls back to the
    plain flat pass unchanged: this recursion only ever engages where it is
    measured necessary."""
    buckets = _project_community_buckets(pid, members, communities)
    if len(buckets) <= 1:
        return _level2_flat_raw_layout(pid, members, link_rows, communities, membership)

    vertex_for = {key: _community_vertex_id(pid, key) for key in buckets}
    oid_community_vertex = {
        oid: vertex_for[key] for key, ms in buckets.items() for oid in ms}

    sub_raws = {
        key: _level2_flat_raw_layout(pid, ms, link_rows, communities, membership)
        for key, ms in buckets.items()
    }
    real_extents = {vertex_for[key]: _level2_extent(sub_raws[key]) for key in buckets}
    nominal_radii = {
        vertex_for[key]: _level1_radius(len(buckets[key])) for key in buckets}
    radii = {
        vid: max(nominal_radii[vid], real_extents[vid]) for vid in vertex_for.values()}

    cross_edges = _intra_project_community_edges(link_rows, oid_community_vertex)
    centroids = _level1_layout(
        list(vertex_for.values()), radii, cross_edges, gutter=_INTRA_PROJECT_GUTTER)

    out: dict[uuid.UUID, np.ndarray] = {}
    for key, vid in vertex_for.items():
        sub_raw = sub_raws[key]
        extent = real_extents[vid]
        scale = max(nominal_radii[vid] / extent, 1.0) if extent > 1e-6 else 1.0
        centroid = centroids[vid]
        for oid, p in sub_raw.items():
            out[oid] = p * scale + centroid
    return out


def _level2_extent(raw: dict[uuid.UUID, np.ndarray]) -> float:
    """The real radius `raw`'s own (already floor-respecting, per
    `_level2_raw_layout_for_project`) spread needs. PACKING GAP FIX, a follow-up
    to the compact-arrangement work: this used to be the 95th percentile member
    radius (the earlier layout version's own statistic, kept when `_level1_layout`
    still added FR-then-`_separate_extents` slack on top). Under the tight circle
    packing used now, `_level1_layout` reserves EXACTLY this radius plus a fixed
    gutter for each project, so the 5% of members the 95th percentile always
    excludes by construction were already landing outside their own project's
    packed disc even in the RAW layout, before any downstream nudge/declump ever
    touched them. Measured live: the top-10 centroid gap went negative (-184.5 to
    -812.7) on a map where the earlier, looser scheme always measured it
    positive. The TRUE max, not the 95th percentile, is what `_level1_layout`'s
    own packing radius must cover; this also tightens `_level2_finalize`'s own
    scale-up-only guarantee (never rescale a project's members past their own
    true spread). Floored at `_MIN_SEPARATION` so an empty or single-member
    project still gets a little real clearance in level 1's own separation
    pass."""
    if not raw:
        return _MIN_SEPARATION
    pts = np.array(list(raw.values()))
    if len(pts) == 1:
        return max(_MIN_SEPARATION, float(np.linalg.norm(pts[0])))
    return max(_MIN_SEPARATION, float(np.max(np.linalg.norm(pts, axis=1))))


def _level2_finalize(
    raw: dict[uuid.UUID, np.ndarray], nominal_radius: float, real_radius: float,
    centroid: np.ndarray,
) -> dict[uuid.UUID, np.ndarray]:
    """The other half of the rescale-compression fix: scale is
    `max(nominal_radius / real_radius, 1.0)`, NEVER below 1.0, so this step can
    only ever GROW the raw layout (filling more of its nominal
    `_level1_radius`-sized disc when there's room) or leave it exactly as
    `_level2_raw_layout_for_project`'s own local declump already made it (when
    the project's real spread already exceeds its nominal budget). It never
    compresses an already floor-respecting layout back below the floor. The
    caller feeds `real_radius`, not `nominal_radius`, into level 1's own
    `_separate_extents` pass whenever the project's real spread is the larger of
    the two, so a project that keeps its full raw size here never encroaches on
    its neighbours either: the same "measure the real thing, never trust the
    formula's guess" pattern `_hub_zone_radius` already uses for the hub
    zone."""
    if not raw:
        return {}
    scale = max(nominal_radius / real_radius, 1.0) if real_radius > 1e-6 else 1.0
    return {oid: p * scale + centroid for oid, p in raw.items()}


def _apply_bridge_nudges(
    positions: dict[uuid.UUID, np.ndarray], link_rows: list[asyncpg.Record],
    membership: dict[uuid.UUID, uuid.UUID], project_centroids: dict[uuid.UUID, np.ndarray],
) -> None:
    """Cross-project semantic edges: a weak nudge toward the OTHER project's own
    level-1 centroid so a bridging member settles nearer the facing edge of its
    own project's disc. Mutates `positions` in place, applied once after both
    levels place everything. Disclosed simplification: folding this into level
    2's own FR would mean reconciling two very different coordinate scales
    inside one force integration; a bounded post-hoc nudge (capped at
    `_CROSS_PROJECT_BRIDGE_NUDGE` of the remaining distance, well inside
    `_LEVEL1_GUTTER`'s own clearance) sidesteps that."""
    nudges: dict[uuid.UUID, np.ndarray] = defaultdict(lambda: np.zeros(2))
    counts: dict[uuid.UUID, int] = defaultdict(int)
    for r in link_rows:
        f, t, lt = r["from_id"], r["to_id"], r["type"]
        if lt in CONTAINER_LINK_TYPES or lt in STRUCTURAL_LINK_TYPES:
            continue
        pf, pt = membership.get(f), membership.get(t)
        if pf is None or pt is None or pf == pt:
            continue
        if f in positions and pt in project_centroids:
            nudges[f] = nudges[f] + (project_centroids[pt] - positions[f])
            counts[f] += 1
        if t in positions and pf in project_centroids:
            nudges[t] = nudges[t] + (project_centroids[pf] - positions[t])
            counts[t] += 1
    for oid, total in nudges.items():
        direction = total / counts[oid]
        positions[oid] = positions[oid] + direction * _CROSS_PROJECT_BRIDGE_NUDGE


def _place_unfiled(
    unfiled_ids: list[uuid.UUID], link_rows: list[asyncpg.Record],
    placed: dict[uuid.UUID, np.ndarray],
    project_centroids: dict[uuid.UUID, np.ndarray] | None = None,
    hub_ids: set[uuid.UUID] | None = None,
) -> dict[uuid.UUID, np.ndarray]:
    """Unfiled objects: one with a live link to an already-placed object lands
    at the mean position of those neighbours; one with none scatters as a
    proper isotropic 2D Gaussian (Box-Muller polar form) centred on the whole
    placed cloud's own centroid, std from that cloud's own spread. Density
    falls off smoothly outward instead of the fixed-radius ring a live
    measurement flagged as a spike on an earlier layout version.

    LONG EDGES: ANY live link counts as a neighbour now, structural and
    container included, NOT semantic only. A live probe traced a visible "beam"
    on the rendered map to roughly 1,960 objects (mostly Messages and Agents)
    whose ONLY links are structural (sent_by, acts_for, works_in...); the old
    semantic-only filter made every one of them fall through to fog, scattered
    far from the real neighbours those structural links actually name, drawing
    edges tens of thousands of units long. `hub_ids` (structural-degree over
    `_HUB_DEGREE_THRESHOLD`, the SAME "universal hub" reading
    `_build_physics_graph` already uses) is EXCLUDED from the neighbour set: a
    hub's own "mean position of neighbours" is meaningless, since almost
    everything structurally touches a hub, so counting it would just centroid
    every unfiled object back onto the hub's own position, the fixed-ring spike
    this scheme was built to avoid in the first place. Only a genuinely
    hub-less, link-less object is real fog.

    UNFILED-VS-PLACED FIX (live specimen): a naive neighbour-mean placement can
    drop an unfiled object right on top of an already-densely-packed project;
    one run's own pre-declump positions were 205 units apart, converging to
    0.058 apart post-declump. The single GLOBAL `_declump` pass at the very end
    of `_physics_positions` couldn't always finish that local cleanup within
    its own iteration budget once the target region was already near floor
    density from real project members. This runs its OWN local `_declump` pass
    here instead, with every already-placed position as a FIXED anchor: the
    SAME anchor-vs-movable mode `_declump` already supports (used unchanged, no
    new mechanism), just invoked at the point of insertion instead of leaving
    all the decluttering work to one pass over the whole population at the end.

    FOG-SCALE FIX (live specimen: a 163k-unit bbox, 0.32 same-project purity):
    the OLD `cloud_std` was the raw std of EVERY placed point's own
    coordinates. For a huge project (one alone r95 in the thousands after
    real-extent separation), that std balloons well past the LEVEL-1 canvas
    scale, so the fog's own Gaussian spread put real density inside project
    regions instead of only "between and around" them. `project_centroids`,
    when given, scales the fog off the spread of PROJECT CENTROIDS instead, the
    level-1 canvas's own scale, not any one project's internal member
    spread."""
    hubs = hub_ids or set()
    neighbours: dict[uuid.UUID, list[np.ndarray]] = defaultdict(list)
    unfiled_set = set(unfiled_ids)
    for r in link_rows:
        f, t = r["from_id"], r["to_id"]
        if f in unfiled_set and t in placed and t not in hubs:
            neighbours[f].append(placed[t])
        if t in unfiled_set and f in placed and f not in hubs:
            neighbours[t].append(placed[f])

    scale_source = project_centroids if project_centroids else placed
    if scale_source:
        scale_pos = np.array(list(scale_source.values()))
        cloud_std = max(float(scale_pos.std()), _UNFILED_FOG_MIN_STD)
    else:
        cloud_std = _UNFILED_FOG_MIN_STD
    if placed:
        cloud_center = np.array(list(placed.values())).mean(axis=0)
    else:
        cloud_center = np.zeros(2)

    out: dict[uuid.UUID, np.ndarray] = {}
    for oid in unfiled_ids:
        pts = neighbours.get(oid)
        if pts:
            # small deterministic jitter (long-edges fix): several unfiled
            # objects sharing the exact same single neighbour would otherwise
            # land on the exact same point. A real but tiny starting
            # coincidence for the anchor-declump below to resolve from, not a
            # spread meant to carry any visual meaning of its own.
            ju1 = max(_hash01(f"unfiled-jitter-r1:{oid}"), 1e-9)
            ju2 = _hash01(f"unfiled-jitter-r2:{oid}")
            jangle = ju2 * 2 * math.pi
            jitter = ju1 * _MIN_SEPARATION * np.array(
                [math.cos(jangle), math.sin(jangle)])
            out[oid] = np.array(pts).mean(axis=0) + jitter
        else:
            u1 = max(_hash01(f"fog-r1:{oid}"), 1e-9)
            u2 = _hash01(f"fog-r2:{oid}")
            radius = math.sqrt(-2.0 * math.log(u1)) * cloud_std
            angle = u2 * 2 * math.pi
            out[oid] = cloud_center + np.array(
                [radius * math.cos(angle), radius * math.sin(angle)])

    if out and placed:
        ids_order = list(out.keys())
        pos = np.array([out[oid] for oid in ids_order])
        anchor_pos = np.array(list(placed.values()))
        pos = _declump(
            pos, anchor_pos, ids_order, min_sep=_MIN_SEPARATION,
            iterations=_PHYSICS_DECLUMP_ITERATIONS)
        out = {oid: pos[i] for i, oid in enumerate(ids_order)}
    return out


class TopologyAcceptanceFailed(Exception):
    """Raised by `_physics_positions` when the top-10 centroid gap comes back
    negative: a follow-up to the compact-arrangement work. Two of the biggest
    districts' own member clouds genuinely overlap on the packed map, not a
    residual `_declump` can clean up (that pass only ever enforces the flat
    per-object floor, blind to which project a pair belongs to). A hard
    refusal, never a silent write of a visibly overlapping map, the same "loud,
    not shipped" discipline `DeclumpVerificationFailed` already holds for the
    flat floor, one level up."""


class DeclumpVerificationFailed(Exception):
    """Raised by `_verify_min_separation` when the post-declump population still
    holds a pair closer than `_PHYSICS_VERIFY_FAIL_RATIO * min_sep`: a genuine
    collapse, not the ordinary convergence residual a narrower fixed-epsilon
    check used to flag (one measured attempt's worst pair, 13.566 of a 15
    floor, read as "invisible" at the time). `_declump`'s own per-call
    iteration cap is silent about non-convergence (it just stops), which is
    exactly how an earlier layout version's nn p50 of 2.8 went unnoticed until
    it was measured live and the requirement became explicit: verify it, and
    fail loudly if not. The hierarchical layout is designed so declump only
    ever does bounded local cleanup at reasonable density and should
    comfortably converge well inside the proportional line; this is the
    tripwire for "it genuinely didn't"."""


def _worst_pair_distance(pos: np.ndarray, min_sep: float) -> float:
    """The smallest pairwise distance anywhere in `pos`, scanned the same
    grid-cell-neighbourhood way `_declump` itself does (never O(n^2)). `min_sep`
    itself is returned when nothing violates it (nothing to report), and a cell
    too dense to check cheaply (`_VERIFY_MAX_CANDIDATES`) reports `0.0`, the
    worst possible value, rather than skip it silently. Shared by
    `_declump_until_converged_or_budget` (proportional verification) and
    `_verify_min_separation`: the SAME measurement drives both "keep iterating"
    and "is this bad enough to refuse the whole migration"."""
    if len(pos) < 2:
        return min_sep
    worst = min_sep
    cells = _grid_cells(pos, min_sep)
    for (cx, cy), _idxs in cells.items():
        candidates = _neighbor_cell_indices(cells, cx, cy)
        if len(candidates) < 2:
            continue
        if len(candidates) > _VERIFY_MAX_CANDIDATES:
            return 0.0
        pts = pos[candidates]
        diffs = pts[:, None, :] - pts[None, :, :]
        dists = np.sqrt((diffs ** 2).sum(axis=-1))
        np.fill_diagonal(dists, np.inf)
        worst = min(worst, float(dists.min()))
    return worst


def _declump_until_converged_or_budget(
    pos: np.ndarray, ids: list[uuid.UUID], *, min_sep: float, budget_secs: float,
    chunk_iterations: int = _PHYSICS_DECLUMP_CHUNK,
) -> tuple[np.ndarray, float, int]:
    """PROPORTIONAL VERIFICATION, a follow-up to an earlier verify-only ruling:
    the OLD scheme ran a single FIXED iteration count and either passed or
    refused outright. Live measurement (one attempt's worst pair, 13.566 of a
    15 floor: a convergence residual, not a collapse, and invisible under the
    old check) showed that a narrow residual miss isn't the same failure as an
    actual structural collapse (other specimens were all under 1 unit apart),
    so treating them identically either refuses good-enough layouts or, under
    the old fixed-count scheme, has no principled stopping point short of a
    hard budget. This runs `_declump` in small chunks, re-measuring the worst
    pairwise distance (`_worst_pair_distance`) after each, until either the
    deficit from `min_sep` clears `_PHYSICS_DECLUMP_CONVERGED_RATIO`
    (comfortably converged) or `budget_secs` wall-clock is spent, whichever
    comes first. Returns the positions, the worst residual distance actually
    reached, and how many iterations ran, so the caller's own result can
    report both regardless of which one stopped it."""
    start = time.monotonic()
    total_iters = 0
    worst = _worst_pair_distance(pos, min_sep)
    converged_floor = min_sep * (1.0 - _PHYSICS_DECLUMP_CONVERGED_RATIO)
    while worst < converged_floor and time.monotonic() - start < budget_secs:
        pos = _declump(
            pos, np.zeros((0, 2)), ids, min_sep=min_sep, iterations=chunk_iterations)
        total_iters += chunk_iterations
        worst = _worst_pair_distance(pos, min_sep)
    return pos, worst, total_iters


def _verify_min_separation(
    worst_pair_distance: float, *, min_sep: float = _MIN_SEPARATION,
) -> None:
    """PROPORTIONAL VERIFICATION: fails only when the worst pair found is under
    `_PHYSICS_VERIFY_FAIL_RATIO * min_sep`. A residual ABOVE that line (e.g.
    13.6 of 15, the "invisible" specimen described above) is tolerated as a
    normal convergence residual, never refused; genuinely collapsed pairs (all
    under 1 unit) still fail loudly well before this line. Takes the
    ALREADY-MEASURED worst distance (from
    `_declump_until_converged_or_budget`'s own return) rather than re-scanning:
    one measurement, two decisions (keep iterating vs. refuse), never computed
    twice."""
    fail_floor = min_sep * _PHYSICS_VERIFY_FAIL_RATIO
    if worst_pair_distance < fail_floor:
        raise DeclumpVerificationFailed(
            f"post-declump verification found a pair {worst_pair_distance:.3f} units "
            f"apart, under {_PHYSICS_VERIFY_FAIL_RATIO} * the {min_sep} floor "
            f"({fail_floor:.3f}) -- a genuine collapse, not a convergence residual")


def _hub_zone_radius(n_hubs: int) -> float:
    """The real max radius `_place_hubs_in_zone`'s own raw (never rescaled)
    sunflower spread needs for `n_hubs` points at `_MIN_SEPARATION`. Unlike
    `_level1_radius`'s area-based packing formula (asymptotically accurate for a
    real project's hundreds-to-thousands of members), a small hub count needs the
    EXACT sunflower extent: the packing-density assumption underestimates badly at
    this scale, and `_place_hubs_in_zone` never rescales DOWN to fit a smaller
    budget (that would recreate the very bug this zone exists to fix), so the
    radius fed into `_separate_extents` has to already match what the raw
    placement actually needs, not the other way around."""
    if n_hubs <= 1:
        return _MIN_SEPARATION
    return _MIN_SEPARATION * math.sqrt(n_hubs - 1 + 0.5)


def _place_hubs_in_zone(
    hub_order: list[uuid.UUID], center: np.ndarray,
) -> dict[uuid.UUID, np.ndarray]:
    """HUB ZONE (live fix; see `_HUB_ZONE_ID`'s own docstring): hubs spread by
    raw `_sunflower_point` (a real minimum-pairwise-spacing guarantee among
    themselves, `hub_order`'s own stable creation-order rank), translated onto
    `center`, NEVER rescaled. Rescaling down to fit a smaller radius budget (the
    way `_level2_layout_for_project` rescales a project's own members) would
    shrink hub-to-hub spacing back below the floor this zone exists to
    guarantee; `_hub_zone_radius` sizes level 1's own separation budget to match
    this placement's real extent instead. Replaces the old "jitter by 1 unit
    around the raw centroid-of-everything" scheme, which had no radius budget of
    its own and could land inside whatever real content happened to sit at that
    centroid."""
    if not hub_order:
        return {}
    raw = np.array([_sunflower_point(i, _MIN_SEPARATION) for i in range(len(hub_order))])
    return {oid: raw[i] + center for i, oid in enumerate(hub_order)}


async def _physics_positions(
    actions: Actions, *, diagnostics: dict[str, Any] | None = None,
) -> dict[uuid.UUID, tuple[float, float]]:
    """The hierarchical physics pipeline: pure enough to unit-test without a
    live migration write past the DB reads at the top. Population + links ->
    communities -> level 2's own RAW per-project FR (already floor-respecting
    via a local declump) -> level 1 (project-contracted FR + extent-aware
    separation, using each project's REAL extent) -> level 2 finalize (scale UP
    only, translated onto its level-1 disc) -> cross-project bridge nudges ->
    unfiled placement -> hub re-centering -> the memory guard -> the final
    declump, now CONVERGE-OR-BUDGET (`_declump_until_converged_or_budget`) ->
    PROPORTIONALLY VERIFIED (`_verify_min_separation`, fails only on a genuine
    collapse, not a narrow residual). Real object positions only. Can raise
    `MemoryBudgetExceeded`, `DeclumpVerificationFailed`, or (a
    compact-arrangement follow-up) `TopologyAcceptanceFailed`; the caller
    (`run_physics_migrate`) turns any of them into a written refusal result
    rather than letting a bad layout write. `diagnostics`, when given, is
    filled with `declump_worst_residual`/`declump_iterations` regardless of
    outcome, so the result reports the worst residual pair and iteration count
    either way."""
    object_ids = await _active_object_ids(actions)
    if not object_ids:
        return {}
    link_rows = await _live_link_rows(actions)
    membership = await _project_membership(actions)
    active = set(object_ids)
    communities = _detect_communities(link_rows, membership, active)

    groups: dict[uuid.UUID, list[uuid.UUID]] = defaultdict(list)
    member_set: set[uuid.UUID] = set()
    for oid in object_ids:
        pid = membership.get(oid)
        if pid is not None and pid in active:
            groups[pid].append(oid)
            member_set.add(oid)
    project_ids = sorted(groups.keys(), key=str)
    project_id_set = set(project_ids)
    unfiled_ids = [oid for oid in object_ids if oid not in member_set and oid not in project_id_set]

    hub_ids = await _hub_ids(actions, object_ids)
    hub_order = [oid for oid in object_ids if oid in hub_ids]

    if diagnostics is not None:
        # LONG EDGES acceptance line: a MEMBERSHIP container (any TARGET of a
        # live in_repo/works_in/holds/member_of link) must never be a hub-zone
        # candidate, so this must always measure 0. Matches `_hub_ids`'s own
        # exclusion set exactly, NOT the wider `CONTAINER_LINK_TYPES`: a
        # legitimate acts_for/spawned_by-target hub (a Person, a coordinator)
        # is supposed to still be here, so counting it as a false "relocation"
        # would measure a claim this check never made.
        container_targets = {
            r["to_id"] for r in link_rows
            if r["type"] in _MEMBERSHIP_CONTAINER_LINK_TYPES}
        diagnostics["layout_container_vertices_relocated"] = len(
            container_targets & set(hub_order))

    # RESCALE COMPRESSION FIX: raw level-2 layouts (already floor-respecting,
    # per `_level2_raw_layout_for_project`'s own local declump) computed BEFORE
    # level 1, so level 1's own separation pass can use each project's REAL extent
    # instead of `_level1_radius`'s nominal area-based guess. The same
    # measure-the-real-thing pattern `_hub_zone_radius` already uses for the hub
    # zone, now generalised to every project.
    radii_nominal = {pid: _level1_radius(len(groups[pid])) for pid in project_ids}
    raw_layouts = {
        pid: _level2_raw_layout_for_project(pid, groups[pid], link_rows, communities, membership)
        for pid in project_ids
    }
    real_extents = {pid: _level2_extent(raw_layouts[pid]) for pid in project_ids}
    radii = {pid: max(radii_nominal[pid], real_extents[pid]) for pid in project_ids}

    level1_ids = list(project_ids)
    if hub_order:
        radii[_HUB_ZONE_ID] = _hub_zone_radius(len(hub_order))
        level1_ids.append(_HUB_ZONE_ID)
    cross_edges = _cross_project_edges(link_rows, membership, project_id_set)
    # ANCHOR's own source (compact-arrangement work): each project's own
    # PREVIOUS run position, read before this run writes anything. A project
    # object IS a real vertex (`positions[pid] = centroids[pid]` below persists
    # its own graph_x/graph_y each run), so its current row already holds
    # exactly the "previous run's own centroid" ANCHOR needs. A first-ever run
    # (or a genuinely new project) simply has no row here and rides along
    # unanchored, same as `_level1_layout`'s own docstring describes.
    prev_project_positions_raw = await positions_for(actions, project_ids)
    anchor = {pid: np.array(xy) for pid, xy in prev_project_positions_raw.items()}
    centroids = _level1_layout(level1_ids, radii, cross_edges, anchor=anchor)

    # LONG EDGES: the osiris repo's own long-edge "beam" was NEVER a level-1
    # spring-weight problem. Live probe traced it to unfiled fog (below), so
    # level-1 weight stays semantic-only, unchanged. Only the result's own
    # top-5-linked-pair reporting survives from that investigation, kept for
    # visibility.
    if diagnostics is not None and cross_edges:
        top5 = sorted(cross_edges.items(), key=lambda kv: -kv[1])[:5]
        pair_stats = []
        for (a, b), link_count in top5:
            dist = float(np.linalg.norm(centroids[a] - centroids[b]))
            radii_sum = radii.get(a, _MIN_SEPARATION) + radii.get(b, _MIN_SEPARATION)
            pair_stats.append({
                "pair": (str(a), str(b)), "link_count": link_count,
                "centroid_distance": dist, "radii_sum": radii_sum,
                "within_2x_radii_sum": dist <= 2 * radii_sum,
            })
        diagnostics["layout_top5_linked_pairs"] = pair_stats

    positions: dict[uuid.UUID, np.ndarray] = {}
    for pid in project_ids:
        positions[pid] = centroids[pid]
        positions.update(_level2_finalize(
            raw_layouts[pid], radii_nominal[pid], real_extents[pid], centroids[pid]))

    _apply_bridge_nudges(positions, link_rows, membership, centroids)
    # POST-NUDGE DECLUMP (live specimen): a bridging member gets nudged
    # INDEPENDENTLY of its own non-bridging project-mates. Each project's own
    # raw layout was already floor-respecting (`_level2_raw_layout_for_project`'s
    # own local declump), and `_level2_finalize`'s scale only ever grows, but a
    # nudge applied AFTER that can still land a bridging member too close to a
    # sibling who never moved. Re-settling HERE, while the population is still
    # just projects+members (no unfiled, no hubs yet), catches that disruption
    # close to where it was introduced instead of leaving it all to the one
    # final global pass, which other live specimens showed doesn't always
    # finish every local pocket it's handed within its own iteration budget.
    if len(positions) > 1:
        ids_order = list(positions.keys())
        pos_arr = np.array([positions[oid] for oid in ids_order])
        pos_arr = _declump(
            pos_arr, np.zeros((0, 2)), ids_order, min_sep=_MIN_SEPARATION,
            iterations=_PHYSICS_DECLUMP_ITERATIONS)
        positions = {oid: pos_arr[i] for i, oid in enumerate(ids_order)}
    project_centroids_only = {pid: centroids[pid] for pid in project_ids}
    positions.update(_place_unfiled(
        unfiled_ids, link_rows, positions, project_centroids_only, hub_ids))

    if hub_order:
        positions.update(_place_hubs_in_zone(hub_order, centroids[_HUB_ZONE_ID]))

    real_positions = np.array([positions[oid] for oid in object_ids])
    reason = await _memory_guard(actions, real_positions)
    if reason:
        raise MemoryBudgetExceeded(reason)

    from src.orchestrator.settings_service import current_stored_value

    stored_budget = await current_stored_value(
        actions.pool, "layout.physics_declump_budget_secs")
    budget_secs = (float(stored_budget) if isinstance(stored_budget, int | float)
                   else _DEFAULT_PHYSICS_DECLUMP_BUDGET_SECS)
    declumped, worst_residual, declump_iters = _declump_until_converged_or_budget(
        real_positions, object_ids, min_sep=_MIN_SEPARATION, budget_secs=budget_secs)
    if diagnostics is not None:
        diagnostics["declump_worst_residual"] = worst_residual
        diagnostics["declump_iterations"] = declump_iters
        diagnostics["community_count"] = len({(pid, c) for pid, c in communities.values()})
        diagnostics["community_seed_scheme"] = "leiden seeded per-project from pid.int"
        final_positions = {oid: declumped[i] for i, oid in enumerate(object_ids)}
        diagnostics.update(_long_edge_counts(link_rows, final_positions))
        diagnostics.update(_layout_acceptance_metrics(
            declumped, object_ids, membership, groups, project_ids, radii,
            packed_centroids=centroids))
        diagnostics.update(_edge_length_percentiles(link_rows, membership, final_positions))
        osiris_pid = await actions.pool.fetchval(
            "SELECT id FROM objects WHERE canonical='repo:osiris' "
            "AND type='SoftwareProject' AND status='active'")
        diagnostics.update(_compactness_metrics(
            declumped, project_ids, radii, osiris_pid, object_ids, groups))
        diagnostics.update(_anchor_displacement(anchor, final_positions))
        gap = diagnostics.get("layout_min_top10_centroid_gap")
        if gap is not None and gap < 0:
            raise TopologyAcceptanceFailed(
                f"top-10 centroid gap is {gap:.1f} (negative) -- two of the "
                "biggest districts' own member clouds overlap on the packed map")
    _verify_min_separation(worst_residual, min_sep=_MIN_SEPARATION)

    return {oid: (float(declumped[i, 0]), float(declumped[i, 1]))
            for i, oid in enumerate(object_ids)}


_LONG_EDGE_THRESHOLD = 20_000.0  # LONG EDGES acceptance line: world-unit length
                                 # past which an edge reads as a visible "beam"
                                 # across the rendered map. The live probe that
                                 # found the unfiled-fog root cause used this exact
                                 # figure (47,866 of 136,089 edges longer than it).


def _edge_length_percentiles(
    link_rows: list[asyncpg.Record], membership: dict[uuid.UUID, uuid.UUID],
    positions: dict[uuid.UUID, np.ndarray],
) -> dict[str, Any]:
    """Compact-arrangement acceptance line: intra- vs cross-district edge
    length, median and 95th percentile; this verification lacked both until
    now. Semantic edges only (container/structural edges are gravity, never
    drawn as lines); an edge with either endpoint unplaced (should not happen
    for a real link row, defensive only) is skipped rather than crashing the
    result."""
    intra: list[float] = []
    cross: list[float] = []
    for r in link_rows:
        f, t, lt = r["from_id"], r["to_id"], r["type"]
        if lt in CONTAINER_LINK_TYPES or lt in STRUCTURAL_LINK_TYPES:
            continue
        pf, pt = positions.get(f), positions.get(t)
        if pf is None or pt is None:
            continue
        length = float(np.linalg.norm(pf - pt))
        mf, mt = membership.get(f), membership.get(t)
        (intra if mf is not None and mf == mt else cross).append(length)

    def _pct(xs: list[float]) -> dict[str, float | int | None]:
        if not xs:
            return {"p50": None, "p95": None, "n": 0}
        arr = np.array(xs)
        return {"p50": float(np.percentile(arr, 50)), "p95": float(np.percentile(arr, 95)),
                "n": len(xs)}

    return {
        "layout_intra_district_edge_length": _pct(intra),
        "layout_cross_district_edge_length": _pct(cross),
    }


def _compactness_metrics(
    pos: np.ndarray, project_ids: list[uuid.UUID], radii: dict[uuid.UUID, float],
    osiris_pid: uuid.UUID | None, object_ids: list[uuid.UUID],
    groups: dict[uuid.UUID, list[uuid.UUID]],
) -> dict[str, Any]:
    """Compact-arrangement's own PRIMARY acceptance number: bbox area /
    sum(pi * r^2) over every district's own packing radius. 1.0 is unreachable
    (circles can't tile a plane gaplessly); worked examples put a realistic
    packed target a few times that, against the old FR-then-separate scheme's
    own tens. The osiris project's own members' bbox is reported SEPARATELY,
    since osiris is asserted to span "the whole map" on the live header: its
    own footprint, not the global one, is what a compaction fix should
    actually move."""
    bbox_min, bbox_max = pos.min(axis=0), pos.max(axis=0)
    width = bbox_max - bbox_min
    bbox_area = float(width[0] * width[1])
    district_area = sum(math.pi * radii.get(pid, _MIN_SEPARATION) ** 2 for pid in project_ids)
    out: dict[str, Any] = {
        "layout_compactness_ratio": bbox_area / district_area if district_area > 1e-9 else None,
        "layout_bbox_area": bbox_area,
        "layout_district_area_sum": district_area,
        "layout_osiris_bbox_width": None,
    }
    if osiris_pid is not None and osiris_pid in groups:
        idx = {oid: i for i, oid in enumerate(object_ids)}
        member_idx = [idx[m] for m in groups[osiris_pid] if m in idx]
        if member_idx:
            opts = pos[member_idx]
            owidth = opts.max(axis=0) - opts.min(axis=0)
            out["layout_osiris_bbox_width"] = owidth.tolist()
    return out


def _anchor_displacement(
    anchor: dict[uuid.UUID, np.ndarray], final_positions: dict[uuid.UUID, np.ndarray],
) -> dict[str, Any]:
    """STABILITY: how far each anchored project actually moved between the
    previous run's own written position and this run's final (post-pack,
    post-declump) one. Mean and max, over whichever projects `anchor` names
    (an empty/absent anchor, e.g. a first-ever run, reports n=0 rather than a
    fabricated zero)."""
    deltas = [
        float(np.linalg.norm(final_positions[pid] - old))
        for pid, old in anchor.items() if pid in final_positions
    ]
    if not deltas:
        return {"layout_anchor_displacement_mean": None,
                "layout_anchor_displacement_max": None, "layout_anchor_displacement_n": 0}
    return {
        "layout_anchor_displacement_mean": float(np.mean(deltas)),
        "layout_anchor_displacement_max": float(np.max(deltas)),
        "layout_anchor_displacement_n": len(deltas),
    }


def _long_edge_counts(
    link_rows: list[asyncpg.Record], positions: dict[uuid.UUID, np.ndarray],
) -> dict[str, Any]:
    """Long-edges own result requirement: every live link whose two endpoints
    both have a final position and end up farther apart than
    `_LONG_EDGE_THRESHOLD`, counted by type: top 8 types plus the grand total
    (acceptance: total under 5,000). Counts every real link type, not just
    semantic ones, exactly the population `_place_unfiled`'s own fix widened to
    cover, so this result field is the direct measurement of whether that fix
    actually worked, not a proxy."""
    counts: dict[str, int] = defaultdict(int)
    total = 0
    for r in link_rows:
        f, t, lt = r["from_id"], r["to_id"], r["type"]
        pf, pt = positions.get(f), positions.get(t)
        if pf is None or pt is None:
            continue
        if float(np.linalg.norm(pf - pt)) > _LONG_EDGE_THRESHOLD:
            counts[lt] += 1
            total += 1
    top8 = sorted(counts.items(), key=lambda kv: -kv[1])[:8]
    return {
        "layout_long_edge_total": total,
        "layout_long_edge_by_type_top8": [{"type": t, "count": c} for t, c in top8],
    }


def _layout_acceptance_metrics(
    pos: np.ndarray, object_ids: list[uuid.UUID], membership: dict[uuid.UUID, uuid.UUID],
    groups: dict[uuid.UUID, list[uuid.UUID]], project_ids: list[uuid.UUID],
    radii: dict[uuid.UUID, float],
    packed_centroids: dict[uuid.UUID, np.ndarray] | None = None,
) -> dict[str, Any]:
    """ACCEPTANCE METRICS, required in every verify-only result from now on:
    per-project centroid/r50/r95/N for the top 10 projects, the minimum
    pairwise centroid gap among them against their own R_a+R_b, 5-NN
    same-project purity for the biggest project, and the global bbox. Computed
    from the SAME final positions the migration would write (or refuse): the
    exact numbers a live measurement caught an earlier layout version's first
    real write failing on (163k-unit bbox, every centroid near-coincident, 0.32
    purity).

    GAP-CHECK CENTROID FIX, a compact-arrangement follow-up: `project_stats`'
    own reported `centroid`/`r50`/`r95` still recompute from final MEMBER
    positions (a genuinely useful diagnostic: how the actual population sits).
    But the min-gap OVERLAP check now compares `packed_centroids`, the current
    layout version's own exact, stable `_level1_layout` output, not that
    recomputed mean. `radii` is that same centroid's own reserved packing
    radius (`_pack_siblings`' own guarantee: any two packed centroids are at
    least `radii[a]+radii[b]+gutter` apart, by construction); a bridge nudge or
    the post-nudge/final declump can shift a handful of MEMBERS toward a
    neighbour without moving the project's own packed anchor at all, and it is
    the anchor, what a district fill actually renders around, whose separation
    this check exists to guarantee. Falls back to the recomputed mean when
    `packed_centroids` is omitted (older callers, and this function's own
    direct unit tests)."""
    idx = {oid: i for i, oid in enumerate(object_ids)}
    top = sorted(project_ids, key=lambda p: -len(groups[p]))[:10]

    project_stats = []
    top_centroids: dict[uuid.UUID, np.ndarray] = {}
    for pid in top:
        member_idx = [idx[m] for m in groups[pid] if m in idx]
        if not member_idx:
            continue
        pts = pos[member_idx]
        centroid = pts.mean(axis=0)
        top_centroids[pid] = (
            packed_centroids[pid] if packed_centroids and pid in packed_centroids
            else centroid)
        dists = np.linalg.norm(pts - centroid, axis=1)
        project_stats.append({
            "project": str(pid), "n": len(member_idx),
            "centroid": centroid.tolist(),
            "r50": float(np.percentile(dists, 50)),
            "r95": float(np.percentile(dists, 95)),
        })

    min_gap = None
    top_list = list(top_centroids.keys())
    for i in range(len(top_list)):
        for j in range(i + 1, len(top_list)):
            a, b = top_list[i], top_list[j]
            dist = float(np.linalg.norm(top_centroids[a] - top_centroids[b]))
            required = radii.get(a, _MIN_SEPARATION) + radii.get(b, _MIN_SEPARATION)
            gap = dist - required
            if min_gap is None or gap < min_gap:
                min_gap = gap

    purity = None
    if top:
        biggest = top[0]
        member_idx = [idx[m] for m in groups[biggest] if m in idx]
        if member_idx:
            rng = np.random.default_rng(42)
            sample_idx = [member_idx[i] for i in rng.choice(
                len(member_idx), size=min(300, len(member_idx)), replace=False)]
            same_project = np.array([membership.get(oid) == biggest for oid in object_ids])
            purities = []
            for i in sample_idx:
                d = np.linalg.norm(pos - pos[i], axis=1)
                nn_idx = np.argsort(d)[1:6]  # skip self (distance 0)
                purities.append(float(same_project[nn_idx].mean()))
            purity = float(np.mean(purities))

    bbox_min, bbox_max = pos.min(axis=0), pos.max(axis=0)
    return {
        "layout_project_stats": project_stats,
        "layout_min_top10_centroid_gap": min_gap,
        "layout_biggest_project_5nn_purity": purity,
        "layout_bbox_min": bbox_min.tolist(),
        "layout_bbox_max": bbox_max.tolist(),
        "layout_bbox_width": (bbox_max - bbox_min).tolist(),
    }


async def _memory_guard(actions: Actions, positions: np.ndarray) -> str | None:
    """COLLAPSED-CONTAINER FIX: checked against POST-FR positions, not the seed.
    A live specimen showed the seed (always sparse by the sunflower's own
    construction) passing clean while FR's own springs/gravity later collapsed
    thousands of container-only siblings onto one point, the exact case this
    guard exists to catch. Still a defensive check against the ONE shape that
    could still cost O(k^2) memory after graph_layout._declump's own grid
    rewrite (its real cost is O(n) for any reasonably spread population): the
    largest SINGLE grid cell's own point count `k`. Returns a written refusal
    reason, or None when safe."""
    from src.orchestrator.settings_service import current_stored_value

    cells = _grid_cells(positions, _MIN_SEPARATION)
    worst_k = max((len(v) for v in cells.values()), default=0)
    stored = await current_stored_value(actions.pool, "layout.physics_max_bytes")
    max_bytes = int(stored) if isinstance(stored, int | float) else _DEFAULT_PHYSICS_MAX_BYTES
    needed = worst_k * worst_k * 16
    if needed > max_bytes:
        return (f"refusing: the worst single post-FR grid cell holds {worst_k} points -- "
                f"a quadratic fallback there would need ~{needed} bytes, over "
                f"layout.physics_max_bytes={max_bytes}")
    return None


async def run_physics_migrate(
    actions: Actions, *, verify_only: bool = False,
) -> AsyncIterator[dict[str, Any]]:
    """The physics layout's own migration entry point: a SINGLE global
    computation over the whole active population (never a batch loop; see the
    module docstring for why), sharing `graph_layout._LAYOUT_LOCK_KEY` with the
    cron heartbeat and `run_layout_migrate` so nothing else touches
    graph_x/graph_y while this runs. Yields coarse stage results (not one per
    batch, since there are none) and a final
    `{"done": True, "placed": N, "peak_rss_kb": N}`. `_physics_positions` can
    raise `MemoryBudgetExceeded` (the collapsed-container fix),
    `DeclumpVerificationFailed`, or `TopologyAcceptanceFailed` (a
    compact-arrangement follow-up), any turned here into a single
    `{"error": ...}` result with no write. `peak_rss_kb` rides on EVERY result
    shape now, not just a real write's own: `--verify-only`, the standard
    diagnostic tool for this pipeline, used to report nothing about memory at
    all despite `_memory_guard` already computing it internally.

    `verify_only=True` (added after a live migration attempt needed a full
    deploy-probe-diagnose cycle just to see whether a fix actually worked):
    computes and verifies everything (population, hierarchical layout, declump,
    the min-sep verification) WITHOUT ever reaching the write step below
    (structurally, not by a flag check inside the write path: the `return` two
    lines above the write loop is what actually guarantees it). A verify-only
    run is read-only by construction, so unlike a real migration it MAY run
    from an undeployed branch against the live population; only a run that
    actually writes still needs deployed code (the migration-entry-point
    rule)."""
    async with actions.pool.acquire() as lock_conn:
        if not await _try_acquire_layout_lock(lock_conn):
            yield {"error": "the layout heartbeat (or a migrate run) currently holds "
                            "the layout lock -- try again shortly"}
            return
        try:
            yield {"stage": "computing"}
            diagnostics: dict[str, Any] = {}
            try:
                positions = await _physics_positions(actions, diagnostics=diagnostics)
            except (MemoryBudgetExceeded, DeclumpVerificationFailed,
                    TopologyAcceptanceFailed) as exc:
                peak_rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                yield {"error": str(exc), "peak_rss_kb": peak_rss_kb, **diagnostics}
                return
            # RECEIPT-STATS follow-up to the bbox compactness work:
            # `peak_rss_kb` used to appear only on a real write's own result,
            # measured AFTER the write loop below, so a `--verify-only` run
            # (the standard diagnostic tool for this pipeline) never reported
            # it at all, despite `_physics_positions`'s own `_memory_guard`
            # already computing memory internally. Measuring right after
            # `_physics_positions` returns/raises covers all three result
            # shapes (error, verify-only, done) from ONE call.
            peak_rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            if verify_only:
                yield {"done": True, "verify_only": True, "placed": len(positions),
                       "peak_rss_kb": peak_rss_kb, **diagnostics}
                return
            yield {"stage": "writing", "count": len(positions)}
            now = datetime.now(UTC)
            ids = list(positions.keys())
            chunk = 5000
            for start in range(0, len(ids), chunk):
                batch_ids = ids[start:start + chunk]
                await _bulk_assert_positions(
                    actions, {oid: positions[oid] for oid in batch_ids}, now)
            peak_rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            yield {"done": True, "placed": len(positions), "peak_rss_kb": peak_rss_kb,
                   **diagnostics}
        finally:
            await _release_layout_lock(lock_conn)
