"""THE GRAPH VISUALIZER (wave B item 1, thread 8839) -- extended for NAVIGABLE SPACE, THE
SERVER, piece A (rulings f832c3a4 + 0a3d6719, operator 2026-09-14, thread b6cb1d7c0b36):
server-side placement over the WHOLE graph, run incrementally by the heartbeat -- positions
stored as current_assertions (graph_x/graph_y), never computed live in the browser or the
renderer. This module feeds the /graph endpoints (supernodes/clusters/viewport) and the
whole-graph typed-array stream (graph_stream.py).

DECLUMP FIX (Thoth mail 10582, PRIORITY -- the operator's own screenshot of the deployed
space showed stacked nodes, collapsed rings, thick edge bundles instead of a spread cloud):
the FIRST version of this placement rule put every object of one type in one project on a
SINGLE fixed-radius circle at a random hash angle -- fine for a handful of objects, but this
house's own real population has (project, type) groups running into the THOUSANDS (Agent in
the unfiled bucket: 18,978; Commit in the osiris project itself: 6,273) and a fixed
circumference simply cannot hold that many points apart. Project centers had the same
disease one level up: a hash into a wide flat spiral-index range gives no guaranteed
MINIMUM separation between two projects, so the extent ends up both sparse in places and
badly clumped in others (measured at ~260,000 units wide for ~49k objects before this fix).

ONE MECHANISM, applied at both levels, replaces the old fixed-circle-plus-hash-angle rule:
a SUNFLOWER/FERMAT SPIRAL keyed on a STABLE RANK (never a hash) -- radius grows with
sqrt(rank), so N points pack into a radius proportional to sqrt(N) with a guaranteed
minimum pairwise spacing, and the rank itself is permanent once assigned (creation-order
via `ROW_NUMBER() OVER (... ORDER BY created_at, id)`, computed by the DATABASE, not
derivable from an object's own id alone) -- a later-created sibling only ever takes a
HIGHER, previously-unused rank, so an existing object's or project's position never moves
once placed, the same incrementality guarantee the id-hash version had, just resolved
against an immutable ORDER instead of an immutable VALUE.
  - PROJECT CENTERS: every active SoftwareProject's rank by creation order; the `unfiled`
    sentinel is pinned to rank 0 so it can never collide with a real project's index; real
    projects start at 1. Spacing sized (measured live before picking the constant) to
    comfortably contain even the worst real project's own extent (osiris itself, ~11.7k
    objects) without two projects' discs ever touching.
  - OBJECTS WITHIN A (project, type) GROUP: same sunflower, keyed on the object's own rank
    within that exact group, offset outward from the type's existing base radius (still
    schema.py's declared type order, unchanged) -- so small groups still read as a tight
    ring near that base radius, and only a group large enough to need it spirals outward
    past it (Thoth's own "beyond one ring's capacity" framing, expressed here as a single
    formula rather than a two-tier ring-then-disc special case).

A bounded intra-project relax pass still nudges each tick's batch toward already-placed
same-project neighbors (unchanged rule: cross-project edges never attract) -- but now ends
with a HARD MINIMUM-SEPARATION PASS: the Fruchterman-Reingold repulsion term APPROACHES a
floor over enough iterations but never GUARANTEES one within the few iterations this
heartbeat actually runs, so strong attraction could still leave two connected nodes
uncomfortably close (or, at the limit, exactly coincident) -- this pass is a direct,
deterministic correction, not another force-simulation step, so the guarantee holds
regardless of how attraction behaved before it ran.

INCREMENTAL, NEVER REVISITED: unchanged mechanism -- a tick only ever considers objects
still missing the CURRENT `graph_layout_v` marker, so a re-run over already-placed objects
moves nothing.

WRITE PATH: unchanged -- graph_x/graph_y/graph_layout_v land as ordinary property
assertions via one multi-row UPDATE+INSERT per property per tick, safe only because
GRAPH_LAYOUT_SOURCE is this triple's sole writer.

THE READING LAYER (Thoth mail 10595, ruling c5953bb1 -- the operator's own second
screenshot: clusters far apart, huge cross-cluster bundles). Live measurement: repo:osiris
degree 20,352, principal:analyst:operator degree 18,472, dev:asuramaya 9,462 -- membership
edges (in_repo, acts_for, works_in, spawned_by, authored_by...) draw a spoke from nearly
every object to one of a handful of shared hubs, and the old intra-project relax pulled on
EVERY same-project edge including those, turning each spoke into a literal spring dragging
distant objects toward the hub. Two changes fix this:
  - STRUCTURAL vs SEMANTIC (src.ontology.link_classes, agreed with Seshat by DM before
    either side committed): relax now pulls ONLY on semantic edges (an actual claim about
    content -- cites, follows, possible_upstream...); a structural/membership edge still
    exists as a real fact, it just never exerts a spring force in this layout.
  - PROJECT CENTERS are no longer a sunflower-by-rank: a weighted force layout over the
    CONTRACTED project graph (`_place_projects`/`_relax_projects`) pulls two projects
    together in proportion to how many live links actually cross between their own
    members, with a hard per-pair minimum (each project's own measured content radius,
    summed, plus a fixed gutter) so two big projects' clusters can never overlap regardless
    of how strongly they're linked. SoftwareProject objects are placed and stored exactly
    like any other object (same graph_layout_v incrementality) -- a member object then
    looks up ITS OWN project's stored center instead of recomputing one.
  - HUB PINNING: an object whose STRUCTURAL-edge degree crosses `_HUB_DEGREE_THRESHOLD`
    (this house's own measured population: 11 objects over 1,000, 84 over 100 -- the
    principal Persons and the biggest projects) is pinned to rank 0 within its own
    (project, type) group -- dead center of its own cluster, never spiraled outward by an
    ordinary creation-order rank, matching the ruling's own "the hub's cluster contains
    the hub" acceptance line.

graph_layout_v bumped again (3 -> 4) to force the one-time migration this change needs.

THE LEGIBILITY PASS, Khnum tip 2 (operator ruling e1cb9e3b, 2026-09-14 evening, on
screenshots and Thoth's own live measurement -- median nearest-neighbour 19 units at
the fitted zoom, "the pink giant is the osiris project's 19,035-agent sunflower disc
drawn solid, the purple onions are type rings"). Piece (h): TYPE RINGS ARE GONE.
Placement within a project is now by SEMANTIC ADJACENCY alone: an object carrying ANY
live semantic edge (globally, not just within this project -- the same reading
`_hub_ids`'s own structural-degree check already uses) is CONNECTED and seeds inside
the project's own inner disc (base radius 0); an object with zero semantic edges of any
kind is HALO and seeds in a fixed outer ring well clear of the worst-case inner disc's
own extent. Only INTRA-project semantic edges ever pull during relax (unchanged from
THE READING LAYER above) -- a connected object with only cross-project semantic edges
still seeds in the inner disc, it just isn't pulled by anything there, identical to how
an ordinary unconnected object behaved before this pass. Both bands use the SAME
sunflower/declump machinery as before, just keyed on a (project, connected) partition
instead of (project, type) -- `_adjacency_ranks` replaces `_group_ranks`,
`adjacency_position` replaces `base_position`. A hub (`_hub_ids`) still pins to rank 0
of its OWN band (connected or halo, whichever it actually falls in) rather than an
ordinary creation-order rank.

graph_layout_v bumped again (4 -> 5) to force the one-time migration this change needs.

DENSITY NOT DISCS (ruling 6f866d9d, operator 2026-09-15 morning, "lets try that" -- the
near view is a green fan because cluster spacing was radii-plus-a-fixed-gutter with the
halo ring between clusters and every edge drawing at full alpha). Server-side tips (f)
and (g):
  (f) CLUSTER SPACING, NOT TWO RADII PLUS 300: `_relax_projects`'s old minimum
      inter-project distance summed two independently sunflower-derived "radii"
      (`_NODE_SPACING * sqrt(member_count + 1)`, the OBJECT-level minimum-pairwise
      constant reused at project-pair scale) plus a flat 300-unit gutter -- for two
      projects near osiris's own size (11,768 objects) that stacked to ~3,554 units,
      exactly the operator's "way too far apart" complaint. Spacing between a pair is
      now proportional to sqrt of the PAIR'S OWN combined member count directly, at a
      much smaller constant, plus a small (50-unit, was 300) fixed gutter -- one
      formula over the pair, not two independent per-project radii summed.
  (g) HALO WRAPS EACH PROJECT'S OWN CLUSTER, NOT ONE RING AROUND THE PLANE: the old
      `_HALO_BASE` was a single flat constant (6000.0) sized against the WORST-CASE
      population everywhere -- every project's halo band started at the same offset
      regardless of that project's own actual size, so a small project's halo ring sat
      wildly farther out than its own connected disc ever reached. The halo base is now
      computed PER PROJECT from that project's own live connected-member count
      (`_project_halo_base`), the same sunflower-extent formula with a safety margin,
      tightening around each cluster's own real content instead of a shared worst case.
graph_layout_v bumped again (5 -> 6) to force the one-time migration this change needs.

THE PHYSICS LAYOUT (operator ruling d7d55257, Thoth mail 11047): the sunflower/
declump scheme above is retired as the WHOLE-GRAPH placement rule -- see
src.orchestrator.graph_physics for the real force simulation that replaces it
(springs for semantic edges, weak container-gravity, nested communities, run once
per migration, never per-tick). This module keeps every piece that scheme still
needs: `_declump` (the physics migration's own final collision floor), `relax`
(this module's ONGOING incremental placement for objects created AFTER a migration
runs still uses it, just seeded differently -- see `layout_batch`'s own docstring),
and `_hub_ids` (graph_physics.py's own "universal hub" definition, reused
unchanged rather than inventing a second one).

`layout_batch`'s OWN placement for a never-before-placed "regular" object changes
here too: instead of the old sunflower base position (`adjacency_position`,
project/type/connected-band ranking), a new object now seeds at the CENTROID of its
already-placed semantic neighbours and live container objects (`_live_containers_of`/
`_centroid_seed`) -- "initialise at the centroid of placed neighbours (or the
container centroid)" -- then the SAME bounded local relax as before nudges it from
that seed, with every already-placed object still pinned.

THE DEAD SUNFLOWER CODE REMOVAL (bbox compactness follow-up, decision cc2f2ea7):
the ADJACENCY/HALO band scheme this section replaced (`_adjacency_ranks`,
`adjacency_position`, `_project_halo_base`, `_project_connected_counts`, plus the
`_INNER_BASE`/`_HALO_BASE`/`_HALO_MARGIN`/`_HALO_MIN`/`_UNFILED_KEY` constants that
existed only to feed it) genuinely had zero callers anywhere and has been removed.
`_place_projects`, `_relax_projects` and `project_center` were WRONGLY grouped with
that dead scheme by an earlier version of this same paragraph -- a stale claim,
caught before it caused a live deletion: all three are THE READING LAYER's own
(ruling c5953bb1) still-live machinery, called directly by `layout_batch` below for
every newly-arriving SoftwareProject and the unfiled sentinel's own center. A
comment naming its own dead siblings is exactly the kind of claim that needs a real
grep before it's trusted, not just re-quoted forward.

graph_layout_v bumped again (6 -> 7) to force the one-time migration this change
needs -- run via graph_physics.run_physics_migrate, NOT `run_layout_migrate`'s own
batch loop (a global force simulation cannot be sliced into independent batches the
way the old sunflower scheme could; see that module's own docstring).
"""
from __future__ import annotations

import hashlib
import json
import math
import uuid
from collections import defaultdict
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import asyncpg
import numpy as np

from src.actions.core import Actions
from src.ontology.link_classes import CONTAINER_LINK_TYPES, STRUCTURAL_LINK_TYPES

GRAPH_LAYOUT_SOURCE = "cron:graph_layout"
_BATCH_SIZE = 1000
_ITERATIONS = 50
_IDEAL_EDGE_LEN = 60.0
_MAX_STEP = 10.0

# NAVIGABLE SPACE, piece A additions ---------------------------------------------------
_LAYOUT_VERSION_PROP = "graph_layout_v"
_LAYOUT_VERSION = 9  # bump this to force one migration pass over every already-placed object
_RELAX_ITERATIONS = 6  # "a FEW iterations" -- a nudge on top of the deterministic base,
                       # never enough to erase the sunflower structure
_GOLDEN_ANGLE = math.pi * (3.0 - math.sqrt(5.0))

# spacing constants, sized against this house's OWN measured population (live, via
# /graph/supernodes and /graph/clusters, never guessed) before the DECLUMP FIX landed:
# worst single (project,type) group is Agent/unfiled at 18,978 (sunflower radius at that
# rank, spacing 15.0, is ~2,067); worst real project is osiris itself at 11,768 objects
# (Commit alone 6,273, radius ~1,188 at the same spacing) across 40 real projects total.
_NODE_SPACING = 15.0  # minimum pairwise spacing within one (project, connected) sunflower disc
_MIN_SEPARATION = _NODE_SPACING  # hard floor the post-relax declump pass enforces
_PROJECT_SPACING = 5000.0  # unfiled's own fixed seed spacing for `project_center` --
                           # real projects are placed by _relax_projects instead, below

# THE READING LAYER additions ----------------------------------------------------------
_PROJECT_GUTTER = 50.0  # small, fixed clearance on top of a pair's own spacing (DENSITY
                        # NOT DISCS tip (f) -- was 300.0, "radii plus 300")
_PROJECT_SPACING_K = 4.0  # DENSITY NOT DISCS tip (f): the old minimum inter-project
                          # distance summed two independently sunflower-derived "radii"
                          # (_NODE_SPACING * sqrt(member_count+1) per project, the
                          # OBJECT-level 15.0 spacing constant reused at project-pair
                          # scale) -- for two projects near osiris's own size (11,768
                          # objects) that stacked to ~3,554 units, the operator's own
                          # "way too far apart" complaint. Spacing between a pair is now
                          # proportional to sqrt of the PAIR'S OWN combined member count
                          # directly, at this much smaller constant: the same worst pair
                          # (11,768 + 11,768) now clears ~653 units instead of ~3,554.
_PROJECT_RELAX_ITERATIONS = 300  # small N (a few dozen projects) -- cheap even at this
                                 # iteration count, and the weighted spring needs more
                                 # rounds than the object-level relax to actually settle
_HUB_DEGREE_THRESHOLD = 1000  # this house's own measured population: 11 objects over
                              # 1,000 structural-degree, 84 over 100 -- 1,000 catches the
                              # unambiguous hubs (principal Persons, the biggest projects)
                              # without pulling in every moderately-busy object
_MEMBERSHIP_CONTAINER_LINK_TYPES = frozenset({"in_repo", "works_in", "holds", "member_of"})
                              # THE LONG EDGES RULING tip (d): the subset of
                              # CONTAINER_LINK_TYPES that names an actual
                              # membership home (a SoftwareProject, a Seat) --
                              # deliberately excludes acts_for/spawned_by, which
                              # model delegation/lineage, not membership, and
                              # whose own targets (a Person, a coordinator) are
                              # exactly the operator's own "keep the zone" case.
                              # See `_hub_ids`'s own docstring for why this is
                              # narrower than the module-wide constant.


LAST_DECLUMP_WORK: dict[str, int] = {"pairs_resolved": 0, "iterations_run": 0}
# THE DECLUMP REWRITE (operator's word 2026-09-18): `_declump` has no timing
# assertion of its own (a shared box's load is never a correctness signal) -- this
# is the real, measured DETERMINISTIC work its most recent call actually did
# (pairs the KD-tree search resolved across every iteration, and how many
# iterations actually ran before the fixed-point stop), for a caller's own
# acceptance test to assert a work bound against instead of a wall-clock one.


def _hash01(key: str) -> float:
    """A deterministic pseudo-random float in [0,1) from a stable hash of `key` -- never
    Python's own hash() (salted per-process, so it would jitter every restart); sha256
    keeps every derived value reproducible across ticks, processes, and reruns."""
    digest = hashlib.sha256(key.encode()).digest()
    return int.from_bytes(digest[:8], "big") / 2**64


def _sunflower_point(rank: int, spacing: float) -> tuple[float, float]:
    """The ONE placement primitive this module builds everything else from: a
    golden-angle sunflower/Fermat spiral point at a given non-negative integer RANK,
    at a given spacing scale. Pure function of (rank, spacing) alone -- the caller is
    responsible for making sure `rank` itself is a STABLE, permanent value (creation-
    order, never a hash) so recomputing this for the same rank always lands on the
    same point, and a later-arriving sibling only ever gets a higher rank, never
    disturbing an earlier one's placement."""
    r = spacing * math.sqrt(rank + 0.5)
    theta = rank * _GOLDEN_ANGLE
    return r * math.cos(theta), r * math.sin(theta)


def project_center(rank: int) -> tuple[float, float]:
    """A sunflower point at a given rank -- used ONLY for the `unfiled` sentinel's own
    fixed center (rank 0, i.e. the origin) now that real projects are placed by
    `_relax_projects`'s weighted force layout instead (THE READING LAYER, ruling
    c5953bb1): unfiled has no real SoftwareProject row to store a position on, and
    isn't a node in the contracted project graph a force layout would place it against
    anyway. Kept as a plain sunflower point (not just a hardcoded origin) so a future
    caller with a real reason to rank unfiled-like sentinels can still do so."""
    return _sunflower_point(rank, _PROJECT_SPACING)


async def unplaced_batch(actions: Actions, limit: int = _BATCH_SIZE) -> list[uuid.UUID]:
    """Object ids still missing the CURRENT layout-version marker, oldest first
    (created_at) -- a stable, deterministic order so successive ticks make real
    progress. Deliberately keyed on the marker, not on graph_x's mere presence: an object
    positioned under a PRIOR layout version (missing today's marker even though it
    already carries graph_x/graph_y from that older scheme) is swept up here too, which
    is the entire mechanism behind the one-time migration a version bump causes."""
    rows = await actions.pool.fetch(
        "SELECT o.id FROM objects o "
        "WHERE o.status NOT IN ('archived','merged','retired') "
        "  AND NOT EXISTS (SELECT 1 FROM current_assertions a "
        "    WHERE a.object_id=o.id AND a.name=$2 AND (a.value #>> '{}')::int = $3) "
        "ORDER BY o.created_at ASC LIMIT $1",
        limit, _LAYOUT_VERSION_PROP, _LAYOUT_VERSION)
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


async def _project_and_type(
    actions: Actions, ids: list[uuid.UUID],
) -> dict[uuid.UUID, tuple[uuid.UUID | None, str]]:
    """Each id's own (project OBJECT ID or None, object type) -- the same `in_repo` ->
    SoftwareProject membership /graph/supernodes already reads, DISTINCT ON the object so
    a rare multi-project membership still yields exactly one (deterministic: the lowest
    link id) rather than fanning an id out into two placement candidates. The project's
    own ID (not its canonical) is what a caller needs to look up ITS stored center via
    `positions_for` -- THE READING LAYER, ruling c5953bb1: a project's position is no
    longer derivable from a rank alone, it has to be read back from wherever
    `_place_projects` actually put it.

    THE MEMBERSHIP UNION FIX (ruling d7d55257, Thoth mail 11221): `project_id` is
    now `COALESCE(<in_repo target>, <project assertion mapped to its repo object
    by canonical 'repo:'||name>)` -- in_repo still wins when an object somehow
    carries both, the assertion is only ever a fallback for an object with NO live
    in_repo link at all. Live specimen: 16,226 objects carried a `project`
    assertion with ZERO carrying an in_repo link, all incrementally placed as
    unfiled before this fix."""
    if not ids:
        return {}
    rows = await actions.pool.fetch(
        "SELECT DISTINCT ON (o.id) o.id, o.type, "
        "  COALESCE(p.id, ap.id) AS project_id "
        "FROM objects o "
        "LEFT JOIN links l ON l.from_id=o.id AND l.type='in_repo' "
        "  AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "LEFT JOIN objects p ON p.id=l.to_id AND p.type='SoftwareProject' "
        "LEFT JOIN current_assertions a ON a.object_id=o.id AND a.name='project' "
        "LEFT JOIN objects ap ON ap.type='SoftwareProject' "
        "  AND ap.canonical = 'repo:' || (a.value #>> '{}') "
        "WHERE o.id = ANY($1::uuid[]) "
        "ORDER BY o.id, l.id",
        ids)
    return {r["id"]: (r["project_id"], r["type"]) for r in rows}


async def _neighbors_of(
    actions: Actions, ids: list[uuid.UUID], *, semantic_only: bool = False,
) -> dict[uuid.UUID, set[uuid.UUID]]:
    """`semantic_only` (THE READING LAYER, ruling c5953bb1): drop every structural/
    membership edge before it can ever reach the relax pass -- a spoke to a shared
    hub (in_repo, acts_for, works_in...) is a real fact, it just never gets to act as
    a spring in this layout, which is exactly the fix for the "huge cross-cluster
    bundle" the operator's own screenshot showed."""
    if not ids:
        return {}
    rows = await actions.pool.fetch(
        "SELECT from_id, to_id, type FROM links "
        "WHERE from_id = ANY($1::uuid[]) OR to_id = ANY($1::uuid[])",
        ids)
    idset = set(ids)
    out: dict[uuid.UUID, set[uuid.UUID]] = {i: set() for i in ids}
    for r in rows:
        if semantic_only and r["type"] in STRUCTURAL_LINK_TYPES:
            continue
        f, t = r["from_id"], r["to_id"]
        if f in idset:
            out[f].add(t)
        if t in idset:
            out[t].add(f)
    return out


async def _live_containers_of(
    actions: Actions, ids: list[uuid.UUID],
) -> dict[uuid.UUID, list[uuid.UUID]]:
    """THE PHYSICS LAYOUT (Thoth mail 11047), item 6: every live CONTAINER_LINK_TYPES
    target FROM each id -- an object can genuinely have several (its own project via
    in_repo, an Agent's own works_in project, acts_for principal, spawned_by parent,
    holds seat, member_of organization), all real candidates for the new-object
    centroid seed below."""
    if not ids:
        return {}
    rows = await actions.pool.fetch(
        "SELECT from_id, to_id FROM links "
        "WHERE from_id = ANY($1::uuid[]) AND type = ANY($2::text[]) "
        "  AND (valid_until IS NULL OR valid_until > now())",
        ids, list(CONTAINER_LINK_TYPES))
    out: dict[uuid.UUID, list[uuid.UUID]] = defaultdict(list)
    for r in rows:
        out[r["from_id"]].append(r["to_id"])
    return out


def _centroid_seed(
    local_rank: int, candidates: list[tuple[float, float]],
    fallback: tuple[float, float],
) -> tuple[float, float]:
    """THE PHYSICS LAYOUT, item 6: "initialise at the centroid of placed neighbours
    (or the container centroid)" -- a plain mean over whichever already-placed
    semantic neighbours and live containers a new object has; `fallback` (the
    unfiled origin) only for the rare genuinely isolated new object with none.

    OFFSET BY A SUNFLOWER POINT keyed on `local_rank` (this object's own index
    within THIS batch, not a stored global rank -- the seed is used exactly once,
    at first placement, so cross-tick reproducibility of the offset itself doesn't
    matter the way it does for a position that's read back later). Several siblings
    sharing their ONE sole container (no semantic edges of their own) would
    otherwise all seed at the EXACT same centroid -- a real specimen (40 such
    siblings, one shared project) hit two compounding failures from that: `relax`'s
    own repulsion term (~ 1/distance) is explosive between near-coincident starting
    points, throwing some objects tens of units off course in just a few bounded
    iterations; and even where relax stayed calm, cramming 40 mutually-~15-unit-
    apart requirements into `_declump`'s bounded iteration budget from a near-total
    coincidence left one pair a few thousandths short of `_MIN_SEPARATION` after
    rounding. A sunflower offset at `_NODE_SPACING` gives every sibling in the SAME
    batch a real head start already close to the declump floor apart from its
    fellows, not just off the exact centroid point."""
    if not candidates:
        cx, cy = fallback
    else:
        cx = sum(p[0] for p in candidates) / len(candidates)
        cy = sum(p[1] for p in candidates) / len(candidates)
    lx, ly = _sunflower_point(local_rank, _NODE_SPACING)
    return cx + lx, cy + ly


def _grid_cells(pos: np.ndarray, cell_size: float) -> dict[tuple[int, int], list[int]]:
    """THE PHYSICS LAYOUT OOM FIX (Thoth mail 11097): a spatial-hash structure built
    to replace a full (n,n,2) pairwise array -- the old form of that array-building
    code built exactly that array over the WHOLE population every iteration, 40 GB
    at n=50,087 (kernel-confirmed OOM kill, anon-rss 26.3 GB before it died).
    Bucketing by `floor(pos / cell_size)` with `cell_size == min_sep` is the
    standard grid-hash guarantee: any two points within `min_sep` of each other are
    either in the same cell or one of its 8 neighbors (never farther), so checking
    only those 9 cells per point catches every real collision with no O(n^2) memory
    anywhere -- the heartbeat's own 1000-object batches never surfaced this because
    a 1000x1000 array (16 MB) is nothing; the physics migration's 50,087x50,087 one
    was 40 GB.

    `graph_layout._declump` (THE DECLUMP REWRITE, operator's word 2026-09-18) no
    longer uses this -- it moved to a `scipy.spatial.cKDTree` pair search instead,
    see that function's own docstring. This spatial-hash structure is still real,
    live code: `graph_physics.py`'s own whole-graph hard-minimum-distance clamp
    (a genuinely separate pass, at migration scale) calls it directly."""
    cells: dict[tuple[int, int], list[int]] = defaultdict(list)
    if len(pos) == 0:
        return cells
    idx = np.floor(pos / cell_size).astype(np.int64)
    for i, (cx, cy) in enumerate(idx):
        cells[(int(cx), int(cy))].append(i)
    return cells


def _neighbor_cell_indices(
    cells: dict[tuple[int, int], list[int]], cx: int, cy: int,
) -> list[int]:
    out: list[int] = []
    for dx in (-1, 0, 1):
        for dy in (-1, 0, 1):
            out.extend(cells.get((cx + dx, cy + dy), []))
    return out


_DECLUMP_PAIR_CHUNK = 1_000_000  # THE DENSE-CELL FIX's own successor: the grid-hash
                                 # version this replaced deliberately capped its own
                                 # vectorized pass at 5,000,000 pairs (~80 MB) and fell
                                 # back to a slow-but-memory-safe Python loop above it
                                 # -- a live specimen (one grid cell, 6,131 coincident
                                 # points, 634M candidate pairs) is exactly why. A
                                 # cKDTree query_pairs call over a similarly dense
                                 # cluster returns the SAME huge pair count in one
                                 # shot (that's real, unavoidable math: if N points are
                                 # all mutually within `min_sep`, there really are
                                 # N*(N-1)/2 violating pairs) -- measured live testing
                                 # this rewrite: 6,000 fully-coincident points spiked
                                 # RSS by 1.4 GB fully vectorized in one pass. Chunking
                                 # the deficit/direction/push math (never the KD-tree
                                 # query itself, which stays one call) keeps the SAME
                                 # vectorized numpy speed with bounded peak memory
                                 # (~56 MB/chunk) regardless of how dense any one
                                 # cluster gets.


def _apply_pair_pushes(
    disp: np.ndarray, pos_a: np.ndarray, pos_b: np.ndarray, i_arr: np.ndarray,
    j_arr: np.ndarray, *, min_sep: float, ids: list[uuid.UUID], tag: str,
    b_is_movable: bool, split: bool,
) -> float:
    """The vectorized deficit/direction/push computation shared by both the
    movable-movable and movable-anchor KD-tree passes below: given the two
    endpoints' own positions (`pos_a[i_arr]`, `pos_b[j_arr]`) for every pair already
    known to be closer than `min_sep` (`query_pairs`/`query_ball_tree` only ever
    return pairs meeting that test), scatters the push into `disp` IN PLACE -- split
    in half onto both sides for a movable-movable pair (`split=True`), or applied
    whole onto the `a` (movable) side alone for a movable/anchor one (`split=False`,
    the anchor never moves) -- and returns the total deficit magnitude summed (a
    real, work-proportional number for the caller's own deterministic-work-bound
    reporting, never a timing figure).

    CHUNKED over `_DECLUMP_PAIR_CHUNK`-sized batches (see its own docstring) --
    bounds peak memory regardless of how many pairs `query_pairs`/`query_ball_tree`
    hand back in one call, without giving up the vectorized numpy speed within each
    chunk.

    EXACT-COINCIDENCE TIE-BREAK unchanged from the grid-hash version: a pair at
    `dist < 1e-9` gets a deterministic hash-derived direction instead of a
    divide-by-zero, keyed on the `a` side's own stable id and the `b` side's own
    stable id when `b_is_movable` (both endpoints share `ids`) or its plain
    positional index when `b` is an anchor (anchors carry no id of their own here)
    -- exactly the same key shape the grid-hash version's own tie-break used."""
    total_deficit = 0.0
    for start in range(0, len(i_arr), _DECLUMP_PAIR_CHUNK):
        i_chunk = i_arr[start:start + _DECLUMP_PAIR_CHUNK]
        j_chunk = j_arr[start:start + _DECLUMP_PAIR_CHUNK]
        delta = pos_a[i_chunk] - pos_b[j_chunk]
        dist = np.linalg.norm(delta, axis=1)
        zero_idx = np.where(dist < 1e-9)[0]
        for z in zero_idx:
            gi, gj = int(i_chunk[z]), int(j_chunk[z])
            gj_key: uuid.UUID | int = ids[gj] if b_is_movable else gj
            a = _hash01(f"{tag}:{ids[gi]}:{gj_key}") * 2 * math.pi
            delta[z] = (math.cos(a), math.sin(a))
            dist[z] = 1.0
        deficit = min_sep - dist
        direction = delta / dist[:, None]
        push_full = direction * deficit[:, None]
        total_deficit += float(deficit.sum())
        if split:
            np.add.at(disp, i_chunk, push_full / 2)
            np.add.at(disp, j_chunk, -push_full / 2)
        else:
            np.add.at(disp, i_chunk, push_full)
    return total_deficit


def _declump(
    pos: np.ndarray, anchor_pos: np.ndarray, ids: list[uuid.UUID], *,
    min_sep: float = _MIN_SEPARATION, iterations: int = 30,
) -> np.ndarray:
    """The HARD MINIMUM-SEPARATION pass (Thoth mail 10582, PRIORITY): a direct,
    deterministic correction, never another force-simulation step -- `relax()`'s own
    repulsion approaches but does not GUARANTEE a floor within a bounded iteration
    count, so strong attraction could still leave two connected nodes (or a node and
    a fixed anchor) closer than `min_sep`, at the limit exactly coincident. Pushes
    every pair closer than `min_sep` apart by exactly the deficit (split evenly
    between two movable points; the FULL deficit onto the movable side of a
    movable/anchor pair, since the anchor never moves) -- iterates a bounded few
    times since separating one pair can nudge another pair together, and stops the
    moment a full pass finds nothing left to fix.

    KD-TREE PAIR SEARCH (THE DECLUMP REWRITE, operator's word 2026-09-18, superseding
    the spatial-hash-grid version this function held before): `scipy.spatial.cKDTree`
    replaces the grid-hash's own cell bucketing entirely. Movable-movable pairs come
    from `cKDTree(pos).query_pairs(min_sep)` -- a single C-level call over the whole
    population, rebuilt each iteration since `pos` moves. Movable-anchor pairs use a
    SECOND tree over `anchor_pos`, built EXACTLY ONCE outside the iteration loop
    (anchors never move, so re-building it every pass would be pure waste) queried via
    `query_ball_tree` from a fresh movable tree each iteration. Same deterministic
    semantics as the grid-hash version it replaces: the same even split of the
    deficit between two movable points, the full deficit onto the movable side of a
    movable/anchor pair, and the same stop-the-moment-a-full-pass-finds-nothing rule
    (`LAST_DECLUMP_WORK` is left holding the real, measured count of pairs resolved
    and iterations actually run by this call -- the acceptance test's own
    deterministic work bound, never a timing figure)."""
    from scipy.spatial import cKDTree

    anchor_tree = cKDTree(anchor_pos) if len(anchor_pos) else None
    pairs_resolved = 0
    iterations_run = 0
    for _ in range(iterations):
        iterations_run += 1
        moved = False

        tree = cKDTree(pos)
        mm_pairs = tree.query_pairs(min_sep, output_type="ndarray")
        if len(mm_pairs):
            i_arr, j_arr = mm_pairs[:, 0], mm_pairs[:, 1]
            disp = np.zeros_like(pos)
            _apply_pair_pushes(
                disp, pos, pos, i_arr, j_arr, min_sep=min_sep, ids=ids, tag="declump",
                b_is_movable=True, split=True)
            pos = pos + disp
            moved = True
            pairs_resolved += len(i_arr)

        if anchor_tree is not None:
            movable_tree = cKDTree(pos)
            candidates = movable_tree.query_ball_tree(anchor_tree, min_sep)
            i_list = [i for i, cand in enumerate(candidates) for _j in cand]
            j_list = [j for cand in candidates for j in cand]
            if i_list:
                i_arr = np.asarray(i_list, dtype=np.int64)
                j_arr = np.asarray(j_list, dtype=np.int64)
                disp2 = np.zeros_like(pos)
                _apply_pair_pushes(
                    disp2, pos, anchor_pos, i_arr, j_arr, min_sep=min_sep, ids=ids,
                    tag="declump-anchor", b_is_movable=False, split=False)
                pos = pos + disp2
                moved = True
                pairs_resolved += len(i_arr)

        if not moved:
            break
    LAST_DECLUMP_WORK["pairs_resolved"] = pairs_resolved
    LAST_DECLUMP_WORK["iterations_run"] = iterations_run
    return pos


def relax(
    unplaced: list[uuid.UUID], neighbors: dict[uuid.UUID, set[uuid.UUID]],
    anchors: dict[uuid.UUID, tuple[float, float]], *, iterations: int = _ITERATIONS,
    seed: int = 42, init: dict[uuid.UUID, tuple[float, float]] | None = None,
) -> dict[uuid.UUID, tuple[float, float]]:
    """Vectorized numpy Fruchterman-Reingold, LOCAL to `unplaced`: those nodes repel each
    other and are pulled toward their neighbors (both other unplaced nodes and fixed
    `anchors`, which never move) -- deterministic seed, so a re-run of the same batch
    reproduces the same layout rather than jittering on every retry.

    `init`, when given, seeds `pos` from these exact coordinates instead of a random
    uniform scatter (NAVIGABLE SPACE piece A: the deterministic sunflower base
    position, so the relax pass nudges toward neighbors from a real starting point
    rather than replacing it with noise; every id in `unplaced` must appear in `init`
    when it is given). `seed`/random init stays the default for every caller that
    doesn't pass one (this module's own pure-function unit tests keep working
    unchanged).

    Ends with `_declump`'s hard minimum-separation pass (Thoth mail 10582) -- see its
    own docstring; this is what actually guarantees two connected nodes never end up
    stacked, since the FR iterations above only ever approach that floor.

    MATRIX operations over the whole batch at once, never a Python double-loop over pairs
    -- measured live: the naive per-pair Python/numpy-scalar version didn't finish 300
    nodes in 60s (numpy's per-call overhead dominates at that granularity); this version
    positions 1000 nodes in a small fraction of a second by computing the full NxN
    displacement in one broadcast per iteration, the same way every real force-layout
    implementation (fcose included) actually does it."""
    n = len(unplaced)
    idx = {nid: i for i, nid in enumerate(unplaced)}
    if init is not None:
        pos = np.array([init[nid] for nid in unplaced], dtype=np.float64)
    else:
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

    pos = _declump(pos, ua_p, unplaced)

    return {nid: (float(pos[idx[nid], 0]), float(pos[idx[nid], 1])) for nid in unplaced}


def _intra_project_neighbors(
    unplaced: list[uuid.UUID],
    neighbors: dict[uuid.UUID, set[uuid.UUID]],
    proj_type: dict[uuid.UUID, tuple[uuid.UUID | None, str]],
) -> dict[uuid.UUID, set[uuid.UUID]]:
    """`neighbors`, filtered to same-PROJECT pairs only -- the ruling's own "edge
    attraction within a project": a cross-project edge never pulls either endpoint,
    regardless of how it would have pulled under the old whole-graph relax. `neighbors`
    itself is expected to already be SEMANTIC-only (see `_neighbors_of`'s own
    `semantic_only` -- THE READING LAYER, ruling c5953bb1: a structural/membership edge
    never pulls here, project-mate or not)."""
    out: dict[uuid.UUID, set[uuid.UUID]] = {}
    for nid in unplaced:
        proj = proj_type.get(nid, (None, "Unknown"))[0]
        out[nid] = {
            nb for nb in neighbors.get(nid, set())
            if proj_type.get(nb, (object(), ""))[0] == proj
        }
    return out


async def _hub_ids(actions: Actions, ids: list[uuid.UUID]) -> set[uuid.UUID]:
    """Objects whose STRUCTURAL-edge degree meets `_HUB_DEGREE_THRESHOLD` -- these get
    pinned to rank 0 within their own (project, type) group (dead center of that
    group's own sunflower disc) rather than an ordinary creation-order rank, per THE
    READING LAYER's own "the hub's cluster contains the hub" acceptance line.

    THE LONG EDGES RULING tip (d) (operator, grounds d7d55257): a MEMBERSHIP
    container (any object that is the TARGET of a live in_repo/works_in/holds/
    member_of link -- a SoftwareProject, a Seat) is NEVER a hub-zone candidate,
    however high its own structural degree measures -- ruling d7d55257 already
    says "the container node is placed AT the centroid it earns", and
    `graph_physics`'s own level-2 machinery (or, for a non-project container,
    `_place_unfiled`'s now-widened neighbour-mean) already gives it exactly that
    position. Pulling a container into the hub zone on top of that doesn't just
    relocate one vertex -- it breaks every one of ITS OWN members' container
    edges at once (measured live: 9 of 43 active projects were hub-classified
    this way, in_repo alone contributing 12,063 of 37,810 edges over 20k world
    units).

    DELIBERATELY NARROWER than `CONTAINER_LINK_TYPES` (which also includes
    acts_for/spawned_by): those two model delegation/lineage, not membership,
    and their own targets are exactly the "non-container hubs (a Person, a
    coordinator) keep the zone" case the operator's own ruling named --
    `test_hub_ids_finds_a_structural_high_degree_object`'s own Person-via-
    acts_for specimen would silently stop being a hub under the wider set,
    caught by running that test before committing to which subset this meant."""
    if not ids:
        return set()
    rows = await actions.pool.fetch(
        "SELECT node, count(*) AS n FROM ("
        "  SELECT from_id AS node, type FROM links "
        "    WHERE valid_until IS NULL OR valid_until > now() "
        "  UNION ALL "
        "  SELECT to_id AS node, type FROM links "
        "    WHERE valid_until IS NULL OR valid_until > now()"
        ") x WHERE type = ANY($2::text[]) AND node = ANY($1::uuid[]) "
        "  AND node NOT IN ("
        "    SELECT DISTINCT to_id FROM links "
        "    WHERE type = ANY($4::text[]) "
        "      AND (valid_until IS NULL OR valid_until > now())"
        "  ) "
        "GROUP BY node HAVING count(*) >= $3",
        ids, list(STRUCTURAL_LINK_TYPES), _HUB_DEGREE_THRESHOLD,
        list(_MEMBERSHIP_CONTAINER_LINK_TYPES))
    return {r["node"] for r in rows}


async def _project_member_counts(
    actions: Actions, project_ids: list[uuid.UUID],
) -> dict[uuid.UUID, int]:
    """Every project's own active member count -- used as a size PROXY (never the
    real bounding radius, which would need each type's own sub-disc; the simple
    sqrt(N) estimate over the TOTAL is deliberately conservative, i.e. an
    overestimate, so the gutter this feeds into never runs short)."""
    if not project_ids:
        return {}
    rows = await actions.pool.fetch(
        "SELECT p.id AS project_id, count(*) AS n FROM objects o "
        "JOIN links l ON l.from_id=o.id AND l.type='in_repo' "
        "  AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "JOIN objects p ON p.id=l.to_id AND p.id = ANY($1::uuid[]) "
        "WHERE o.status NOT IN ('archived','merged','retired') "
        "GROUP BY p.id",
        project_ids)
    return {r["project_id"]: int(r["n"]) for r in rows}


async def _project_link_weights(
    actions: Actions, project_ids: list[uuid.UUID],
) -> dict[tuple[uuid.UUID, uuid.UUID], int]:
    """Cross-project edge weight for every pair among `project_ids` -- the SAME
    contraction /graph/supernodes already computes for its own `project_edges`
    (live links between two objects whose OWN in_repo project differs), returned as
    a weighted pair map instead of a UI-shaped list. Every link type counts here
    (not just semantic ones) -- this is about how much two PROJECTS actually
    reference each other's work, not what pulls inside a layout's relax pass."""
    if not project_ids:
        return {}
    rows = await actions.pool.fetch(
        "WITH proj_of AS ("
        "  SELECT o.id AS object_id, p.id AS project_id FROM objects o "
        "  JOIN links l ON l.from_id=o.id AND l.type='in_repo' "
        "    AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "  JOIN objects p ON p.id=l.to_id AND p.id = ANY($1::uuid[])) "
        "SELECT LEAST(a.project_id, b.project_id) AS p1, "
        "  GREATEST(a.project_id, b.project_id) AS p2, count(*) AS weight "
        "FROM links l "
        "JOIN proj_of a ON a.object_id = l.from_id "
        "JOIN proj_of b ON b.object_id = l.to_id "
        "WHERE a.project_id <> b.project_id "
        "  AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "GROUP BY LEAST(a.project_id,b.project_id), GREATEST(a.project_id,b.project_id)",
        project_ids)
    return {(r["p1"], r["p2"]): int(r["weight"]) for r in rows}


def _relax_projects(
    unplaced_ids: list[uuid.UUID],
    anchors: dict[uuid.UUID, tuple[float, float]],
    member_counts: dict[uuid.UUID, int],
    weights: dict[tuple[uuid.UUID, uuid.UUID], int],
    *, iterations: int = _PROJECT_RELAX_ITERATIONS, gutter: float = _PROJECT_GUTTER,
    spacing_k: float = _PROJECT_SPACING_K,
) -> dict[uuid.UUID, tuple[float, float]]:
    """THE READING LAYER's own project-center layout (ruling c5953bb1): plain Python,
    never vectorized -- N is a few dozen projects, not thousands, so the clarity of a
    direct pairwise loop matters more than the constant-factor speedup `relax()`
    needs at object scale. Two differences from `relax()`'s own model: the minimum
    distance between two centers is proportional to sqrt of the PAIR'S OWN combined
    member count plus a small gutter (DENSITY NOT DISCS tip (f) -- see
    `_PROJECT_SPACING_K`'s own docstring for why this replaced the old two-independent-
    radii-summed formula), and attraction exists ONLY between projects that actually
    share cross-project links, scaled by how many -- an unlinked pair only ever
    repels, which is what makes "no two linked projects farther apart than an
    unlinked pair of similar size" true by construction rather than by luck.
    Already-placed projects (`anchors`) are fixed, exactly like the object-level
    relax's own anchors. Seeded from a small sunflower point purely for a numerically
    stable, deterministic starting position -- the FINAL position is force-derived,
    the seed carries no visual meaning of its own."""
    pos: dict[uuid.UUID, tuple[float, float]] = {
        pid: _sunflower_point(i, 10.0) for i, pid in enumerate(unplaced_ids)
    }
    all_ids = unplaced_ids + list(anchors.keys())

    def get_pos(pid: uuid.UUID) -> tuple[float, float]:
        return pos[pid] if pid in pos else anchors[pid]

    def weight_of(a: uuid.UUID, b: uuid.UUID) -> float:
        key = (a, b) if str(a) < str(b) else (b, a)
        return float(weights.get(key, 0))

    def min_dist_of(a: uuid.UUID, b: uuid.UUID) -> float:
        return spacing_k * math.sqrt(
            member_counts.get(a, 0) + member_counts.get(b, 0) + 2) + gutter

    for _ in range(iterations):
        disp = {pid: (0.0, 0.0) for pid in unplaced_ids}
        for a in unplaced_ids:
            ax, ay = pos[a]
            for b in all_ids:
                if b == a:
                    continue
                bx, by = get_pos(b)
                dx, dy = ax - bx, ay - by
                dist = math.hypot(dx, dy) or 0.01
                min_dist = min_dist_of(a, b)
                if dist < min_dist:
                    f = min_dist - dist
                    disp[a] = (disp[a][0] + dx / dist * f, disp[a][1] + dy / dist * f)
                w = weight_of(a, b)
                if w > 0 and dist > min_dist:
                    f = w * min(dist - min_dist, 20.0) * 0.02
                    disp[a] = (disp[a][0] - dx / dist * f, disp[a][1] - dy / dist * f)
        moved = False
        for a in unplaced_ids:
            dx, dy = disp[a]
            mag = math.hypot(dx, dy)
            if mag > 0.01:
                moved = True
                capped = min(mag, 20.0)
                pos[a] = (pos[a][0] + dx / mag * capped, pos[a][1] + dy / mag * capped)
        if not moved:
            break

    # HARD MINIMUM-DISTANCE CLAMP, same spirit as `_declump` but with a PER-PAIR floor
    # (the pair's own combined-count spacing plus the gutter) instead of one flat
    # constant -- the iterative spring above approaches this floor, this guarantees it.
    for _ in range(iterations):
        moved = False
        for a in unplaced_ids:
            ax, ay = pos[a]
            for b in all_ids:
                if b == a:
                    continue
                bx, by = get_pos(b)
                dx, dy = ax - bx, ay - by
                dist = math.hypot(dx, dy)
                min_dist = min_dist_of(a, b)
                if dist < min_dist:
                    moved = True
                    if dist < 1e-9:
                        angle = _hash01(f"project-declump:{a}:{b}") * 2 * math.pi
                        dx, dy = math.cos(angle), math.sin(angle)
                        dist = 1.0
                    deficit = min_dist - dist
                    push = deficit if b not in pos else deficit / 2
                    ax += dx / dist * push
                    ay += dy / dist * push
                    pos[a] = (ax, ay)
        if not moved:
            break
    return pos


async def _place_projects(
    actions: Actions, unplaced_ids: list[uuid.UUID],
) -> dict[uuid.UUID, tuple[float, float]]:
    """THE READING LAYER (ruling c5953bb1): project centers via a weighted force
    layout over the CONTRACTED project graph instead of a rank-based sunflower --
    two projects that actually reference each other's own objects end up closer than
    two unrelated projects of similar size, never a hash and never independent of the
    real cross-project link count. Already-placed projects are fixed anchors (never
    revisited), matching every other object's own incrementality guarantee."""
    all_active = await actions.pool.fetch(
        "SELECT id FROM objects WHERE type='SoftwareProject' AND status='active'")
    all_ids = [r["id"] for r in all_active]
    unplaced_set = set(unplaced_ids)
    already_placed = [pid for pid in all_ids if pid not in unplaced_set]
    anchors = await positions_for(actions, already_placed)
    member_counts = await _project_member_counts(actions, all_ids)
    weights = await _project_link_weights(actions, all_ids)
    return _relax_projects(unplaced_ids, anchors, member_counts, weights)


async def _bulk_assert_positions(
    actions: Actions, placed: dict[uuid.UUID, tuple[float, float]], observed_at: datetime,
) -> None:
    """Records graph_x, graph_y, and the version marker for a whole tick's batch --
    append-only and superseding exactly like assert_property (an existing current row for
    this source+name is flipped non-current, never mutated in place), just covering the
    batch with one multi-row statement per property instead of one call per object (see
    the module docstring's WRITE PATH section for why that's safe here)."""
    if not placed:
        return
    ids = list(placed.keys())
    columns: list[tuple[str, list[object]]] = [
        ("graph_x", [round(placed[i][0], 2) for i in ids]),
        ("graph_y", [round(placed[i][1], 2) for i in ids]),
        (_LAYOUT_VERSION_PROP, [_LAYOUT_VERSION for _ in ids]),
    ]
    async with actions.pool.acquire() as conn, conn.transaction():
        for name, values in columns:
            await conn.execute(
                "UPDATE assertions SET is_current=false WHERE object_id = ANY($1::uuid[]) "
                "  AND name=$2 AND source_id=$3 AND is_current",
                ids, name, GRAPH_LAYOUT_SOURCE)
            await conn.execute(
                "INSERT INTO assertions (object_id, name, value, source_id, observed_at, "
                "  confidence, is_current) "
                "SELECT oid, $2, val::jsonb, $3, $4, $5, true "
                "FROM unnest($1::uuid[], $6::text[]) AS t(oid, val)",
                ids, name, GRAPH_LAYOUT_SOURCE, observed_at, 0.9,
                [json.dumps(v) for v in values])


async def layout_batch(actions: Actions, *, limit: int | None = None) -> int:
    """One heartbeat tick: place up to `limit` objects still missing the current layout
    version. SoftwareProject objects in the batch get THE READING LAYER's own weighted
    force-layout placement (`_place_projects`) and are written FIRST, so every other
    object placed in the SAME tick can look up its own project's real stored center
    rather than a placeholder. Every other object gets a deterministic CENTROID base
    position (THE PHYSICS LAYOUT, Thoth mail 11047, item 6: the mean of its already-
    placed semantic neighbours' and live containers' own positions -- `_centroid_seed`/
    `_live_containers_of` -- falling back to the unfiled origin only when genuinely
    isolated), nudged by a few iterations of intra-project SEMANTIC-only edge
    attraction anchored on already-placed same-project neighbors, then hard-declumped.
    Returns how many objects were newly positioned in total (0 when the graph is fully
    placed under the current version -- the tick's own natural quiescence, no flag
    needed).

    THIS IS ONLY THE ONGOING INCREMENTAL RULE for a genuinely new object arriving
    after a migration has already run -- the migration itself
    (graph_physics.run_physics_migrate) is a real global force simulation over the
    whole graph at once, not this function looped to quiescence (see that module's
    own docstring for why the two are genuinely different shapes, not one reused as
    the other).

    `limit=None` (every real caller -- the cron heartbeat and `run_layout_migrate`)
    reads `layout.batch_size` off the LIVE settings table (Thoth mail 10609, product
    law: every action has an entry point) via `current_stored_value` -- effect='next_tick' is
    genuine here, not the env-overlay path that only covers effect='immediate' keys --
    falling back to `_BATCH_SIZE` when the key has never been written. Passing an
    explicit `limit` (every test in this module) bypasses the settings lookup
    entirely, same as before."""
    if limit is None:
        from src.orchestrator.settings_service import current_stored_value
        stored = await current_stored_value(actions.pool, "layout.batch_size")
        limit = int(stored) if isinstance(stored, int | float) else _BATCH_SIZE
    unplaced = await unplaced_batch(actions, limit)
    if not unplaced:
        return 0

    type_rows = await actions.pool.fetch(
        "SELECT id, type FROM objects WHERE id = ANY($1::uuid[])", unplaced)
    type_by_id = {r["id"]: r["type"] for r in type_rows}
    unplaced_projects = [oid for oid in unplaced if type_by_id.get(oid) == "SoftwareProject"]
    unplaced_regular = [oid for oid in unplaced if oid not in set(unplaced_projects)]

    now = datetime.now(UTC)
    placed_count = 0

    if unplaced_projects:
        project_positions = await _place_projects(actions, unplaced_projects)
        await _bulk_assert_positions(actions, project_positions, now)
        placed_count += len(project_positions)

    if unplaced_regular:
        neighbors = await _neighbors_of(actions, unplaced_regular, semantic_only=True)
        unplaced_set = set(unplaced_regular)
        neighbor_ids = sorted(
            ({nb for nbs in neighbors.values() for nb in nbs} - unplaced_set), key=str)
        proj_type = await _project_and_type(actions, unplaced_regular + neighbor_ids)
        containers = await _live_containers_of(actions, unplaced_regular)
        container_ids = sorted(
            ({cid for cids in containers.values() for cid in cids} - unplaced_set),
            key=str)
        unfiled_center = project_center(0)

        anchors = await positions_for(
            actions, sorted(set(neighbor_ids) | set(container_ids), key=str))

        base = {}
        for local_rank, oid in enumerate(unplaced_regular):
            candidates = [anchors[nb] for nb in neighbors.get(oid, set()) if nb in anchors]
            candidates += [anchors[cid] for cid in containers.get(oid, []) if cid in anchors]
            base[oid] = _centroid_seed(local_rank, candidates, unfiled_center)

        intra = _intra_project_neighbors(unplaced_regular, neighbors, proj_type)
        placed = relax(
            unplaced_regular, intra, anchors, iterations=_RELAX_ITERATIONS, init=base)
        await _bulk_assert_positions(actions, placed, now)
        placed_count += len(placed)

    return placed_count


_LAYOUT_LOCK_KEY = "graph_layout_batch"  # advisory-lock name shared by the cron
                                        # heartbeat and run_layout_migrate below


async def _try_acquire_layout_lock(conn: asyncpg.Connection) -> bool:
    """SESSION-scoped `pg_try_advisory_lock`, deliberately -- a transaction-scoped
    lock would release the instant the acquiring query's own tiny transaction
    commits, defeating the entire point of holding it for a whole migration run.
    The historical outage this house learned from (#172: a connection returned to
    the pool while still holding a session lock wedged the fleet for 15 minutes) is
    avoided by construction here, not by avoiding session locks altogether: the ONLY
    caller, `run_layout_migrate`, always releases via `_release_layout_lock` in a
    `finally` BEFORE the `async with actions.pool.acquire()` block that owns this
    connection ever exits -- the lock is never left to the pool's own connection
    reset to clean up."""
    return bool(await conn.fetchval(
        "SELECT pg_try_advisory_lock(hashtext($1))", _LAYOUT_LOCK_KEY))


async def _release_layout_lock(conn: asyncpg.Connection) -> None:
    await conn.execute("SELECT pg_advisory_unlock(hashtext($1))", _LAYOUT_LOCK_KEY)


async def run_layout_migrate(
    actions: Actions, *, limit: int | None = None,
) -> AsyncIterator[dict[str, Any]]:
    """THE MIGRATION ENTRY POINT (Thoth mail 10609): loop `layout_batch` until
    `unplaced_batch` runs dry, yielding one receipt per batch as it happens rather
    than collecting a final report -- a `graph_layout_v` bump otherwise waits on the
    cron heartbeat's own 1000-objects/5-minute pace (hours for a real migration).
    Refuses outright (yields a single `{"error": ...}` receipt, does no work) if the
    cron heartbeat is mid-tick and already holds `_LAYOUT_LOCK_KEY` -- see
    `_try_acquire_layout_lock`'s own docstring for why this is session-scoped and
    safe. The SAME `layout_batch` the cron heartbeat calls -- never a second
    implementation of the placement logic, just a tighter loop around it."""
    async with actions.pool.acquire() as lock_conn:
        if not await _try_acquire_layout_lock(lock_conn):
            yield {"error": "the layout heartbeat (or another migrate run) currently "
                            "holds the layout lock -- try again shortly"}
            return
        try:
            batch_no = 0
            total_placed = 0
            while True:
                n = await layout_batch(actions, limit=limit)
                batch_no += 1
                total_placed += n
                yield {"batch": batch_no, "placed": n, "total_placed": total_placed}
                if n == 0:
                    break
        finally:
            await _release_layout_lock(lock_conn)
