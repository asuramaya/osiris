<!-- topic: concepts -->

# The Composer — grounded spec

The composer is Osiris's front end: the place where intent becomes a **composition** — a
saved, forkable op-tree over the neutral graph. This spec grounds the op vocabulary in
two shipped, battle-tested models so we descend from documented primitives instead of
inventing a worse query language:

- **Palantir Foundry — Object Set operations** ([object-set functions API][p-os],
  [aggregate-object-set API][p-agg]): the closed set of operations over an ontology —
  `filter`, `searchAround`, `union/intersect/subtract`, `aggregate (groupBy + count/
  sum/avg/min/max/cardinality)`, `orderBy/take`. Plus **Functions** for anything the
  ops can't express.
- **Notion — databases, relations, rollups, filters** ([relations & rollups][n-rr],
  [filter database query][n-filter]): the same shape for prosumers — database **filter**,
  **relation** (a link), **rollup** (aggregate over a relation), **sort**, and
  **formulas** (`map`/`filter`/`length`) as the escape hatch.

They independently land on the **same vocabulary**. That convergence is the signal: this
is the right closed set, not our invention.

## The model: a closed op set + a Function escape hatch

The decisive lesson from Palantir's API is that the object-set vocabulary is **small and
closed** (~8 ops). Everything else is a **Function** (arbitrary registered logic). We
adopt that split exactly:

- **Ops** — neutral, composable, domain-blind. A bounded set; we resist growing it.
- **Functions / transforms** — the escape hatch for anything not expressible as ops
  (a value map like `country_of`, a fuzzy name match). Named, pure, registered — *not*
  new ops. This is Osiris's `_TRANSFORMS` registry today; it is Palantir's Functions and
  Notion's formulas.

## The op vocabulary (the closed set)

| Osiris op | Palantir | Notion | Does | Status |
|-----------|----------|--------|------|--------|
| `subject` | (the object in context) | (current page) | the object in focus | ✅ have |
| `select {object_type, where}` | `.filter()` | database filter | objects matching property conditions | ✅ have |
| `traverse {from, direction, hops, link_type}` | `.searchAround()` | relation | follow links to related objects | ✅ have |
| `collect {from, properties, transform}` | (project) | rollup *show original/unique* | the values of properties over a set | ✅ have |
| `subtract {left, right}` | `.subtract()` | — | set/value difference | ✅ have |
| `union {sets}` | `.union()` | — | combine sets | ⬜ P1 |
| `intersect {sets}` | `.intersect()` | — | objects in all sets | ⬜ P1 |
| `aggregate {from, group_by[], metric}` | `.groupBy()/.segmentBy()` + `.count()/.sum()/.average()/.min()/.max()/.cardinality()` | rollup *count/sum/…* | group + a metric per group | ⬜ P1 |
| `order {from, by, dir}` + `take {n}` | `.orderBy()` + `.take(n)` | sort + limit | rank + top-N | ⬜ P1 |

`aggregate` request shape mirrors Palantir's `AggregateObjectSetRequestV2`: a list of
**group_by dimensions** (`{property, type: exact|range}`) and a **metric**
(`{type: count|sum|avg|min|max|cardinality, field?}`).

`select`'s `where` filters a property with a predicate op — `eq` · `contains` ·
`matches_all` (every whitespace token present, any order — word-order-proof recall) ·
`lt` · `gt` · `present` · `absent` (`monitor.match_condition`, shared by the lens and the
watch evaluator).

## Guardrails (adopt Palantir's, they're load-tested)

- **`traverse` depth ≤ 3 hops** — Palantir caps `searchAround` at 3 per query. Deep
  traversal explodes; 3 is the real-world sweet spot.
- **`aggregate` ≤ 3 dimensions** — Palantir caps multi-dimensional aggregation at 3D
  (groupBy + segmentBy). Same reason.
- **The op set stays closed.** A new requirement is a **Function**, not a new op, until
  proven otherwise. The ~10 ops above cover Palantir's entire object-set surface.

## What we deliberately DO NOT build (the anti-reinvention findings)

1. **No generic `join` / SQL.** This was the trap. Palantir has **no join primitive** —
   relating two sets is either `intersect` (set algebra) or `searchAround` (a *link*).
   So **screening** ("does this person match the watchlist?") is not a `join` op. It is
   either: (a) resolution first creates a `same_as`/link, after which it's `traverse` +
   `intersect`; or (b) a **Function** (fuzzy name match — registered logic, the way
   `screen_network` already is). We do not invent a fuzzy-join operator.
2. **No formula language.** Notion's formulas and Palantir's Functions are *code*, not a
   mini-DSL we maintain. Our equivalent is the named-transform registry + Python
   Functions. We don't build an expression interpreter.
3. **No kNN / vector op (yet).** Palantir has it; we don't need it for the read-models.
   It's a Function if ever needed.

## How the read-models decompose (validates the closed set)

Every opinionated read-model is ops + a Function — none needs a new op. But the split
between "pure op-tree" and "Function" is sharper than first sketched. Reading the actual
code (not the wish): **only `discrepancy` is a pure op-tree.** The other three are
*Functions* — their precision lives in domain logic the closed ops deliberately can't
express, which is exactly what Functions are for. Pretending otherwise would ship a worse,
op-tree rewrite (e.g. a `coinvest` that drops its platform-degree filter). The eviction
keeps the logic; it just moves it behind a *named, forkable* `{"op":"function"}` reference.

- **`discrepancy`** = `subtract(collect(location, country°) over traverse(subject, 2),
  collect(home-props, country°) over subject)` — a **pure op-tree** (`country°` is a named
  transform). ✅ proven byte-equal.
- **`coinvest`** — a **Function**. The shape is `traverse → traverse → aggregate(count) →
  order → take`, but the *precision* is merge-aware cluster resolution, a platform-**degree**
  filter (an operator wired into >N companies is plumbing, not a thesis sponsor), and
  feeder-SPV exclusion. None of that is op-expressible; it's registered logic. ✅ evicted as
  a Function, byte-equal.
- **`subject_report`** — a **Function**. Buckets identity fragments into verified /
  corroborated / speculative by a *computed* evidence tier (strongest `evidence_class` +
  distinct-source count + seed/subject status), not a stored property `aggregate` could
  group on. ✅ evicted as a Function, byte-equal.
- **`screen_network`** — a **Function** (fuzzy + shared-identifier watchlist match over the
  financing network) — *not* a join. ✅ evicted as a Function, byte-equal.

So P1 closed the op vocabulary (`union`/`intersect`/`aggregate`/`order`/`take`); P2 proved
the closed-ops-**plus-Functions** model end to end: one read-model is a pure composition,
three are Functions a composition references — and *nothing* is a bespoke read-model welded
into engine code anymore.

## Compositions as saved objects

A composition is a saved op-tree (the `compositions` table). This is **Palantir's saved
Object Sets / Quiver Analyses** and **Notion's saved Views / Templates** — a first-class,
named, forkable, shareable artifact. A **watch** is a composition with a tripwire
execution; a **lens** runs on demand. Defaults ship as templates (forkable, not welded).

[p-os]: https://www.palantir.com/docs/foundry/functions/api-object-sets
[p-agg]: https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/ontology-object-sets/aggregate-object-set
[n-rr]: https://www.notion.com/help/relations-and-rollups
[n-filter]: https://developers.notion.com/reference/post-database-query-filter
