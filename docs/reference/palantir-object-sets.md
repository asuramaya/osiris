<!-- source: https://www.palantir.com/docs/foundry/functions/api-object-sets | vendor: palantir | topic: object-set operations -->
# Palantir Foundry — Object Set operations

The closed vocabulary Osiris's composition ops descend from. An Object Set is an unordered
collection of objects of a single type, lazily materialized (`.all()`); operations compose
into a query, and anything they can't express is a **Function**.

## The core idea
Object Sets defer loading and query efficiently — filter, traverse, aggregate, and combine
sets, materializing only when asked. The op set is **small and closed**; this is the signal
Osiris adopted (`select` ⇐ `.filter`, `traverse` ⇐ `.searchAround`, `aggregate` ⇐ `.groupBy`,
`order`/`take`, `union`/`intersect`/`subtract`).

## Operations
- `.search()` — start from all objects of a type.
- `.filter()` / `.exactMatch()` / `.range()` (`.lt/.lte/.gt/.gte`) / `.contains()` / `.isTrue/.isFalse()` — property predicates.
- phrase / token matchers (`.phrase`, `.matchAnyToken`, `.fuzzyMatchAllTokens`, …) — keyword search.
- geo filters (`.withinDistanceOf`, `.withinPolygon`, `.withinBoundingBox`).
- `Filters.and/or/not()` — boolean combination.
- `.searchAroundPassengers()` — traverse link types to related objects (**max 3 levels deep**).
- `.nearestNeighbors()` — kNN over embedding properties (k ≤ 100).
- `.union()` / `.intersect()` / `.subtract()` — set algebra (relating two sets is set algebra
  or a link traversal — **never a join**).
- `.orderBy()` + `.take()` — rank then top-N (take requires an order first).
- `.groupBy()` (`.topValues/.exactValues/.byRanges/.byFixedWidth/.byYear`) + `.segmentBy()`
  (a 2nd dimension) + metrics `.count/.average/.max/.min/.sum/.cardinality()`.
- `.all()` (≤ 100,000) / `.allAsync()` — materialize.

## Functions — the escape hatch
When operations are insufficient, a **Function** (TypeScript/Python) runs custom logic over an
object set: filtering beyond predicates, multi-step transforms, external API/model calls,
stateful aggregation. Osiris's `_FUNCTIONS` registry is exactly this — coinvest / screen /
subject_report live there, referenced from a composition as `{"op":"function","name":…}`.
