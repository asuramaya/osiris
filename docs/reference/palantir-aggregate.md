<!-- source: https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/ontology-object-sets/aggregate-object-set | vendor: palantir | topic: aggregate object set -->
# Palantir Foundry — Aggregate Object Set

Computes aggregations over an object set: grouping dimensions + metric calculations. Osiris's
`aggregate` op mirrors this request shape (and the `changelog by area` lens is exactly a
`groupBy(scope).count()`).

## Request shape
- **ObjectSet** — `objectType`, `type` (`base` or a variant), optional `filter`.
- **GroupBy dimensions** (one or more):
  - `exact` — group by distinct field values.
  - `range` — partition by startValue/endValue intervals.
  - `fixed-width` — uniform numeric buckets.
  - `by-time` — temporal bucketing (hourly / daily / monthly / …).
- **Metrics** (each has a `name` + `field`): `count`, `sum`, `avg`, `min`, `max`,
  `approximateDistinct` (cardinality estimate).

## Caps & options
- Multi-dimensional aggregation is capped (groupBy + segmentBy → **≤ 3 dimensions**) — Osiris
  adopts this as `MAX_AGGREGATE_DIMS`.
- `accuracy` = `REQUIRE_ACCURATE` | `ALLOW_APPROXIMATE`; response carries `ACCURATE`/`APPROXIMATE`.

## What this teaches the Osiris renderer
An aggregate result is **rows** of `{group, metric}` — a ranking/grouping, not a graph. The
right view is an interactive grouped table where a group drills into the set it counts (the
missing primitive Phase 3 adds), echoing Notion's rollup-on-a-relation.
