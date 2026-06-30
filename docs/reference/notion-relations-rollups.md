<!-- source: https://www.notion.com/help/relations-and-rollups | vendor: notion | topic: relations and rollups | grounds: src/orchestrator/compositions.py -->
# Notion — relations & rollups

The prosumer half of Osiris's op grounding. Notion independently lands on the same shape as
Palantir's object sets — a **relation** is a link, a **rollup** is an aggregate over that link —
which is the convergence that told us the closed op set is right, not invented.

## Relation
A property that links pages in one database to pages in another (or itself). Two-way relations
propagate edits both directions automatically; one-way are unidirectional. A relation can be
single-page or unlimited. (Osiris: a `LinkType` / a `traverse`.)

## Rollup — aggregate over a relation
A rollup applies a calculation to a property of the related pages:
- **Display**: `Show original`, `Show unique values`.
- **Count**: `Count all`, `Count values`, `Count unique values`, `Count empty`, `Count not
  empty`, `Percent empty` / `Percent not empty`.
- **Number**: `Sum`, `Average`, `Median`, `Min`, `Max`, `Range`.
- **Date**: `Earliest date`, `Latest date`, `Date range`.

(Osiris: `collect` ⇐ show-original/unique; `aggregate` ⇐ count/sum/….)

## Composition
Relations make the connections; rollups compute metrics across them — e.g. relate Customers →
Items, then `Sum` the related items' Price for total spend, which can itself roll up at the
table level. In Osiris this is `aggregate(metric) over traverse(...)` — a rollup is an
aggregate on a search-around.
