<!-- source: https://www.notion.com/help/intro-to-databases | vendor: notion | topic: databases and views | grounds: src/ui/static/osiris.js -->
# Notion — databases & views

The model behind Osiris's **shape-aware renderer** (Phase 3): one set of data, many switchable
VIEWS, the right view chosen by the data's shape — Palantir's multi-modal object set seen from
the prosumer side. Every Notion item is a page; a database is a collection of pages with
properties; a view is a lens over that collection.

## What a database is
A collection of pages — each row IS a page — with customizable properties (date / status /
relation / …), shown through interchangeable views.

## View types (the Phase-3 vocabulary)
| View | Best for |
|------|----------|
| **Table** | row-column data, all properties visible at once — the scannable default |
| **List** | sequential, title-focused browsing |
| **Board** (kanban) | status/category workflows as card columns |
| **Calendar** | time-based events and deadlines |
| **Gallery** | visual-first, image-prominent cards |
| **Timeline** | project phases / duration-based scheduling — a **ranking by date** |

The lesson for Osiris: an `order`/`take` composition is a *Timeline/List*, an `aggregate` is a
*Board/grouped table*, a `traverse` ego-network is a *graph* — the view follows intent, not a
raw node count. (The Phase-0 fix made `order/take/aggregate → Table`; Phase 3 generalizes it.)

## View controls
- **Properties** — contextualize/label items (which become a Table's columns).
- **Filters** — restrict by property values (Osiris's filter chips).
- **Sorts** — order by a property (Osiris's `order`).
- **Grouping** — cluster items hierarchically by a property (Osiris's `aggregate` / Group-by).
