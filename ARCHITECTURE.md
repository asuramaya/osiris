# Architecture

This describes the **current shape** of Osiris. [`DESIGN.md`](DESIGN.md) is the original,
deeper design document; where the two differ, this file reflects what is actually built.
The build history, with rationale and dead-ends, is in [`CLAUDE.md`](CLAUDE.md).

## The one idea

A small, stable **kernel** (an event-sourced, evidence-graded, merge-aware entity graph)
with a ring of **drivers** that only ever *emit* into it through a narrow interface, and
two **surfaces** over it. The kernel imports no driver; drivers depend on the kernel.

That seam — the **narrow waist** — is the whole architecture. It lets the messy,
ever-growing collection layer (every connector, every messy source) accrete and fail
independently without ever destabilizing the core or the read models.

```
        surfaces        MCP server  ·  FastAPI app
                              │
        ─────────────── narrow waist (Actions API) ───────────────
                              │
        kernel          event-sourced graph  +  resolution
                              │
        drivers         federators · extractors · vantage-bound crawlers
                              │
        world           EDGAR · OpenSanctions · GLEIF · Wikidata · courts · chain · …
```

## The kernel

The source of truth. Append-only and audited; nothing is overwritten, so provenance
survives forever.

- **Objects / assertions / links / events** in Postgres. Objects are entities
  (Organization, Person, CryptoAddress, CourtCase, …). Assertions are graded facts about
  them. Links are typed, graded edges.
- **Event-sourced identity.** Merges are events (`object_events` is truth;
  `objects.status / merged_into` is a projection). A snapshot replays events to a time;
  an unmerge is a compensating event. Loser links stay in place and are resolved on read.
- **Evidence taxonomy** (`parsers/evidence.py`) — the single source of confidence. Every
  fact is graded by *how it was obtained*:
  `SELF_DECLARED · AUTHORITATIVE_API · DIRECT_OBSERVATION · CO_OCCURRENCE · DERIVED`, with
  `CORROBORATED` computed at read time when ≥2 independent sources agree. Confidence is a
  *projection* of the class, not a guess a parser invents.
- **Resolution** (`ontology/`) — deterministic canonicalization where a natural key
  exists; cross-base fusion by normalized name and by **shared LEI** (a global primary
  key → deterministic merge); a Person-vs-Organization classifier at ingest; review-gated
  probabilistic merges for people (a Person is never auto-merged). Read models are
  **merge-aware** — they expand to the full identity set, or they silently drop data.
- **The narrow waist** is the `Actions` API (`actions/core.py`):
  `create_or_find_object · assert_property · create_link · merge_objects · set_status`.
  A driver depends only on this. Everything is idempotent (find-or-create on canonical),
  every background claim is atomic (a partial-unique index on active statuses), and
  mutations flow into a durable `outbox`.

## The drivers (placeful satellites)

Drivers reach into the messy world and emit graded, sourced mutations through the waist.
They differ along one axis that dictates everything — **placefulness**:

- **Placeless federators** — pure HTTP over open bases; stateless, rate-limited per
  origin, run anywhere. EDGAR (companies + Form D), OpenSanctions (sanctions/PEP + OFAC
  crypto wallets), Wikidata, GLEIF (LEI + ownership), OrgBook BC, CourtListener,
  ClinicalTrials.gov. Etherscan (EVM on-chain) is the one keyed source.
- **Heavy extractors** — OCR / local-ML / AI-extraction; resource-spiky, belong in their
  own worker pool. (The AI-extraction-as-universal-parser pattern is on the roadmap.)
- **Vantage-bound crawlers** — tied to an IP and an identity (antibot portals, co-browse,
  a real browser + session leases). The most isolated and most secret-bearing zone;
  designed to run as a **dial-out queue consumer**, possibly on the operator's own
  machine, so collection can stay at the edge while the kernel centralizes. (Present but
  experimental in v0.1 — see [below](#whats-in-the-tree).)

## The surfaces

Two thin, stateless edges over the same kernel — they hold no truth:

- **MCP server** (`mcp_server.py`) — the AI-facing surface; 18 tools, each accepting a
  UUID or a name. External, optional, audited (every tool flows through the Actions
  layer), never embedded in the kernel. This is the primary adoption path.
- **FastAPI app** (`api/app.py`) — the human surface. Cases, objects, graph view,
  dossiers, the review tray. Currently behind the MCP in coverage (parity is on the
  roadmap, intentionally last).

A keystone both surfaces read: `orchestrator/sources.py` — the investigation playbook *as
data* (`suggest(object_type)` → which sources/analyses apply). It externalizes the "what
do I do next?" judgment so neither a human nor an AI has to carry it.

## Deployment rings

The rings coordinate through **Postgres + Redis — not an RPC mesh.** Each unit is a
process that dials PG (truth + durable outbox + atomic claim) and Redis (Arq job queue +
per-origin rate buckets + pub/sub). Fate-isolation comes from separate OS processes, not
service discovery.

- **Ring 0 — core:** Postgres + the kernel. Singleton, stateful, protected, backed up.
  Nothing else's failure may touch it. Imports no driver.
- **Ring 1 — edges:** the API and MCP processes. Stateless, restartable, later replicable.
- **Ring 2 — workers:** the Arq workers (cascades, ingestion, the coming monitoring loops).
  Where the flaky, long-running work and its failures live — must be fate-isolated from
  core and edges.
- **Ring 3 — drivers:** as above, split by placefulness.

The kernel is **placeless** (runs on any reliable box); the federators are
placeless-but-rate-limited; the crawlers are **placeful** (live wherever their IP,
identity, or secrets force them). The core centralizes; the edges replicate; the
satellites stay where the world makes them stand. See [`ROADMAP.md`](ROADMAP.md) for the
sequence of cuts from one box to hosted.

## Stack

Python 3.12 (uv), async throughout (asyncpg, httpx, arq). FastAPI. Postgres 16 + Redis 7.
Alembic migrations (sync psycopg, migrations only). Tests: pytest-asyncio +
testcontainers against real Postgres/Redis — never SQLite, never mocks. `mypy --strict`
and `ruff` clean on `src/`.

## What's in the tree

The supported v0.1 surface is the kernel, the federators listed above, the analytics
(dossier, network sanctions screening, footprint discrepancy, co-investment, cross-base
resolution), and the two surfaces.

Also present but **experimental / not part of the supported surface** (kept because they
are tested and green, deferred from the engine's focus, useful when their capability is
needed): the cookie-lease / co-browse / federation-preview subsystems
(`connectors/{browser,leases}.py`, `orchestrator/{cobrowse,federation}.py`), the
osint4all augmentation tray, and the frontier-policy research lab (`src/lab/` — an offline
simulator that raced graph-frontier policies; its finding was that the hard problem was
never the frontier but multi-source entity resolution, so the bio-frontier work is parked,
not killed). Treat these as labs, not load-bearing.
