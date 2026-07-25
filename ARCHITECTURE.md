<!-- topic: concepts -->

# Architecture

This describes the **current shape** of Osiris. The build history, with rationale and
dead-ends, is frozen in [`docs/HISTORY.md`](docs/HISTORY.md); the original design
document lives in git history (`DESIGN.md`, removed 2026-07) and, like the history, as
queryable `Reference` nodes inside the graph itself — Osiris is its own documentation
store (`consult_canon` over MCP).

## The one idea

A small, stable **kernel** (an event-sourced, evidence-graded, merge-aware entity graph)
with a ring of **drivers** that only ever *emit* into it through a narrow interface, and
two **surfaces** over it. The kernel imports no driver; drivers depend on the kernel.

That seam — the **narrow waist** — is the whole architecture. It lets the messy,
ever-growing collection layer (every connector, every messy source) accrete and fail
independently without ever destabilizing the core or the read models.

The same kernel serves two corpora with no change — **your own work** (git history,
decisions, the file tree) and **the public record** — because the domain is never the
identity, the engine is. And an autonomic loop, the **pulse**, keeps it fresh: it senses
when a tracked source changes, re-ingests only the delta, and accumulates what changed. The
engine *reads and tells*; it never mutates the world you own (it has no hands over your
systems — see the surfaces). It has exactly one outward act, declared and off by default: the
**wake trigger** can start a Claude session in a repo that has unread fleet mail. It summons a
mind; it does not become one. See [`README.md`](README.md#one-hand-the-wake-trigger).

```
        surfaces        MCP server  ·  FastAPI console  ·  compositions (the composer)
                              │
        ─────────────── narrow waist (Actions API) ───────────────
                              │
        kernel          event-sourced graph  +  resolution
                              │
        loop            the pulse — sense a change · re-ingest the delta · digest it
                              │
        drivers         self-track (git) · federators · extractors · vantage-bound crawlers
                              │
        world           your repos · EDGAR · OpenSanctions · GLEIF · Wikidata · courts · chain · …
```

## The kernel

The source of truth. Append-only and audited; nothing is overwritten, so provenance
survives forever.

- **Objects / assertions / links / events** in Postgres. Objects are entities — for the
  public record: Organization, Person, CryptoAddress, CourtCase, …; for your own work:
  SoftwareProject, Commit, Decision, Thread, File, Reference. Assertions are graded facts
  about them; links are typed, graded edges. The type catalog is a declared semantic layer
  (`ontology/schema.py`); the UI reads it, it never hardcodes types.
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

- **Self-track drivers** — the developer face. `gitlog` (a repo's history → SoftwareProject
  / Commit / developer) and `files` (the tree → File nodes, metadata only; content stays in
  git, read on demand). These are **deterministic observers**: a commit exists, a file
  changed. They cost nothing, they are never wrong, and they are the backbone of the
  developer face. Federating *your own work* is federation too: same waist, same grading, and
  cross-repo identity resolution fuses *you* across your repos.

  A third kind — an **inferring** miner that reads conversation transcripts and proposes
  loose ends you forgot — is a different animal, and it is held to a different standard: it
  may **propose**, never assert, and its licence is its measured rate of use. It is being
  rebuilt and is currently **off**; see [`ROADMAP.md`](ROADMAP.md). The line that matters is
  not background-vs-foreground and not cheap-model-vs-expensive: it is **observe vs infer**.
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

- **MCP server** (`mcp_server.py`) — the AI-facing surface; each tool accepts a UUID or a
  name. External, optional, audited (every tool flows through the Actions layer), never
  embedded in the kernel. This is the primary adoption path.
- **FastAPI console** (`api/app.py`) — the human surface, sharing a live cursor with the
  MCP: when Claude focuses an object or runs a lens, the screen follows (the front end *is*
  the conversation). Cases, objects, the graph view, dossiers, the review tray, the
  document viewer.

Over both sits the **composer**: analyses are *compositions* — saved, forkable op-trees
over the neutral graph (a small closed op set grounded in Palantir's Object Set API +
Notion's rollups, plus a Function escape hatch). Opinionated read-models (a dossier, a
family audit, the pulse digest) are Functions a composition references, so opinion lives in
a forkable spec the user owns, not welded into engine code. Two keystones both surfaces
read: `orchestrator/sources.py` (the investigation playbook *as data*) and
`ontology/schema.py` (the type catalog). Neither surface holds truth or opinion.

## Deployment rings

The rings coordinate through **Postgres + Redis — not an RPC mesh.** Each unit is a
process that dials PG (truth + durable outbox + atomic claim) and Redis (Arq job queue +
per-origin rate buckets + pub/sub). Fate-isolation comes from separate OS processes, not
service discovery.

- **Ring 0 — core:** Postgres + the kernel. Singleton, stateful, protected, backed up.
  Nothing else's failure may touch it. Imports no driver.
- **Ring 1 — edges:** the API and MCP processes. Stateless, restartable, later replicable.
- **Ring 2 — workers:** the Arq workers (cascades, ingestion) and the **pulse** — the
  autonomic loop that senses when a tracked repo/source changes, re-ingests the delta,
  re-runs the lenses, and accumulates a "what changed while you were away" digest. Where
  the flaky, long-running, always-on work lives — must be fate-isolated from core and edges.
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
