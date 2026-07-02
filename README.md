# Osiris

**The memory an AI doesn't have.** A keyless, provenance-first entity-graph engine that
turns a stream of records — *your own work, or the public record* — into durable, sourced,
always-watched memory you compose by conversation. It remembers, it watches, and it tells
you what changed while you were away. It does **not** act for you.

Point it at your repositories and it becomes cross-project memory: every decision you made
and why, every open thread, what drifted across a family of similar projects, and a
heartbeat that notices the moment something changes. Point it at the public record and it
becomes an entity-intelligence engine: companies, filings, sanctions, litigation, on-chain
activity, fused into one graph. Same engine, same kinetic waist, same provenance — two
proven faces.

> **Status.** The kernel is real and proven (379 tests, real Postgres/Redis, `ruff` +
> `mypy --strict` clean). It has deliberately reset **twice** toward *engine-as-product* —
> the domain is never the identity, the engine is. The development log is kept in the open
> in [`CLAUDE.md`](CLAUDE.md), and Osiris tracks its own git history *inside itself* (the
> first and most-dogfooded use). This is a pre-`v0.2` working tree, honest about its edges.

---

## Why it exists — the Osiris/Claude split

An AI has intelligence but no persistent memory, no provenance, and no continuity — it
can't remember last week, can't tell you *how* it knows a thing, and can't run while you're
asleep. **Osiris is precisely those missing organs.** The division of labor:

- **Osiris is the brain and the alarm clock** — a durable graph (memory), where every fact
  carries *how it was obtained* (provenance), watched by an autonomic loop (continuity).
- **Claude is the intelligence**, in two modes: a **lens** (foreground, on-demand — you
  ask, it reasons and drives) and a **tripwire** (background, always-on, flash-tier — it
  narrates what changed, cheaply, 24/7).
- **Neither has hands.** Osiris reads and tells; it never mutates your systems. Acting is
  left to you, to `git`, to your own tools — the last inch stays where trust already lives.

Everything below follows from that split.

---

## What the engine does

- **Turns records into a provenance-graded graph.** Every property and link records its
  **evidence class** — self-declared / authoritative-API / direct-observation /
  co-occurrence / derived / corroborated — so *confidence is a projection of provenance,
  not a guess*. The graph is an append-only, event-sourced ledger; a merge is a reversible
  event.
- **Resolves entities without ever guessing blindly.** Resolution is **candidate-gated** —
  block on a cheap key, judge only the bounded candidates, never all-pairs — so it scales,
  and it *never auto-merges a person*: it queues a review with its reasons.
- **Comes alive with a heartbeat.** An autonomic *pulse* senses when a tracked source
  changes, re-ingests only the delta, re-runs the lenses, and accumulates a *"what changed
  while you were away"* digest. Off-the-clock insight you return to.
- **Is composed by conversation.** Analyses are **compositions** — saved, forkable op-trees
  over the neutral graph (Notion's model, grounded in Palantir's Object Set API) authored by
  Claude from a sentence. Opinion lives in the composition *you* own, never welded into the
  engine.
- **Is driven two ways.** An [MCP](https://modelcontextprotocol.io) server (AI-facing) and a
  FastAPI console (human-facing), over the same engine. The front end *is* the conversation:
  a shared cursor means when Claude focuses something, your screen follows.

The shape is **"Palantir × Notion, composed by conversation"**: an entity-ontology
*substrate* (Palantir, with provenance) whose front end is a *composer* (Notion's neutral
primitives), authored by an AI. See [`ARCHITECTURE.md`](ARCHITECTURE.md).

---

## The two proven faces

### 1. Your own work — cross-project memory (the flagship)

Point Osiris at your repositories. It ingests each git history, tree, and decision, resolves
*you* across repos even when you commit under different emails, and holds your work as one
graph:

- **Decisions & threads, mined from your own commit messages** — "why is it this way?" and
  "what's still blocked?" become a *query*, not a re-read of hundreds of commits.
- **Cross-repo identity resolution** — the same developer fragmented across personal and
  GitHub no-reply emails is surfaced for review (never auto-merged).
- **Family audit** — for a set of sibling projects, what *drifted*: which repos lack a
  CONTRIBUTING, and — the deeper layer — whether the shared files actually *agree* (it found
  a real family licensed inconsistently: MIT in one repo, AGPL in the others).
- **The heartbeat** — a daemon that watches your repos and, the second a commit lands,
  senses it, mines its decisions, and logs the finding — unattended.
- **The design canon** — Palantir/Notion's models ingested as queryable memory, so you
  *cite* a solved problem instead of re-deriving it (`consult_canon("no join")`).

Osiris uses this on **itself** — its own genesis is a subject in its graph.

### 2. The public record — entity intelligence

Point Osiris at open bases and it federates them into one provenance-graded entity graph,
keyless: SEC EDGAR (companies + Form D private placements), OpenSanctions (sanctions/PEP +
OFAC crypto wallets), Wikidata, GLEIF (global LEI + ownership), OrgBook BC, CourtListener,
ClinicalTrials.gov, and Etherscan (EVM on-chain — the one source needing a free key). It
resolves the same company across `cik:` / `Qxxx` / `lei:` / `bc-reg:`, grades every claim,
and emits a sourced, litigation-defensible dossier — identity, principals, financing,
litigation, on-chain + sanctions exposure — every line tagged `source · how-obtained · date`.

Same kernel, same resolution, same provenance as face #1. The domain is not the identity.

---

## Quickstart

### Track your projects (the flagship path)

```bash
uv sync
DATABASE_URL=postgresql://osiris:osiris@127.0.0.1:5432/osiris uv run alembic upgrade head
# seed the default rooms + lenses and ingest the design canon (idempotent — a fresh DB is
# otherwise an empty shell: no rooms, no compositions, no canon)
uv run python -m src.init
# ingest a repo WHOLE — history + file tree + mined decisions, one idempotent call
uv run python -m src.ingest.project /path/to/your/repo
# start the heartbeat over several repos (senses changes, builds the digest)
OSIRIS_DEV_REPOS=/path/a,/path/b uv run python -m src.orchestrator.pulse --watch 600
```

### Drive it with an AI

```bash
uv run python -m src.mcp_server        # stdio transport; add to your MCP client config
```
The repo ships a project-scoped [`.mcp.json`](.mcp.json), so Claude Code registers the `osiris`
server automatically — no manual config. Then, in any MCP client: *"What decisions have I made
across my repos, and what drifted in the family?"* — or, for the public-record face, *"Build a
dossier on Celsius Network."*

### Develop

```bash
cp .env.example .env && docker compose up -d && uv sync
DATABASE_URL=postgresql://osiris:osiris@127.0.0.1:5432/osiris uv run alembic upgrade head
uv run pytest            # real Postgres/Redis via testcontainers, never mocks
ruff check src/ tests/ && uv run mypy --strict src/
```
Python 3.12 (uv), async throughout (asyncpg, httpx, arq), FastAPI, Postgres 16 + Redis 7.
See [`CONTRIBUTING.md`](CONTRIBUTING.md).

---

## Proof

Validated on real, hard targets — not toy fixtures:

- **Osiris on itself** — its own git history is a subject in its graph; the `briefing`
  lens orients a fresh session on arrival, the family audit found a real license
  inconsistency across a sibling project family, and the heartbeat caught a live commit
  and mined its decision *unattended*, the second it landed.
- **Neuralink** — federated the entity, then surfaced the buried Form D financing swarm,
  the SPV operators, the clinical footprint, and a disclosed-vs-operational geography
  discrepancy — then *verified and killed* two tempting-but-false leads. The discipline
  working.
- **Sanctions fusion** — a live Etherscan trace of an OFAC-listed wallet fuses with the
  federated sanctions base on a shared on-chain canonical; screening surfaces a sanctioned
  counterparty it sent 34 ETH to.

[`samples/`](samples/) ships literal dossier output for the public-record face — real
dossiers, shown with their warts, because provenance is the point.

---

## What it can't do (yet, or by design)

- **It has no hands, on purpose.** It will not write to your repos, commit, or act in your
  systems. It produces the sourced finding; you (or Claude with a shell) apply it. Crossing
  into autonomous mutation of your systems is a
  [trap it deliberately refuses](ROADMAP.md#deliberately-not-done-and-why).
- **The public-record face reaches only the open *entity* commons** — by design (keyless),
  it is weak on private *persons*. A [safety feature, not only a limitation](RESPONSIBLE_USE.md).
- **Provenance makes errors auditable, not absent.** There is known entity-resolution and
  extraction noise — read the tiers and sources, don't trust blindly. See
  [`ROADMAP.md`](ROADMAP.md).
- The heartbeat's *reflection* layer (flash-tier Claude narrating the meaning of a change)
  is the active next step; today the pulse reports reliable facts, not yet interpretation.

---

## License & responsible use

Licensed under **AGPL-3.0** (see [`LICENSE`](LICENSE)). The public-record face produces
claims about real people and organizations; it is dual-use. Read
[`RESPONSIBLE_USE.md`](RESPONSIBLE_USE.md) — intended use, the data-source licenses (notably
OpenSanctions' non-commercial terms), and why "only public data" is both the ethic and the
legal shield.

*No warranty. Osiris surfaces and sources; it does not adjudicate truth. Every claim it
emits must be read with its provenance and verified before you act or publish.*
