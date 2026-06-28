# Roadmap

This roadmap is public on purpose. Osiris is built as a sequence of **proofs around a
kernel** — each one validates a single capability by pointing the engine at a real,
hard target, not a toy. The persona is never the deliverable; the *capability the persona
forces* is. This document is the honest map of what's proven, what's next, and what is
deliberately not done.

## The shape — "Palantir × Notion, composed by conversation"

Osiris is an entity-ontology **substrate** (Palantir's ontology, on the open public
record, with provenance and a 24/7 tripwire) whose front end is a **composer** (Notion's
neutral primitives + the user's own composed opinion), authored by an **AI** (Claude over
MCP — the front end is the conversation). The intelligence is Claude; Osiris is what an
intelligence lacks: durable, structured, sourced memory that runs when you're away. The
composer is the open-source answer to Palantir's forward-deployed engineers — Claude
composes the ontology from a sentence instead of a consulting army.

**The active arc — finish the composer** (see [`docs/COMPOSER.md`](docs/COMPOSER.md)). A
composition is a saved, forkable op-tree over the graph; the op vocabulary is a small
*closed* set (grounded in Palantir's Object Set API + Notion's rollups) plus a Function
escape hatch. The work, each a cut: complete the ops (`aggregate`/`order`/`union`/
`intersect`) → evict the opinionated read-models (`discrepancy` is already a composition)
so opinion lives in forkable specs the user owns → fold the watch and the lens into one
primitive → a generic renderer → the composer becomes the front end and the console pages
are cut. Done = the engine holds no opinion.

## The capability ladder

Each "holder" of the tool forces the kernel to grow one capability. We climb the ladder
one rung at a time, and each rung is a demonstrable proof.

| Rung | Holder | Capability it forces | Status |
|------|--------|----------------------|--------|
| **Convergence** | investigative journalist | resolve disparate public facts into one sourced picture, on demand — the **lens** | ✅ **proven** (Neuralink, Celsius) |
| **Persistence** | broker / monitor | watch a stream of public events and fire a sourced lead when something matches — the **tripwire** | ✅ **built** (cron ladder; not yet live for a user) |
| **Correlation at scale** | analyst / compliance | fire *accurately* on a whole population, not just on a target | ○ planned |
| **Standing system** | — | a live, self-updating entity graph that surfaces emergent patterns | ○ horizon |

The lens is **retrospective** ("who is this / what happened"); the tripwire is
**prospective** ("tell me when X happens"). Convergence + persistence is the line between
an *investigation tool* and an *intelligence system* — and the half that the open-source
world doesn't have.

## Next: persistence (the "cron" / monitoring capability)

The broker proof is not about real estate; it is the proof that the kernel can **watch**,
not just **resolve**. Build order (each step proves one thing and leaves a clean seam —
and we never combine two new hard things in one step, which is the mistake that sinks
collection-first projects):

1. **Resurrect the watch, source-agnostic** — scheduler (Arq cron) + per-source watermark
   + a **subscription evaluator** that drains the durable outbox, matches new graph
   mutations against saved criteria, and emits an alert. The one new primitive.
2. **Worker ⊥ surface cut** — isolate the worker process from the API so a runaway
   monitoring job can never take down the console or corrupt the truth. The deployment
   foundation. *Proof: a failure drill.*
3. **Broker PoC on easy collection** — prove the full loop (schedule → delta → ingest →
   resolve → trigger → sourced alert) on a source that already works (new filings,
   dockets, sanctions deltas). *And prove it stays quiet on noise.*
4. **AI-extraction driver, alone** — prove that one messy document → graded entities with
   no bespoke parser, collapsing the per-source parser tax.
5. **Compose** — cron watches a document source → AI-extracts → emits graded → resolves →
   fires a sourced lead. This is "document → lead, done right."

The reliability this needs — idempotent emit, atomic claim, durable outbox, global rate
limits — is already in the kernel. The monitoring capability *inherits* it. That is the
payoff of building the kernel first.

## Deployment, as a sequence of cuts (not a rewrite)

The architecture separates by **placefulness, blast radius, and trust zone**, and the
rings coordinate through Postgres + Redis — not an RPC mesh. See
[`ARCHITECTURE.md`](ARCHITECTURE.md).

- **Today** — all on one box, bound to localhost. Correct for a single operator.
- **Cut 1** (when crons land) — the worker becomes its own process, fate-isolated from the
  API. Same box, separate units.
- **Cut 2** (when a pool bites) — worker pools by resource type (light federators / heavy
  extractors / vantage-bound browsers).
- **Cut 3** (when adoption forces multi-user) — managed Postgres/Redis, scaled surfaces,
  a secret manager, object-store artifacts, and a thin local **agent** the hosted kernel
  dispatches placeful (antibot / session-bound) collection to.

The **lens** can run anywhere, on demand; the **tripwire** forces an always-on hosted
tier. Each rung of the capability ladder is also a rung of deployment commitment.

## Adoption

- **MCP first.** "Add Osiris to your AI assistant and tell it to investigate X" is the
  primary distribution path — lowest effort, best reach, already built.
- **Hosted try-it demo** second, once the deliverable is clean enough that a stranger's
  first dossier is trustworthy.
- **Human web UI** last — it sits downstream of a still-moving engine and would thrash if
  built now.

## Deliberately *not* done (and why)

- **County / property-record crawling at scale.** Two prior projects died trying to
  brute-crawl ~3,000 fragmented county systems. We don't re-enter that grave. The strategy
  is *federate finished bases* + targeted, human-in-the-loop, AI-navigated lookups for the
  last mile — never speculative mass-scrape.
- **Acronym/fuzzy auto-merge of distinct entities.** Token-overlap matching (e.g. merging
  every "Neuralink" SPV because they share the word) creates false merges, and a false
  merge is worse than no merge. Resolution stays deterministic-or-review-gated.
- **De-anonymizing private individuals.** The keyless constraint structurally points the
  tool at the open entity commons, not at private persons. We keep it that way on purpose.

## Known noise

Honesty about the current deliverable's rough edges (all visible *with* their provenance,
which is the point — errors are auditable, not hidden):

- Entity resolution leaves name variants and cross-base fragments that need a review pass.
- Some geographic/relationship inference has edge-case false positives.
- US beneficial-ownership data is largely a **policy wall** (FinCEN BOI is not public);
  shell-piercing works best where open registries exist (UK PSC, EU, GLEIF).
- Coverage is broad but in places shallow — many connectors are proven on one or two real
  targets, not hardened across the long tail.

For an *adoption* goal, trust is the whole moat, so closing this noise is treated as
existential work, not polish — especially before the monitoring capability turns a quiet
footnote into a 3am false alert.
