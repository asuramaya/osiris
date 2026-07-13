# Roadmap

This roadmap is public on purpose. Osiris is built as a sequence of **proofs around a
kernel** — each one validates a single capability by pointing the engine at a real,
hard target, not a toy. The persona is never the deliverable; the *capability the persona
forces* is. This document is the honest map of what's proven, what's next, and what is
deliberately not done.

## The shape — "Palantir × Notion, composed by conversation"

Osiris is an entity-ontology **substrate** (Palantir's ontology, with provenance and a
24/7 loop) whose front end is a **composer** (Notion's neutral primitives + the user's own
composed opinion), authored by an **AI** (Claude over MCP — the front end is the
conversation). The intelligence is Claude; Osiris is what an intelligence lacks: durable,
structured, sourced memory that runs when you're away. The composer is the open-source
answer to Palantir's forward-deployed engineers — Claude composes the ontology from a
sentence instead of a consulting army.

**The engine serves two corpora, unchanged** — *your own work* (git history, decisions,
the file tree) and *the public record*. The developer/project-memory face is the flagship
(and the most-dogfooded: Osiris tracks its own genesis inside itself); the public-record
face is entity intelligence. The domain is never the identity, the engine is.

**The principle that bounds it: no hands.** Osiris reads and tells; it never mutates your
systems (writes a file, commits, sends). It produces the sourced finding; *you*, or Claude
with a shell, or `git`, apply it. Crossing into autonomous action over the user's real
systems is a trap the engine deliberately refuses — the moat is the memory and the
provenance, not a robot arm, and blast radius would destroy the trust that is the whole
point.

**The composer shipped.** The op vocabulary is a small *closed* set (grounded in Palantir's
Object Set API + Notion's rollups) plus a Function escape hatch (see
[`docs/COMPOSER.md`](docs/COMPOSER.md)); the opinionated read-models are Functions a
forkable composition references; the front end is the composer. **The active arc is the
developer persona and the living memory** — the pulse that senses your work and, next, a
flash-tier reflection layer that narrates what a change *means*.

## The capability ladder

Each "holder" of the tool forces the kernel to grow one capability. We climb the ladder
one rung at a time, and each rung is a demonstrable proof.

| Rung | Holder | Capability it forces | Status |
|------|--------|----------------------|--------|
| **Convergence** | journalist / developer | resolve disparate facts into one sourced picture, on demand — the **lens** | ✅ **proven** (Neuralink, Celsius; your repos → decisions, threads, the family audit) |
| **Persistence** | developer / broker | sense a stream of events the moment it changes and accumulate it — the **tripwire / the pulse** | ✅ **proven** (the heartbeat catches a live commit + mines its decision, unattended) |
| **Reflection** | developer | narrate what a change *means*, not just that it happened — flash-tier Claude in the loop | ○ **active** |
| **Correlation at scale** | analyst / compliance | fire *accurately* on a whole population, not just a target | ○ planned |
| **Standing system** | — | a live, self-updating graph that surfaces emergent patterns | ○ horizon |

The lens is **retrospective** ("who is this / what happened"); the tripwire is
**prospective** ("tell me when X happens"). Convergence + persistence is the line between
an *investigation tool* and an *intelligence system* — and the half that the open-source
world doesn't have.

## Shipped: persistence (the "cron" / the pulse)

The kernel can **watch**, not just **resolve** — proven end to end. The full ladder
landed: a source-agnostic scheduler + per-source watermark + a subscription evaluator that
drains the durable outbox and emits alerts; the worker ⊥ surface cut (a runaway job can't
take down the console) with a self-healing reaper; a real source watcher; the
AI-extraction driver (one messy document → graded entities, no bespoke parser); and the
composed *document → sourced lead* pipeline. For the developer face this became the
**pulse** — an autonomic loop that senses a repo's HEAD moving, re-ingests the delta,
re-runs the lenses, and accumulates a "what changed while you were away" digest. It caught
a live commit and mined its decision, unattended. All of it inherited the kernel's
reliability (idempotent emit, atomic claim, durable outbox) — the payoff of building the
kernel first.

## Next: the living memory (reflection)

The pulse today reports reliable **facts** (N new commits, new drift, a new decision). The
active work is the **reflection** layer — a flash-tier Claude (Haiku, headless, keyless via
the local CLI) that runs in the loop and narrates what a change *means*: "you're porting
this pattern across the family," "this decision contradicts one in another repo," "you've
solved this problem three different ways." Deterministic facts stay the trusted floor;
reflection is a graded, speculative lead you glance at. This is the turn from *current* to
*insightful* — off-the-clock insight, not just off-the-clock detection.

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
- **Acting over the user's systems (no hands).** Osiris will not write to your repos,
  commit, send, or automate a mutation of your real systems. It produces the sourced
  finding; you or Claude-with-a-shell apply it. This isn't a missing feature — it's a
  refused one: the moat is the memory and the provenance, not a robot arm, and an
  autonomous mutator you have to *watch* is the opposite of the trust the tool is for.

  **The honest footnote:** there is one outward act, and we declare it rather than hide it
  behind the word "never". The **wake trigger** can start a Claude session in a repo with
  unread fleet mail — Osiris summons a mind, it does not become one. It ships **off**, it is
  bounded by *lifetime* attempts per message (a rate is not a bound — we learned that the
  expensive way, and the story is in [`README.md`](README.md#one-hand-the-wake-trigger)), and
  every wake is recorded. The line we hold is the *last inch*: nothing Osiris starts inherits
  a shell or write access unless you grant it.

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
