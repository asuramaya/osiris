# Osiris

**A keyless, provenance-first entity engine for following the money through the public record — and an AI can drive it.**

Paste a name. Osiris federates the open public record — corporate filings, sanctions
lists, court dockets, the global company registry, on-chain activity — into one
entity graph where **every fact carries its source, how it was obtained, and when**,
then emits a sourced, litigation-defensible dossier.

It is built so that a single person, with no subpoena, no LexisNexis, and no API keys
they have to pay for, can assemble the public record into a case faster than they could
by hand — and so that an AI assistant can do it for them.

> **Status: v0.1.0 — first public release.** The engine is real and proven (see
> [Proof](#proof-so-far)). It is also honest about its edges (see
> [What it can't do yet](#what-it-cant-do-yet)). It was built by one operator with an AI
> pair; the development log is kept in the open in [`CLAUDE.md`](CLAUDE.md).

---

**What it does today**
- Federates **keyless open bases** into one provenance-graded entity graph: SEC EDGAR
  (companies + Form D private placements), OpenSanctions (sanctions/PEP + OFAC crypto
  wallets), Wikidata, GLEIF (global LEI registry + ownership), OrgBook BC (Canadian
  corporate registry), CourtListener (federal litigation), ClinicalTrials.gov, and
  Etherscan (EVM on-chain traces — the one source that needs a free key).
- **Resolves entities across those bases** — the same company fragmented as `cik:`,
  `Qxxx`, `lei:`, `bc-reg:` is fused; a person and an org are told apart; a shared LEI is
  a deterministic merge.
- **Grades every claim** by *how it was obtained* (self-declared / authoritative-API /
  direct-observation / co-occurrence / derived / corroborated) — confidence is a
  projection of provenance, not a guess.
- **Produces a sourced dossier**: identity, principals, financing (Form D feeders),
  litigation, footprint discrepancy, co-investment, on-chain + sanctions exposure —
  every line tagged `source · how-obtained · date`.
- **Screens a network**: is anyone in this entity's financing network — its principals,
  feeder funds, fund operators — on a sanctions/PEP list?
- Is **drivable by an AI** through an [MCP](https://modelcontextprotocol.io) server (18
  tools) *and* by a human through a FastAPI app.

**What it wants to do** (see [`ROADMAP.md`](ROADMAP.md))
- Turn from a **lens** (investigate on demand) into a **tripwire** (watch a stream of
  public events and fire a sourced lead when something matches) — the monitoring /
  "cron" capability that serves brokers, compliance, and standing intelligence.
- Pierce shell ownership end-to-end (deed → LLC → human) wherever open registries allow.
- Add on-chain coverage beyond EVM (Solana), and document-level extraction (OCR + AI)
  for the messy long tail.

**What it can't do yet**
- It only reaches the open **entity** commons. By design (keyless), it is weak on private
  *persons* — it federates companies, filings, PEPs, and wallets, not your neighbour.
  This is a [safety feature, not only a limitation](RESPONSIBLE_USE.md).
- No property/county-records connectors yet; no always-on monitoring yet; the human web
  UI is partial (the MCP surface is ahead of it).
- The deliverable still has known data-quality noise (entity-resolution and geocoding
  edge cases). Provenance makes errors *auditable*, not *absent* — read tiers and
  sources, don't trust blindly. See [`ROADMAP.md`](ROADMAP.md#known-noise).

---

## Who it's for

The same capability — *resolve an entity, map its network from the public record, attach
provenance* — is one sword held by many hands: the investigative journalist following
grift, the investor doing diligence, the analyst piercing a shell, the compliance team
screening a counterparty. Osiris is the sword, not any one hand.

The first hand we built for, and the one the documentation speaks to, is the
**independent follow-the-money investigator** — because they publish, they credit their
tools, and they need every claim to survive a defamation threat. The provenance kernel is
built for exactly that.

---

## Quickstart

### Drive it with an AI (the short path)

Osiris exposes its capabilities as an MCP server. Point any MCP client (Claude Desktop,
Claude Code, an agent) at it and ask it to investigate something:

```bash
uv sync
# bring up Postgres + Redis (see docker-compose.yml)
DATABASE_URL=postgresql://osiris:osiris@127.0.0.1:5432/osiris uv run alembic upgrade head
uv run python -m src.mcp_server        # stdio transport; add to your MCP client config
```

Then: *"Use Osiris to build a dossier on Celsius Network"* — the assistant calls
`search → suggest_sources → ingest_* → screen_financing_network → dossier_report` and
hands you a sourced case file.

### Run it as a library / CLI

Every connector is also a module:

```bash
uv run python -m src.ingest.edgar_formd "Neuralink"          # SEC Form D financing
uv run python -m src.ingest.gleif "Tesla, Inc."              # global LEI + ownership
uv run python -m src.ingest.courtlistener "Celsius Network"  # litigation
ETHERSCAN_API_KEY=... uv run python -m src.ingest.etherscan 0xADDRESS   # on-chain trace
```

### Develop

```bash
cp .env.example .env
docker compose up -d
uv sync
DATABASE_URL=postgresql://osiris:osiris@127.0.0.1:5432/osiris uv run alembic upgrade head
uv run pytest            # real Postgres/Redis via testcontainers, never mocks
ruff check src/ tests/   # lint
uv run mypy --strict src/
```

Python 3.12 (uv), async throughout (asyncpg, httpx, arq), FastAPI, Postgres 16 + Redis 7,
Alembic migrations. See [`CONTRIBUTING.md`](CONTRIBUTING.md).

---

## Proof so far

The engine is validated by pointing it at real, hard targets and following the money
end-to-end — not by toy fixtures:

- **Neuralink** — federated the entity, then surfaced the buried Form D financing swarm
  (a long tail of named SPVs funnelling small global investors into a company most assume
  is closed), the SPV operators, the clinical-trial footprint, and a footprint
  discrepancy between disclosed and operational geography. Then *verified and killed* two
  tempting-but-false leads (a "China capital" thread that resolved to a Vancouver firm) —
  the discipline working.
- **Celsius / Mashinsky** — a documented collapse where the financing *is* the story:
  the Form D officers resolve to the criminally-charged principals; the litigation is the
  bankruptcy and clawback suits; one clean sourced dossier, AI-driven.
- **Sanctions fusion** — a live Etherscan trace of an OFAC-listed wallet (CHATEX,
  Andariel/DPRK) fuses with the federated sanctions base on a shared on-chain canonical,
  and screening surfaces a sanctioned counterparty it sent 34 ETH to — the crawl × base
  edge, with provenance.

237 tests green (real Postgres/Redis), `ruff` + `mypy --strict` clean.

---

## How it's built

A small, stable **kernel** (event-sourced, evidence-graded, merge-aware entity graph)
with a ring of **drivers** (the federators) that only ever *emit* into it through a narrow
interface, and two **surfaces** over it (the MCP server and the FastAPI app). The kernel
imports no driver; drivers depend on the kernel. That seam is what lets the messy,
ever-growing collection layer accrete without destabilizing the core.

See [`ARCHITECTURE.md`](ARCHITECTURE.md) for the current shape and
[`DESIGN.md`](DESIGN.md) for the original deep design.

---

## License & responsible use

Licensed under **AGPL-3.0** (see [`LICENSE`](LICENSE)). Osiris produces claims about real
people and organizations from public records. It is a dual-use tool: read
[`RESPONSIBLE_USE.md`](RESPONSIBLE_USE.md) before you use it — it covers intended use, the
data-source licenses (notably OpenSanctions' non-commercial terms), and why "only public
data" is both the ethic and the legal shield.

*No warranty. Osiris surfaces and sources the public record; it does not adjudicate truth.
Every claim it emits must be read with its provenance and verified before you act or
publish.*
