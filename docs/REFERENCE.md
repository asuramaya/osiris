# Reference

Generated-from-code reference for Osiris. For the prose explanation see
[`../ARCHITECTURE.md`](../ARCHITECTURE.md); for the build plan see
[`../ROADMAP.md`](../ROADMAP.md).

## Data model

The graph is **objects** (entities) with **assertions** (graded facts) and **links**
(graded typed edges), all append-only in Postgres, with merges recorded as
**object_events** (truth) projected onto `objects.status / merged_into`.

| Object type | Canonical scheme | Minted by |
|-------------|------------------|-----------|
| `Organization` | `cik:NNNNNNNNNN` · `lei:<20>` · `bc-reg:<id>` · `Qxxxx` · `sec-org:<name>` · `company:<name>` · `ctgov-org:<name>` | edgar, gleif, orgbook, wikidata, edgar_formd, clinicaltrials |
| `Person` | `sec-person:<name>` · `ctgov-person:<name>` · `Qxxxx` | edgar_formd, clinicaltrials, wikidata |
| `CryptoAddress` | `eth:<chainid>:<addr>` (also OFAC wallets, fused) | etherscan, opensanctions |
| `CourtCase` | `courtlistener:<docket_id>` | courtlistener |
| `ClinicalTrial` | `nct:<NCTID>` | clinicaltrials |
| `Account` `Username` `Email` `Phone` `Domain` `URL` | canonicalized observable | footprint helpers |

Same `(type, canonical)` ⇒ same object (find-or-create dedup). Cross-base fusion merges
the rest: by normalized name, by **shared LEI** (deterministic), or via review-gated
probabilistic ER. A `Person` is never auto-merged.

## Evidence classes

Confidence is a **projection of how a fact was obtained**, not a parser's guess.

| Class | Base confidence | Meaning |
|-------|----------------|---------|
| `SELF_DECLARED` | 0.90 | the entity says so (rel=me, profile field, own commit email) |
| `AUTHORITATIVE_API` | 0.85 | an authoritative dataset/registry asserts it |
| `CORROBORATED` | 0.80 | **read-time only** — ≥2 independent sources agree |
| `DIRECT_OBSERVATION` | 0.60 | observed to exist (a fetched page, an on-chain tx) |
| `DERIVED` | 0.40 | inferred (handle from email local-part, format variant) |
| `CO_OCCURRENCE` | 0.35 | seen near the subject (mined from a snippet) |

`CORROBORATED` is computed at read time (storing it would go stale). The frontier and
the subject-report read tiers off these classes.

## Sources & capabilities (`src/orchestrator/sources.py` — the playbook as data)

`suggest(object_type)` returns the capabilities worth running on an object — both
surfaces (MCP + API) read this.

### Collect (federate a base)

| id | applies to | keyless | yields |
|----|-----------|:------:|--------|
| `wikidata` | Organization, Person | ✅ | founders/officers, official social accounts, relationship network |
| `edgar_formd` | Organization | ✅ | private financing rounds — officers, amounts, investor counts, feeder SPVs |
| `edgar_expand` | Person, Organization | ✅ | every filing mentioning an operator → their co-investment book |
| `gleif` | Organization | ✅ | LEI (deterministic global key) + jurisdiction + ownership parents |
| `bc_registry` | Organization | ✅ | BC registration #, CRA business #, type/status/jurisdiction (+ the family) |
| `litigation` | Organization, Person | ✅ | lawsuits & enforcement — dockets, parties, judges |
| `clinicaltrials` | Organization | ✅ | registered human trials — status, sites, investigators |
| `facility_cotenants` | Organization | ✅ | the other sponsors running trials at a clinical site |
| `footprint` | Username, Email, Account, Person, Domain, URL | ✅ | GitHub/social/web identifiers via the cascade |
| `etherscan` | CryptoAddress | ⚠️ keyed | an EVM address's top counterparties, balance, token flow, contract identity |

### Analyze (read-model lenses)

| id | applies to | yields |
|----|-----------|--------|
| `dossier` | Organization, Person | identity properties + the named relationship network |
| `discrepancy` | Organization | operational geography the disclosed home omits |
| `coinvestment` | Organization | other companies funded by SPVs sharing a (non-platform) operator |
| `subject_report` | Person, Account, Username, Email | who is this? — Verified / Corroborated / Speculative tiers |
| `sanctions_screen` | Person, Organization | name/identifier matches vs the ingested sanctions/PEP base |
| `sanctioned_wallet` | CryptoAddress | is this address or any counterparty an OFAC-listed wallet? + holder |
| `network_screen` | Organization, Person | is anyone in this entity's financing network on a watchlist? |

Data-source licenses (notably OpenSanctions **CC-BY-NC**) are in
[`../RESPONSIBLE_USE.md`](../RESPONSIBLE_USE.md).

## MCP tools (`src/mcp_server.py` — 18)

Each accepts a UUID **or** a name. Run: `uv run python -m src.mcp_server` (stdio).

| Tool | Kind | Does |
|------|------|------|
| `suggest_sources` | orient | the playbook for an object — what to collect/analyze next |
| `search` | orient | find objects by name substring |
| `aim_entity` | collect | Wikidata entity + relationships + official social accounts |
| `ingest_form_d` | collect | SEC Form D financing — officers, amounts, feeder SPVs |
| `expand_operator` | collect | every Form D mentioning a repeat-player → their portfolio |
| `lookup_lei` | collect | GLEIF LEI + jurisdiction + ownership parents |
| `verify_bc_entity` | collect | BC corporate registry (the entity + its corporate family) |
| `ingest_trials` | collect | ClinicalTrials.gov — status, sites, investigators |
| `ingest_litigation` | collect | CourtListener — dockets, parties, judges |
| `trace_wallet` | collect | Etherscan EVM trace — counterparties, balance, token flow |
| `expand_clinical_site` | collect | the other sponsors at a clinical facility |
| `consolidate` | hygiene | re-type mistyped entities + resolve cross-base merges + collapse variants |
| `dossier` | analyze | identity + named relationship network (JSON) |
| `discrepancy` | analyze | operational geography the disclosed home omits |
| `coinvestment` | analyze | co-investment ties (platform-filtered) |
| `screen_wallet` | analyze | is this address / a counterparty an OFAC-listed wallet? |
| `screen_financing_network` | analyze | is anyone in the financing network on a watchlist? |
| `dossier_report` | output | the provenance-annotated Markdown dossier (the deliverable) |

See [`../samples/`](../samples/) for real `dossier_report` + evidence outputs.

## The narrow waist (`src/actions/core.py`)

Every driver emits only through these; the kernel imports no driver.

```
create_or_find_object(type, canonical, actor, case_id?, hop_distance?) -> uuid
assert_property(object_id, name, value, source_id, observed_at, confidence, *, evidence_class?, ...) -> int
create_link(from_id, to_id, type, source_id, observed_at, confidence, *, properties?, evidence_class?, ...) -> int
merge_objects(winner_id, loser_id, justification, actor) -> None
set_status(object_id, status, justification, actor) -> None
```

All idempotent (find-or-create on canonical); background work claims via an atomic
partial-unique index on `helper_runs`; mutations flow to a durable `outbox`.

## Repo map

```
src/
  actions/        the kernel — event-sourced Actions API (the narrow waist)
  ontology/       canonicalize · entity_type (Person↔Org) · resolution (ER, cross-base, screening)
  parsers/        evidence taxonomy + per-source parsers
  ingest/         the federators (one module per open base) + their CLIs
  orchestrator/   sources (the playbook) · cascade · router · ratelimit · budgets ·
                  dossier · discrepancy · coinvest · enrich · monitor (the watch)
  dissemination/  dossier_report (the Markdown deliverable) · brief (PDF)
  connectors/     network seams (http clients, store, browser/leases [experimental])
  api/            FastAPI app (the human surface)
  workers/        Arq worker (background/cascade execution)
  mcp_server.py   the MCP server (the AI surface)
  lab/            offline frontier-policy research [experimental]
alembic/          migrations (sync psycopg)
helpers/          footprint helper manifests (YAML)
tests/            pytest-asyncio + testcontainers (real Postgres/Redis)
samples/          real generated dossiers + evidence exports
```
