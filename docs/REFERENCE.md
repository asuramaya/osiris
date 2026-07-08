# Reference

Generated-from-code reference for Osiris. For the prose explanation see
[`../ARCHITECTURE.md`](../ARCHITECTURE.md); for the build plan see
[`../ROADMAP.md`](../ROADMAP.md).

The graph is **objects** (entities) with **assertions** (graded facts) and **links**
(graded typed edges), all append-only in Postgres, with merges recorded as
**object_events** (truth) projected onto `objects.status / merged_into`. Same
`(type, canonical)` => same object (find-or-create dedup); cross-base fusion merges the
rest by normalized name, by **shared LEI** (deterministic), or via review-gated
probabilistic ER. A `Person` is never auto-merged.

<!-- BEGIN generated:schema (run `python -m src.ontology.schema`) -->
## Data model

Generated from `src/ontology/schema.py` — the declared semantic layer (the single source of truth). Do not edit by hand; run `python -m src.ontology.schema`.

### Entity object types

| Type | Canonical schemes | Description |
|------|-------------------|-------------|
| `Organization` | `cik:` · `lei:` · `bc-reg:` · `Q` · `sec-org:` · `company:` · `ctgov-org:` | A company, fund, agency, or other organization. |
| `Person` | `sec-person:` · `ctgov-person:` · `Q` · `subject:` · `cluster:` · `person:` · `dev:` | An individual — officer, director, investigator, developer, identity hub (resolved probabilistically; never auto-merged). |

### Asset object types

| Type | Canonical schemes | Description |
|------|-------------------|-------------|
| `CryptoAddress` | `eth:` · `wallet:` | An on-chain wallet/address (EVM, fused with OFAC designations). |
| `Property` | `harris-notice:` | A real-property parcel / foreclosure notice. |

### Record object types

| Type | Canonical schemes | Description |
|------|-------------------|-------------|
| `CourtCase` | `courtlistener:` | A litigation docket / opinion (parties, court, judge). |
| `ClinicalTrial` | `nct:` | A registered human trial (status, sites, investigators). |
| `ObservedData` | `ioc:` · `threatfox:` | Raw evidence — a scraped record / feed entry behind a claim. |

### Identity object types

| Type | Canonical schemes | Description |
|------|-------------------|-------------|
| `Account` | `github:` · `twitter:` · `linkedin:` · `instagram:` · `youtube:` · `facebook:` · `soundcloud:` · `replit:` · `gitlab:` · `pypi:` | A platform account (github:, twitter:, …) — a footprint fragment. |
| `Username` | — | A handle — connective tissue across platforms. |
| `Email` | — | An email-address observable. |
| `Phone` | — | A phone-number observable (enriched offline). |

### Web object types

| Type | Canonical schemes | Description |
|------|-------------------|-------------|
| `URL` | `http` · `https` | A web page / search hit / archived snapshot. |
| `Domain` | `about.me` | A DNS domain observable. |

### ThreatIntel object types

| Type | Canonical schemes | Description |
|------|-------------------|-------------|
| `IntrusionSet` | — | An actor cluster — tracked related intrusion activity. |
| `ThreatActor` | — | The human or group behind activity. |
| `Campaign` | — | Time-bounded activity attributed to an actor. |
| `Malware` | — | Malicious software — a capability an actor uses. |
| `Tool` | — | Legitimate/utility software used in operations. |
| `AttackPattern` | — | A technique (MITRE ATT&CK Txxxx) — HOW something is done. |
| `Indicator` | — | An IOC / detection signal — a hash, IP, or domain. |
| `CourseOfAction` | — | A mitigation / response to a technique (ATT&CK course-of-action). |
| `Tactic` | — | An ATT&CK tactic — the adversary's goal a technique serves. |
| `Identity` | — | A STIX identity — the named individual/org/sector behind activity. |

### Software object types

| Type | Canonical schemes | Description |
|------|-------------------|-------------|
| `SoftwareProject` | `repo:` | A software repository / project. |
| `Commit` | `commit:` | A version-control commit — an event in a project's history. |
| `Thread` | `thread:` | An open thread / wall / next-step — project memory of what's unresolved. |
| `Reference` | `ref:` | A design/reference document — external canon (Palantir/Notion) or own docs, ingested as project memory. |
| `Decision` | `decision:` | An architectural/design decision mined from the project's own commit rationale — the 'why', as queryable institutional memory. |
| `File` | `file:` | A tracked file in a repository (metadata only — content stays in git, read on demand). Its `role` lets analogous files be compared across repos. |
| `Agent` | `agent:` | A Claude instance operating over the graph — an analyst in the fleet. Carries its model (source-model provenance) and works in a project on behalf of a principal. 'A man and all his imaginary friends.' |
| `Tension` | `tension:` | A held POLARITY — two positions in productive tension, neither settled. Unlike a Decision (which settles) or a Thread (which closes), a tension is HELD: the current lean is recorded but never auto-resolved or consolidated away; the lean history is the dance across sessions. |

### Observable object types

| Type | Canonical schemes | Description |
|------|-------------------|-------------|
| `IPv4` | — | An IPv4 address observable. |
| `TelegramChannel` | — | A Telegram channel observable. |
| `FileHash` | — | A file hash observable (md5/sha1/sha256). |
| `Phrase` | — | Free text — a search seed. |

### Link types

| Link | Connects | Meaning |
|------|----------|---------|
| `controlled_by` | CryptoAddress → Person/Organization | Asset/wallet is controlled by a holder. |
| `owns` | Person/Organization → * | Owns the target. |
| `owned_by` | — | Is owned by the target. |
| `subsidiary_of` | Organization → Organization | Is a subsidiary of the target org. |
| `ultimate_parent` | Organization → Organization | Ultimate parent org (GLEIF level-2). |
| `founded_by` | Organization → Person | Founded by the target person. |
| `officer` | Organization → Person | Has the target as an officer. |
| `director` | Organization → Person | Has the target as a director. |
| `ceo` | Organization → Person | Has the target as CEO. |
| `chairperson` | Organization → Person | Has the target as chairperson. |
| `directs` | Person → Organization | Person directs the target org. |
| `promoter` | — | Promoter of the target. |
| `represents` | — | Legal/agent representation. |
| `associate_of` | Person → Person | Known associate. |
| `member_of` | Person → Organization | Membership in an organization. |
| `employs` | Organization → Person | Employment relationship. |
| `not_same_as` | — | Negative ER memory — confirmed distinct (suppresses re-match). |
| `family` | Person → Person | Familial relationship. |
| `sponsors` | Organization → ClinicalTrial | Sponsors the target (trial/event). |
| `investigator` | ClinicalTrial → Person | Investigator on the target trial. |
| `site` | ClinicalTrial → Organization | Trial site / facility. |
| `raises_for` | Organization → Organization | Feeder SPV raises capital for the core company. |
| `transacted_with` | CryptoAddress → CryptoAddress | On-chain counterparty (aggregated flow). |
| `litigation` | Organization/Person → CourtCase | Party/mention in a court case. |
| `appears_in` | — | Subject appears in the target record. |
| `has_account` | Person → Account | Identity hub has the target account. |
| `has_email` | Person → Email | Has the target email. |
| `has_url` | * → URL | Has/owns the target URL. |
| `has_domain` | * → Domain | Has/owns the target domain. |
| `has_subdomain` | Domain → Domain | Subdomain of. |
| `is_profile` | Account → * | Account is a profile of the subject. |
| `declares` | — | Self-declared social/owned link (rel=me, profile). |
| `committed_as` | * → Email | Commit-authored as the target email. |
| `derived_handle` | — | Handle derived from a local-part (speculative). |
| `co_occurs` | — | Co-occurs near the subject in a snippet (speculative). |
| `rel_me` | Account/Person → Account/URL | Self-declared identity link (rel=me / profile). |
| `spouse` | Person → Person | Spouse. |
| `registered_with` | — | Registered with the target authority/registry. |
| `related_to` | — | Generic association — the specific kind (e.g. an AI-extracted relationship) is kept in the link's `relation` property. |
| `search_variant` | — | A search/handle variant. |
| `linked_to` | — | Generic association. |
| `has_observation` | * → ObservedData | Points at raw observed evidence. |
| `acts_for` | Agent → Person | Agent acts on behalf of the principal (AUTHORITY). |
| `works_in` | Agent → SoftwareProject | Agent operates in the target project. |
| `spawned_by` | Agent → Agent | Sub-agent was spawned by (delegated from) its direct parent agent — the fractal DELEGATION tree, distinct from acts_for (authority). |
| `succeeded_from` | Agent → Agent | Minted heir → its ancestor (ruling be292762): a fresh context arriving across a succession seam or wearing a retired face is MINTED its own lineage-linked id (agent:<base>-ii…) instead of writing under the dead name — SUCCESSION, distinct from spawned_by (delegation). |
| `archived_snapshot` | * → URL | A Wayback/archive snapshot of the target. |
| `same_as` | — | Identity merge edge (loser → winner). |
| `uses` | — | Actor/source employs this capability or technique. |
| `indicates` | Indicator → * | Indicator points at the linked malware/technique. |
| `based-on` | Indicator → * | Indicator derived from raw observed evidence. |
| `subtechnique-of` | AttackPattern → AttackPattern | A more specific technique under a broader one. |
| `authored_by` | Commit → Person | Commit authored by a developer. |
| `in_repo` | Commit/File → SoftwareProject | Commit or file belongs to a repository. |
| `follows` | Commit → Commit | Commit follows its parent (the history DAG). |
| `noted_in` | Thread → Commit | A thread / wall surfaced in this commit's rationale. |
| `resolved_by` | Thread → Commit | The later commit that addressed this thread (closes it). |
| `cites` | Reference → Reference | This document cites / draws from that reference. |
| `informs` | Reference → SoftwareProject/Commit | This reference grounds / informs that artifact. |
| `mentions` | Reference/Commit → Organization/Person | This document names that entity in its text (the doc joins the graph). |
| `decided_in` | Decision → Commit | Decision was stated in this commit (the 'why', sourced). |
| `supersedes` | Decision → Decision | This decision overrides/replaces an earlier one. |
| `grounded_by` | Decision → Reference | Decision is grounded by this design reference (the canon). |
<!-- END generated:schema -->

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
                  dossier · discrepancy · coinvest · enrich · monitor (the watch) ·
                  watchers (source pullers) · compose (doc→lead) · satellite (placeful)
  dissemination/  dossier_report (the Markdown deliverable) · brief (PDF)
  connectors/     network seams (http clients, store, browser/leases [experimental])
  api/            FastAPI app (the human surface)
  workers/        Arq worker — enqueued jobs (expand_case_job) + crons (cascade drain,
                  watch evaluate/tick, stale-run reaper). Fate-isolated from the API.
  mcp_server.py   the MCP server (the AI surface)
  lab/            offline frontier-policy research [experimental]
alembic/          migrations (sync psycopg)
helpers/          footprint helper manifests (YAML)
deploy/           systemd units (api/worker) + env example; see docs/DEPLOY.md
tests/            pytest-asyncio + testcontainers (real Postgres/Redis)
samples/          real generated dossiers + evidence exports
```
