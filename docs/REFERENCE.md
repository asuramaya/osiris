<!-- topic: reference -->

# Reference

Generated-from-code reference for Osiris. For the prose explanation see
[`../ARCHITECTURE.md`](../ARCHITECTURE.md); for the build plan see
[`../ROADMAP.md`](../ROADMAP.md).

The graph is **objects** (entities) with **assertions** (graded facts) and **links**
(graded typed edges), all append-only in Postgres, with merges recorded as
**object_events** (truth) projected onto `objects.status / merged_into`. The same
`(type, canonical)` pair always resolves to the same object (find-or-create dedup);
merges across separate data sources happen by normalized name, by shared LEI
(deterministic), or through review-gated probabilistic entity resolution. A `Person`
object is never merged automatically.

<!-- osiris:compiled:begin v=schema-v1 -->
## Data model

Generated from `src/ontology/schema.py`, the declared semantic layer and single source of
truth. Do not edit by hand; run `python -m src.ontology.schema`.

### Entity object types

| Type | Canonical schemes | Description |
|------|-------------------|-------------|
| `Organization` | `cik:` · `lei:` · `bc-reg:` · `Q` · `sec-org:` · `company:` · `ctgov-org:` | A company, fund, agency, or other organization. |
| `Person` | `sec-person:` · `ctgov-person:` · `Q` · `subject:` · `cluster:` · `person:` · `dev:` | An individual: officer, director, investigator, developer, or identity hub (resolved probabilistically; never merged automatically). |

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
| `ObservedData` | `ioc:` · `threatfox:` | Raw evidence: a scraped record or feed entry behind a claim. |

### Identity object types

| Type | Canonical schemes | Description |
|------|-------------------|-------------|
| `Account` | `github:` · `twitter:` · `linkedin:` · `instagram:` · `youtube:` · `facebook:` · `soundcloud:` · `replit:` · `gitlab:` · `pypi:` | A platform account (github:, twitter:, etc.): a footprint fragment. |
| `Username` | n/a | A handle: connective tissue across platforms. |
| `Email` | n/a | An email-address observable. |
| `Phone` | n/a | A phone-number observable (enriched offline). |

### Web object types

| Type | Canonical schemes | Description |
|------|-------------------|-------------|
| `URL` | `http` · `https` | A web page / search hit / archived snapshot. |
| `Domain` | `about.me` | A DNS domain observable. |

### ThreatIntel object types

| Type | Canonical schemes | Description |
|------|-------------------|-------------|
| `IntrusionSet` | n/a | An actor cluster: tracked related intrusion activity. |
| `ThreatActor` | n/a | The human or group behind activity. |
| `Campaign` | n/a | Time-bounded activity attributed to an actor. |
| `Malware` | n/a | Malicious software: a capability an actor uses. |
| `Tool` | n/a | Legitimate/utility software used in operations. |
| `AttackPattern` | n/a | A technique (MITRE ATT&CK Txxxx): how something is done. |
| `Indicator` | n/a | An IOC / detection signal: a hash, IP, or domain. |
| `CourseOfAction` | n/a | A mitigation / response to a technique (ATT&CK course-of-action). |
| `Tactic` | n/a | An ATT&CK tactic: the adversary's goal a technique serves. |
| `Identity` | n/a | A STIX identity: the named individual/org/sector behind activity. |

### Software object types

| Type | Canonical schemes | Description |
|------|-------------------|-------------|
| `SoftwareProject` | `repo:` | A software repository / project. |
| `Commit` | `commit:` | A version-control commit: an event in a project's history. |
| `Thread` | `thread:` | An open thread, wall item, or next step: project memory of what's unresolved. |
| `Reference` | `ref:` | A design/reference document: external canon or this project's own docs, ingested as project memory. |
| `Decision` | `decision:` | An architectural/design decision mined from a project's own commit rationale: the reasoning behind a change, kept as queryable institutional memory. |
| `File` | `file:` | A tracked file in a repository (metadata only; content stays in git and is read on demand). Its `role` lets similar files be compared across repos. |
| `Agent` | `agent:` | An AI agent session operating over the graph, working in a project on behalf of a principal. Carries its source model as provenance. |
| `Tension` | `tension:` | A held polarity: two positions in productive tension, neither one settled. Unlike a Decision (which settles something) or a Thread (which closes), a Tension stays open: its current lean is recorded but never auto-resolved, and its history of leaning one way or another over time is kept. |
| `BlindSpot` | `blindspot:` | A project's registered blind spot: something its own test/verification setup cannot check from where it runs, and a note on where real verification actually happens (one recorded case: hundreds of headless-browser tests passed while every mobile device was actually broken). Held like a Tension, as a stable per-project fact that's never resolved away, and surfaced at `orient()` so a session knows the limits of its own test coverage before trusting a passing test run. |
| `Superstition` | `superstition:` | A dead workaround: a practice that a since-fixed bug once justified, explicitly retired by name once the underlying fix landed. The lesson behind a workaround often outlives the bug that caused it, so `record_decision(obsoletes=[...])` retires these explicitly, and `orient()` announces recent retirements project-wide so anyone still following the old workaround is told to stop. |
| `Practice` | `practice:` | A transferable technique: the positive counterpart to a Superstition, closing a gap where lessons learned in one place were not reaching anyone else (one case: the same install step gotcha was independently rediscovered by two separate teams in the same hour because nothing durable held the lesson, only markdown files nobody else's search reached). Shape: `statement` (one imperative line), `failure_prevented` (the concrete symptom, findable mid-failure), and `surface` (matching a BlindSpot's domain vocabulary). A Practice is timeless, not tied to a moment: unlike a Decision ("we chose X here, at this time"), a Practice is true regardless of repo or date. Its `confirmed` count is derived from `witnesses` links at read time, never stored as an incrementing counter, to avoid a read-then-write race under concurrent updates. A refuted Practice converts to a Superstition (`record_decision(refutes=...)`), but the original Practice stays active and carries a `refuted_by` flag rather than being retired outright, so a half-remembered but now-refuted lesson stays findable, with the flag attached. |
| `Reflection` | `reflection:` | A memory kept for its own sake: open-ended or reflective conversation, kept as exactly what it is, remembered and queryable but never treated as actionable. No work surface (briefing, wall, backlog, duty extraction) may present it as a task, and nothing resolves or closes it, because there is nothing to resolve. |
| `Message` | `message:` | A piece of inter-agent mail or a direct message, graphed as the postal layer between agents. Messages carry `sent_by`/`addressed_to`/`broadcast_to`/`replies_to` edges and can `mentions` decisions, threads, or agents for context that's traversable in the graph. Written once and never mutated, only read and acknowledged. The underlying message-delivery table is the operational record; the Message object is what makes mail traversable as part of the graph. |
| `Seat` | `seat:` | A durable role within a project or team: the fleet's addressable identity. Minted exactly once as `seat:<uuid8>` and never re-keyed: its handle, its project/team, and its working-directory anchor are all mutable assertions layered on top of it, because the underlying bug class this fixes was keying identity on mutable facts like path or session id. Individual agent sessions hold a Seat in succession via `holds`; the Seat outlives all of them, and exists before its first session ever starts, which is what lets the manager daemon export its identity at process start. |

### Observable object types

| Type | Canonical schemes | Description |
|------|-------------------|-------------|
| `IPv4` | n/a | An IPv4 address observable. |
| `TelegramChannel` | n/a | A Telegram channel observable. |
| `FileHash` | n/a | A file hash observable (md5/sha1/sha256). |
| `Phrase` | n/a | Free text: a search seed. |

### Link types

| Link | Connects | Meaning |
|------|----------|---------|
| `controlled_by` | CryptoAddress → Person/Organization | Asset/wallet is controlled by a holder. |
| `owns` | Person/Organization → * | Owns the target. |
| `owned_by` | n/a | Is owned by the target. |
| `subsidiary_of` | Organization → Organization | Is a subsidiary of the target org. |
| `ultimate_parent` | Organization → Organization | Ultimate parent org (GLEIF level-2). |
| `founded_by` | Organization → Person | Founded by the target person. |
| `officer` | Organization → Person | Has the target as an officer. |
| `director` | Organization → Person | Has the target as a director. |
| `ceo` | Organization → Person | Has the target as CEO. |
| `chairperson` | Organization → Person | Has the target as chairperson. |
| `directs` | Person → Organization | Person directs the target org. |
| `promoter` | n/a | Promoter of the target. |
| `represents` | n/a | Legal/agent representation. |
| `associate_of` | Person → Person | Known associate. |
| `member_of` | Person → Organization | Membership in an organization. |
| `employs` | Organization → Person | Employment relationship. |
| `not_same_as` | n/a | Negative entity-resolution memory: confirmed distinct, suppresses re-matching. |
| `family` | Person → Person | Familial relationship. |
| `sponsors` | Organization → ClinicalTrial | Sponsors the target (trial/event). |
| `investigator` | ClinicalTrial → Person | Investigator on the target trial. |
| `site` | ClinicalTrial → Organization | Trial site / facility. |
| `raises_for` | Organization → Organization | Feeder SPV raises capital for the core company. |
| `transacted_with` | CryptoAddress → CryptoAddress | On-chain counterparty (aggregated flow). |
| `litigation` | Organization/Person → CourtCase | Party/mention in a court case. |
| `appears_in` | n/a | Subject appears in the target record. |
| `has_account` | Person → Account | Identity hub has the target account. |
| `has_email` | Person → Email | Has the target email. |
| `has_url` | * → URL | Has/owns the target URL. |
| `has_domain` | * → Domain | Has/owns the target domain. |
| `has_subdomain` | Domain → Domain | Subdomain of. |
| `is_profile` | Account → * | Account is a profile of the subject. |
| `declares` | n/a | Self-declared social/owned link (rel=me, profile). |
| `committed_as` | * → Email | Commit-authored as the target email. |
| `derived_handle` | n/a | Handle derived from a local-part (speculative). |
| `co_occurs` | n/a | Co-occurs near the subject in a snippet (speculative). |
| `rel_me` | Account/Person → Account/URL | Self-declared identity link (rel=me / profile). |
| `spouse` | Person → Person | Spouse. |
| `registered_with` | n/a | Registered with the target authority/registry. |
| `related_to` | n/a | Generic association: the specific kind (for example, an AI-extracted relationship) is kept in the link's `relation` property. |
| `search_variant` | n/a | A search/handle variant. |
| `linked_to` | n/a | Generic association. |
| `has_observation` | * → ObservedData | Points at raw observed evidence. |
| `acts_for` | Agent → Person | Agent acts on behalf of the principal (authority). |
| `works_in` | Agent → SoftwareProject | Agent operates in the target project. |
| `spawned_by` | Agent → Agent | A sub-agent was spawned by (delegated from) its direct parent agent: the delegation tree, distinct from `acts_for` (authority). |
| `sent_by` | Message → Agent | Message was sent by an agent (provenance). |
| `addressed_to` | Message → Agent | Direct message was addressed to this specific agent. |
| `broadcast_to` | Message → SoftwareProject | Message was broadcast to a project channel. |
| `replies_to` | Message → Message | Message replies to an earlier message (reply chain). |
| `in_thread` | Message → Thread | Message belongs to a persistent work thread. |
| `succeeded_from` | Agent → Agent | A newly minted successor session → the session it succeeded: when a session picks up after a model swap or a fresh context, it gets its own lineage-linked id rather than continuing to write under the old session's name. Distinct from `spawned_by` (delegation), this edge is about succession. |
| `forked_from` | SoftwareProject → SoftwareProject | Successor project → its ancestor project: the declared shape for treating a new, independent sibling project as a fork rather than a merge. Two objects, each keeping its full independent history, connected by one edge instead of being collapsed into one. Mirrors `succeeded_from`'s successor-to-predecessor direction at the Agent level, but moves no data the way a merge does: `in_repo`/`works_in`/`governs` edges on both sides stay exactly where they were. Minted only by an explicit fork action, never inferred from evidence alone. |
| `succeeds_seat` | Agent → Agent | Holder → the agent session that held this Seat immediately before it. Distinct from `succeeded_from`, which tracks succession of the same conversation/lineage across a model swap: this instead tracks a different conversation taking over the same role in the same project, so a Seat's holder history is walkable from the graph record. (This edge was added after a real incident where its absence let one session mistake a live, separately-running session for its own past self and ask to be merged with it.) |
| `governs` | Seat → SoftwareProject | The charter: the repositories a Seat is responsible for. A Seat's scope of responsibility follows the Seat, not the working directory it happens to be running in. Distinct from `works_in` (an individual session's current working location): `governs` is an explicit, self-declared charter that survives both a folder move and a handoff to a successor, since it lives on the Seat rather than accumulating per-session. Healed by a compensating event (`valid_until`) when a repo drops off the charter, never deleted, so a Seat's shrinking scope stays a fact the graph remembers. |
| `holds` | Agent → Seat | The binding: the agent session currently holding a durable Seat. Minted at attach time (via the one-time token the spawning process exported at birth), re-linked to a successor at every handoff so the binding follows the active lineage; the old link is healed via `valid_until`, never deleted, so a Seat's holder history stays walkable. Distinct from `succeeds_seat` (holder → prior holder, session-to-session): `holds` is session → role. |
| `archived_snapshot` | * → URL | A Wayback/archive snapshot of the target. |
| `same_as` | n/a | Identity merge edge (loser → winner). |
| `uses` | n/a | Actor/source employs this capability or technique. |
| `indicates` | Indicator → * | Indicator points at the linked malware/technique. |
| `based-on` | Indicator → * | Indicator derived from raw observed evidence. |
| `subtechnique-of` | AttackPattern → AttackPattern | A more specific technique under a broader one. |
| `authored_by` | Commit → Person | Commit authored by a developer. |
| `in_repo` | Commit/File/Decision/Thread/Tension/Reflection/BlindSpot/Superstition/Practice → SoftwareProject | Belongs to a repository: commits and files from the git ingest, plus captured session items (decisions, threads, tensions, reflections, blind spots, superstitions, practices) filed to their project. |
| `follows` | Commit → Commit | Commit follows its parent (the history DAG). |
| `noted_in` | Thread → Commit | A thread / wall item surfaced in this commit's rationale. |
| `resolved_by` | Thread → Commit/Decision | The artifact that addressed this thread (closes it): either a later commit found automatically, or the commit/decision a session names explicitly when resolving the thread. This is the strong closure link, backed by a named artifact. |
| `closed_by` | Thread → Agent | Who closed this thread: minted unconditionally whenever a thread is resolved, because relying solely on `resolved_by` (which only fires when an artifact is named) was found to leave the majority of closures with no traversable trace at all. Distinct from `resolved_by` (what closed it, the strong artifact-backed witness), `closed_by` is the weak edge that always exists, minted only when `resolved_by` does not land for a given closure (free-text or unresolvable artifact, or none named), never both, so a closure always mints exactly one closure edge. Points at whichever agent resolved the thread. |
| `cites` | Reference/Decision/Thread → Reference/Decision/Thread/Practice/Superstition | This document cites, or draws from, that reference, or a Decision/Thread's own prose named another object by id in its text. This is a weaker, general claim than `answers` (which specifically means a Decision settling a Thread): `cites` just means an author's own text points at another object. A `self_referential` flag on the link marks an author citing their own earlier work, kept separate from genuine cross-author structure. |
| `informs` | Reference → SoftwareProject/Commit | This reference grounds / informs that artifact. |
| `mentions` | Reference/Commit/Message → Organization/Person/Decision/Thread/Agent | This object names/references that entity: an organization, person, decision, thread, or agent mentioned in text. For messages, this is what makes inter-agent conversation traversable: a message can be linked directly to the Decision or Thread it discusses. |
| `decided_in` | Decision → Commit | Decision was stated in this commit (the reasoning behind it, sourced). |
| `supersedes` | Decision → Decision | Declared in the schema but never actually implemented as a link (zero rows, ever measured). The real mechanism that replaced it is a `supersedes`/`superseded_by` property pair stored directly on the Decision objects, chosen because it fits the event-sourced model (reasserting a value is enough to "unwind" it, with no link-retraction primitive needed). There are over a hundred real corrections in the graph using that property-pair mechanism; none of them use this link. See `record_decision(supersedes=...)`'s own docstring for how it actually works. |
| `grounded_by` | Decision → Reference | Decision is grounded by this design reference. |
| `answers` | Decision → Thread | This decision is the answer to that thread: the ruling a question was raised to get. Distinct from `decided_in` (where it was said) and `grounded_by` (what it rests on), `answers` names what the decision actually settled. Minted by `record_decision(resolves=...)`, which closes the thread in the same action, so a decision that names its own question never leaves that question open. |
| `witnesses` | Practice → Decision/Commit/Thread | This Decision/Commit/Thread is evidence for the Practice: one witness is a hunch, several is a confirmed pattern. Minted explicitly by `record_decision(confirms=...)` or `record_practice(witnesses=...)`, never auto-linked from a mere topical search match, the same discipline followed by `grounded_by`/`obsoletes`/`supersedes`. The `confirmed` count on a Practice is this link's count, read at query time. |
| `implements` | Decision → Decision | This Decision is a specific execution of that standing, more general decision; unlike `supersedes`, the parent decision stays in force. This captures a relationship that's neither "supersedes" nor "cites": a specific application of a general standing rule. Same general-to-specific family as `witnesses`. |
| `rediscovers` | Decision → Decision | This later Decision independently arrived at a finding an earlier one already recorded, without anyone noticing the earlier one first. Points from the later finding to the earlier one. It buries neither side (unlike `supersedes`), and unlike `implements`, the later decision doesn't execute the earlier one's plan, it re-derives the same conclusion independently. Minted by `record_decision(rediscovers=...)`. |
| `narrows` | Decision → Decision | This later Decision bounds the scope of an earlier one without refuting or superseding it: the earlier decision's own conclusion stays correct within its now-more-visible limit. Non-destructive by construction, with no property write and no status change on either side, unlike `supersedes`. Minted by `record_decision(narrows=...)`; surfaced on the bounded decision via `recall()`'s `narrowed_by` field. |
| `managed_by` | Seat → Seat | The org chart: a worker Seat's manager of record, either the Seat that minted it or the Seat it was adopted under. This is the first Seat-to-Seat link type, distinct from `holds` (session → role) and `governs` (Seat → the repos it's responsible for): this one is role → role, the reporting structure that coordinator Seats extend themselves with. Never healed by `valid_until` on a mere reassignment request; the minting function only ever adds a missing edge, never removes one, since restructuring the org chart is treated as a deliberate, explicit action rather than an automatic side effect. |
| `peer_of` | Seat → Seat | A symmetric Seat-to-Seat partnership: recognition-first, making a pair legible to mail routing, review assignment, and succession. This catalog has no `symmetric` flag: the link is stored as a single directional row (in whichever order the caller named the pair), and every reader has to query both the from-id and the to-id side, the same pattern used elsewhere for a symmetric relationship stored on a directional column. Version 1 supports pairs only, no chains: the peer-linking function refuses if either side already carries an active `peer_of` edge. |
<!-- osiris:compiled:end -->

## Evidence classes

Confidence is a **projection of how a fact was obtained**, not a parser's guess.

| Class | Base confidence | Meaning |
|-------|----------------|---------|
| `SELF_DECLARED` | 0.90 | the entity says so (rel=me, profile field, own commit email) |
| `AUTHORITATIVE_API` | 0.85 | an authoritative dataset/registry asserts it |
| `CORROBORATED` | 0.80 | **read-time only**: at least 2 independent sources agree |
| `DIRECT_OBSERVATION` | 0.60 | observed to exist (a fetched page, an on-chain tx) |
| `DERIVED` | 0.40 | inferred (handle from email local-part, format variant) |
| `CO_OCCURRENCE` | 0.35 | seen near the subject (mined from a snippet) |

`CORROBORATED` is computed at read time (storing it would go stale). The frontier and
the subject-report read tiers off these classes.

## Sources & capabilities (`src/orchestrator/sources.py`, the playbook as data)

`suggest(object_type)` returns the capabilities worth running on an object; both
surfaces (MCP + API) read this.

### Collect (federate a base)

| id | applies to | keyless | yields |
|----|-----------|:------:|--------|
| `wikidata` | Organization, Person | ✅ | founders/officers, official social accounts, relationship network |
| `edgar_formd` | Organization | ✅ | private financing rounds: officers, amounts, investor counts, feeder SPVs |
| `edgar_expand` | Person, Organization | ✅ | every filing mentioning an operator, giving their co-investment book |
| `gleif` | Organization | ✅ | LEI (deterministic global key) + jurisdiction + ownership parents |
| `bc_registry` | Organization | ✅ | BC registration #, CRA business #, type/status/jurisdiction (+ the family) |
| `litigation` | Organization, Person | ✅ | lawsuits & enforcement: dockets, parties, judges |
| `clinicaltrials` | Organization | ✅ | registered human trials: status, sites, investigators |
| `facility_cotenants` | Organization | ✅ | the other sponsors running trials at a clinical site |
| `footprint` | Username, Email, Account, Person, Domain, URL | ✅ | GitHub/social/web identifiers via the cascade |
| `etherscan` | CryptoAddress | ⚠️ keyed | an EVM address's top counterparties, balance, token flow, contract identity |

### Analyze (read-model lenses)

| id | applies to | yields |
|----|-----------|--------|
| `dossier` | Organization, Person | identity properties + the named relationship network |
| `discrepancy` | Organization | operational geography the disclosed home omits |
| `coinvestment` | Organization | other companies funded by SPVs sharing a (non-platform) operator |
| `subject_report` | Person, Account, Username, Email | who is this? Verified / Corroborated / Speculative tiers |
| `sanctions_screen` | Person, Organization | name/identifier matches vs the ingested sanctions/PEP base |
| `sanctioned_wallet` | CryptoAddress | is this address or any counterparty an OFAC-listed wallet? + holder |
| `network_screen` | Organization, Person | is anyone in this entity's financing network on a watchlist? |

Data-source licenses (notably OpenSanctions **CC-BY-NC**) are in
[`../RESPONSIBLE_USE.md`](../RESPONSIBLE_USE.md).

## MCP tools (`src/mcp_server.py`, 18 total)

Each accepts a UUID **or** a name. Run: `uv run python -m src.mcp_server` (stdio).

| Tool | Kind | Does |
|------|------|------|
| `suggest_sources` | orient | the playbook for an object: what to collect/analyze next |
| `search` | orient | find objects by name substring |
| `aim_entity` | collect | Wikidata entity + relationships + official social accounts |
| `ingest_form_d` | collect | SEC Form D financing: officers, amounts, feeder SPVs |
| `expand_operator` | collect | every Form D mentioning a repeat-player, giving their portfolio |
| `lookup_lei` | collect | GLEIF LEI + jurisdiction + ownership parents |
| `verify_bc_entity` | collect | BC corporate registry (the entity + its corporate family) |
| `ingest_trials` | collect | ClinicalTrials.gov: status, sites, investigators |
| `ingest_litigation` | collect | CourtListener: dockets, parties, judges |
| `trace_wallet` | collect | Etherscan EVM trace: counterparties, balance, token flow |
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

All calls are idempotent (find-or-create on canonical); background work claims work items
via an atomic partial-unique index on `helper_runs`; mutations flow to a durable `outbox`.

## The console's Settings pane

Open it from the command palette (`Ctrl+K`, then *Settings*) or the gear icon in the
console's own header. One pane, five sections, each loaded independently: one section
failing to load never blanks the other four.

| Section | Shows | Backed by |
|---------|-------|-----------|
| **Key** | soul-key status (present, storage method, path, age, recovery paths) plus Init, Rotate, Restore-drill, Recover, and Enroll-recovery buttons | see [`KEYS.md`](KEYS.md) |
| **Backup & Offload** | the editable offload-targets panel plus a read-only view of backup status (timers, targets, presence, last successful offload) | see [`BACKUP.md`](BACKUP.md) |
| **Registry** | every registered setting, readable and writable | `GET /settings`, `POST /settings` |
| **Operator Desk** | items grouped by urgency (needs a decision, blocked on a process, informational, your own queue, dismissed), each with an acknowledge action, and a reply box on the decision group; a fold-candidates group with a copy-pasteable merge command and a reject action (merges are never run automatically from this interface) | `GET /operator/desk`, `POST /operator/desk/reply`, `GET /merge-candidates` |
| **The machine** | a hands-on-the-hardware readiness checklist, described below | several read-only routes |

The machine section covers the physical computer Osiris actually runs on. Each row shows a
status light and a short detail line: a filled green circle means confirmed working, a
faint hollow circle means confirmed not working, and a question mark means nothing has
checked it live.

Rows shown: whether the soul key is present, whether the restic credential is present, one
row per `local` offload target asking whether it's mounted (shown as unknown only when the
target has no expected mount point to check), one row per `restic` offload target asking
whether it's reachable (always shown as unknown, since reachability is never probed live;
watch the last successful offload time in the Backup & Offload section instead), and one row
confirming that the running code matches what was last deployed. See
[`DEPLOY.md`](DEPLOY.md#the-deploy-snapshot-localbinosiris-never-runs-a-gates-candidate-tree)
for what that last check compares.

### REST API routes (operator console, local machine only)

Every route below is a bare path on the console's own web application, with no shared
prefix. The console only ever listens on the local machine, so none of these are reachable
from anywhere else, and none of them are exposed to an automated agent.

| Route | Method | Does |
|-------|--------|------|
| `/soul-key/status` | GET | soul-key status facts, never key bytes |
| `/soul-key/init` | POST | mint the first soul key (`{owner?, path?, backend?, print_recovery?, restart?}`) |
| `/soul-key/rotate` | POST | rotate the soul key (`{finish?, print_recovery?}`) |
| `/soul-key/restore-drill` | POST | run the off-box restore drill (`{repo_url?}`) |
| `/soul-key/recovery-material` | POST | step 1 of browser-based Security Key enrollment. Issues single-use key material, held server-side for at most 60 seconds |
| `/soul-key/recovery-material/complete` | POST | step 2. Writes the recovery data the browser just prepared |
| `/soul-key/recovery-blob` | GET | read the non-secret wrapped recovery data, to start a browser recovery |
| `/soul-key/recover-from-browser` | POST | complete recovery after the browser unwraps the key locally |
| `/restic-key/status` | GET | restic-password status facts, never the password |
| `/deploy-status` | GET | `{running_sha, deploy_snapshot_sha, in_sync}`, comparing the running process against the pinned deployment snapshot |
| `/operator/desk` | GET | the same data the desk page shows, in structured form |
| `/operator/desk/reply` | POST | reply to a desk item (`{id, body}`); the sender is always fixed and never taken from the request |
| `/backup-settings` | GET / POST | read/write the backup configuration, see [`BACKUP.md`](BACKUP.md) |
| `/settings` | GET / POST | read/write the general settings registry |

## Repo map

```
src/
  actions/        the kernel: event-sourced Actions API (the narrow waist)
  ontology/       canonicalize · entity_type (Person↔Org) · resolution (ER, cross-base, screening)
  parsers/        evidence taxonomy + per-source parsers
  ingest/         the federators (one module per open base) + their CLIs
  orchestrator/   sources (the playbook) · cascade · router · ratelimit · budgets ·
                  dossier · discrepancy · coinvest · enrich · monitor (the watch) ·
                  watchers (source pullers) · compose (doc→lead) · satellite (placeful)
  dissemination/  dossier_report (the Markdown deliverable) · brief (PDF)
  connectors/     network seams (http clients, store, browser/leases [experimental])
  api/            FastAPI app (the human surface)
  workers/        Arq worker: enqueued jobs (expand_case_job) + crons (cascade drain,
                  watch evaluate/tick, stale-run reaper). Fault-isolated from the API.
  mcp_server.py   the MCP server (the AI surface)
  lab/            offline frontier-policy research [experimental]
alembic/          migrations (sync psycopg)
helpers/          footprint helper manifests (YAML)
deploy/           systemd units (api/worker) + env example; see docs/DEPLOY.md
tests/            pytest-asyncio + testcontainers (real Postgres/Redis)
samples/          real generated dossiers + evidence exports
```
