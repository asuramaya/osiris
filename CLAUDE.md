# CLAUDE.md — session-start notes

Read `DESIGN.md` for the architecture (the spine). This file tracks **what's been
decided since** and **current build state**. When a decision here contradicts
`DESIGN.md`, these decisions win until `DESIGN.md` is updated in one pass.

## What this is
A self-hosted OSINT orchestrator. **The product is the entity-graph engine itself**
(event-sourced Actions + cascade + ER + evidence-graded ingest); **footprint recon**
and **TTP/TTAL pattern intelligence** (STIX SDOs) are two thin *frontends* over it.
(Reset decision 2026-06-24 — supersedes the earlier "patterns are THE spine" framing;
both verticals are first-class frontends, neither is the spine. See the Reset entry in
the build order.)

## Runtime context (locked)
- **Single machine, single operator — this box IS prod.** No Cloudflare edge
  (Tunnel/Access/R2 all scratched). Services bind to 127.0.0.1.
- Auth: a single static identity via `OSIRIS_ACTOR` (fills the role CF Access had).
- Object store: local filesystem (`OSIRIS_ARTIFACT_DIR`), not R2.
- Generous resources — no 4GB constraint.

## Decisions that override DESIGN.md (to be folded back in later, one doc)
1. Merges are **event-sourced** (`object_events` is truth; `objects.status/merged_into`
   are a projection). Snapshots replay events to T; unmerge = compensating event.
2. Multi-source = **keep the set**. `assertions` use a *backward* `supersedes` pointer
   (rows immutable). `current_assertions` view = non-superseded set; consumers select
   across sources. Authoritative clock = `observed_at`.
3. ER: auto-merge only deterministic-canonical types; never auto-merge Person.
4. Triggers are a **projection of manifests**; per-case enable/disable in `cases.trigger_overrides`.
5. Graph projection: live, fed by `outbox`; engine = Apache AGE (seam now, extension later).
6. Dorking = analyst augmentation (human-gated), not an autonomous engine.
7. STIX export wraps custom objects (Vehicle/Vessel/TelegramChannel) as `observed-data`.
8. classify() = regex -> local ML on-box -> LLM (Claude API/OAuth) fallback.
9. Cookie-lease key via OS keyring (Phase 5).
10. Concurrency: durable `outbox` for cascades (not pub/sub); `helper_runs` atomic claim
    via partial unique index on active statuses (retry-safe).

OPEN: #4 windowed-helper lifecycle (dormancy N, backfill K, append-vs-supersede) — Phase ~10.

## Stack
Python 3.12 (uv), async everywhere (asyncpg, httpx, arq). FastAPI. Postgres 16 + Redis 7
via Docker Compose. Alembic migrations (sync psycopg — migrations only). Tests:
pytest-asyncio + testcontainers (real Postgres, never SQLite/mocks). `mypy --strict` on `src/`.

## Run
```
cp .env.example .env
docker compose up -d                 # needs docker-compose-plugin (see below)
uv sync
DATABASE_URL=postgresql://osiris:osiris@127.0.0.1:5432/osiris uv run alembic upgrade head
uv run pytest
```
NOTE: `docker compose` plugin is not yet installed on this box
(`sudo apt install docker-compose-plugin`). The Docker daemon works, so testcontainers
and `docker run` do too.

## Front door + caching (post-Phase-9 increment)
- `classify()` regex front door (`src/ontology/classify.py`): paste anything → type.
- API: `POST /cases`, `POST /cases/{id}/intake` (classify+intake), `POST /cases/{id}/expand`
  (run_cascade once); UI wires New-case + paste-anything seed box + Expand into the case view.
- **Persistent response cache** (`helper_cache` table, migration 0004; `src/orchestrator/
  cache.py`): cascade + federation fetch through `cached_fetch` → a (helper,object) pair is
  fetched at most once per cache_ttl. Makes deep/repeat expansion cheap.
- **Arbitrary depth:** `max_hop_distance: null` in case budgets = unbounded (safe via
  idempotent objects + active-claim dedup + rate credits). `trigger_overrides` now wired
  (per-case helper enable/disable). 86 tests green.

## Live prototype features (operator console on :8011)
- **SSE live updates:** `GET /cases/{id}/stream` pushes stats; UI EventSource updates
  badges/graph as the cascade expands.
- **osint4all integration:** `src/connectors/osint4all.py` generates `suggest`-tier
  manifests (built-in SEED + `import_startme` for the real board export); suggest tier
  routes to the handoff tray (augmentation link-outs, ruling #6). Merged into app manifests.
- **Co-browse from tray:** `POST /handoffs/{id}/cobrowse` (`cobrowse_open`) drives real
  Chrome, captures the session as a lease, returns a summary.
- **Phase 10 PDF brief:** `src/dissemination/brief.py` (reportlab) → `GET /cases/{id}/brief.pdf`.
- **EXPAND FIX:** triggers are now projected from manifests on app startup (`project_triggers`
  in lifespan). Root cause of "expand returns blank" was an unprojected triggers table on
  fresh deployments — the cascade matched no helpers. 93 tests green.

## Build order (TTP/TTAL-first; see osiris memory / DESIGN.md §14 reordered)
- **Phase 0 (DONE):** schema + 6 actions (`src/actions/core.py`) + audit/outbox + tests.
  7 tests green (testcontainers), ruff + mypy --strict clean. Remaining for Phase 0:
  Compose bring-up (needs docker-compose-plugin) — testcontainers covers tests meanwhile.
- **Phase 1 (DONE):** STIX ATT&CK ingest + export round-trip. `src/ontology/{stix,ingest,
  export,ingest_cli}.py`, `src/connectors/mitre.py`, migration 0002 (external_id index).
  13 tests green. Proven on LIVE enterprise bundle: 4815 objects / 21025 links / 0 dangling;
  Lazarus G0032 -> 93 techniques + 22 malware + 4 tools; 1-hop dossier exports as valid
  STIX 2.1 (524 objects). DPRK fixture in tests/fixtures/. `python -m src.ontology.ingest_cli`.
- **Phase 2 (DONE):** helper registry + manifest format + trigger projection (#5) +
  synchronous runner + first OSINT->TTP helper (ThreatFox). `src/orchestrator/
  {manifests,triggers,runner}.py`, `src/parsers/{base,threatfox}.py`, `src/connectors/
  {threatfox,store}.py`, `helpers/threatfox_malware_iocs.yaml`. 24 tests green. Proven:
  IOC->TTP convergence (ThreatFox AppleJeus IOC -> indicates -> ATT&CK Malware <- uses <-
  Lazarus), evidence content-addressed, atomic helper_run claim, STIX export of the chain.
  FINDING: abuse.ch now requires a free Auth-Key — "keyless ThreatFox" (DESIGN §3/§14) is
  dead; connector reads THREATFOX_AUTH_KEY; tests are fixture-based/hermetic. CIRCL OSINT
  MISP feed IS keyless (candidate for a later bulk galaxy-ingest path).
- **Phase 3 (DONE):** router (tier=open) + per-origin token buckets + durable outbox
  relay + budget-gated cascade + Arq wiring. `src/orchestrator/{ratelimit,budgets,router,
  cascade}.py`, `src/db/redis.py`, `src/workers/arq_worker.py`, `src/connectors/{crtsh,
  registry}.py`, `src/parsers/crtsh.py`, `helpers/crtsh_subdomains.yaml`. 32 tests green
  (real PG + real Redis via testcontainers). Cascade = pure async coroutines (drain_outbox/
  fire_triggers/dispatch) tested directly; Arq is the prod process. Connector = injected
  network seam. Proven: crt.sh self-similar feed cascades and TERMINATES on hop budget
  (no infinite loop); halts on rate-credit exhaustion; atomic claim dedup; token-bucket DEFER.
- **Phase 4 (DONE):** human-in-the-loop. `handoffs` table (migration 0003), centralized
  challenge detection (`src/orchestrator/challenges.py`), suspend/resume state machine +
  batched tray (`src/orchestrator/handoff.py`), gated-tier dispatch + ChallengeDetected
  mid-fetch suspend wired into cascade, human-handoff budget credit, gated Telegram helper
  (`src/parsers/telegram.py` + manifest), Playwright-CDP driver skeleton (`src/connectors/
  browser.py`, lazy-import — NOT yet a dep). 43 tests green. State authoritative on
  helper_runs.status; tray ordered by priority heuristic (hop-distance; true fan-out
  unknowable pre-run). DECISION PENDING: `uv add playwright` (+~150MB chromium) to enable
  live co-browse — deferred, flagged to operator.
- **Phase 5 (DONE):** cookie leases + LIVE co-browse. Playwright/cryptography/keyring added
  as deps; real Chromium works on this box (note: `playwright install chromium` has no
  prebuilt for ubuntu26.04 but a working chromium is present — launch verified). Lease
  subsystem (`src/connectors/leases.py`: Fernet encryption at rest, key via OS keyring →
  0600 file fallback → OSIRIS_LEASE_KEY override; (IP,UA) binding; `fetch_with_lease`
  server reuse). Real co-browse driver (`src/connectors/browser.py` `co_browse`: CDP-connect
  to operator's Chrome, or launch own headless). Co-browse handoff resolution
  (`src/orchestrator/cobrowse.py`). Router gains SERVER_WORKER_WITH_LEASE (gated + valid
  IP-bound lease → server reuse, the single-box happy path). 50 tests green incl. a LIVE
  test driving real Chromium: render local page → capture session cookie as encrypted lease
  → resolve handoff → reuse lease server-side past a 403 wall. Live test self-skips if
  Chromium can't launch. NOTE: cascade auto-use of SERVER_WORKER_WITH_LEASE needs per-helper
  server-side HTML scrapers (router decision + reuse proven; auto-dispatch wiring deferred).
- **Phase 6 (DONE):** Entity Resolution. `src/ontology/canonicalize.py` (per-type: Email
  gmail-dot/+strip, Phone best-effort E.164, Domain punycode, IP via ipaddress, BTC
  case-preserving), `src/ontology/intake.py` (canonicalize→find-or-create, preserve
  observed_value), `src/ontology/resolution.py` (probabilistic Person candidates: shared
  email/phone, name+dob, name+employer+city; behavioral IntrusionSet candidates on shared
  TTPs §11; resolve confirm→merge / reject→not_same_as; negative-memory suppression; review
  tray). Never auto-merges Person (#3). 60 tests green. NB: ipaddress rejects leading-zero
  IPv4 octets; PG `real` is float4 (approx in score compares).
- **Phase 7 (DONE):** Object Set Service (`src/api/app.py`, FastAPI factory `create_app(pool)`):
  /objects search, /objects/{id} (+multi-source props), /objects/{id}/graph (Cytoscape
  nodes+edges), /objects/{id}/helpers (from manifest registry), /cases/{id}/tray,
  /merge-candidates, /cases/{id}/snapshot?at= (event-sourced time-travel). Cytoscape UI
  scaffold at `src/ui/static/index.html` (mounted /ui). 67 tests green (httpx ASGITransport
  against test pool). NB: parse `at` to datetime in Python (asyncpg won't bind str→timestamptz);
  B008 ignored for src/api (FastAPI Depends idiom).
- **Phase 8 (DONE):** Federation / promote. `src/orchestrator/federation.py`
  (federated_query = connector+parser, NO persist; to_preview; promote materializes a
  selected subset via Actions, attributed by a 'manual' helper_run). API: POST /federate
  (preview in place), POST /promote (materialize selected). Connectors injectable via
  app.state.connectors for hermetic tests. 70 tests green.
- **Phase 9 (DONE):** windowed pattern helpers + hygiene. #4 RESOLVED: append evidence /
  supersede judgment, budget-only termination (no dormancy), backfill bounded by budget.
  `src/orchestrator/windows.py` (parse_duration, due_buckets, tick), `src/orchestrator/
  hygiene.py` (promote_campaigns ≥N ObservedData→lifecycle=published; archive_stale_patterns
  →set_status archived event-sourced), `src/parsers/tgstat.py` + manifest (windowed:
  bucket 7d/slide 1d/lookback 30d). Added Actions.set_status (event-sourced), claim_run
  window_bucket param, budget cap now counts DISTINCT helper_id (windowed re-runs exempt).
  79 tests green. Append=new ObservedData per window; supersede=rolling assessment property
  on stable Campaign (within-source supersession auto-applies).
- **Phase 10 (DONE):** dissemination — PDF brief generator (STIX bundle export already
  shipped in Phase 1). Original build order complete.
- **Phase 11 (DONE):** recursive footprint crawl + identity convergence (keyless; co-browse/
  token-passing deprioritized by operator). All keyless/no-antibot. 128 tests green, ruff +
  mypy --strict clean. Six commits:
  (A) **cross-case CACHED fix** — `router.has_cached_run_for_case`; `cascade._rematerialize_cached`
  re-links a globally-cached helper result into the requesting case (cache hit ⇒ no network,
  claim-idempotent, no rate credit). (B) **snippet mining** — `src/parsers/snippets.py`
  (find-style email/@handle/phone/URL regexes, canonicalized, conservative) folded into
  `parse_searxng_results`; mined selectors emit @0.4 `co_occurs` so the cascade crawls them.
  (C) **url_fetch** — `connectors/urlfetch.py` (http(s)+text/html only, byte-cap, challenge→
  suspend) + `parsers/webpage.py` (stdlib html.parser: rel=me 0.8 / mailto 0.7 / profile 0.5 /
  page_title). (D) **github_user** — `connectors/github.py` keyless api.github.com/users/{u}
  (404/403/429 graceful) + `parsers/github.py` (Account+Email+URL+twitter), consumes Username,
  low rps + 24h cache for the 60/hr unauth cap. (E) **convergence** —
  `resolution.find_footprint_merge_candidates` (shared-handle 0.6 / shared rel=me source 0.9
  Account↔Account) + `ensure_person_hub` (idempotent `cluster:<key>` Person, has_account/
  has_email, carries anchoring email); `hygiene.converge_identities` wired into `expand_case`
  after the fixpoint (hubs only from strong signals: bio-email match, rel=me; never auto-merges
  Person #3). (F) **subject anchor** — `POST /objects/{id}/subject` mints a per-case
  `subject:<case_id>` Person hub + links the fragment + seeds Person candidates; UI gains an
  "Identity" legend group (Person/Account/Username/Email/Phone), Account/Username/Phone TYPE
  styles, a "★ This is me" inspector button, and relation-legend entries.
  Proven live on username `asuramaya`: github_user→real github account, dorking→asuramaya.com +
  linkedin/soundcloud accounts via url_fetch/url_accounts, shared-handle candidates in the tray,
  and the subject anchor minting the identity hub. (Pre-Phase-11 increment: Phone footprint via
  offline libphonenumber + searxng_phone; broader keyless username enumeration + url_accounts
  profile patterns.)
- **Phase 11.1 (DONE):** anchor-and-pivot + precision. First live self-OSINT run was mostly
  noise (breadth-first dorking on common name "hector" + greedy github URL parsing). Fixes:
  (1) `accounts.profile_account()` reserved-path stoplist (github.com/{about,features,...} no
  longer fake Accounts; shared by url_accounts + webpage); (2) `handles.py` skips common
  first-name/generic locals (hector, info, …) + ≤2-char handles; (3) `urlfetch` uses a browser
  UA so Cloudflare-fronted personal sites serve real HTML; (4) NEW **`github_deep`** helper
  (`connectors/github_deep.py` + `parsers/github_deep.py`, consumes a confirmed github Account):
  mines profile README → self-declared social links (LinkedIn/SoundCloud/Twitter, `declares`
  @0.85), repo homepages → owned Domains/URLs (`has_url`), and commit authorship (API
  `author=` filter) → the real `committed_as` Email @0.9 (noreply dropped); keyless, optional
  GITHUB_TOKEN, bounded calls, 24h cache. 132 tests green. Proven: seeding username `asuramaya`
  alone now auto-yields github:asuramaya, soundcloud:wrenaudio7, linkedin:priya-kowalski-…,
  dakota.jm@gmail.com (committed_as), and owned domains (asuramaya.com/chronohorn.com/
  decepticons.win/madapesai.com) — with the prior account-noise eliminated.
- **RESET — engine-as-product (DONE, 2026-06-24):** the program had grown into a "confused
  tech demo" carrying two identities (threat-intel spine + footprint crawler). Decision: the
  **engine is the product**; both verticals are thin frontends. Root-caused the noise as a
  *crawl + ingest* problem, not a filtering one, and fixed the two seams. Branch
  `reset-engine-product` (tag `pre-reset-archive` for recovery). 138 tests green, ruff +
  mypy --strict clean. Commits:
  - **Step 0 declutter** — lifted `converge_identities` into `ontology/resolution.py`; removed
    the runtime-isolated windowed-tick vertical (`hygiene.py` promote/archive, `windows.py`,
    `tgstat`). Leases/federation/cobrowse/osint4all archival DEFERRED (leases is coupled into
    the kept router; not low-risk).
  - **Seam 1 — evidence-class ingest** (`src/parsers/evidence.py`): an `EvidenceClass` taxonomy
    (SELF_DECLARED / AUTHORITATIVE_API / DIRECT_OBSERVATION / CO_OCCURRENCE / DERIVED; read-time
    CORROBORATED) is now the source of truth for confidence — `emit()/link()` derive it from the
    class. `ObjectSpec` gained per-property classing (the old single shared confidence was the
    real defect); `LinkSpec` gained `evidence_class`. Persisted via migration 0005 (nullable
    column on assertions+links; view recreated). All footprint parsers ported; threat-intel
    parsers keep NULL (back-compat). The link class is the key signal: an enumerated same-handle
    Account is `DIRECT_OBSERVATION` (it exists) but its `has_account` link is `CO_OCCURRENCE`
    (same handle != same person).
  - **Seam 2 — anchor-and-pivot frontier** (`src/orchestrator/frontier.py`): `tier_of`/
    `is_expandable`, gated in `cascade.fire_triggers`. A node whose only inbound links are
    speculative (co-occurrence/derived) is a LEAF and never spawns crawls — until a second
    non-speculative source corroborates it and a later `expand_case` round finds it expandable
    (free promotion via the existing fixpoint). Seed = `hop_distance=0`; subject-tag = anchor;
    NULL/observation/anchor classes still expand (only restricts, never widens).
  - **Seam 3 — subject report** (`frontier.subject_report`, `GET /objects/{id}/subject-report`,
    UI panel off "★ This is me"): "who is this?" as Verified Core / Corroborated / Speculative,
    each fragment annotated with its evidence_class + source count + confidence.
  - **Live regression caught a real bug:** `create_or_find_object` never sets
    `case_objects.added_by_run`, so the original `_is_seed` (added_by_run IS NULL) made every
    node a "seed" → gate + report were no-ops in prod. Fixed to `hop_distance=0`. Proven live on
    `asuramaya`: github:asuramaya (authoritative) + asuramaya.com (self-declared) land in
    Verified; tumblr:asuramaya (enumerated, co-occurrence) is quarantined to Speculative.
  - NB: api.github.com unauth 60/hr throttles `github_deep`'s README/commit calls mid-run
    (partial README declares) — set `GITHUB_TOKEN` for the full enumeration. SearXNG container
    needs its `/tmp/searxng/settings.yml` mount repaired after a reboot (search-dorking path).
  - REMAINING: fold these decisions + DESIGN.md into one doc; finish the deferred archival
    (leases/federation/cobrowse/osint4all) with the router lease-route surgery.
- **POST-RESET SESSION — entity-intelligence pivot + ingest bases + lab (DONE, 2026-06-25):**
  9 commits on `reset-engine-product` (c91bca7..4282101), 143 tests green, ruff + mypy --strict
  clean. The arc: a long Socratic session that pressure-tested the whole project ("make the
  emperor naked" / constantly try to self-destruct it) and ended by pivoting collection from
  crawl to *federating finished open bases*. Key strands:
  - **Frontier-policy lab** (`src/lab/`, kernel-read-only): an offline simulator that races
    frontier policies (gate / PageRank / Thompson-bandit / chemotaxis / stigmergy) over
    synthetic + recorded substrates. FINDING: chemotaxis (run-and-tumble, explore-rate coupled
    to recent yield) modestly beats the naked bandit *only in the noisy, budget-scarce regime*;
    stigmergy is costume in static worlds. On the REAL keyless `asuramaya` substrate every
    policy hits 100%/0% — the frontier is **moot when collection is clean**. Meta-finding: an
    organic "shape" yields a good *parameterization of a known algorithm* (bandit/PageRank),
    not a new algorithm — so the bio-frontier work is conditional-on-noise, parked not killed.
    `src/lab/record.py` records a live substrate; recorded footprints are **gitignored (PII)**.
  - **Three keyless collectors added** (`connectors/parsers`: wayback, github_social, bluesky;
    manifests + registries). All verified correct; all yield NOTHING for `asuramaya` (no
    gists/GPG/Bluesky, domains unarchived). Proved the keyless surface for a careful,
    distinctive-handle subject is ~exhausted at GitHub. The "more data about me" lives behind
    antibot/keyed/broker walls the keyless constraint forbids.
  - **THE PIVOT (data is the moat/wall):** keyless gets the whole open *entity* graph and almost
    no private-*person* data. So keyless steers Osiris toward **entity intelligence** (the
    OpenSanctions/Sayari space), away from personal footprint. Don't crawl county/local records
    (the "ForeScan grave"); *federate finished open bases*. See [[osiris-data-strategy]].
  - **Bulk-ingest bases** (`src/ingest/`): `opensanctions.py` (FollowTheMoney loader — schema→
    object type, properties→assertions, relationship-entities→links; **role-typed endpoint
    stubs** bridge edges whose endpoints live elsewhere, enriched in place by a later same-id
    ingest) and `edgar.py` (SEC company_tickers.json → Organizations; SEC *inverts* antibot —
    403s browser UAs, wants a contact UA). Live: ~350 sanctioned persons/orgs + 800 SEC
    companies + 166 ownership/family/director links in the demo graph.
  - **Sanctions screening (the two halves meet):** `resolution.find_sanctions_candidates`
    name-matches the crawled footprint against ingested watchlist entities and queues review
    candidates (0.5, name-only, length-guarded; never auto-flags, #3); wired into
    `converge_identities`. This is the capability uniquely Osiris's: the autonomous crawl
    resolves against the open base it federated.
  - **Demo DB state (PG :5439):** 3235 objects, 662 links, 1807 opensanctions + 800 edgar.
    No GITHUB_TOKEN set; SearXNG container still dead (mount). `asuramaya` substrate fixture in
    `fixtures/substrate/asuramaya/` (gitignored).
  - **NEXT (operator's pick, in leverage order):** (1) Wikidata ingest — enriches the 309 FtM
    stubs in place (same Qxxx keys) + biggest open entity graph; (2) sharpen screening
    (alias-aware + shared-identifier, not name-only); (3) point the existing subject-report /
    graph view at an ingested entity to render its ownership/family network.
- **ENTITY-INTELLIGENCE BUILDOUT — the three NEXT moves, in order (DONE, 2026-06-25):** 3 commits
  on `reset-engine-product` (d82422e..6eba046), 151 tests green, ruff + mypy --strict clean.
  - **(1) Wikidata enricher** (`src/ingest/wikidata.py`): enrich-only federation over the keyless
    `wbgetentities` API. Reads the `Qxxx` stubs already in the graph, fetches labels/descriptions/
    literal facts (birthDate/website/...), writes them back as AUTHORITATIVE_API on the EXISTING
    object id (find-by-canonical → enrich in place; same find-or-create-or-stub as the FtM loader,
    so it composes in layers). `relationships=True` forms links from Wikidata's own claims
    (P22/P26/P40/P1830/...), stubbing absent endpoints. Live: **141/141 demo stubs named, 0 left**
    — the FtM family/ownership edges now connect two NAMED PEPs (was void→void). Bug caught live:
    `languages=en` filtered labels server-side, dropping 2 entities whose name lives only under
    Wikidata's `mul` (language-agnostic) code (common for transliterated names) → fixed to
    `en|mul` + mul-aware label fallback. `python -m src.ingest.wikidata enrich [limit] [--rel]`.
  - **(2) Sharpened screening** (`resolution.find_sanctions_candidates`): was exact-name-only @0.5.
    Now two scored/merged signals — **SHARED STRONG IDENTIFIER** (email/website/phone) @0.9
    (watchlist identifiers from ANY source, so a wikidata-enriched website screens too) +
    **ALIAS-AWARE** name match over `{name ∪ alias}` @0.5. The FtM loader now stores
    `alias`/`weakAlias`/secondary-names as a list-valued `alias` assertion (it dropped them
    before). "Watchlist entity" = any object with an opensanctions assertion. Live: 0 candidates
    on `asuramaya` (correct — private subject ≠ PEP; mechanism proven by 4 real-PG tests). NB:
    alias-awareness applies to entities ingested AFTER this change (re-ingest to backfill 1807).
  - **(3) Entity dossier** (`src/orchestrator/dossier.py`, `GET /objects/{id}/dossier`, UI
    "Dossier ▸" button): the "who is this?" read model for a FEDERATED entity (subject_report's
    tier lens collapses when every fact is AUTHORITATIVE_API). Identity properties (multi-source
    set) + relationship network grouped by direction/type, every endpoint NAMED (falls back to
    canonical; dedups repeated edges). Live: Bhagaban Majhi (Indian politician, b.1950-03-18)
    --family--> Pradeep Kumar Majhi — the three commits composing (OpenSanctions edge + Wikidata
    names + dossier render).
  - **Demo DB state (PG :5439) now:** ~141 Qxxx stubs named (0 bare), dossier/screening live.
    Still no GITHUB_TOKEN; SearXNG still dead (mount).
  - **NEXT (unstarted):** Wikidata `--rel` pass to GROW the graph (untested live; watch stub
    growth); more open bases (GLEIF/OpenAlex/USAspending — same `src/ingest/` pattern);
    name+DOB screening escalation now that watchlist carries birthDate; fold decisions into
    DESIGN.md; finish deferred archival (leases/federation/cobrowse/osint4all).
- **PROVE-THE-DIFFERENTIATOR — base→crawl bridge (DONE, 2026-06-26):** picked option (C) "prove
  the crawl×base bridge once" as a forcing function. 2 commits (aa201f6, 9cca6b2), 154 tests
  green, ruff + mypy --strict clean. The empirical investigation reshaped the goal:
  - **Original (C) "crawl SURFACES a base entity" is structurally dead.** No org-from-domain
    collector exists, and the crawl's population (person footprints) doesn't intersect the bases'
    (PEPs/companies). "Fuse the bases" is also dead in this demo — OpenSanctions × EDGAR name
    overlap = **0** (non-US sanctioned vs US public; different populations).
  - **But the bridge fires base→crawl, and there's live data:** 113 demo watchlist entities carry
    a crawlable `website`. Seed THOSE → keyless crawl → fuse the open-web footprint onto the
    registered record. That direction has shared entity space.
  - **The seam** (`src/orchestrator/enrich.py` `seed_web_presence`): mints linked URL+Domain
    objects from a federated entity's website/domain props, one hop past it, stamped
    AUTHORITATIVE_API so the anchor-pivot frontier lets the cascade crawl them.
  - **Live caught the same lesson as the reset, in miniature — yield is an EXTRACTION problem,
    not a crawl one.** First run on ELECTRA PRO LLC (sanctioned; voltara.example) confirmed the name
    (page_title) but harvested 0 emails — because the webpage parser only read `mailto:`, while
    `contact@voltara.example` sat in plain `<span>` text. Fix: `parse_webpage` now mines visible text for
    Email/Phone ONLY (not URL/handle — the breadth noise) via the existing snippet miner;
    own-domain email = DIRECT_OBSERVATION, off-domain = CO_OCCURRENCE. Re-run **harvested
    contact@voltara.example + a contact phone** onto the sanctioned entity — a new identifier the
    OpenSanctions record lacked. The differentiator delivers, keyless, with provenance.
  - **Honest limits:** ~1/3 of the 113 are antibot-walled (clp.com.hk → 403); the crawl still
    spreads ~41 internal URLs (url_fetch/url_accounts following links) — bounded by hop budget,
    low-value, unpruned. Demo DB (PG :5439) now also has a `bridge_*` case + voltara.example crawl. NB:
    no Redis on the box by default — `docker run -d --rm --name osiris-bridge-redis -p 6379:6379
    redis:7` for live cascade runs.
  - **NEXT for this thread:** prune internal-URL spread (tighten url_fetch/url_accounts to
    profile-shaped only); a `POST /objects/{id}/enrich-web` endpoint + UI button (the function
    exists, not yet wired to the API/UI); run the bridge across all 113 to measure real yield.
  - **YIELD MEASURED across all 113 (2026-06-26, commit 51e517f):** keyless fetch+parse of each
    federated entity's website → **37% surface a NEW identifier** the registered record lacked,
    **31% an email or social account** (the trustworthy signal: info@vendor-c.example, info@vendor-a.example,
    twitter:djiglobal, kontakt@vendor-b.example, …). Outcome split: new-id 37% / name-only 16% /
    empty 7% / **antibot-wall 16%** / dead-or-timeout 22%. So base→crawl enrichment is a
    ~1-in-3 headline capability, not a fluke — and ~38% is unreachable keyless (wall+dead).
    Measurement also caught a real noise bug → fixed: phone mining grabbed dates/year-ranges
    (2026.06.24, 1989-1990); now requires '+' or >=10 digits. RESIDUAL noise: 10-15-digit IDs
    still slip as phones; internal-URL spread (~41/entity) still unpruned. Yield harness:
    `scratchpad/yield.py` (uses the real urlfetch connector + webpage parser).
- **AIMED AT NEURALINK — official social accounts + `aim <name>` (DONE, 2026-06-26, b237594):**
  pointed the engine at a real non-toy subject. The federated half worked great
  (Neuralink Q29043471 → founded_by Elon Musk + Jared Birchall, named); the base→crawl half
  came up **empty** — neuralink.com is a minimal marketing page (200 OK, NOT antibot) with zero
  machine-readable identifiers. FINDING: base→crawl yield is inverse to company sophistication —
  SMBs (voltara.example) leak contact emails in HTML, big tech doesn't. The signal lives in Wikidata's
  curated social-account claims, which the loader ignored. Built:
  - `ingest_entities` mints official Account objects from social claims (P2002 twitter / P2003
    instagram / P2013 facebook / P2397 youtube / P4264 linkedin / P3185 vk), has_account @
    AUTHORITATIVE_API, always-on (`_SOCIAL` map).
  - `search_entity(name)` (wbsearchentities) + `aim()`: name → qid → ingest(entity + relationships
    + social) + stub-enrich. CLI `python -m src.ingest.wikidata aim <name>`.
  - Live `aim "Neuralink"` → entity + founders + twitter:neuralink / facebook:neuralinkcorporation
    / youtube / linkedin — a real keyless intelligence picture where the homepage crawl yielded
    nothing. Renders via the existing `/objects/{id}/dossier`.
  - NEXT: founder pivot (aim at Musk → his companies; does the network cross into the EDGAR
    public-company base?). Cross-base org resolution is UNWIRED (screening is footprint×
    opensanctions only). Demo DB now has Neuralink + a neuralink_* case.
- **CROSS-BASE ORG RESOLUTION — fuse the bases (DONE, 2026-06-26, 67b411b):** the founder pivot
  paid off. Aiming at Musk pulls Tesla (Q478214) from Wikidata; EDGAR has "Tesla, Inc." (a cik).
  The earlier "OpenSanctions × EDGAR overlap = 0" was a NORMALIZATION failure — names differ only
  by legal form. `normalize_org_name()` strips punctuation + legal-form suffix tokens
  (inc/corp/llc/sa/ooo/pjsc/…); `find_cross_base_candidates()` buckets Organizations by that key
  and queues a 0.6 review candidate for any two distinct objects with DIFFERENT provenance
  (cross-base, not a within-base dup). Wired into `converge_identities`. Never auto-merges (#3).
  Live: `aim "Tesla, Inc."` → Wikidata Q478214 → resolves to EDGAR cik:0001318605 — one clean
  candidate, no flood. The open bases now fuse on shared entities. Driver: `scratchpad/
  crossbase.py`. NEXT: sharpen with shared-identifier corroboration (website/LEI) to lift 0.6→0.9;
  a `POST /entities/aim?name=` endpoint + UI so "aim at X" is a console action, not a CLI.
- **SEC FORM D — the buried private-placement layer (DONE, 2026-06-26, ebd8a7c):** the operator
  asked to sharpen until the tool surfaces something NON-PUBLIC and surprising. Found the vein:
  private companies file **Form D** when raising capital (officers, amount raised, investor count,
  address), and a swarm of feeder **SPVs** file their own to repackage access — public but
  aggregated nowhere. The company_tickers loader only saw PUBLIC companies, so this was invisible.
  `src/ingest/edgar_formd.py` (keyless): EDGAR full-text search (efts.sec.gov) -> Form D filings ->
  each primary_doc.xml -> Organization (canonical cik:NNN, same scheme as edgar.py -> cross-base
  fuses to Wikidata) + offering facts + Person officers/directors by role. `aim_form_d(name)`;
  `python -m src.ingest.edgar_formd <name>`. Live on Neuralink: 14 filings -> Neuralink Corp.
  ($280,274,981 raised / 24 investors / $14,995 min / officers Elon Musk + Jared Birchall /
  Fremont CA, NOT the public SF HQ) + the feeder swarm (MAV Neuralink LP = a Jakarta fund, $1.3M
  from 107 investors at $1k min; DPV/VUVP/CGF2021/MegaCap series). Cross-base resolved
  cik:0001708503 <-> Q29043471. The surprise: a global retail secondary-market for a company most
  think is closed. THIS is the engine's edge — aggregating buried public filings nobody connects.
  Driver: `scratchpad/neuralink_formd.py`. NEXT: link SPVs -> core company (raises_for); pull SPV
  managers as a people network; full-text search beyond Form D (S-1, 13D/G ownership).
- **FORM D FUNNEL + CLINICAL TRIALS + DISCREPANCY (DONE, 2026-06-26, ccba82f/a655074/ec598d5):**
  deepening the Neuralink case into a real analytic chain.
  - **Feeder funnel** (`edgar_formd.link_feeders` + `_link_once` idempotency): SPVs now
    `raises_for` the core company (name-inferred, co_occurrence); managers (already ingested per
    filing) become reachable. Neuralink Corp ($280M) ← 12 SPVs (~$14.5M); Brett Sagan/Sydecar (DE)
    runs 5 of them; Sajid Rahman/MyAsia VC (Jakarta) the 107-investor retail feeder.
  - **ClinicalTrials.gov ingest** (`src/ingest/clinicaltrials.py`, keyless v2 API): trials →
    ClinicalTrial nodes (status/whyStopped/enrollment/has_results) + sponsor `sponsors` + `site`
    facilities + `investigator` people. Live: Neuralink = 6 trials, 5 countries, 8 sites, 4 named
    surgeons (Ponce/Jagid/Roser/Pouratian). INTEGRITY FINDING (operator asked re: dead human
    patients / buried failure): FDA MAUDE + recalls = 0 for Neuralink; all trials RECRUITING, 0
    terminated, 0 results posted → NO public evidence of patient deaths. The real "wall" is
    regulatory: investigational-device (IDE) adverse events are FDA-confidential, not in MAUDE,
    until a trial completes. Did NOT fabricate; reported the null. This ingest is the *monitor* —
    a TERMINATED flip or posted results is when evidence surfaces.
  - **Footprint discrepancy** (`src/orchestrator/discrepancy.py`): home (disclosed) vs operational
    (2-hop activity) geography; flags foreign countries the disclosures omit. Live on Neuralink
    (discloses US only): operates in UAE (Abu Dhabi trial), UK (UCL/Newcastle), Canada (Toronto
    trial + Vancouver financing), Indonesia (Jakarta feeder). The shadow footprint. NB: trial sites
    are partner hospitals (clinical reach ≠ offices); SPV entries are financing reach.
  - NEXT: keyless PATENTS source (PatentsView API now keyed — needs alt: USPTO bulk/ODP or Google
    Patents XHR with correct query); a scheduled trial-status WATCH; `POST /objects/{id}/discrepancy`
    + UI; pull SPV managers' own other filings (repeat-player expansion).
- **THREAD-PULLING TOOLS + CLUSTER PULL (DONE, 2026-06-26, 1754390..a414e21):** turned the
  Neuralink case into reusable network tools, then aimed them at the rest of the cluster.
  - **Repeat-player expansion** (`edgar_formd.expand_filings` + `search_filings(match_issuer=False)`
    + `_target_company` SPV-name parser + `link_spv_targets`): ingest every Form D MENTIONING an
    operator → link each SPV `raises_for` a `company:<name>` node. CLI `... edgar_formd expand <op>`.
    Sydecar=5323 filings, Sajid Rahman/MyAsiaVC=391. EFTS returns ~100/page, 500s on `from` offset.
  - **consolidate_companies** (resolution): event-sourced prefix-merge of the noisy company: layer
    (Crusoe Green Meadow→Crusoe). **coinvestment_ties** (`orchestrator/coinvest`): rank companies by
    shared SPV operators. **expand_facility** (clinicaltrials): trials at a SITE → co-tenant sponsors
    (filters to the queried facility only — was slurping all global sites of multi-site trials).
  - **CLUSTER FINDINGS (live):** Neuralink's SPV backers (Sydecar/CGF2021 in DE, MyAsiaVC in
    Jakarta) ALSO fund OpenAI, Anthropic, Groq, Worldcoin, Atom Computing, Crusoe, Starlab — the
    frontier-tech smart-money cluster. Co-investment: Anthropic is the hub (shares 3 operators w/
    Neuralink); Groq↔Anthropic↔Worldcoin tightly tied via MyAsiaVC. Cleveland Clinic Abu Dhabi
    (Neuralink's UAE trial site) co-hosts AbbVie/Pfizer/Lilly/Roche/Novo/Merck/Medtronic/Oxford —
    an established multinational trial hub, 19 sponsors.
  - KNOWN NOISE: `company:`/`cik:` fragmentation (Neuralink vs Neuralink Corp. unmerged → self-ties);
    SPV-name parse residue (OpenAI s); related-person entity artifacts (LLC Sydecar). All speculative-
    graded for review. NEXT: cross-base merge company:↔cik: (not just candidate); patents (keyless
    source still TBD); apply expand/coinvest to OpenAI/Anthropic directly (their own Form Ds).
- **MCP SERVER + SOURCE PLAYBOOK — the AI-facing surface (DONE, 2026-06-26, 77c9076):** answered
  "can Osiris run without Claude as a crutch?" structurally. Two crutches: (A) Claude-as-developer
  (fine, temporary — frozen capability), (B) Claude-as-runtime-analyst (the real risk: source
  selection, sequencing, ad-hoc SQL, interpretation). The fix isn't to remove B, it's to FORMALIZE
  it as an interface. Architecture: engine (pure capability) ← MCP server (AI-facing, external/
  optional/audited; never embedded) + FastAPI app (human front-end), both over the same functions.
  - `src/orchestrator/sources.py`: the investigation playbook AS DATA. `suggest(object_type)` →
    capabilities worth running (collect-then-analyze). Externalizes "private co → Form D" judgment;
    both surfaces read it. THE keystone.
  - `src/mcp_server.py` (FastMCP, `mcp==1.28.1` dep): 11 tools (suggest_sources/search/aim_entity/
    ingest_form_d/expand_operator/ingest_trials/expand_clinical_site/consolidate/dossier/
    discrepancy/coinvestment). All accept a UUID **or name** (fuzzy: exact then shortest-substring).
    Run `uv run python -m src.mcp_server` (stdio). mypy override: src.mcp_server disallow_untyped_
    decorators=false.
  - BUG the MCP surfaced (tool earning its keep): cross-base merge leaves loser links in place
    (resolve-on-read), so analytics on raw links miss them → discrepancy dropped UAE/UK. Fixed
    `discrepancy._cluster` to follow merged_into recursively. LESSON: all read-models must be
    merge-aware (coinvest/dossier likely need the same WITH RECURSIVE merged_into expansion).
  - NEXT (Option-1 completion = tool stands alone): wire the analytics to REST endpoints + UI panels
    (discrepancy/coinvest/funnel/suggest-sources — dossier already is) so the HUMAN front-end can
    drive everything the MCP can; make remaining read-models merge-aware; map more EDGAR country
    codes (C7 unmapped). THEN run the OpenAI/Anthropic playbook THROUGH the tool (the real test).
- **PERSONA PIVOT + COURT RECORDS + DOSSIER OUTPUT (DONE, 2026-06-26, b413ef8/7cafd96):** named
  the target USER — the independent follow-the-money investigator (Coffeezilla / dossier-builder).
  This reframes everything: "buried-but-public≠secret" (a ceiling I flagged) is their ENTIRE JOB;
  the provenance kernel (append-only/evidence-graded/hashed) I called plumbing is the #1 feature
  (they get sued — every claim needs a litigation-defensible receipt); keyless open-base federation
  IS their toolkit (no subpoena/LexisNexis). Competitor = Maltego+spreadsheet, not Palantir. See
  [[osiris-product-persona]]. Build order for the persona: court records → dossier output →
  blockchain ([[osiris-blockchain-layer]], deferred; reuse asuramaya/remix-etherscan-mcp +
  asuramaya/exciton). Built #1 and #2:
  - **Court records** (`src/ingest/courtlistener.py`): keyless CourtListener v4 (RECAP dockets +
    opinions) by party name → CourtCase nodes (court/dates/docket/judge/parties/attorneys/firms),
    linked to subject DIRECT_OBSERVATION when a NAMED PARTY, CO_OCCURRENCE when merely mentioned —
    the grading IS the precision tool (full-text 'Neuralink' returns 40, only ROGERS v. NEURALINK /
    Bernard v. Neuralink are party). MCP tool `ingest_litigation`; registry `litigation`.
  - **Dossier output** (`src/dissemination/dossier_report.py`): the deliverable. One call →
    provenance-annotated Markdown (identity / Form-D financing / litigation / discrepancy /
    co-investment / sources), every claim tagged source·how-obtained·date. Merge-aware, dedups.
    MCP tool `dossier_report(name)`. Proven live on Neuralink end-to-end via MCP.
  - MCP surface now 14 tools (added ingest_litigation, dossier_report). 179 tests green.
  - KNOWN NOISE in the deliverable (data-quality, all speculative-graded): co-investment shows SPV-
    name variants (Anthropic Magnitude/Secondary); litigation party-match is loose substring (some
    false 'named party'); C7 EDGAR country code unmapped. NEXT: blockchain layer; tighten party
    match + SPV-name consolidation; REST/UI panels so the HUMAN front-end reaches parity with MCP.
- **BLOCKCHAIN LAYER — EVM trace + sanctions fusion (DONE, 2026-06-26, 05b29e2/084c5ca):** build
  #3 for the persona (crypto-fraud is half the beat). EVM half shipped; Solana deferred. 187 tests
  green, ruff + mypy --strict clean. See [[osiris-blockchain-layer]].
  - **On-chain trace** (`src/ingest/etherscan.py`): Etherscan v2 (multichain by `chainid`) turns an
    EVM address into a financial picture. The footprint noise lesson carries over —
    `aggregate_counterparties` collapses normal+token txs into top-K per-counterparty totals (count,
    ETH in/out, tokens, first/last seen) rather than a node per tx; materialized as `transacted_with`
    links + balance + contract identity (getsourcecode), all AUTHORITATIVE_API (the ledger is ground
    truth). KEYLESS BENDS HERE: Etherscan rejects unkeyed calls → `ETHERSCAN_API_KEY` (free) required;
    absent it the connector returns an error dict, never crashes a run.
  - **THE DIFFERENTIATOR — fuse trace × sanctions base** (`opensanctions` CryptoWallet pass +
    `etherscan.screen_against_sanctions`): the loader's _TYPE map skipped FtM `CryptoWallet`, so OFAC
    SDN wallets never landed. Now a CryptoWallet pass mints a `CryptoAddress` keyed on a canonical
    ALIGNED with the tracer (`_wallet_canonical`: EVM addr → `eth:1:<lower>` regardless of ERC-20
    currency label) + a `controlled_by` edge to the holder. Because the canonical matches
    `etherscan._addr_canonical`, an OFAC wallet and a later trace of the same address dedupe into ONE
    object via create_or_find — **fusion for free, no merge, both provenances on one node**.
    `screen_against_sanctions` then flags the subject or any counterparty carrying an opensanctions
    provenance + names the holder. "Did my subject move money through a sanctioned wallet?" — the
    crawl×base edge applied to crypto. MCP `trace_wallet`/`screen_wallet`; sources playbook entries.
  - Bug caught by the fusion test: the fused node carries multi-source `address` assertions
    (etherscan + opensanctions) → scalar address subquery needs LIMIT 1 (the kernel's multi-source
    hazard surfacing in a read-model). MCP surface now 16 tools.
- **BLOCKCHAIN LIVE-PROOF — dossier section + OFAC re-ingest + real trace (DONE, 2026-06-26,
  289abd3/c061e1d/db952c3):** wired crypto into the deliverable and proved the whole loop
  live on real OFAC wallets. 191 tests green, ruff + mypy --strict clean.
  - **Dossier crypto section** (`dossier_report._crypto_section`): for a traced wallet subject,
    renders Top counterparties (decimal-adjusted token flow + ETH, `_flows`) + ⚑ Sanctions exposure
    (subject-is-listed? counterparty-is-listed? + named holder), etherscan+opensanctions in the
    sources appendix. Renders only when the cluster has `transacted_with` edges (entity dossiers
    unchanged).
  - **LIVE-VERIFIED against Ethereum mainnet** (key activated after a delay). vitalik.eth + USDT
    confirmed field shapes (USDT→TetherToken). The live run caught two defects hermetic tests
    missed → fixed: zero/burn address ranked as a counterparty (carrying scam-airdrop spam tokens
    $2000/0x4/14.3.3 → `_BURN` filter) and token AMOUNTS dropped (only symbols → `token_in/out`
    {symbol:amount} decimal-adjusted, the money was invisible). Also added rate-limit retry (free
    tier 3/sec; 4 calls/fetch tripped it).
  - **OFAC re-ingest** (us_ofac_sdn FtM, keyless ~50MB): focused slice = 797 CryptoWallets + 87
    holders + intra-holder rels → demo DB :5439 now has **786 CryptoAddress (97 EVM-keyed eth:1:),
    797 controlled_by** links to named holders (GARANTEX, Andariel/DPRK, CHATEX, …).
  - **THE LOOP, LIVE:** trace CHATEX `0x5512…21b0` via Etherscan → fuses with the OFAC object
    (subject_sanctioned=True) → screen finds a counterparty `0x4854…6b4a` that is ALSO an OFAC
    CHATEX wallet, to which CHATEX **sent 34.2 ETH** — a real on-chain link between two designations,
    surfaced as a sourced dossier finding. Same for Andariel→Andariel. The differentiator delivers
    end-to-end on real data. Driver: scratchpad ad-hoc (OFAC file at scratchpad/ofac_sdn.ftm.json).
  - HONEST LIMITS / NOT DONE: (1) counterparties stay anonymous unless verified-contract or OFAC
    (keyless Etherscan has no exchange labels — PRO-only); cross-token VALUE ranking needs prices.
    (2) Solana half deferred (Helius-keyed). (3) dossier Identity shows multi-source scalar (address)
    twice — honest but redundant; a dedup-by-value display is a general dossier polish. (4) ETHERSCAN
    env-shadowing: operator's shell profile exports a STALE key (16NGD…) that overrides .env's valid
    one (MGY4…) — runs need the key inline until the profile export is fixed. NEXT: Solana; price
    enrichment for value-ranked counterparties; REST/UI parity.
- **NEURALINK THREAD — novelty check + 3 sharpening tools (DONE, 2026-06-26,
  53a93a1/abb09da/7eb230b):** returned to the product thread; web-checked whether Osiris's
  Neuralink findings are already public, then built 3 tools in order. 194 tests green, ruff +
  mypy --strict clean.
  - **"Is the story already out?" (web)** — VERDICT: the real stories are all public (SEC probe
    reopened re: misleading investors on implant safety; USDA animal-welfare probe; $650M Series E;
    the SPV secondary-market phenomenon via EquityZen/Hiive/Forge). What Osiris has that's NOT
    written up: the ENUMERATED named roster of 13 feeder SPVs + investor counts + their GPs. But
    honestly that's plumbing, not scandal. Chased + KILLED two foreign-GP leads: Brilliant Phoenix
    (BP Neuralink, Chinese-heritage GPs Wu+Chen) = a VANCOUVER EMD, not China; MyAsiaVC (Sajid
    Rahman, 107-investor Jakarta SPV) = legit emerging-markets VC. The discipline (verify before
    publishing) working — handed no unverified China-scare.
  - **(1) coinvest platform filter** (`coinvest._PLATFORM_RE` + `platform_degree=12`): the
    co-investment "cluster" was mostly fund-admin-platform artifact — Brett Sagan/Sydecar "ties"
    Neuralink to Anthropic AND to dentists (Dentologie/MoldCo). Filter platforms by name + by
    DEGREE (an operator wired into >12 distinct cos is a serial-admin, catches platform signatories
    no name list would). Live: Neuralink ties collapse ~15→3; the survivor is the REAL signal —
    Sajid Rahman/MyAsiaVC bridges Neuralink↔Groq↔Worldcoin. The prior "Anthropic is the hub" claim
    was Sydecar artifact, correctly gone.
  - **(2) dossier Principals section + Celsius fraud proof** (`dossier_report` principals): the
    dossier lacked a "who runs it" section (1st question in a fraud dossier). Added — people linked
    (officer/director/founder/manager/agent) grouped one-line-per-person with merged roles, merge-
    aware. Proven on a target where financing IS the story (contrast to Neuralink=plumbing): live
    aim_form_d+ingest_litigation on Celsius/Mashinsky → Form D officers ARE the charged principals
    (Mashinsky, Daniel Leon, Yaron Shalem/Hogeg-case, the CFOs); litigation = the collapse (SDNY
    bankruptcy + 370-fraud clawback suits). Residue: Person name variants ("Alex" vs "Alexander"
    Mashinsky) render as separate principals (ruling #3, no auto-merge); display dedup deferred.
  - **(3) `resolution.screen_network`** — the un-chased thread: does any money-mover behind Neuralink
    trace to a watchlist? Form D names no LPs, but names the SPV officers/GPs → NETWORK screen.
    Gathers principals + feeder SPVs + SPV operators, matches each vs watchlist (shared id 0.9 /
    alias-aware name 0.5) with the connecting path; read-only, never asserts (#3), merge-aware.
    Wired to dossier (⚑ section, hits-only), MCP (`screen_financing_network` → 17 tools), sources.
    Live: Neuralink's 29-member network AND Celsius BOTH clean vs the ingested OFAC SDN/PEP set —
    honest null; fires on a planted dirty operator (hermetic test). NB "clean"=vs what's ingested,
    not all watchlists globally (fuller OpenSanctions PEP ingest widens it). NEXT: Person display-
    dedup; broader PEP ingest; aim the playbook at an active-fraud target where the network lights up.
- **ORGBOOK BC — keyless Canadian corporate-registry verification (DONE, 2026-06-26, 7db2326):**
  driven by a live investigation ("is Brilliant Phoenix actually Canadian?"). US-only (EDGAR)
  verification was a real jurisdiction gap. `src/ingest/orgbook.py` federates OrgBook BC
  (orgbook.gov.bc.ca, keyless) — an Organization per BC registration, canonical `bc-reg:<num>`,
  carrying CRA business number / type / status / jurisdiction / reg date, AUTHORITATIVE_API; a
  family name pulls the whole group. MCP `verify_bc_entity` (→18 tools); sources `bc_registry`.
  198 tests green. Live: 'Brilliant Phoenix' → 10 ACTIVE BC entities exposing the structure —
  Holdings (control) → Capital Mgmt → GP + Carry GP → per-thesis LPs (AI / Israel 1 / Neuralink /
  SpaceX) + a local Mortgage Investment Corp. Verdict on the investigation: registration/operations
  = definitively Canadian (BC) — triple-confirmed (SEC Form D 'A1' + CSA NRD#58800 + BC registry);
  China nexus is real but lives in the FOUNDERS' background (Chunhua Wu — Fudan, UBC Sauder prof,
  co-founded a China internet/financial-info co; Ye/Cindy Chen) not in any provable capital/control
  link; LP capital source is the keyless ceiling (BC beneficial-ownership register non-public, SPV
  LPs undisclosed). HONEST LIMIT: cross-base does NOT fuse BC 'Brilliant Phoenix Neuralink LP' with
  EDGAR 'BP Neuralink LP' — the BP acronym defeats name-normalization, no shared identifier.
  NEXT (would close the loop SAFELY): fix edgar_formd to type entity-shaped related-persons
  (Inc./LLC/GP Inc./Fund Admin) as Organizations not Persons → 'Brilliant Phoenix GP Inc.' becomes
  an Org that deterministically cross-base-fuses with bc-reg:A0127997 (also cleans the
  Sydecar/MyAsiaVC "Person" artifacts). CSA NRS / CanLII deferred (CanLII needs a key → breaks
  keyless; CSA aretheyregistered.ca is a JS app w/o a clean public API).
- **PERSON-vs-ORG CLASSIFIER — at ingest + retroactive repair (DONE, 2026-06-26, d58cee0):** the
  root cause behind the BC↔EDGAR fusion miss + the "LLC Sydecar"/"MyAsiaVC" Person artifacts: Form
  D's related-persons parser typed every entity as Person. `src/ontology/entity_type.py`
  (`is_organization`/`classify_entity_type`/`clean_entity_name`) — CONSERVATIVE (legal-form token /
  curated org-noun / '&' ⇒ Org; a plain name stays Person, so a real person is never made a
  company; strips Form D 'N/A' placeholders). Applied in `edgar_formd` at ingest (Org→`sec-org:`,
  Person→`sec-person:`). `resolution.reclassify_mistyped_entities` heals EXISTING mistyped nodes by
  minting the Org + MERGING the Person into it (event-sourced, reversible, one-direction); wired
  into MCP `consolidate`. Live: 31 GP/LLC/Fund "persons" re-typed → 5 cross-base merges → **EDGAR
  'Brilliant Phoenix GP Inc.' FUSES with bc-reg:A0127997**, closing the BC↔EDGAR loop through the
  GP. Emperor-naked: a "name has a digit ⇒ org" first cut false-flagged 'Desiree Lambert Inmate No.
  13432-046' (+2 clinical-contact artifacts) → removed (redundant; real orgs carry a word token),
  reversed the 3 bad demo merges, added a stray-digit regression. 232 tests green. NB: no `unmerge`
  Action exists (design intent only) — the demo false-positive reversal was direct SQL on :5439.
  RESIDUAL: clinicaltrials still mints junk Person nodes from contact-field strings (pre-existing,
  separate parser issue).
- **GLEIF + DELIVERABLE HARDENING (DONE, 2026-06-26, 3d95c26/c7e7955):** operator picked "GLEIF
  global registry" + "harden the deliverable" (deferred acronym-LP cross-base as false-positive-
  prone). 237 tests green, ruff + mypy --strict clean.
  - **GLEIF** (`src/ingest/gleif.py`): keyless global LEI registry. Organization per LEI
    (`lei:<LEI>`) + jurisdiction/status/country + `subsidiary_of`/`ultimate_parent` links from
    Level-2 ownership. The LEI is a DETERMINISTIC cross-base key → `find_cross_base_candidates`
    gains a shared-LEI pass (0.95 vs 0.6 name). Fuzzy name filter post-filtered to normalized-name
    matches (cuts 'ProShares Ultra Anthropic' ETF noise). MCP `lookup_lei`; sources `gleif`. Live:
    GOLDMAN SACHS INTERNATIONAL → GS GROUP UK → THE GOLDMAN SACHS GROUP, INC. HONEST LIMIT: GLEIF
    Level-2 parent reporting is VOLUNTARY/sparse — most entities (Tesla subs, Anthropic PBC) 404.
  - **Harden the deliverable (3 fixes):** (1) MERGE-AWARE Principals — the dossier section I'd
    added joined link targets raw + filtered active, so a confirmed Person merge DROPPED the
    loser's roles (violated the merge-aware invariant in my own new code); now forward-resolves
    each link to its merge winner (walk merged_into) + groups by resolved id. (2) NAME-VARIANT
    person ER candidates (`resolution._name_variant`): two people behind the SAME company (resolved
    to its winner — cluster-aware) with identical names (0.7) or nickname/initial variant
    (Alex/Alexander, 0.55); review-gated, never auto (#3). (3) JUNK-PERSON guard
    (`entity_type.is_plausible_person_name`): ClinicalTrials skips contact-string "officials"
    ("Call 1-877-…", "…(dept. 2834)"). Live proof: confirming the Mashinsky candidate collapses
    the Celsius dossier to ONE "Alexander Mashinsky — director, founded_by, officer (edgar,
    wikidata)". DELIBERATELY SKIPPED: acronym LP cross-base (BP↔Brilliant Phoenix) — token-overlap
    on "neuralink" would merge every Neuralink SPV; GP-level fusion + LEI link the families safely.
- **v0.1.0 PUBLIC RELEASE PREP (2026-06-27):** first release to https://github.com/asuramaya/osiris.
  Strategy crystallized in a long discussion (see [[osiris-roadmap-deployment]]): the kernel is the
  spine the operator's two dead projects (ForeScan foreclosure lead-gen, Lakshmi gov-contract
  crawler) circled but never built; both died collection-first (swamped by local processing + bad-
  data theatrics). Capability ladder = each holder forces one kernel capability: journalist→
  convergence (PROVEN), broker→persistence/cron (NEXT), analyst→correlation, then standing graph.
  Deployment = separate by placefulness/blast-radius/trust-zone; PG+Redis as the bus (no RPC mesh);
  cuts not rewrites; lens runs anywhere, tripwire forces always-on. Adoption = MCP-first, AGPL-3.0,
  journalists as beachhead but the engine is horizontal ("same sword, many hands"); keyless = a
  safety feature (open entity commons, not private persons); trust/noise is existential for adoption.
  Wrote public docs: README (honest does/wants/can't), ROADMAP (the ladder + the deliberately-not-
  done), ARCHITECTURE (rings + narrow waist), RESPONSIBLE_USE (dual-use + data-license matrix —
  OpenSanctions CC-BY-NC is the gotcha), CONTRIBUTING, SECURITY, LICENSE (AGPL-3.0). Repo hygiene:
  no secrets/PII tracked (verified), removed empty extension/, tidied .gitignore. Deferred
  subsystems (leases/cobrowse/federation/osint4all/lab) kept but documented as experimental — not
  deleted pre-release. 237 tests green. NEXT: promote reset-engine-product→main, push to remote
  (pending operator go-ahead + license confirm), then build the persistence/cron capability.
- **CRON BUILD Phase 1 — the watch (lens→tripwire) (DONE, 2026-06-26):** first rung of the
  persistence ladder (ROADMAP "Next: persistence", step 1). The one new primitive that turns the
  kernel from a *lens* (investigate on demand) into a *tripwire* (fire when the public record
  changes). 249 tests green (+12), ruff + mypy --strict clean. All source-agnostic — NO real
  collection yet (that's Phase 3). `src/orchestrator/monitor.py` + migration 0006:
  - **Two cursors, two decoupled consumers of one outbox.** The evaluator claims via a NEW
    `outbox.evaluated_at` flag (gap-free `FOR UPDATE SKIP LOCKED`, mirrors the cascade's
    `published_at`) — it never touches `published_at`, so draining the cascade and evaluating
    subscriptions are independent passes. The `watermarks` table (`key`/`cursor`) is the OTHER
    cursor — generic per-SOURCE delta cursor (`source:<id>`), used by `tick`, not the evaluator.
  - **`tick(actions, source_id, puller)`** — source-agnostic scheduled pull: read cursor →
    injected `puller(cursor)` → materialize each `WatchItem` through Actions (idempotent) →
    advance cursor AFTER commit (crash mid-tick re-pulls the same delta, find-or-create dedups).
    The puller is the only collection-specific part; a real connector registers into
    `arq_worker.SOURCE_TICKS` in Phase 3.
  - **`evaluate_subscriptions(pool, sink=)`** — drains un-evaluated outbox rows, matches each
    against active `subscriptions` (saved `criteria` jsonb), emits an `alerts` row per match.
    `matches()` clauses (all AND, source-agnostic): event_types / object_type / canonical_prefix
    / canonical_contains / property_name / value_contains. Idempotent: claim flag + UNIQUE
    (subscription_id, outbox_id) ON CONFLICT DO NOTHING. Prospective by design (a new sub fires on
    tomorrow's events, not the backlog... caveat: it WILL match any still-unevaluated backlog rows).
  - **Dumb alert sink (NOT a CRM):** the `alerts` table is the durable record (always written
    first); a `Sink` callback delivers the side-channel (default `_post_webhook` if the sub has a
    `webhook_url`) AFTER commit — a sink failure logs + leaves the row, never loses the alert or
    aborts the batch. `delivered_at` stamped only on sink success.
  - **Arq wiring:** `evaluate_watch` cron @ 5s (offset +2s from the cascade drain), `run_source_ticks`
    @ :00 each minute (iterates `SOURCE_TICKS`, one bad source can't sink the rest). **API:** POST
    `/subscriptions`, GET `/subscriptions`, GET `/alerts?subscription_id=` (the sink read back).
  - **Proof:** hermetic `tests/test_monitor.py` (12, real PG) — matcher clauses, watermark upsert,
    tick materializes+advances+re-pull-empty, evaluator fires-on-match / quiet-on-noise /
    idempotent / inactive-silent / sink-failure-keeps-alert / decoupled-from-published_at. Plus ONE
    LIVE run (throwaway PG :5544, migration 0006 forward-clean): canned tick of {Neuralink, Apple}
    → subscription `value_contains:neuralink` fired exactly 1 alert (Neuralink), Apple stayed quiet,
    re-run fired 0, webhook sink delivered=True. Driver: `scratchpad/watch_live.py`.
  - **NEXT:** Phase 2 — worker ⊥ surface cut (isolate the worker process from the API; failure
    drill). Then Phase 3 — register a REAL puller (new SEC filings / sanctions deltas) into
    SOURCE_TICKS and prove the full loop end-to-end on a live source.
