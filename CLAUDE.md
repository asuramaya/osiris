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
