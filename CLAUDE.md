# CLAUDE.md — session-start notes

Read `DESIGN.md` for the architecture (the spine). This file tracks **what's been
decided since** and **current build state**. When a decision here contradicts
`DESIGN.md`, these decisions win until `DESIGN.md` is updated in one pass.

## What this is
A self-hosted OSINT orchestrator whose **product is TTP/TTAL pattern intelligence**
(STIX SDOs) derived from OSINT. Patterns are the spine, not a late phase.

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
- **Phase 7 (next):** Object Set Service + graph read API (FastAPI) + Cytoscape/MapLibre
  UI scaffold; right-click helpers from manifest registry; snapshot/timeline. Then federation.
