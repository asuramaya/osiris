# DESIGN.md — Gotham-Shaped OSINT Orchestrator

> **For Claude Code:** This document is the architectural spine of the project.
> Read it in full before writing any code. The four primitives in §2 are
> non-negotiable; deviations need explicit user approval. Build in the order
> in §14. When in doubt, append, don't overwrite.

---

## 1. Vision

A self-hosted, free-tier, human-in-the-loop OSINT platform shaped after Palantir
Gotham. The user inputs *anything* — an email, a phone, a domain, a Telegram
handle, a vehicle plate, a photo — and the system:

1. **Classifies** the input into a typed ontology object.
2. **Routes** to applicable helpers (server-side, browser-side, or human-gated).
3. **Fans out** queries adapted per source (dorks, templates, transliterations).
4. **Pipes CAPTCHAs and authenticated calls** to the analyst's real browser
   session — never tries to solve or evade.
5. **Asserts** typed properties and links back into a versioned graph with
   full provenance.
6. **Cascades** — every new assertion can trigger further helpers, building
   a Case (research session / profile / dossier) iteratively.
7. **Resolves entities** as data accumulates; surfaces merge candidates for
   human approval below confidence threshold.
8. **Supports pattern objects** (TTP/TTAL) as first-class citizens, not as
   tags — so Campaigns, Attack-Patterns, Intrusion-Sets are navigable.

The platform never overwrites a fact; every assertion is appended with
`(source, query, evidence_uri, observed_at, confidence)`. The graph at any
past moment is reconstructible.

---

## 2. The Four Non-Negotiable Primitives

These are the load-bearing concepts. If any one is shortcut, regret follows
around helper #20.

### 2.1 Dynamic Ontology

Typed **objects** (Person, Email, Phone, Domain, IPv4, Account, Document,
Vehicle, Vessel, Location, Event, Case, Campaign, AttackPattern, …),
typed **links** between them (`owns`, `registered_with`, `posted_from`,
`same_as`, `uses`, `attributed_to`, `targets`, `located_at`, `indicates`),
typed **properties** on both. Versioned, editable at runtime, security and
lineage attached to every field.

Object types and link types are defined in YAML, hot-reloadable. STIX 2.1
is the seed vocabulary — see §11.

### 2.2 Helpers / Parsers

A helper is a pluggable unit with a *manifest* (see §5). It declares what
it consumes, what it emits, its execution tier, and its rate budget.

The orchestrator **never hardcodes** "call holehe on an email." It asks:
*"which helpers' input signatures match this object's current state?"* —
and dispatches them.

Adding a new OSINT source = writing one manifest + one parser function.
Nothing else changes.

### 2.3 Actions

**The only way to mutate the ontology.** Six primitives:

- `create_object(type, canonical_form, case_id) -> object_id`
- `assert_property(object_id, name, value, source_id, evidence_uri, observed_at, confidence)`
- `create_link(from_id, to_id, type, properties, source_id, observed_at, confidence)`
- `merge_objects(winner_id, loser_id, justification)`
- `split_object(object_id, partition_spec, justification)`
- `tag_object(object_id, tag, scope)`

Every action is audited (append-only `audit_log`), attributed (who/what
called it), and reversible. UI clicks, helper outputs, and analyst edits
**all** go through this layer. No bypassing.

### 2.4 Object Set Service + Federation

Search/filter/aggregate over the typed graph (REST + GraphQL), plus the
ability to query external sources *in place* and **promote** results into
the ontology on analyst demand. The graph isn't a sink; it's a working
surface.

---

## 3. Infrastructure Topology

```
Analyst's browser (anywhere)
   │  HTTPS via *.your-domain.tld
   ▼
Cloudflare edge  ── Access policy (email OTP / SSO, free tier ≤50 users)
   │  cloudflared tunnel (outbound from Ubuntu, no inbound ports)
   ▼
Ubuntu host  (4–8 GB / 2 vCPU; Hetzner CX22 or similar, ~$5–12/mo)
   ├─ Orchestrator API (FastAPI)              → api.domain.tld
   ├─ Realtime hub (SSE + Redis pub/sub)      → ws.domain.tld
   ├─ Graph UI (Cytoscape.js + MapLibre)      → app.domain.tld
   ├─ Postgres (ontology + audit log)
   ├─ Redis (queues, token buckets, pub/sub, leases)
   ├─ Worker pool (Arq, async-native)
   ├─ SearXNG (self-hosted meta-search, internal)
   └─ Server-side connectors (keyless: crt.sh, archive.org, urlscan public,
       Ahmia, RDAP, hackertarget, Mastodon public, nitter)

Analyst's laptop
   ├─ Real Chrome with normal profile/sessions
   ├─ Browser extension (MV3) — client-side helpers, DOM scrape,
   │   cookie-lease capture, challenge handoff
   └─ Connects to ws.domain.tld with CF Access service token
```

### Cloudflare specifics

- **Tunnel = free, unmetered, no port forwarding.** `cloudflared` as systemd service.
- **Access free tier** = up to 50 users. Gates every hostname with SSO/OTP.
  Don't roll your own auth.
- **WebSocket idle reaper ~100s** — implement ping/pong keepalive or use SSE
  for job updates.
- **R2 free tier (10 GB)** — store scraped HTML, screenshots, evidence
  artifacts with signed URLs.
- **No proxying outbound through CF for scraping.** Server-side connectors
  egress from Ubuntu's IP directly.

### Lock-down checklist

- Docker Compose; all services bound to `127.0.0.1`; `cloudflared` is the
  only public ingress.
- `ufw default deny incoming`.
- Unattended-upgrades on; fail2ban for SSH; SSH key-only.
- Put the orchestrator API behind Access too, not just the UI.
- Service tokens for the analyst's browser extension auth.

---

## 4. Core Schema (Postgres)

Append-only at the assertion level. JSONB for flexible per-type fields.

```sql
-- Object types and link types are config (YAML), not table rows.
-- Tables below are the runtime store.

CREATE TABLE cases (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name         text NOT NULL,
    owner        text NOT NULL,
    budgets      jsonb NOT NULL DEFAULT '{}',
    created_at   timestamptz NOT NULL DEFAULT now(),
    archived_at  timestamptz
);

CREATE TABLE objects (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    type         text NOT NULL,                 -- 'Email', 'Person', 'Campaign', ...
    canonical    text NOT NULL,                 -- normalized form used for ER
    case_ids     uuid[] NOT NULL DEFAULT '{}',  -- multi-case membership
    status       text NOT NULL DEFAULT 'active',-- 'active'|'merged'|'archived'|'draft'
    merged_into  uuid REFERENCES objects(id),
    created_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (type, canonical)
);
CREATE INDEX ON objects (type);
CREATE INDEX ON objects USING GIN (case_ids);

CREATE TABLE assertions (
    id            bigserial PRIMARY KEY,
    object_id     uuid NOT NULL REFERENCES objects(id),
    name          text NOT NULL,                -- property name
    value         jsonb NOT NULL,
    source_id     text NOT NULL,                -- helper id or 'analyst:<email>'
    evidence_uri  text,                         -- R2 path to raw artifact
    observed_at   timestamptz NOT NULL,
    confidence    real NOT NULL CHECK (confidence BETWEEN 0 AND 1),
    superseded_by bigint REFERENCES assertions(id),
    created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON assertions (object_id, name);
CREATE INDEX ON assertions (source_id);

CREATE TABLE links (
    id            bigserial PRIMARY KEY,
    from_id       uuid NOT NULL REFERENCES objects(id),
    to_id         uuid NOT NULL REFERENCES objects(id),
    type          text NOT NULL,                -- 'uses', 'owns', 'same_as', ...
    properties    jsonb NOT NULL DEFAULT '{}',
    source_id     text NOT NULL,
    evidence_uri  text,
    first_seen    timestamptz,
    last_seen     timestamptz,
    confidence    real NOT NULL CHECK (confidence BETWEEN 0 AND 1),
    created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON links (from_id, type);
CREATE INDEX ON links (to_id, type);

CREATE TABLE helper_runs (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    helper_id     text NOT NULL,
    object_id     uuid NOT NULL REFERENCES objects(id),
    case_id       uuid NOT NULL REFERENCES cases(id),
    status        text NOT NULL,                -- queued|running|done|failed|awaiting_human
    tier          text NOT NULL,                -- open|fragile|gated|manual
    started_at    timestamptz,
    finished_at   timestamptz,
    result        jsonb,
    error         text
);
CREATE INDEX ON helper_runs (status);
CREATE INDEX ON helper_runs (case_id, object_id);

CREATE TABLE triggers (
    id            serial PRIMARY KEY,
    on_event      text NOT NULL,                -- 'object_created'|'property_added'|'link_created'|'object_merged'
    match         jsonb NOT NULL,               -- e.g. {"type":"Email"} or {"type":"Domain","property":"subdomains"}
    helper_id     text NOT NULL,
    conditions    jsonb NOT NULL DEFAULT '{}',
    enabled       boolean NOT NULL DEFAULT true
);

CREATE TABLE audit_log (
    id            bigserial PRIMARY KEY,
    action        text NOT NULL,
    actor         text NOT NULL,                -- helper id or 'analyst:<email>'
    case_id       uuid REFERENCES cases(id),
    payload       jsonb NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE merge_candidates (
    id            bigserial PRIMARY KEY,
    a_id          uuid NOT NULL REFERENCES objects(id),
    b_id          uuid NOT NULL REFERENCES objects(id),
    score         real NOT NULL,
    reasons       jsonb NOT NULL,
    resolved      text,                         -- null|'merged'|'rejected'
    resolved_by   text,
    resolved_at   timestamptz,
    UNIQUE(a_id, b_id)
);

CREATE TABLE cookie_leases (
    id            bigserial PRIMARY KEY,
    origin        text NOT NULL,
    cookie_blob   jsonb NOT NULL,               -- encrypted at rest
    ua            text NOT NULL,
    bound_ip      inet,
    expires_at    timestamptz NOT NULL,
    issued_by     text NOT NULL                 -- analyst email
);
```

**Rules:**

- `assertions` is append-only. Never `UPDATE`; supersede by inserting a new row
  pointing the old one's `superseded_by`.
- `links` may be deactivated by writing a corresponding `not_same_as` link or
  by adding a `valid_until` property — never deleted.
- "Current value" of a property is a view:
  `SELECT DISTINCT ON (object_id, name) ... ORDER BY object_id, name, created_at DESC WHERE superseded_by IS NULL`.

---

## 5. Helper Manifest Format

One YAML file per helper under `helpers/<id>.yaml`. Loaded at startup,
hot-reloadable on file change.

```yaml
id: holehe_email_signup_probe
name: holehe-style email signup probe
description: Probes ~120 sites for account existence by email
consumes:
  type: Email
  requires_properties: []          # nothing needed beyond the object itself
emits:
  - type: Account
    confidence_floor: 0.7
  - type: Link
    link_type: registered_with
    confidence_floor: 0.7
tier: open                          # open | fragile | gated | manual
origin: multi                       # 'multi' means many origins; uses internal table
rate:
  per_origin_rps: 0.5
  per_origin_concurrent: 2
  jitter_ms: [400, 1200]
template:                           # optional; for query-based helpers
  url: "{site.endpoint}"
  method: POST
  body: '{"email":"{object.canonical}"}'
windowing: null                     # set for pattern helpers (see §11)
cache_ttl: 3600                     # seconds
challenge_handling:                 # how to respond if challenge detected
  on_cloudflare: handoff
  on_captcha: handoff
  on_login_wall: abandon_with_warning
parser: holehe_signup_parser        # python function name in parsers module
```

The orchestrator's behavior is **entirely driven** by these manifests + the
trigger table. No business logic in code paths.

---

## 6. The Orchestrator Loop

```python
def on_input(blob, case_id, actor):
    type_ = classify(blob)                      # regex → ML fallback
    canonical = normalize(type_, blob)
    obj_id = action.create_or_find_object(type_, canonical, case_id, actor)
    fire_triggers(event='object_created', object_id=obj_id, case_id=case_id)

def fire_triggers(event, object_id, case_id, **ctx):
    obj = get_object(object_id)
    for trig in triggers.matching(event, obj, ctx):
        if not budget_allows(case_id, trig.helper_id): continue
        if already_run(trig.helper_id, object_id, case_id): continue
        enqueue_helper_run(trig.helper_id, object_id, case_id)

async def worker():
    while True:
        run = await queue.pop()
        helper = registry[run.helper_id]
        route = router.decide(helper, run.object_id)

        if route == 'CACHED':
            assertions = cache.get(helper.id, run.object_id)
        elif route == 'SERVER_WORKER':
            assertions = await helper.run_server(get_object(run.object_id))
        elif route == 'SEARXNG_WORKER':
            assertions = await helper.run_via_searxng(get_object(run.object_id))
        elif route == 'CLIENT_BROWSER':
            assertions = await dispatch_to_extension(helper, run.object_id)
        elif route == 'AWAITING_HUMAN':
            mark_awaiting_human(run); continue
        elif route == 'DEFER':
            requeue_with_backoff(run); continue

        for a in assertions:
            apply_action(a)                     # audited
            # apply_action emits events → fire_triggers cascades automatically
```

**Cascade termination** is enforced by per-case budgets, *not* by queue
emptiness:

- `max_rate_limit_credits` (server-side requests)
- `max_human_handoffs`
- `max_hop_distance` (graph distance from seed)
- `max_helpers_per_object` (prevent loops on a single hot entity)

When any budget hits zero, the case pauses and notifies the analyst.

---

## 7. The Router (Routing Decision)

Per-`(helper, object)`, not per-source globally. Same source can be tier-1
for `/sitemap.xml` and tier-3 for `/search`.

```python
def route(helper, object) -> Route:
    # 1. cache
    if cache.fresh(helper.id, object.id): return CACHED

    # 2. open tier — server-side, bot-tolerant
    if helper.tier == 'open':
        if tokens_available(helper.origin): return SERVER_WORKER
        return DEFER

    # 3. fragile tier — Google/Bing scrape, public Nitter, etc.
    if helper.tier == 'fragile':
        if searxng_healthy(): return SEARXNG_WORKER
        return CLIENT_BROWSER

    # 4. gated tier — Cloudflare/Turnstile/login walls
    if helper.tier == 'gated':
        lease = cookie_leases.get(helper.origin)
        if lease and lease.valid_for_server_egress():
            return SERVER_WORKER_WITH_LEASE
        if lease:                               # lease only valid in analyst's session
            return CLIENT_BROWSER_WITH_SESSION
        if analyst_online(): return CLIENT_BROWSER
        return AWAITING_HUMAN

    # 5. manual tier — paid/expensive; never auto-route
    if helper.tier == 'manual': return AWAITING_HUMAN
```

**Adaptive querying** lives here too: before dispatch, the router resolves
template variables from the case's accumulated state (e.g. prior helpers
discovered `corp.com` → next helper's `{domain}` slot is filled).

**Challenge detection signatures** (centralized; helpers don't duplicate):

- Cloudflare interstitial (`/cdn-cgi/challenge`)
- Turnstile script tags / iframes
- hCaptcha, reCAPTCHA iframes
- Akamai sensor headers
- PerimeterX `_px*` cookies
- Sudden HTML length collapse vs. baseline
- Redirects to known login walls

On detection → suspend job, persist URL + partial state + cookies snapshot,
push handoff card to analyst's UI.

---

## 8. Query Adaptation & Dorking

### Per-source query templates

Map `(input_type, source_id) → template + post-process`. Examples:

| Source         | Input    | Template                                              |
|----------------|----------|-------------------------------------------------------|
| crt.sh         | Domain   | `?q=%25{domain}` (SQL-LIKE, not regex)                |
| archive.org    | Domain   | `cdx/search/cdx?url={domain}/*&matchType=prefix...`   |
| GitHub search  | Email    | `"{email}" in:file`                                   |
| GitHub search  | Secret   | `"{secret_prefix}" extension:env`                     |
| Google CSE     | Phrase   | strip operators CSE doesn't honor                     |
| Yandex Images  | Image    | reverse via Yandex Images upload form                 |
| Telegago       | Handle   | cyrillic transliteration variants if RU-adjacent      |

### Dork families (intent-keyed, site-agnostic)

```yaml
credential_exposure:
  - 'intext:"BEGIN OPENSSH" "{handle}"'
  - '"{email}" filetype:env'
  - '"{email}" filetype:sql'

presence:
  - '"{handle}" (inurl:profile OR inurl:user OR inurl:u/)'

document_leak:
  - '"{org}" filetype:pdf (confidential OR internal)'
  - '"{org}" filetype:xlsx'

paste:
  - '"{indicator}" (site:pastebin.com OR site:ghostbin.com OR site:rentry.co OR site:paste.ee)'

bucket:
  - '"{org}" (site:s3.amazonaws.com OR site:storage.googleapis.com OR site:blob.core.windows.net)'

code:
  - '"{secret_prefix}" (site:github.com OR site:gitlab.com OR site:bitbucket.org)'
```

Permute, substitute, expand synonyms (handle ↔ email-local-part ↔ display-name
variants), rank by expected signal, run top K. The long tail eats rate budget.

### Adaptive narrowing

- First pass broad → if >50 results, add discriminators (employer, city, year).
- If 0, drop quotes, try transliterations, try email local-part alone.
- Use prior hits as new inputs (a found username from site A seeds site B's dork).
- Negative dorks matter as much as positive (`-site:pinterest.com -site:facebook.com/pages`).

---

## 9. CAPTCHA / Human-in-the-Loop Routing

**Principle:** never solve, never evade. Pipe challenges to the analyst's
real browser session.

### Handoff modes (analyst picks per source)

1. **Co-browse.** Orchestrator hands URL to analyst's extension; analyst
   solves in their real session; extension scrapes resulting DOM → posts
   JSON back. Server-side connector is done.
2. **Cookie lease.** Analyst solves once; extension exfiltrates `cf_clearance`
   + UA to orchestrator; orchestrator reuses for short TTL on follow-up
   server-side requests. *Caveat:* lease is bound to (analyst IP, analyst UA);
   reusing across IPs voids immediately. Effective on bot-fight-mode CF
   sites, useless against properly configured Turnstile. Measure per source.
3. **Abandon.** Some sources (Google "unusual traffic", LinkedIn auth walls)
   aren't worth even human time per query. Flag as session-scoped client-side only.

### UX: batch the handoffs

Don't ping the analyst on every challenge. Accumulate for ~30s into a single
tray ("you have 6 sites waiting"), ordered by which ones block the most
downstream tasks. Analyst burns through in a flow state; dispatcher drains
unblocked work in parallel. **This is the actual difference between "OSINT
toolkit" and "platform."**

### Suspend/resume state machine

```
queued → running → done
              ↓
         awaiting_human  → (analyst opens tray)
                          → in_browser
                          → result_posted_back
                          → done
                        OR → abandoned (analyst skip)
```

Suspended runs persist: URL, query, cookies snapshot, partial results, DAG
parents waiting on them. On resume, downstream triggers fire normally.

---

## 10. Entity Resolution

Three layers, increasing cost:

### 10.1 Deterministic (auto-merge, cheap, must-have)

Same canonical form → same object. Canonicalization rules per type:

- **Email**: lowercase; for Gmail, strip dots in local-part and `+suffix`.
- **Phone**: E.164.
- **Username**: `(platform, lowercase(handle))` tuple. Cross-platform username
  match is *not* deterministic — it's probabilistic.
- **Domain**: lowercase, strip trailing dot, IDN → punycode.
- **IPv4/IPv6**: canonical text form.
- **BTC address**: as-is (case-sensitive for some encodings).
- **Person**: no natural PK; surrogate UUID. ER is probabilistic.

### 10.2 High-confidence probabilistic (auto-merge + flag for review)

- Exact name + exact DOB
- Exact name + same employer + same city
- Same email/phone on both candidate Person objects

### 10.3 Low-confidence / fuzzy (never auto-merge)

Name similarity + weak signals → create a `MergeCandidate` row; surface in
review tray. Analyst confirms → `merge_objects` action; rejects → permanent
`not_same_as` link (prevents future re-suggestion).

### Merge semantics

- Winner keeps canonical id. Loser becomes `alias_of` link target.
- All source-attributed assertions from both sides preserved.
- Losing object's id resolves to winner via `merged_into`.
- Re-fire helpers on winner where the losing side had data the winner lacks.

---

## 11. Pattern Objects — TTP / TTAL

Pattern intelligence (Tactics, Techniques, Procedures + Activities + Locations)
is first-class, not tags. STIX 2.1 already nailed the vocabulary; we adopt it.

### Pattern object types

| Type             | Meaning                                                  |
|------------------|----------------------------------------------------------|
| AttackPattern    | A technique (MITRE ATT&CK Txxxx)                         |
| Tool             | The means                                                |
| IntrusionSet     | An actor cluster                                         |
| Campaign         | Time-bounded activity                                    |
| ThreatActor      | The human/group                                          |
| Indicator        | Pattern-as-detection-rule (YARA, Sigma, etc.)            |
| ObservedData     | Raw evidence (a scraped post, a paste, a netflow record) |
| CourseOfAction   | Mitigation/response                                      |

### Link types (carry `first_seen`, `last_seen`, `confidence`, `source`)

- `Actor --uses--> Tool`
- `Campaign --attributed-to--> Actor`
- `Campaign --targets--> Identity | Sector | Location`
- `Activity --located-at--> Location` (with `observed_at`)
- `Indicator --indicates--> AttackPattern`

### Seed the ontology

**Ingest MITRE ATT&CK STIX bundle on day one** — gives ~200 prebuilt
AttackPattern objects with descriptions, kill-chain phases, platforms.
**Ingest MISP galaxies** for Threat-Actor and Tool seeds. **Ingest ThreatFox**
for current Indicator feed. Helpers *link to* these existing objects rather
than coining new ones — every report immediately speaks the same language
as every other CTI shop.

### Windowed helpers

Pattern helpers run over rolling time windows, re-emitting as windows advance:

```yaml
id: tgstat_channel_behavior
consumes:
  type: TelegramChannel
emits:
  - {type: Campaign,      confidence_floor: 0.4}
  - {type: AttackPattern, confidence_floor: 0.5}
  - {type: ObservedData,  confidence_floor: 0.9}
tier: open
windowing:
  bucket: 7d
  slide: 1d
```

The router treats windowed helpers as Case-scoped cron jobs (not infinite
recurrence — scoped by Case lifetime + budget).

### Pattern hygiene

Without controls you'll get a "Campaign graveyard." Two cheap rules:

- Require ≥N `ObservedData` links before a Campaign is promoted from `draft`
  to `published` (default hidden in views).
- Auto-archive pattern objects whose newest `ObservedData` edge is older
  than the pattern's typical activity window.

### Behavioral merge candidates

ER extends to patterns: two `IntrusionSet` objects sharing three
`AttackPattern`s and two `Tool`s with overlapping `first_seen` windows →
queue `MergeCandidate`. This is how disparate cases converge.

---

## 12. Case Scoping

A `Case` is the investigation/session/profile container. It holds:

- **Object membership** (which entities are in scope; objects can belong to
  multiple cases).
- **Budgets**: rate-limit credits, human-handoff credits, hop budget,
  helpers-per-object cap.
- **Trigger overrides** (analyst can disable cascades for this case).
- **Snapshots** (frozen views — "what did we know on Tuesday?").
- **Dissemination exports**: STIX bundle, PDF brief, JSON dump.

The Case **is** the research product the user described. Everything attaches
to it. UI canvas is always rendered in a case context.

---

## 13. Tech Stack (Concrete Choices)

| Layer              | Choice                                          |
|--------------------|-------------------------------------------------|
| OS                 | Ubuntu 24.04 LTS                                |
| Reverse ingress    | Cloudflare Tunnel (`cloudflared` as systemd)    |
| Auth               | Cloudflare Access (free tier)                   |
| Orchestrator API   | Python 3.12 + FastAPI + asyncpg                 |
| Worker pool        | Arq (async-native, Redis-backed)                |
| Queue / pub-sub    | Redis 7                                         |
| Datastore          | Postgres 16 (JSONB heavy)                       |
| Object store       | Cloudflare R2 (free tier, 10 GB)                |
| Meta-search        | SearXNG (self-hosted, internal)                 |
| Graph UI           | Cytoscape.js                                    |
| Map UI             | MapLibre GL JS                                  |
| Browser extension  | Chrome MV3, native-messaging + ws to hub        |
| Realtime to UI     | SSE (CF-tunnel-friendlier than ws long-lived)   |
| Container layer    | Docker Compose                                  |

All services bind to `127.0.0.1`; `cloudflared` is the sole public ingress.

---

## 14. Build Order

Build phases. Steps 1–4 = working prototype. Everything after = makes it
feel like Gotham.

1. **Ontology + actions + Postgres schema.** No helpers yet. Prove
   hand-issued `assert_property` audits cleanly. End-to-end Docker Compose
   bring-up. Tests for each action primitive.
2. **Helper registry + manifest format + one keyless helper (crt.sh).**
   Prove the trigger cascade fires: input domain → crt.sh helper runs →
   subdomain objects created → trigger doesn't loop.
3. **Router with `tier=open` only, server workers, cache, token buckets.**
   Add 5–10 more keyless helpers (archive.org CDX, urlscan public, RDAP,
   Ahmia, dnsdumpster scrape, hackertarget free).
4. **Browser extension + ws/SSE bridge.** Add `tier=gated`. Implement the
   analyst tray (batched handoffs). End-to-end: input email → server-side
   helpers cascade → one helper hits CF challenge → analyst tray notifies →
   analyst solves in real browser → DOM scraped → assertions flow back.
5. **Cookie-lease subsystem.** Capture, store encrypted, scope to (IP, UA),
   reuse for server-side egress where viable.
6. **Entity resolution v1 — deterministic only.** Canonicalization rules
   per type. Auto-merge on canonical match.
7. **Case scoping + budgets.** Multi-case object membership. Budget
   enforcement in the trigger fan-out.
8. **Cytoscape UI.** Right-click an object → "available helpers" comes from
   manifest registry, not hardcoded. Timeline scrubber. Pivot menus.
9. **ER v2 — probabilistic + review queue.** MergeCandidate surface in the
   tray.
10. **Federation / promote.** Lazy helpers query in place; analyst clicks
    "promote" to materialize results into the case graph.
11. **TTP/TTAL pattern objects + STIX ATT&CK ingest.** Windowed helpers.
    Pattern hygiene rules.
12. **Export / dissemination.** STIX 2.1 bundle export. PDF brief generator.

---

## 15. Day-One Decisions (Pin Now)

1. **STIX 2.1 as the seed ontology vs. roll-your-own.**
   - **Recommended: STIX 2.1 from day one.** TTP/TTAL vocabulary is free,
     interop with OpenCTI/MISP forever, ATT&CK ingests directly.
   - Roll-your-own ships faster on day one and you regret it by helper #20.
2. **Postgres + JSONB vs. dedicated graph DB (Neo4j).**
   - **Recommended: Postgres.** Single-store simplicity, fits 4 GB RAM.
     Cytoscape doesn't need a property graph backend at this scale.
     Revisit only if traversal queries exceed 100ms p95.
3. **Browser extension vs. CDP-attached Playwright.**
   - **Recommended: extension** for production UX (no terminal needed,
     normal browser profile, MV3-native). Playwright-CDP as a power-user
     fallback.
4. **SSE vs. WebSocket** for orchestrator → UI updates.
   - **Recommended: SSE.** Survives CF tunnel idle reaper better; one-way
     fits our needs (UI doesn't push back through this channel).

---

## 16. Conventions for Claude Code

Follow these unless the user overrides:

- **Python 3.12.** Async everywhere (`async def`, `httpx.AsyncClient`,
  `asyncpg`). No `requests`, no sync DB drivers.
- **Type hints required.** `mypy --strict` clean on `src/`.
- **Append-only at the assertion level.** Never `UPDATE` `assertions`.
  Supersede with a new row.
- **Actions are the only mutation path.** If you're about to write raw SQL
  outside `src/actions/`, stop and call the action.
- **One helper = one manifest + one parser function.** No business logic
  in code paths the manifest doesn't describe.
- **Tests with pytest-asyncio.** Each action and each helper has tests.
  Postgres fixtures use a real Postgres in Docker (testcontainers), not
  SQLite, not mocks.
- **Secrets via environment, never committed.** `.env.example` checked in,
  `.env` gitignored.
- **Migrations via Alembic.** Schema changes are migrations, not raw SQL.
- **Logs are structured (JSON), include `case_id` and `helper_id`** when in
  context.
- **Rate limits are origin-scoped, not global.** Token buckets in Redis.
- **Don't try to solve CAPTCHAs.** Detect, suspend, hand off to analyst.
  Ever.
- **Don't add an external dependency without asking** beyond the stack in §13.

### Project layout

```
.
├── DESIGN.md                    # this file
├── CLAUDE.md                    # session-start notes for Claude Code
├── docker-compose.yml
├── docker-compose.override.yml  # local dev (gitignored)
├── .env.example
├── alembic/
├── helpers/                     # YAML manifests
│   ├── crtsh_subdomains.yaml
│   ├── archive_cdx.yaml
│   └── ...
├── src/
│   ├── actions/                 # the only mutation layer
│   ├── ontology/                # type defs, canonicalization, ER
│   ├── orchestrator/            # router, trigger fan-out, budgets
│   ├── workers/                 # Arq tasks
│   ├── parsers/                 # helper parser functions
│   ├── connectors/              # HTTP clients, lease store
│   ├── api/                     # FastAPI routes
│   └── ui/                      # static Cytoscape app
├── extension/                   # Chrome MV3
└── tests/
```

---

## 17. References

- Palantir Foundry/Gotham docs: https://www.palantir.com/docs/foundry/ontology/overview
- STIX 2.1 spec: https://docs.oasis-open.org/cti/stix/v2.1/stix-v2.1.html
- MITRE ATT&CK STIX bundle: https://github.com/mitre/cti
- OpenCTI architecture (for cross-reference): https://docs.opencti.io
- OSINT4ALL link inventory (the input universe): https://osint4all.github.io
- SpiderFoot (for the publisher/subscriber pattern, simpler scale):
  https://github.com/smicallef/spiderfoot

---

**End of DESIGN.md.** When a future decision contradicts something here,
update this file first, then change the code. The doc is the spine.
