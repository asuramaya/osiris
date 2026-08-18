<!-- topic: deployment -->

# Deployment

Osiris separates by **blast radius**, not by feature. Two long-running processes,
coordinating only through Postgres + Redis (no RPC mesh between them):

| Unit | Process | Role | If it dies |
|------|---------|------|-----------|
| `osiris-api` | `uvicorn src.api.app:app` | the human surface (read API + console + SSE) | the console is briefly down; the truth and the worker are untouched |
| `osiris-worker` | `arq src.workers.arq_worker.WorkerSettings` | the cascade drain, the watch (evaluator + ticks), the reaper | crawls pause; on restart it re-claims orphaned runs and resumes the durable outbox — no work lost, nothing double-emitted |

This is **Cut 1** of the deployment ladder in [`../ROADMAP.md`](../ROADMAP.md): same box,
two units, fate-isolated. Everything before this ran in one process.

## Routine deploys: `osiris deploy`

Everything below this section is about **standing the box up the first time**. Once it's
up, the day-to-day deploy — restart the services that hold new code, prove they came back
healthy, and catch anything that needed a seed or a migration and didn't get one — is one
command: `osiris deploy` (see [`CLI.md`](CLI.md#osiris-deploy) for the full ritual). It
refuses outright on an uncommitted change under `src/` rather than silently shipping a
half-written edit, which is exactly the failure the by-hand `systemctl --user restart
osiris-mcp osiris-worker osiris-console && python scripts/osiris_smoke.py` protocol this
replaces once caught only by a manager's own well-timed `git status`.

## The discipline: nothing heavy runs in a request path

The API **enqueues** heavy work onto the worker and returns immediately; it never runs
a crawl in its own event loop. Concretely, `POST /cases/{id}/expand` enqueues the
`expand_case_job` Arq job — the worker executes it, and the SSE stream
(`GET /cases/{id}/stream`) surfaces progress by reading the same Postgres the worker
writes to. A runaway expansion therefore can never block or crash the console.

## Why the cut is safe (the reliability is in the kernel)

The worker can be killed and restarted at any time because the kernel already
guarantees:

- **Atomic claim** — the `helper_runs_active_claim` partial unique index admits exactly
  one worker per `(helper, object, case, window)`. Two workers never double-dispatch.
- **Durable outbox** — cascade events are written in the same transaction as the data
  change; a restarted worker drains whatever is unpublished. Nothing is fire-and-forget.
- **Idempotent emit** — `create_or_find_object` is find-or-create on canonical, and
  assertions supersede within-source, so re-running a reaped job can't fork the graph.
- **The reaper** — a worker that dies *mid-run* leaves a stuck `running` row that the
  claim index would block forever. `reap_stale_runs` (a worker cron) resets such
  orphans to `failed` after a timeout, releasing the claim so the restart re-claims a
  fresh run. Human-wait states (24h handoffs) are exempt.

These are proven by `tests/test_failure_drill.py` (the drill as mechanism, not a
process kill): single-winner claim under contention, orphan → reap → re-claim, and
idempotent re-emit.

## Install (single box)

```bash
# one-time
sudo useradd -r -s /usr/sbin/nologin osiris
sudo install -d -o osiris /opt/osiris /var/lib/osiris/artifacts /etc/osiris
sudo cp deploy/osiris.env.example /etc/osiris/osiris.env   # then edit
# deploy the code + venv to /opt/osiris, run `uv sync`, `alembic upgrade head`

sudo cp deploy/osiris-api.service deploy/osiris-worker.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now osiris-api osiris-worker
```

Both units are `Restart=always`. Postgres and Redis are the only shared dependencies;
run them as their own services (or containers) the units order after.

## Postgres tuning (thread e6fd3772 piece 1)

`deploy/postgresql.conf` has the justified values (measured against this box's own
osiris-pg, not guessed from a generic sizing guide) and where each apply path draws
from it: `deploy/up.sh`'s own `docker run` and `deploy/docker-compose.full.yml`'s
postgres service both carry the same `-c` flags for a FRESH container. For an EXISTING
running container (this box's own hand-run `osiris-pg`, or any other already-standing
instance), apply without a data-volume remount via `deploy/postgres_tuning.sql`:

```bash
docker exec -i osiris-pg psql -U osiris -d osiris -f deploy/postgres_tuning.sql
docker restart osiris-pg   # shared_buffers + maintenance_work_mem need this;
                            # the rest reload on the SIGHUP the script's own
                            # pg_reload_conf() call already sent
```

## The heartbeat + user-level units (single-operator dev box)

The units above are **system** units (a service account, `/opt` deploy, `EnvironmentFile`).
On the single-operator box where Osiris IS the operator's own workspace, the same processes
install as **user** units — running as you, against your dev instance and the repos in your
home dir. Four are relevant there, all sourced from `deploy/user/*.service`:

| Unit | Process | Role |
|------|---------|------|
| `osiris-mcp` | `python -m src.mcp_server` | the fleet's shared MCP floodgate (streamable-http, one pool) |
| `osiris-worker` | `arq …WorkerSettings` | the tripwire (evaluator + ticks), cascade drain, reaper — turns the kernel from a *lens* into a *tripwire* |
| `osiris-console` | `uvicorn --factory create_app` | the read-only membrane lens + console |
| `osiris-pulse` | `python -m src.orchestrator.pulse --watch N` | the developer-persona **heartbeat**: senses which repos' HEAD moved, re-ingests, re-runs the lenses, records the delta as findings (read back via the `pulse-digest` lens). Read-only on the repos. |

**REPO-MANAGED** (thread e6fd3772 piece 3-infra): `osiris deploy` installs every
`deploy/user/*.service` file over `~/.config/systemd/user/` and daemon-reloads only if
something changed — none of these are hand-authored or hand-diverged from git any more. First
install (one time; `osiris deploy` handles every deploy after this):

```bash
mkdir -p ~/.config/systemd/user
cp deploy/user/*.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now osiris-mcp osiris-worker osiris-console osiris-pulse
loginctl enable-linger "$USER"   # keep them running after logout / across reboots
```

The heartbeat's liveness is a **dead-man's-switch computed on read**: `pulse-digest` leads with
a status row that re-derives staleness every time you look, so if the pulse unit dies the digest
says *heartbeat DEAD since &lt;time&gt;* — the alarm can't die with the daemon that rings it.

## Full topology as one stack (containers)

The whole ring set — Postgres, Redis, a one-shot migration, the API, the worker, and
an opt-in placeful satellite — is declared in
[`../deploy/docker-compose.full.yml`](../deploy/docker-compose.full.yml). The surfaces
share one PG+Redis bus (no RPC mesh); migrations gate the app start; the API and worker
are distinct services (the API uses the image's default uvicorn command, the worker
overrides it to `arq`). Build + run:

```bash
docker compose -f deploy/docker-compose.full.yml up -d --build
docker compose -f deploy/docker-compose.full.yml --profile satellite up -d   # + a satellite
```

On a box without the compose plugin yet, `deploy/up.sh` brings the same topology up with
infra as containers and the API/worker as local processes (`deploy/down.sh` tears down).
The manifest is guarded by `tests/test_deploy_topology.py` so it can't silently drift.

## Off-box Claude Code session hooks

The session-lifecycle hooks (`osiris_whisper.py`/SessionStart, `osiris_precompact.py`/
PreCompact, `osiris_spawn.py`/SubagentStart+Stop, `osiris_sessionend.py`/SessionEnd) post
to the MCP server over loopback by default — correct only when the Claude Code session
runs on the SAME box as the worker. A session on a different machine (a second dev box, a
container host) needs each hook pointed at the real worker URL, or its session-death
seams (compaction, session end) never reach the fleet's worker at all — invisibly, since
every hook is fail-open and stays silent on stdout. Set, wherever that machine's Claude
Code hooks run:

```bash
OSIRIS_AUTOMOUNT_URL=http://<worker-host>:8790/automount
OSIRIS_SWEEP_URL=http://<worker-host>:8790/sweep
OSIRIS_SPAWN_URL=http://<worker-host>:8790/spawn
OSIRIS_SESSION_END_URL=http://<worker-host>:8790/session-end
```

Each hook logs which URL it posted to and whether it connected to its own stderr, so a
misconfigured off-box machine is diagnosable from its own hook output rather than a
silent gap in the graph.

## Operational tail (before a beat goes live for a user)

A watch that pages a real operator needs guards the demo doesn't:

- **Alert throttle (the 3am-false-alert guard).** The durable `alerts` row is always
  written; only *delivery* is rate-capped — `OSIRIS_ALERT_MAX_PER_WINDOW` per
  `OSIRIS_ALERT_WINDOW_SECS` per watch, and never the same (watch, object) inside
  `OSIRIS_ALERT_COOLDOWN_SECS`. A burst floods the table, not the operator; suppressed
  deliveries are logged and readable at `GET /alerts`.
- **Delivery sink.** A watch with a `webhook_url` POSTs there; else, if `OSIRIS_ALERT_EMAIL`
  is set, it emails (`OSIRIS_SMTP_HOST`/`_PORT`/`_USER`/`_PASSWORD`); else it logs. Email
  requested but SMTP unset → recorded-only + a warning, never a crash.
- **Worker dead-man's-switch.** The worker heartbeats every 30s; `GET /health/worker`
  returns `{status: ok|stale|never, age_secs}` (`stale` past
  `OSIRIS_WORKER_HEARTBEAT_STALE_SECS`). Point an uptime check at it so a silently-dead
  tripwire is visible, not discovered via a missed alert.
- **Backups.** The event-sourced graph is the asset. `deploy/backup.sh` writes a
  timestamped, pruned `pg_dump`; `deploy/restore.sh` restores one (drops + recreates the
  schema first). Cron the backup daily and **test the restore** against a throwaway DB —
  an untested backup is not a backup.

  ```bash
  DATABASE_URL=… OSIRIS_BACKUP_DIR=/var/backups/osiris KEEP=14 deploy/backup.sh
  DATABASE_URL=…(throwaway) deploy/restore.sh /var/backups/osiris/osiris-<stamp>.dump
  ```

## The connection envelope at scale (task #180 piece 2)

> **Stated max-workers number: ~1,000 concurrent Claude Code sessions is safe on this
> box's current pool sizes, at a statusline render cadence of 5 seconds or slower.**
> pgbouncer is REFUTED at that scale — see "THE DECISION" near the end of this section
> for the arithmetic. If you've landed here from a stranger's box wanting the one
> number, that's it; read on only for the reasoning and the thresholds that would change
> it.

Before this work, every statusline render and every Stop-hook turn-end opened its OWN
`asyncpg.connect()` — a per-process fork, not a pool checkout. Measured live (Thoth,
2026-08-18): 138 tx/s and 23 backends against an idle fleet of 16 sessions — connection
count scaled with **live sessions × render/turn frequency**, unbounded, heading straight
into `max_connections` the moment the fleet grew past a few dozen concurrent sessions.
`/heartbeat` and `/stop` (both on `osiris-mcp`'s own shared, bounded pool) replace that:
a session's statusline render or turn-stop is now a function call multiplexed over a
FIXED pool, not a new connection. This section is the arithmetic for how far that
actually reaches, not a recommendation — the numbers below are measured on this box, not
assumed.

### Fixed daemon budget vs `max_connections`

This box's Postgres runs at `max_connections = 100` (`deploy/postgresql.conf`). Four
long-running daemons each hold a BOUNDED pool (`osiris_{mcp,worker,api,manager}_pool_size`,
`src/config/settings.py`):

| Daemon | Pool cap (`max_size`) | `application_name` |
|---|---|---|
| `osiris-mcp` | 20 | `osiris-mcp` |
| `osiris-worker` | 16 (raised from 10, msg 5340 — see the hour-long measurement below) | `osiris-worker` |
| `osiris-console` (api) | 10 | `osiris-console` |
| `osiris-manager` | 10 | `osiris-manager` |
| **Fixed budget** | **56 / 100** | — |

That leaves **~41 connections of headroom** (100 − 56 − Postgres's own
`superuser_reserved_connections`, typically 3) for ad hoc CLI invocations, ingest
scripts, and anything else not yet routed through a named, bounded pool — several such
call sites still exist (`src/cli.py`'s per-subcommand pools, the `src/ingest/*` scripts,
`ontology/ingest_cli.py`, `bootstrap.py`, `satellite.py`), each opening its own untagged,
unbounded-by-settings connection per invocation. They are BURSTY, not steady-state, so
they share the 41-connection headroom rather than each getting a dedicated cap.

**The critical decoupling**: this fixed 56-connection budget does NOT grow with fleet
size any more. A session's marginal Postgres-connection cost, post-#180, is ZERO
steady-state — its statusline/stop traffic is absorbed into `osiris-mcp`'s already-open
20-connection pool. What scales with fleet size now is **request throughput against that
pool**, not raw connection count.

### Per-session request cost (measured live, this box, 2026-08-18)

`/heartbeat` (statusline render, the heavier of the two — mail/dm/briefs/souls/wakes/
owed/spend/resolved-identity in one composite read) and `/stop` (turn-end, deliverable
phase — one mail-count query) round-trip times over 20 samples each, against the live
20-connection `osiris-mcp` pool:

| Route | p50 | p90 | max |
|---|---|---|---|
| `/heartbeat` | 57ms | 89ms | 89ms |
| `/stop` (deliverable phase) | 16ms | 21ms | 21ms |

Naive serial-throughput ceiling per route (`pool_size / p50`, i.e. every connection
saturated back-to-back — optimistic, since asyncpg's pool checkout overlaps under
Starlette's async event loop, but a useful floor):

- `/heartbeat`: 20 conns ÷ 0.057s ≈ **350 req/s**
- `/stop`: 20 conns ÷ 0.016s ≈ **1,250 req/s** (rides the SAME pool as `/heartbeat`, so
  the two compete for the same 20 connections under load — this is a shared ceiling,
  not two independent ones)

### The 1000-session envelope

`/stop` fires once per turn-end — at 1000 concurrently active sessions averaging a turn
every ~45s, that's ≈22 req/s: trivial against either ceiling above.

`/heartbeat` fires once per statusline render. This is the actual constraint:

| Render cadence (per session) | Demand at 1000 sessions | vs 350 req/s ceiling |
|---|---|---|
| every 2s (tight — every keystroke/render) | ~500 req/s | **OVER** — the pool queues, renders lag |
| every 5s | ~200 req/s | comfortable, ~40% headroom |
| every 10s | ~100 req/s | comfortable, ~70% headroom |

Claude Code's actual statusline refresh cadence varies by client and isn't pinned by
this house — the table names the THRESHOLD, not a claimed cadence. If the fleet
approaches 1000 concurrently live sessions rendering faster than roughly every 3-4
seconds on average, `osiris_mcp_pool_size` needs raising before `/heartbeat` becomes the
bottleneck (each +10 to the pool cap buys roughly +175 req/s of ceiling, cheaply — the
pool cap costs Postgres connections, not CPU, and there's ~41 of headroom before
`max_connections` itself needs raising).

### Decision table: when pgbouncer becomes necessary

pgbouncer is NOT needed by the fixed-daemon-budget math above — 56/100 with 41 of
headroom has real slack. It becomes worth adding when ANY of these actually happens,
not preemptively:

| Trigger | Why pgbouncer specifically |
|---|---|
| A 5th+ long-running daemon needs its own bounded pool and the fixed budget would cross ~75/100, leaving under 25 for ad hoc/burst traffic | Transaction-level pooling lets many app-level "connections" share fewer real Postgres backends — buys headroom without raising `max_connections` (and its `work_mem × connections` memory cost, already the reasoning behind this box's own `max_connections=100` choice) |
| Ad hoc script/CLI concurrency (the untagged, per-invocation `create_pool` call sites above) spikes past the ~41-connection headroom during a burst — e.g. several concurrent backfill/ingest runs | Those call sites are inherently bursty and not individually worth a dedicated bounded pool each; pgbouncer absorbs the burst without touching every call site |
| `/heartbeat`'s own pool needs to grow past what's comfortable against `max_connections`'s total ceiling (i.e. raising `osiris_mcp_pool_size` repeatedly is no longer free headroom) | pgbouncer transaction pooling multiplies effective capacity per real Postgres connection, the same lever as (1), applied to the busiest single pool instead of the daemon count |

None of these were true on this box as of the first pass through this arithmetic — since
sharpened by a real hour-long measurement below, which changed one number (the worker
pool cap) and confirmed the rest.

### The hour-long measurement (msg 5340, "decided not deferred")

A hidden risk in the arithmetic above: it was built from live latency benchmarks and a
single 11-minute tx/min reading, not a sustained real-load window. Ran a proper one —
239 samples every 15s over a full hour (2026-08-18 15:16–16:16 UTC), against this box's
own real, non-synthetic fleet activity (other live seats, this session's own work, a
`pg_dump` backup that happened to fire mid-window):

| Metric | Result |
|---|---|
| Total tx/min (whole DB, measured) | ~12,482 |
| tx/min from the 4 named daemons combined | ~3,180 (mcp 2,988 + worker 120 + console 72 + manager ~0) |
| tx/min from untagged ("(unnamed)") connections | ~9,277 — **the majority of all write load on this box, not the daemons** |
| Peak backends observed, any single sample | 22 of the (then-)fixed 50-connection budget, pre-bump |
| Peak per-daemon utilization | `osiris-mcp` 13/20 (65%); **`osiris-worker` 9/10 (90%)**; `osiris-console` 2/10 (20%) |

**The untagged majority is the real finding, not a footnote.** Three-quarters of this
box's actual transaction load comes from ad hoc CLI/script connections — the class this
section already named as sharing the ~41-connection headroom, never individually pooled.
This is exactly where pgbouncer's transaction-level multiplexing would pay for itself
FIRST if it ever needs to — not the four named daemons, whose combined load (3,180
tx/min, ~53 tx/s) is a small fraction of any of the throughput ceilings measured above.

**UPDATE (msg 5364): the ad hoc class is now tagged, not bucketed.** Every `osiris`
CLI subcommand (`osiris-cli:<subcommand>`), every standalone `src/ingest/*.py`/
`scripts/*.py` invocation (`osiris-script:<name>`), and every Stop-hook/statusline/
fleet-glance fallback connection (`osiris-hook:<name>`) now sets `application_name` —
`pool_health`'s `by_application` breakdown attributes the 74% by name on the NEXT
measurement instead of lumping it under `(unnamed)`. The apportionment above was taken
BEFORE this tagging landed; a future re-measurement should show real names where
`(unnamed)` used to dominate.

**`osiris-worker` peaking at 90% of its own cap is the one number worth acting on now,
cheaply, without pgbouncer**: `osiris_worker_pool_size` raised 10→16 in this same
change — a config-only bump, zero new infrastructure, buys real headroom on the ONE
daemon that came closest to its ceiling under real load today.

RSS: `osiris-mcp` restarted mid-window (an unrelated deploy landed at 16:04 UTC),
confounding a clean hour-long growth curve — reported honestly rather than dropped. The
observable data: a fresh `osiris-mcp` process grows from ~100MB to 490–860MB within
15–20 minutes of normal activity, consistent with the bounded-cache structural argument
below, not a slow leak (a genuine unbounded leak would keep climbing well past the
first 20 minutes, not plateau in that range).

### Why RSS shouldn't scale with worker count (structural, not measured)

`osiris-mcp`'s hot in-memory state is already capped, independent of fleet size
(Thoth DM 2795, the 1G-OOM follow-up): `_agents`/`_seam_rows`/`_seam_pcts` (256-entry LRU
caches) and `sessions._wake_verdict` (4096-entry) all prune back to half their cap once
exceeded. Past those caps, an ADDITIONAL concurrent worker costs ~0 marginal RSS — its
entry evicts an older one rather than growing the dict. The one deliberately unbounded
cache (`_prev_seen`, paired 1:1 with `_agents` keys) is the smallest and least-frequently
written of the four by its own design comment, so its unbounded growth is a slow,
bounded-in-practice cost, not the multiplier the OLD per-session-connection design would
have been.

### THE DECISION: REFUTED, not deferred

**pgbouncer is not warranted at 1,000 workers on this box's current architecture.**
Numbers, not a guess: the fixed 56-connection daemon budget doesn't grow with worker
count (§ above); the daemons' own real measured load (3,180 tx/min combined) is a small
fraction of the ~350 req/s `/heartbeat` ceiling; RSS growth is capped by bounded in-
process caches, not worker count. **Stated max-workers number: ~1,000 concurrent
sessions is safe under this box's current pool sizes at a render cadence of 5 seconds or
slower** (the crossover into `/heartbeat` pool contention lands around 700–800 workers
only at a tight 2–3 second cadence — see the render-cadence table above). The action
this measurement actually earned is the cheap one already applied: `osiris_worker_pool_
size` 10→16, closing the one real headroom gap this hour's data found. Revisit pgbouncer
if a future measurement shows the untagged/ad-hoc connection class itself approaching the
~41-connection headroom during a genuine burst — that is where it would help first, not
the four named daemons.

## Later cuts (not yet)

When a pool bites, split the worker by resource class (light federators / heavy
extractors / vantage-bound browsers) — same code, more units. When adoption forces
multi-user, managed Postgres/Redis + scaled surfaces, and the placeful satellite moves
to its own box/vantage (it already reaches only Postgres). See the ROADMAP cut sequence.
