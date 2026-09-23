<!-- topic: deployment -->

# Deployment

Osiris separates by **blast radius**, not by feature. Two long-running processes,
coordinating only through Postgres + Redis (no RPC mesh between them):

| Unit | Process | Role | If it dies |
|------|---------|------|-----------|
| `osiris-api` | `uvicorn src.api.app:app` | the human surface (read API + console + SSE) | the console is briefly down; the truth and the worker are untouched |
| `osiris-worker` | `arq src.workers.arq_worker.WorkerSettings` | the cascade drain, the watch (evaluator + ticks), the reaper | crawls pause; on restart it re-claims orphaned runs and resumes the durable outbox, no work lost, nothing double-emitted |

This is **Cut 1** of the deployment sequence in [`../ROADMAP.md`](../ROADMAP.md): same
machine, two units, fate-isolated. Everything before this ran in one process.

## Routine deploys: `osiris deploy`

Everything below this section is about standing the system up for the first time. Once
it's up, the day-to-day deploy is one command: `osiris deploy` (see
[`CLI.md`](CLI.md#osiris-deploy) for the full process). It restarts the services that
hold new code, proves they came back healthy, and catches anything that needed a seed or
a migration and didn't get one. It refuses outright on an uncommitted change under
`src/` rather than silently shipping a half-written edit, which is exactly the failure a
manual `systemctl --user restart osiris-mcp osiris-worker osiris-console && python
scripts/osiris_smoke.py` sequence used to risk, caught only by a well-timed, manually run
`git status`.

## The deploy snapshot: `~/.local/bin/osiris` never runs a test candidate's tree

`~/.local/bin/osiris` is a symlink, but it does **not** point at
`~/code/osiris/.venv/bin/osiris` (the main checkout's own editable install). It points
into a separate worktree, `~/.local/share/osiris/deployed`, pinned at whatever commit
`osiris deploy` last recorded as successfully deployed. `osiris deploy` itself is always
run from the checkout; only the operator-facing shortcut moves.

**Why this exists:** the three long-running services (`osiris-mcp`, `osiris-worker`,
`osiris-console`) run in-process, surviving a test run untouched. They're only ever
restarted by a green `osiris deploy`. The CLI used to be a plain editable-install symlink
into the main checkout, which has no equivalent protection: a CI run merges a candidate
branch into that checkout for the duration of its test run, and anything invoking
`~/.local/bin/osiris` in that window, including the operator, by hand, ran the
candidate's code, not the deployed one. This happened for real once: `osiris resume`
died with `SoulKeyMissing` mid-test-run, because the checkout momentarily carried an
in-flight encryption change that predated the operator's own session.

**The fix, on every green deploy only** (never on smoke failure; a broken deploy must
not become the operator's next CLI): `scripts/update_deploy_snapshot.sh` moves (or
creates, on a fresh machine) `~/.local/share/osiris/deployed` to the deployed commit via
`git worktree add --detach` / `git checkout --detach`, runs `uv sync` inside that
worktree (a genuinely separate `.venv`, its own editable install rooted at the snapshot,
not the checkout), and atomically retargets `~/.local/bin/osiris` at the snapshot's own
`.venv/bin/osiris`. See [`CLI.md`](CLI.md#osiris-deploy) for where this sits in the
deploy process's own step order.

## The discipline: nothing heavy runs in a request path

The API **enqueues** heavy work onto the worker and returns immediately; it never runs
a crawl in its own event loop. Concretely, `POST /cases/{id}/expand` enqueues the
`expand_case_job` Arq job. The worker executes it, and the SSE stream
(`GET /cases/{id}/stream`) surfaces progress by reading the same Postgres the worker
writes to. A runaway expansion therefore can never block or crash the console.

## Why the cut is safe (the reliability is in the kernel)

The worker can be killed and restarted at any time because the kernel already
guarantees:

- **Atomic claim**: the `helper_runs_active_claim` partial unique index admits exactly
  one worker per `(helper, object, case, window)`. Two workers never double-dispatch.
- **Durable outbox**: cascade events are written in the same transaction as the data
  change; a restarted worker drains whatever is unpublished. Nothing is fire-and-forget.
- **Idempotent emit**: `create_or_find_object` is find-or-create on canonical, and
  assertions supersede within-source, so re-running a reaped job can't fork the graph.
- **The reaper**: a worker that dies *mid-run* leaves a stuck `running` row that the
  claim index would block forever. `reap_stale_runs` (a worker cron) resets such
  orphans to `failed` after a timeout, releasing the claim so the restart re-claims a
  fresh run. Human-wait states (24h handoffs) are exempt.

These are proven by `tests/test_failure_drill.py` (the drill as mechanism, not a
process kill): single-winner claim under contention, orphan → reap → re-claim, and
idempotent re-emit.

## Install (single machine)

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

## Postgres tuning

`deploy/postgresql.conf` has the justified values (measured against this machine's own
`osiris-pg`, not guessed from a generic sizing guide) and where each apply path draws
from it: `deploy/up.sh`'s own `docker run` and `deploy/docker-compose.full.yml`'s
postgres service both carry the same `-c` flags for a fresh container. For an existing
running container (a hand-run `osiris-pg` on this machine, or any other already-standing
instance), apply without a data-volume remount via `deploy/postgres_tuning.sql`:

```bash
docker exec -i osiris-pg psql -U osiris -d osiris -f deploy/postgres_tuning.sql
docker restart osiris-pg   # shared_buffers + maintenance_work_mem need this;
                            # the rest reload on the SIGHUP the script's own
                            # pg_reload_conf() call already sent
```

## The heartbeat and user-level units (single-operator dev machine)

The units above are **system** units (a service account, `/opt` deploy,
`EnvironmentFile`). On a single-operator machine where Osiris is the operator's own
workspace, the same processes install as **user** units, running as you, against your
dev instance and the repos in your home directory. Four are relevant there, all sourced
from `deploy/user/*.service`:

| Unit | Process | Role |
|------|---------|------|
| `osiris-mcp` | `python -m src.mcp_server` | the fleet's shared MCP endpoint (streamable-http, one connection pool) |
| `osiris-worker` | `arq …WorkerSettings` | the evaluator, tick scheduler, cascade drain, and reaper: turns the kernel from a read-only view into an active monitor |
| `osiris-console` | `uvicorn --factory create_app` | the read-only console and viewing surface |
| `osiris-pulse` | `python -m src.orchestrator.pulse --watch N` | a periodic background process: senses which repos' HEAD moved, re-ingests, re-runs the analysis passes, and records the delta as findings (read back via the `pulse-digest` tool). Read-only on the repos. |

**Managed from the repository:** `osiris deploy` installs every `deploy/user/*.service`
file over `~/.config/systemd/user/` and reloads the daemon only if something changed;
none of these are hand-authored or hand-diverged from git any more. First install (one
time; `osiris deploy` handles every deploy after this):

```bash
mkdir -p ~/.config/systemd/user
cp deploy/user/*.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now osiris-mcp osiris-worker osiris-console osiris-pulse
loginctl enable-linger "$USER"   # keep them running after logout / across reboots
```

The pulse process's own liveness is a **dead-man's-switch computed on read**:
`pulse-digest` leads with a status row that re-derives staleness every time you look, so
if the pulse unit dies the digest says *heartbeat DEAD since &lt;time&gt;*. The alarm
can't die along with the daemon that would otherwise ring it.

## Soul-store encryption

The soul store, the byte-exact transcript archive (`soul_lines`/`soul_lines_cold`), is
encrypted at rest by Osiris itself, one tier above host-disk trust, using a key sealed as a
systemd user credential by default (never a plaintext file unless you explicitly choose
`--backend file`). The full custody mechanism, every `osiris soul-key`/`osiris restic-key`
action, the Security Key recovery flow, and what happens with no key at all: see
[`KEYS.md`](KEYS.md). First install, in your own terminal as whichever user the services run
as:

```bash
osiris soul-key init
osiris soul-key enroll-recovery
```

The wiring is already shipped in `deploy/user/osiris-mcp.service`/`osiris-worker.service`.
`osiris deploy` installs it; there is nothing to hand-edit. Both services import the
sealed credential, which systemd resolves against the standard per-user credential store
(`~/.config/credstore.encrypted/soul.key`) and decrypts before the process starts, with
no separate decryption step needed at read time. This import tolerates a missing
credential at service start, unlike an older configuration that failed the service's own
start outright when the credential didn't exist yet. That older behavior created a
bootstrap deadlock, since minting the key normally means running the command line
through the already-deployed console, which needs the service running first.
`Environment=OSIRIS_SOUL_KEY_FILE=%h/.config/osiris/soul.key` also stays on both
services: a logical path the command line resolves related files against, not where the
sealed key itself lives.

A missing key does not stop either service from starting. See KEYS.md's "What happens with
no key" section for the deliberate behavior and the exact warning text.

## Full topology as one stack (containers)

The whole set of components, Postgres, Redis, a one-shot migration, the API, the worker,
and an opt-in location-aware satellite service, is declared in
[`../deploy/docker-compose.full.yml`](../deploy/docker-compose.full.yml). The surfaces
share one Postgres+Redis backend (no RPC mesh between them); migrations gate the app
start; the API and worker are distinct services (the API uses the image's default
uvicorn command, the worker overrides it to `arq`). Build and run:

```bash
docker compose -f deploy/docker-compose.full.yml up -d --build
docker compose -f deploy/docker-compose.full.yml --profile satellite up -d   # + a satellite
```

On a machine without the compose plugin yet, `deploy/up.sh` brings the same topology up
with infra as containers and the API/worker as local processes (`deploy/down.sh` tears
down). The manifest is guarded by `tests/test_deploy_topology.py` so it can't silently
drift.

## Claude Code session hooks running on a different machine

The session-lifecycle hooks (`osiris_hook.py whisper`/SessionStart, `osiris_hook.py
precompact`/PreCompact, `osiris_hook.py spawn`/SubagentStart+Stop, `osiris_hook.py
session-end`/SessionEnd) post to the MCP server over loopback by default. This is
correct only when the Claude Code session runs on the same machine as the worker. A
session on a different machine (a second dev machine, a container host) needs each hook
pointed at the real worker URL, or its session-lifecycle events (compaction, session
end) never reach the fleet's worker at all, invisibly, since every hook is fail-open and
stays silent on stdout. Set, wherever that machine's Claude Code hooks run:

```bash
OSIRIS_AUTOMOUNT_URL=http://<worker-host>:8790/automount
OSIRIS_SWEEP_URL=http://<worker-host>:8790/sweep
OSIRIS_SPAWN_URL=http://<worker-host>:8790/spawn
OSIRIS_SESSION_END_URL=http://<worker-host>:8790/session-end
```

Each hook logs which URL it posted to and whether it connected, to its own stderr, so a
misconfigured off-machine setup is diagnosable from its own hook output rather than a
silent gap in the graph.

## Operational checklist (before a watch goes live for a user)

A watch that pages a real operator needs guards a demo doesn't:

- **Alert throttle (guards against a flood of false alerts).** The durable `alerts` row
  is always written; only *delivery* is rate-capped: `OSIRIS_ALERT_MAX_PER_WINDOW` per
  `OSIRIS_ALERT_WINDOW_SECS` per watch, and never the same (watch, object) inside
  `OSIRIS_ALERT_COOLDOWN_SECS`. A burst floods the table, not the operator; suppressed
  deliveries are logged and readable at `GET /alerts`.
- **Delivery sink.** A watch with a `webhook_url` posts there; else, if
  `OSIRIS_ALERT_EMAIL` is set, it emails (`OSIRIS_SMTP_HOST`/`_PORT`/`_USER`/
  `_PASSWORD`); else it logs. Email requested but SMTP unset means recorded-only, plus a
  warning, never a crash.
- **Worker dead-man's-switch.** The worker heartbeats every 30 seconds; `GET
  /health/worker` returns `{status: ok|stale|never, age_secs}` (`stale` past
  `OSIRIS_WORKER_HEARTBEAT_STALE_SECS`). Point an uptime check at it so a silently-dead
  worker is visible, not discovered via a missed alert.
- **Backups.** The event-sourced graph is the asset. `deploy/backup.sh` writes a
  timestamped, pruned `pg_dump`; `deploy/restore.sh` restores one (drops and recreates
  the schema first). Cron the backup daily and **test the restore** against a throwaway
  database: an untested backup is not a backup.

  ```bash
  DATABASE_URL=... OSIRIS_BACKUP_DIR=/var/backups/osiris KEEP=14 deploy/backup.sh
  DATABASE_URL=...(throwaway) deploy/restore.sh /var/backups/osiris/osiris-<stamp>.dump
  ```
- **`/tmp` is a shared, finite resource across every concurrent agent on this machine;
  test runs must not write there.** `/tmp` is tmpfs, RAM-backed, with a fixed inode
  ceiling (1,048,576 on the machine this was measured on) shared by every concurrent
  process, not disk space. A real incident: pytest's own basetemp (every `tmp_path`
  fixture's real file output) landed under `/tmp/pt-<pid>` regardless of `$TMPDIR`, one
  directory per invocation, never cleaned up (pytest's retention pruning only ever
  revisits its own auto-numbered default basetemp, never a caller-supplied one). One
  ordinary day of concurrent agent activity left 92 such leftover trees, accounting for
  99.1% of all files under `/tmp`. At real overnight concurrency this reached roughly 1
  million inodes, and every Bash tool call across every agent (each writes a small
  per-call output file under `/tmp`) failed with ENOSPC until it cleared, unassisted,
  hours later. Fixed at the root (`tests/conftest.py`'s `_default_basetemp()`): basetemp
  now honors `$TMPDIR` when set, and defaults to `/var/tmp` (real disk, no fixed inode
  ceiling) rather than `/tmp` when it isn't. `TMPDIR=/var/tmp/osiris-scratch` is still
  the documented convention, now actually load-bearing rather than silently ignored by
  pytest's own basetemp. `osiris_preflight`'s inode-use alarm (`_tmp_inode_pct`, 80%
  threshold) is the early warning if this class of leak ever recurs some other way; it
  is not itself the fix.
- **Obligation hygiene (a deliberate exception to the "off by default" rule below).**
  `osiris_obligation_hygiene_enabled` is true by default, unlike every sibling scheduled
  writer described above, by explicit operator decision to ship it already turned on.
  Every 15 minutes: an open `kind='obligation'` Thread idle 7+ days (its own
  `last_touched` stale and its owner silent across that window) draws one nudge message
  to its owner, or the operator's own inbox, when the owner is a project name or
  resolves to no live agent; 7 more days of continued silence past that nudge marks it a
  `hygiene_stage='stale_candidate'` and notifies the operator once. It never auto-
  resolved at either stage: this mechanism only nudges and surfaces, never closes or
  reclassifies a thread on its own authority. `obligation_hygiene.hygiene_status(pool)`
  gives counts-per-stage-per-project on demand; `hygiene_execute(execute=False)` (the
  default) previews a tick's plan without writing anything.

## The connection envelope at scale

> **Summary: roughly 1,000 concurrent Claude Code sessions is safe on this machine's
> current pool sizes, at a statusline render cadence of 5 seconds or slower.**
> pgbouncer is not needed at that scale; see "The decision" near the end of this
> section for the arithmetic. If you only want the one number, that's it; read on only
> for the reasoning and the thresholds that would change it.

Before this work, every statusline render and every Stop-hook turn-end opened its own
`asyncpg.connect()`, a per-process fork, not a pool checkout. Measured live: 138 tx/s
and 23 backends against an idle fleet of 16 sessions. Connection count scaled with
**live sessions x render/turn frequency**, unbounded, heading straight into
`max_connections` the moment the fleet grew past a few dozen concurrent sessions.
`/heartbeat` and `/stop` (both on `osiris-mcp`'s own shared, bounded pool) replace that:
a session's statusline render or turn-stop is now a function call multiplexed over a
fixed pool, not a new connection. This section is the arithmetic for how far that
actually reaches, not a recommendation. The numbers below are measured on the machine
this was tested on, not assumed.

### Fixed daemon budget vs `max_connections`

This machine's Postgres runs at `max_connections = 100` (`deploy/postgresql.conf`).
Four long-running daemons each hold a bounded pool
(`osiris_{mcp,worker,api,manager}_pool_size`, `src/config/settings.py`):

| Daemon | Pool cap (`max_size`) | `application_name` |
|---|---|---|
| `osiris-mcp` | 8 | `osiris-mcp` |
| `osiris-worker` | 4 | `osiris-worker` |
| `osiris-console` (api) | 10 | `osiris-console` |
| `osiris-manager` | 10 | `osiris-manager` |
| **Fixed budget** | **32 / 100** | |

These are the settings' current defaults. The hour-long measurement in its own section
below was taken against an earlier, larger pair of caps (`osiris-mcp` 20, `osiris-worker`
16); after that measurement, both were cut again on 2026-09-07 for an unrelated reason,
a shrunken Postgres `shared_buffers` making every idle backend's memory cost more, not a
new throughput finding. Nothing about the request-rate ceilings that measurement was
built from has been re-checked against these smaller caps since. The next two sections
re-derive that arithmetic conservatively from the current 8/4/10/10 defaults; read the
hour-long measurement section for how the underlying per-request latencies were
obtained, not for the pool sizes it names.

That leaves **roughly 65 connections of headroom** (100 minus 32 minus Postgres's own
`superuser_reserved_connections`, typically 3) for ad hoc CLI invocations, ingest
scripts, and anything else not yet routed through a named, bounded pool. Several such
call sites still exist (`src/cli.py`'s per-subcommand pools, the `src/ingest/*` scripts,
`ontology/ingest_cli.py`, `bootstrap.py`, `satellite.py`), each opening its own
untagged, unbounded-by-settings connection per invocation. They are bursty, not
steady-state, so they share the 65-connection headroom rather than each getting a
dedicated cap.

**The key point**: this fixed 32-connection budget does not grow with fleet size any
more. A session's marginal Postgres-connection cost is now zero at steady state: its
statusline/stop traffic is absorbed into `osiris-mcp`'s already-open 8-connection
pool. What scales with fleet size now is **request throughput against that pool**, not
raw connection count.

### Per-session request cost (measured live)

`/heartbeat` (statusline render, the heavier of the two: mail/dm/briefs/souls/wakes/
owed/spend/resolved-identity in one composite read) and `/stop` (turn-end, deliverable
phase: one mail-count query) round-trip times over 20 samples each, against the live
20-connection `osiris-mcp` pool that existed at measurement time:

| Route | p50 | p90 | max |
|---|---|---|---|
| `/heartbeat` | 57ms | 89ms | 89ms |
| `/stop` (deliverable phase) | 16ms | 21ms | 21ms |

These per-request latencies depend on Postgres round-trip time, not queue depth, so
they should hold roughly steady at the smaller pool cap too, up to the point the pool
itself starts queuing checkouts. Naive serial-throughput ceiling per route (`pool_size /
p50`, that is, every connection saturated back-to-back; optimistic, since asyncpg's pool
checkout overlaps under Starlette's async event loop, but a useful floor), re-derived
against the current 8-connection `osiris-mcp` cap rather than the 20 connections this
measurement actually ran against:

- `/heartbeat`: 8 connections / 0.057s is about **140 req/s** (down from 350 at the old
  20-connection cap)
- `/stop`: 8 connections / 0.016s is about **500 req/s** (down from 1,250; rides the
  same pool as `/heartbeat`, so the two compete for the same 8 connections under load,
  a shared ceiling, not two independent ones)

### The 1000-session envelope

`/stop` fires once per turn-end. At 1000 concurrently active sessions averaging a turn
every 45 seconds, that's about 22 req/s: trivial against either ceiling above.

`/heartbeat` fires once per statusline render. This is the actual constraint, re-derived
against the current 140 req/s ceiling:

| Render cadence (per session) | Demand at 1000 sessions | vs 140 req/s ceiling |
|---|---|---|
| every 2s (tight, every keystroke/render) | ~500 req/s | **over budget**: the pool queues, renders lag |
| every 5s | ~200 req/s | **over budget** (was comfortable at the old 350 req/s ceiling) |
| every 10s | ~100 req/s | tight, only ~29% headroom |
| every 15s | ~67 req/s | comfortable, ~52% headroom |

Claude Code's actual statusline refresh cadence varies by client and isn't fixed by
this project; the table names the threshold, not a claimed cadence. At the current pool
cap, 1000 concurrently live sessions need a render cadence of roughly 15 seconds or
slower to stay comfortably under the ceiling; the crossover to over-budget now lands
around a 7-second average cadence, not the 3-4 seconds the pre-cut caps allowed. If the
fleet approaches 1000 sessions rendering faster than that, `osiris_mcp_pool_size` needs
raising back up before `/heartbeat` becomes the bottleneck (each +10 to the pool cap
still buys roughly +175 req/s of ceiling, cheaply: the pool cap costs Postgres
connections, not CPU, and there's about 65 of headroom before `max_connections` itself
needs raising).

### Decision table: when pgbouncer becomes necessary

pgbouncer is not needed by the fixed-daemon-budget math above: 32/100 with 65 of
headroom has real slack. It becomes worth adding when any of these actually happens,
not preemptively:

| Trigger | Why pgbouncer specifically |
|---|---|
| A 5th+ long-running daemon needs its own bounded pool and the fixed budget would cross ~75/100, leaving under 25 for ad hoc/burst traffic | Transaction-level pooling lets many app-level "connections" share fewer real Postgres backends. It buys headroom without raising `max_connections` (and its `work_mem x connections` memory cost, already the reasoning behind this machine's own `max_connections=100` choice) |
| Ad hoc script/CLI concurrency (the untagged, per-invocation `create_pool` call sites above) spikes past the ~65-connection headroom during a burst, for example several concurrent backfill/ingest runs | Those call sites are inherently bursty and not individually worth a dedicated bounded pool each; pgbouncer absorbs the burst without touching every call site |
| `/heartbeat`'s own pool needs to grow past what's comfortable against `max_connections`'s total ceiling (raising `osiris_mcp_pool_size` repeatedly is no longer free headroom) | pgbouncer transaction pooling multiplies effective capacity per real Postgres connection, the same lever as the first row, applied to the busiest single pool instead of the daemon count |

None of these were true as of the first pass through this arithmetic, since sharpened
by a real hour-long measurement below, which changed one number (the worker pool cap)
and confirmed the rest.

### The hour-long measurement

A hidden risk in the arithmetic above: it was built from live latency benchmarks and a
single 11-minute tx/min reading, not a sustained real-load window. A proper measurement
followed: 239 samples every 15 seconds over a full hour, against this machine's own
real, non-synthetic activity (other live sessions, this session's own work, a `pg_dump`
backup that happened to fire mid-window):

| Metric | Result |
|---|---|
| Total tx/min (whole database, measured) | ~12,482 |
| tx/min from the 4 named daemons combined | ~3,180 (mcp 2,988 + worker 120 + console 72 + manager ~0) |
| tx/min from untagged ("(unnamed)") connections | ~9,277, **the majority of all write load, not the daemons** |
| Peak backends observed, any single sample | 22 of the (then-)fixed 50-connection budget, pre-bump |
| Peak per-daemon utilization | `osiris-mcp` 13/20 (65%); **`osiris-worker` 9/10 (90%)**; `osiris-console` 2/10 (20%) |

**The untagged majority is the real finding, not a footnote.** Three-quarters of the
actual transaction load comes from ad hoc CLI/script connections, the class this
section already named as sharing the (then-)41-connection headroom, never individually
pooled. This is exactly where pgbouncer's transaction-level multiplexing would pay for
itself first if it ever needs to, not the four named daemons, whose combined load
(3,180 tx/min, ~53 tx/s) is a small fraction of any of the throughput ceilings measured
above.

**The ad hoc class is now tagged, not bucketed.** Every `osiris` CLI subcommand
(`osiris-cli:<subcommand>`), every standalone `src/ingest/*.py`/`scripts/*.py`
invocation (`osiris-script:<name>`), and every Stop-hook/statusline/fleet-glance
fallback connection (`osiris-hook:<name>`) now sets `application_name`.
`pool_health`'s `by_application` breakdown attributes that 74% by name on the next
measurement instead of lumping it under `(unnamed)`. The apportionment above was taken
before this tagging landed; a future re-measurement should show real names where
`(unnamed)` used to dominate.

**`osiris-worker` peaking at 90% of its own cap is the one number worth acting on now,
cheaply, without pgbouncer**: `osiris_worker_pool_size` was raised from 10 to 16 in this
same change, a config-only bump, zero new infrastructure, buying real headroom on the
one daemon that came closest to its ceiling under real load. That headroom was later
given back: on 2026-09-07, `osiris_worker_pool_size` was cut again, to 4, for the idle-
backend-memory reason described above, not because this measurement's 90%-utilization
finding stopped being true. Nothing has re-measured `osiris-worker`'s own peak
utilization against the smaller cap since.

RSS: `osiris-mcp` restarted mid-window (an unrelated deploy landed partway through),
confounding a clean hour-long growth curve; reported honestly rather than dropped. The
observable data: a fresh `osiris-mcp` process grows from ~100MB to 490-860MB within
15-20 minutes of normal activity, consistent with the bounded-cache structural argument
below, not a slow leak (a genuine unbounded leak would keep climbing well past the
first 20 minutes, not plateau in that range).

### Why RSS shouldn't scale with worker count (structural, not measured)

`osiris-mcp`'s hot in-memory state is already capped, independent of fleet size:
`_agents`/`_seam_rows`/`_seam_pcts` (256-entry LRU caches) and
`sessions._wake_verdict` (4096-entry) all prune back to half their cap once exceeded.
Past those caps, an additional concurrent worker costs roughly zero marginal RSS: its
entry evicts an older one rather than growing the dict. The one deliberately unbounded
cache (`_prev_seen`, paired 1:1 with `_agents` keys) is the smallest and
least-frequently written of the four by its own design comment, so its unbounded growth
is a slow, bounded-in-practice cost, not the multiplier the old per-session-connection
design would have been.

### The decision: pgbouncer is not needed, at least for now

**pgbouncer is not warranted at 1,000 workers on the current architecture.** Numbers,
not a guess: the fixed 32-connection daemon budget doesn't grow with worker count (see
above); the daemons' own real measured load (3,180 tx/min combined, measured against
the larger, pre-2026-09-07 caps) is a small fraction of even the smaller, re-derived
~140 req/s `/heartbeat` ceiling; RSS growth is capped by bounded in-process caches, not
worker count. **Summary: roughly 1,000 concurrent sessions is safe under the current
pool sizes at a render cadence of 15 seconds or slower** (re-derived from the current
8-connection `osiris-mcp` cap; see the render-cadence table above). This is a tighter
threshold than the 5-second figure this section originally concluded, because the pool
caps that arithmetic was built on were cut on 2026-09-07 for an unrelated, idle-memory
reason, after this hour-long measurement was taken, and nothing has re-measured actual
throughput or worker-pool utilization against the smaller caps since. If a render
cadence faster than roughly 7-10 seconds becomes the norm at fleet scale, raising
`osiris_mcp_pool_size` back up is the first lever, cheap and config-only; revisit
pgbouncer if a future measurement shows the untagged/ad-hoc connection class itself
approaching the ~65-connection headroom during a genuine burst, that is where it would
help first, not the four named daemons.

## Later cuts (not yet)

When a pool becomes a bottleneck, split the worker by resource class (light versus
heavy tasks, browser-bound work), same code, more units. When adoption forces
multi-user use, managed Postgres/Redis plus scaled surfaces, and the location-aware
satellite service moves to its own machine (it already reaches only Postgres). See the
ROADMAP cut sequence.
