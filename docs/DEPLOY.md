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
[`KEYS.md`](KEYS.md). First install: `osiris deploy` creates the key and the backup password when they are
missing, before it restarts the services, so there is no key command to run. The one step
that needs you, in your own terminal as whichever user the services run as, is enrolling
your security key:

```bash
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
- **Clean-up by default.** Leftovers used to pile up until the disk was full, because nothing
  ever removed them. `osiris-cleanup.timer` (daily, installed by `osiris deploy` with the other
  timers) runs `scripts/osiris_cleanup.py --apply`, which removes: entries under
  `/var/tmp/osiris-scratch` and `pt-<pid>` test directories in `/var/tmp` and `/tmp` whose newest
  file is older than two days and that no running process is working inside; merged, clean
  worker worktrees under `.claude/worktrees/` that have been idle three days, with their merged
  branches (a generated `.venv` or tool cache is not work; any other uncommitted or untracked
  file keeps the worktree); and Docker's dangling images, build cache older than a week, and
  anonymous volumes that no container references. It never runs `docker system prune` or
  `docker volume prune` and never touches a named volume, so the database's own volume is
  outside every rule. Run it by hand without `--apply` to see exactly what it would do and how
  much it would free; `--json` prints the plan as data.
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

> **Summary: on today's per-request cost, this machine's pools (8 for `osiris-mcp`, 4
> for `osiris-worker`) serve roughly 200 concurrent Claude Code sessions at a statusline
> render cadence of 15 seconds (about 100 with half the capacity kept free), not
> 1,000.** The pools themselves are not the limit and are not saturating (measured
> below); the limit is that a statusline render now costs about 0.6 seconds of one
> connection, ten times the 57 ms this section was first built on. pgbouncer is still
> not needed. Read on for the measurement and what would change the number.

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

This machine's Postgres runs at `max_connections = 72`. That number is not set by hand:
`pg_autotune` derives it as the fixed daemon budget below plus a stated 40-connection
headroom target (`compute_recommended`, `src/orchestrator/pg_autotune.py`), persists it
with `ALTER SYSTEM`, and it takes effect at the next Postgres restart (read live on
2026-10-12: `max_connections` 72, from `postgresql.auto.conf`). `deploy/postgresql.conf`
still carries the older hand-set values (including a `max_connections=100` comment) as a
historical record; it is not what is running. Four long-running daemons each hold a
bounded pool
(`osiris_{mcp,worker,api,manager}_pool_size`, `src/config/settings.py`):

| Daemon | Pool cap (`max_size`) | `application_name` |
|---|---|---|
| `osiris-mcp` | 8 | `osiris-mcp` |
| `osiris-worker` | 4 | `osiris-worker` |
| `osiris-console` (api) | 10 | `osiris-console` |
| `osiris-manager` | 10 | `osiris-manager` |
| **Fixed budget** | **32 / 72** | |

These are the settings' current defaults (`src/config/settings.py`), and they are what
runs: the units and drop-ins for all four daemons set no pool-size override, and
neither does the repository's `.env` (checked 2026-10-12). The hour-long measurement in
its own section below was taken against an earlier, larger pair of caps (`osiris-mcp`
20, `osiris-worker` 16). Both were cut together on 2026-09-07 (`osiris-mcp` 20 to 8 and
`osiris-worker` 16 to 4; `git log -G'osiris_(mcp|worker)_pool_size: int' --
src/config/settings.py` finds the commit), deliberately and
for an unrelated reason: a dispatch to shrink Postgres `shared_buffers` (9.7 GB to 3
GB) and to flatten the worker's boot-time connection spike made each idle backend's
memory cost more, and the commit records that the MCP daemon's own concurrency "never
measured near its old ceiling". It was not a throughput finding. The premise has since
moved: `pg_autotune` now sets `shared_buffers` from available RAM (9,830 MB read live
on 2026-10-12), so whether the caps should come back up is a question for a fresh
measurement, not something this section can settle. The next sections re-derive the
arithmetic from the current 8/4/10/10 defaults; read the hour-long measurement section
for how the underlying per-request latencies were obtained, not for the pool sizes it
names.

That leaves **37 connections of headroom** (72 minus 32 minus Postgres's own
`superuser_reserved_connections`, 3) for ad hoc CLI invocations, ingest scripts, and
anything else not yet routed through a named, bounded pool. (`pool_health` reports 40,
the same figure before the 3 reserved connections.) Several such call sites still exist
(`src/cli.py`'s per-subcommand pools, the `src/ingest/*` scripts,
`ontology/ingest_cli.py`, `bootstrap.py`, `satellite.py`), each opening its own
untagged, unbounded-by-settings connection per invocation. They are bursty, not
steady-state, so they share the 37-connection headroom rather than each getting a
dedicated cap.

**The key point**: this fixed 32-connection budget does not grow with fleet size any
more. A session's marginal Postgres-connection cost is now zero at steady state: its
statusline/stop traffic is absorbed into `osiris-mcp`'s already-open 8-connection
pool. What scales with fleet size now is **request throughput against that pool**, not
raw connection count.

One live reading, a single sample: on 2026-10-12 `pg_stat_activity` held 15 client
backends in all. `osiris-mcp` held 8 of its 8 (4 active), `osiris-worker` 4 of 4 (1
active), `osiris-console` 1 of 10 and `osiris-manager` 1 of 10, plus the reading's own
connection. A pool holding all its connections open is normal; it is idle retention, not
saturation, and the 61-minute sample below is what says how busy they really are.

### Per-session request cost (measured live, 2026-10-13)

`/heartbeat` (statusline render, the heavier of the two: mail/dm/briefs/souls/wakes/
owed/spend/resolved-identity in one composite read) and `/stop` (turn-end, deliverable
phase: mail-count and stale-obligation reads) were timed over 61 minutes on the live
`osiris-mcp` (8-connection pool, about 9 live sessions, the real fleet at its normal
load), one probe of each every 10 seconds, about 300 samples each, no failures. The
`/heartbeat` probe was sent two ways: as an anonymous render (a project only), and as a
real session, which is what a statusline does (it resolves the session, its seat, its
owed obligations and its team on top of the composite read).

| Route | p50 | p95 | max |
|---|---|---|---|
| `/heartbeat`, real session | 595 ms | 688 ms | 938 ms |
| `/heartbeat`, anonymous | 110 ms | 133 ms | 1,793 ms (one outlier) |
| `/stop` (deliverable phase) | 463 ms | 520 ms | 724 ms |

The first measurement of this section, in August 2026 against a 20-connection pool and
a smaller database, found 57 ms and 16 ms. Today's real-session `/heartbeat` is about ten
times slower and `/stop` about thirty times slower. The time is spent inside the request,
not waiting for a connection (the acquire-wait figures below are microseconds), so a
bigger pool would not make a render faster; it would only let more of them run at once.
Reducing the queries a render makes (the real-session path adds about 480 ms to the
anonymous one) is the cheapest lever on every number that follows.

Throughput ceiling per route if each request holds one connection for its whole
latency (`pool_size / p50`; an upper bound on what the pool can serve, since a request
that holds its connection for less than its full latency would go faster, and a
request bound by the Postgres or event-loop CPU would go slower):

- `/heartbeat`: 8 connections / 0.595 s is about **13 req/s**
- `/stop`: 8 connections / 0.463 s is about **17 req/s**. It rides the same pool as
  `/heartbeat`, so the two share the 8 connections, one ceiling and not two.

### Is the pool saturating? (measured live, 2026-10-13)

No, on every signal available. Over the same 61 minutes:

- **Backend occupancy**, sampled every 2 seconds (1,793 samples, from a separate
  connection outside the daemons' pools): `osiris-mcp` had 0.53 backends busy on average
  of 8 (p95 4, all 8 busy in 2 samples, 0.1%); `osiris-worker` had 0.13 of 4 busy on
  average (p95 1, all 4 busy in 1 sample). Both pools hold all their connections open
  nearly all the time, which is idle retention, not load.
- **Acquire wait** (queueing for a free connection, which `pg_stat_activity` cannot see;
  read from `osiris-mcp`'s own recent window of its last 1,000 acquires, 21 windows taken
  three minutes apart): p50 0.011 to 0.017 ms, p99 0.039 ms or less, worst single wait
  15.8 ms once and 0.23 ms or less in every other window. These windows cover only the
  most recent seconds of traffic each, so they show typical queueing well and a rare
  burst poorly.
- **Starvation**: no starvation, acquire-timeout or too-many-connections line appears in
  seven days of the journals of all four daemons (892,890 lines for `osiris-mcp`).
- **Real demand**: the `osiris-mcp` access log for the same seven days shows 104,369
  `/heartbeat` requests; the busiest minute had 82 of them (1.4 req/s), the 99th
  percentile minute 1.0 req/s, and a typical active minute 12 (0.2 req/s). Against the
  13 req/s ceiling that is a peak near 10%, which agrees with the occupancy above: 1.4
  req/s at 0.6 s each holds 0.8 connections.

`osiris-worker`'s own acquire waits cannot be read from outside its process (only the
`osiris-mcp` window is exposed through `fleet()`), so for the worker the evidence is
occupancy and the journal, not wait time. It is idle on both.

### How many sessions this serves

`/heartbeat` fires once per statusline render. Sessions served at the 13 req/s ceiling,
and with half the ceiling kept free for bursts, by per-session render cadence:

| Render cadence (per session) | Sessions at the ceiling | Sessions with half the ceiling free |
|---|---|---|
| every 2s | about 27 | about 13 |
| every 5s | about 67 | about 33 |
| every 10s | about 134 | about 67 |
| every 15s | about 201 | about 100 |
| every 30s | about 402 | about 201 |
| every 60s | about 804 | about 402 |

For comparison, the fleet at measurement time had about 9 live sessions and peaked at
1.4 req/s, which is a render about every 6 to 7 seconds per session if all nine were
rendering (the live-session count at the busiest minute was not recorded). `/stop`
fires once per turn-end, far less often per session than a render, and shares the pool.
At 1,000 concurrent sessions rendering every 15 seconds the demand would be about 67
req/s, five times the ceiling; reaching 1,000 sessions needs either a cadence near 75
seconds or a much cheaper render.

If the session count grows toward the figures above, the levers in order: cut the
queries a real-session render makes (it is the whole cost); then `osiris_mcp_pool_size`
8 to 16, which doubles the ceiling to about 27 req/s if Postgres has CPU to spare (not
measured here). A pool bump raises the fixed budget from 32 to 40, and `pg_autotune`
derives `max_connections` as the budget plus 40, so the setting becomes 80; it takes
effect only at a Postgres restart, which is the operator's decision, and the three
reserved connections leave 37 for ad hoc use either way.

### Decision table: when pgbouncer becomes necessary

pgbouncer is not needed by the fixed-daemon-budget math above: 32/72 with 37 of
headroom is thinner than the 65 this section once claimed, but still has slack. It
becomes worth adding when any of these actually happens, not preemptively:

| Trigger | Why pgbouncer specifically |
|---|---|
| A 5th+ long-running daemon needs its own bounded pool and the fixed budget would grow until the ad hoc headroom (`max_connections` minus the budget minus 3) falls under about 25 | Transaction-level pooling lets many app-level "connections" share fewer real Postgres backends. It buys headroom without raising `max_connections` (and its `work_mem x connections` memory cost, already the reasoning behind this machine's own `max_connections=100` choice) |
| Ad hoc script/CLI concurrency (the untagged, per-invocation `create_pool` call sites above) spikes past the 37-connection headroom during a burst, for example several concurrent backfill/ingest runs | Those call sites are inherently bursty and not individually worth a dedicated bounded pool each; pgbouncer absorbs the burst without touching every call site |
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

(The figures this measurement section names, a
50-connection budget and 41 of headroom, were the values at the time. The live numbers are
32 of 72 and 37 above.)

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

**pgbouncer is not warranted on the current architecture.** The fixed 32-connection
daemon budget (of 72) doesn't grow with worker count; neither pool is saturating
(measured above: 0.1% of samples with every connection busy, acquire waits in
microseconds, no starvation in seven days); and RSS growth is capped by bounded
in-process caches, not worker count. pgbouncer multiplies connections, and connections
are not what limits this machine: the limit is the 0.6 seconds a real-session render
now holds a connection, which pgbouncer would not shorten. **Summary: about 200
concurrent sessions at a 15-second render cadence on the current pools** (derived from
the latency measured on 2026-10-13; the 1,000-session figure this section started from
rested on the August latency and does not hold). Revisit pgbouncer if a future
measurement shows the untagged/ad-hoc connection class itself approaching the
37-connection headroom during a genuine burst; that is where it would help first, not the
four named daemons.

## Later cuts (not yet)

When a pool becomes a bottleneck, split the worker by resource class (light versus
heavy tasks, browser-bound work), same code, more units. When adoption forces
multi-user use, managed Postgres/Redis plus scaled surfaces, and the location-aware
satellite service moves to its own machine (it already reaches only Postgres). See the
ROADMAP cut sequence.
