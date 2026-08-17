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
home dir. Two are relevant there:

| Unit | Process | Role |
|------|---------|------|
| `osiris-worker` | `arq …WorkerSettings` | the tripwire (evaluator + ticks), cascade drain, reaper — turns the kernel from a *lens* into a *tripwire* |
| `osiris-pulse` | `python -m src.orchestrator.pulse --watch N` | the developer-persona **heartbeat**: senses which repos' HEAD moved, re-ingests, re-runs the lenses, records the delta as findings (read back via the `pulse-digest` lens). Read-only on the repos. |

`deploy/osiris-pulse.service` is the user-unit template (its header documents the install).
Install both:

```bash
mkdir -p ~/.config/systemd/user
cp deploy/osiris-pulse.service ~/.config/systemd/user/
# author ~/.config/systemd/user/osiris-worker.service from deploy/osiris-worker.service in the
# same user-level form: drop User=/EnvironmentFile=, set WorkingDirectory=<repo>, put the DB/
# Redis URLs inline as Environment=, and use the absolute venv arq (user units get no shell PATH).
systemctl --user daemon-reload
systemctl --user enable --now osiris-pulse osiris-worker
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

## Later cuts (not yet)

When a pool bites, split the worker by resource class (light federators / heavy
extractors / vantage-bound browsers) — same code, more units. When adoption forces
multi-user, managed Postgres/Redis + scaled surfaces, and the placeful satellite moves
to its own box/vantage (it already reaches only Postgres). See the ROADMAP cut sequence.
