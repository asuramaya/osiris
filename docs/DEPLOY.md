# Deployment

Osiris separates by **blast radius**, not by feature. Two long-running processes,
coordinating only through Postgres + Redis (no RPC mesh between them):

| Unit | Process | Role | If it dies |
|------|---------|------|-----------|
| `osiris-api` | `uvicorn src.api.app:app` | the human surface (read API + console + SSE) | the console is briefly down; the truth and the worker are untouched |
| `osiris-worker` | `arq src.workers.arq_worker.WorkerSettings` | the cascade drain, the watch (evaluator + ticks), the reaper | crawls pause; on restart it re-claims orphaned runs and resumes the durable outbox — no work lost, nothing double-emitted |

This is **Cut 1** of the deployment ladder in [`../ROADMAP.md`](../ROADMAP.md): same box,
two units, fate-isolated. Everything before this ran in one process.

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

## Later cuts (not yet)

When a pool bites, split the worker by resource class (light federators / heavy
extractors / vantage-bound browsers) — same code, more units. When adoption forces
multi-user, managed Postgres/Redis + scaled surfaces, and the placeful satellite moves
to its own box/vantage (it already reaches only Postgres). See the ROADMAP cut sequence.
