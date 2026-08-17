#!/usr/bin/env bash
# Single-box bring-up of the full topology WITHOUT the docker-compose plugin (this box
# doesn't have it yet). Infra (Postgres + Redis) runs as containers; the API and worker
# run as local processes — same rings, same PG+Redis bus, fate-isolated. Once the
# compose plugin is installed, prefer:
#   docker compose -f deploy/docker-compose.full.yml up -d --build
set -euo pipefail
cd "$(dirname "$0")/.."

PG_PORT="${PG_PORT:-5432}"
REDIS_PORT="${REDIS_PORT:-6379}"
export DATABASE_URL="postgresql://osiris:osiris@127.0.0.1:${PG_PORT}/osiris"
export REDIS_URL="redis://127.0.0.1:${REDIS_PORT}/0"

echo "[up] infra (Postgres + Redis) as containers"
# tuning flags: deploy/postgresql.conf has the full justification (thread e6fd3772
# piece 1) — override-only, never replaces the image's own generated config, so every
# other stock default (data_directory, socket paths, ...) is untouched.
docker run -d --rm --name osiris-pg -e POSTGRES_USER=osiris -e POSTGRES_PASSWORD=osiris \
  -e POSTGRES_DB=osiris -p "127.0.0.1:${PG_PORT}:5432" postgres:16 \
  -c shared_buffers=4GB -c effective_cache_size=12GB -c work_mem=32MB \
  -c maintenance_work_mem=512MB -c random_page_cost=1.1 >/dev/null
docker run -d --rm --name osiris-redis -p "127.0.0.1:${REDIS_PORT}:6379" redis:7 >/dev/null

echo "[up] waiting for Postgres"
for _ in $(seq 1 30); do docker exec osiris-pg pg_isready -U osiris >/dev/null 2>&1 && break; sleep 0.5; done

echo "[up] migrate to head"
uv run alembic upgrade head >/dev/null

echo "[up] API (uvicorn) + worker (arq) as separate processes"
uv run uvicorn src.api.app:app --host 127.0.0.1 --port 8011 >/tmp/osiris-api.log 2>&1 &
echo $! > /tmp/osiris-api.pid
uv run arq src.workers.arq_worker.WorkerSettings >/tmp/osiris-worker.log 2>&1 &
echo $! > /tmp/osiris-worker.pid

echo "[up] done. API on http://127.0.0.1:8011  (logs: /tmp/osiris-{api,worker}.log)"
echo "[up] down with: deploy/down.sh"
