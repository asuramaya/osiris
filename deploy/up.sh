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
# COMPOSE DRIFT FIX (Thoth mail 9122 item 2, wave 16): this used to run --rm with no
# volume at all — a real restart cycle would have STOPPED AND REMOVED the container,
# discarding every row silently, and never configured WAL archiving (archive_mode/
# archive_command), the exact "a recreate can never drop WAL archiving" failure this
# fix closes. Named volume osiris-pg-data (not an anonymous/compose-prefixed one) is
# THE volume the real box's own osiris-pg already runs on (confirmed via `docker
# inspect osiris-pg` and osiris_archive_wal.sh's own header comment) — using the same
# name here means a recreate on THIS box reattaches to real data instead of silently
# starting empty. --restart unless-stopped matches the live box's own policy (a crash
# self-heals; a deliberate `docker stop` stays stopped, matching deploy/down.sh).
#
# archive_mode/archive_command/wal_level are the STRUCTURAL facts nothing else
# rederives — scripts/osiris_archive_wal.sh (bind-mounted read-only from the repo, so
# it's always the checked-in version, never a stale manually-`docker cp`'d copy) is
# what archive_command actually invokes.
#
# TUNING FLAGS (shared_buffers/effective_cache_size/work_mem/maintenance_work_mem/
# random_page_cost) ARE DELIBERATELY NOT SET HERE ANYMORE: scripts/osiris_pg_autotune.py
# (deploy/osiris-pg-autotune.timer, daily) derives these from the box's own RAM/CPU and
# persists them via ALTER SYSTEM — the values that WERE hardcoded here (4GB/12GB/32MB/
# 512MB) are years-stale relative to what's live now (measured: shared_buffers alone is
# live at 7.7GB, not 4GB) and a recreate using this script would have silently DOWN-
# TUNED a production box back to those old numbers every time. The image's own stock
# defaults are a safe day-0 floor; autotune's first daily run corrects them within 24h.
docker run -d --name osiris-pg --restart unless-stopped \
  -e POSTGRES_USER=osiris -e POSTGRES_PASSWORD=osiris -e POSTGRES_DB=osiris \
  -p "127.0.0.1:${PG_PORT}:5432" \
  -v osiris-pg-data:/var/lib/postgresql/data \
  -v "$(pwd)/scripts/osiris_archive_wal.sh:/var/lib/postgresql/data/osiris_archive_wal.sh:ro" \
  postgres:16 \
  -c wal_level=replica -c archive_mode=on \
  -c archive_command='bash /var/lib/postgresql/data/osiris_archive_wal.sh %p %f' >/dev/null
docker run -d --name osiris-redis --restart unless-stopped \
  -p "127.0.0.1:${REDIS_PORT}:6379" -v osiris-redis-data:/data redis:7 >/dev/null

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
