#!/usr/bin/env bash
# Tear down the deploy/up.sh stack. Stops, never removes (up.sh's containers no longer
# run --rm, compose-drift fix Thoth mail 9122 item 2) — `docker start osiris-pg
# osiris-redis` resumes the same containers with their real data untouched.
set -uo pipefail
for p in api worker; do
  [ -f "/tmp/osiris-$p.pid" ] && kill "$(cat /tmp/osiris-$p.pid)" 2>/dev/null || true
  rm -f "/tmp/osiris-$p.pid"
done
docker stop osiris-pg osiris-redis >/dev/null 2>&1 || true
echo "[down] stopped api, worker, Postgres, Redis"
