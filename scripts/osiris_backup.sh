#!/usr/bin/env bash
# Nightly graph dump (born 2026-07-08, the day the anonymous-volume arrangement nearly ate
# the civilization at a reboot). Dumps the durable container's DB to backups/, keeps 14.
set -euo pipefail
DIR="/home/asuramaya/code/osiris/backups"
mkdir -p "$DIR"
docker exec osiris-pg pg_dump -U osiris -d osiris > "$DIR/osiris-$(date +%Y%m%d-%H%M%S).sql"
ls -1t "$DIR"/osiris-*.sql | tail -n +15 | xargs -r rm --
