#!/usr/bin/env bash
# Backup the Osiris graph — the asset. A single-operator box needs a one-command,
# restore-tested dump before it goes live (the entity graph is event-sourced; a lost DB
# is a lost case file). Writes a timestamped, compressed pg_dump and prunes old ones.
#
#   DATABASE_URL=postgresql://osiris:osiris@127.0.0.1:5432/osiris \
#     OSIRIS_BACKUP_DIR=/var/backups/osiris KEEP=14 deploy/backup.sh
#
# Cron it daily:  0 3 * * *  DATABASE_URL=... OSIRIS_BACKUP_DIR=... /opt/osiris/deploy/backup.sh
set -euo pipefail

: "${DATABASE_URL:?set DATABASE_URL to the Postgres DSN to back up}"
BACKUP_DIR="${OSIRIS_BACKUP_DIR:-/var/backups/osiris}"
KEEP="${KEEP:-14}"   # how many daily dumps to retain

mkdir -p "$BACKUP_DIR"
# stamp via the DB clock (no host-clock assumptions); Fc = custom format (parallel restore)
STAMP="$(psql "$DATABASE_URL" -tAc "select to_char(now(),'YYYYMMDD-HH24MISS')")"
OUT="$BACKUP_DIR/osiris-$STAMP.dump"

echo "[backup] dumping → $OUT"
pg_dump "$DATABASE_URL" --format=custom --no-owner --no-privileges --file "$OUT"
echo "[backup] wrote $(du -h "$OUT" | cut -f1)"

# prune all but the newest $KEEP dumps
mapfile -t old < <(ls -1t "$BACKUP_DIR"/osiris-*.dump 2>/dev/null | tail -n +"$((KEEP + 1))")
if [ "${#old[@]}" -gt 0 ]; then
  echo "[backup] pruning ${#old[@]} old dump(s) (keeping $KEEP)"
  rm -f "${old[@]}"
fi
echo "[backup] done."
