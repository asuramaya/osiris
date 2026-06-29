#!/usr/bin/env bash
# Restore an Osiris graph from a pg_dump produced by deploy/backup.sh. A backup you have
# never restored is a backup you do not have — test this against a throwaway DB.
#
#   DATABASE_URL=postgresql://osiris:osiris@127.0.0.1:5432/osiris \
#     deploy/restore.sh /var/backups/osiris/osiris-20260628-030000.dump
#
# DESTRUCTIVE: drops + recreates the public schema of the TARGET DATABASE_URL first.
set -euo pipefail

: "${DATABASE_URL:?set DATABASE_URL to the TARGET Postgres DSN (will be overwritten)}"
DUMP="${1:?usage: restore.sh <dump-file>}"
[ -f "$DUMP" ] || { echo "[restore] no such dump: $DUMP" >&2; exit 1; }

echo "[restore] WARNING: this overwrites $DATABASE_URL with $DUMP"
echo "[restore] dropping + recreating schema public…"
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;"

echo "[restore] restoring (parallel)…"
pg_restore --dbname "$DATABASE_URL" --no-owner --no-privileges --jobs 4 "$DUMP"

OBJ="$(psql "$DATABASE_URL" -tAc 'select count(*) from objects' 2>/dev/null || echo '?')"
echo "[restore] done — $OBJ objects in the graph."
