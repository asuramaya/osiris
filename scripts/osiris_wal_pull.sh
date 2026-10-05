#!/usr/bin/env bash
# THE WAL PULL (vault lane item 3), its own job on its own timer (osiris-wal-pull.timer, every
# 15 minutes). It used to be the tail of osiris_backup.sh, which also takes a full pg_dump of a
# database of tens of gigabytes: the dump saturated the disk and the pull, which is small and
# should run often, waited behind it.
#
# osiris_archive_wal.sh (bind-mounted read-only from the repo into the database container) stages
# each completed WAL segment inside the pgdata volume. The container carries no bind mount for the
# vault (`docker inspect osiris-pg` shows exactly one mount, the pgdata volume), so getting
# segments OUT is the host's job: copy whatever is staged into the vault, then remove the
# in-container staging copy once it is safely out, bounding pgdata volume growth while the vault
# becomes the durable, off-container copy. A silent no-op (never a hard failure) when archiving is
# not enabled yet or the container is not reachable: WAL archiving is opt-in infrastructure.
set -euo pipefail
VAULT="${OSIRIS_VAULT:-$HOME/osiris-vault}"
mkdir -p "$VAULT"
WAL_VAULT="$VAULT/wal_archive"
mkdir -p "$WAL_VAULT"
if docker exec osiris-pg test -d /var/lib/postgresql/data/wal_archive 2>/dev/null; then
  while IFS= read -r seg; do
    [ -n "$seg" ] || continue
    # a copy the archive step is still writing: never pull or remove half a segment
    case "$seg" in *.tmp) continue ;; esac
    if [ ! -e "$WAL_VAULT/$seg" ]; then
      docker exec osiris-pg cat "/var/lib/postgresql/data/wal_archive/$seg" \
        > "$WAL_VAULT/$seg.tmp" 2>/dev/null \
        && mv "$WAL_VAULT/$seg.tmp" "$WAL_VAULT/$seg" \
        || rm -f "$WAL_VAULT/$seg.tmp"
    fi
    # pulled (or already had a copy): safe to prune the in-container staging file
    [ -e "$WAL_VAULT/$seg" ] \
      && docker exec osiris-pg rm -f "/var/lib/postgresql/data/wal_archive/$seg" 2>/dev/null || true
  done < <(docker exec osiris-pg ls -1 /var/lib/postgresql/data/wal_archive 2>/dev/null)
fi
