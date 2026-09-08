#!/usr/bin/env bash
# THE WEEKLY BASE BACKUP (vault lane, operator ruling 39384a87/c53a5fc0, item 3: "WAL
# archiving into the vault plus a weekly base backup"). A pg_basebackup is the physical
# starting point WAL replay needs for point-in-time recovery — WAL archiving alone (this
# lane's own osiris_archive_wal.sh + osiris_backup.sh's own pull) can replay FORWARD from
# a base, but has no base of its own; without one, recovery has nothing to start from and
# every WAL segment ever archived is dead weight.
#
# `pg_basebackup -Ft -z` (tar format, compressed) run VIA `docker exec`, its own stdout
# piped straight to the host-side vault file — no bind mount, no writing inside the
# container's ephemeral root FS needed (the exact same reason osiris_archive_wal.sh lives
# inside the pgdata VOLUME instead: nothing outside that volume survives a real restart
# cycle, and pg_basebackup's own output here never needs to touch the container's
# filesystem at all).
set -euo pipefail
VAULT="${OSIRIS_VAULT:-$HOME/osiris-vault}"
BASEBACKUP_DIR="$VAULT/basebackups"
mkdir -p "$BASEBACKUP_DIR"

STAMP="$(date +%Y%m%d-%H%M%S)"
OUT="$BASEBACKUP_DIR/osiris-basebackup-$STAMP.tar.gz"

# atomic: write to .new, rename only once pg_basebackup's own stdout fully closes with a
# clean exit — a killed/interrupted run must never leave a half-written file wearing the
# final name (the exact `osiris-*.dump`/transcript-tarball idiom this whole lane uses).
docker exec osiris-pg pg_basebackup -U osiris -D - -Ft -z -Xnone --checkpoint=fast \
  > "$OUT.new"
mv "$OUT.new" "$OUT"

# RETENTION: osiris_prune_ladder.py's own DB-dump ladder (dry-run by default, --apply
# gated the same as everywhere else in this lane) — a base backup, unlike a WAL segment,
# IS a complete, independently-restorable unit on its own, exactly the "every survivor a
# full" shape that ladder already implements and tests. No second thinning
# implementation here.
