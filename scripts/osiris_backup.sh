#!/usr/bin/env bash
# The graph's survival ritual (born 2026-07-08, the day the anonymous-volume arrangement
# nearly ate the civilization at a reboot; residuals closed under task #51).
#
# Every 6 hours (timer): dump the durable container's DB to backups/ (keep 28 ≈ 7 days),
# refresh the repo's git bundle (the ~180 unpushed commits' only second copy until P6
# pushes), and rsync the lot into the vault — so the vault is a LIVE mirror, not the
# one-time snapshot it was born as. Same-disk still (the off-box rung stays open until
# the operator names a target); RPO drops from 24h to 6h.
set -euo pipefail
REPO="/home/asuramaya/code/osiris"
DIR="$REPO/backups"
VAULT="/home/asuramaya/osiris-vault"
mkdir -p "$DIR" "$VAULT"

docker exec osiris-pg pg_dump -U osiris -d osiris > "$DIR/osiris-$(date +%Y%m%d-%H%M%S).sql"
ls -1t "$DIR"/osiris-*.sql | tail -n +29 | xargs -r rm --

# the repo bundle: all refs, atomic replace (never a half-written only-copy)
git -C "$REPO" bundle create "$VAULT/osiris-repo.bundle.new" --all 2>/dev/null \
  && mv "$VAULT/osiris-repo.bundle.new" "$VAULT/osiris-repo.bundle"

# the vault mirrors the dump dir (dumps only; deletions NOT propagated — the vault may
# hold more history than the working set, never less)
rsync -a "$DIR"/osiris-*.sql "$VAULT/" 2>/dev/null || cp -n "$DIR"/osiris-*.sql "$VAULT/" || true
ls -1t "$VAULT"/osiris-2*.sql 2>/dev/null | tail -n +57 | xargs -r rm --
