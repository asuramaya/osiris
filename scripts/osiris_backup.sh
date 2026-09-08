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
# Portable: derive the repo from THIS script's location, never a hardcoded home. A path baked
# to one machine is a script that only works for the person who wrote it.
REPO="${OSIRIS_REPO:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
DIR="$REPO/backups"
VAULT="${OSIRIS_VAULT:-$HOME/osiris-vault}"
mkdir -p "$DIR" "$VAULT"

# COMPRESSED, -Fc (the vault lane, operator ruling 39384a87/c53a5fc0, item 1): "what's the
# hot penalty?" — none on the live server; compression runs in the pg_dump CLIENT, the
# reads it issues are identical to a plain -Fp dump, only the dump's own wall-clock grows.
# -Fc's own custom format also restores selectively and in parallel (`pg_restore -j`),
# which a flat .sql text dump cannot. Extension is `.dump`, never `.sql` — -Fc is a binary
# format, and a `.sql`-named custom-format file misleads the next reader into `psql <`ing
# it, which fails opaquely on binary garbage instead of naming the actual mistake.
#
# THE DISK GUARD (the vault lane, item 5): refuse to WRITE a new full dump when there is
# not room for one — this is the same emergency that opened the whole lane (92% full, ~3
# days runway at 44GB/day), caught by a human noticing rather than by any check in this
# house. A refusal alarms the desk and skips ONLY this dump; it never prunes anything
# itself (that stays osiris_prune_ladder.py's job, always dry-run until the operator's own
# word) and never blocks the rest of this script's other duties (bundle/vault mirror/WAL
# pull all still run below).
if ! "$REPO/.venv/bin/python" "$REPO/scripts/osiris_disk_guard.py" "$DIR"; then
  "$REPO/.venv/bin/python" "$REPO/scripts/osiris_alarm.py" --from backup \
    "DISK GUARD: refusing to write a new full pg_dump into $DIR — not enough free space for " \
"another dump the size of the last one plus margin. Run scripts/osiris_prune_ladder.py for a " \
"dry-run of what could be pruned; nothing is deleted automatically." || true
else
  docker exec osiris-pg pg_dump -U osiris -d osiris -Fc > "$DIR/osiris-$(date +%Y%m%d-%H%M%S).dump"
fi
# `|| true`: a guard-refused first-ever run leaves $DIR with zero dumps, and the glob
# below then fails to expand at all — under pipefail that would kill the whole script
# over a directory listing, not a real backup failure.
ls -1t "$DIR"/osiris-*.dump 2>/dev/null | tail -n +29 | xargs -r rm -- || true

# the repo bundle: all refs, atomic replace (never a half-written only-copy)
git -C "$REPO" bundle create "$VAULT/osiris-repo.bundle.new" --all 2>/dev/null \
  && mv "$VAULT/osiris-repo.bundle.new" "$VAULT/osiris-repo.bundle"

# the vault mirrors the dump dir (dumps only; deletions NOT propagated — the vault may
# hold more history than the working set, never less)
rsync -a "$DIR"/osiris-*.dump "$VAULT/" 2>/dev/null || cp -n "$DIR"/osiris-*.dump "$VAULT/" || true
ls -1t "$VAULT"/osiris-2*.dump 2>/dev/null | tail -n +57 | xargs -r rm --

# THE TRANSCRIPT VAULT IS RETIRED (thread 78efd46d item 3, operator ruling: "once (2)
# [the round-trip proof] passes the transcript archive line leaves osiris_backup.sh; the
# DB backups (WAL + the ladder) carry the histories"). This block used to tar
# ~/.claude/projects + every seat's .crush store into weekly incremental chains — dead
# weight now that the soul store (soul_lines) already holds every Claude Code session
# byte-exact, INSIDE the pg_dump this same script already takes above, and the round-
# trip proof (osiris_preflight.py --drill, weekly) keeps proving that's still true.
# Already-existing tarball chains from before this retirement stay in the vault
# untouched here — they thin over time under osiris_prune_ladder.py's own
# plan_prune_transcript_chains (still live, still tested — pruning an ALREADY-CREATED
# chain is a different concern from CREATING new ones, and this retirement only touches
# the latter) once the operator's own word clears them via --apply. Crush's own .crush
# stores lost their tar coverage here too, unrelated to soul_lines (piece 1's own scope
# was claude-code only) — a real gap, tracked rather than silently reintroduced later.

# WAL PULL (vault lane item 3): osiris_archive_wal.sh (deployed to
# /var/lib/postgresql/data/osiris_archive_wal.sh, INSIDE the pgdata VOLUME so it survives
# a real container recreate) stages each completed WAL segment there — this container
# carries no bind mount for the vault (checked: `docker inspect osiris-pg` shows exactly
# one mount, the pgdata volume), so getting segments OUT is this script's own job, same
# division of labor as everything else in this file (the container does the Postgres-
# side work, the host does the vault-side work). Copies whatever's staged into the vault,
# then prunes the in-container staging copy once it's safely out — bounds pgdata volume
# growth while the vault becomes the durable, off-container copy. Silent no-op (never a
# hard failure) when archiving isn't enabled yet or the container isn't reachable —
# WAL archiving is opt-in infrastructure, not assumed here.
WAL_VAULT="$VAULT/wal_archive"
mkdir -p "$WAL_VAULT"
if docker exec osiris-pg test -d /var/lib/postgresql/data/wal_archive 2>/dev/null; then
  while IFS= read -r seg; do
    [ -n "$seg" ] || continue
    if [ ! -e "$WAL_VAULT/$seg" ]; then
      docker exec osiris-pg cat "/var/lib/postgresql/data/wal_archive/$seg" \
        > "$WAL_VAULT/$seg.tmp" 2>/dev/null \
        && mv "$WAL_VAULT/$seg.tmp" "$WAL_VAULT/$seg" \
        || rm -f "$WAL_VAULT/$seg.tmp"
    fi
    # pulled (or already had a copy) — safe to prune the in-container staging file
    [ -e "$WAL_VAULT/$seg" ] \
      && docker exec osiris-pg rm -f "/var/lib/postgresql/data/wal_archive/$seg" 2>/dev/null || true
  done < <(docker exec osiris-pg ls -1 /var/lib/postgresql/data/wal_archive 2>/dev/null)
fi
