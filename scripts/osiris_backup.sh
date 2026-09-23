#!/usr/bin/env bash
# The graph's backup process (born 2026-07-08, the day an anonymous-volume arrangement
# nearly lost the whole database at a reboot; residuals closed under task #51).
#
# Every 6 hours (timer): dump the durable container's DB straight to the VAULT (the
# canonical copy: backups/ keeps only the last day, and the vault owns retention),
# refresh the repo's git bundle, and hardlink the dump into backups/ for fast local
# access to the last day only. Same-disk still (an off-box destination remains a future
# option, not yet configured).
set -euo pipefail
# Portable: derive the repo from THIS script's location, never a hardcoded home. A path baked
# to one machine is a script that only works for the person who wrote it.
REPO="${OSIRIS_REPO:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
DIR="$REPO/backups"
VAULT="${OSIRIS_VAULT:-$HOME/osiris-vault}"
mkdir -p "$DIR" "$VAULT"

# COMPRESSED, -Fc (the vault lane, item 1): there is no penalty on the live server for
# using compression here; it runs in the pg_dump CLIENT, the reads it issues are
# identical to a plain -Fp dump, only the dump's own wall-clock grows.
# -Fc's own custom format also restores selectively and in parallel (`pg_restore -j`),
# which a flat .sql text dump cannot. Extension is `.dump`, never `.sql`: -Fc is a binary
# format, and a `.sql`-named custom-format file misleads the next reader into `psql <`ing
# it, which fails opaquely on binary garbage instead of naming the actual mistake.
#
# THE DISK GUARD (the vault lane, item 5): refuse to WRITE a new full dump when there is
# not room for one. This is the same emergency that opened the whole lane (92% full, ~3
# days runway at 44GB/day), caught by a human noticing rather than by any check in this
# house. A refusal alarms the desk and skips ONLY this dump; it never prunes anything
# itself (that stays osiris_prune_ladder.py's job, always dry-run until a human clears
# it) and never blocks the rest of this script's other duties (bundle/local cache/WAL
# pull all still run below). Checks $VAULT now, not $DIR: the vault is where the real
# write lands (see below).
NEW_DUMP=""
if ! "$REPO/.venv/bin/python" "$REPO/scripts/osiris_disk_guard.py" "$VAULT"; then
  "$REPO/.venv/bin/python" "$REPO/scripts/osiris_alarm.py" --from backup \
    "DISK GUARD: refusing to write a new full pg_dump into $VAULT — not enough free space " \
"for another dump the size of the last one plus margin. Run scripts/osiris_prune_ladder.py " \
"for a dry-run of what could be pruned; nothing is deleted automatically." || true
else
  # THE VAULT IS CANONICAL, backups/ IS A THIN LOCAL CACHE: backups/ keeps one day, the
  # vault owns retention. One write, straight to the vault: no more dump-then-rsync-mirror
  # two-copy dance, and no more separate hardcoded "keep 57" cutoff drifting out of sync
  # with osiris_prune_ladder.py's own retention plan (now on its own weekly timer, part 1).
  # The vault's retention is that plan's job alone from here on.
  NEW_DUMP="$VAULT/osiris-$(date +%Y%m%d-%H%M%S).dump"
  if ! docker exec osiris-pg pg_dump -U osiris -d osiris -Fc > "$NEW_DUMP"; then
    echo "osiris_backup: pg_dump failed — removing the partial dump, continuing with the rest of this run" >&2
    rm -f "$NEW_DUMP"
    NEW_DUMP=""
  fi
fi

# the repo bundle: all refs, atomic replace (never a half-written only-copy). `||`-
# terminated (2026-09-09 fix, from an investigation into a related failure): under
# `set -e` a bare `cmd1 && cmd2` statement that fails aborts the WHOLE script right
# there, never reaching the hardlink/WAL-pull sections below. This is exactly the
# failure mode a still-present transcript-archive `tar` call (since retired) hit live
# on 2026-09-08 16:30 CDT, leaving 465 WAL segments stuck staged in the container for a
# full 6-hour tick. A bundle refresh failing today would have been the SAME class of
# silent, total outage for every duty below it; this line is now the last one in the
# script that could still cause it, so it gets the identical treatment.
git -C "$REPO" bundle create "$VAULT/osiris-repo.bundle.new" --all 2>/dev/null \
  && mv "$VAULT/osiris-repo.bundle.new" "$VAULT/osiris-repo.bundle" \
  || echo "osiris_backup: git bundle refresh failed — leaving the previous bundle in place, continuing" >&2

# backups/ HARDLINKS the vault's own new dump (same filesystem, free: never a second
# copy of the bytes) purely for fast local access; ITS OWN retention is a fixed, small
# "keep 1 day" cutoff (6-hourly * 4), nothing more. This is a CACHE, not a second
# archive. `ln` falling back to `cp` covers the rare case of $DIR and $VAULT crossing a
# filesystem boundary (a hardlink can't span one; a plain copy still can).
if [ -n "$NEW_DUMP" ]; then
  ln "$NEW_DUMP" "$DIR/$(basename "$NEW_DUMP")" 2>/dev/null \
    || cp "$NEW_DUMP" "$DIR/$(basename "$NEW_DUMP")"
fi
ls -1t "$DIR"/osiris-*.dump 2>/dev/null | tail -n +5 | xargs -r rm -- || true

# THE TRANSCRIPT VAULT IS RETIRED: once the round-trip proof passes, the transcript
# archive line leaves osiris_backup.sh; the DB backups (WAL + the retention plan) carry
# the histories. This block used to tar ~/.claude/projects + every seat's .crush store
# into weekly incremental chains, dead weight now that the soul store (soul_lines)
# already holds every Claude Code session byte-exact, INSIDE the pg_dump this same
# script already takes above, and the round-trip proof (osiris_preflight.py --drill,
# weekly) keeps proving that's still true. Already-existing tarball chains from before
# this retirement stay in the vault untouched here; they thin over time under
# osiris_prune_ladder.py's own plan_prune_transcript_chains (still live, still tested:
# pruning an ALREADY-CREATED chain is a different concern from CREATING new ones, and
# this retirement only touches the latter) once a human clears them via --apply.
# Crush's own .crush stores lost their tar coverage here too, unrelated to soul_lines
# (piece 1's own scope was claude-code only): a real gap, tracked rather than silently
# reintroduced later.

# WAL PULL (vault lane item 3): osiris_archive_wal.sh (deployed to
# /var/lib/postgresql/data/osiris_archive_wal.sh, INSIDE the pgdata VOLUME so it survives
# a real container recreate) stages each completed WAL segment there. This container
# carries no bind mount for the vault (checked: `docker inspect osiris-pg` shows exactly
# one mount, the pgdata volume), so getting segments OUT is this script's own job, same
# division of labor as everything else in this file (the container does the Postgres-
# side work, the host does the vault-side work). Copies whatever's staged into the vault,
# then prunes the in-container staging copy once it's safely out, bounding pgdata volume
# growth while the vault becomes the durable, off-container copy. Silent no-op (never a
# hard failure) when archiving isn't enabled yet or the container isn't reachable:
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
    # pulled (or already had a copy): safe to prune the in-container staging file
    [ -e "$WAL_VAULT/$seg" ] \
      && docker exec osiris-pg rm -f "/var/lib/postgresql/data/wal_archive/$seg" 2>/dev/null || true
  done < <(docker exec osiris-pg ls -1 /var/lib/postgresql/data/wal_archive 2>/dev/null)
fi
