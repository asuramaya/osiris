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
# word) and never blocks the rest of this script's other duties (bundle/vault
# mirror/transcript archive/WAL pull all still run below).
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

# THE TRANSCRIPT VAULT (Phase 0 triage, operator's word, 2026-07-21): the fleet's memory
# lives on disk as transcripts before it's ever a graph row — ~/.claude/projects (every
# Claude Code session) and each seat's .crush store (the Crush harness's own session db,
# ~/.osiris/seats/<handle>/.crush/).
#
# INCREMENTAL, NEVER A FULL SNAPSHOT EVERY DAY (the vault lane, operator ruling
# 39384a87/c53a5fc0, item 4, explicitly separate from item 2's "every kept DUMP is a
# full" — a pg_dump is inherently a full graph snapshot by construction; this is not
# that, and the operator's own words approved "incremental transcript archive" as its
# own, different shape). GNU tar's `--listed-incremental` is the standard mechanism: the
# snapshot file tracks every archived path's mtime/inode across every run within its own
# CHAIN, so the chain's first day is a full baseline (level 0) and every day after
# archives ONLY what changed — the same soul_lines/harness_turns bytes were being
# re-tarred in FULL, unchanged, every single day before this (~/.claude/projects alone
# dwarfing the DB dump itself).
#
# BOUNDED, WEEKLY CHAINS (Thoth msg 8211, off this same ruling): an UNBOUNDED single
# chain forever is itself still unbounded growth, and RESTORE REQUIRES THE WHOLE CHAIN IN
# ORDER (the level-0 base plus every incremental after it) — pruning any tarball out of
# the middle of an open-ended chain silently breaks every later day's restorability. The
# snapshot file and the tarball name are both keyed by ISO week (`%G-W%V`): a new week
# has no snapshot file yet, so tar starts a fresh level-0 baseline automatically — no
# extra logic needed beyond the naming. `osiris_prune_ladder.py`'s own
# `plan_prune_transcript_chains` prunes whole weekly CHAINS at a time (never a tarball out
# of the middle of one), keeping the last 4 weekly chains plus one per month — see that
# module. The soul-store lane (queued behind this) retires this whole archive once its
# own round-trip proof passes, so this shape only has to hold for weeks, not forever.
#
# TAR'S OWN CHANGED-FILE EXIT (31 daily tarballs stuck as `.tar.gz.new` since Aug 12):
# exit 1 means "a file changed while being read" — normal and expected against LIVE
# session files, not corruption; the archive tar actually wrote is still usable. Only
# exit 2+ is a real failure. The old `&&`-chained rename silently dropped every exit-1
# run's `.new` file forever, never once completing the atomic rename.
WEEK="$(date +%G-W%V)"
SNAPSHOT="$VAULT/.transcript-archive-$WEEK.snar"
TRANSCRIPTS="$VAULT/claude-transcripts-$WEEK-$(date +%Y%m%d).tar.gz"
tar_args=(-C "$HOME" .claude/projects)
while IFS= read -r store; do
  # -C "seats" "<handle>/.crush" (never a bare ".crush") — two seats' stores would
  # otherwise collide on the SAME flattened archive path and overwrite each other
  [ -n "$store" ] && tar_args+=(-C "$HOME/.osiris/seats" "$(basename "$(dirname "$store")")/.crush")
done < <(find "$HOME/.osiris/seats" -maxdepth 2 -iname ".crush" -type d 2>/dev/null)
tar_rc=0
tar --listed-incremental="$SNAPSHOT" -czf "$TRANSCRIPTS.new" "${tar_args[@]}" 2>/dev/null || tar_rc=$?
if [ "$tar_rc" -le 1 ]; then
  mv "$TRANSCRIPTS.new" "$TRANSCRIPTS"
else
  # a real tar failure (2+) — actually LEAVE the .new file, never delete the one
  # artifact that would let a human see what tar produced before it died
  echo "osiris_backup: transcript archive failed (tar exit $tar_rc), leaving $TRANSCRIPTS.new for inspection" >&2
fi

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
