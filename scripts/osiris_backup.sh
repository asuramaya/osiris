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
docker exec osiris-pg pg_dump -U osiris -d osiris -Fc > "$DIR/osiris-$(date +%Y%m%d-%H%M%S).dump"
ls -1t "$DIR"/osiris-*.dump | tail -n +29 | xargs -r rm --

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
# snapshot file (never pruned by this script) tracks every archived path's mtime/inode
# across every run, so day 1 is a full baseline (level 0) and every day after archives
# ONLY what changed — the same soul_lines/harness_turns bytes were being re-tarred in
# FULL, unchanged, every single day before this (~/.claude/projects alone dwarfing the DB
# dump itself). RESTORE REQUIRES THE WHOLE CHAIN IN ORDER (the snapshot file's own
# level-0 base plus every incremental after it) — this is why the retention ladder (the
# vault lane's next item, 2) must NOT prune transcript tarballs the same "keep newest N"
# way the DB dumps use: deleting an old incremental out of the middle of the chain
# silently breaks every later day's restorability. Flagged here, not solved here — this
# script does not prune transcript tarballs at all now (the old blind "keep 3" line
# is GONE, not merely relaxed) until the ladder work gives this chain its own real
# retention shape.
#
# TAR'S OWN CHANGED-FILE EXIT (31 daily tarballs stuck as `.tar.gz.new` since Aug 12):
# exit 1 means "a file changed while being read" — normal and expected against LIVE
# session files, not corruption; the archive tar actually wrote is still usable. Only
# exit 2+ is a real failure. The old `&&`-chained rename silently dropped every exit-1
# run's `.new` file forever, never once completing the atomic rename.
SNAPSHOT="$VAULT/.transcript-archive.snar"
TRANSCRIPTS="$VAULT/claude-transcripts-$(date +%Y%m%d).tar.gz"
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
