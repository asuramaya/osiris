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

docker exec osiris-pg pg_dump -U osiris -d osiris > "$DIR/osiris-$(date +%Y%m%d-%H%M%S).sql"
ls -1t "$DIR"/osiris-*.sql | tail -n +29 | xargs -r rm --

# the repo bundle: all refs, atomic replace (never a half-written only-copy)
git -C "$REPO" bundle create "$VAULT/osiris-repo.bundle.new" --all 2>/dev/null \
  && mv "$VAULT/osiris-repo.bundle.new" "$VAULT/osiris-repo.bundle"

# the vault mirrors the dump dir (dumps only; deletions NOT propagated — the vault may
# hold more history than the working set, never less)
rsync -a "$DIR"/osiris-*.sql "$VAULT/" 2>/dev/null || cp -n "$DIR"/osiris-*.sql "$VAULT/" || true
ls -1t "$VAULT"/osiris-2*.sql 2>/dev/null | tail -n +57 | xargs -r rm --

# THE TRANSCRIPT VAULT (Phase 0 triage, operator's word, 2026-07-21): the fleet's memory
# lives on disk as transcripts before it's ever a graph row — ~/.claude/projects (every
# Claude Code session) and each seat's .crush store (the Crush harness's own session db,
# ~/.osiris/seats/<handle>/.crush/). Poured ONCE by hand on Jul 8, never refreshed since —
# 13 days of souls sat unprotected. Interim until soul-store piece 1 lands: dumb and
# boring, same atomic-write and newest-N pruning idioms as the rest of this script.
TRANSCRIPTS="$VAULT/claude-transcripts-$(date +%Y%m%d).tar.gz"
tar_args=(-C "$HOME" .claude/projects)
while IFS= read -r store; do
  # -C "seats" "<handle>/.crush" (never a bare ".crush") — two seats' stores would
  # otherwise collide on the SAME flattened archive path and overwrite each other
  [ -n "$store" ] && tar_args+=(-C "$HOME/.osiris/seats" "$(basename "$(dirname "$store")")/.crush")
done < <(find "$HOME/.osiris/seats" -maxdepth 2 -iname ".crush" -type d 2>/dev/null)
tar czf "$TRANSCRIPTS.new" "${tar_args[@]}" 2>/dev/null && mv "$TRANSCRIPTS.new" "$TRANSCRIPTS"
ls -1t "$VAULT"/claude-transcripts-*.tar.gz 2>/dev/null | tail -n +4 | xargs -r rm --
