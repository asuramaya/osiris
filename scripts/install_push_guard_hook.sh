#!/bin/sh
# Installs the pre-push secret/PII guard into this repo's SHARED .git/hooks/pre-push
# (scripts/push_guard.py, 2026-08-15 incident). Deliberately does NOT touch core.hooksPath
# (#133's landmine stays untouched) -- hooks are not per-worktree, so writing directly into
# the common .git/hooks directory covers every worktree of this repo at once, from wherever
# this script is run.
#
# IDEMPOTENT: re-running this when the hook is already installed and current is a silent
# no-op (exit 0, one confirming line) -- never rewrites a byte that's already correct, and
# never fails just because it's been run before. push_guard.hook_status() is the read-only
# twin of this check, wired into `osiris deploy`'s own report so a MISSING or STALE hook is
# as visible as a failing gate, not something that quietly rots after one box gets it.
set -eu

COMMON_DIR="$(git rev-parse --path-format=absolute --git-common-dir)"
TOPLEVEL="$(git rev-parse --show-toplevel)"
SOURCE="$TOPLEVEL/.githooks/pre-push"
TARGET="$COMMON_DIR/hooks/pre-push"

if [ ! -f "$SOURCE" ]; then
    echo "install_push_guard_hook: SOURCE MISSING — $SOURCE not found. Nothing installed." >&2
    exit 1
fi

if [ -f "$TARGET" ] && cmp -s "$SOURCE" "$TARGET"; then
    echo "install_push_guard_hook: already installed and current — $TARGET"
    exit 0
fi

mkdir -p "$COMMON_DIR/hooks"
cp "$SOURCE" "$TARGET"
chmod +x "$TARGET"
echo "install_push_guard_hook: installed — $TARGET (covers every worktree of this repo)"
