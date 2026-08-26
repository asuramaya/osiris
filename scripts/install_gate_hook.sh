#!/bin/sh
# Installs the gates-are-law pre-commit hook (scripts/gate_hook.py, task #131/#133) into this
# repo's SHARED .git/hooks/pre-commit. Deliberately does NOT touch core.hooksPath -- #103
# correctly named that a LANDMINE (it is machine-wide, so osiris's own gate would fire in
# every other repo on this box too, each lacking the venv/database gate_hook.py reaches for).
# push_guard already solved the identical problem for pre-push by copying the tracked shim
# straight into the shared .git/hooks directory instead of touching hooksPath at all -- this
# script is that SAME mechanism, reused verbatim for pre-commit (ruling 754482bf, #133).
#
# IDEMPOTENT: re-running this when the hook is already installed and current is a silent
# no-op (exit 0, one confirming line) -- never rewrites a byte that's already correct, and
# never fails just because it's been run before. scripts/gate_hook.py's own hook_status() is
# the read-only twin of this check, wired into `osiris deploy`'s own report so a MISSING or
# STALE hook is as visible as a failing gate, not something that quietly rots after one box
# gets it.
set -eu

COMMON_DIR="$(git rev-parse --path-format=absolute --git-common-dir)"
TOPLEVEL="$(git rev-parse --show-toplevel)"
SOURCE="$TOPLEVEL/.githooks/pre-commit"
TARGET="$COMMON_DIR/hooks/pre-commit"

if [ ! -f "$SOURCE" ]; then
    echo "install_gate_hook: SOURCE MISSING — $SOURCE not found. Nothing installed." >&2
    exit 1
fi

if [ -f "$TARGET" ] && cmp -s "$SOURCE" "$TARGET"; then
    echo "install_gate_hook: already installed and current — $TARGET"
    exit 0
fi

mkdir -p "$COMMON_DIR/hooks"
cp "$SOURCE" "$TARGET"
chmod +x "$TARGET"
echo "install_gate_hook: installed — $TARGET (covers every worktree of this repo)"
