#!/bin/sh
# Installs the shared-checkout guard (.githooks/reference-transaction) into this repo's SHARED
# .git/hooks/reference-transaction. Same mechanism as install_push_guard_hook.sh: copy the tracked
# shim into the common hooks directory (never core.hooksPath, which is machine-wide), which covers
# every worktree at once; the hook itself only acts in the shared checkout.
#
# IDEMPOTENT: when the hook is already installed and current this is a silent no-op.
set -eu

COMMON_DIR="$(git rev-parse --path-format=absolute --git-common-dir)"
TOPLEVEL="$(git rev-parse --show-toplevel)"
SOURCE="$TOPLEVEL/.githooks/reference-transaction"
TARGET="$COMMON_DIR/hooks/reference-transaction"

if [ ! -f "$SOURCE" ]; then
    echo "install_checkout_guard_hook: SOURCE MISSING - $SOURCE not found. Nothing installed." >&2
    exit 1
fi

if [ -f "$TARGET" ] && cmp -s "$SOURCE" "$TARGET"; then
    echo "install_checkout_guard_hook: already installed and current - $TARGET"
    exit 0
fi

mkdir -p "$COMMON_DIR/hooks"
cp "$SOURCE" "$TARGET"
chmod +x "$TARGET"
echo "install_checkout_guard_hook: installed - $TARGET (the shared checkout now refuses HEAD off main)"
