#!/bin/sh
# Installs the slash-command docs (commands/*.md, task #204's structural fix) into
# ~/.claude/commands/ — the SAME mechanism install_gate_hook.sh already uses for the
# pre-commit hook: copy a tracked file from the repo to its machine-wide destination,
# idempotent, per-file compare, never touching anything this repo doesn't own.
#
# WHY THIS EXISTS (Thoth ruling msg 6918, decision 012b36fb, superseding a34a9850): the
# slash-command surface used to live ONLY at ~/.claude/commands/*.md, outside every git
# repo entirely — decision a34a9850's own original reasoning was that no CI gate could
# see it there, so the parity gate had to read the live machine copy instead. That same
# property (a file the git diff being committed cannot contain) let one agent's
# unfinished edit fail every OTHER agent's pre-commit gate — THREE workers blocked at
# once on 2026-09-04, all on the same live seat.md race. The fix: commands/*.md is now
# the SOURCE OF TRUTH, tracked in the repo; this script is how it reaches the machine.
#
# IDEMPOTENT: re-running when every file is already installed and current is a silent
# no-op (exit 0, one confirming line per file). A file present on the machine but not
# in commands/ (a local, un-tracked slash command) is left alone — this only ever
# copies FROM the repo, never deletes anything it doesn't own.
set -eu

TOPLEVEL="$(git rev-parse --show-toplevel)"
SOURCE_DIR="$TOPLEVEL/commands"
TARGET_DIR="${CLAUDE_COMMANDS_DIR:-$HOME/.claude/commands}"

if [ ! -d "$SOURCE_DIR" ]; then
    echo "install_commands: SOURCE MISSING — $SOURCE_DIR not found. Nothing installed." >&2
    exit 1
fi

mkdir -p "$TARGET_DIR"

installed=0
current=0
for src in "$SOURCE_DIR"/*.md; do
    [ -e "$src" ] || continue
    name="$(basename "$src")"
    target="$TARGET_DIR/$name"
    if [ -f "$target" ] && cmp -s "$src" "$target"; then
        current=$((current + 1))
        continue
    fi
    cp "$src" "$target"
    installed=$((installed + 1))
done

echo "install_commands: $installed installed/updated, $current already current — $TARGET_DIR"
