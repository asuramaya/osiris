#!/bin/sh
# Installs + enables the vault lane's three weekly timer pairs — osiris-prune-manifest,
# osiris-prune-apply, osiris-base-backup — the SAME idempotent copy-and-compare
# mechanism install_commands.sh already uses for the slash-command surface (Thoth ruling
# msg 6949: "deploy is the one sanctioned hand that writes machine files"). Thoth mail
# 8437: "have deploy install these three units the way it installs slash commands, so a
# stranger's box gets them without a hand" — these three were hand-installed once
# already this session; this is what makes that a one-time cost, not a recurring one.
#
# UNLIKE the other deploy/*.timer pairs beside these three (osiris-backup,
# osiris-preflight, osiris-pg-autotune, osiris-retention-reaper), which stay
# hand-install-only "ships as plumbing" per their own comment, these three are
# deploy-managed now — every `osiris deploy` keeps them installed, current, and enabled
# without a hand.
#
# IDEMPOTENT: the copy is compare-then-copy (never touches a byte-identical target); a
# repeat `systemctl --user enable --now` on an already-enabled, already-active timer is
# a silent no-op by systemd's own contract.
#
# TEST-SAFE: OSIRIS_SYSTEMD_USER_DIR redirects the copy target (same escape hatch
# install_commands.sh's own CLAUDE_COMMANDS_DIR established) — systemctl is only ever
# invoked when writing to the box's OWN real unit directory, so this script runs for
# real inside cmd_deploy's own test suite without mutating the operator's actual
# systemd session.
set -eu

TOPLEVEL="$(git rev-parse --show-toplevel)"
REAL_TARGET_DIR="$HOME/.config/systemd/user"
TARGET_DIR="${OSIRIS_SYSTEMD_USER_DIR:-$REAL_TARGET_DIR}"

UNITS="osiris-prune-manifest osiris-prune-apply osiris-base-backup"

mkdir -p "$TARGET_DIR"

installed=0
current=0
for name in $UNITS; do
    for ext in service timer; do
        src="$TOPLEVEL/deploy/$name.$ext"
        [ -f "$src" ] || continue
        target="$TARGET_DIR/$name.$ext"
        if [ -f "$target" ] && cmp -s "$src" "$target"; then
            current=$((current + 1))
            continue
        fi
        cp "$src" "$target"
        installed=$((installed + 1))
    done
done

if [ "$TARGET_DIR" = "$REAL_TARGET_DIR" ]; then
    if [ "$installed" -gt 0 ]; then
        systemctl --user daemon-reload
    fi
    systemctl --user enable --now \
        osiris-prune-manifest.timer osiris-prune-apply.timer osiris-base-backup.timer
fi

echo "install_prune_timers: $installed installed/updated, $current already current — $TARGET_DIR"
