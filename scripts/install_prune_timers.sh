#!/bin/sh
# Installs + enables the timer lane's SEVEN timer pairs: osiris-prune-manifest,
# osiris-prune-apply, osiris-base-backup, osiris-backup, osiris-preflight,
# osiris-pg-autotune, and osiris-offload (the opportunistic offload runner). This is
# the SAME idempotent copy-and-compare mechanism install_commands.sh already uses for
# the slash-command surface, on the principle that deploy is the one sanctioned agent
# that writes machine files, so a fresh box gets these units without a human hand.
# The original three were hand-installed once already; widened to five, then to six,
# then to seven: osiris-pg-autotune was the last hand-installed timer a prior census
# named as a gap. A panel field (or, now, a registered schedule) that silently does
# nothing until a human hand-installs it is worse than no field.
#
# RENDERED, NOT COPIED VERBATIM: each `.timer` passes through scripts/render_units.py
# first (generalized past the backup lane alone), which substitutes a configured
# OnCalendar= override for the shipped default when one is set, so a panel-written
# schedule becomes real the moment deploy runs. A unit with no override renders
# byte-identical to its shipped file, so the compare-then-copy below stays a true
# no-op for it; the render step itself fails OPEN (falls back to the shipped defaults)
# rather than ever blocking this install on a DB hiccup.
#
# IDEMPOTENT: the copy is compare-then-copy (never touches a byte-identical target); a
# repeat `systemctl --user enable --now` on an already-enabled, already-active timer is
# a silent no-op by systemd's own contract.
#
# TEST-SAFE: OSIRIS_SYSTEMD_USER_DIR redirects the copy target (same escape hatch
# install_commands.sh's own CLAUDE_COMMANDS_DIR established); systemctl is only ever
# invoked when writing to the box's OWN real unit directory, so this script runs for
# real inside cmd_deploy's own test suite without mutating the operator's actual
# systemd session.
set -eu

TOPLEVEL="$(git rev-parse --show-toplevel)"
REAL_TARGET_DIR="$HOME/.config/systemd/user"
TARGET_DIR="${OSIRIS_SYSTEMD_USER_DIR:-$REAL_TARGET_DIR}"

UNITS="osiris-prune-manifest osiris-prune-apply osiris-base-backup osiris-backup osiris-preflight osiris-pg-autotune osiris-offload"

mkdir -p "$TARGET_DIR"

RENDER_DIR="$(mktemp -d)"
trap 'rm -rf "$RENDER_DIR"' EXIT
"$TOPLEVEL/.venv/bin/python" "$TOPLEVEL/scripts/render_units.py" \
    --deploy-dir "$TOPLEVEL/deploy" --out "$RENDER_DIR" \
    || cp "$TOPLEVEL"/deploy/osiris-*.timer "$TOPLEVEL"/deploy/osiris-*.service "$RENDER_DIR/"

installed=0
current=0
for name in $UNITS; do
    for ext in service timer; do
        src="$RENDER_DIR/$name.$ext"
        [ -f "$src" ] || src="$TOPLEVEL/deploy/$name.$ext"
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
        osiris-prune-manifest.timer osiris-prune-apply.timer osiris-base-backup.timer \
        osiris-backup.timer osiris-preflight.timer osiris-pg-autotune.timer \
        osiris-offload.timer
fi

echo "install_prune_timers: $installed installed/updated, $current already current — $TARGET_DIR"
