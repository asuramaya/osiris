#!/bin/sh
# THE DEPLOY SNAPSHOT: pins ~/.local/bin/osiris at a separate worktree checked out to
# the deployed sha, so the operator's CLI never runs a CI candidate tree. Live incident
# this closes: `osiris resume` died with SoulKeyMissing because ~/.local/bin/osiris was
# a symlink straight into ~/code/osiris/.venv/bin/osiris, an EDITABLE install that reads
# source out of the main checkout, and a CI run had a candidate branch merged into that
# same checkout for its test run at that exact moment. The long-lived services survive a
# CI run untouched (they run in-process from their own last restart); only the CLI shim
# was reading whatever happened to be on disk in ~/code/osiris right then.
#
# Called ONLY by `osiris deploy`, ONLY after a green restart+smoke (the same rule every
# other install_*.sh script here follows: deploy is the one sanctioned process that
# writes machine files), never on a raw restart or reboot, and never idempotent-safe to
# run standalone against an arbitrary sha without that context.
#
# Usage: update_deploy_snapshot.sh <repo_root> <deployed_sha>
#   OSIRIS_DEPLOY_SNAPSHOT_DIR overrides the worktree path (default ~/.local/share/osiris/deployed)
#   OSIRIS_DEPLOY_SNAPSHOT_LINK overrides the shim path (default ~/.local/bin/osiris)
# Both overrides exist so a test can point this at a tmp_path sandbox rather than a real box's
# $HOME. Production never sets either.
set -eu

REPO_ROOT="${1:?update_deploy_snapshot.sh: missing <repo_root>}"
SHA="${2:?update_deploy_snapshot.sh: missing <deployed_sha>}"
SNAPSHOT_DIR="${OSIRIS_DEPLOY_SNAPSHOT_DIR:-$HOME/.local/share/osiris/deployed}"
LINK_PATH="${OSIRIS_DEPLOY_SNAPSHOT_LINK:-$HOME/.local/bin/osiris}"

mkdir -p "$(dirname "$SNAPSHOT_DIR")"
mkdir -p "$(dirname "$LINK_PATH")"

if git -C "$SNAPSHOT_DIR" rev-parse --git-dir >/dev/null 2>&1; then
    # already a worktree of this repo (or some repo): move it to the new sha in place.
    # The fetch is a no-op when $SHA is already reachable (the common case: this worktree
    # shares object storage with $REPO_ROOT via the same .git), and cheap otherwise.
    git -C "$SNAPSHOT_DIR" fetch --quiet "$REPO_ROOT" "$SHA" 2>/dev/null || true
    git -C "$SNAPSHOT_DIR" checkout --quiet --detach "$SHA"
else
    # a stale non-worktree directory (or nothing) at this path would make `worktree add`
    # refuse outright, so clear it first. Nothing here is ever the operator's real code; it's
    # a machine-managed pin this script alone owns.
    rm -rf "$SNAPSHOT_DIR"
    git -C "$REPO_ROOT" worktree add --quiet --detach "$SNAPSHOT_DIR" "$SHA"
fi

( cd "$SNAPSHOT_DIR" && uv sync --quiet )

SHIM="$SNAPSHOT_DIR/.venv/bin/osiris"
if [ ! -x "$SHIM" ]; then
    echo "update_deploy_snapshot.sh: uv sync produced no $SHIM — $LINK_PATH left untouched" >&2
    exit 1
fi

TMP_LINK="${LINK_PATH}.new"
rm -f "$TMP_LINK"
ln -s "$SHIM" "$TMP_LINK"
mv "$TMP_LINK" "$LINK_PATH"

echo "deploy snapshot: $LINK_PATH -> $SHIM (pinned at $SHA)"
