#!/usr/bin/env bash
# Install the Osiris bridge into a vendored DSH (DeepSeek Harness) checkout.
#
# WHY THIS EXISTS (task #194): the bridge is a Cordis plugin that must live INSIDE
# the harness's own pnpm workspace to resolve its peer deps, but that harness is a
# vendored third-party clone we do not own and cannot commit to. Left alone, the
# package sits there UNTRACKED and the ten registration edits sit there UNCOMMITTED —
# one `git clean -fdx`, branch switch, or upstream pull and the whole integration is
# gone. This script makes the osiris repo the source of truth and the harness a
# derived, re-creatable target.
#
# Idempotent: safe to re-run after any harness reset. Run it again after every
# upstream pull.
#
#     dsh-plugin/install.sh [/path/to/deepseek-harness] [/path/to/dsh/profile]
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HARNESS="${1:-/home/asuramaya/code/dsh/deepseek-harness}"
PROFILE="${2:-$HOME/.dsh/profiles/web}"
PKG_DIR="$HARNESS/packages/experimental/osiris-bridge"
PKG_NAME="@deepseek-ai/dsh-experimental-osiris-bridge"

say() { printf '  %s\n' "$*"; }

[ -d "$HARNESS/packages/experimental" ] || {
  echo "not a DSH harness checkout: $HARNESS" >&2; exit 1; }

echo "==> 1/5 sources -> $PKG_DIR"
mkdir -p "$PKG_DIR/src" "$PKG_DIR/tests"
for f in package.json tsconfig.json tsdown.config.ts README.md README.zh.md README.i18n.yaml; do
  cp "$HERE/osiris-bridge/$f" "$PKG_DIR/$f"
done
cp "$HERE/osiris-bridge/src/"*.ts   "$PKG_DIR/src/"
cp "$HERE/osiris-bridge/tests/"*.ts "$PKG_DIR/tests/"
say "9 source files copied"

echo "==> 2/5 harness registration overlay"
# The overlay touches tracked upstream files (tsconfig.host.json project refs, the
# experimental package README index, generated doc catalogs, pnpm-lock). Applying it
# twice must not double-apply, so check first and treat already-applied as success.
cd "$HARNESS"
if git apply --check --reverse "$HERE/harness-overlay.patch" 2>/dev/null; then
  say "already applied — nothing to do"
elif git apply --check "$HERE/harness-overlay.patch" 2>/dev/null; then
  git apply "$HERE/harness-overlay.patch"; say "applied"
else
  say "REFUSED: overlay does not apply cleanly to this harness tree."
  say "Upstream has moved. Re-derive it:  git -C '$HARNESS' diff > '$HERE/harness-overlay.patch'"
  exit 1
fi

echo "==> 3/5 workspace link + build"
# Only resolve the workspace when the links are actually absent. A vendored harness
# already carries them, and an unconditional `pnpm install` there costs minutes and
# churns a lockfile we do not own — the first cut of this script timed out doing
# exactly that against an already-installed tree.
if [ -d "$PKG_DIR/node_modules/@deepseek-ai" ]; then
  say "workspace links present — skipping pnpm install"
else
  say "linking workspace (first install; this takes a few minutes)"
  pnpm install --filter "$PKG_NAME..." --prefer-offline || pnpm install
fi
# The package declares NO build script — the harness builds workspace-wide
# (`tsc -b tsconfig.host.json && tsdown`). Do the same two steps scoped to THIS
# package: the project reference emits lib/types/*.js, then tsdown bundles those
# into lib/index.js + lib/invariant.js per its own tsdown.config.ts.
( cd "$HARNESS" && npx tsc -b "packages/experimental/osiris-bridge/tsconfig.json" )
( cd "$PKG_DIR" && npx tsdown >/dev/null 2>&1 )
[ -f "$PKG_DIR/lib/index.js" ] || { echo "build produced no lib/index.js" >&2; exit 1; }
say "lib/index.js built ($(stat -c %s "$PKG_DIR/lib/index.js") bytes)"

echo "==> 4/5 profile resolution -> $PROFILE"
# A bare symlink here is what the hand-wired setup used, and `pnpm install` in the
# profile prunes anything absent from package.json. Declare the dependency so the
# link is a WORKSPACE FACT rather than a loose file that survives only by luck.
mkdir -p "$PROFILE/node_modules/@deepseek-ai"
ln -sfn "$PKG_DIR" "$PROFILE/node_modules/@deepseek-ai/dsh-experimental-osiris-bridge"
python3 - "$PROFILE/package.json" "$PKG_DIR" <<'PY'
import json, os, sys
path, pkg_dir = sys.argv[1], sys.argv[2]
doc = json.load(open(path)) if os.path.exists(path) else {"name": "dsh-profile", "private": True}
deps = doc.setdefault("dependencies", {})
want = "link:" + pkg_dir
if deps.get("@deepseek-ai/dsh-experimental-osiris-bridge") != want:
    deps["@deepseek-ai/dsh-experimental-osiris-bridge"] = want
    json.dump(doc, open(path, "w"), indent=2)
    open(path, "a").write("\n")
    print("  declared as a link: dependency")
else:
    print("  dependency already declared")
PY

echo "==> 5/5 verify against the live Osiris server"
BASE="${OSIRIS_BASE_URL:-http://127.0.0.1:8790}"
if code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 \
      -H 'content-type: application/json' \
      -d '{"session_id":"install-probe","cwd":"'"$PWD"'","source":"install.sh"}' \
      "$BASE/automount" 2>/dev/null); then
  say "/automount -> HTTP $code $([ "$code" = 200 ] && echo '(reachable)' || echo '(NOT ok)')"
else
  say "/automount unreachable at $BASE — bridge will degrade to a note, which is by design"
fi

echo
echo "Installed. Register it by merging deploy/dsh-osiris-preset.yml into"
echo "$PROFILE/cordis.patch.yml, then restart DSH (HMR does not reliably pick up a new plugin)."
