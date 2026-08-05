#!/usr/bin/env bash
# The stranger's test (task #97): one command, re-run after every fix to docs/INSTALL.md
# or the install path it describes.
#
#   ./scripts/stranger_test.sh                                # mirrors local `main`
#   ./scripts/stranger_test.sh https://github.com/asuramaya/osiris   # the real public repo
#
# WHAT THIS ISOLATES:
#   - No inherited env vars: `docker run` below passes none through from this shell.
#   - No ~/.osiris, no .venv, no uv cache: the container filesystem starts empty; nothing
#     of this host's home directory or venv is bind-mounted in.
#   - No collision with, or silent reuse of, this host's own docker state: Postgres/Redis
#     run inside a nested dockerd private to the test container (Docker-in-Docker), not
#     via this host's docker socket. That matters concretely — this box already has
#     containers literally named osiris-pg/osiris-redis from unrelated dev work; a
#     host-socket harness would collide with them or quietly inherit their already-pulled
#     image layers.
#   - The default source is a LOCAL bare mirror of `main`, not the real GitHub URL — because
#     as of this run, https://github.com/asuramaya/osiris answers with an empty ref
#     advertisement (verified: `git ls-remote origin` returns nothing, and a raw
#     info/refs?service=git-upload-pack request 200s with the null-OID "empty repo" reply).
#     Nothing has been pushed there since the PII rewrite. Pass the real URL as $1 once the
#     operator publishes, to test the actual clone path instead of this stand-in.
#
# WHAT THIS DOES NOT ISOLATE (named, not silently assumed away):
#   - Base OS: one Debian bookworm-slim image. "Linux" in INSTALL.md's prerequisites covers
#     far more than that; this is one representative point, not the space.
#   - Python 3.12 is provided by uv's own managed toolchain (uv sync honors
#     `requires-python`), not a system python3.12 package — a legitimate reading of the
#     prerequisite, but not evidence a system-python stranger sees identical behavior.
#   - Network: assumes unrestricted outbound HTTPS (PyPI, astral's python builds, the
#     container registry, GitHub). A stranger behind a corporate proxy or an offline box is
#     not represented here.
#   - Docker image cache: this container's nested dockerd starts cold every run (postgres:16
#     / redis:7 are pulled fresh each time) — slower than a repeat stranger run, but a truer
#     first-run measurement than a warm host cache would give.
#   - Timing/hardware: this host's CPU/disk speed, not a lower-end machine's.
#   - INSTALL.md's own steps 5 (systemd --user units) and 6 (wiring into Claude Code) are
#     NOT replayed: step 5 needs a real logind session (fragile/meaningless in a throwaway
#     container) and step 6 needs an authenticated Claude Code install, which can't be
#     scripted headlessly. Both are named gaps, not passed-by-omission.
#   - This replays INSTALL.md only. It does not touch README.md's or CONTRIBUTING.md's own
#     (overlapping but not identical) quickstarts, except where step 7's gate commands are
#     compared across all three docs inside run.sh.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SOURCE_ARG="${1:-}"
OUT_DIR="${OUT_DIR:-$REPO_ROOT/.stranger-test-out}"
IMAGE_TAG="osiris-stranger-test"

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

MOUNTS=()
if [ -z "$SOURCE_ARG" ]; then
  MIRROR_DIR="$OUT_DIR/mirror.git"
  MAIN_SHA="$(git -C "$REPO_ROOT" rev-parse main)"
  echo "no source given — mirroring local main @ $MAIN_SHA into a bare repo"
  git -C "$REPO_ROOT" clone --bare --single-branch --branch main "$REPO_ROOT" "$MIRROR_DIR" \
    >"$OUT_DIR/mirror-clone.log" 2>&1
  SOURCE_URL="file:///mirror/osiris.git"
  MOUNTS+=(-v "$MIRROR_DIR:/mirror/osiris.git:ro")
else
  echo "source given explicitly: $SOURCE_ARG"
  SOURCE_URL="$SOURCE_ARG"
fi

echo "building the stranger-machine image..."
docker build -t "$IMAGE_TAG" "$SCRIPT_DIR/stranger_test" >"$OUT_DIR/build.log" 2>&1 \
  || { echo "image build failed — see $OUT_DIR/build.log"; tail -40 "$OUT_DIR/build.log"; exit 1; }

echo "running the harness (privileged, isolated dind — see this script's own header for"
echo "what that does and does not isolate)..."
set +e
docker run --rm --privileged \
  -e STRANGER_SOURCE="$SOURCE_URL" \
  -v "$OUT_DIR:/out" \
  "${MOUNTS[@]}" \
  "$IMAGE_TAG"
RUN_RC=$?
set -e

echo ""
echo "=== full log: $OUT_DIR/log.txt ==="
if [ -s "$OUT_DIR/walls.txt" ]; then
  echo "=== WALLS ($(wc -l < "$OUT_DIR/walls.txt")) — $OUT_DIR/walls.txt ==="
  cat "$OUT_DIR/walls.txt"
else
  echo "no walls file written — container may not have started run.sh at all (harness bug, not a doc finding); check $OUT_DIR/log.txt"
fi
exit "$RUN_RC"
