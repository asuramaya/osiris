#!/usr/bin/env bash
# Runs INSIDE the "stranger machine" container (see ../Dockerfile). Replays
# docs/INSTALL.md verbatim, in the order the doc presents it, and records PASS/WALL per
# step to $OUT_DIR/log.txt + walls.txt. It does not fix anything it finds broken — a wall
# hit here is this pass's product, not a bug for this script to paper over (task #97,
# mail thread 3715). See scripts/stranger_test.sh for what this container does and does
# not isolate from the host.
set +e  # one wall must not hide the next — continue past failures

STRANGER_SOURCE="${STRANGER_SOURCE:-file:///mirror/osiris.git}"
OUT_DIR="${OUT_DIR:-/out}"
mkdir -p "$OUT_DIR"
LOG="$OUT_DIR/log.txt"
WALLS="$OUT_DIR/walls.txt"
: > "$LOG"
: > "$WALLS"

say() { printf '%s\n' "$*" | tee -a "$LOG"; }

# step NAME -- CMD    runs CMD via bash -c, streams+logs output, records PASS/WALL.
# Each call is its own subshell (cd/export don't persist across steps) — anything a later
# step needs (DATABASE_URL, REDIS_URL) is exported in run.sh's own top-level scope instead,
# matching where INSTALL.md itself puts those export lines.
step() {
  local name="$1"; shift
  say ""
  say "=== STEP: $name ==="
  say "+ $*"
  bash -c "$*" 2>&1 | tee -a "$LOG"
  local rc=${PIPESTATUS[0]}
  if [ "$rc" -eq 0 ]; then
    say "PASS: $name"
  else
    say "WALL: $name (exit $rc)"
    echo "$name (exit $rc)" >> "$WALLS"
  fi
}

say "### stranger-test harness — replaying docs/INSTALL.md ###"
say "source: $STRANGER_SOURCE"

# Harness plumbing, not a doc step: the local-mirror substitution (see stranger_test.sh's
# header) bind-mounts a host-owned bare repo in; git >=2.35.2 refuses to touch a repo it
# doesn't own unless told to trust it. A real `https://` clone never hits this — it is an
# artifact of the substitution, not a product finding, so it is fixed here rather than left
# to cascade into every later step (which is exactly what it did the first time this ran).
git config --global --add safe.directory /mirror/osiris.git 2>/dev/null || true

say ""
say "--- harness bootstrap: starting the nested dockerd (not a doc step; INSTALL.md's own"
say "    prerequisite #0 says Docker is already installed and running) ---"
dockerd --storage-driver=vfs >/var/log/dockerd.log 2>&1 &
for _ in $(seq 1 30); do
  docker info >/dev/null 2>&1 && break
  sleep 1
done
if ! docker info >/dev/null 2>&1; then
  say "HARNESS FAILURE (not a doc finding): nested dockerd never came up"
  tail -40 /var/log/dockerd.log | tee -a "$LOG"
fi

step "0. prerequisites present" "git --version && uv --version && docker --version"

mkdir -p /work
step "1. clone + dependencies" \
  "cd /work && git clone '$STRANGER_SOURCE' osiris && cd osiris && uv sync"

step "2. substrate — postgres + redis containers" \
  "docker run -d --name osiris-pg -e POSTGRES_USER=osiris -e POSTGRES_PASSWORD=osiris \
     -e POSTGRES_DB=osiris -p 127.0.0.1:5432:5432 postgres:16 && \
   docker run -d --name osiris-redis -p 127.0.0.1:6379:6379 redis:7"
export DATABASE_URL=postgresql://osiris:osiris@127.0.0.1:5432/osiris
export REDIS_URL=redis://127.0.0.1:6379/0
say "exported DATABASE_URL, REDIS_URL (doc's own export lines, run in this shell so later"
say "steps inherit them)"
say "NOTE: INSTALL.md's docker run lines carry no --health-cmd (unlike this repo's own"
say "docker-compose.yml, which does) and the doc does not tell the stranger to wait for"
say "readiness before step 3. This harness does not add a wait either — step 3 below runs"
say "immediately, exactly as a copy-paste stranger would hit it. If it races, that IS the"
say "finding, not a harness bug to smooth over."

step "3. schema + seed" \
  "cd /work/osiris && uv run alembic upgrade head && uv run python -m src.init"

# The doc's own claim (docs/INSTALL.md's verify line, README/CONTRIBUTING's aren't this
# specific) is that a bare GET here correctly returns 406, not that it returns 2xx — so
# the check below asserts exactly 406, it does not use curl -f (which would treat 406 as
# a failure, i.e. would flunk the documented-correct behavior).
step "4. three surfaces + doc's own health verify" \
  "cd /work/osiris && \
   (uv run uvicorn --factory src.api.app:create_app --host 127.0.0.1 --port 8011 >/tmp/console.log 2>&1 &) && \
   (uv run arq src.workers.arq_worker.WorkerSettings >/tmp/worker.log 2>&1 &) && \
   (OSIRIS_MCP_TRANSPORT=streamable-http uv run python -m src.mcp_server >/tmp/mcp.log 2>&1 &) && \
   sleep 8 && \
   curl -sf 127.0.0.1:8011/health && echo && \
   mcp_code=\$(curl -s -o /dev/null -w '%{http_code}' 127.0.0.1:8790/mcp) && \
   echo \"mcp http_code=\$mcp_code (expect 406 per docs/INSTALL.md)\" && \
   [ \"\$mcp_code\" = 406 ]"
say "--- surface logs (tails) ---"
for f in /tmp/console.log /tmp/worker.log /tmp/mcp.log; do
  say "-- $f --"; tail -20 "$f" 2>/dev/null | tee -a "$LOG"
done
pkill -f uvicorn 2>/dev/null; pkill -f "arq src.workers" 2>/dev/null; pkill -f src.mcp_server 2>/dev/null

step "7a. pytest (all three docs agree on this one)" "cd /work/osiris && uv run pytest -q"

# The rest of step 7 is EXTRACTED from the checked-out docs at run time, not copied here
# as a fixed string — a fixed copy is exactly what went stale the moment the docs were
# fixed to agree with each other (this script used to hardcode three DIFFERENT ruff
# invocations to catch cross-doc drift; once the drift was fixed, that comparison started
# testing wording no doc contains anymore, which would have silently kept reporting
# "walls" for a problem that no longer exists). Grepping the live files means a future
# doc edit gets re-tested automatically — the whole point of "one command, re-run after
# every fix."
readme_line="$(grep -m1 'ruff check' /work/osiris/README.md)"
contrib_ruff="$(grep -m1 'ruff check' /work/osiris/CONTRIBUTING.md)"
contrib_mypy="$(grep -m1 'mypy' /work/osiris/CONTRIBUTING.md)"
install_ruff="$(grep -m1 'ruff check' /work/osiris/docs/INSTALL.md)"
install_mypy="$(grep -m1 'mypy' /work/osiris/docs/INSTALL.md)"

if [ -z "$readme_line" ] || [ -z "$contrib_ruff" ] || [ -z "$contrib_mypy" ] || [ -z "$install_ruff" ] || [ -z "$install_mypy" ]; then
  say "HARNESS FAILURE (not a doc finding): could not extract one of the five expected gate lines from README.md/CONTRIBUTING.md/docs/INSTALL.md — their structure changed and this script's grep patterns need updating"
else
  step "7b. README.md's own gate line" "cd /work/osiris && $readme_line"
  step "7c. CONTRIBUTING.md's own gate lines" "cd /work/osiris && $contrib_ruff && $contrib_mypy"
  step "7d. INSTALL.md's own gate lines" "cd /work/osiris && $install_ruff && $install_mypy"
fi

say ""
say "=== SUMMARY ==="
if [ -s "$WALLS" ]; then
  say "WALLS HIT ($(wc -l < "$WALLS")):"
  cat "$WALLS" | tee -a "$LOG"
else
  say "no walls — every replayed step passed"
fi
