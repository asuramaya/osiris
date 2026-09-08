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
# NOTE: the three surfaces started above are KEPT ALIVE through the two operator-requested
# proofs below (dispatch 2c63770d, wave 6) — both need the live MCP server on :8790 (Proof
# A's rename cascade goes through the same orchestrator code the MCP `project(action=
# 'rename')` tool wraps; Proof B's whisper hook posts to :8790/automount). Teardown (the
# pkill trio) moves to just before step 7a, past both proofs — see there.

say ""
say "=== PROOF A (dispatch 2c63770d): mint a project + two seats, rename with the cascade, ==="
say "=== verify the manifest and the read alias                                           ==="
say "CLI verbs used below are the console-script doors onto the SAME orchestrator functions"
say "the MCP tools wrap (osiris = src.cli:main, per pyproject.toml's [project.scripts]; the"
say "bare 'osiris <verb>' form matches every example in the CLI's own --help/epilog text)."

step "5a. mint a coordinator seat + strangerproj's PIN (osiris new)" \
  "cd /work/osiris && \
   uv run osiris new strangercoord --project strangerproj | tee /tmp/new_coord.out && \
   grep -oE 'seat:[0-9a-f]+' /tmp/new_coord.out | head -1 > /tmp/coord_seat.txt && \
   test -s /tmp/coord_seat.txt && \
   echo \"captured coordinator seat: \$(cat /tmp/coord_seat.txt)\""

# osiris new/mint-seat's own --project only writes the SEAT's pin — it does not itself mint
# a graph SoftwareProject (found live here: charter-for and rename-project both refused
# 'strangerproj' with 'not a known repo'/'no such SoftwareProject' until this step ran; a
# harness-scripting gap, not a cascade bug — every one of osiris new/mint-seat/charter-for/
# rename-project's own docstrings independently says so, this just hadn't wired the doors in
# the right order the first time). osiris create-project is the SAME create_project the MCP
# tool wraps (wave 3, thread 5bf6447c) — the actual mint.
step "5b. actually MINT the strangerproj SoftwareProject (osiris create-project)" \
  "cd /work/osiris && \
   uv run osiris create-project strangerproj \
     'stranger-test proof A (dispatch 2c63770d): needs a real SoftwareProject to charter and rename' \
     | tee /tmp/create_proj.out"

step "5c. mint a second, managed seat under the coordinator (osiris mint-seat) — 'two seats'" \
  "cd /work/osiris && \
   COORD=\$(cat /tmp/coord_seat.txt) && \
   uv run osiris mint-seat strangerworker --manager \"\$COORD\" --project strangerproj \
     | tee /tmp/mint_worker.out"

# charter-for's own receipt never puts a per-repo refusal under 'error' (the whole-call key
# cmd_charter_for checks for its own exit code) — a rejected repo lands under 'rejected' with
# the call still exiting 0 ('one bad item never sinks the whole batch', charter_for's own
# docstring) — so this step's own grep -v on 'rejected:' is the thing that actually catches a
# silently-ungoverned seat, not the CLI's exit code alone.
step "5d. charter the coordinator seat to GOVERN strangerproj (osiris charter-for)" \
  "cd /work/osiris && \
   COORD=\$(cat /tmp/coord_seat.txt) && \
   uv run osiris charter-for \"\$COORD\" --repos strangerproj \
     --because 'stranger-test proof A (dispatch 2c63770d): charter a seat so the rename cascade has a governing seat to touch' \
     | tee /tmp/charter.out && \
   ! grep -q '^rejected:' /tmp/charter.out"

step "5e. osiris rename-project strangerproj -> strangerproj2, --apply (the cascade itself)" \
  "cd /work/osiris && \
   uv run osiris rename-project strangerproj strangerproj2 \
     'stranger-test proof A (dispatch 2c63770d): prove the rename cascade + read alias' \
     --apply | tee /tmp/rename.out"

step "5f. verify the manifest actually names the chartered seat as touched/already-correct" \
  "cd /work/osiris && \
   COORD=\$(cat /tmp/coord_seat.txt) && \
   uv run python /usr/local/bin/verify_manifest.py /tmp/rename.out \"\$COORD\""

step "5g. verify the READ ALIAS: strangerproj and strangerproj2 both resolve to the same object" \
  "cd /work/osiris && \
   uv run python /usr/local/bin/verify_read_alias.py strangerproj strangerproj2"

say ""
say "=== PROOF B: mechanical seat mount + the one-sentence whisper, before the first turn  ==="
say "Deliberately a SEPARATE project (strangerprojb) from Proof A's renamed strangerproj2,"
say "not the renamed name itself: project_coordinator_seat's own governs-lookup (seats.py)"
say "matches a project's CANONICAL exactly ('repo:<name>') — and rename_project's own"
say "contract (project_identity.py) is that the canonical NEVER moves on a rename, only the"
say "mutable name property does. Pinning strangerproj2 (the post-rename NAME, not the"
say "canonical) into a fresh tree's .osiris would ask this mechanism a question it was never"
say "built to answer (a name-property-aware coordinator lookup is a different, separate"
say "proof from mechanical seat mount) — so this proof stays on its own uncontested project,"
say "isolating the ONE mechanism being proven (dae06a32) from the ONE proven in Proof A above."

step "6a. mint a second, separate coordinator + strangerprojb's PIN (osiris new)" \
  "cd /work/osiris && \
   uv run osiris new strangercoordb --project strangerprojb | tee /tmp/new_coordb.out && \
   grep -oE 'seat:[0-9a-f]+' /tmp/new_coordb.out | head -1 > /tmp/coordb_seat.txt && \
   test -s /tmp/coordb_seat.txt && \
   echo \"captured coordinator-b seat: \$(cat /tmp/coordb_seat.txt)\""

step "6b. actually MINT the strangerprojb SoftwareProject (osiris create-project)" \
  "cd /work/osiris && \
   uv run osiris create-project strangerprojb \
     'stranger-test proof B (thread dae06a32): needs a real SoftwareProject to charter, so project_coordinator_seat has a governs edge to find' \
     | tee /tmp/create_projb.out"

step "6c. charter that coordinator to GOVERN strangerprojb (mechanical_seat_mount needs one)" \
  "cd /work/osiris && \
   COORDB=\$(cat /tmp/coordb_seat.txt) && \
   uv run osiris charter-for \"\$COORDB\" --repos strangerprojb \
     --because 'stranger-test proof B (thread dae06a32): an unmanaged seat must govern the project before mechanical_seat_mount can mint a fresh worker under it' \
     | tee /tmp/charterb.out && \
   ! grep -q '^rejected:' /tmp/charterb.out"

step "6d. drop a .osiris pin in a fresh tree (seat=strangerhandle, an UNMINTED handle)" \
  "mkdir -p /work/strangerseat-tree && \
   cat > /work/strangerseat-tree/.osiris <<'PINEOF'
seat = 'strangerhandle'
project = 'strangerprojb'
PINEOF
   cat /work/strangerseat-tree/.osiris"

# python3 note: this container has no SYSTEM python3 at all (only uv's own managed toolchain
# under ~/.local/share/uv/python/, no PATH symlink) — a bare 'python3' wall-hit here on first
# run is a harness-scripting gap, not a doc finding (docs/INSTALL.md's own real-world target
# is a Claude Code harness invoking this SAME hook script with ITS OWN system python3, which
# a real operator's box has); 'uv run python3' resolves the SAME osiris_hook.py against the
# project's own managed interpreter instead, matching how every other step already invokes
# python in this harness.
step "6e. simulate a SessionStart whisper (no prior mount) — assert the mechanical sentence" \
  "cd /work/osiris && \
   echo '{\"session_id\": \"strangertest-session-1\", \"cwd\": \"/work/strangerseat-tree\", \"source\": \"startup\"}' \
     | uv run python3 scripts/osiris_hook.py whisper | tee /tmp/whisper.out && \
   grep -qF 'do nothing until spoken to' /tmp/whisper.out && \
   grep -qF 'strangerhandle' /tmp/whisper.out && \
   grep -qF 'strangerprojb' /tmp/whisper.out"

say ""
say "--- tearing down the three live surfaces (started in step 4, kept alive through both"
say "    proofs above) ---"
pkill -f uvicorn 2>/dev/null; pkill -f "arq src.workers" 2>/dev/null; pkill -f src.mcp_server 2>/dev/null

step "7a. pytest (all three docs agree on this one)" "cd /work/osiris && uv run pytest -q"

# The rest of step 7 USED TO extract the gate lines from the checked-out docs at run
# time, on the theory that grepping the live files means a future doc edit gets
# re-tested automatically. It went stale anyway (thread 2edf2878, decision fb8e723c):
# README.md and docs/INSTALL.md were consolidated to point at CONTRIBUTING.md rather
# than each repeating the commands, so the grep came back empty for both and steps
# 7b-7d silently never ran — the exact "walls hidden by staleness" failure this
# extraction was supposed to prevent, just from the opposite direction (a doc that
# stopped saying something, not one that started saying something different). Fixed
# by running the two real gate commands directly, hardcoded — they can no longer
# silently skip on a doc-structure change, at the cost of needing a manual edit here
# if the commands themselves ever change (the same trade CONTRIBUTING.md itself makes
# by being the one place that states them).
step "7b. ruff" "cd /work/osiris && uv run ruff check src tests"
step "7c. mypy" "cd /work/osiris && uv run mypy --strict src"

say ""
say "=== SUMMARY ==="
if [ -s "$WALLS" ]; then
  say "WALLS HIT ($(wc -l < "$WALLS")):"
  cat "$WALLS" | tee -a "$LOG"
else
  say "no walls — every replayed step passed"
fi
