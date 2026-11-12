<!-- topic: getting-started -->

# Installation & Setup

This guide takes you from an empty environment to a running Osiris instance with active agent connections across DeepSeek Harness, Claude Code, and other MCP-enabled environments.

---

## 1. Prerequisites

- **OS**: Linux or macOS
- **Python**: 3.12+ (managed with [`uv`](https://docs.astral.sh/uv/))
- **Database**: PostgreSQL 16+ (with `pg_trgm` and optional `pgvector`)
- **Cache/Queue**: Redis 7+
- **Agent Harnesses** (one or more):
  - [DeepSeek Harness (DSH)](https://github.com/deepseek-ai)
  - Claude Code
  - Cursor, Windsurf, OpenDevin, or any standard MCP client
- **Test-suite only** (not needed to run the service, only for `uv run pytest`):
  - Node.js: `test_js_syntax.py` shells out to `node --check` on the static UI's `.js` files
  - `zstd`: `test_dsh_adapter.py` builds its fixtures via a real `zstd`-compressed session

---

## 2. Clone & Sync Dependencies

```bash
git clone https://github.com/asuramaya/osiris.git
cd osiris
uv sync
```

---

## 3. Database & Redis Setup

### Option A: Docker (Fastest)

```bash
# Start PostgreSQL 16 and Redis 7
docker run -d --name osiris-pg \
  -e POSTGRES_USER=osiris \
  -e POSTGRES_PASSWORD=osiris \
  -e POSTGRES_DB=osiris \
  -p 127.0.0.1:5432:5432 \
  osiris-pg:16-repack   # postgres:16 plus pg_repack; build it first: docker build -t osiris-pg:16-repack deploy/pg

docker run -d --name osiris-redis \
  -p 127.0.0.1:6379:6379 \
  redis:7

export DATABASE_URL="postgresql://osiris:osiris@127.0.0.1:5432/osiris"
export REDIS_URL="redis://127.0.0.1:6379/0"
```

### Option B: Existing PostgreSQL & Redis
Export your connection strings:

```bash
export DATABASE_URL="postgresql://<user>:<password>@127.0.0.1:<port>/<dbname>"
export REDIS_URL="redis://127.0.0.1:<port>/0"
```

---

## 4. Database Migrations & Initial Seeding

Run database schema migrations and seed default ontology catalog types and design canon:

```bash
# Apply all Alembic migrations
uv run alembic upgrade head

# Seed catalog types, default rooms, compositions, and design references
uv run python -m src.init
```

---

## 5. Starting the Core Services

Run these services directly or manage them via systemd (see [Step 7](#7-systemd-service-management)):

```bash
# 1. Start the persistent FastMCP server (default port 8790)
OSIRIS_MCP_TRANSPORT=streamable-http uv run python -m src.mcp_server

# 2. Start the background worker (queue drain, lease cleanup, and digests)
uv run arq src.workers.arq_worker.WorkerSettings

# 3. (Optional) Start the human-facing FastAPI console (default port 8011)
uv run uvicorn --factory src.api.app:create_app --host 127.0.0.1 --port 8011
```

### Verifying Service Health
- **Console**: `curl -s http://127.0.0.1:8011/health` → `{"status":"ok"}`
- **MCP Server**: `curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8790/mcp` → `406` (406 indicates the streamable HTTP endpoint is active and awaiting client handshake).

---

## 6. Connecting Agent Harnesses

### A. DeepSeek Harness (DSH)

#### 1. Add MCP Server Bridge
In your DSH profile configuration (e.g. `~/.dsh/profiles/web/cordis.patch.yml`):

```yaml
plugins:
  "@deepseek-ai/dsh-mcp-client":
    servers:
      osiris:
        type: streamable-http
        url: http://127.0.0.1:8790/mcp
```

#### 2. (Optional) Install the Native DSH Cordis Plugin
To enable in-process auto-mounting and automatic context-seam settlement, install the
plugin into your own DSH checkout (the plugin source lives in this repo, under
`dsh-plugin/osiris-bridge/`, and is copied into the harness, never authored there):

```bash
dsh-plugin/install.sh [/path/to/deepseek-harness] [/path/to/dsh/profile]
```

Both arguments are optional; each defaults to the paths named in the script itself.
Safe, and required, to re-run after every upstream harness pull. See
`dsh-plugin/README.md` for what the script does and why.

---

### B. Claude Code

Add Osiris to your Claude Code user config:

```bash
claude mcp add --scope user --transport http osiris http://127.0.0.1:8790/mcp \
  --header 'X-Osiris-Job: ${CLAUDE_JOB_DIR}'
```

`osiris deploy` then approves the server once for every directory (it adds `osiris` to
`enabledMcpjsonServers` in `~/.claude/settings.json`, keeping every other setting), so Claude
Code stops asking you to approve it again for each new git worktree or repo. Re-running is a
no-op, and a settings file it cannot safely merge is left untouched and reported.
`osiris onboard` writes no per-repo `.mcp.json` once the user-scope registration above exists
(pass `--repo-pinned` to write one anyway).

Or add to project-specific `.mcp.json` (without the user-scope registration above):

```json
{
  "mcpServers": {
    "osiris": {
      "type": "streamable-http",
      "url": "http://127.0.0.1:8790/mcp",
      "headers": {
        "X-Osiris-Job": "${CLAUDE_JOB_DIR}"
      }
    }
  }
}
```

---

### C. Cursor & Windsurf

In Cursor/Windsurf MCP configuration:

```json
{
  "mcpServers": {
    "osiris": {
      "url": "http://127.0.0.1:8790/mcp"
    }
  }
}
```

---

## 7. Systemd Service Management (Production / Local Daemon)

To keep Osiris running persistently in the background across reboots:

```bash
mkdir -p ~/.config/systemd/user
cp deploy/user/*.service ~/.config/systemd/user/

# Reload and enable units
systemctl --user daemon-reload
systemctl --user enable --now osiris-mcp osiris-worker osiris-pulse osiris-console osiris-manager

# Allow user units to persist after logout
loginctl enable-linger "$USER"
```

Check status:
```bash
systemctl --user status osiris-mcp osiris-worker osiris-manager
```

---

## 8. Recovery method and backup targets (first run only)

The encryption key and the backup password need no command: `osiris deploy` (step 7) creates
both when they are missing, before it restarts the services, and a background job in the
worker encrypts any older plain-text rows and shows its progress in `osiris soul-key
status`. Only the steps that need you are left. Run each in your own terminal, as the same
user the services run as (your own login user, for the deployment shape used above):

```bash
# 1. Enroll a FIDO2 Security Key as your recovery path (plug it in first). The same
#    enrollment also protects the backup password
osiris soul-key enroll-recovery

# 2. Optional: prove the recovery works. Same PIN and touch, changes nothing
osiris soul-key verify-recovery

# 3. Add your offload targets: a local drive, a network drive, or both (see BACKUP.md)
osiris backup-settings write \
  --offload-add nas --offload-kind restic --offload-target "sftp:nas.local:/backups/osiris" \
  --offload-schedule "*:0/15" --because "first-run offload target"

# 4. Run one offload attempt by hand to confirm it actually reaches the target
osiris offload-runner tick

# 5. Prove the offload target genuinely restores, not just that it accepted a backup
osiris soul-key restore-drill
```

`osiris backup-status` and the console's Settings pane, opened from the command palette or
the header's gear icon, are your ongoing dashboard after this. See
[`BACKUP.md`](BACKUP.md) and [`REFERENCE.md`](REFERENCE.md#the-consoles-settings-pane).
