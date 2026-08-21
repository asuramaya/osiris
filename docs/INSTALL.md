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
  postgres:16

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
To enable in-process auto-mounting and automatic context-seam settlement:

```bash
cd dsh-plugin
npm install
npm run build
```

Add the plugin to your Cordis configuration:
```yaml
plugins:
  "osiris-dsh-plugin":
    path: "/path/to/osiris/dsh-plugin"
    mcpUrl: "http://127.0.0.1:8790/mcp"
```

---

### B. Claude Code

Add Osiris to your Claude Code user config:

```bash
claude mcp add --scope user --transport http osiris http://127.0.0.1:8790/mcp \
  --header 'X-Osiris-Job: ${CLAUDE_JOB_DIR}'
```

Or add to project-specific `.mcp.json`:

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
systemctl --user enable --now osiris-mcp osiris-worker osiris-pulse osiris-console

# Allow user units to persist after logout
loginctl enable-linger "$USER"
```

Check status:
```bash
systemctl --user status osiris-mcp osiris-worker
```
