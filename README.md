# Osiris

**The persistent memory and coordination graph for AI agents.** A harness-agnostic, provenance-first entity-graph engine that turns work history, architectural decisions, loose ends, and public records into durable, queryable, graph-backed memory.

Osiris gives autonomous agents and human developers persistent continuity across sessions, context windows, compactions, and harness boundaries (DeepSeek Harness, Claude Code, Cursor, Windsurf, or custom agent frameworks).

---

## Why Osiris Exists

Modern LLM agent harnesses operate in transient contexts. When a context window compacts or a session ends, reasoning is lost, decisions are forgotten, and cross-session loose ends disappear into unstructured logs.

**Osiris provides the missing organs:**

1. **Persistent Memory Graph** — An append-only, event-sourced entity graph in PostgreSQL + Redis holding every project, commit, decision, open thread, agent identity, and design reference.
2. **Harness Agnosticity** — Operates over standard Model Context Protocol (MCP) and custom lightweight adapters. Whether running DeepSeek Harness (DSH), Claude Code, or any MCP-enabled agent, all agents share the exact same living graph.
3. **Provenance & Evidence Taxonomy** — Every assertion and link carries an evidence grade (`SELF_DECLARED`, `AUTHORITATIVE_API`, `DIRECT_OBSERVATION`, `CO_OCCURRENCE`, `DERIVED`, `CORROBORATED`). Confidence is a verifiable projection of source provenance, never a heuristic guess.
4. **First-Class Inter-Agent Mail & Fleet Coordination** — Graphed communication between agents with `Message` objects and typed graph links (`sent_by`, `addressed_to`, `broadcast_to`, `replies_to`, `in_thread`, `mentions`), with at-least-once delivery, prior-art detection, and multi-tenant seat management.
5. **Granular, High-Efficiency Retrieval** — Replaces massive context-bloating dumps with lightweight queries: `get_status` (~360 chars), `graph_search` (scoped subgraph retrieval with neighborhood expansion), `get_decision_list`, `get_thread_list`, and `consult_canon`.

---

## Architecture at a Glance

```
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                            AGENT HARNESSES                                  │
  │   DeepSeek Harness (DSH)    │   Claude Code   │   Cursor / Windsurf / Custom │
  │   (Cordis In-Process Plugin)│   (CLI / Hooks) │   (Standard MCP Clients)    │
  └─────────────────────────────┴─────────────────┴─────────────────────────────┘
                                       │ (MCP over HTTP / JSON-RPC :8790)
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                          SURFACES & RETRIEVAL                               │
  │  FastMCP Server (130+ Tools)  ·  FastAPI Console (:8011)  ·  Composer / Lenses│
  │  Granular Retrieval: get_status · graph_search · get_decision_list · get_mail│
  └─────────────────────────────────────────────────────────────────────────────┘
                                       │
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                       NARROW WAIST (Actions API)                            │
  │   create_or_find_object · assert_property · create_link · merge · set_status│
  └─────────────────────────────────────────────────────────────────────────────┘
                                       │
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                           EVENT-SOURCED KERNEL                              │
  │   PostgreSQL 16 (Objects, Links, Assertions, Events)  ·  Redis 7 (Bus/Cache)│
  │   Evidence Taxonomy  ·  Merge/Unmerge Algebra  ·  Autonomous Heartbeat Loop  │
  └─────────────────────────────────────────────────────────────────────────────┘
```

---

## Key Capabilities

### 1. Granular Agent State & Graph-Aware Search
Instead of consuming tens of thousands of tokens dumping entire project histories on every turn:
- **`get_status`**: Ultra-lightweight glance (~360 chars) returning current identity, mail counters, and fleet pulse.
- **`graph_search(query, project?, lineage?, max_depth?)`**: Lexical + semantic hybrid search scoped to a project or agent lineage, with 1–3 hop graph neighborhood expansion.
- **`get_decision_list` / `get_thread_list`**: Paginated and filtered access to decisions and open obligations/tasks/questions.
- **`consult_canon`**: Recalls design references and historical architecture essays on demand.

### 2. Graphed Inter-Agent Mailbox
Agents coordinate across project boundaries:
- **Broadcasts (`to="<project>"`)** and **Direct Messages (`to_agent="<id>"`)**.
- Every exchange mints a **`Message`** object in the ontology with typed graph relationships (`sent_by`, `addressed_to`, `replies_to`, `in_thread`).
- **Prior-Art Inspection**: Dispatched messages and recorded rulings automatically check against standing decisions and practices to prevent re-deriving known solutions.

### 3. Unified Lifecycle Hooks & Harness Adapters
- **DeepSeek Harness (DSH)**: Zero-friction integration via the native Cordis plugin (`dsh-plugin`), providing automatic turn-start mounting, background status polling, and pre-compaction graph settlement.
- **Claude Code**: Collapsed 13 legacy standalone subprocess hooks into a single, high-performance stdlib CLI (`scripts/osiris_hook.py`) communicating over fast HTTP endpoints (~30ms vs ~500ms).
- **Harness Adapter Protocol**: Pluggable `HarnessAdapter` protocol supporting transcript reading and model swap discovery across formats.

---

## Quickstart

### 1. Prerequisites & Environment
- Python 3.12+ (`uv` recommended)
- PostgreSQL 16+ (with `pg_trgm`)
- Redis 7+

```bash
# Clone and sync dependencies
git clone https://github.com/asuramaya/osiris.git
cd osiris
uv sync

# Run database migrations
DATABASE_URL=postgresql://osiris:osiris@127.0.0.1:5432/osiris uv run alembic upgrade head

# Initialize default ontology catalog and design canon
uv run python -m src.init
```

### 2. Running the Server
```bash
# Start persistent MCP server on port 8790
DATABASE_URL=postgresql://osiris:osiris@127.0.0.1:5432/osiris uv run python -m src.mcp_server

# (Optional) Start the human-facing console on port 8011
DATABASE_URL=postgresql://osiris:osiris@127.0.0.1:5432/osiris uv run uvicorn src.api.app:app --port 8011
```

### 3. Connecting Agent Harnesses

#### DeepSeek Harness (DSH)
Add to your DSH configuration profile (e.g. `~/.dsh/profiles/web/cordis.patch.yml`):

```yaml
plugins:
  "@deepseek-ai/dsh-mcp-client":
    servers:
      osiris:
        type: streamable-http
        url: http://127.0.0.1:8790/mcp
```

#### Claude Code
Add to your `~/.claude/settings.json` or project `.mcp.json`:

```json
{
  "mcpServers": {
    "osiris": {
      "type": "streamable-http",
      "url": "http://127.0.0.1:8790/mcp"
    }
  }
}
```

---

## The Agent Lifecycle Ritual

When working in an Osiris-backed project, agents follow a structured, low-overhead cycle:

1. **Mount** (`mount(cwd, job_dir)`): Connects the session to the persistent identity graph and binds to its designated seat.
2. **Glance** (`get_status()`): Checks unread mail, active obligations, and fleet pulse (~360 chars).
3. **Recall** (`graph_search(q)` / `consult_canon(q)`): Recalls prior rulings and architectural invariants before writing code.
4. **Capture** (`record_decision(...)` / `open_thread(...)`): Records architectural rulings, choices, and open obligations as they occur.
5. **Settle** (`settle(...)`): Flushes pending decisions, closes resolved threads, and seals the context window state before compaction.

---

## Documentation Index

- [**Cross-Harness Architecture**](docs/CROSS_HARNESS.md) — Universal harness contract, adapter protocol, and multi-harness setup.
- [**Installation & Setup**](docs/INSTALL.md) — Full installation guide, systemd units, Docker, and environment configuration.
- [**System Architecture**](ARCHITECTURE.md) — Detailed kernel design, Actions waist, ontology schemas, and event sourcing.
- [**Deployment Guide**](docs/DEPLOY.md) — Production operations, service daemon management, and envelope limits.
- [**CLI & MCP Reference**](docs/CLI.md) — Complete command-line tools and MCP tool surface documentation.
- [**Agent Ritual & Memory Laws**](docs/RITUAL.md) — Memory invariants, provenance rules, and context handoff practices.

---

## License & Responsible Use

Osiris is open-source under the MIT License. See [LICENSE](LICENSE) and [RESPONSIBLE_USE.md](RESPONSIBLE_USE.md) for ethical guidelines regarding keyless public entity federation.
