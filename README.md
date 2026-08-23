# Osiris 👁️

[![MCP Server](https://img.shields.io/badge/MCP-Streamable--HTTP%20%3A8790-blue.svg?style=flat-square&logo=anthropic)](https://modelcontextprotocol.io/)
[![DeepSeek Harness](https://img.shields.io/badge/DeepSeek_Harness-DSH-0066FF.svg?style=flat-square)](https://github.com/deepseek-ai)
[![Cordis Plugin](https://img.shields.io/badge/Cordis_Plugin-dsh--plugin-4E73DF.svg?style=flat-square)](dsh-plugin/)
[![Claude Code](https://img.shields.io/badge/Claude_Code-Native_Hooks-black.svg?style=flat-square)](https://docs.anthropic.com/claude/docs/claude-code)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16%2B_pg__trgm-336791.svg?style=flat-square&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Redis](https://img.shields.io/badge/Redis-7%2B_EventBus-DC382D.svg?style=flat-square&logo=redis&logoColor=white)](https://redis.io/)
[![Python](https://img.shields.io/badge/Python-3.12%2B_uv-3776AB.svg?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg?style=flat-square)](LICENSE)

> **The persistent memory and coordination graph for AI agents.**  
> *Palantir's Object Set ontology × Notion's structured databases × MCP for universal agent memory.*

Osiris is a self-hosted, harness-agnostic, provenance-first entity-graph engine. It turns transient agent reasoning, architectural rulings, loose ends, inter-agent postal communication, and public entity records into durable, queryable, event-sourced graph memory across context windows, compactions, and harness boundaries.

```
                  +--------------------------------------------------+
                  |                 AGENT HARNESSES                  |
                  |   Claude Code  *  DeepSeek (DSH)  *  Cursor/AI   |
                  +-------------------------+------------------------+
                                            | MCP (:8790) / HTTP
                  +-------------------------v------------------------+
                  |               OSIRIS MEMORY LAYER                |
                  |  Granular Retrieval  *  Palantir x Notion Algebra|
                  |  Evidence Taxonomy   *  Graphed Fleet Mailbox    |
                  |  Soul Store (Bytea)  *  Multi-Seat Coordination  |
                  +-------------------------+------------------------+
                                            | Actions Waist
                  +-------------------------v------------------------+
                  |               EVENT-SOURCED KERNEL               |
                  |   PostgreSQL 16 (Append-Only)  *  Redis 7 (Bus)  |
                  +--------------------------------------------------+
```

---

## 🏷️ Tags & Ecosystem

`memory` · `cordis` · `dsh` · `dsh-plugin` · `deepseek-harness` · `deepseek` · `claude-code` · `cursor` · `mcp` · `model-context-protocol` · `agentic-ai` · `ai` · `llm` · `knowledge-graph` · `provenance` · `palantir-ontology` · `notion-databases` · `event-sourcing` · `autonomous-agents` · `fleet-coordination`

---

## ⚡ Why Osiris?

| Dimension | Standard LLM Agent Contexts | Osiris Memory Graph |
| :--- | :--- | :--- |
| **Persistence** | Lost on session end, restart, or context compaction | Append-only, event-sourced graph in PostgreSQL 16 + Redis |
| **Harness Portability** | Trapped inside one proprietary harness format | Universal MCP server on `:8790` (DSH, Claude Code, Cursor, Crush) |
| **Retrieval Cost** | 50K–100K token prompt dumps that inflate bills | Granular getters (`get_status` ~360 chars, paginated lists, scoped search) |
| **Truth & Provenance** | Heuristic strings & hallucinated consensus | Strict 6-tier Evidence Taxonomy with within-source supersession |
| **Multi-Agent Teams** | Siloed sessions without message routing | Graphed postal layer (`Message` objects + typed `sent_by`/`in_thread` links) |
| **Transcript Archive** | Volatile local scratch files | Byte-exact Soul Store with deterministic SHA-256 hash chains |

---

## 🧬 Core Primitives

### 1. The Narrow Actions Waist
Every write into Osiris passes through the **Actions Waist** (`src/actions/core.py`). Direct database writes are forbidden. Every change produces an immutable `object_events` log entry, an `outbox` event for async consumers, and an `audit_log` entry.

```python
# Atomic mutation through the Actions Waist
async with actions.atomic():
    oid = await actions.create_or_find_object("SoftwareProject", "repo:osiris", actor="agent:thoth")
    await actions.assert_property(oid, "status", "active", source_id="agent:thoth", observed_at=now, confidence=1.0)
    await actions.create_link(from_id=decision_id, to_id=oid, type_="in_repo", source_id="agent:thoth", observed_at=now, confidence=1.0)
```

### 2. The Evidence Taxonomy
Confidence is never a freeform number. Every fact carries an explicit provenance class:

- **`SELF_DECLARED`** (1.00) — Direct first-party statement (e.g. an agent declaring its own architectural choice).
- **`AUTHORITATIVE_API`** (0.95) — Verified response from canonical APIs (EDGAR, CourtListener, OpenSanctions).
- **`DIRECT_OBSERVATION`** (0.90) — Observed runtime telemetry or execution transcript facts.
- **`CORROBORATED`** (0.85) — Multi-source agreement from independent observers.
- **`CO_OCCURRENCE`** (0.50) — Statistical co-occurrence or heuristic proximity.
- **`DERIVED`** (0.40) — Inferred background backfill (e.g. automated transcript mining).

### 3. Palantir × Notion Composition Engine
Compositions are reusable, forkable queries expressed as an 8-operator closed algebraic AST (`src/orchestrator/compositions.py`):
- **`select`**, **`traverse`**, **`collect`**, **`subtract`**, **`union`**, **`intersect`**, **`aggregate`**, **`order`**, **`take`**
- Function transforms: `search` (fused BM25 + trigram + semantic vectors), `discrepancy`, `who_is_this`.

---

## 🚀 Multi-Harness Integration

Osiris serves a universal **Model Context Protocol (MCP)** interface over Streamable-HTTP on `http://127.0.0.1:8790/mcp`.

### DeepSeek Harness (DSH)
Native zero-friction integration via the Cordis lifecycle plugin in `dsh-plugin/`:
- In-process `turn/start` auto-mounting and seat binding.
- Zero-subprocess background heartbeat status polling.
- In-flight `session/end-seed` graph settlement before context compaction.

```yaml
# Add to ~/.dsh/profiles/web/cordis.patch.yml
plugins:
  "@deepseek-ai/dsh-mcp-client":
    servers:
      osiris:
        type: streamable-http
        url: http://127.0.0.1:8790/mcp
```

### Claude Code
Unified, lightweight stdlib hook runner (`scripts/osiris_hook.py`) replacing 13 legacy scripts:
```json
// Add to ~/.claude/settings.json or .mcp.json
{
  "mcpServers": {
    "osiris": {
      "type": "streamable-http",
      "url": "http://127.0.0.1:8790/mcp"
    }
  }
}
```

### Cursor / Windsurf / Custom Agents
Connect any MCP-compatible agent directly to `http://127.0.0.1:8790/mcp`.

---

## 📬 Graphed Inter-Agent Postal Layer

Agents communicate through typed, asynchronous postal channels with at-least-once delivery:
- **Broadcast Channels**: `send(to="<project>")` — Group mail delivered to all agents mounted on a repo.
- **Direct Messages**: `send(to_agent="<agent_id>")` — Private agent-to-agent DMs.
- **Graph-Native Topology**: Every dispatch mints a first-class `Message` node in PostgreSQL with typed links:
  - `sent_by` -> `Agent`
  - `addressed_to` -> `Agent` / `Seat`
  - `broadcast_to` -> `SoftwareProject`
  - `replies_to` -> `Message`
  - `in_thread` -> `Thread`
- **Prior-Art Inspection**: Automatically queries the knowledge graph on dispatch to alert agents if an identical decision or practice already exists.

---

## 🔎 High-Efficiency Granular Retrieval

Stop spending 90% of your context window on briefings. Osiris provides tiered retrieval:

```
+-----------------------------------------------------------------------------+
|  Tier 1: Glance (~360 chars)                                                |
|  get_status() -> {you: "agent:dsh00001", project: "osiris", mail: "0 unread"}|
+-----------------------------------------------------------------------------+
|  Tier 2: Paginated Lists (~1-2KB)                                           |
|  get_thread_list(project="osiris", kind="obligation", limit=5)              |
|  get_decision_list(project="osiris", limit=5)                               |
|  get_mail()                                                                 |
+-----------------------------------------------------------------------------+
|  Tier 3: Scoped Graph Search (~2-5KB)                                       |
|  graph_search(query="advisory lock xact", project="osiris", max_depth=1)    |
|  consult_canon(query="actions waist")                                       |
+-----------------------------------------------------------------------------+
|  Tier 4: Full Briefing & Recall (On Demand)                                 |
|  orient() * recall(ref="c396b0a2") * dossier(object_ref="repo:osiris")      |
+-----------------------------------------------------------------------------+
```

---

## 🛠️ Quickstart

### 1. Prerequisites
- Python 3.12+ (`uv` package manager)
- PostgreSQL 16+ with `pg_trgm` and `vector` extensions
- Redis 7+

### 2. Installation & Database Setup
```bash
# Clone the repository
git clone https://github.com/asuramaya/osiris.git
cd osiris

# Install Python dependencies
uv sync

# Run database migrations
DATABASE_URL=postgresql://osiris:osiris@127.0.0.1:5432/osiris uv run alembic upgrade head

# Seed core ontology types and design canon
uv run python -m src.init
```

### 3. Launch the Services
```bash
# 1. Start the Osiris MCP Server (HTTP / JSON-RPC on port 8790)
DATABASE_URL=postgresql://osiris:osiris@127.0.0.1:5432/osiris \
REDIS_URL=redis://127.0.0.1:6379/0 \
uv run python -m src.mcp_server

# 2. (Optional) Start the Osiris UI & Console on port 8011
DATABASE_URL=postgresql://osiris:osiris@127.0.0.1:5432/osiris \
uv run uvicorn src.api.app:app --port 8011

# 3. (Optional) Start Background Worker
DATABASE_URL=postgresql://osiris:osiris@127.0.0.1:5432/osiris \
uv run arq src.workers.arq_worker.WorkerSettings
```

---

## 🔄 The Agent Lifecycle Ritual

When an agent enters an Osiris workspace, it follows a structured 5-step ritual:

1. **`mount(cwd)`** — Link session into the fleet; bind to designated Seat and restore durable lineage identity.
2. **`get_status()`** — Glance at identity, unread mail, and fleet pulse in <400 characters.
3. **`graph_search(query)` / `consult_canon(query)`** — Check existing rulings and design canon before re-deriving solutions.
4. **`record_decision(...)` / `open_thread(...)`** — Persist architectural rulings, choices, and open obligations as they occur.
5. **`settle()`** — Mechanical end-of-turn seal verifying all decisions, threads, and git status are durable in the graph before compaction.

---

## 📚 Documentation Index

- 📖 [**Cross-Harness Specification**](docs/CROSS_HARNESS.md) — Multi-harness architecture, DSH Cordis plugin, and adapter protocols.
- 🏗️ [**System Architecture**](ARCHITECTURE.md) — Deep dive into the Actions Waist, event-sourcing kernel, and schema catalog.
- ⚙️ [**Installation & Operations**](docs/INSTALL.md) — Systemd units, Docker Compose, Redis configuration, and environment setup.
- 🚢 [**Deployment Guide**](docs/DEPLOY.md) — Production operations, daemon management, pool caps, and envelope limits.
- 📜 [**Agent Ritual & Memory Laws**](docs/RITUAL.md) — Constitutional memory invariants, evidence rules, and context handoff mechanics.
- 🗺️ [**Project Roadmap**](ROADMAP.md) — Milestones, completed arcs, and active development roadmap.

---

## ⚖️ Constitution & Invariants

1. **Never auto-merge Person** — Identity merges are review-gated, always.
2. **Osiris HAS HANDS, admitted and governed** — Lifecycle daemons own operational facts, never thought.
3. **Event-sourced, append-only kernel** — Objects, assertions, and links are never destructively deleted; heal with compensating events.
4. **Actions Waist invariant** — All graph mutations flow through `src/actions/core.py`.
5. **Evidence-graded ingest** — `SELF_DECLARED` > `AUTHORITATIVE_API` > `DIRECT_OBSERVATION` > `DERIVED`.
6. **The Membrane** — The autonomous loop may close, but never silently and never irreversibly.
7. **Keyless public collection** — Open entity commons for corporate/public data without private credential leakage.
8. **Build publicly** — Clean milestones, verifiable test suites, and strict secret/PII scanning before release.

---
