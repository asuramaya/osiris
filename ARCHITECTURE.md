# System Architecture

Osiris is an event-sourced, provenance-first entity graph engine designed to provide persistent memory, knowledge retrieval, and structured coordination for AI agents across diverse harnesses.

---

## Core Architectural Layers

```
                     ┌─────────────────────────────────────────┐
                     │            AGENT HARNESSES              │
                     │  DeepSeek Harness · Claude Code · Custom│
                     └────────────────────┬────────────────────┘
                                          │
                     ┌────────────────────▼────────────────────┐
                     │          HARNESS ADAPTER LAYER          │
                     │  HarnessAdapter Protocol · Zstd/JSONL   │
                     │  Model Swap Detection · Session Anchors │
                     └────────────────────┬────────────────────┘
                                          │
                     ┌────────────────────▼────────────────────┐
                     │           SURFACES & RETRIEVAL          │
                     │  FastMCP (:8790) · FastAPI Console (:8011)│
                     │  Granular Tools · Graph-Aware Search    │
                     └────────────────────┬────────────────────┘
                                          │
                     ┌────────────────────▼────────────────────┐
                     │         NARROW WAIST (Actions API)      │
                     │  create_or_find_object · assert_property│
                     │  create_link · merge_objects· set_status│
                     └────────────────────┬────────────────────┘
                                          │
                     ┌────────────────────▼────────────────────┐
                     │         EVENT-SOURCED GRAPH KERNEL      │
                     │  Objects · Links · Assertions · Events  │
                     │  PostgreSQL 16 (pg_trgm/vector) · Redis │
                     └─────────────────────────────────────────┘
```

---

## 1. The Narrow Waist (Actions API)

All graph writes, whether initiated by MCP tools, background workers, or ingest drivers, pass exclusively through the **Actions Waist** (`src/actions/core.py`).

The Waist guarantees:
- **Idempotency**: Objects and assertions are deduplicated on natural keys and canonical schemes.
- **Append-Only Immutability**: No rows are deleted. Status updates and merges are recorded as typed events in `object_events` and projected forward.
- **Provenance Grading**: Every assertion and link records an `evidence_class`:
  - `SELF_DECLARED`: Direct testimony from an agent or developer.
  - `AUTHORITATIVE_API`: Direct from an authoritative registry (EDGAR, GLEIF, etc.).
  - `DIRECT_OBSERVATION`: Observed system fact (git commits, file trees, process state).
  - `CO_OCCURRENCE`: Extracted association from co-occurring entities.
  - `DERIVED`: Machine-inferred or heuristically extracted fact.
  - `CORROBORATED`: Dynamically computed when multiple independent sources agree.
- **Atomic Outbox**: Mutations emit events into an outbox for downstream pulse processing.

---

## 2. The Ontology Schema

The schema (`src/ontology/schema.py`) defines the graph vocabulary:

### Core Entity Types
- **`SoftwareProject`** (`repo:<name>`): A tracked codebase or repository.
- **`Commit`** (`commit:<hash>`): A git commit in a project's timeline.
- **`Decision`** (`decision:<uuid>`): A recorded architectural choice, ruling, or rejection.
- **`Thread`** (`thread:<uuid>`): An open duty, question, obligation, or task.
- **`Agent`** (`agent:<id>`): An individual runtime session identity.
- **`Seat`** (`seat:<handle>`): A persistent role/seat (e.g., "Thoth", "Sekhmet") held by a succession of agents.
- **`Message`** (`message:<uuid>`): A first-class inter-agent communication artifact.
- **`Reference`** (`ref:<slug>`): An ingested specification, paper, or design canon essay.
- **`File`** (`file:<path>`): A tracked file node in a project's tree.

### Public Record Entities
- **`Organization`**, **`Person`**, **`CryptoAddress`**, **`CourtCase`**, **`ClinicalTrial`**.

### Core Graph Link Types
- **Succession & Identity**: `succeeded_from`, `holds`, `managed_by`, `peer_of`.
- **Project Affiliation**: `works_in`, `governs`, `in_repo`.
- **Reasoning & Proof**: `grounded_by`, `witnesses`, `supersedes`, `answers`, `resolved_by`.
- **Inter-Agent Messaging**: `sent_by`, `addressed_to`, `broadcast_to`, `replies_to`, `in_thread`, `mentions`.

---

## 3. Harness Adapter Layer

The Harness Adapter layer (`src/ingest/harness/`) provides a universal interface to diverse agent substrates:

```python
class HarnessAdapter(Protocol):
    name: str
    def discover(self, root: Path, job_dir: str | None = None) -> SessionLocator | None: ...
    def read_turns(self, locator: SessionLocator) -> Iterator[TurnRow]: ...
    def enumerate(self, root: Path) -> Iterator[SessionLocator]: ...
```

- **`ClaudeCodeSessionAdapter`**: Parses JSONL transcripts from `~/.claude/projects/` and anchors via `~/.claude/jobs/<id>`.
- **`DshSessionAdapter`**: Parses zstd-compressed JSONL event streams from `~/.dsh/sessions/` and extracts model headers, tool invocations, and boundaries.
- **Custom Adapters**: New agent frameworks implement `HarnessAdapter` to integrate into the transcript store and soul store.

---

## 4. Granular Retrieval Surface

To prevent context-window exhaustion from massive whole-graph dumps, Osiris provides granular, high-signal retrieval tools:

1. **`get_status()`**: Ultra-compact identity and mailbox summary (~360 chars).
2. **`graph_search(query, project?, lineage?, max_depth?)`**: Hybrid full-text (Postgres `tsvector` + `pg_trgm`) and semantic vector retrieval with configurable 1–3 hop graph traversal.
3. **`get_decision_list(project, limit?, offset?)`**: Paginated decision history with rationale and supersession state.
4. **`get_thread_list(project, kind?, owner?, limit?, offset?)`**: Paginated active obligations and open tasks.
5. **`consult_canon(query)`**: Keyword and semantic search over ingested architecture essays and design reference nodes.

---

## 5. First-Class Graphed Messaging

Agent communication is modeled directly inside the entity graph:
- When an agent calls `send(to=...)` or `send(to_agent=...)`:
  1. The message is inserted into `fleet_messages` for delivery leasing and queue dispatch.
  2. A `Message` entity is minted in `objects`.
  3. Graph links (`sent_by`, `addressed_to`, `broadcast_to`, `replies_to`, `in_thread`) are established.
  4. Mentions of decisions, threads, and agents are automatically wired via `mentions` links.
- Prior-art detection runs at write-time to notify senders if an ask or ruling duplicates existing graph knowledge.

---

## 6. Lifecycle Hook Architecture

Osiris supports two complementary lifecycle execution models:

1. **In-Process DSH Cordis Plugin** (`dsh-plugin/`):
   - Runs natively inside DeepSeek Harness.
   - Automatically handles session mounting, periodic status checks, and pre-compaction settlement without spawning external processes.

2. **Unified CLI Hook** (`scripts/osiris_hook.py`):
   - A single, fast standard-library Python CLI replacing legacy multi-process scripts.
   - Executes 7 core subcommands (`statusline`, `stop`, `whisper`, `session-end`, `precompact`, `spawn`, `anchor`).
   - Dispatches lightweight HTTP requests to the running FastMCP daemon (~30ms response).
