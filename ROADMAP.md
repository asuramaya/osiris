# Osiris Roadmap

This roadmap tracks the development of Osiris as a persistent, provenance-first entity graph engine and cross-harness memory substrate for AI agents.

---

## Architectural Pillars

1. **Persistent Memory Substrate**: Durable graph-backed state across context compactions, session boundaries, and agent generations.
2. **Harness Agnosticity**: Full interoperability across DeepSeek Harness (DSH), Claude Code, Cursor, Windsurf, OpenDevin, and custom agent runtimes via standard MCP and pluggable adapters.
3. **Provenance-First Evidence**: Confidence is a deterministic projection of source provenance, not an ungrounded hallucination.
4. **Graphed Multi-Agent Coordination**: Inter-agent communication modeled as first-class graph objects and typed relationships with prior-art detection.
5. **Granular, High-Signal Retrieval**: Fast, bounded retrieval primitives replacing context-exhausting whole-graph dumps.

---

## Completed Milestones

### ✅ Phase 1: Harness Agnosticity & DSH Ingest
- **Universal `HarnessAdapter` Protocol**: Standardized interface for discovering, parsing, and streaming turns across harness formats.
- **DSH Session Adapter**: Streaming parser for zstd-compressed JSONL event transcripts in `~/.dsh/sessions/`.
- **Harness-Agnostic Model & Swap Detection**: Observation-first model identity reading across diverse runtimes.

### ✅ Phase 2: Granular Retrieval Primitives
- **`get_status()`**: Ultra-lightweight glance (~360 chars) returning identity, unread mail counts, and fleet pulse.
- **`graph_search()`**: Scoped hybrid search (Postgres `tsvector` + `pg_trgm` + local embeddings) with configurable 1–3 hop graph traversal.
- **Paginated Decision & Thread Retrieval**: `get_decision_list()` and `get_thread_list()` with status and kind filters.
- **Design Canon & Reference Recall**: `consult_canon()` for on-demand retrieval of architectural essays and reference nodes.

### ✅ Phase 3: Graphed Multi-Agent Messaging
- **First-Class `Message` Objects**: Minted in the graph upon dispatch with `sent_by`, `addressed_to`, `broadcast_to`, `replies_to`, and `in_thread` links.
- **Prior-Art Write Guard**: Automatic search-based verification when sending asks or rulings to prevent redundant re-derivation.
- **At-Least-Once Delivery & Leasing**: Structured mail queueing with timeout-based recovery.

### ✅ Phase 4: Lifecycle Hook Consolidation & DSH Plugin
- **In-Process DSH Cordis Plugin**: `dsh-plugin` handling auto-mounting, periodic status monitoring, and pre-compaction settlement.
- **Consolidated CLI Hook (`scripts/osiris_hook.py`)**: Collapsed 13 legacy subprocess scripts into a single, high-performance stdlib CLI (~30ms response over HTTP).

---

## Active & Upcoming Horizons

### 🔄 Multi-Lineage Synthesis & Reflection
- **Automated Living Synthesis**: Background workers synthesizing high-level project trajectory digests from raw commit and decision streams.
- **DeepSeek V4 Optimization**: Tailored retrieval and reasoning structures taking advantage of deep context and MoE architectures.
- **Cross-Lineage Conflict Resolution**: Structured tooling for detecting and reconciling conflicting rulings across concurrent agent lanes.

### 🔭 Distributed Graph Federation
- **Multi-Node Synchronization**: Replicating entity subgraphs across edge nodes and remote developer environments.
- **Autonomous Review Gates**: Automated peer-review workflows for merges and high-impact property assertions.
