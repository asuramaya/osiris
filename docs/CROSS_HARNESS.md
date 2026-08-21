# Cross-Harness Operation

Osiris is an "MCP for memory" graph: a harness-agnostic persistence layer that any
MCP-compatible agent can read from and write to. This document covers the contract
between Osiris and the agent harness (Claude Code, DSH, Crush, Cursor, etc.).

## Architecture

```
Agent (any MCP client) ←→ [MCP streamable-http] ←→ Osiris MCP Server (:8790)
                                                       ↕
                                              Postgres (:5601) — event-sourced graph
                                              Redis (:6396) — queues / token buckets
```

The Osiris MCP server is a Python FastMCP process running on port 8790. It:
- Exposes ~130+ tools via MCP streamable HTTP transport
- Runs as a systemd user service (`osiris-mcp.service`)
- Shares ONE database pool for the whole fleet (bounded connections)
- Has NO persistent in-memory state (identity survives server bounces via `agent_mounts` table)

## Identity / Mount

To attribute writes to YOUR agent (instead of the anonymous `session` bucket):

```
mount(cwd="/path/to/project", job_dir="<durable-anchor-path>")
```

The `job_dir` is harness-specific:
- **Claude Code**: `~/.claude/jobs/<id>` (the CLAUDE_JOB_DIR)
- **DSH**: auto-detected from the workspace slug in `~/.dsh/sessions/<slug>/`
- **Crush**: auto-detected from `~/.local/share/crush/projects.json` or `<cwd>/.crush/`
- **Other harnesses**: pass any unique identifier; without one, mount still works but identity
  is ephemeral (lost on MCP server restart)

Mounting with a known anchor RE-ATTACHES you to your previous identity (agent id, seat,
lineage). Mounting for the first time MINTS a new agent.

**For Crush/Cursor/OpenCode**: any MCP client works — connect to `http://127.0.0.1:8790/mcp`
with standard streamable HTTP. Pass `job_dir` as your session identifier if available,
or omit it and mount with just `cwd`.

## Tool Surface (Phase 2 — Graphy Primitives)

Rather than relying on the `orient()` monolith (~59K chars), use the granular tools:

| Tool | Purpose | Size |
|---|---|---|
| `get_status()` | Identity + mail + pulse | ~360 chars |
| `get_thread_list(project, kind?, owner?, limit?, offset?)` | Paginated threads | bounded |
| `get_decision_list(project, limit?, offset?)` | Paginated decisions | bounded |
| `get_mail()` | Unread count + asks | ~150 chars |
| `graph_search(query, project?, lineage?, max_depth?, limit?)` | Graph-aware search | bounded |
| `search(query)` | Full-text search (any scope) | bounded |

## Graph-Aware Search

`graph_search` extends the standard `search` with:
- **`project`**: restrict results to objects linked to a SoftwareProject
- **`lineage`**: restrict to objects authored by a specific agent lineage
- **`max_depth`**: expand each hit with its N-hop neighborhood (linked objects)

The search engine runs four doors: strict FTS → OR-relaxation → trigram (typo tolerance)
→ semantic (local static embeddings, model2vec, no GPU, no API key). Results are fused
by reciprocal rank with grade × recency weighting.

## DSH Integration

The `cordis.patch.yml` overlay at `~/.dsh/profiles/web/cordis.patch.yml` connects DSH
to Osiris:

```yaml
- insert:
    - id: memory-osiris
      name: '@deepseek-ai/dsh-mcp-client'
      config:
        serverName: osiris
        transport: streamable-http
        url: 'http://127.0.0.1:8790/mcp'
        reconnect:
          enabled: true
          initialDelayMs: 300
          maxDelayMs: 5000
          maxAttempts: 30
```

Known limitation: the MCP SDK's `StreamableHTTPClientTransport` may cache stale session
IDs across server restarts. If tools become unavailable, touch the patch file (triggers HMR)
or restart the Host.

## Harness Adapter Interface

Osiris defines a `HarnessAdapter(Protocol)` for normalizing transcript formats:

```python
class HarnessAdapter(Protocol):
    name: str
    def discover(self, *, cwd, job_dir, root=None) -> SessionLocator | None: ...
    def read_turns(self, locator, *, since_idx=0) -> Iterator[TurnRow]: ...
    def enumerate(self, *, root=None) -> Iterator[SessionLocator]: ...
```

Implemented adapters:
- `ClaudeJsonlAdapter` — `~/.claude/projects/<slug>/<sid>.jsonl`
- `DshSessionAdapter` — `~/.dsh/sessions/<slug>/session-<uuid>.jsonl.zstd`
- `CrushSqliteAdapter` — `<data_dir>/crush.db`

To add a new harness: implement the protocol, register in `transcript_store.py`.

## Recording Decisions

The most important write tool for cross-harness continuity:

```
record_decision(
    summary="SHORT TITLE",
    kind="decision|ruling|choice",
    rationale="Full reasoning...",
    repo="project-name",
    resolves=["thread:<id>"],  # if this answers an open thread
    supersedes=["decision:<id>"],  # if this corrects a prior decision
)
```

This is how the graph accumulates institutional memory across sessions, agents, and harnesses.

## Universal Command Layer

Every slash command maps to a documented MCP tool. Harnesses without a slash command
system call the MCP tool directly; harnesses with one (Claude Code, DSH) register thin
wrappers.

| Claude Code Slash | MCP Tool | Purpose |
|---|---|---|
| /mail | get_mail() | Show unread inbox |
| /settle | settle() | Write back decisions/threads before leaving |
| /status | get_status() | Quick identity + mail glance |
| /threads | get_thread_list(project=...) | Show open threads for a project |
| /decisions | get_decision_list(project=...) | Show recent decisions |
| /graph | graph_search(query=..., project=...) | Search graph scoped to project |
| /search | search(query=...) | Full-text search (any scope) |
| /orient | orient(project=...) | Full briefing (monolith -- prefer get_status) |
| /recall | recall(ref=...) | Read full record of a thread/decision |
| /decide | record_decision(summary=..., rationale=...) | Record a ruling |
| /inbox | inbox() | Read fleet mailbox |
| /send | send(to=..., body=...) | Send fleet mail |
| /mount | mount(cwd=...) | Link identity to this session |

For DSH: all MCP tools are available directly as mcp__osiris__<tool>. No separate
slash command registration needed -- the tool IS the command.

## Lifecycle Plugin

The @deepseek-ai/dsh-osiris-lifecycle plugin replaces Claude Code's 13 subprocess
hooks with in-process event listeners:

| Lifecycle Event | Action | Replaces CC Hook |
|---|---|---|
| First turn (turn/start) | mount(cwd) | osiris_whisper.py |
| Session end (session/end-seed) | settle() | osiris_stophook.py |
| Turn boundary | get_status() | osiris_statusline.py (periodic) |
| Periodic poll (30s) | get_status() | osiris_statusline.py (chrome render) |
| Compaction | settle() via DSH auto | osiris_precompact.py |

## Process Architecture (Part 5)

After the lifecycle plugin is active, the following processes are redundant:

| Process | Replaced By | Status |
|---|---|---|
| osiris-pulse (heartbeat daemon) | MCP server's built-in /heartbeat route | Can be disabled; MCP server self-monitors |
| osiris-manager (PTY broker) | Harness-native --bg sessions (launch tool) | No-op; launch uses claude --bg directly |
| osiris_*.py scripts (13 files) | osiris_hook.py (unified) + lifecycle plugin | Deprecated; use osiris_hook.py <subcommand> |
