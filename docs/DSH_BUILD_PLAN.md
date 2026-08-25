# DSH Build Plan: Cross-Harness Osiris

## Diagnosis

1. **Mail is not graphed.** `fleet_messages` is a relational side-table with `reply_to`/
   `thread_id` FKs but ZERO graph edges. Mail can't be traversed, traced, or searched with
   `graph_search()`. This is the "graphed inter-agent communication" gap.

2. **CC hook architecture forks a python subprocess per event.** 13 `osiris_*.py` scripts
   each do their own cold `asyncpg.connect()` on every render/event. Claude measured
   ~138 tx/s and 23 PG backends for 16 sessions. The DSH equivalent must be in-process.

3. **Slash commands are CC-only wrappers disguised as features.** They exist because CC
   doesn't surface MCP tools in its command palette. Any functioning MCP client doesn't
   need them as slash commands.

## Part 1: Graphed Message/Postal Dispatch

- New object type `Message` (or `message` category on Thread).
- New link types in `src/ontology/schema.py`:
  - `sent_by` (Message → Agent)
  - `addressed_to` (Message → Agent)
  - `broadcast_to` (Message → SoftwareProject)
  - `replies_to` (Message → Message)
  - `in_thread` (Message → Thread)
  - `mentions` (Message → Agent/Decision/Thread) — the killer feature
- `send_message` + `read_inbox` write/read graph edges alongside the relational table.
- `graph_search` now surfaces messages with their connected context.

## Part 2: CC Hook Collapse (thin hooks + server-side logic)

- Collapse 13 `osiris_*.py` into ONE `osiris_hook.py` with subcommands.
- Each hook becomes a ~5-line stdlib-only shim: urllib POST + exit.
- Kill cold `asyncpg.connect()` fallbacks — HTTP-first only; degraded line on failure.
- Server-side custom routes (`/heartbeat`, `/stop`, `/automount`, etc.) become the ONLY
  implementation; the hooks are now pure transport.

## Part 3: osiris-bridge (in-process lifecycle plugin) — SHIPPED

Delivered as `@deepseek-ai/dsh-experimental-osiris-bridge`, NOT as the
`dsh-osiris-lifecycle` this plan named. Source of truth: `dsh-plugin/osiris-bridge/`;
install with `dsh-plugin/install.sh` (task #194).

- Cordis plugin on `agent/session-start` and `agent/disposed`.
- Reaches Osiris over the HTTP doors (`/automount`, `/session-end`) and binds the
  shared MCP connection by executing `mcp__osiris__mount` once through `ctx.tools`.
  The plan's `ctx.mcp` service was a guess; it does not exist as assumed.
- Injects the server-rendered whisper as a plugin-sourced snapshot message.
- Every leg fails OPEN — a cold server costs a note, never a session.
- NOT delivered: compaction-succession (a DSH compacted session gets a new id) and
  a `settle()` leg on `agent/turn-stopping`. Both still open.

## Part 4: Universal Command Layer

- "Every command is a documented MCP tool; each harness registers a thin wrapper."
- Mapping table lives in docs/CROSS_HARNESS.md.
- DSH: commands call MCP tools directly (already available).

## Part 5: Retire redundant processes

- Fold osiris-pulse heartbeat into the MCP server or make optional.
- PTY broker (osiris-manager) becomes no-op with harness-native background sessions.
