# @deepseek-ai/dsh-experimental-osiris-bridge

English | [中文](README.zh.md)

Harness-native bridge to an [Osiris](https://github.com/asuramaya/osiris) fleet-memory server. Every session on a host that also runs the Osiris MCP server (via `@deepseek-ai/dsh-mcp-client`) is mounted into the fleet at `agent/session-start` and released at `agent/disposed` — the same contract Claude Code gets from Osiris's SessionStart/SessionEnd hook scripts, delivered as a native Cordis plugin with no python dependency.

## What it does

1. **Automount.** On `agent/session-start` the bridge posts `{session_id, cwd, job_dir, source}` to the Osiris server's `/automount` route. The `job_dir` is the session's own durable directory (`<sessions-root>/<project-slug>/session-<uuid>`), resolved through the configured session-persistence backend (`ctx.sessionPersistence.locate`), so a non-default sessions root still produces the anchor Osiris's DSH adapter recognizes.
2. **Connection bind.** The MCP SDK's streamable-http transport carries static headers only, so Osiris's per-request `X-Osiris-Job` re-attach lane is unavailable. Instead the bridge drives the real `mcp__<serverName>__mount` tool once through the tool registry: the call rides the same MCP connection the agent's own tool calls use, and Osiris caches the session's identity against that connection — later osiris tool calls re-attach without any header.
3. **Whisper injection.** The bridge asks the server to render the whisper paragraph (`render: true`; ONE renderer lives in Osiris, no TypeScript twin) and injects it as plugin-sourced snapshot context, so the session starts knowing its agent id, project, model, mail, obligations, and durable anchor.
4. **Session end.** On `agent/disposed` the bridge posts `{session_id, job_dir}` to `/session-end`, releasing the mount row the moment the session ends instead of waiting for liveness decay.

Subagent sessions (delegation depth > 0) are automounted too (durable row, fleet visibility) but never bind the shared connection and receive an honest note instead of the whisper: their osiris calls ride the parent's MCP connection.

## Config

```yaml
- osiris-bridge:
    baseUrl: http://127.0.0.1:8790   # the Osiris MCP server's plain-HTTP hook listener
    serverName: osiris               # the dsh-mcp-client serverName prefixing its tools
    bindConnection: true             # bind depth-0 sessions by calling the mount tool
    timeoutMs: 3000                  # per-request budget for the Osiris HTTP calls
```

Requires a session-persistence backend with per-session artifacts (the jsonl backend). A backend without them (SQLite) leaves the anchor unresolved and the bridge no-ops with a warning. Fail-open by design, mirroring Osiris's own hook contract: an unreachable server, a failed bind, or a missing tool degrades to an honest injected note and never blocks a session start.

## Model Experience

### Osiris whisper at session start

#### What the model sees

One plugin-sourced user message whose text is the Osiris server's rendered whisper: the session's fleet identity (`agent:<id>`, project, observed model), unread mail and obligations, and the durable anchor to re-mount with after a connection bounce. The exact text is owned by Osiris (`scripts/osiris_whisper.py`) and is data-dependent on the fleet graph; the bridge only transports it.

#### Token effect

One context message per session start (plus one fallback note per failed bind). Content is graph-dependent and grows with the project's obligations summary, capped by Osiris's whisper renderer.

#### KV Cache effect

The whisper appends after the session's reusable prefix at the first request that claims it; it does not replace earlier tokens. Later requests re-send it as ordinary history until compaction.

## Known Limitations and Deferred Work

- **One connection, one identity** — all agents in one host process share the dsh-mcp-client's single MCP connection, so Osiris attributes osiris tool calls to whichever depth-0 session last bound the connection. Concurrent depth-0 sessions in one process interleave attribution; per-agent MCP connections would require product changes in `dsh-mcp-client`.
- **Subagents inherit the parent's attribution** — delegation depth > 0 sessions never bind (binding would steal the parent's cached identity) and their osiris calls are attributed to the parent.
- **No reconnect re-bind** — after the MCP server restarts and the client reconnects (new connection key), the bind is cold again; the next osiris call bounces "mount first" and the whisper's durable-anchor sentence is the recovery path. Detecting reconnects and re-binding is deferred.
- **No compaction succession** — Osiris's compaction mint keys on a stable session id; a DSH compacted session carries a new id, so the successor-mint flow does not fire from this bridge yet.
- **Stop/offload ritual not bridged** — Osiris's Stop-hook deliverable check (`/stop`) is not wired to `agent/turn-stopping`; the settle ritual remains the agent's own habit.
