/**
 * Osiris bridge — harness-native fleet memory mounting for Osiris agents.
 *
 * Every session on a host running the Osiris MCP server gets the same treatment
 * Claude Code gets from its SessionStart hook, without a python script: at
 * `agent/session-start` the bridge posts the session's own durable anchor to
 * Osiris `/automount` (mounting it through the tested path), then binds the
 * shared MCP connection by driving the real `mcp__osiris__mount` tool once,
 * and injects the rendered whisper (name, project, mail, obligations) into the
 * session's opening context. At `agent/disposed` the bridge posts
 * `/session-end` so the mount row releases the instant the session ends.
 *
 * The anchor is the session's on-disk directory (`~/.dsh/sessions/<slug>/
 * session-<uuid>`), resolved through the configured session-persistence backend
 * (`locate`) — the same authoritative source the Claude Code hook bridge uses
 * for `transcript_path` — so a non-default sessions root still produces the
 * path Osiris's DSH adapter recognizes.
 *
 * @module @deepseek-ai/dsh-experimental-osiris-bridge
 */

import { dirname } from 'node:path'
import type { Context } from '@deepseek-ai/cordis'
import z from '@deepseek-ai/schemastery'
import type { Agent, SessionStartSource } from '@deepseek-ai/dsh-agent'
import { CallId } from '@deepseek-ai/dsh-llm/brand'
import { createUserMessage } from '@deepseek-ai/dsh-llm'
// Side-effect type imports: declaration-merge ctx.tools and ctx.sessionPersistence.
import type {} from '@deepseek-ai/dsh-tools'
import type {} from '@deepseek-ai/dsh-session-persistence'

/** Cordis plugin name used by loader diagnostics and injected-context sources. */
export const name = 'osiris-bridge'

/** Services required by the bridge: the tool registry it binds through. */
export const inject = ['tools']

/** Default Osiris MCP server base URL (the osiris-mcp.service loopback listener). */
export const DEFAULT_BASE_URL = 'http://127.0.0.1:8790'

/** Default server name the dsh-mcp-client entry registers Osiris tools under. */
export const DEFAULT_SERVER_NAME = 'osiris'

/** Default per-request budget for the automount/session-end HTTP calls. */
export const DEFAULT_TIMEOUT_MS = 3_000

/** Bridge configuration. */
export interface Config {
  /** Osiris MCP server base URL (its plain-HTTP hook listener). */
  baseUrl: string
  /** The dsh-mcp-client `serverName` whose `mount` tool binds the connection. */
  serverName: string
  /**
   * Bind the shared MCP connection to depth-0 sessions by calling the Osiris
   * `mount` tool once at session start. Disable when the Osiris server entry
   * uses a different transport or the tools are not loaded.
   */
  bindConnection: boolean
  /** Per-request budget in milliseconds for the Osiris HTTP calls. */
  timeoutMs: number
}

/** Schemastery validation for {@link Config}. */
export const Config: z<Config> = z.object({
  baseUrl: z.string().default(DEFAULT_BASE_URL),
  serverName: z.string().default(DEFAULT_SERVER_NAME),
  bindConnection: z.boolean().default(true),
  timeoutMs: z.number().default(DEFAULT_TIMEOUT_MS),
})

/** The subset of the /automount payload the bridge reads. */
interface AutomountResult {
  agent?: string
  project?: string
  job_dir?: string
  whisper_text?: string | null
  error?: string
}

/** Bind outcome for one depth-0 session start. */
export interface BindOutcome {
  /** Whether the Osiris mount tool accepted the bind. */
  ok: boolean
  /** Why the bind did not happen, for logs and the injected fallback note. */
  reason?: string
}

/** Session facts the bridge derives from the header. */
export interface SessionAnchor {
  /** The session's id (`session-<uuid>`). */
  readonly id: string
  /** The session's working directory (header cwd, else process cwd). */
  readonly cwd: string
  /** The durable session directory under the sessions root, when resolvable. */
  readonly jobDir: string | undefined
  /** The session's delegation depth; 0 is a top-level session. */
  readonly depth: number
}

/**
 * Resolve one session's durable anchor directory from the persistence backend.
 *
 * The jsonl backend owns one directory per session; `locate` returns its log
 * path without materializing anything, so `dirname` is the anchor Osiris keys
 * mount rows on. A backend without per-session artifacts (SQLite) leaves the
 * anchor undefined and the greet degrades to a job_dir-less automount.
 * @param ctx - context carrying an optional `sessionPersistence` service.
 * @param header - the session's immutable creation metadata.
 * @returns the session's id, cwd, delegation depth, and anchor directory (when resolvable).
 */
export function resolveAnchor(ctx: Context, header: Agent['session']['header']): SessionAnchor {
  const located = ctx.get('sessionPersistence')?.locate(header)
  const jobDir = located !== undefined && located.kind === 'jsonl'
    ? dirname(located.path)
    : undefined
  return {
    id: header.id,
    cwd: header.cwd ?? process.cwd(),
    jobDir,
    depth: header.delegationDepth ?? 0,
  }
}

/** POST one JSON body and decode a JSON response, or undefined on any failure. */
async function postJson(
  url: string, body: Record<string, unknown>, timeoutMs: number,
): Promise<AutomountResult | undefined> {
  try {
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(timeoutMs),
    })
    if (!response.ok) return undefined
    return await response.json() as AutomountResult
  } catch {
    return undefined
  }
}

/**
 * Bind the shared MCP connection to one session by driving the real Osiris
 * `mount` tool through the tool registry. The call rides the same MCP
 * connection the agent's own tool calls use, so Osiris associates that
 * connection with the session's identity and later calls re-attach without a
 * header (the X-Osiris-Job header lane is static per transport in the MCP SDK).
 * @param ctx - context carrying the tool registry to call through.
 * @param config - server URL, tool name, and per-request budget.
 * @param anchor - the session's resolved anchor (its `jobDir` names the mount row).
 * @returns whether the bind succeeded, with a reason when it did not.
 */
export async function bindConnection(
  ctx: Context, config: Config, anchor: SessionAnchor,
): Promise<BindOutcome> {
  if (anchor.jobDir === undefined) {
    return { ok: false, reason: 'no durable session directory to bind' }
  }
  const toolName = `mcp__${config.serverName}__mount`
  if (ctx.tools.get(toolName) === undefined) {
    return { ok: false, reason: `tool ${toolName} is not registered (server name mismatch or MCP server not connected)` }
  }
  const result = await ctx.tools.execute({
    callId: CallId(`osiris-bridge:${anchor.id}`),
    name: toolName,
    arguments: { cwd: anchor.cwd, job_dir: anchor.jobDir },
    signal: AbortSignal.timeout(config.timeoutMs),
  })
  if (result.isError) {
    return { ok: false, reason: 'the mount tool call failed' }
  }
  const value = (result as { value?: unknown }).value
  if (value !== null && typeof value === 'object' && 'error' in value
    && typeof (value as { error?: unknown }).error === 'string') {
    return { ok: false, reason: `mount refused: ${(value as { error: string }).error}` }
  }
  return { ok: true }
}

/** The note a subagent session gets: registered, but riding the parent's connection. */
function subagentNote(out: AutomountResult): string {
  const who = out.agent ?? 'an unregistered session'
  const project = out.project !== undefined ? ` (project ${out.project})` : ''
  return `◈ OSIRIS — the fleet's shared memory. This subagent session is registered as ${who}${project}. ` +
    'Its osiris tool calls ride the parent session\'s MCP connection and are attributed to the parent; ' +
    'record durable findings through the parent, or have the parent write them. orient() for bearings.'
}

/** The fallback note when the server rendered no whisper or the bind failed. */
function fallbackNote(out: AutomountResult, anchor: SessionAnchor, why: string): string {
  const who = out.agent ?? 'an unregistered session'
  const mountHint = anchor.jobDir !== undefined
    ? `mount(cwd='${anchor.cwd}', job_dir='${anchor.jobDir}')`
    : `mount(cwd='${anchor.cwd}')`
  return `◈ OSIRIS — the fleet's shared memory. It knows this session as ${who}, but ` +
    `the connection was not bound (${why}). The first osiris call may bounce 'mount first'; ` +
    `re-mount with ${mountHint} and you are whole.`
}

/** Inject one plugin-sourced snapshot context message into the session. */
function injectWhisper(agent: Agent, text: string): void {
  agent.inject(createUserMessage({
    content: [{ type: 'text', text }],
    source: { kind: 'plugin', plugin: name, form: 'snapshot', sections: [{ name, text }] },
  }))
}

/**
 * Register the Osiris bridge for the lifetime of `ctx`.
 *
 * Fail-open by design, mirroring Osiris's own hook contract: an unreachable
 * server, a failed bind, or a missing persistence backend degrades to an
 * honest injected note and never blocks or slows a session start.
 *
 * @param ctx - plugin context; listeners are disposed with it.
 * @param config - server URL, tool name, and bind behavior.
 */
export function apply(ctx: Context, config: Config): void {
  /** Session ids this bridge greeted, mapped to the anchor it mounted under. */
  const greeted = new Map<string, string | undefined>()

  const greet = async (agent: Agent, source: SessionStartSource): Promise<void> => {
    const anchor = resolveAnchor(ctx, agent.session.header)
    greeted.set(anchor.id, anchor.jobDir)
    const body: Record<string, unknown> = {
      session_id: anchor.id,
      cwd: anchor.cwd,
      source,
    }
    if (anchor.jobDir !== undefined) body.job_dir = anchor.jobDir
    // The honesty gate's testimony: the connection is bound (below) exactly when
    // env_job names the anchor, so the rendered "ALREADY MOUNTED" claim stays true.
    const bindEligible = config.bindConnection && anchor.depth === 0 && anchor.jobDir !== undefined
    if (bindEligible) {
      body.render = true
      body.env_job = anchor.jobDir
    }
    const out = await postJson(`${config.baseUrl}/automount`, body, config.timeoutMs)
    if (out === undefined) {
      ctx.logger.warn(
        `osiris-bridge: automount unreachable for ${anchor.id} (${config.baseUrl})`)
      injectWhisper(agent,
        '◈ OSIRIS (fleet memory) is configured but its server is unreachable right now. '
        + `When your work touches shared knowledge, try ${anchor.jobDir !== undefined ? `mount(cwd='${anchor.cwd}', job_dir='${anchor.jobDir}')` : `mount(cwd='${anchor.cwd}')`} — it may be back.`)
      return
    }
    if (out.error !== undefined) {
      ctx.logger.warn(`osiris-bridge: automount failed for ${anchor.id}: ${out.error}`)
      injectWhisper(agent,
        `◈ OSIRIS available — automount failed (${out.error}); call `
        + `mount(cwd='${anchor.cwd}') by hand, then orient().`)
      return
    }
    if (anchor.depth > 0) {
      injectWhisper(agent, subagentNote(out))
      return
    }
    if (!bindEligible) {
      // No bind intended: the server-rendered unanchored whisper (or a short
      // note) is honest — the connection really does need a first mount().
      const text = typeof out.whisper_text === 'string' && out.whisper_text.length > 0
        ? out.whisper_text
        : fallbackNote(out, anchor, 'connection binding disabled')
      injectWhisper(agent, text)
      return
    }
    const bind = await bindConnection(ctx, config, anchor)
    if (bind.ok) {
      const text = typeof out.whisper_text === 'string' && out.whisper_text.length > 0
        ? out.whisper_text
        : fallbackNote(out, anchor, 'the server rendered no whisper text')
      injectWhisper(agent, text)
    } else {
      ctx.logger.warn(`osiris-bridge: ${bind.reason}`)
      injectWhisper(agent, fallbackNote(out, anchor, bind.reason ?? 'unknown'))
    }
  }

  const farewell = async (agent: Agent): Promise<void> => {
    const id = agent.session.header.id
    const jobDir = greeted.get(id)
    greeted.delete(id)
    if (jobDir === undefined) return  // never greeted under an anchor: nothing to end
    await postJson(
      `${config.baseUrl}/session-end`,
      { session_id: id, job_dir: jobDir },
      config.timeoutMs,
    )
  }

  ctx.on('agent/session-start', ({ agent, source }) => {
    void greet(agent, source).catch((error: unknown) => {
      ctx.logger.warn(`osiris-bridge: greet failed: ${String(error)}`)
    })
  })
  ctx.on('agent/disposed', ({ agent }) => {
    void farewell(agent).catch((error: unknown) => {
      ctx.logger.warn(`osiris-bridge: farewell failed: ${String(error)}`)
    })
  })
}
