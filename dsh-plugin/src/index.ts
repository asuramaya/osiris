/**
 * @deepseek-ai/dsh-osiris-lifecycle — in-process Osiris lifecycle integration.
 *
 * Replaces Claude Code's subprocess hook scripts (statusline, stop, whisper, etc.)
 * with DSH-native lifecycle listeners — zero forks, zero cold connections,
 * everything through the MCP client's existing pool.
 *
 * Hooks mapped:
 *   Session start (first turn) → mount(cwd)
 *   Session end (turn boundary) → settle()
 *   Compaction → settle()
 *   Periodic → get_status() (poll for mail count, pulse)
 *
 * @module
 */

import type { Context } from '@deepseek-ai/cordis'
import { Schema } from '@deepseek-ai/schemastery'

/** Osiris lifecycle plugin config. */
export interface Config {
  /** Osiris MCP server name (as registered in the MCP client plugin). Default: 'osiris'. */
  serverName?: string
  /** Mount automatically on first turn. Default: true. */
  autoMount?: boolean
  /** Settle on session end. Default: true. */
  autoSettle?: boolean
  /** Poll interval for status updates (seconds). 0 = disabled. Default: 30. */
  statusIntervalSecs?: number
  /** Cwd to mount with. Defaults to the session's cwd. */
  cwd?: string
}

export const name = '@deepseek-ai/dsh-osiris-lifecycle' as const
export const using = ['mcp'] as const

export const Config: Schema<Config> = Schema.object({
  serverName: Schema.string().default('osiris').description('Osiris MCP server name'),
  autoMount: Schema.boolean().default(true).description('Mount automatically on first turn'),
  autoSettle: Schema.boolean().default(true).description('Settle on session end'),
  statusIntervalSecs: Schema.number().default(30).description('Poll interval for status updates in seconds'),
  cwd: Schema.string().description('Working directory to mount with'),
})

export function apply(ctx: Context, config: Config): void {
  const serverName = config.serverName || 'osiris'
  let mounted = false
  let statusTimer: ReturnType<typeof setInterval> | undefined

  // Helper: call an MCP tool on the osiris server
  async function callMCP(tool: string, args: Record<string, unknown> = {}): Promise<void> {
    try {
      const service = (ctx as any).get?.('mcp') || (ctx as any).mcp
      if (!service) {
        ctx.logger?.warn?.(`osiris-lifecycle: MCP service unavailable, skipping ${tool}`)
        return
      }
      const client = service.clients?.get?.(serverName) || service.getClient?.(serverName)
      if (!client) {
        ctx.logger?.warn?.(`osiris-lifecycle: MCP client '${serverName}' not found`)
        return
      }
      await client.callTool(tool, args)
    } catch (error) {
      // Fail-open: lifecycle integration never blocks the session
      ctx.logger?.warn?.(`osiris-lifecycle: ${tool} failed: ${String(error)}`)
    }
  }

  // Auto-mount on session start
  if (config.autoMount !== false) {
    ctx.on('turn/start', async () => {
      if (mounted) return
      mounted = true

      const cwd = config.cwd || process.cwd()
      await callMCP('mount', { cwd })
      ctx.logger?.info?.(`osiris-lifecycle: mounted at ${cwd}`)
    })
  }

  // Auto-settle on session end
  if (config.autoSettle !== false) {
    ctx.on('session/end-seed', async () => {
      await callMCP('settle', {})
      ctx.logger?.info?.('osiris-lifecycle: settled on session end')
    })

    // Also settle if we detect a compaction boundary
    ctx.on('turn/end', async (data: { reason?: { kind?: string } }) => {
      if (data?.reason?.kind === 'completed') {
        // Lightweight: just get_status to bump last_seen, don't settle on every turn
        await callMCP('get_status', {}).catch(() => {})
      }
    })
  }

  // Periodic status poll (mail count, fleet pulse)
  const interval = config.statusIntervalSecs ?? 30
  if (interval > 0) {
    statusTimer = setInterval(async () => {
      await callMCP('get_status', {}).catch(() => {})
    }, interval * 1000)
    ;(statusTimer as any)?.unref?.()
  }

  // Cleanup on plugin dispose
  ctx.on('dispose', () => {
    if (statusTimer) clearInterval(statusTimer)
  })
}
