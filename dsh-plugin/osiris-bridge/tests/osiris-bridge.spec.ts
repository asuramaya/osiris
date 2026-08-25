import { afterEach, describe, expect, it, vi } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Context } from '@deepseek-ai/cordis'
import type { Agent } from '@deepseek-ai/dsh-agent'
import AgentLoop from '@deepseek-ai/dsh-agent-loop'
import { mountAgentLoopTestDependencies } from '@deepseek-ai/dsh-agent-loop-testkit'
import { createUserMessage } from '@deepseek-ai/dsh-llm'
import { SessionId } from '@deepseek-ai/dsh-session'
import JsonlSessionPersistence from '@deepseek-ai/dsh-session-persistence-jsonl'
import { defineContentToolFixture } from '@deepseek-ai/dsh-tools'
import { MockAdapter, textResponse } from '../../../core/agent-loop/tests/mock-adapter.ts'
import * as OsirisBridge from '../src/index.ts'

const SID = 'session-feedbeef-0000-4000-8000-000000000000'
const CWD = '/w/demo'
const roots: string[] = []

afterEach(() => {
  vi.unstubAllGlobals()
  for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true })
})

/** The fake osiris HTTP surface: record automount/session-end bodies, answer canned payloads. */
function stubOsirisHttp(automountResponse: unknown = { agent: 'agent:feedbeef', project: 'demo' }) {
  const automounts: Record<string, unknown>[] = []
  const sessionEnds: Record<string, unknown>[] = []
  const fetchStub = vi.fn(async (url: string | URL, init?: RequestInit) => {
    const target = String(url)
    const rawBody = typeof init?.body === 'string' ? init.body : '{}'
    const body = JSON.parse(rawBody) as Record<string, unknown>
    const respond = (payload: unknown) => new Response(JSON.stringify(payload), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    })
    if (target.endsWith('/automount')) {
      automounts.push(body)
      return respond(automountResponse)
    }
    if (target.endsWith('/session-end')) {
      sessionEnds.push(body)
      return respond({ released: 1 })
    }
    return new Response('not found', { status: 404 })
  })
  vi.stubGlobal('fetch', fetchStub)
  return { automounts, sessionEnds }
}

/** A real cordis assembly: tool registry, jsonl persistence, agent loop, bridge, a fake mount tool. */
async function assemble(
  automountResponse?: unknown,
  options: { mountTool?: boolean; http?: 'stub' | 'down' } = {},
) {
  const http = options.http === 'down'
    ? (() => {
      vi.stubGlobal('fetch', vi.fn(async () => { throw new Error('connection refused') }))
      return { automounts: [] as Record<string, unknown>[], sessionEnds: [] as Record<string, unknown>[] }
    })()
    : stubOsirisHttp(automountResponse)
  const ctx = new Context()
  await mountAgentLoopTestDependencies(ctx)
  const storageRoot = mkdtempSync(join(tmpdir(), 'dsh-osiris-bridge-'))
  roots.push(storageRoot)
  await ctx.plugin(JsonlSessionPersistence, { root: storageRoot })
  await ctx.plugin(AgentLoop, { agents: [] })
  const mountCalls: unknown[] = []
  if (options.mountTool !== false) {
    ctx.tools.register(defineContentToolFixture({
      name: 'mcp__osiris__mount',
      description: 'fake osiris mount for the bridge test',
      parameters: {},
      async execute(args) {
        mountCalls.push(args)
        return [{ type: 'text' as const, text: 'mounted' }]
      },
    }))
  }
  const fiber = await ctx.plugin(OsirisBridge)
  const adapter = new MockAdapter([textResponse('ok')])
  ctx.llm.registerAdapter(['mock'], adapter)
  return { ctx, http, mountCalls, fiber, storageRoot }
}

/** Create one live agent (its publish fires agent/session-start synchronously). */
function startAgent(ctx: Context): Agent {
  return ctx.agentLoop.create(SessionId(SID), { provider: 'mock', model: 'mock' }, { cwd: CWD })
}

/** The bridge's plugin-sourced context messages that reached the session log. */
function pluginMessages(agent: Agent): string[] {
  return [...agent.session.events]
    .flatMap(event => event.type === 'user/message' ? [event.data] : [])
    .filter(message => message.source.kind === 'plugin' && message.source.plugin === 'osiris-bridge')
    .map(message => message.content
      .filter(block => block.type === 'text')
      .map(block => block.text)
      .join('\n'))
}

describe('osiris-bridge session lifecycle', () => {
  it('automounts a starting session under its durable session directory and binds the connection', async () => {
    const { ctx, http, mountCalls } = await assemble()
    startAgent(ctx)
    await vi.waitFor(() => { expect(http.automounts.length).toBe(1) })
    await vi.waitFor(() => { expect(mountCalls.length).toBe(1) })

    const body = http.automounts[0] as Record<string, unknown>
    expect(body.session_id).toBe(SID)
    expect(body.cwd).toBe(CWD)
    // THE EXPLICIT ANCHOR: the session's own on-disk directory from the jsonl backend
    expect(String(body.job_dir)).toContain(SID)
    // the honesty-gate testimony: the bridge binds, so env_job names the anchor
    expect(body.env_job).toBe(body.job_dir)
    expect(body.render).toBe(true)
    // the bind drove the REAL mount tool through the REAL tool registry
    expect(mountCalls[0]).toMatchObject({ cwd: CWD, job_dir: body.job_dir })
  })

  it('injects the server-rendered whisper into the session as plugin context', async () => {
    const { ctx, http, mountCalls } = await assemble({
      agent: 'agent:feedbeef',
      project: 'demo',
      job_dir: '/nowhere/session-feedbeef',
      whisper_text: '◈ OSIRIS — You are ALREADY MOUNTED as agent:feedbeef (project demo)',
    })
    const agent = startAgent(ctx)
    await vi.waitFor(() => { expect(http.automounts.length).toBe(1) })
    await vi.waitFor(() => { expect(mountCalls.length).toBe(1) })

    // the queued whisper is claimed by the first real turn and logged durably
    agent.followup(createUserMessage({
      content: [{ type: 'text', text: 'go' }],
      source: { kind: 'user' },
    }))
    await agent.whenIdle()

    const messages = pluginMessages(agent)
    expect(messages.length).toBe(1)
    expect(messages[0]).toContain('ALREADY MOUNTED as agent:feedbeef')
  })

  it('posts session-end with the anchor when the agent is disposed', async () => {
    const { ctx, http } = await assemble()
    const agent = startAgent(ctx)
    await vi.waitFor(() => { expect(http.automounts.length).toBe(1) })
    expect(http.sessionEnds.length).toBe(0)

    // the loop's own tested teardown path emits agent/disposed; the bridge answers it
    ctx.emit('agent/disposed', { agent })
    await vi.waitFor(() => { expect(http.sessionEnds.length).toBe(1) })

    expect(http.sessionEnds[0]).toMatchObject({ session_id: SID })
    expect(String(http.sessionEnds[0]?.job_dir)).toContain(SID)
  })

  it('degrades to the unreachable note when the server is down', async () => {
    const { ctx } = await assemble(undefined, { http: 'down' })
    const agent = startAgent(ctx)
    agent.followup(createUserMessage({
      content: [{ type: 'text', text: 'go' }],
      source: { kind: 'user' },
    }))
    await agent.whenIdle()

    const messages = pluginMessages(agent)
    expect(messages.length).toBe(1)
    expect(messages[0]).toContain('unreachable')
    expect(messages[0]).toContain('job_dir=')
  })

  it('skips the connection bind when the mount tool is absent and says so honestly', async () => {
    const { ctx, http } = await assemble({
      agent: 'agent:feedbeef',
      project: 'demo',
      whisper_text: '◈ OSIRIS — It knows you as agent:feedbeef',
    }, { mountTool: false })
    const agent = startAgent(ctx)
    agent.followup(createUserMessage({
      content: [{ type: 'text', text: 'go' }],
      source: { kind: 'user' },
    }))
    await agent.whenIdle()

    expect(http.automounts.length).toBe(1)
    const messages = pluginMessages(agent)
    // the bind failed honestly: the note names the manual door, never claims bound
    expect(messages.length).toBe(1)
    expect(messages[0]).toContain('not registered')
    expect(messages[0]).not.toContain('ALREADY MOUNTED')
  })
})

describe('osiris-bridge pure helpers', () => {
  it('resolveAnchor uses the jsonl backend location and delegation depth', async () => {
    const { ctx } = await assemble()
    const anchor = OsirisBridge.resolveAnchor(ctx, {
      version: 0,
      id: SessionId('session-12345678-0000-4000-8000-000000000000'),
      createdAt: 0,
      cwd: '/w/demo',
      delegationDepth: 2,
    })
    expect(anchor.id).toBe('session-12345678-0000-4000-8000-000000000000')
    expect(anchor.cwd).toBe('/w/demo')
    expect(anchor.depth).toBe(2)
    expect(anchor.jobDir).toContain('session-12345678-0000-4000-8000-000000000000')
    await ctx.fiber.dispose()
  })

  it('bindConnection refuses without a jobDir', async () => {
    const { ctx } = await assemble()
    const outcome = await OsirisBridge.bindConnection(ctx, {
      baseUrl: 'http://127.0.0.1:8790',
      serverName: 'osiris',
      bindConnection: true,
      timeoutMs: 3_000,
    }, {
      id: 'session-x',
      cwd: '/w',
      jobDir: undefined,
      depth: 0,
    })
    expect(outcome.ok).toBe(false)
    await ctx.fiber.dispose()
  })
})
