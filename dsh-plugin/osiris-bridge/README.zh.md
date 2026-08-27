# @deepseek-ai/dsh-experimental-osiris-bridge

[English](README.md) | 中文

面向 [Osiris](https://github.com/asuramaya/osiris) 舰队记忆服务器的 harness 原生桥接。在同时运行 Osiris MCP 服务器（通过 `@deepseek-ai/dsh-mcp-client`）的主机上，每个会话在 `agent/session-start` 时挂载进舰队、在 `agent/disposed` 时释放 —— 与 Claude Code 通过 Osiris 的 SessionStart/SessionEnd 钩子脚本获得的契约相同，但以原生 Cordis 插件交付，无 python 依赖。

## 它做什么

1. **Automount。** `agent/session-start` 时向 Osiris 服务器的 `/automount` 路由 POST `{session_id, cwd, job_dir, source}`。`job_dir` 是会话自身的持久目录（`<sessions-root>/<project-slug>/session-<uuid>`），通过已配置的会话持久化后端（`ctx.sessionPersistence.locate`）解析，因此非默认 sessions root 也能得到 Osiris DSH 适配器可识别的锚点。
2. **连接绑定。** MCP SDK 的 streamable-http 传输只携带静态 header，Osiris 按请求重连的 `X-Osiris-Job` 通道不可用。桥接改为通过工具注册表真实调用一次 `mcp__<serverName>__mount` 工具：该调用走的就是 agent 自己工具调用所用的那条 MCP 连接，Osiris 会把该会话的身份缓存到该连接上 —— 之后的 osiris 工具调用无需任何 header 即可重连身份。
3. **Whisper 注入。** 桥接请求服务器渲染 whisper 段落（`render: true`；渲染器唯一存在于 Osiris，没有 TypeScript 副本），并以 plugin 来源的 snapshot 上下文注入，让会话从第一步就知道自己的 agent id、项目、模型、邮件、义务与持久锚点。
4. **会话结束。** `agent/disposed` 时向 `/session-end` POST `{session_id, job_dir}`，会话结束的瞬间释放挂载行，而不是等待存活窗口衰减。

子代理会话（delegation depth > 0）同样被 automount（持久行、舰队可见），但从不绑定共享连接，并收到一条诚实的说明而非 whisper：它们的 osiris 调用走的是父会话的 MCP 连接。

## 配置

```yaml
- osiris-bridge:
    baseUrl: http://127.0.0.1:8790   # the Osiris MCP server's plain-HTTP hook listener
    serverName: osiris               # the dsh-mcp-client serverName prefixing its tools
    bindConnection: true             # bind depth-0 sessions by calling the mount tool
    timeoutMs: 3000                  # per-request budget for the Osiris HTTP calls
```

需要具备每会话工件（artifacts）的会话持久化后端（jsonl 后端）。没有该性质的后端（SQLite）无法解析锚点，桥接以一条警告空转。按设计 fail-open，与 Osiris 自身的钩子契约一致：服务器不可达、绑定失败或工具缺失都退化为一条诚实的注入说明，绝不阻塞会话启动。

## Model Experience

### 会话启动时的 Osiris whisper

#### 模型看到什么

一条 plugin 来源的 user 消息，文本为 Osiris 服务器渲染的 whisper：会话的舰队身份（`agent:<id>`、项目、观测到的模型）、未读邮件与义务，以及连接掉线后用于重新挂载的持久锚点。确切文本由 Osiris（`scripts/osiris_hook.py` 的 `render_whisper`）拥有，内容随舰队图谱变化；桥接只负责运输。

#### Token 效应

每次会话启动一条上下文消息（每次绑定失败附加一条回退说明）。内容依赖图谱，随项目义务摘要增长，由 Osiris 的 whisper 渲染器封顶。

#### KV Cache 效应

whisper 在首个认领它的请求处追加到会话可复用前缀之后；它不替换更早的 token。后续请求将其作为普通历史重发，直至压缩。

## Known Limitations and Deferred Work

- **一条连接，一个身份** —— 同一主机进程内的所有 agent 共享 dsh-mcp-client 的单条 MCP 连接，因此 Osiris 把 osiris 工具调用归属到最后一个绑定连接的 depth-0 会话。同一进程内并发的 depth-0 会话会交错归属；每 agent 一条 MCP 连接需要 `dsh-mcp-client` 的产品级改动。
- **子代理继承父会话的归属** —— delegation depth > 0 的会话从不绑定（绑定会抢占父会话的缓存身份），其 osiris 调用被归属到父会话。
- **无重连再绑定** —— MCP 服务器重启且客户端重连（新连接 key）后绑定变冷；下一次 osiris 调用会以 "mount first" 弹回，whisper 中的持久锚点句是恢复路径。检测重连并再绑定被推迟。
- **无压缩继承（succession）** —— Osiris 的压缩铸币依赖稳定会话 id；DSH 压缩后的会话携带新 id，因此该桥接尚不触发 successor-mint 流程。
- **未桥接 Stop/offload 仪式** —— Osiris 的 Stop 钩子交付物检查（`/stop`）未接入 `agent/turn-stopping`；settle 仪式仍由 agent 自觉执行。
