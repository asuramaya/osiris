# DSH integration — the Osiris bridge

`osiris-bridge/` is the source of truth for the Cordis plugin that mounts a DSH
(DeepSeek Harness) session into Osiris. It is checked in **here**, in the repo we own,
and *installed into* the harness — never authored there.

## Why it lives here and not in the harness

The plugin must sit inside the harness's own pnpm workspace to resolve its peer
dependencies (`cordis`, `dsh-agent`, `dsh-tools`, `dsh-session-persistence`, …). But
that harness — `~/code/dsh/deepseek-harness` — is a **vendored clone of
`github.com/deepseek-ai/deepseek-harness`**. We cannot commit to it.

So for two days the entire integration existed as an untracked directory plus ten
uncommitted edits to tracked upstream files, held together by a hand-made symlink. A
single `git clean -fdx`, branch switch, or `git pull --rebase` would have destroyed all
of it, silently, with no copy anywhere. That was task **#194**.

The fix is directional: **osiris is the origin, the harness is a derived target.**

## Install

```sh
dsh-plugin/install.sh [/path/to/deepseek-harness] [/path/to/dsh/profile]
```

Idempotent, and safe — required, in fact — to re-run after every upstream pull. It
copies the nine sources, applies `harness-overlay.patch`, links + builds through pnpm,
declares the profile dependency, and probes `/automount`.

Then merge `deploy/dsh-osiris-preset.yml` into `~/.dsh/profiles/web/cordis.patch.yml`
and **restart DSH** — HMR does not reliably pick up a newly registered plugin, which is
why the live patch file accumulated `# hmr kick` comments.

## The two moving parts

**`osiris-bridge/`** — the package. Four legs, every one fail-open:

| event | what it does |
|---|---|
| `agent/session-start` | resolves the durable anchor via `sessionPersistence.locate()`, POSTs `/automount` |
| (same turn) | binds the shared MCP connection by executing `mcp__osiris__mount` once through `ctx.tools.execute` |
| (same turn) | injects the server-rendered whisper as a plugin-sourced snapshot message |
| `agent/disposed` | POSTs `/session-end` |

**`harness-overlay.patch`** — the ten tracked-file edits the harness needs to *know*
the package exists: `tsconfig.host.json` project reference, the `packages/experimental`
README index (3 locales), the generated doc catalogs (5 files), `pnpm-lock.yaml`.
Regenerate with `git -C <harness> diff > dsh-plugin/harness-overlay.patch`.

## Known limits, stated rather than discovered later

- **Requires the jsonl session-persistence backend.** On SQLite, `locate()` yields no
  per-session artifact, the anchor is `undefined`, no bind happens, and the bridge
  degrades to an honest note. That is the designed failure, not a bug to hunt.
- **One shared MCP connection.** Concurrent depth-0 sessions interleave attribution.
- **No re-bind after an MCP reconnect.**
- **No compaction-succession mint** — a DSH compacted session arrives with a new id.
- **Depth > 0 (subagents) automount but never bind**; they ride the parent's connection.
- `/stop` is not wired to `agent/turn-stopping`.

## Superseded

`@deepseek-ai/dsh-osiris-lifecycle` (was `dsh-plugin/{package.json,src,lib}`) is retired
as of 2026-08-25. It reached Osiris through a guessed `ctx.mcp` service behind `as any`
casts, hardcoded an absolute path to the harness's `tsc` in its build script, and had no
anchor resolution, no `/automount`, and no whisper. It is in git history if ever needed.
