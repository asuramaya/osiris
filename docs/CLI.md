<!-- topic: operations -->

# The `osiris` CLI — the operator's console-script

The rulings this surface answers to: **2ee43411** — deploy tooling is a first-class step,
never a raw asyncpg heredoc; **ad19a779** — graph navigation is a complete, cheap MCP/CLI
surface, never something you fall back to raw SQL for; **45b074bf** — "no bash runes; the
user never debugs the machinery." Together they say the same thing from three directions:
**there are exactly two doors into this system — the MCP tools (for an agent) and this CLI
(for a human at a terminal) — and neither door is ever a hand-rolled `python -c` or a raw
`psql` session.** If a task needs either of those, that is itself a finding: something this
surface should have covered and doesn't yet.

Installed via `uv sync` (task #69) — `pyproject.toml`'s `[project.scripts]` makes `osiris`
a real console-script in `.venv/bin/`, not a script you `python -m` from a particular
directory. Every subcommand below works from anywhere: your own office, `~/code/osiris`, a
tmux pane three levels removed. Where a command DOES care about a specific checkout (`osiris
deploy` — see below), that is because the operation is inherently about *that* checkout, the
same way `git status` only ever means something once you're inside a repo; it is not the CWD
bug this CLI otherwise closes (see `src/config/dev_env.py`: a bare invocation with no
`DATABASE_URL` exported targets the real dev instance, never a silent, wrong prod-shaped
default).

Every subcommand follows the same law: **an error names the next step.** A dark daemon says
which unit to check; an ambiguous handle lists the candidates; a refusal says what to fix.
None of them print a raw Python traceback for a condition the CLI can see coming.

## `osiris attach <handle>`

Attach to a seat's live PTY session — the interactive replacement for the raw
`.venv/bin/python -m src.manager.attach "[OS] imhotep"` invocation this CLI retired. Give it
the bare handle (`imhotep`, `thoth`, case-insensitive); it asks the manager daemon
(`osiris-manager`, over its control socket) for the live session roster and matches your
handle against the window name's own `[TAG] Handle` convention — you never need to know that
convention exists.

- **No match** → lists every currently-live session so you can see what's actually there.
- **Ambiguous match** (two live windows both plausibly named by your handle) → lists the
  candidates and asks for something more specific; it never guesses.
- **Dark daemon** → names `osiris-manager` as the thing to check
  (`systemctl --user status osiris-manager`).

Ctrl-C or the remote process exiting both end the session cleanly; the raw PTY stream (not
this CLI) owns your terminal for the duration, the same as any other interactive attach.

## `osiris smoke`

The deploy-time liveness probe the fleet itself runs (task #63) — two independent probes,
neither silencing the other: every named chrome route (`/desk`, `/fleet`, `/roadmap`, …)
walked directly over HTTP, and a real round-trip through `osiris-mcp`'s own `smoke` tool
(proving the pool a live agent actually gets, not a throwaway one spun up just to answer this
call — the exact class of bug that shipped past 1600 green unit tests once, `1da1bf2`).
Silent but for one line when everything is green; on any regression, prints the failing
surfaces by name.

Run this any time you want to know "is it actually up," and always right after a restart —
static gates (ruff/mypy/pytest) proved they cannot catch an event-loop-lifecycle bug; only a
real post-boot probe can.

## `osiris seed [--compositions-only]`

`src/init.py`'s seeder as a first-class deploy step (task #63) — adding a new default
composition, or re-rooming an existing one, is never a raw `asyncpg` heredoc against the live
DB again. `--compositions-only` seeds + rooms `DEFAULT_COMPOSITIONS` and skips the slow,
one-time design-canon ingest (repo-relative doc paths that only make sense on a fresh
install); bare `osiris seed` runs the full ritual. Idempotent either way — a second run never
duplicates a composition or a room, it only heals forward.

## `osiris launch <handle> [--model] [--debug]`

Bodies a seat. **Default lane (task #72, following `trigger.launch_seat`'s own flip, rulings
`0fe36e59` + `33d6a2eb` clause 3): `claude --bg`** — the harness's own background-session
surface. Every body this creates is visible in the operator's own `claude agents` list *by
construction*; identity rides the session's own first turn (a boot prompt telling it to
`mount()` then `claim_name(handle)`, the same proven adoption path a human follows into a
fresh office), since `--bg`'s claimed process has no channel for env-var identity stamping
(confirmed live — see `_spawn_claude_bg`'s own docstring for the full autopsy).

**`--debug` keeps the original osiris PTY-broker lane alive** as an explicit fallback — the
manager daemon's `pty_spawn` op, called directly, attachable via `osiris attach <handle>` —
for an incident, or a build with no `claude --bg`. It is no longer the default; the harness-
native lane's own body is *not* attachable this way (it has no PTY osiris ever opened), so
reach for `--debug` specifically when you need an interactive terminal on the fresh process.

Neither lane is `trigger.py`'s `launch_seat()` — that verb's own docstring is explicit ("THE
OPERATOR NEVER CALLS THIS... the operator's hand stays out-of-band"), because it's a
*seat-to-seat* verb gated by a `managed_by` graph edge. A human driving this CLI already **is**
the out-of-band hand — the same trust boundary `src.manager.attach` already stands in for —
so both lanes mirror `launch_seat`'s own primitives directly (same `claude --bg`/`claude
agents --json` calls, same boot-prompt wording, same `pty_spawn` op) rather than calling it.

Both lanes share the same correct model precedence (`--model` flag > the target seat's own
stamped `intended_model` > the wake-lane economy default) and the same bounded-poll honesty
discipline — never a bare `launched: true`:

- **`--debug` (PTY) lane**: polls a few seconds (bounded — never an indefinite wait) for two
  facts, is the window actually alive and has a fresh body mounted at the office and
  self-reported *which* model it's running. If those two disagree — the receipt says
  `MISMATCH` and names both models — that is thread `20e4feb6`'s own bug class, caught
  mechanically instead of by a human re-deriving it from `doors()`/`dossier()` by hand (as
  this exact incident once required, decision `8e9c48d9`).
- **Default (`--bg`) lane**: polls `claude agents --json` (bounded, same discipline) for the
  spawned body to appear, then reports it confirmed — `find it in claude agents as
  '[house] handle'` — plus the harness's own session id when the roster carries one.

If a live body already holds the handle, `launch` returns that instead of minting a twin —
never launch a second body onto a seat that's already occupied. In the default lane that
body is reachable via `claude agents`/`claude resume`, never `osiris attach` (that door is
`--debug`'s PTY lane only).

## `osiris fleet [--full]`

The same `fleet()` MCP tool answers, called over the wire (a small MCP client, not a second
implementation of a ~260-line query) — the roster grouped by project, live agents expanded,
retired sessions collapsed to a count. `--full` expands everything; the bare form is the
glanceable tree. Reach for this the same moment you'd otherwise open the console's `/fleet`
page from a terminal that has no browser.

## `osiris migrate [--check]`

The ENV-CORRECT migration verb (thread `c4681c38` leg 1) — `alembic upgrade head`, run
**in-process** via alembic's own command API, never a subprocess `alembic` rune. That
distinction is the whole point: a bare `alembic upgrade head` typed at a shell connects to
the prod-shaped `5432` default, because `alembic/env.py` reads `DATABASE_URL` and nothing set
the dev fallback first in *that* shell — exactly the footgun class ruling `45b074bf` bans.
Running it in-process means `apply_dev_fallback()` (the same call every DB-backed subcommand
makes) has already set `os.environ["DATABASE_URL"]` before alembic's own env.py ever reads
it — no rune, no passthrough, nothing for a human to get wrong.

`--check` reports a pending revision without applying it — for a human who wants to know, or
for `osiris deploy`'s own gate below, which uses the identical comparison to decide
refuse-or-run. Bare `osiris migrate` applies: up to date prints and exits clean; a pending
revision runs `upgrade head` and reports what it applied (`0037..0038 applied`); a failed
upgrade is reported honestly and exits nonzero — never a raw traceback.

## `osiris deploy`

The deploy ritual as one guarded verb (thread `e51a841c`) — it replaces the by-hand
`systemctl --user restart osiris-mcp osiris-worker osiris-console && python
scripts/osiris_smoke.py` protocol a manager used to run themselves, after a live near-miss:
batch 3's deploy was held because a file under `src/` carried another agent's uncommitted
WIP, and the three services import straight from the working tree — only a by-hand `git
status`, done at exactly the right moment, caught it before a restart would have shipped a
half-written edit.

Five steps, always in this order:

1. **The dirty-tree guard.** Any tracked file under `src/` with an uncommitted change (staged
   or not — everything but a brand-new untracked file, which nothing imports yet) REFUSES the
   deploy outright, naming every file. It does **not** guess whose work it is — that's a
   fragile heuristic that could as easily misattribute as help — it points you at project
   mail instead: check for a collision-watch broadcast naming the file before assuming it's
   abandoned. This refusal is the correct behavior, not an inconvenience: deploy is always
   operator-recoverable (commit or stash, then re-run), which is exactly why refusing rather
   than restarting anyway is the safe default here — unlike a serving-path guard, where a
   refusal has its own cost.
2. **The migration gate, BEFORE anything restarts** (thread `c4681c38` leg 2). The DB's
   `alembic_version` is compared against the latest migration's revision id on disk; a match
   prints `migrations: up to date` and proceeds. A pending revision RUNS it (the same
   `osiris migrate` machinery) and prints what it applied; a failed upgrade REFUSES the whole
   deploy outright — nothing restarts, nothing is left half-migrated. This closes a real near
   miss: batch 6's own deploy restarted `osiris-mcp` onto code expecting a column the DB
   didn't have yet, and only reported the pending migration *after* — a window where new code
   ran against the old schema, surviving only because those particular writes happened to be
   fail-open. A deploy is now atomic from the schema's point of view: the restart simply never
   happens until the schema underneath it is current, or the deploy refuses and says why.
3. **Restart** `osiris-mcp`, `osiris-worker`, `osiris-console` (`systemctl --user restart`).
4. **Smoke**, per-surface (the same probe `osiris smoke` runs).
5. **The composition seeder gap, named by comparison, never assumed.** The DB's actual
   composition count against `DEFAULT_COMPOSITIONS`'s own length — a shortfall prints exactly
   what to run (`osiris seed`) rather than leaving you to guess whether it happened.

### The deploy taxonomy: two classes of surface

Not every change is gated by a restart, and `osiris deploy` says so plainly rather than
implying a hold protects everything:

- **Restart-deployed** — the long-running services (`osiris-mcp`, `osiris-worker`,
  `osiris-console`) import straight from the working tree at process start. A held,
  uncommitted change here is genuinely inert until the next restart; the dirty-tree guard
  above is what makes that hold meaningful.
- **Commit-deployed** — a `Type=oneshot` systemd timer (`osiris-preflight`, `osiris-backup`)
  reads its script fresh off disk at *every fire*, independent of any long-running process.
  There is no restart to gate here, and no hold protects it: whatever's on disk (committed
  or not) is already effectively live at the script's next tick. `osiris deploy` detects a
  dirty file backing one of these units (derived from parsing `deploy/*.service`, never a
  hardcoded list) and names it as an informational note, not a refusal — refusing a restart
  that has no bearing on the surface in question would be theater, not a guard. For these,
  review has to happen *before* the commit, not after.

## The house law behind all seven

`osiris` is one of exactly two ways into this system — the other is the MCP tool surface an
agent uses. Both exist so that "how do I do X" always has a documented, honest, idempotent
answer instead of a hand-rolled one-off. If you find yourself reaching for a raw `psql`
session, a bare `systemctl` invocation, or a `python -c` heredoc against the live database,
stop — that gap is a bug in this surface, not a reason to route around it. See
[`../ARCHITECTURE.md`](../ARCHITECTURE.md) for how the graph itself is structured, and this
project's fleet mailbox for reporting a gap you find.
