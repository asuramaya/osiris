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

**Bare `osiris` (no arguments) is the front door, not an error dump** (dispatch 3678/3681,
the operator's own "make the cli itself explain better than a dump so it can be friendly"):
it prints the newcomer path as two copy-pasteable lines, groups every command by what
you're trying to DO rather than alphabetically, and states plainly that there is no
separate project-creation command beyond `osiris new` itself. The exit code stays `2` (a
real usage condition — no command was given), only the text changed. Every subcommand's
own `--help` ends with at least one worked, realistic invocation.

**The two commands meant to be memorized** (dispatch 3685/3688, the operator's own "too
much witchcraft to spawn a project... I'll remember 'osiris x y z' boom"): `osiris new
<handle>` then `osiris launch <handle>` — nothing to a working, independent mind. See
`osiris new` below; everything else on this page is discoverable when you need it, not
something to hold in memory in advance.

**`--actor` defaults to `console`** (one of `src.orchestrator.seats._OPERATOR_ACTORS`'s own
sentinels) on every sanctioned-second-door command below — a raw terminal call already
carries operator authority by construction (no MCP round-trip, no borrowed agent identity),
so typing it by hand was never adding information. Pass it explicitly only to attribute an
act to someone or something else.

**Consistency with the MCP surface is now an enforced invariant, not a convention**
(decision `0b29f1cbcc5a`): `tests/test_cli_mcp_parity.py` walks both the argparse structure
here and the live MCP tool registry, and fails the suite if any shared act's name or
parameters drift apart without an explicit, reasoned allowlist entry. If you're renaming or
re-parameterizing a command below, that test — not this file — is the one that will catch
you first.

## `osiris new <handle> [path] [--project P] [--model M] [--actor <who>]`

**One of the two commands meant to be memorized** (dispatch 3685/3688) — the whole point
is that a human should be able to spawn an independent, self-managed project from memory,
months from now, with no notes. Founds a **self-managed seat**: a Seat with NO
`managed_by` edge at all, ever — not a flag, the absence itself (Ooblek's own real shape,
read off its dossier before this was built rather than assumed: self-claimed, then
officed, then — hours later, live — self-declared its own `governs` edge; no minting
agent, no manager, ever involved). Composes the SAME primitives `mint-seat` does
(`ensure_seat`, the office scaffold) rather than reimplementing them — see
`mintseat.found_seat`.

One act does all of: create the code workspace directory if absent (`path`, defaulting to
`~/code/<handle>` — **no git repo required**, proven not assumed: a seat's office is
already routinely a bare, non-repo folder that mounts fine, and project resolution reads
a `.osiris` pin or a bare folder name, never git), write that workspace's own `.osiris`
pin (`project` defaults to `handle`), mint the seat, scaffold its identity office at the
standard `~/.osiris/seats/<handle>/` location (distinct from the workspace — offices.py's
own ruling `ed5f5ce2`: "agents sit at `~/.osiris/seats/<handle>/`, code stays in the
repos they GOVERN"), and `bind_seat_tree` the workspace to the new seat so `osiris
launch` spawns into the *code*, never the identity office. Prints the exact `osiris
launch <handle>` line — the second command never needs remembering either.

**Does not create a `governs` edge.** Ooblek's own real bootstrap order was self-claim,
then office, then — once actually live — self-charter; inventing a `governs` fact on an
unlaunched mind's behalf would be exactly the kind of thing this call has no standing to
assert. The scaffolded `CLAUDE.md` already tells a fresh, self-managed seat that its
first act is `charter(repos=[...])` naming its own project, in its own voice, once live.

Idempotent — a handle that already names a living, already self-managed seat converges
(fills in whatever's missing, mints nothing new); a handle already `managed_by` someone
else REFUSES (`osiris new` founds independence, it does not strip an existing manager);
a near-miss handle (ruling `7cffda8f`) refuses the same way `mint-seat`'s own fresh path
does.

**Not on the MCP surface, deliberately, and not a violation of the surface-unity law**
(decision `0b29f1cbcc5a`'s own reasoning, worth restating since it looks like a
conflict): `walk_in` is a mind that ALREADY EXISTS naming itself — a self-act. `osiris
new` is an OPERATOR founding a seat for a mind that does not exist yet, then launching
into it. Different actor, different precondition, different moment — two genuinely
different acts may carry two different names without violating "one act, one name."

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
  mechanically instead of by a human re-deriving it from `whois()`/`dossier()` by hand (as
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

## `osiris merge <dupe> <into> --evidence <text> [--actor <who>]`

The sanctioned second door onto `orchestrator.merge.merge` — the same self-typing function
the `merge` MCP tool wraps, no softened gate. Type is read off `dupe`'s own form
(`agent:...`/`seat:...`/else → `SoftwareProject`); every refusal any of the three original
`fold_X` verbs had is reachable unchanged. Exists for a live client whose deferred-tool
index sits frozen across a deploy (ruling `482c3d0f`), or a worker whose sandbox classifier
permits an installed entrypoint but refuses a raw `DATABASE_URL` script. The merge event and
`same_as` link the MCP wrapper's own receipt surfaces (SoftwareProject merges only, matching
the MCP tool's own conditional) are queried and printed here too — two doors onto one
function return the same receipt, never a weaker echo.

**Renamed from `fold-project`** (dispatch 3683, decision `0b29f1cbcc5a`): `fold_project` was
retired as an MCP tool in favor of `merge`/`unmerge` for findability (ruling `31c02dca`,
decision `a926a8d0`) and the CLI silently kept the old name — two halves of this house using
different words for one act, the exact failure the operator's own consistency law names.
`osiris fold-project <dupe> <into> --evidence <text>` still works, identical arguments,
SoftwareProject-only — a hidden, deprecated alias that prints a one-line pointer to `merge`
on every call. Never advertised in the front-door listing; never silently broken either.

## `osiris unmerge <dupe> --because <text> [--actor <who>] [--execute]`

The sanctioned second door onto `orchestrator.merge.unmerge` — the same function the
`unmerge` MCP tool wraps. Reverses a wrongful `merge`; type is read off `dupe`'s own form,
same rule as `merge`. **Dry run is the default**, matching the MCP tool's own convention
exactly: without `--execute` this only prints the reversal plan (nothing written); review
it, then re-run with `--execute` to apply it. Built alongside `merge`'s own CLI rename —
the two verbs are a pair on the MCP side and had no reason to stay asymmetric here.

## `osiris charter-for <seat> --repos a,b,c --because <text> [--actor <who>]`

The sanctioned second door onto `charter.charter_for` — same guard as the MCP tool, fully
enforced (the `managed_by`/operator-actor check is the whole point of this verb, never
softened here). `--repos` is the WHOLE charter, not an increment; `--actor` must be the
seat's own manager or an operator actor (defaults to `console`, already an operator actor),
or it refuses by name.

## `osiris amend-practice <ref> <amendment> [--actor <who>]`

The sanctioned second door onto `capture.amend_practice` — narrows a LIVE practice's
guidance without touching its id, its `statement` (the idempotency key), or its witness
count. Refuses on a refuted practice or a blank amendment, the same as the MCP tool. Folded
directly into `practices()`'s own live listing, not left write-only.

## `osiris annotate-thread <ref> <note> [--actor <who>]`

The sanctioned second door onto `capture.annotate_thread` — the same function the
`annotate_thread` MCP tool wraps. Appends to a thread's record without closing it;
`summary`/`status` are never touched. `ref` matches a Thread's UUID, canonical, short-id
prefix, or summary substring, regardless of the thread's own open/resolved/deferred status.

Named as a gap before it was built: `charter-for`'s own docstring already listed this verb
(alongside `amend-decision`) as sharing `fold-project`'s shape — shipped, deployed, and
invisible to a fleet client whose deferred-tool index sat frozen (ruling `482c3d0f`) — but
only `fold-project`/`charter-for`/`amend-practice` had a second door built. Calls the
orchestrator function directly with an explicit `--actor`, the same reason those three do:
a real provenance loss otherwise, not a generic "session" bucket.

## `osiris amend-decision <ref> <addendum> [--actor <who>]`

The other half of that same pair — the sanctioned second door onto `capture.amend_decision`.
Appends reasoning to a LIVE decision as understanding develops, WITHOUT superseding it;
`summary`/`rationale`/`kind` are never touched. Refuses when `ref` resolves to a decision
already superseded (amend the successor instead, or use `record_decision(supersedes=...)`
for an actual correction).

## `osiris mint-seat <handle> [--manager <seat>] [--project] [--house] [--model] [--actor] [--adopt] [--force]`

The sanctioned second door onto `mintseat.mint_seat` — a DIFFERENT shape of gap than the
doors above: those exist because a client's tool index can go stale; this one exists because
the `mint_seat` MCP tool has **no `manager` parameter at all** — it infers the manager from
the calling agent's own held seat ("the calling seat is always the manager... minting into
someone else's org is a console act, deliberately absent here," `mint_seat`'s own
docstring). A raw terminal has no mounted agent identity to infer from.

**`--manager` is inferred, not required** (dispatch 3678/3681): omit it and this door looks
for the *sole* existing seat in the target house — `--house` if given, else the cwd's own
`.osiris` pin, else the cwd's own directory name — and uses it. Zero or several candidate
seats refuses loudly, naming what was tried and how to disambiguate, never a silent guess.
Naming a `--house` with no seats in it at all (a brand-new house/project) always needs an
explicit `--manager` naming any existing seat elsewhere — crossing into an empty house has
nothing to infer from by construction, and `mint_seat`'s own cross-house guard requires it
anyway (satisfied automatically: `--actor` already defaults to an operator actor).

One call does the whole ceremony: `ensure_seat` + the office scaffold (dir, `.osiris` pin,
`CLAUDE.md` + `charter.md`) + an `intended_model` stamp + the `managed_by` edge to the
manager. Idempotent — a handle that already names a living seat is *adopted*
(fill-missing-only), never twinned; `--adopt` states that intent explicitly (refuses on no
match rather than silently minting fresh), `--force` is the only door past a near-miss
handle refusal (ruling `7cffda8f`). Both are **deliberate console-only escape hatches** —
stated explicitly in `mint_seat`'s own MCP docstring since dispatch 3683's addendum — an
agent caller can never reach them, on purpose: an ordinary coordinator's mint never needs to
refuse-instead-of-adopt or force past a safety guard. The receipt prints `mint_seat`'s own
occupancy-aware next step — vacant names the exact `launch(target=...)` call to body it next.

This closes the exact gap this file's own house law names below: before this command
existed, standing up a brand-new seat from a terminal had no door but a hand-rolled
`python -c` heredoc against the live DB — precisely what ruling `45b074bf` bans.

## The house law behind every subcommand

`osiris` is one of exactly two ways into this system — the other is the MCP tool surface an
agent uses. Both exist so that "how do I do X" always has a documented, honest, idempotent
answer instead of a hand-rolled one-off. If you find yourself reaching for a raw `psql`
session, a bare `systemctl` invocation, or a `python -c` heredoc against the live database,
stop — that gap is a bug in this surface, not a reason to route around it. See
[`../ARCHITECTURE.md`](../ARCHITECTURE.md) for how the graph itself is structured, and this
project's fleet mailbox for reporting a gap you find.
