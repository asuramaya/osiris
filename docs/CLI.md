<!-- topic: operations -->

# The `osiris` CLI

This CLI and the MCP tool surface are the only two supported ways to operate this
system: the MCP tools for an agent, this CLI for a human at a terminal. Deploy tooling
is meant to be a first-class step, not a raw asyncpg heredoc; graph navigation is meant
to be a complete, cheap CLI/MCP surface, not something you fall back to raw SQL for.
Neither entry point is ever a hand-rolled `python -c` script or a raw `psql` session.
If a task needs one of those, that's a gap in this surface, not something to route
around.

Installed via `uv sync`: `pyproject.toml`'s `[project.scripts]` entry makes `osiris` a
real console-script in `.venv/bin/`, not something you run with `python -m` from a
specific directory. Every subcommand below works from any directory, including outside
the repository. Where a command does care about a specific checkout (`osiris deploy`,
see below), that's because the operation is inherently about that checkout, the same
way `git status` only means something once you're inside a repo. This is not the
working-directory bug this CLI otherwise avoids: see `src/config/dev_env.py`. A bare
invocation with no `DATABASE_URL` exported targets the real development instance,
never a silent, wrong production default.

Every subcommand follows the same principle: an error names the next step. A dead
daemon says which unit to check; an ambiguous handle lists the candidates; a refusal
says what to fix. None of them print a raw Python traceback for a condition the CLI
can see coming.

Running `osiris` with no arguments prints a helpful entry point instead of an error
dump: it shows the newcomer path as two copy-pasteable lines, groups every command by
what you're trying to do rather than alphabetically, and states plainly that there is
no separate project-creation command beyond `osiris new` itself. The exit code stays
`2` (a real usage condition, no command was given); only the text changed. Every
subcommand's own `--help` ends with at least one worked, realistic invocation.

The two commands worth memorizing: `osiris new <handle>` then `osiris launch
<handle>`, enough to get a working, independent instance running. See `osiris new`
below; everything else on this page is discoverable when you need it, not something to
hold in memory in advance.

`--actor` defaults to `console` (one of the operator-actor sentinels in
`src.orchestrator.seats._OPERATOR_ACTORS`) on every command below that also exists as
an MCP tool. A raw terminal call already carries operator authority by construction:
there's no MCP round-trip and no borrowed agent identity, so typing `--actor` by hand
normally adds no information. Pass it explicitly only to attribute an action to
someone or something else.

Consistency with the MCP surface is an enforced invariant, not just a convention:
`tests/test_cli_mcp_parity.py` walks both the argparse structure here and the live MCP
tool registry, and fails the test suite if any shared command's name or parameters
drift apart without an explicit, reasoned allowlist entry. If you're renaming or
re-parameterizing a command below, that test, not this file, is what catches it first.

## `osiris new <handle> [path] [--project P] [--model M] [--actor <who>]`

One of the two commands worth memorizing: this lets a person found an independent,
self-managed project from memory, without notes. It founds a self-managed seat: a Seat
with no `managed_by` edge at all, ever, not a flag but the literal absence of that
edge. (This mirrors how self-managed seats have actually come into being in practice:
self-claimed, then given an identity office, then, once live, self-declaring their own
`governs` edge, with no minting agent and no manager ever involved.) It composes the
same primitives `mint-seat` does (`ensure_seat`, the office scaffold) rather than
reimplementing them; see `mintseat.found_seat`.

One call does all of the following: create the code workspace directory if it doesn't
exist (`path`, defaulting to `~/code/<handle>`; no git repo required, since a seat's
office is already routinely a bare, non-repo folder that mounts fine, and project
resolution reads a `.osiris` pin or a bare folder name, never git); write that
workspace's own `.osiris` pin (`project` defaults to `handle`); mint the seat;
scaffold its identity office at the standard `~/.osiris/seats/<handle>/` location
(distinct from the workspace: agents sit at `~/.osiris/seats/<handle>/`, code stays in
the repositories they govern); and bind the workspace to the new seat so `osiris
launch` spawns into the code, never the identity office. It prints the exact `osiris
launch <handle>` line, so the second command never needs remembering either.

**Does not create a `governs` edge.** In practice, self-managed seats have come to
life through self-claim, then an identity office, then, once actually live, a
self-issued charter; inventing a `governs` fact on an unlaunched seat's behalf would go
beyond what this command has standing to assert. The scaffolded `CLAUDE.md` already
tells a fresh, self-managed seat that its first act is `charter(repos=[...])` naming
its own project, in its own voice, once live.

Idempotent: a handle that already names a living, already self-managed seat converges
(fills in whatever's missing, mints nothing new); a handle already managed by someone
else refuses (`osiris new` founds independence, it does not strip an existing
manager); a near-miss handle refuses the same way `mint-seat`'s own fresh path does.

**Not on the MCP surface, deliberately, and not an inconsistency with the rule above
that CLI and MCP commands stay in sync:** the MCP-only `walk_in` command is for a mind
that already exists, naming itself, a self-act. `osiris new` is an operator founding a
seat for a mind that does not exist yet, then launching into it. Different actor,
different precondition, different moment: two genuinely different actions can carry
two different names without breaking the "one action, one name" rule.

## `osiris attach <handle>`

Attach to a seat's live PTY session: the interactive replacement for the raw
`.venv/bin/python -m src.manager.attach "[OS] <handle>"` invocation this CLI retired.
Give it the bare handle (case-insensitive); it asks the manager daemon
(`osiris-manager`, over its control socket) for the live session roster and matches
your handle against the window name's own `[TAG] Handle` convention, so you never need
to know that convention exists.

- **No match** lists every currently-live session so you can see what's actually
  there.
- **Ambiguous match** (two live windows both plausibly named by your handle) lists the
  candidates and asks for something more specific; it never guesses.
- **Dead daemon** names `osiris-manager` as the thing to check
  (`systemctl --user status osiris-manager`).

Ctrl-C, or the remote process exiting, both end the session cleanly. The raw PTY
stream, not this CLI, owns your terminal for the duration, the same as any other
interactive attach.

## `osiris smoke`

The deploy-time liveness probe used by the deploy process: two independent probes,
neither silencing the other. Every named chrome route (`/desk`, `/fleet`, `/roadmap`,
and so on) is walked directly over HTTP, and a real round-trip is made through
`osiris-mcp`'s own `smoke` tool, proving the connection pool a live agent actually gets
rather than a throwaway one spun up just to answer this call (a real bug once shipped
past more than 1600 green unit tests this way). Silent except for one line when
everything is green; on any regression, it prints the failing surfaces by name.

Run this any time you want to know whether the system is actually up, and always
right after a restart: static checks (ruff, mypy, pytest) can't catch an
event-loop-lifecycle bug; only a real post-boot probe can.

## `osiris seed [--compositions-only]`

`src/init.py`'s seeder as a first-class deploy step: adding a new default
composition, or re-rooming an existing one, is never a raw `asyncpg` heredoc against
the live database again. `--compositions-only` seeds and rooms `DEFAULT_COMPOSITIONS`
and skips the slow, one-time design-canon ingest (repo-relative doc paths that only
make sense on a fresh install); bare `osiris seed` runs the full process. Idempotent
either way: a second run never duplicates a composition or a room, it only heals
forward.

## `osiris launch <handle> [--model] [--debug]`

Gives a seat a running process. **Default mode: `claude --bg`**, the harness's own
background-session surface. Every process this creates is visible in the operator's
own `claude agents` list by construction. Identity is established on the session's
first turn (a boot prompt telling it to `mount()` then `claim_name(handle)`, the same
proven adoption path a human follows into a fresh office), since a `--bg` process has
no channel for env-var identity stamping (confirmed live: see `_spawn_claude_bg`'s own
docstring for the full explanation).

**`--debug` keeps the original PTY-broker mode alive** as an explicit fallback: the
manager daemon's `pty_spawn` operation, called directly, attachable via `osiris attach
<handle>`, for an incident, or when `claude --bg` isn't available. It is no longer the
default; the harness-native mode's own process is not attachable this way (it has no
PTY osiris ever opened), so reach for `--debug` specifically when you need an
interactive terminal on the fresh process.

Neither mode is `trigger.py`'s `launch_seat()`: that function's own docstring is
explicit that the operator should never call it directly, because it's a seat-to-seat
function gated by a `managed_by` graph edge. A human driving this CLI already acts as
that out-of-band authority, the same trust boundary `src.manager.attach` already
stands in for, so both modes mirror `launch_seat`'s own primitives directly (same
`claude --bg`/`claude agents --json` calls, same boot-prompt wording, same `pty_spawn`
operation) rather than calling it.

Both modes share the same model precedence (`--model` flag, then the target seat's own
stamped `intended_model`, then an economy default for a low-stakes wake) and the same
bounded-poll honesty discipline, never a bare `launched: true`:

- **`--debug` (PTY) mode**: polls for a few seconds (bounded, never an indefinite
  wait) for two facts: whether the window is actually alive, and whether a fresh
  process has mounted at the office and self-reported which model it's running. If
  those two disagree, the receipt reports a mismatch and names both models, a known
  bug class now caught mechanically instead of requiring a human to re-derive it from
  `whois()`/`dossier()` by hand.
- **Default (`--bg`) mode**: polls `claude agents --json` (bounded, same discipline)
  for the spawned process to appear, then reports it confirmed, with a pointer to find
  it in `claude agents` under the reported handle, plus the harness's own session id
  when the roster carries one.

If a live process already holds the handle, `launch` returns that instead of starting
a second one: it never launches a second process onto a seat that's already occupied.
In the default mode that process is reachable via `claude agents`/`claude resume`,
never `osiris attach` (that's for the `--debug` PTY mode only).

## `osiris fleet [--full]`

The same `fleet()` MCP tool answers, called over the wire (a small MCP client, not a
second implementation of the underlying query): the roster grouped by project, live
agents expanded, retired sessions collapsed to a count. `--full` expands everything;
the bare form is the glanceable tree. Reach for this the same moment you'd otherwise
open the console's `/fleet` page from a terminal that has no browser.

## `osiris migrate [--check]`

Runs migrations from the correct environment: `alembic upgrade head`, run in-process
via alembic's own command API, never a subprocess `alembic` command. That distinction
is the whole point: a bare `alembic upgrade head` typed at a shell connects to the
production-shaped `5432` default, because `alembic/env.py` reads `DATABASE_URL` and
nothing set the development fallback first in that shell, exactly the failure mode
this CLI is built to prevent. Running it in-process means `apply_dev_fallback()` (the
same call every database-backed subcommand makes) has already set
`os.environ["DATABASE_URL"]` before alembic's own env.py ever reads it: no manual
step, nothing for a human to get wrong.

`--check` reports a pending revision without applying it, useful for a human who wants
to know, or for `osiris deploy`'s own gate below, which uses the identical comparison
to decide whether to refuse or run. Bare `osiris migrate` applies: up to date prints
and exits clean; a pending revision runs `upgrade head` and reports what it applied
(`0037..0038 applied`); a failed upgrade is reported honestly and exits nonzero, never
a raw traceback.

## `osiris deploy`

The deploy process as one guarded command. It replaces a manual `systemctl --user
restart osiris-mcp osiris-worker osiris-console && python scripts/osiris_smoke.py`
sequence, after a near miss: a deploy was once held because a file under `src/`
carried another agent's uncommitted work-in-progress, and the three services import
straight from the working tree; only a manually run `git status`, done at exactly the
right moment, caught it before a restart would have shipped a half-written edit.

Five steps, always in this order:

1. **The dirty-tree guard.** Any tracked file under `src/` with an uncommitted change
   (staged or not, everything but a brand-new untracked file, which nothing imports
   yet) refuses the deploy outright, naming every file. It does not guess whose work
   it is, since that's a fragile heuristic that could as easily misattribute as help;
   instead it points you at checking for any broadcast naming the file before
   assuming it's abandoned. This refusal is the correct behavior, not an
   inconvenience: a deploy is always recoverable by the operator (commit or stash,
   then re-run), which is exactly why refusing rather than restarting anyway is the
   safe default here, unlike a guard on a live-serving path, where a refusal has its
   own cost.
2. **The migration gate, before anything restarts.** The database's
   `alembic_version` is compared against the latest migration's revision id on disk;
   a match prints `migrations: up to date` and proceeds. A pending revision runs it
   (the same `osiris migrate` machinery) and prints what it applied; a failed upgrade
   refuses the whole deploy outright, nothing restarts, nothing is left
   half-migrated. This closes a real near miss: a deploy once restarted `osiris-mcp`
   onto code expecting a column the database didn't have yet, and only reported the
   pending migration afterward, a window where new code ran against the old schema,
   surviving only because those particular writes happened to be fail-open. A deploy
   is now atomic from the schema's point of view: the restart simply never happens
   until the schema underneath it is current, or the deploy refuses and says why.
3. **Restart** `osiris-mcp`, `osiris-worker`, `osiris-console` (`systemctl --user
   restart`).
4. **Smoke test**, per-surface (the same probe `osiris smoke` runs).
5. **The composition count check, verified by comparison, never assumed.** The
   database's actual composition count is compared against `DEFAULT_COMPOSITIONS`'s
   own length; a shortfall prints exactly what to run (`osiris seed`) rather than
   leaving you to guess whether it happened.
6. **The deploy snapshot.** Only on a green deploy (smoke came back clean, a real
   HEAD was recorded): pins `~/.local/bin/osiris` at a worktree checked out to the
   deployed commit (`scripts/update_deploy_snapshot.sh`), separate from the main
   checkout; see
   [`DEPLOY.md`](DEPLOY.md#the-deploy-snapshot-localbinosiris-never-runs-a-gates-candidate-tree)
   for why. This step never runs on a smoke failure; the checkout keeps running
   `osiris deploy` itself either way, only the operator-facing shortcut moves.

### Two classes of deployed surface

Not every change is gated by a restart, and `osiris deploy` says so plainly rather
than implying a hold protects everything:

- **Restart-deployed**: the long-running services (`osiris-mcp`, `osiris-worker`,
  `osiris-console`) import straight from the working tree at process start. A held,
  uncommitted change here is genuinely inert until the next restart; the dirty-tree
  guard above is what makes that hold meaningful.
- **Commit-deployed**: a `Type=oneshot` systemd timer (`osiris-preflight`,
  `osiris-backup`) reads its script fresh off disk at every fire, independent of any
  long-running process. There's no restart to gate here, and no hold protects it:
  whatever's on disk (committed or not) is already effectively live at the script's
  next tick. `osiris deploy` detects a dirty file backing one of these units (derived
  from parsing `deploy/*.service`, never a hardcoded list) and names it as an
  informational note, not a refusal; refusing a restart that has no bearing on the
  surface in question would be pointless. For these, review has to happen before the
  commit, not after.

## `osiris merge <dupe> <into> --evidence <text> [--actor <who>]`

The command-line equivalent of `orchestrator.merge.merge`, the same underlying
function the `merge` MCP tool wraps, with no softened checks. Type is read off
`dupe`'s own form (`agent:...`/`seat:...`/else defaults to `SoftwareProject`); every
refusal the original merge verbs had is reachable unchanged. This exists for a live
client whose deferred-tool index is stale after a deploy, or a worker whose sandbox
restricts it to an installed entrypoint and refuses a raw `DATABASE_URL` script. The
merge event and `same_as` link (the same receipt surfaces the MCP wrapper prints, for
SoftwareProject merges only, matching the MCP tool's own conditional) are queried and
printed here too, so both entry points onto this function return the same receipt,
never a weaker one.

**Renamed from `fold-project`.** `fold_project` was retired as an MCP tool in favor of
`merge`/`unmerge` for findability, and the CLI had kept the old name: two parts of the
same system using different words for one action. `osiris fold-project <dupe> <into>
--evidence <text>` still works, identical arguments, SoftwareProject-only: a hidden,
deprecated alias that prints a one-line pointer to `merge` on every call. It's never
advertised in the front-door listing, but it's never silently broken either.

## `osiris unmerge <dupe> --because <text> [--actor <who>] [--execute]`

The command-line equivalent of `orchestrator.merge.unmerge`, the same function the
`unmerge` MCP tool wraps. Reverses a wrongful `merge`; type is read off `dupe`'s own
form, same rule as `merge`. **Dry run is the default**, matching the MCP tool's own
convention exactly: without `--execute` this only prints the reversal plan (nothing
written); review it, then re-run with `--execute` to apply it. Built alongside
`merge`'s own CLI rename, since the two verbs are a pair on the MCP side and had no
reason to stay asymmetric here.

## `osiris charter-for <seat> --repos a,b,c --because <text> [--actor <who>]`

The command-line equivalent of `charter.charter_for`, with the same guard as the MCP
tool, fully enforced (the `managed_by`/operator-actor check is the whole point of this
command, never softened here). `--repos` is the whole charter, not an increment;
`--actor` must be the seat's own manager or an operator actor (defaults to `console`,
already an operator actor), or it refuses by name.

## `osiris amend-practice <ref> <amendment> [--actor <who>]`

The command-line equivalent of `capture.amend_practice`: narrows a live practice's
guidance without touching its id, its `statement` (the idempotency key), or its
witness count. Refuses on a refuted practice or a blank amendment, the same as the MCP
tool. Folded directly into `practices()`'s own live listing, not left write-only.

## `osiris annotate-thread <ref> <note> [--actor <who>]`

The command-line equivalent of `capture.annotate_thread`, the same function the
`annotate_thread` MCP tool wraps. Appends to a thread's record without closing it;
`summary`/`status` are never touched. `ref` matches a Thread's UUID, canonical name,
short-id prefix, or summary substring, regardless of the thread's own
open/resolved/deferred status.

This was a known gap before it was built: `charter-for`'s own docstring already
listed this command (alongside `amend-decision`) as sharing `fold-project`'s shape,
shipped, deployed, and invisible to a fleet client whose deferred-tool index was
frozen, but only `fold-project`/`charter-for`/`amend-practice` had a command-line
equivalent built. It calls the orchestrator function directly with an explicit
`--actor`, the same reason those three do: a real loss of provenance otherwise, not a
generic "session" bucket.

## `osiris amend-decision <ref> <addendum> [--actor <who>]`

The other half of that same pair: the command-line equivalent of
`capture.amend_decision`. Appends reasoning to a live decision as understanding
develops, without superseding it; `summary`/`rationale`/`kind` are never touched.
Refuses when `ref` resolves to a decision already superseded (amend the successor
instead, or use `record_decision(supersedes=...)` for an actual correction).

## `osiris mint-seat <handle> [--manager <seat>] [--project] [--model] [--actor] [--adopt] [--force]`

The command-line equivalent of `mintseat.mint_seat`, a different shape of gap than
the commands above: those exist because a client's tool index can go stale; this one
exists because the `mint_seat` MCP tool has no `manager` parameter at all. It infers
the manager from the calling agent's own held seat (the calling seat is always the
manager; minting into someone else's organization is deliberately a console-only
action). A raw terminal has no mounted agent identity to infer from.

**`--manager` is inferred, not required**: omit it and this command looks for the sole
existing seat in the current directory's own pinned project and uses it. Zero or
several candidate seats refuses loudly, naming what was tried and how to disambiguate,
never a silent guess. There is no `--house`/`--project` flag for the new seat's own
project here: a worker's project is never a second, independently given value, it
comes from its manager's own project by construction, so naming `--manager` explicitly
is the only way to mint into a project the current directory isn't already pinned to
(this works automatically since `--actor` already defaults to an operator actor).
`--project` still exists, but stamps a different thing: the new seat's own `.osiris`
pin (never invented if omitted; declare it later with `charter(repos=[...])` once it
actually governs one), distinct from which project the seat itself belongs to.

One call does all of it: `ensure_seat` plus the seat-directory scaffold (directory,
`.osiris` pin carrying project and model, `CLAUDE.md` and `charter.md`), an
`intended_model` stamp, and the `managed_by` edge to the manager. Idempotent: a handle
that already names a living seat is adopted (fill-missing-only), never duplicated;
`--adopt` states that intent explicitly (refuses on no match rather than silently
minting fresh), `--force` is the only way past a near-miss handle refusal. Both are
deliberate console-only options, stated explicitly in `mint_seat`'s own MCP docstring:
an agent caller can never reach them, on purpose, since an ordinary coordinator's mint
never needs to refuse-instead-of-adopt or force past a safety guard. The receipt
prints `mint_seat`'s own occupancy-aware next step: if the seat is vacant, it names
the exact `launch(target=...)` call to start it next.

This closes a real gap: before this command existed, standing up a brand-new seat
from a terminal had no path but a hand-rolled `python -c` heredoc against the live
database, exactly what this CLI is built to prevent.

## `osiris backfill <target> [--apply] [--because R] [--only-bases ID,...] [--limit N] [--newest-first]`

The command-line equivalent of one of seven repair targets (identity/provenance
backfills), calling the same `orchestrator.backfill.run_backfill` that the `backfill`
MCP tool and the UI's Repairs panel call. Dry run is the default for every target;
`--apply` writes and requires `--because`.

## `osiris composition <list|run|run-spec|save> [name] [--spec JSON] [--json]`

Mirrors the `composition` MCP tool for `list`/`run`/`save`; `run-spec` is a CLI-only
fourth mode straight onto `compositions.run_spec` for an ephemeral, never-saved spec.

## `osiris backup-settings <get|write> [--vault-path P] [--timer UNIT=ONCALENDAR]... [--offload-add NAME --offload-kind local|restic --offload-target PATH_OR_URL --offload-schedule ONCALENDAR [--offload-mountpoint P] [--offload-disabled]] [--offload-remove NAME] [--timer-schedules JSON] [--offbox-repositories JSON] [--because R] [--ruling REF]`

Reads or writes the backup configuration: the vault path, the five backup timer
schedules, and offload targets. `write` requires `--because`.

`--vault-path` must always resolve to a location on this machine: validated as
absolute, existing, and writable, and refused outright if it doesn't resolve onto a
mount point declared as always-present in the system configuration. The vault must
survive a reboot unattended. Sitting on the same disk as the system drive only
produces a warning, never a refusal.

`offload_targets` replaces an older, deprecated field kept readable for one more
release; a read fills in `offload_targets` automatically from the old rows when the
new field was never explicitly set. Each target is `{name, kind: local|restic,
path_or_url, expected_mountpoint (local only), schedule, enabled}`, describing an
intermittent target such as a drive only present when docked, or a network drive only
reachable on the local network, not just a single address. `--timer`/`--offload-add`/
`--offload-remove` are convenient command-line-only flags: they read the current
settings first and merge in just the named unit or target, so a one-line change
doesn't require re-typing the whole field. `--timer-schedules`/`--offbox-repositories`
(raw JSON, full replace) remain for scripted bulk writes.

## `osiris backup-status [--vault P] [--backups P] [--json]`

A live health check: timer schedules and live service state, vault dump and
base-backup counts, disk headroom, prune schedule details, and plan status. Called
directly rather than through the generic composition command, since no other path
passes through its own `--vault`/`--backups` path overrides. `--vault`/`--backups`
override the production paths, for a test or a non-standard setup.

## `osiris soul-key <status|init|rotate|restore-drill|enroll-recovery|recover> [flags]`

Mint, inspect, rotate, or recover the soul-store encryption key, and drill an off-box
backup's own restorability. Full mechanism, custody ladder, every flag, and exact
terminal output for each action: [`KEYS.md`](KEYS.md#everyday-operation).
`status`/`init`/`rotate`/`restore-drill` are also exposed over the console's own
network routes (reachable only from the local machine). `enroll-recovery`/`recover`
stay command-line only by design: no action here is ever exposed to an automated
agent.

```
osiris soul-key init
osiris soul-key init --restart
osiris soul-key status
osiris soul-key enroll-recovery
osiris soul-key rotate
osiris soul-key rotate --finish
osiris soul-key recover
osiris soul-key restore-drill
```

## `osiris restic-key <status|init> [--path P] [--backend host-cred|host+tpm2|file] [--json]`

The offload runner's own credential command: a separate secret from the soul key,
protecting the restic repository's own encryption rather than the soul store.
`status`/`init` only; no `rotate`/`enroll-recovery`/`recover` yet. Rotating a restic
password additionally needs a live pass against the repository itself, a deliberate
scope cut for now. See [`KEYS.md`](KEYS.md#the-restic-password).

```
osiris restic-key init
osiris restic-key status
```

## `osiris offload-runner tick [--vault P] [--json]`

Runs one pass of the opportunistic offload runner over every enabled offload target:
an absent `local` target (for example, an unplugged drive) is skipped silently, while
a present `local` target or any `restic` target gets a real backup attempt
(initializing the repository first if it looks uninitialized). Writes a per-target
result (`last_successful_offload`/`last_attempt_at`/`last_error`) that `osiris
backup-status` reads back. Meant to run automatically every 15 minutes, but safe to
run by hand any time. Refuses with one clear error, doing nothing per-target, if
`osiris restic-key init` was never run. See
[`BACKUP.md`](BACKUP.md#the-opportunistic-offload-runner).

## `scripts/osiris_prune_ladder.py [--manifest | --apply-if-clear | --apply]`: the retention schedule

Not an `osiris` subcommand yet: a standalone script, run by hand or by a weekly pair
of scheduled tasks. Thins the database dump population in `backups/` and the vault,
plus the vault's own base backups, write-ahead log archive, and transcript chains,
using a classic thinning pattern: every survivor inside 48 hours stays whole, 48 hours
to 30 days thins to one per day, 30 days to a year thins to one per week, and beyond
that thins to one per month. A dry run is always the default: a bare invocation only
prints what would be removed, `--apply` actually deletes what it just printed, and the
weekly pair instead uses `--manifest` (sends the identical dry run for review) then,
about 20 hours later, `--apply-if-clear` (applies only if that plan is still marked
current, meaning it was given in advance and not since marked stale). This is a
distinct mechanism from `osiris retention` (which thins database tables and has its
own independent `--execute` gate); this script only ever touches files on disk.

**Dormant seat transcripts.** A re-minted worker seat can carry a large resumable
transcript sitting under its own identity directory that nothing revisits once the
seat's successor takes over. `--manifest`/`--apply-if-clear` group the same
transcript-cache-prune population (dead: no file activity past `--dead-after-days`,
default 30, and fully captured by the soul store, the identical safety test every
other session-cache prune already uses) by which seat's own identity directory
produced it, naming each dormant seat and its total size in the mailed plan, for
example `worker-name: 3 file(s), 57.2 MB`. This is a summary for review, never a
separate deletion path: the underlying files apply, or don't, exactly as the flat
transcript-cache-prune population already did, behind the identical plan-then-mark-
stale gate, never outside it. `--seat-root`/`--projects-root` override the default
seat and transcript directory locations, test-only flags never set in production.

## `osiris digest [--hours N] [--mark-seen] [--json] [--text]`

The command-line equivalent of `fleet_digest`: the operator's view into the autonomous
fleet (roster/health, activity, the danger map, laundering, spend,
obligation_pressure). Omitting `--hours` matches watermark mode: what's new since the
operator last looked, never advancing the watermark unless `--mark-seen` says so
explicitly. `--text` prints the raw human-readable render with no `--json`, for a
script or a hook.

## `osiris settle [--decisions JSON] [--threads-open JSON] [--threads-resolve JSON] [--repo-path P] [--standing-orders unchanged] [--because R] [--json] [--text]`

The command-line equivalent of the `settle` MCP tool, called over the wire: the
end-of-context checkpoint. No args means the read-only completeness-check surface.
Each of `--decisions`/`--threads-open`/`--threads-resolve` is a JSON array of that
verb's own keyword arguments (`[{"summary": "...", "is_handoff": true}]` records a
handoff decision, for instance), the same convention as `osiris composition run-spec`'s
own `--spec`. Built so `scripts/osiris_hook.py`'s PreCompact fallback has a real
subcommand to record a machine-generated handoff decision through.

## Why this CLI exists

`osiris` is one of exactly two ways into this system, the other is the MCP tool
surface an agent uses. Both exist so that "how do I do X" always has a documented,
honest, idempotent answer instead of a hand-rolled one-off. If you find yourself
reaching for a raw `psql` session, a bare `systemctl` invocation, or a `python -c`
heredoc against the live database, stop: that's a gap in this surface, not a reason to
route around it. See [`../ARCHITECTURE.md`](../ARCHITECTURE.md) for how the graph
itself is structured, and report any gap you find.
