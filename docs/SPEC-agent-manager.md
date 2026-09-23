<!-- topic: concepts -->

# SPEC: Osiris as Terminal / Agent Manager (mind, body, and interface layers)

**Status:** Design locked, revision 2. A design session on 2026-07-14 settled every question
left open in revision 1: hands, substrate, availability, sequencing, the interface's container,
and the long-term goal. Implementation starts at Phase 0, end to end.

**Authority:** this file is the implementation blueprint. The reasoning behind each choice lives
in the project's decision graph, indexed in §9; read that record (`consult_canon` / `search`)
before coding, and call `record_decision` as you build. This file summarizes that record; it
never replaces it.

> This file is a working blueprint for implementation planning, not a full knowledge dump (see
> CLAUDE.md #10, which asks that knowledge live in the graph rather than in markdown files).
> When a decision changes, record it in the graph first, then update this file to match.

---

## 0. The problem this solves

Osiris currently runs as roughly 16 terminal tabs in Warp, each holding one `claude` session in
a repository. Three related problems, one root cause:

1. **A crashed process took down the whole fleet.** A 14.6 GB headless-Chrome child process died
   inside Warp's systemd scope, and the out-of-memory killer took down the entire scope: every
   session, with all its in-flight work, gone at once. Osiris agents have durable memory (the
   graph) but their runtime processes borrow the desktop's own resource cgroup, so an unrelated
   process crashing can kill them all.
2. **A reboot or crash means a manual restart.** The process and the agent's identity are the
   same thing in Warp today; kill the tab and the agent is gone. Nothing restarts it.
3. **A restarted model session leaves orphans.** When a safety fallback, a fork, or a model swap
   starts a new session id, the terminal keeps tracking the old id. Whoever was talking to that
   session loses the thread, because the tab is now pointed at nothing.

The underlying bug, confirmed by field diagnosis, is that `path = project = identity`: the
current working directory string is used to reconstruct an agent's identity after the fact, by
guesswork, in both the terminal harness and in Osiris itself. That guesswork is what causes
stale identity references and one case of a resumed session mounting into another session's
role by mistake.

**Scale target:** the goal is to take Osiris from roughly 20 parallel projects today to 200 or
more, on a flat subscription instead of per-token spend that would otherwise scale into six
figures, because a human reviewing and directing many agents in parallel is what makes that
economical. Build for that target, not just for near-term relief of the crash problem above.

---

## 1. The design principles (settled 2026-07-14, and these outrank convenience)

1. **Process lifecycle is explicit and owned, not implicit.** The earlier assumption that "no
   component owns agent lifecycle" turned out to be false: something was always managing it
   (Warp, the harness, manual restarts), and every lifecycle-related bug, including a runaway
   process count that grew into the hundreds, grew in the gap left by that lack of ownership. A
   manager daemon now owns process lifecycle explicitly: every process it starts is metered
   (usage receipts), capped (spend and resource limits), started cold by default (nothing
   restarts a process automatically without an explicit attach or a warm-start flag), and every
   lifecycle action is logged and reviewable, never silent or irreversible.
2. **Process backends are provider-agnostic; plain Linux is the default.** Never assume a
   specific hypervisor is available. There is one `BodyProvider` interface, with the calls
   `summon(kind, cores, ram, repo_ref, seat_anchor, budget) -> handle`, `dissolve(handle)`, and
   `receipt(handle)`, implemented by two backends: **local** (the default: transient systemd
   user scopes with hard memory ceilings, metered through cgroup v2's `cpu.stat` core-seconds,
   `memory.peak` ram-seconds, and exit code) and a **microVM backend** (an upgrade path, using
   PVH microVMs, same interface, stronger isolation). Usage receipts have the same shape
   regardless of backend, and no Osiris feature may require the microVM backend to be present.
3. **Availability is treated as critical infrastructure.** Osiris without its graph is
   non-functional, so the manager daemon screams loudly at the OS level (a desktop notification
   over DBus, with or without the visual interface running) if the graph goes down; it never
   silently degrades a panel. Graph redundancy and failover are a planned later addition. The
   manager daemon itself is designed to be the one process that cannot crash the fleet: it holds
   only handles to other processes, never the processes' own resources directly; everything
   heavy it starts runs in its own transient scope with its own ceiling; it restarts
   automatically (`Restart=always`), is protected from being killed first under memory pressure
   (`OOMScoreAdjust`), reserves memory for itself (`MemoryLow`); and the daemon dying is a
   recoverable, expected event: all state can be reconstructed from the graph, the usage
   receipts, and the running processes themselves (which keep running even if the daemon
   restarts, and get re-adopted when it comes back).
4. **No stopgaps; the unified interface is the real deliverable.** Interim relief measures are
   explicitly rejected as the goal. `osiris attach` exists as a debugging tool, never as the
   intended way to use the product. The unified interface that replaces the current
   tab-per-session Warp setup is what "done" means here.
5. **The long-term goal is a reusable protocol, not just this product.** The intended end state
   is a provider-agnostic set of memory primitives for AI agents, sometimes described as "shared
   memory infrastructure for agents": provenance-graded recall, identity and lineage tracking,
   graded mail, leased obligations, and usage metering, built first for Osiris and only later
   generalized for other tools to adopt. Osiris is the reference implementation, not a closed
   platform. It is not competing with other AI coding tools' own desktop interfaces; those are a
   possible future integration target, not a rival to build against now. Support for other AI
   harnesses is a later, separate effort; the near-term work targets Claude Code specifically.
6. **The interface is a single container.** One web application, wrapped in a thin native shell,
   with the browser itself available only as a token-gated fallback. Details in §5.

---

## 2. Architecture: three concerns, three separate bottlenecks, not one shared language

| Concern | Owner | Language | Why |
|---|---|---|---|
| **Mind** (graph, provenance, identity, mailbox, metering, spend ceilings, compositions, background miners, the manager daemon's own logic) | Osiris (this repo) | **Python, staying 100%** | This layer is IO-bound, and its ~900-test suite is the real asset. Porting it later, if ever needed, is an option kept open by that test suite acting as a spec, not something to pursue now. |
| **Bodies** (process backends, lifecycle, network boundaries) | `BodyProvider` backends: local systemd scopes (default) / microVM backend (upgrade) | Python client / **Rust for the microVM backend** | See design principle 2. |
| **Interface** (the unified cockpit that replaces the current tab-per-session setup) | New component | **Web application in a thin Rust shell** | See design principle 6 and §5. |

Warp itself is the counter-example to "just rewrite everything in Rust": Warp is written in
Rust and still went down in the crash described in §0, because it bundled its own Chrome process
that owned the sessions running inside it. The failure was architectural, not a language choice.
The boundary between these three concerns should be a socket, not a shared process.

### The core rule: the interface owns nothing

The **manager daemon** holds the running fleet: it starts processes, holds their handles, and
brokers terminal connections. The **graph** holds the durable truth: who exists, lineage, mail,
spend. The interface is a pure, disposable client, the same as any other process it displays: it
can crash, the machine can reboot, a new interface window can open, and the fleet underneath is
unaffected. Multiple interface windows can attach to the same daemon and graph.

### Two channels inside each terminal session

Inside each terminal session, the user can run whatever they want: Claude Code, Codex, vim. So
the design separates two channels:

- **The terminal channel: universal, dumb, faithful.** A real virtual terminal, with full
  emulation, alternate screen support, mouse support, resize handling, and scrollback. It never
  behaves like a chat widget parsing the output of whatever's running inside it, and makes no
  assumptions about what that program is. This channel works for any program, forever.
- **The graph channel: opt-in enrichment.** The context panel next to the terminal is fed only by
  what the program running inside chooses to report to the graph (mount events, decisions, mail,
  spend), through a harness-specific adapter. A terminal running an unrecognized program still
  gets a working terminal, just with an empty or dim context panel: it's never broken.

The interface never tries to correlate the two channels by reading raw terminal output; any
correlation between "what's happening in the terminal" and "what shows in the graph panel" is
done in the graph itself, keyed by a session identifier assigned at process start.

### Assigning identity when a process starts, not reconstructing it later

Today, a session's identity is reconstructed after the fact by guessing from its working
directory, which is why identity gets attached to a stale path or, in one confirmed case, one
session mounts into another's role. In the new design, **the manager daemon assigns identity at
process start**: it mints the session's durable identifier and exports it into the new process's
environment before the harness inside it does anything at all. Guessing based on the current
directory is no longer needed for processes started this way; that guess-based path only remains
as a fallback for processes not started by the daemon.

### Persistent terminal state

The terminal broker keeps each terminal's screen state and scrollback, not just a raw file
descriptor, so a reattaching interface window repaints instantly instead of starting blank.
Detach/reattach behaves like `tmux`; this design adds a graph layer on top of that same idea.

### The three layers don't read each other's writes

The mind layer writes its own facts (decisions, threads, mail); the manager daemon writes only
lifecycle facts (processes, terminal sessions, usage receipts); the interface writes nothing at
all. Each of the three layers owns its own layer of state, and an agent watching its own graph
context next to its own terminal is just a read-only mirror, not a feedback loop writing back
into itself.

---

## 3. Phase 0: process backend, metering, and near-term fixes (buildable now, no new hardware)

### 0.1 The `BodyProvider` interface and the local backend

- Define the interface described in design principle 2. Implement the **local backend for
  real** (it is the default product tier, not a placeholder for tests):
  `systemd-run --user --scope` with hard `MemoryMax`/`MemoryHigh` limits and optional CPU
  pinning; a usage receipt minted from cgroup v2 (`cpu.stat`, `memory.peak`, exit cause), written
  to disk with an fsync when the process is reaped.
- In `src/orchestrator/trigger.py`, add `_spawn_in_body(...)` alongside the existing
  `_spawn_claude`, routing new process starts through the provider interface. The existing wake
  behavior is unchanged: processes stay dark by default, re-arm only within their existing
  scope, and stay metered and capped. Only the underlying process backend changes.
- A stub client for the microVM backend, returning a canned usage receipt, keeps that backend's
  tests passing before the real hardware exists (see §6).

### 0.2 A second metering dimension

- `src/ingest/wake_cost.py`, plus a migration, should record **resource-seconds**:
  `core_seconds`, `ram_seconds`, and `exit_cause`, alongside the existing `total_cost_usd`
  dollar figure (either as a sibling `body_usage` table or as added columns). These should be
  event-dated by the usage receipt, with the same event-sourcing discipline used elsewhere in
  the system. The spend ceiling check should read both dimensions.

### 0.3 Ship named-recipient delivery guarantees early

- `send(to_agent=NAME)` should echo back the resolved recipient and their lineage, and should
  hard-fail if the name doesn't resolve to a claimed identity (`require_seat=true`); `fleet()`
  should print the list of claimed names. This is small and doesn't depend on the manager daemon,
  so it should ship in the current work cycle.

### 0.4 Four small fixes, all confirmed by real incidents

- **False "down" readings in the status line** (`scripts/osiris_hook.py`, the status line's
  current home; an earlier script, `scripts/osiris_statusline.py`, was retired when the status
  line moved to a hook): a 1.0-second connect timeout produces false "down" readings under load;
  the check needs to distinguish "slow" from "actually down."
- **Fix a null-baseline comparison gate** (`src/orchestrator/agents.py` / `forks.py`): a missing
  prior-model value means "no observation yet," not "the model changed"; the seam-detection logic
  should never treat a null value as a comparison baseline.
- **Deduplicate `open_thread` calls across session restarts**: check for a near-duplicate summary
  before minting a new thread, confirmed necessary by two independent incidents.
- **Fix a race condition in identity detection at mount/orient time**, now confirmed by four
  separate incidents (including one case where the same lineage was double-counted at a context
  compaction boundary): `orient()` should be the single source of truth for detecting an identity
  transition; `mount()` must not assert a transition it can't confidently confirm. This should
  also close a related gap: mounting should fail loudly when the stored identity anchor
  contradicts what the calling process claims; an identity anchor is unique to one mind, never
  shared across a tree of sub-processes; a dead session's identity anchor is never handed to a
  live one; and a dead lineage can be inspected but never reassigned to a new holder.

---

## 4. Phase 1: the manager daemon (the core process; the fix for the restart problem)

A new systemd user unit, `osiris-manager`, built from its first commit around the principle in
design principle 3: it holds handles, never processes directly; every process it starts gets its
own transient scope; it re-adopts running processes after a restart; all of its state is
reconstructible.

### 4.1 Decouple identity from file path first: everything else depends on this

- Each identity ("Seat," described below) is keyed on a durable id; its working directory is a
  mutable pointer, not the identity itself.
- **A new `governs` relationship**: `seat -> governs -> [repos]`, meaning a Seat's scope of
  responsibility is defined by which repositories it governs, not by which directory it happens
  to be running in. `orient()` gains a mode scoped to that governed set.
- **A Seat rebind/migration primitive**, which moves a Seat's working-directory anchor while
  preserving its identity, lineage, attribution, and mail history. This is currently blocked on
  this primitive shipping (one real Seat was orphaned by a folder move and needs this to recover).
  A pilot migration is planned for the smallest available case first.
- **Design settled: the Seat becomes a first-class graph object.** A new object type, `Seat`
  (canonical id shape `seat:<uuid8>`), minted exactly once and never re-keyed; natural keys were
  explicitly rejected for it, since both its working directory and its display handle are mutable
  facts, and identity should never be keyed on a mutable fact. It carries assertions for
  `handle`, `house` (which project/team it belongs to), `anchor_cwd`, and `policy`. A `holds` link
  from Agent to Seat names the current holder; when a session hands off to a successor, `holds`
  is re-linked to the successor so the binding follows the active lineage. Holder history is kept
  as a `succeeds_seat` chain. A session's own lineage root was considered and rejected as the
  durable identity, because it is still just a session id, one Seat can outlive multiple lineages
  held in succession, and assigning identity at process start requires an identity that exists
  before the first session even begins. Attribution stays scoped per individual session (never
  merged across holders); a separate existing rule about individual-mind identity is untouched,
  because addressing gets re-keyed onto the Seat specifically because individual sessions end.
  Rollout is staged: **A**, purely additive (schema changes, a `seats.py` module with
  `ensure_seat`, a `seat_tokens` table, `agent_mounts.seat_id`, new attach parameters, and daemon
  export of the identity into the environment); **B**, re-key addressing for mail, leases, and
  orient; **C**, a visitor role distinct from the seat holder; **D**, retire the
  working-directory guess for daemon-managed sessions. Stage A changes nothing for any session
  not using the new environment variables.

### 4.2 Assigning identity at process start

- The daemon starts every managed process with its identity anchor already set in its
  environment (see §2). The older guess-based path remains only for processes not started by the
  daemon.
- **The attach handshake**: the process that starts a session resolves or mints its Seat, mints a
  one-time attach token, and exports `OSIRIS_SEAT_ID` and `OSIRIS_ATTACH_TOKEN` into the new
  process's environment before anything else runs. The harness-specific adapter then presents
  `(session, seat, token)` back to the server, which verifies it and binds them together
  (updating `agent_mounts.seat_id` and the `holds` link); the token is then marked used. If a
  different session ever tries to present the same token, that is refused loudly: this closes a
  real leak where sub-processes inherit their parent's environment variables (the same class of
  issue previously found with `CLAUDE_JOB_DIR`). Once a session is bound to its Seat, no token is
  needed to reattach after a restart. Tokens live in a plain, non-append-only table, since they
  are short-lived secrets that need to be revocable.

### 4.3 Seats as first-class, plus a visitor role

- Exactly one claimed Seat per project/team ("house"); sessions attach to a Seat. The roster
  distinguishes the Seat's holder from a visiting session; sessions carrying a `subagent_id` land
  under their parent's sub-tree, never counted as independent project peers.
- Work items are modeled as **single-assignee leased obligations**: attempting a second
  assignment surfaces the existing lease instead of creating a duplicate.

### 4.4 Restarting from the graph's own record

- On startup, the daemon reads the current fleet state from the graph and restarts processes
  according to a **warm/cold policy**, cold by default (see design principle 1; per-Seat warm
  flags are a product decision left to the deployment operator, see §7). The graph itself is the
  restart manifest; manual restarts are no longer needed.

### 4.5 Terminal broker

- The daemon holds terminal sessions along with their screen state (see the tmux-like semantics
  in §2). `osiris attach <seat>` remains a debugging tool; the unified interface is the intended
  product surface (see design principle 4).

### 4.6 Addressing by lineage, not by session id

- Everything is addressed by Seat or by `lineage_head`, never by a raw session id. When a model
  swap or restart moves the lineage head, the interface follows it automatically, and the earlier
  session collapses into history beneath the new head rather than becoming an orphan.

### 4.7 Batch resolution of related threads

- `record_decision(resolves=...)` gains a batch/pattern form, so a single decision can close an
  entire set of threads it supersedes at once, rather than requiring them to be closed one by one.

### 4.8 The alert

- The daemon watches graph health continuously; if the graph goes down, it triggers a DBus
  desktop notification and raises the interface window, loudly and immediately, whether or not
  the interface happens to be open at the time. This should never fail silently (see design
  principle 3).

### 4.9 Keep what already works

- The existing concurrent-edit warning caught a real conflict in practice; keep it, and keep
  extending this general idea of surfacing what's actually true about the current environment in
  real time.

---

## 5. Phase 2: the unified interface (the actual milestone)

- **One web application**: the console's existing HTML views plus a terminal component
  (`xterm.js`). A fleet list, grouped by lineage head with earlier generations collapsed,
  lets the user select a Seat and see a split view: a **live terminal** (the terminal channel)
  next to a **graph context panel** (lineage, graded mail, obligations, decisions, spend against
  the ceiling). The existing `:8011` console views render natively inside this interface; the
  console and the manager become one surface.
- **Primary packaging: a thin Rust shell around the operating system's own webview** (in the
  style of Tauri). This gives the interface a real window identity (a dock icon, a global
  hotkey, OS-level notifications), avoids the background-tab throttling a browser tab would get,
  loads only this application's own UI, and talks to the daemon over a Unix domain socket, with
  no network listener open by default.
- **The browser is a secondary, fallback access path**: reaching the interface through a browser
  tab requires an explicit token, and is intended as a future remote or mobile access path once a
  real authentication design is in place (see §7). Any network exposure gets origin checks and a
  token requirement from the first commit that adds it.
- **A one-week validation step before committing to this packaging approach**: run Claude Code
  fullscreen inside `xterm.js`, inside a WebKitGTK-based webview, and confirm it performs well
  enough. If it doesn't, the fallback is a dedicated Chromium instance in `--app` mode. The
  decision is made based on that test's results.
- Only visibly open terminal sessions are rendered client-side (this keeps the design workable at
  200+ projects); the daemon holds state for everything not currently on screen.
- **Why this avoids Warp's failure mode**: no bundled browser engine, and the interface owns none
  of the underlying processes. Both of the specific problems that took down Warp are absent by
  design.

---

## 6. Phase 3: the microVM backend (an upgrade tier, gated on new hardware becoming available)

- **Contract:** MCP tools exposing `summon`/`dissolve`/`receipt_get`, alongside existing
  hardware-management tools, backed by a JSON-RPC `body.*` interface on the hardware controller.
  Osiris passes it `(kind, cores, ram, repo_ref, seat_anchor, budget)`.
- **Mechanism:** Xen PVH guest domains, booted directly from a kernel and an in-RAM initramfs; a
  golden image is defined as a `(kernel, initramfs, cmdline)` manifest; target boot-to-shell time
  is 1 to 2 seconds. Hard memory limits (`max_memkb`), pinned to efficiency cores via a dedicated
  CPU pool, and no memory ballooning for these ephemeral processes (ballooning is reserved for
  long-lived target machines, a separate concept described below).
- **Usage receipts are written before a process is torn down**: the host (dom0) is the metering
  authority; receipts are fsync'd before destroy, and even a crashed process's death still
  produces one.
- **Network egress:** default-deny; the only allowed destination is a vsock-based channel with no
  IP networking at all. An nftables-based rule is an interim measure until proper
  authentication over vsock ships.
- **Two distinct kinds of guest, never conflated:** long-lived, full-OS target machines
  (ballooned, can be paused and resumed, created via `instance_create`) versus ephemeral,
  never-resumed processes (created via `summon`/`dissolve`). A process runs its work against a
  target machine; the two are always separate.
- **Sequencing:** this phase is gated on new physical hardware becoming available, after which
  the rollout order is: desktop migrated onto the hypervisor, then guest networking, then vsock
  authentication, then a first version of the process backend on this hardware, then a first
  field test running one process, on one target, under one Seat, with a hardware-priced usage
  receipt.

---

## 7. Decisions that need the deployment operator's direct sign-off

- **New physical hardware coming online** unblocks the hypervisor tier described in Phase 3.
- **Warm-vs-cold policy per Seat**: the recommendation is cold by default, with a warm flag
  available for processes that need to stay resident (for example, a watcher process).
- **The pending Seat migration** described in §4.1, once that primitive ships.
- **Remote access authentication design**, required before the interface is ever exposed outside
  localhost.
- **A hardware-backed signing key** (SSH or FIDO2) for fail-closed auto-update.
- **Scope of the graph redundancy work**: the direction is set in design principle 3; the scope
  and timing are a deployment decision for later.

---

## 8. Build order

1. **Phase 0**: the `BodyProvider` interface, the local backend, the second metering dimension,
   named-recipient delivery guarantees, and the four small fixes. These are the first commits;
   after them, every new session already speaks the process-backend interface.
2. **Phase 1**: the manager daemon, starting with decoupling identity from file path (§4.1, since
   the governs relationship, the migration primitive, and the orphaned-Seat fix all follow from
   it). This delivers restart resilience using the local backend, before any new hardware exists.
3. **Phase 2**: the unified interface. The WebKitGTK validation spike happens in week one,
   alongside Phase 0; the full build happens once Phase 1 exposes a stable control surface to
   build against. The interface replacing the current tab-per-session setup is the actual
   milestone for this project.
4. **Phase 3**: point the process-backend layer at the real hardware controller once new hardware
   is available.

Gates before every commit, per CLAUDE.md: `uv run pytest` (testcontainers, real Postgres),
`uv run ruff check src tests`, `uv run mypy src` (`--strict`). This is a shared tree: stage only
your own changes, never `git add -A`, and coordinate with other active sessions before committing.

---

## 9. Supporting decisions (background reasoning, recorded in the project's decision graph)

The full reasoning behind each design principle and phase above is recorded as a set of
individual decisions in the project's own graph (queryable via `consult_canon` / `search`),
covering: the constitutional amendment admitting managed process lifecycle; the provider-agnostic
bodies design and cgroup-based metering; the availability doctrine around graph health and the
manager daemon's own crash-resistance; the "no band-aids" framing of the interface as the real
milestone, with the human's attention as the actual scaling constraint; the interface's container
design (web app, thin native shell, browser demoted to fallback, WebKitGTK spike); the long-term
protocol goal; the original incident analysis of the Warp crash described in §0; the decision to
split mind, body, and interface by concern rather than porting the whole system to one language;
the decision that the console and the manager daemon should be one graph-first surface; the two
distinct guest kinds (targets vs. processes) with opposite lifecycles; the locked-in microVM
mechanism (PVH boot, in-RAM initramfs, dom0 as the metering authority, vsock with no IP); the
original field diagnosis that `path = identity` is the root bug behind Phase 1; the network
egress rule design; the "lineage head is the addressable identity" rule; the identity-race
incident and the anchor-collision bug class that Phase 1 §4.2 closes at the root; and the Seat
identity-core design itself (Seat as a first-class object, the attach handshake, addressing
re-keyed onto the Seat), staged as phases A through D in §4.1.
