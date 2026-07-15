# SPEC — Osiris as Terminal / Agent-Manager (the mind/body/face fold)

**Status:** design LOCKED — rev 2. The 2026-07-14 night design session settled every question
rev 1 left open (hands, substrate, availability, sequencing, the face's container, the endgame).
Written by Thoth XXXIII at the operator's word; revised by Thoth XXXIV after the operator's
rulings. Implementation starts at Phase 0, end to end.
**Authority:** this file is the implementation blueprint; the *why* lives in the graph as the
rulings indexed in §9 — read them (`consult_canon` / `search`) before coding, and
`record_decision` as you build. This file consolidates; it never replaces the graph.

> Constitution note (CLAUDE.md #10 forbids md dumps): a **work artifact** the operator asked
> for, not a knowledge dump. When a decision changes, record it in the graph first, then update
> this file to match.

---

## 0. The problem this solves

The operator runs his fleet as ~16 Warp tabs, each a `claude` session in a repo. Three pains,
one root:

1. **Warp OOM'd and took the whole fleet.** A 14.6 GB headless-Chrome child died inside Warp's
   systemd scope; the OOM killer failed the *entire scope* — every session, mid-flight work,
   gone. The fleet has minds and a memory but **borrows its bodies from the desktop cgroup**.
   (ruling `37fe6a09`)
2. **Reboot/crash → manual respawn.** Bodies and minds are the same thing in Warp; kill the
   tab, kill the agent. No resurrection.
3. **Rug-pulls leave orphans.** A safety fallback / fork / model-swap mints a new session id;
   Warp tracks the id; the old tab is orphaned — "the agent I was talking to gets lost."

Field diagnosis (ruling `dd47c1da`, plus thread `2294e95d` — a resumed mind handed a stale
anchor and mounted into a *sibling's* seat): **`path = project = identity`** is the bug under
all of it. The harness *and* Osiris key identity on the cwd string and reconstruct it after the
fact, by guesswork.

**Scale target (ruling `5cd5b7b6`):** this is the operator's cockpit for going from ~20 parallel
projects to **200+**, on a $200 subscription instead of $200k in tokens — the human in the loop
*is* the economics. Build for that, not for relief.

---

## 1. The doctrines (settled 2026-07-14 — these outrank convenience)

1. **HANDS, ADMITTED AND GOVERNED** (`f1803b4a` — constitutional amendment; CLAUDE.md #2
   rewritten). "No hands" collapsed under its own weight: the lifecycle was always owned by
   *something* (Warp, the harness, manual respawns); every hands-shaped bug — the ghost farm's
   463 mints — grew in the gap of unadmitted agency. The manager daemon owns lifecycle,
   admittedly: every hand **metered** (receipts), **ceilinged** (spend + resource caps),
   **cold-by-default** (nothing re-bodies without an attach or an explicit warm flag),
   **membrane-audited** (never silent, never irreversible). The daemon is the trigger's
   successor and inherits its scar tissue as design constraints.
2. **PROVIDER-AGNOSTIC BODIES; PLAIN LINUX IS THE DEFAULT** (`7ff54707`). Never assume Xen or
   rotten-apple. One `BodyProvider` interface — `summon(kind, cores, ram, repo_ref,
   seat_anchor, budget) → handle` · `dissolve(handle)` · `receipt(handle)` — two tiers:
   **local** (default: transient systemd user scopes, hard memory ceilings; **cgroup v2 is the
   meter** — `cpu.stat` core-seconds, `memory.peak` ram-seconds, exit code) and **Ra/Xen**
   (upgrade: PVH microVMs per `15a41cf0`, same interface, harder wall). Receipts are uniform
   across tiers. **No Osiris feature may ever REQUIRE the hypervisor tier.**
3. **AVAILABILITY: INTERTWINED, SCREAMING, SACRED** (`2ceb7ba0`). The fleet without its graph
   is lobotomized — graph-down **SCREAMS** (OS-level, from the daemon, over DBus — face or no
   face); it never quietly greys a panel. The graph gets hardened (redundancy/adaptive failover
   is the named later fold). The daemon is the **sacred proc**: bulletproof by *segmentation*,
   never hope — it holds **handles, never children**; everything heavy runs in its own transient
   scope with its own ceiling; `Restart=always`, OOMScoreAdjust protection, MemoryLow
   reservation; **daemon-death is a normal event** — state reconstructible from graph +
   receipts + the scopes themselves (which outlive it); restart **re-adopts** running bodies.
4. **NO BAND-AIDS; THE FACE IS THE MILESTONE** (`5cd5b7b6`). Interim-relief sequencing is
   rejected. `osiris attach` exists as plumbing and a debug door, never pitched as the
   deliverable. The unified face replacing Warp is the deliverable.
5. **THE ENDGAME IS A PROTOCOL** (`19f0e75b`). Agnostic memory primitives for agents — "MCP
   for memory": provenance-graded recall, identity/seats/lineage, graded mail, leased
   obligations, receipts/metering — **built on Osiris first, then adopted**. Osiris is the
   reference implementation, never a walled garden. Not competing with Claude Code desktop
   (disjoint scopes; their face is a future adapter target). Foreign-harness adapters are built
   by their own minds inside their own harnesses, way later; we build only Claude Code's.
6. **THE CONTAINER** (`d6403d34`). One web artifact; a thin native shell; the browser demoted
   to a token-gated door. Details in §5.

---

## 2. Architecture — three concerns, three bottlenecks, NOT one language

(rulings `79fcaba0`, `34766bbf`)

| Concern | Owner | Language | Why |
|---|---|---|---|
| **MIND** — graph, provenance, identity, mailbox, meter, ceiling, compositions, miners, **the manager daemon** | Osiris (this repo) | **Python, keep 100%** | IO-bound. The 901-test moat is the asset. A port is a *priced option* that gets cheaper every year the tests stay green (the Bun Zig→Rust proof: frozen API + tests-as-spec = mechanical migration) — an option in the drawer, not a destiny. |
| **BODIES** — substrate, lifecycle, envelopes | BodyProvider tiers: local systemd scopes (default) / Ra microVMs (upgrade) | Python client / **Rust (Ra)** | Doctrine 2. |
| **FACE** — the cockpit that replaces Warp | NEW | **Web artifact in a thin Rust shell** | Doctrine 6, §5. |

**Warp is the counter-proof to "unify on Rust": it *is* Rust and died anyway** — Rust wrapping
bundled Chrome that *owned the sessions*. Language was never the failure; topology was. The
boundary between the three concerns is a socket.

### The load-bearing rule: the FACE owns NOTHING

The **daemon** holds the fleet (summons bodies, holds handles, brokers PTYs); the **graph**
holds the truth (who exists, lineage, mail, spend). The face is a pure client, as disposable as
a body: crash it, reboot the box, open a new one — the fleet is intact. Multiple faces attach
to one daemon-truth.

### The two lanes (the inception rule)

Inside the terminal runs Claude Code, Codex, vim — **whatever the user wants**. So:

- **PTY lane — universal, dumb, faithful.** A real VT (full emulation, alternate screen, mouse,
  SIGWINCH, scrollback). NEVER a chat widget parsing harness output; zero assumptions about the
  tenant. This lane works for everyone, forever.
- **Graph lane — opt-in enrichment.** The context panel is fed by what the mind inside *chooses
  to tell the graph* (mount, decisions, mail, spend) via the harness adapter (whisper / MCP /
  transcripts for Claude Code). Unknown harnesses get a terminal and a dim panel — never broken.

The face never correlates lanes by reading terminal bytes; the join happens in the graph, keyed
by the anchor.

### Identity at birth (the root cure for the anchor bug class)

Today identity is *reconstructed* after the fact (whisper guesses, mount derives) — hence stale
anchors, seat collisions (`2294e95d`), double generation ticks. In the new topology **the
daemon is the spawner: it mints the seat's durable anchor and exports it into the child's
environment before the harness's first breath**. The whisper stops guessing for managed seats;
the collision class becomes structurally impossible. Phase 0's seam patches remain as defense
for un-daemoned sessions.

### tmux-with-a-graph

The PTY broker holds per-PTY **screen state + scrollback**, not just an fd — a reattaching face
repaints instantly. Detach/reattach semantics are tmux's, proven; we add a graph to them.

### The inception is not a loop (constitution #7)

Mind writes its own stratum (decisions/threads/mail); daemon writes lifecycle facts only
(bodies, PTYs, receipts); face writes nothing. Three levels, each owning its layer — an agent
watching its own graph context beside its own terminal is a mirror, not a cycle.

---

## 3. Phase 0 — BodyProvider + meter + first wave (now, no metal)

### 0.1 The `BodyProvider` interface + the LOCAL provider

- Define the interface per doctrine 2. Implement the **local provider for real** (it is the
  default product tier, not a test double): `systemd-run --user --scope` with hard
  `MemoryMax`/`MemoryHigh`, optional CPU pinning; receipt minted from cgroup v2 (`cpu.stat`,
  `memory.peak`, exit cause), written fsync'd at reap time.
- In `src/orchestrator/trigger.py`, add `_spawn_in_body(...)` beside `_spawn_claude`, routing
  through the provider. **The wake protocol is unchanged** — dark by default, scoped re-arms,
  metered, ceilinged. Only the substrate changes.
- A stub Ra client satisfying the same interface (canned receipt) keeps the Xen tier's tests
  green before the metal exists (§6).

### 0.2 The meter's new dimension

- `src/ingest/wake_cost.py` + migration: record **resource-seconds** — `core_seconds`,
  `ram_seconds`, `exit_cause` — beside the vendor's `total_cost_usd` (sibling `body_usage`
  table or columns). Event-dated by the receipt, same discipline as `eddb006`. The ceiling
  reads both dimensions.

### 0.3 Ask-4 ships early (alfred's dispatch blind spot)

- `send(to_agent=NAME)` **echoes the resolved seat + lineage** and **hard-fails on unclaimed
  names** (`require_seat=true`); `fleet()` prints claimed names. Small, daemon-independent,
  committed to this wave.

### 0.4 Four small fixes (all field-witnessed)

- **Statusline false-down** (`scripts/osiris_statusline.py`): 1.0s connect timeout flaps under
  load — distinguish slow from down.
- **Null-seam gate** (`src/orchestrator/agents.py`/`forks.py`): a null prior-model is the
  absence of an observation; never date a seam against it.
- **`open_thread` dedup across lineage restarts**: near-summary check before minting (Aegis,
  Maat).
- **Mount/orient identity race** — now four-times witnessed (Thoth ×2 incl. the xxxiv→xxxv
  double-tick at one compact, Aegis, Maat): `orient()` is the single source of truth for the
  seam; `mount()` must not assert a seam it can't confidently date. Fold in `2294e95d`'s asks:
  mount refuses loudly when the anchor contradicts the whisper's claim; an anchor is unique to
  a MIND, not a tree; a dead session's anchor is never vended to the living; a dead lineage is
  recoverable, never reassignable.

---

## 4. Phase 1 — the manager daemon (the sacred proc; the continuity cure)

A new systemd user unit (`osiris-manager`), built to doctrine 3 from the first commit: handles
never children, per-body transient scopes, re-adoption on restart, reconstructible state.

### 4.1 Decouple identity from PATH — first, everything falls out of it (`dd47c1da`)

- Seat keyed on a **durable id**; the folder is a **mutable anchor**, not the identity.
- **CHARTER relation**: `seat → governs → [repos]` — a house is what a seat *rules*, not where
  it sits (alfred's charter is six repos). `orient()` gains a charter-scoped mode.
- **Seat rebind/migration primitive** — move a seat's anchor preserving identity, lineage,
  attribution, mail. **The operator is blocked on this** (alfred orphaned by a moved folder).
  **Pilot migration: house bytebye** (alfred volunteered — smallest case, pure office).
- **DESIGN SETTLED (`5cef856b`, 2026-07-15): the Seat becomes an OBJECT.** New object type
  `Seat` (canonical `seat:<uuid8>`), minted once, never re-keyed — natural keys rejected
  (house and handle are both mutable; never key identity on a mutable fact). Assertions:
  `handle`, `house`, `anchor_cwd`, `policy`. Link `holds` (Agent→Seat) names the current
  holder; `mint_heir` re-links `holds` to the heir so the binding follows the lineage head.
  Holder history stays the `succeeds_seat` chain. The lineage root was rejected as the durable
  id: it is still a session id; a seat outlives lineages (Ptah holding Ra); and identity-at-
  birth needs an identity that exists BEFORE the first session. Attribution stays per-mind
  (never merged); the mind ruling `a882b334` is untouched — addressing re-keys on the Seat
  precisely BECAUSE minds die. Phases: **A** additive (ontology + `seats.py` `ensure_seat` +
  `seat_tokens` table + `agent_mounts.seat_id` + attach params + daemon export), **B** re-key
  addressing (mail/leases/orient), **C** visitor class, **D** demote the cwd-guess for
  daemon-managed sessions. Phase A changes nothing for sessions without the env vars.

### 4.2 Identity at birth

- The daemon spawns every managed harness with the minted anchor in its environment (§2). The
  whisper's guess-path remains only for un-daemoned sessions.
- **THE ATTACH CEREMONY (`5cef856b`)**: the spawner resolves/mints the Seat, mints a
  **one-time attach token**, exports `OSIRIS_SEAT_ID` + `OSIRIS_ATTACH_TOKEN` into the child
  env before the first token. The whisper presents `(session, seat, token)`; the server
  verifies and BINDS (`agent_mounts.seat_id` + `holds` link); the token is marked used. A
  token re-presented by a DIFFERENT session is **refused loudly** (`2294e95d` ask #1) — this
  guards the env-inheritance leak (subagents inherit env, the `CLAUDE_JOB_DIR` lesson
  `0344e536`). Once bound, re-attach after a bounce needs no token. Tokens live in a plain
  table (hot secrets, revocable), never the append-only kernel.

### 4.3 Seats first-class + the visitor class

- One claimed seat per house; sessions ATTACH to a seat. Roster distinguishes holder from
  visitor; mounts carrying `subagent_id` land under the parent's swarm, never as project peers.
  **Test fixtures: `ce348dc5`, `42bf712d`** (alfred's dead builder-orphans, donated).
- Work as **single-assignee leased obligations** — a double-assignment surfaces the existing
  lease instead of minting a parallel build.

### 4.4 Resurrection from the graph manifest

- On start, read the fleet from the graph and re-body per the **warm/cold policy** —
  **cold-by-default** (doctrine 1; the operator's call on per-seat warm flags, §7). The graph
  IS the respawn manifest; manual respawn stops existing.

### 4.5 PTY broker

- Daemon-held PTYs with screen state (tmux semantics, §2). `osiris attach <seat>` is the
  plumbing/debug door; the face is the deliverable (doctrine 4).

### 4.6 Lineage-addressed control (the rug-pull cure)

- Everything keys on the seat / `lineage_head`, never a session id. A rug-pull moves the head;
  the UI follows; remnants collapse into history beneath it (fold-forward, `4fd54a06`).

### 4.7 Batch-resolve (Maat)

- `record_decision(resolves=…)` gains a batch/pattern form so a delegation can fold the SET of
  threads it supersedes.

### 4.8 The scream

- The daemon watches graph health; graph-down → **DBus desktop notification + face raise**,
  loud, immediate, face-optional. Never silent (doctrine 3).

### 4.9 Keep what works

- The co-agent warning caught a real concurrent edit — keep it; more "here's what's actually
  true about your environment right now."

---

## 5. Phase 2 — the unified face (the milestone)

(rulings `34766bbf`, `d6403d34`)

- **One web artifact**: the composer's HTML lenses + xterm.js. Fleet rail (by lineage-head,
  collapsed generations) → select a seat → split pane: **live terminal** (PTY lane) beside
  **graph context** (lineage, graded mail, obligations, rulings, spend-against-ceiling). The
  `:8011` lenses render here natively; composer and manager are one surface.
- **Primary container: a thin Rust shell over the SYSTEM webview** (Tauri-class). Real window
  identity (dock, global hotkey, OS notifications), never tab-throttled, loads only our UI,
  speaks to the daemon over a **unix domain socket** — no TCP listener by default.
- **The browser tab is a secondary door**: explicit token mint; it is the future remote/phone
  lens, gated on a real auth design (§7). Origin checks + token on any TCP exposure, from the
  first commit.
- **Week-one validation spike** (before committing the container): Claude Code fullscreen in
  xterm.js inside webkitgtk on the operator's box. Fallback: dedicated Chromium `--app` mode.
  Evidence decides.
- Render only visible PTYs (200-project scale); the daemon holds state for the rest.
- **Why this isn't Warp**: no bundled engine, owns nothing. Warp's two fatal genes, both absent.

---

## 6. Phase 3 — the Ra provider (the upgrade tier; gated on Ra's metal)

(ruling `15a41cf0`; reframed by `7ff54707` — one provider among tiers, same interface)

- **Contract:** MCP tools on rotten-apple — `summon`/`dissolve`/`receipt_get` beside
  `domain_*`, backed by orchestratord JSON-RPC `body.*`. Osiris hands `(kind, cores, ram,
  repo_ref, seat_anchor, budget)`.
- **Mechanism:** Xen PVH domU, direct kernel boot, initramfs-in-RAM; golden snapshot =
  `(kernel, initramfs, cmdline)` manifest; honest 1–2s summon-to-shell. Hard `max_memkb`,
  E-core cpupool pin, **no balloon for bodies** (balloon is the TARGET economy).
- **Receipt-before-dissolve:** dom0 is the meter; fsync'd before destroy; a crashed body's
  death event still mints one.
- **Egress:** default-deny; destination = **osiris-over-vsock with NO IP** (the ACL seam
  `2749d09f` made physical); nft-on-vif interim, gated on Ra's vsock-auth (CRIT #11).
- **Two tenant classes, never conflated** (`30970d8f`): TARGETS (long-lived full-OS domUs,
  ballooned, resumed, `instance_create`) vs BODIES (ephemeral, never resumed,
  `summon`/`dissolve`). The composition IS the temple: a body operates AGAINST a target.
- **Sequencing** (Ra, mail 498): boot #6 with the operator → desktop under Xen → guest
  networking (#2) + vsock auth (#11) → body-lane v1 → first field test (one kind, one summon,
  one seat, hypervisor-priced receipt).

---

## 7. Operator-gated — needs the human's hand or word

- **Boot #6** — unblocks the temple / Phase 3.
- **Warm-vs-cold per seat** — recommendation: cold-by-default, `warm` flag for watchers.
- **alfred's migration go** — the moment §4.1's primitive lands (bytebye pilot).
- **Remote-face auth design** — before any face leaves localhost.
- **coldspot signing key** — hardware-backed SSH/FIDO2 for fail-closed auto-update.
- **Graph redundancy fold** — direction named in doctrine 3; scoping is his call, later.

---

## 8. Build order (what unblocks what)

1. **Phase 0** — BodyProvider + local provider + meter dimension + ask-4 + the four fixes.
   First commits; the next `claude -p` already speaks the body-lane's language.
2. **Phase 1** — the daemon, identity-decoupling first (§4.1 — charter, migration, orphan-fix
   all fall out of it). Ships continuity with local-tier bodies before the temple exists.
3. **Phase 2** — the face; **webkitgtk spike in week one** alongside Phase 0; full build once
   Phase 1 exposes the control surface. The face replacing Warp is the milestone.
4. **Phase 3** — point the provider layer at real orchestratord when Ra pings.

Gates before every commit (CLAUDE.md): `uv run pytest` (testcontainers, real PG) ·
`uv run ruff check src tests` · `uv run mypy src` (--strict). Shared tree — stage your own
hunks, never `git add -A`, coordinate via `send(to='osiris')`.

---

## 9. Graph rulings index (the WHY — read these)

- `f1803b4a` — **constitutional amendment**: hands admitted and governed; the daemon is the
  trigger's successor.
- `7ff54707` — provider-agnostic bodies; plain-Linux default; cgroup-is-the-meter.
- `2ceb7ba0` — availability doctrine: intertwined, screaming, sacred proc.
- `5cd5b7b6` — endgame doctrine: no band-aids; the human is the economics; adapters stay
  abstract.
- `d6403d34` — the face's container: web artifact, thin native shell, browser demoted;
  webkitgtk spike; the scream belongs to the daemon.
- `19f0e75b` — the endgame is a protocol: memory primitives, agnostic, Osiris-first.
- `37fe6a09` — the Warp OOM canon: the fleet must not live inside the desktop's cgroup.
- `79fcaba0` — mind/body/face split by concern; don't port the mind (a port is a priced
  option, not a destiny).
- `34766bbf` — composer + manager = one graph-first surface; the face owns nothing.
- `30970d8f` — two tenant classes (targets vs bodies), opposite lifecycles.
- `15a41cf0` — the body lane's mechanism locked (PVH, initramfs-in-RAM, dom0-is-the-meter,
  vsock-no-IP).
- `dd47c1da` — the fleet field-specced Phase 1; `path=identity` is the root bug.
- `2749d09f` — the ACL seam; the body's egress rules are its enforcement layer.
- `4fd54a06` — fold forward; the face of a lineage is the latest mind to talk to the operator.
- `065c374e` (resolved) / thread `2294e95d` (open) — the seam race + the anchor-collision bug
  class Phase 1 §4.2 kills at the root.
- `5cef856b` — the identity-core design: Seat as first-class object (`seat:<uuid8>`), the
  attach ceremony (one-time token, refuse-loudly), addressing re-keyed on the Seat; the mind
  layer keeps its seams. Phases A–D in §4.1.
