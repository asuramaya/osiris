# SPEC — Osiris as Terminal / Agent-Manager (the mind/body/face fold)

**Status:** design locked, implementation not started. Written by Thoth XXXIII (osiris seat,
2026-07-14) at the operator's word, for the successor to implement **end to end**.
**Authority:** this file is the implementation blueprint; the *why* lives in the graph as the
rulings indexed at the bottom — read them with `consult_canon` / `search` before coding, and
`record_decision` as you build. This file consolidates them into something you code from; it
does not replace them.

> Constitution note (CLAUDE.md #10 forbids md dumps): this is a **work artifact**, an
> implementation spec the operator explicitly asked for — not a knowledge dump. The durable
> memory is still the graph. Keep it that way: when a decision changes, record it in the graph
> and update this file to match; never let this file become the source of truth for *why*.

---

## 0. The problem this solves

The operator runs his fleet as ~16 Warp tabs, each a `claude` session in a repo. Three pains,
all the same root:

1. **Warp OOM'd and took the whole fleet.** A 14.6 GB headless-Chrome child died inside Warp's
   systemd scope; the OOM killer failed the *entire scope* — every session, mid-flight work,
   gone. The fleet has minds and a memory but **borrows its bodies from the operator's desktop
   cgroup**. (ruling `37fe6a09`)
2. **Reboot/crash → manual respawn.** Bodies and minds are the same thing in Warp; kill the tab,
   kill the agent. No resurrection.
3. **Rug-pulls leave orphans.** A safety fallback / fork / model-swap mints a new session id;
   Warp tracks the id; the old tab is orphaned and "the agent I was talking to gets lost."

Plus the field diagnosis from the fleet (rulings `dd47c1da`): **`path = project = identity`** is
a bug under all of it — the harness *and* Osiris key identity on the cwd string, so a moved
folder orphans a session and a seat's house can't be a *set* of repos.

---

## 1. Architecture — three concerns, three bottlenecks, NOT one language

(rulings `79fcaba0`, `34766bbf`)

| Concern | Owner | Language | Why |
|---|---|---|---|
| **MIND** — graph, provenance, identity, mailbox, meter, ceiling, compositions, miners | Osiris (this repo) | **Python, keep 100%** | IO-bound (PG/Redis/HTTP/subprocess). Rust buys nothing but a multi-month rewrite of the one thing that must never lose memory. |
| **BODIES** — hypervisor, microVM lifecycle, resource envelopes | Ra / rotten-apple | **Rust, already** | The metal. (ruling `15a41cf0`) |
| **FACE** — the terminal/agent-manager window that replaces Warp | NEW | **Rust (a client)** | A terminal's hot path is keystroke→render latency; a native TUI has no Electron/Chrome cgroup to OOM. |

**Warp is the counter-proof to "unify on Rust": it *is* Rust and it died anyway** — Rust wrapped
around Chrome carrying an agent farm's memory. Language was never the failure; topology was.
Polyglot on purpose; the boundary between the three is **a socket**.

### The load-bearing rule: the FACE owns NOTHING

Warp's fatal flaw: the window owns the sessions, so window-death = agent-death. The fix:

- A **lifecycle-owning daemon** (Python, in this repo) holds the fleet — summons bodies, holds
  handles, brokers PTYs.
- The **graph** holds the truth (who exists, lineage, mail, spend).
- The **face** is a pure client, as disposable as a body. Crash it, reboot the box, open a new
  one — the fleet is intact. Multiple faces (desktop/phone/web) attach to one daemon-truth.

### The reframe that makes composer + manager ONE thing

There was never a boundary. The composer is lenses over graph objects; **an agent is already a
graph object** (a seat, a lineage, mail, a mount, spend). Warp gives it a terminal without
knowing it's a node; Osiris knows it's a node without giving it a terminal. The unified face
makes them the same node with two facets: the **live terminal** (agent acting, a PTY into its
body) beside the **graph context** (agent remembered — lineage, graded mail, obligations,
decisions, spend). Selecting a seat shows both.

### Two INDEPENDENT cures (the plan's leverage)

- **Daemon (Phase 1) = the continuity cure.** Reboots/face-crashes stop losing agents. **Needs
  no metal** — works even with today's in-slice `claude -p` as an interim body, because the wins
  come from the daemon+graph owning identity, not from the body being a microVM.
- **microVM (Phase 3) = the OOM cure.** No agent can kill the box. Gated on Ra's temple.

Do not make one wait on the other.

---

## 2. Phase 0 — the Ra boundary + meter + small fixes (mine, now, no metal)

Everything here builds against a **stub orchestratord** so the Python side is green before Ra's
hardware exists.

### 0.1 `_spawn_in_body` — the body-spawn lane

- In `src/orchestrator/trigger.py`, add a second spawn path beside `_spawn_claude`:
  `_spawn_in_body(kind, cores, ram, repo, anchor, budget, *, spawn=<orchestratord client>)`.
- It calls Ra's `summon(kind, cores, ram, repo_ref, seat_anchor, budget)` → `handle`; injects the
  seat's durable anchor and the repo; runs the seat's `claude` inside the body; and registers the
  handle so the receipt can be collected.
- **The entire wake protocol is unchanged**: dark by default, scoped re-arms name their subjects
  (`osiris_trigger_projects`), the meter reads the receipt event-dated, the ceiling watches spend.
  Only the substrate changes.
- **Stub for now:** a fake orchestratord client that returns a synthetic handle and writes a
  canned receipt to a temp path. Interface must match §5 (Ra contract) exactly so the swap to real
  orchestratord is a one-line client change.

### 0.2 The meter's new dimension

- `src/ingest/wake_cost.py`: extend the receipt envelope parse to record **hypervisor
  resource-seconds** — `core_seconds` (domain cpu_time), `ram_seconds` (envelope × wall), and
  `exit_cause` (shutdown code) — beside the vendor's `total_cost_usd`. Add columns to `llm_usage`
  (migration) or a sibling `body_usage` table; event-dated by the receipt file's mtime, same
  discipline as the resume-mode receipt fix (`eddb006`).
- The ceiling reads both dimensions. "A hand you cannot cost is a hand you cannot govern" — the
  microVM's envelope IS its cost accounting.

### 0.3 Four small fixes tonight surfaced

- **Statusline false-down** (`scripts/osiris_statusline.py`): the 1.0s `asyncpg.connect` timeout
  flaps under load and prints `graph unreachable` when the graph is fine. Distinguish slow from
  down (retry once, or widen the timeout, or show "graph slow" vs "graph down"). The graph was UP
  the whole time it read down tonight.
- **Seam-dating gate refuses a null** (`src/orchestrator/agents.py` / `forks.py`): a null/unknown
  prior-model is the ABSENCE of an observation, not a prior value — a fresh model reading can never
  be "fresher than" or "disagree with" a null, so it must not date a seam against it (forks.py
  already carries this exact lesson for `model IS NULL`). Defense-in-depth even though tonight's
  seam was a real fable→opus fallback. (thread `065c374e`, resolved as misdiagnosis but the
  hardening stands)
- **`open_thread` idempotency across a lineage restart** (dedup): the same fact minted twice across
  a compaction/restart because the summary differed slightly. A "does a thread with a near-summary
  already exist for this project?" check before minting. (Aegis + Maat both hit this.)
- **The mount/orient identity race** (thrice-witnessed tonight — Thoth, Aegis, Maat): `mount()`
  asserts a model-seam it isn't confident in; `orient()` tells the true story a beat later; a mind
  acting on `mount()` alone confesses a rug-pull that never happened. **Fix: `orient()` is the
  single source of truth for the seam, OR `mount()` stays silent on a seam it can't confidently
  date.** (captured in `dd47c1da`)

---

## 3. Phase 1 — the manager daemon (Python) — the continuity cure, no metal

A lifecycle-owning daemon (a new arq/systemd user unit, e.g. `osiris-manager`) holds the fleet.

### 3.1 Decouple identity from PATH — the root fix (ruling `dd47c1da`)

`path = project = identity` is the bug under alfred's orphaning. The daemon must:

- Key the **seat** on a durable id, never a cwd string. The folder becomes a **mutable anchor**,
  not the identity.
- Add a **CHARTER relation**: `seat → governs → [repos]`. A house is *what a seat rules*, not
  *where it sits* (alfred's charter is six repos — ByeByte/RAMstein/kast/coldspot/phanspeed/gestalt
  — no folder can express it). `orient()` gains a charter-scoped mode.
- Provide a **seat rebind / migration primitive**: move a seat's anchor cwd while preserving
  identity, lineage, attribution, and mail. **THE OPERATOR IS BLOCKED ON THIS** — he moved alfred's
  folder and can't carry him across without it. Critical path.

### 3.2 Seats as first-class objects (alfred's asks 1, 5, 6)

- One claimed seat per house; **sessions ATTACH to a seat** (the `Name II` lineage machinery is
  half of this already).
- **Roster distinguishes holder from visitor.** A **VISITOR CLASS**: mounts carrying `subagent_id`
  land under the parent's swarm, **never as project peers** (today two ephemeral builder-subagents
  that mounted `cwd=ByeByte` were minted as first-class peers of the seat — a continuity fork on
  the books). Consider refusing peer-mounts from known subagent transcripts.
- **Work as single-assignee leased obligations:** `open_thread(kind='obligation', assignee=seat)`
  with single-assignee enforcement — a double-assignment surfaces the existing lease instead of
  minting a parallel build.

### 3.3 Resurrection from the graph manifest

- On daemon start, read the fleet from the graph (`fleet()`), see which seats had live bodies,
  and **re-body** them: summon via Ra (or in-slice interim body), resume the transcript by its
  durable anchor, re-attach the PTY. **The graph IS the respawn manifest** — manual respawn stops
  existing.
- Governed by the **warm/cold policy** (operator's call, §6): re-body only warm seats at boot,
  leave cold ones dormant and re-body on attach.

### 3.4 PTY broker

- `osiris attach <seat>` drops the operator into that seat's live terminal inside its body. This is
  the one capability Warp has that the graph doesn't. The daemon brokers the PTY so the *face* can
  attach/detach without owning the process.

### 3.5 Lineage-addressed control (the rug-pull cure)

- Everything keys on the **seat / `lineage_head`**, never a session id. A rug-pull/fallback/fork
  moves the head; the UI follows it; "Thoth" is always the current mind and remnants collapse into
  history beneath the head (the fleet tree already collapses retired generations). This dissolves
  the "lost agent" orphan.
- `send(to_agent=NAME)` must **echo the resolved seat + lineage** and **hard-fail on an unclaimed
  name** (offer `require_seat=true`); `fleet()` must **print claimed names** (alfred's ask 4 — a
  build order resolved silently to an id tonight, unverified). This one is small and can ship early,
  independent of the daemon.

### 3.6 Batch-resolve (Maat)

- `record_decision(resolves=…)` closes ONE thread. Add a **batch/pattern form** so a delegation
  decision can name the SET of threads it supersedes and have the graph fold them — otherwise a
  seat that hands off work keeps carrying open threads on its own briefing (manual archaeology).

### 3.7 Keep what works

- The co-agent warning ("another live agent in this project cwd right now, don't `git add -A`")
  **caught a real concurrent-edit tonight** — keep it and do more of that "here's what's actually
  true about your environment right now." It's the smallest working instance of the daemon's whole
  thesis.

---

## 4. Phase 2 — the unified face (Rust TUI, additive, a socket client)

- The window that replaces Warp. Left rail = the fleet tree (`fleet()`). Select a seat → split
  pane: **live terminal** (PTY via Phase 1) + **graph context** (lineage, graded mail, obligations,
  recent rulings, spend-against-ceiling).
- **Owns nothing.** Pure client of the daemon+graph over the existing socket (MCP socket +
  automount:8790 + console:8011). Crash it, reboot, reopen: fleet intact. Multiple faces attach to
  one daemon-truth.
- The composer lenses that live at `:8011` today (desk, mail, fleet, decision-log) render natively
  here instead of as chrome pages.
- Proceeds **in parallel** the moment Phase 1 exposes the control surface over the socket.

---

## 5. Phase 3 — the temple wiring (gated on Ra's metal) + the Ra contract

(ruling `15a41cf0`, mail threads 497/498/507)

Point the stub orchestratord at the real one. This is where the **microVM OOM cure** lands.

### The body-lane contract (Ra owns the mechanism; Osiris conforms)

- **Interface:** MCP tools on the rotten-apple server — `summon` / `dissolve` / `receipt_get`
  beside `domain_*`, backed by orchestratord JSON-RPC `body.*` methods. No second control socket.
- **Osiris hands:** `(kind, cores, ram, repo_ref, seat_anchor, budget)`. `summon` → `handle`.
  Receipt lands at a known dom0 state-volume path AND via `receipt_get`.
- **Mechanism:** a Xen PVH domU with **direct kernel boot** (no firmware, no qemu, no disk);
  rootfs is an **initramfs in RAM** (the ThinDom0 trick one ring down). `dissolve` = domain
  destroy, RAM home to Xen. Golden snapshot per kind = a `(kernel, initramfs, cmdline)` manifest —
  no memory-image staleness; kind-updates are file swaps.
- **Honest boot:** 1–2s summon-to-shell (NOT "~ms" — that was Firecracker marketing). Prices fine:
  the ledger cares about the receipt, not the boot.
- **Envelope (confirms the two-class split, ruling `30970d8f`):** hard `max_memkb`, vcpus pinned
  to the E-core cpupool (phanspeed contract), **no balloon for bodies** (balloon is the TARGET
  economy). Worst case is the body's own OOM eating the body; dom0 never notices. **Warp's OOM
  becomes structurally impossible.**
- **Receipt-before-dissolve:** dom0 IS the meter — exact `core_seconds` (cpu_time), `ram_seconds`
  (envelope × wall, exact under fixed allocation), `exit_cause` (shutdown code). orchestratord
  writes it fsync'd BEFORE destroy completes; a crashed body's death event still mints one.
- **Egress — two holes, default-deny:** DESTINATION is **osiris-over-vsock with NO IP at all** (a
  body with no IP can't reach what it has no standing to touch — the cross-project ACL seam
  `2749d09f` made physical). `nft`-on-vif is the interim; gated on Ra's **vsock-auth (CRIT #11)**
  closing first.

### Two tenant classes — never conflate (ruling `30970d8f`)

- **TARGETS** — long-lived full-OS domUs (Windows/macOS/Kali/whatever). Subjects of work, not
  agents. Expensive to create, cheap to keep, **balloon-managed**, resumed. `instance_create` is
  the target verb.
- **BODIES** — ephemeral microVMs (this spec's lane). Cheap to boot, **never resumed**, state
  disposable. `summon`/`dissolve` is the body verb. Keeping them separate IS constitution invariant
  7 (an ownership boundary at design time).
- The composition IS the temple: an ephemeral body operates AGAINST a long-lived target (a Kali
  roller against a Windows target).

### Sequencing (Ra's honesty, mail 498)

Box still boots bare today (`running_under_xen: false` is correct). This session closed the
guest-create blocker (a four-bug chain ending at CMA phantom memory, found in sim). **Boot #6 is
queued with the operator.** Then: boot desktop under Xen → guest networking (#2) + vsock auth (#11)
→ body-lane v1 → **first field test as specced** (one kind, one summon, one seat, hypervisor-priced
receipt).

---

## 6. Operator-gated — needs the human's hand or word

- **Boot #6** — his reboot; unblocks the temple / Phase 3.
- **Warm-vs-cold per seat** — decides what the daemon re-bodies at boot. *Recommendation:
  cold-by-default (summon-on-attach in ~1–2s, the microVM payoff), with a `warm` flag only for
  seats that must always run (a watcher, a long build).*
- **Seat migration primitive (Phase 1 §3.1)** — he's blocked on it to move alfred, who is orphaned
  now.
- **coldspot signing key** — provision a hardware-backed SSH/FIDO2 key (`ssh-keygen -Y`, per
  project) so coldspot's fail-closed auto-update has something to verify against.
- **Thoth-side desk items alfred routed upstream** — durable project-id independent of path, a
  seat-lease check on subagent spawn, documenting "mail is the only lane to a parked session."

---

## 7. Build order (what unblocks what)

1. **Phase 0** — mine, now, no metal. Stub boundary + meter dimension + the four fixes. First
   commit; makes the next `claude -p` already speak the body-lane's language.
2. **Phase 1** — the manager daemon. The continuity cure. Identity-decoupling first (§3.1) because
   the charter + migration + orphan-fix all fall out of it. Ships value with in-slice interim
   bodies before the temple exists.
3. **Phase 2** — the Rust face, in parallel once Phase 1 exposes the control surface.
4. **Phase 3** — stub→real orchestratord swap when Ra pings that the metal's under Xen.

Gates before every osiris commit (CLAUDE.md): `uv run pytest` (testcontainers, real PG) ·
`uv run ruff check src tests` · `uv run mypy src` (--strict). Shared tree — stage your own hunks,
never `git add -A`, coordinate via `send(to='osiris')`.

---

## 8. Graph rulings index (read these — the WHY)

- `37fe6a09` — the Warp OOM canon: the fleet must not live inside the desktop's cgroup.
- `79fcaba0` — mind/body/face split by concern not language; don't port Osiris to Rust.
- `34766bbf` — composer + agent-manager = one graph-first surface; the face owns nothing.
- `30970d8f` — the temple's two tenant classes (targets vs bodies) are opposite; bodies are microVMs.
- `15a41cf0` — the body lane's mechanism locked (PVH domU, initramfs-in-RAM, dom0-is-the-meter,
  vsock-no-IP egress, division of labor).
- `dd47c1da` — the fleet field-specced Phase 1 (alfred's six asks + Maat's fixes); `path=identity`
  is the root bug; decouple identity from path.
- `2749d09f` — the ACL seam (canon-bootstrap fallback + panopticon shelf lean on it); the body's
  egress rules are its enforcement layer.
- `4fd54a06` — the face of a lineage is the latest mind to talk to the operator; fold forward.
- `065c374e` (resolved) — the mount/orient seam race + null-is-not-a-prior-model hardening.
