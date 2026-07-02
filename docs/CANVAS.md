# The neutral canvas — audit & rebuild spec

Status: **spec / in progress.** This document is the fixed reference for rebuilding the
Osiris front end from a half-built entity-investigation console into a neutral *composition
canvas*. It is the honest accounting of what's there (the "bad & ugly" ledger), what
survives, and the target we're building toward. Companion to [`COMPOSER.md`](COMPOSER.md)
(the op vocabulary) — that spec is the *language*; this is the *surface*.

## The diagnosis

The rot is **not** spread evenly. The engine underneath is clean:

- `src/ui/static/osiris.js` (383 lines) is a sound, type-driven renderer **library** — its
  dispatch (`renderResult` switches on `kind ∈ objects|values|rows|data`) is domain-neutral.
- The composition **primitives** are solid (`select / traverse / collect / table / sections /
  order / take / aggregate / rollup + present/absent`).
- The **data model** (event-sourced Actions, evidence-graded assertions, the graph) is sound.

**Almost all the ugliness is one file:** the ~625-line inline `<script>` at the bottom of
`src/ui/static/index.html` (lines ~214–839) — a hastily-built entity-investigation console,
grown while the product direction was still unsettled — plus a layer of demo-data residue
leaking into it. The wreck is contained. `osiris.js` and the primitives survive; the inline
shell dies.

---

## Part I — the ledger (bad & ugly)

### A. Data-layer cruft (the instance) — *purge*
- **Persona pollution.** `seed_default_compositions` seeds BOTH products into every instance.
  On the developer instance, 4 entity-intel lenses sit unassigned (`room=None`, so they leak
  into the "All" view): `co-investment-ties`, `operational-vs-disclosed-geography`,
  `screen-financing-network`, `who-is-this` — plus 2 still-ACTIVE demo watches (`neuralink
  lead`, `zephyr lead`).
- **Stray demo entities** in an otherwise-clean dev graph: `Zephyr Dynamics LLC`,
  `Neuralink Corp`, `Elon Musk`, `Jared Birchall`, `Marcus Vael`, `Priya Anand` (all
  `extracted-*` from old AI-extraction demos) — the ghosts that haunted the inspector.
- **Test-case litter:** `cli-extract-test`, `compose-cli`, `compose-novel` (the last is what
  the top dropdown pins). Keep `Osiris self-track`.
- **Wrong default landing:** `console_state.room_id = NULL` → the UI lands on the "All"
  god-view, not the built `engineer` room (which has `home = briefing`).
- **Debris:** 2 demo alerts; ~14k undrained `outbox` rows (no worker on this instance).

### B. Hardcoded entity chrome in the shell — *rip out*
- Top bar: **"Type key"** (`:148`), **"Review"** + merge-review apparatus (`:149`,
  `refreshReview`/`openReview`/`resolveCand`), *"Search the **ontology**"* framing (`:144`).
- Toolbar: **"Search around"** (`:178`), **"Expand ▸ / run collectors"** (`:179`).
- Left rail: **"Add to analysis / classify"** intake (`:165` → `/cases/{id}/intake`).
- Inspector: **"Dossier"** (`:520`), **"Read ▸"** (hardcodes `['Commit','Reference',
  'SoftwareProject']`, `:516`), **"Tag"** (`:521`), **"Search around"** (`:518`).

### C. Dead code — *delete*
- `osiris.js`: `HOW` map (exported, unused); `VIEWS` const (exported, unused — `viewsFor`
  returns literals); `OPSYM` defined twice (library + inline, used inconsistently).
- `index.html`: `reviewpop` cleanup for an element nothing creates; two parallel object-search
  paths (header `#search` popover vs ⌘K palette); redundant `#aroundBtn` (`disabled hidden`).
- CSS: `.mono`, `.badge` — never applied.

### D. Stubs / half-built — *finish or cut*
- **Double-tap "explore deeper" is a no-op** — `dbltap → onFocus(id,true)`, but the 2nd arg
  is dropped all the way down; identical to single-tap.
- **`window.prompt()` is the UX** for groupBy / addFilter / saveAs / newRoom / tag.
- **Watch/Spec authoring** — Spec accepts only hand-typed JSON ("Claude authors over MCP").
- **3 EventSources, uneven lifecycle** — `/console/stream` opened at boot, never closed;
  `/cases/{id}/stream` refetches the whole 400-object set every tick; `expandSet` has no
  completion/error handling.
- **All errors silently swallowed** (`.catch(()=>{})`), so failures are invisible.

### E. Three-pane fragility — *the actual source of "hostile"*
- **`SET` is overloaded and clobbered** — normally the analysis objects, but
  `syncPanesToResult()` overwrites it with the lens result (`:379`); after a lens the left
  rail silently stops meaning "the analysis".
- **`FOCUS` and `RESULT` mutually stomp** — each path nulls the other; selection depends on
  which ran last.
- **Two competing right-pane renderers** — `inspect()` writes object detail;
  `syncPanesToResult()` overwrites `#right` with a "Showing <lens>" summary.
- **Two "click an object" semantics** — `inspectOnly` (table rows) vs `focus` (graph/set,
  also clears RESULT + re-lays-out).
- **Global mutable state across 4+ declaration sites**, with the shared-cursor `/console`
  sync as a *second* write path mutating the same globals under hand-rolled reentrancy gating.

### F. Renderer domain leaks — *neutralize the leaf atoms* (dispatch is fine)
- `timelineList` hardcodes `_DATEKEYS`/`_SUMKEYS` — a mash of git (`authored_date`,
  `rationale`), real-estate (`sale_date`, `filed_date`), generic (`osiris.js:282`).
- `objectsTable`/`objectDetail` skip-set `{name, demo, tag}` + the **DEMO badge**
  special-case (`osiris.js:85,93`).
- **A whole second rendering path** — `viewContent`/`renderDiff` (`:532`) render
  markdown/git-diff straight into `#result`, competing with the generic renderer and assuming
  Commit/Reference shape.

---

## Part II — what survives (keep, don't throw out)

`osiris.js` renderer **dispatch** · the composition primitives · the `engineer` room's 5 real
op-trees (`briefing`, `changelog by area`, `open threads`, `recent work`, `the composer arc`)
· the dev graph core (Commit/File/Decision/Reference/Thread/SoftwareProject) · `dev_pulses`,
`object_events`, `console_state` · **the 2 `asuramaya` Persons + 1 merge_candidate = the real
dev-identity review** (the "Review 1" badge is legitimate, not cruft).

---

## Part III — the target: the neutral canvas

One surface, no entity chrome. A persona is a **room of compositions**, never coded UI.

- **One selection model.** A single `selection` (an object id or none) instead of the
  mutually-stomping `FOCUS`/`RESULT`/`SET` triangle. The left set, center result, and right
  inspector are three *views of one state*, never three write-targets.
- **One renderer.** Everything the center shows goes through `renderResult` (type-driven).
  The git-diff/markdown viewer becomes just another `kind`, not a competing path.
- **One click semantics.** Clicking an object anywhere means the same thing.
- **The shell is generic:** `[composition list + search] → [result renderer] → [inspector
  that follows the result]`. No "Type key / Review / Dossier / Search-the-ontology / Expand"
  in the frame — those are *compositions or functions* a room can include, not chrome.
- **Persona = room.** The room scopes which compositions appear; the composer authors new
  ones. Landing = the room's `home` composition.

---

## Part IV — the cut sequence

1. **Data purge** (Part I.A) — deletion + room-scoping, reversible; de-noises the instance so
   the canvas is built against clean, quiet data. *(done — see the purge record below.)*
2. **Dead-code cleanup** (I.C) — *done.* Removed `HOW` map + `VIEWS` const (+ their exports)
   from `osiris.js`; deduped `OPSYM` to the single library copy (inline dup gone, `index.html`
   uses `Osiris.OPSYM`); removed the dead `#aroundBtn` (hidden, never shown) + its enable line;
   dropped the phantom `reviewpop` from `closePop`; removed unused `.mono`/`.badge` CSS.
   Non-breaking (no console errors; `test_ui_render` green; eyes-verified).
   **Re-scoped on review:** the "renderer leaves" (I.F) split in two — the DEMO badge and the
   `{name, demo, tag}` skip-set are **generic Osiris conventions** (flag synthetic data, hide
   internal tags), *not* domain leaks → KEPT. The real leaks (`_DATEKEYS`/`_SUMKEYS` timeline
   heuristics, the `viewContent`/`renderDiff` diff-viewer competing with `renderResult`) are
   **redesign, not deletion** → moved to step 3.
3. **The canvas rebuild** (I.B, I.D, I.E, + the I.F redesign) — the entity-console shell → the
   neutral canvas of Part III: one selection model, one renderer (fold the diff-viewer in as a
   `kind`; make the timeline key-picking schema-driven), one click semantics.

Deferred (belongs to the persona/composer work, not the canvas): **persona-scoped default
seeding** — `DEFAULT_COMPOSITIONS` should declare a room per composition so a fresh instance
seeds the right persona's lenses, not both.

---

## Appendix — data purge record

Ran on the developer instance (PG:5601), 2026-07-01. All reversible or preserved (nothing
real destroyed):

- **Rescoped, not deleted** — the 4 entity-intel lenses + 2 demo watches moved to a new
  `analyst` room (preserved for the entity-intel face, out of the developer's view); the two
  demo watches also **deactivated** so they stop firing.
- **Completed the persona split** — the 6 dev lenses that were unassigned (`decision-log`,
  `projects`, `project`, `family-consistency`, `family-drift`, `pulse-digest`) + `design-canon`
  moved into the `engineer` room. Result: **engineer = 12 dev lenses, analyst = 6 entity-intel,
  0 unassigned**; the orphan `Canon` room dropped.
- **Archived** (event-sourced `set_status`, reversible) — the 6 stray `extracted-*` demo
  entities (Zephyr Dynamics, Neuralink Corp, Elon Musk, Jared Birchall, Marcus Vael, Priya
  Anand). Graph is now pure dev data: 246 Commit / 110 File / 13 Decision / 10 Reference /
  8 Thread / 4 SoftwareProject / 2 Person (the real `asuramaya` identities).
- **Deleted** — the 3 test cases (`cli-extract-test`, `compose-cli`, `compose-novel`), nulling
  their append-only FK refs first (demo assertions/links/events unlinked, not destroyed). Only
  `Osiris self-track` remains.
- **Cleared** — 2 demo alerts, 14,260 undrained outbox rows.
- **Landing** — `console_state` pointed at `engineer` / `briefing`, stale focus cleared.

**Shell finding surfaced by the purge** (→ belongs to step 3, the canvas): the UI *ignores*
`console_state.room_id` on load and hardcodes "land on the first case's graph" (`All` scope →
`Osiris self-track`). Room scoping is correct in the data, but the shell doesn't default to it
— you must pick the room manually. The neutral canvas must land on the console room, not a case.
