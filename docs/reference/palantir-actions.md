<!-- source: https://www.palantir.com/docs/foundry/action-types/overview | vendor: palantir | topic: action types — the kinetic write path | grounds: src/actions/core.py -->
# Palantir Foundry — Action Types (the kinetic write path)

The model behind Osiris's **kinetic waist** (`src/actions/core.py` — the six Actions that
are the *only* way anything is written to the graph). The semantic layer (object/link/property
types, `schema.py`) says what CAN exist; the kinetic layer says how it COMES to exist — and
Palantir's discipline is that the two are separate and the kinetic path is *singular*.

## What an Action is
An Action is a single transaction that changes properties of one or more objects (and their
links) by user-defined logic. The user doesn't edit the store directly — they *take an action*,
which encapsulates both the change and its business context, and the change commits as a unit.

## The discipline (what NOT to build)
- **One write path, no side doors.** Every modification to objects, properties, and links flows
  through an Action Type. There is no "just UPDATE the row" — the architecture deliberately
  constrains what can change outside this path, so there are no orphaned or inconsistent states.
  (Osiris: nothing writes the graph except `Actions`; parsers/ingest/UI all call it. A parser
  that did its own INSERT would bypass evidence-grading and the audit ledger — forbidden.)
- **Function-backed, not hand-rolled per call site.** Complex logic, conditional edits, and
  external calls live in Functions that back the action — definition is decoupled from
  implementation. (Osiris: resolution/canonicalize feed the Actions; the call sites stay thin.)
- **Validation + permissioning happen IN the action, before commit.** Authorization and domain
  constraints are enforced consistently because they're enforced in the one path, not
  re-implemented at every caller.
- **The action carries context.** A commit isn't a naked mutation; it records *why* (the actor,
  the run). This is the seam Osiris extends with its two enrichments below.

## Osiris's two enrichments over the base model
Palantir's kinetic layer is the spine; Osiris welds two kernel-wide invariants onto it that
Palantir does not have, precisely because the write path is singular (so they're enforced once):
- **Evidence-grading** — every property/link committed through an Action carries an evidence
  class → confidence (`parsers/evidence.py`). A claim is never unsourced.
- **Event-sourcing** — the graph is an append-only ledger; `object_events` is truth, the
  current view is a projection. A merge is a compensating event, reversible.

## Why this is the keystone
Because the write path is the *one* place every fact passes through, it is the only place that
has to be correct for provenance, audit, and reversibility to hold everywhere. Get the kinetic
waist right once and the whole graph is litigation-defensible by construction — which is the
target persona's #1 requirement, not a nicety.
