<!-- topic: ritual -->

# The seam ritual — writing back before a session ends

A Claude session inside this house can end at any instant: a compaction, a crash, an
operator closing a tab. **What is not in the graph does not exist** — a fact reasoned about
only in a context window that then vanishes was never really known at all. This document is
the operational discipline that makes that survivable, for both the human reading an agent's
work and the agent (this one, or a successor) doing it.

## Write back as you go, not at the end

Three verbs, called the moment their event happens, never batched for a session's close:

- **`record_decision`** — the instant a ruling lands: an architecture pivot, a deliberate
  rejection, a scoping call. The epochal ones never land in a commit message at all; if it
  isn't written here, the *why* behind a piece of code outlives the commit that explains it
  by exactly as long as someone's memory does.
- **`open_thread(kind='obligation')`** — the instant work starts, blocks, or a gap is found
  that isn't this session's job to close. `kind='obligation'` is what promotes a loose end
  from "something I noticed" to "owed work" on the project's own wall — `orient()` sorts by
  it, so a stray note doesn't stay merely lit forever without becoming anyone's move.
- **`resolve_thread`** — the instant a thread's question is actually answered, with an
  `artifact` (a commit hash, a decision id) as the closure witness — not a prose sentence
  claiming closure that a later reader has no way to verify against.

Idempotent on their own summaries, on purpose: writing the same fact twice never forks the
record, so there is no cost to writing early and writing often. The alternative — reasoning
carefully in-context and only writing a summary at the very end — is exactly the failure mode
this discipline exists to close, because the end doesn't always come.

## `settle()` — the end-of-context ritual as one call

Call `settle()` with no arguments first: a read-only **surface** of where things stand —
open obligations this project owns, whether a structured handoff marker already exists, and
(pass `repo_path=…`) whether the actual code checkout has uncommitted work sitting in it. It
answers "am I safe to compact?" without a single by-hand `git status` or a manual scan of the
graph's open threads — the same shape of near-miss `osiris deploy`'s own dirty-tree guard
closes for a *deploy*, closed here for a *session's own end*.

Call it again **with** `decisions` / `threads_open` / `threads_resolve` to accept a whole
brain-dump in the same act — each a list of dicts, each dict the exact keyword arguments the
real verb above takes. `settle` dispatches every one of them through the genuine
`record_decision`/`open_thread`/`resolve_thread` calls (never a shortcut that skips their own
checks), then **confirms** by re-reading the now-updated graph. `complete: true` only when
nothing is left explicitly unwritten — if it isn't, `settle` names exactly what's missing,
which is the whole point: you are told what to write, not left to guess whether you're done.

### `is_handoff`

Pass `is_handoff: true` on the decision that summarizes a session's own state-of-the-board.
This mints a **structured, typed marker** on that decision — not prose a successor has to
grep for and hope they matched the right wording (the root fragility behind more than one
"Thoth II"-style mislabel this house has hit). A successor's own `orient()` finds a
`is_handoff` marker directly, the same way `whois()` finds a `holds` graph edge rather than a
cached column: the strong, structural fact, not a string that merely *looks like* the fact.

A good handoff decision names: every commit made this stretch, every decision id worth a
successor's attention, what's still explicitly open (and why it was left that way, not just
that it was), and a concrete next step if one is known — "check inbox for X's reply before
starting Y" beats a summary the next session has to re-derive from scratch.

## The seam whisper

Past a configured context threshold (`osiris_seam_whisper_pct`, default 63% — two-thirds,
the operator's own word: the whisper starts when a third of the working life remains), every
MCP tool response carries one extra field: `"context": "NN% — seam soon; write back as you
go"`. It is not a countdown to panic over — it is the ambient nudge that turns "write back
before you compact" from a rule you have to remember into a fact the tools themselves keep
surfacing at exactly the moment it starts to matter. Treat it as license to wrap up the
current thought and settle, not as an instruction to stop mid-sentence.

## Held, undeployed

A build in this house follows one loop, always: **build on a clean tree → gates green
(targeted pytest, ruff, mypy) → commit → report to your manager, with the commit hash and
test evidence → the manager approves or declines → the manager (never the worker) deploys,**
usually batched with several other workers' held commits at once. "Held undeployed" in a
build report is not a caveat — it is the loop working as designed: a worker's own hands never
restart a service or push past their own commit, and a manager's review is the gate between
"gated and correct" and "actually live." `osiris deploy` (see [`CLI.md`](CLI.md)) is now the
mechanical form of the manager's own half of that loop — the dirty-tree guard it runs is the
same discipline this section describes, enforced in code instead of by a by-hand `git
status` done at exactly the right moment.

## Collision watch

More than one live agent can be working the same checkout at once. Before staging a hunk in a
file another agent might also be touching, **announce it on the project channel first** —
name the exact region, and re-check `git status` immediately before staging to catch a live
collision that landed while you were writing the message. Never `git add -A`; stage only the
files you can name. This is cheap — one message — against the cost of two agents' edits to
the same region silently clobbering each other.
