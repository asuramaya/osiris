<!-- topic: ritual -->

# The Agent Lifecycle Ritual

In an autonomous agent workflow, context windows are transient. Sessions end due to compactions, crashes, or user tab closures.

**What is not in the graph does not exist.** A fact reasoned about only inside an unpersisted context window is permanently lost when that context vanishes. This document defines the operational discipline that guarantees continuity across sessions, agent generations, and harness boundaries.

---

## The Five-Step Lifecycle Cycle

```
  ┌─────────────────────────────────────────────────────────────┐
  │ 1. MOUNT: Bind session anchor & seat identity              │
  │    mount(cwd, job_dir)                                     │
  └──────────────────────────────┬──────────────────────────────┘
                                 │
  ┌──────────────────────────────▼──────────────────────────────┐
  │ 2. GLANCE: Check unread mail, obligations, and fleet pulse  │
  │    get_status() · get_mail()                                │
  └──────────────────────────────┬──────────────────────────────┘
                                 │
  ┌──────────────────────────────▼──────────────────────────────┐
  │ 3. RECALL: Query graph knowledge & prior rulings            │
  │    graph_search(query) · consult_canon(query)               │
  └──────────────────────────────┬──────────────────────────────┘
                                 │
  ┌──────────────────────────────▼──────────────────────────────┐
  │ 4. CAPTURE: Record architectural rulings & open obligations │
  │    record_decision(...) · open_thread(...) · resolve_thread(...)
  └──────────────────────────────┬──────────────────────────────┘
                                 │
  ┌──────────────────────────────▼──────────────────────────────┐
  │ 5. SETTLE: Brain-dump pending state before context boundary │
  │    settle(decisions=[...], threads_resolve=[...])           │
  └─────────────────────────────────────────────────────────────┘
```

---

## 1. Write Back As You Go (Continuous Capture)

Never defer recording decisions to the end of a session. Call the appropriate write tool the moment a determination is made:

### A. `record_decision`
Call the moment an architectural choice, ruling, invariant, or deliberate rejection is reached:
- Cites grounds (`grounds=["ref:<slug>"]`).
- Supersedes outdated rulings (`supersedes=["decision:<uuid>"]`).
- Closes answered threads in the same atomic act (`resolves=["thread:<uuid>"]`).
- Automatically checks for prior art across the graph.

### B. `open_thread(kind='obligation')`
Call the moment an owed task, follow-up duty, or blocking question is identified:
- `kind='obligation'`: Promotes a loose end into owed work on the project's task board.
- `kind='task'`: Ordinary tracked work item.
- `kind='question'`: Open question requiring human or peer clarification.
- `owner`: Stamped with `"operator"`, a seat handle, or unowned for any agent to pick up.

### C. `resolve_thread`
Call the moment work is completed or an obligation is mooted:
- `artifact`: A verifiable commit hash (`commit:<hash>`), decision ID (`decision:<uuid>`), or file:line pointer.
- Mentions and wires the closure edge directly in the graph.

---

## 2. Granular Retrieval vs Monolithic Briefings

Avoid context-window bloat by using bounded, high-efficiency tools:

- **`get_status()`**: Replaces large briefing dumps with a ~360 character summary of your identity, mail counts, and fleet pulse.
- **`graph_search(query, project?, lineage?, max_depth?)`**: Performs lexical + semantic search with 1–3 hop graph traversal rather than reading whole files.
- **`get_decision_list()` & `get_thread_list()`**: Paginated inspection of project state.
- **`consult_canon(query)`**: Recalls solved architecture patterns from the shared reference library.

---

## 3. `settle()` — The Pre-Compaction Seal

Before a context boundary or session close:

1. **Surface Status** (`settle()` with no arguments):
   - Reads open obligations owned by this agent or project.
   - Verifies whether uncommitted git changes exist in the active working tree.
2. **Execute Dump** (`settle(decisions=[...], threads_open=[...], threads_resolve=[...])`):
   - Atomically records all pending rulings.
   - Wires closure edges between decisions and resolved threads.
   - Confirms that `complete: true` is reached with no rejected items.

### The Structured Handoff (`is_handoff: true`)
When concluding a major phase or handing off to a future generation, mark the summary decision with `is_handoff: true`.
- Mints a structured, typed handoff property on the graph.
- Successor sessions will immediately see this handoff at full fidelity upon mounting.
- Successors retire the handoff by acknowledging receipt (`ack_handoff(ref=<id>)`).

---

## 4. Multi-Agent Coordination & Collision Prevention

When multiple agents or seats operate concurrently in the same workspace or project:

1. **Check Working Tree & Claim Lanes**:
   - Inspect open threads to ensure another agent is not already leasing the same files.
2. **Announce Irreversible or Broad Changes**:
   - Send a broadcast (`send(to="<project>", body="...")`) or DM before modifying shared infrastructure.
3. **Stage Only Exact Files**:
   - Never run blanket `git add .`; stage only the specific files you have modified.
