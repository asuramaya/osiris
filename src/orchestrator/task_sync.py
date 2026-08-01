"""The harness tasklist reconciled against the graph — Phase 1, REPORT-ONLY (Thoth DM 2636,
decisions ab27af61/42f63782, Practice 3262cdc9: "a short id, a handle, a basename, or a
number is a HINT until you've named what scopes it").

WHY THIS EXISTS: the graph and a Claude Code harness's own TaskCreate/TaskList/TaskUpdate
tool disagree about what's open, and nothing has ever compared them (decision af63ea63,
"three ledgers, none authoritative"). But "the harness tasklist" is not one ledger — it is
one store PER project/session-launch context (~/.claude/tasks/<uuid>/, 91 of them on one
box the night this was measured), each numbering its own tasks independently from a low
integer. Nothing makes a bare id globally unique — the same id "107" named three unrelated
things across three different stores, verified live (decision 42f63782). So a task's binding
key is TWO properties, `harness_task_id` + `harness_task_store`, never one.

THIS MODULE DOES NOT TOUCH ~/.claude/tasks ITSELF. Every store's own `.lock` file is
evidence it expects tool-mediated writes (TaskCreate/TaskUpdate), not file surgery — reaching
in directly would be the harness-side twin of "RAW SQL IS A DEFECT REPORT" (decision
a2cf8405) against a store this house does not own. Every function here takes task data
already shaped like the harness tool's own JSON ({"id", "subject", "description", "status",
...}) as plain input — where that data comes from is the CALLER's problem (TaskList/TaskGet
in production; a clearly-labeled, read-only research driver for a one-off dry run — see
scripts/task_sync_dryrun.py). Keeping the harness IO out of this module is what makes it
testable without a real store and keeps this module honest about not being the enumeration
door it explicitly recommends never building (decision ab27af61: "no sanctioned door exists
to enumerate all 91 stores... flag it upstream, don't build a workaround").

THE BINDING RULE, UNCHANGED FROM PHASE 1c AND cf3dcd79: refuse, never guess. A citation is
found in a task's own prose (today's only bridge — "Graph thread 5da19aa6, ruling 10f4058b"),
resolved through the exact same strict ladder every other short-id caller in this house uses
(`_find_thread`, `require_identifier=True` — no free-text/summary-substring leg, the guess
this feature exists to refuse). A citation that resolves to more than one Thread (RefAmbiguous
— now understood per cf3dcd79 to often mean one multi-source-touched object, not two) or to
none is UNRESOLVABLE, reported by its exact string, never silently dropped and never
silently bound to a best guess.

FIVE BUCKETS, NEVER A BARE COUNT STANDING IN FOR THEM (Thoth's own failure-mode requirement,
DM 2562/2636): bound / bound_partial (some citations resolved, some didn't — a real, distinct
state, not folded into either clean bucket) / cited_unresolvable / uncited. The fifth,
disagreement, and the reverse-direction sixth, thread_side_orphans, are computed by
`reconcile` once the Thread-side universe is known — see its own docstring.

THE in_repo TRAP (Thoth DM 2636, the same night, from Khnum's and Seshat's independent
findings): any thread enumeration scoped through a repo-scoped lens silently excludes every
Thread with no `in_repo` edge at all — 56 of 2,568 fleet-wide the night this was measured.
That is the same law as this module's own binding rule, aimed at REACHABILITY instead of
UNIQUENESS: "what can this query actually reach" is `harness_task_store`'s sibling question
to "what is this unique within". So `reconcile` takes thread ROWS from
`thread_closure.enumerate_threads` called UNSCOPED (no `repo=`) — the only way to see
`has_in_repo=False` rows at all — never a repo-scoped query of its own.
"""
from __future__ import annotations

import re
import uuid
from typing import Any

import asyncpg

# A citation is an 8-hex-char token named near the word "thread"/"threads" in free text —
# the dominant real shape found in this house's own task descriptions ("Graph thread
# 5da19aa6, ruling 10f4058b", "Graph threads: a94935ad, f01b3fcc, 588148bb, 00f6a18d").
# Deliberately NOT matching every bare 8-hex token in a description (a git short sha, an
# unrelated id) — proximity to the keyword is the only signal cheap enough to trust without
# guessing, and getting it wrong in the SAFE direction (missing a real citation, landing it
# in `uncited`) is the only acceptable failure mode here; the resolution step below is what
# actually decides correctness, not this regex.
_CITATION_RE = re.compile(
    r"threads?\s*[:\s]\s*([0-9a-f]{8}(?:\s*,\s*[0-9a-f]{8})*)", re.IGNORECASE,
)


def parse_thread_citations(description: str) -> list[str]:
    """Pure. Candidate 8-hex Thread short-ids named near "thread(s)" in a harness task's own
    `description` text, in first-seen order, de-duplicated. Never resolves anything — that
    is `resolve_task_citations`'s job, against the live graph."""
    out: list[str] = []
    for m in _CITATION_RE.finditer(description or ""):
        for tok in m.group(1).split(","):
            tok = tok.strip().lower()
            if tok and tok not in out:
                out.append(tok)
    return out


async def resolve_task_citations(
    pool: asyncpg.Pool, task: dict[str, Any],
) -> dict[str, Any]:
    """One harness task ({"id", "description", ...}, the harness tool's own shape) -> its
    binding bucket. Never writes — Phase 1 is report-only. Never guesses: an ambiguous or
    unmatched citation is named, not dropped and not silently bound.

    Returns {"task_id", "bucket", ...}: bucket is one of
      "uncited"             — no thread-shaped citation found in the description at all.
      "bound"                — every citation resolved to exactly one Thread each.
                                carries "thread_ids" (str uuids).
      "bound_partial"        — at least one citation resolved AND at least one did not; a
                                real, distinct state (this task is not cleanly bindable),
                                never folded into "bound" or "cited_unresolvable".
                                carries "thread_ids" (the ones that DID resolve) and
                                "failed" (the ones that didn't, see below).
      "cited_unresolvable"   — every citation found failed to resolve to exactly one Thread.
                                carries "failed": [{"citation", "why"}], `why` is the exact
                                RefAmbiguous message or "no Thread matches" — never a bare
                                boolean, so a reader can act on WHY without re-deriving it."""
    from src.orchestrator.capture import RefAmbiguous, _find_thread

    candidates = parse_thread_citations(task.get("description") or "")
    if not candidates:
        return {"task_id": task["id"], "bucket": "uncited"}

    resolved: list[uuid.UUID] = []
    failed: list[dict[str, str]] = []
    for tok in candidates:
        try:
            tid = await _find_thread(pool, tok, require_identifier=True)
        except RefAmbiguous as exc:
            failed.append({"citation": tok, "why": str(exc)})
            continue
        if tid is None:
            failed.append({"citation": tok, "why": "no Thread matches"})
            continue
        resolved.append(tid)

    if resolved and not failed:
        return {"task_id": task["id"], "bucket": "bound",
                "thread_ids": [str(t) for t in resolved]}
    if resolved and failed:
        return {"task_id": task["id"], "bucket": "bound_partial",
                "thread_ids": [str(t) for t in resolved], "failed": failed}
    return {"task_id": task["id"], "bucket": "cited_unresolvable", "failed": failed}


def _status_disagrees(task_status: str, thread_property_status: str | None) -> bool:
    """A task's own status and its bound Thread's `property_status` disagree about whether
    the work is done. Pure boolean, no side — `reconcile` is the one that names WHICH task/
    thread pair, never resolves the disagreement itself (Thoth: "never resolved here, only
    flagged" — thread_closure.py's own topology_property_disagreement makes the identical
    choice for the edge/property axis; this is that discipline applied to the task/thread
    axis)."""
    task_done = task_status == "completed"
    thread_done = thread_property_status == "resolved"
    return task_done != thread_done


async def reconcile(
    pool: asyncpg.Pool, tasks: list[dict[str, Any]], *, thread_kind_field: str = "task",
) -> dict[str, Any]:
    """The full Phase-1 report. `tasks` is a flat list of harness task dicts from however
    many stores the caller gathered (each SHOULD carry its own `_store` key naming which —
    see scripts/task_sync_dryrun.py; not required here, just echoed back per-row if present,
    since this function does not know or care where a task came from, only what it says).

    Walks `thread_closure.enumerate_threads` UNSCOPED (no repo=) to build the full active-
    Thread universe INCLUDING has_in_repo=False rows — the in_repo trap this module's own
    docstring names. Every bound/bound_partial thread_id is cross-checked against that
    universe for its `property_status`, to compute the fifth bucket (disagreement) and the
    sixth, reverse-direction one (thread_side_orphans: Threads already carrying
    kind=`thread_kind_field` — 'task' by default — that no task in `tasks` bound to).
    Each disagreement row's own `task_status` is looked up by (task_id, store), not task_id
    alone — the SAME collision hazard the binding step already refuses (a bare id repeats
    across stores; keying by id alone would let one store's status silently stand in for
    another's). A disagreement row carries `store` whenever its task did.

    Returns the five-buckets-plus-one report, THE BUCKETS NEVER COLLAPSED TO A BARE COUNT:
    {"bound": [...], "bound_partial": [...], "cited_unresolvable": [...], "uncited": [...],
     "disagreement": [...], "thread_side_orphans": [...],
     "counts": {each bucket name: len(...)}}."""
    from src.orchestrator.thread_closure import enumerate_threads

    # Keyed by (task_id, store), NEVER task_id alone — a bare id collides across stores
    # (this module's own law, Practice 3262cdc9). Keying by id alone here would silently
    # let one store's task overwrite another's status for disagreement-checking, exactly
    # the hazard the binding step above already refuses to repeat.
    status_by_task_key = {
        (t["id"], t.get("_store")): str(t.get("status") or "") for t in tasks
    }

    buckets: dict[str, list[dict[str, Any]]] = {
        "bound": [], "bound_partial": [], "cited_unresolvable": [], "uncited": [],
    }
    bound_thread_ids: set[str] = set()
    for task in tasks:
        row = await resolve_task_citations(pool, task)
        if "_store" in task:
            row["store"] = task["_store"]
        buckets[row["bucket"]].append(row)
        if row["bucket"] in ("bound", "bound_partial"):
            bound_thread_ids.update(row["thread_ids"])

    # the full active-Thread universe, UNSCOPED — the only way has_in_repo=False rows are
    # ever visible (the in_repo trap named in this module's docstring)
    thread_status: dict[str, str | None] = {}
    thread_kind: dict[str, str | None] = {}
    after: uuid.UUID | None = None
    while True:
        page = await enumerate_threads(pool, after=after)
        for r in page["rows"]:
            thread_status[str(r["thread_id"])] = r["property_status"]
        ids = [r["thread_id"] for r in page["rows"]]
        if ids:
            kind_rows = await pool.fetch(
                "SELECT o.id, (SELECT a.value #>> '{}' FROM current_assertions a "
                " WHERE a.object_id=o.id AND a.name='kind' "
                " ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS kind "
                "FROM objects o WHERE o.id = ANY($1::uuid[])", ids,
            )
            for kr in kind_rows:
                thread_kind[str(kr["id"])] = kr["kind"]
        nxt = page.get("next_after")
        if not nxt:
            break
        after = uuid.UUID(nxt)

    disagreement: list[dict[str, Any]] = []
    for row in buckets["bound"] + buckets["bound_partial"]:
        task_status = status_by_task_key.get((row["task_id"], row.get("store")), "")
        for tid in row["thread_ids"]:
            if _status_disagrees(task_status, thread_status.get(tid)):
                entry = {
                    "task_id": row["task_id"], "thread_id": tid,
                    "task_status": task_status,
                    "thread_property_status": thread_status.get(tid),
                }
                if "store" in row:
                    entry["store"] = row["store"]
                disagreement.append(entry)

    thread_side_orphans = [
        {"thread_id": tid} for tid, k in thread_kind.items()
        if k == thread_kind_field and tid not in bound_thread_ids
    ]

    return {
        **buckets,
        "disagreement": disagreement,
        "thread_side_orphans": thread_side_orphans,
        "counts": {
            "bound": len(buckets["bound"]),
            "bound_partial": len(buckets["bound_partial"]),
            "cited_unresolvable": len(buckets["cited_unresolvable"]),
            "uncited": len(buckets["uncited"]),
            "disagreement": len(disagreement),
            "thread_side_orphans": len(thread_side_orphans),
        },
    }
