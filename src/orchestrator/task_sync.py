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
from datetime import datetime
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

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


# ── THE WRITE HALF (Thoth DM 2722, authorizing the three-tier design of DM 2687) ─────────
#
# TIER 1 — additive-only correlation facts, never a status. Safe to auto-execute: being
# wrong here is inert (nothing consumes this property yet) and self-correcting (a later
# assertion supersedes it). Property, not a link type (Thoth's ruling, DM 2722: a property
# is reversible with no schema change; a LinkType is a permanent vocabulary commitment,
# and the operator's pending `claim`/`supported_by` reshape (22d47acb) may make this a link
# BY CONSTRUCTION later — promoting a property to a link is cheap, demoting a LinkType is
# not).
#
# TIER 2 — visibility only, via open_thread (fully reversible: resolve_thread later).
# NEVER touches either side's status. Split into a PURE summary-generator (tier2_mints)
# and a side-effecting executor (mint_tier2_threads) on purpose: Thoth's gate is on SCALE,
# not distrust — 40 disagreement + 13 orphan rows is 53 new threads onto a board already
# hard to read, so the summaries must be reviewed before they are minted, not after.
#
# TIER 3 (never auto-write: no status sync either direction, no auto-closing orphans, no
# inventing bindings for uncited/cited_unresolvable, nothing written to ~/.claude/tasks, no
# repo-scoped thread enumeration) needs no code — it is the shape of what these two
# functions deliberately do NOT do.

_SOURCE = "task_sync"
_CORRELATION_PROPERTY = "harness_task_citation"


def _correlation_source_id(task_id: str, store: str | None) -> str:
    """Deterministic, per CITING TASK (not shared) — current_assertions resolves one
    winner per (object, name, SOURCE), so a shared source would let a second task citing
    the same Thread silently overwrite the first task's citation. Per-citation sourcing
    lets N simultaneous citations of one Thread coexist by the graph's own multi-source
    mechanic instead of fighting it (Thoth, DM 2722: "the part of this design I would
    defend hardest")."""
    return f"harness:{store}:{task_id}" if store is not None else f"harness:{task_id}"


def tier1_targets(report: dict[str, Any]) -> list[dict[str, Any]]:
    """Pure. Every (task_id, store, thread_id) TIER 1 may bind: `bound` rows in full,
    `bound_partial` rows' RESOLVED half only — their failed citations stay refused, never
    invented (Tier 3). No writes."""
    targets: list[dict[str, Any]] = []
    for row in report["bound"] + report["bound_partial"]:
        for tid in row["thread_ids"]:
            targets.append(
                {"task_id": row["task_id"], "store": row.get("store"), "thread_id": tid}
            )
    return targets


async def write_tier1_correlations(
    actions: Actions, report: dict[str, Any], *, observed_at: datetime,
) -> list[dict[str, Any]]:
    """Execute TIER 1: one `harness_task_citation` property assertion per target from
    `tier1_targets`, value {"task_id", "store"}, evidence_class=SELF_DECLARED — the
    citation was declared by the task's own author in its own free text; this only
    captures it durably, it does not infer or guess it (an ambiguous or unmatched citation
    already refused upstream, in resolve_task_citations, never reaches this function).
    Never writes a status. Never touches ~/.claude/tasks. Returns one receipt per write."""
    conf = confidence_for(EvidenceClass.SELF_DECLARED)
    written: list[dict[str, Any]] = []
    for target in tier1_targets(report):
        source_id = _correlation_source_id(target["task_id"], target["store"])
        await actions.assert_property(
            uuid.UUID(target["thread_id"]), _CORRELATION_PROPERTY,
            {"task_id": target["task_id"], "store": target["store"]},
            source_id, observed_at, conf,
            evidence_class=EvidenceClass.SELF_DECLARED.value,
        )
        written.append({**target, "source_id": source_id})
    return written


def tier2_mints(report: dict[str, Any]) -> list[dict[str, Any]]:
    """Pure. The visibility-thread {"kind", "summary", "row"} TIER 2 WOULD mint for every
    disagreement + thread_side_orphans row — one per row, summary built deterministically
    from (task_id, store, thread_id, statuses) so a rerun dedupes through open_thread's own
    idempotent-on-summary matching instead of spamming the board on every dry run. Executes
    nothing — see `mint_tier2_threads` for the side-effecting half, deliberately separate
    (Thoth, DM 2722: dry-run and show the actual summaries before executing; the gate is
    scale — 53 new threads onto a board the operator already called unreadable — not
    distrust of the design)."""
    mints: list[dict[str, Any]] = []
    for d in report["disagreement"]:
        store_label = d.get("store") if d.get("store") is not None else "?"
        summary = (
            f"TASK/THREAD DISAGREEMENT: harness task {d['task_id']} (store {store_label}) "
            f"says status={d['task_status']!r}; Thread {d['thread_id'][:8]} carries "
            f"property_status={d['thread_property_status']!r}. task_sync never resolves "
            f"this automatically (Tier 3) — needs a human/agent look."
        )
        mints.append({"kind": "obligation", "summary": summary, "row": d})
    for o in report["thread_side_orphans"]:
        summary = (
            f"THREAD SIDE ORPHAN: Thread {o['thread_id'][:8]} carries kind=task but no "
            f"harness task cites it (task_sync dry run). Stale, or the harness lost track "
            f"of it — task_sync never auto-closes an orphan, needs a human/agent look."
        )
        mints.append({"kind": "obligation", "summary": summary, "row": o})
    return mints


async def mint_tier2_threads(
    actions: Actions, mints: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Execute TIER 2: one open_thread(kind='obligation', arc='Fleet-Hygiene') per mint
    from `tier2_mints`. DO NOT CALL against production data without the summaries having
    been reviewed first — see this module's own write-half note and Thoth DM 2722. A rerun
    is safe: open_thread is idempotent on the exact summary string, so an unchanged
    disagreement/orphan collapses onto the same Thread instead of duplicating. Returns one
    {"summary", "thread_id"} receipt per mint."""
    from src.orchestrator.capture import open_thread

    out: list[dict[str, Any]] = []
    for m in mints:
        tid = await open_thread(
            actions, m["summary"], kind=m["kind"], arc="Fleet-Hygiene", source=_SOURCE,
        )
        out.append({"summary": m["summary"], "thread_id": str(tid)})
    return out
