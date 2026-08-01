"""Thread closure derived from TOPOLOGY, not the `status` property (Phase 2, Thoth DM 2508,
decision cb38d922) — the read side of the `thread_closure_edges` view (migration 0043,
widened to its weak tier by 0044).

THE ARGUMENT (measured, not assumed, cb38d922): a thread's `status` assertion can be written
by more than one source (an agent opens, another resolves) and `assert_property` only
supersedes WITHIN a source, so both rows stay live — three different queries against the
same 737 threads gave three different open-counts the same night. `resolved_by`/`answers`
edges either exist or they don't; they cannot disagree with themselves the way a multi-source
property can. This module is the query-time home for that asymmetry: it reads the raw
`thread_closure_edges` view (kept deliberately unopinionated — see 0043's own docstring) and
turns it into the two judgment calls a caller actually needs, WITHOUT switching any existing
read path over to it. Nothing here is wired into orient(), compile_handoff, or any MCP tool
yet — that is Phase 2b, and it needs the closure-edge coverage this migration starts to widen
first (see CALLERS TO MOVE below).

`closed_by_topology=True` IS A GROUND-TRUTH POSITIVE — an edge exists, full stop.
`closed_by_topology=False` IS NOT "CONFIRMED OPEN" — read this twice before wiring anything
to it. Khnum's Phase 1a (commit 23c5991) made resolve_thread() mint a closure edge
UNCONDITIONALLY going forward — `resolved_by` (strong) when `artifact` resolves, else
`closed_by` (weak) to the resolving agent — so the artifact-less gap cb38d922 measured (the
majority of resolve_thread() calls, 408-of-527) no longer widens. It does NOT retroactively
heal it: every thread closed BEFORE that commit landed, by resolve_thread() with no
artifact, still has NO closure edge at all — indistinguishable here from a thread nobody has
ever touched. Until a historical backfill mints edges retroactively for those (a separate,
not-yet-proposed piece), the only safe reading of a False row is "no closure edge found
yet" — a caller that needs a real open/closed verdict today should still fall back to the
`status` property for the False case, not treat this view as authoritative on its own for
absence.

A SECOND, SHARPER REASON False IS NOT "OPEN" (Thoth's catch via Sekhmet's finding cf3dcd79,
thread 6212d9f5, 2026-08-01): `_resolve_ref`'s short-id branch (`_find_thread` ->
`_resolve_ref` in capture.py) LEFT JOINs `current_assertions` with NO source filter — and
`current_assertions` legitimately yields one row PER SOURCE. A thread whose `summary` was
touched by two different sources therefore produces two joined rows for the SAME object,
`len(rows) > 1` fires, and `RefAmbiguous` refuses a resolve_thread/record_decision(resolves=)
call against a real, UNIQUE short id — a miscount, not a genuine collision (verified by
reading the exact SQL, not just taking the finding on trust). The refusal itself is safe (no
partial write, confirmed by test_resolve_thread_refuses_on_a_colliding_short_id_ref_and_
mints_nothing) — but the TRIGGER is two agents having touched one thread's text, which is
what active collaboration looks like here. Bites SHORT-ID resolution specifically (a full
UUID ref short-circuits before ever reaching this branch); short ids are this fleet's
dominant citation convention in resolves=/artifact=, so this is not a rare corner. Whether it
becomes a PERMANENT gap depends on whether the caller retries with a full UUID after seeing
the (self-revealing — both candidates share one id) RefAmbiguous, so this is a bias in
closure LATENCY/likelihood, not an absolute guarantee — but it points the wrong way: the more
agent attention a thread got, the likelier its next short-id close attempt gets refused, the
likelier it still reads `closed_by_topology=False` here. Not this module's bug to fix
(capture.py's `_resolve_ref` is a different file, different owner) — recorded here because a
reader of this view is exactly who needs to know their "unclosed" count skews toward the
threads the fleet worked on together, not away from them.

THE TRANSITION-PERIOD DISAGREEMENT (Thoth's own framing, the interesting case): a thread can
carry BOTH a closure edge AND a current status='open' assertion — a decision named it in
`resolves=` (or resolve_thread(artifact=...) ran) while a separate source's 'open' write is
still the freshest thing THAT source ever said. This module does not pick a winner — same
law `_fn_lint`'s `land("contradiction", "warn", ...)` and fold_project's
"refuse rather than destroy the disagreement" already follow (#102's standing machinery,
cited by Thoth directly). `topology_property_disagreement=True` surfaces it; resolving it is
a mind's job, same as every other contradiction this kernel already knows how to flag rather
than silently arbitrate. The much more common inverse — an edge is absent but status says
'resolved' — is NOT flagged: that is the well-understood, expected 408-shaped gap, not a
live dispute, and flagging all of it would drown the one signal that's actually new.

STRENGTH TIERS: `resolved_by` and `answers` are `strong` (artifact- or ruling-backed — the
closing act named something a mind can go read). `closed_by` (Thread -> Agent, Khnum's
Phase 1a, wired in by 0044) is `weak` — self-attested, WHO closed it rather than WHAT closed
it, minted only when `resolved_by` does not land for that same closure (capture.py's
resolve_thread mints exactly one closure edge per close, never both). Reading this module
required NO code change to add the weak tier — the `strength` ranking below already carried
a 'weak' entry, put there when the view could not yet produce one; extending the view was
the whole one-line-UNION-ALL-arm point of building it this way.

CALLERS TO MOVE (Phase 2b, not this piece — named so the switch-over is cheap and obvious,
per Thoth's explicit ask; grep `name='status'` + `type='Thread'` to re-verify this list
against a later HEAD before acting on it):
  - src/orchestrator/compositions.py: `open_thread_wall` (1779, THE central read — feeds
    orient(), compile_handoff's `open` lens, and the chrome wall via `_fn_wall`),
    `rank_open_threads` (1722, ranks whatever `open_thread_wall` hands it — no status read of
    its own, but its INPUT changes if `open_thread_wall` moves), `_fn_echoes` (~1184),
    `_fn_lint`'s status-regression + contradiction checks (~1389/1536/1567 — may become
    partially redundant with `topology_property_disagreement` above), `_fn_roadmap_open`
    (~2051).
  - src/mcp_server.py: `_owned_open_threads` (1817, an agent's own-open-threads for mount/
    orient receipts), `orient` (2170, two separate winning-status reads).
  - src/orchestrator/capture.py: `find_near_duplicate_open_thread` (720, scopes its dedup
    check to status='open' threads only).
  - src/orchestrator/mailbox.py: `_operator_queue` (769).
  - src/orchestrator/{handshake,vitals,neighborhoods,dispose,pulse}.py and
    src/ingest/{closure,sessions,threads}.py: one hand-rolled winning-status read each
    (`automount`, `operator_debts`, `_neighborhoods`, the `candidates`/`dispose`/`orphans`
    shared WHERE clause, `_snapshot`, `_open_untouched_threads`, `_resolve_own_threads`/
    `_resolved_summaries`, `resolve_threads`).
None of these are touched by this piece — this module only adds a new, unused-by-default
read path alongside them.
"""
from __future__ import annotations

import uuid
from typing import Any

import asyncpg


async def thread_closure_status(
    pool: asyncpg.Pool, *, repo: uuid.UUID | None = None,
    thread_ids: list[uuid.UUID] | None = None,
) -> list[dict[str, Any]]:
    """One row per active Thread in scope: `thread_id`, `closed_by_topology` (bool — True is
    ground truth, False is NOT "confirmed open", see module docstring), `strength`
    ('strong'|'weak'|None — the highest-strength closure edge found, None if none),
    `closure_edges` (every edge found, raw), `property_status` (the winning `status`
    assertion today, exactly as every existing hand-rolled reader computes it — kept
    alongside so a caller can cross-check without a second query), and
    `topology_property_disagreement` (bool — an edge says closed while `property_status`
    says 'open'; never resolved here, only flagged).

    Scope is `repo` (an already-resolved SoftwareProject id, same shape
    `open_thread_wall(pool, proj)` takes), `thread_ids` (an explicit set), both together
    (intersection), or neither (every active Thread fleet-wide — the same unscoped
    posture `current_assertions` itself has; scope it at the call site for anything
    latency-sensitive)."""
    where = ["o.type = 'Thread'", "o.status = 'active'", "o.merged_into IS NULL"]
    args: list[Any] = []
    if repo is not None:
        args.append(repo)
        where.append(
            f"EXISTS (SELECT 1 FROM links l WHERE l.from_id = o.id "
            f"AND l.type = 'in_repo' AND l.to_id = ${len(args)})"
        )
    if thread_ids is not None:
        args.append(thread_ids)
        where.append(f"o.id = ANY(${len(args)}::uuid[])")
    scope_rows = await pool.fetch(
        f"SELECT o.id FROM objects o WHERE {' AND '.join(where)}", *args
    )
    ids = [r["id"] for r in scope_rows]
    if not ids:
        return []

    edge_rows = await pool.fetch(
        "SELECT thread_id, edge_type, strength, closer_id, source_id, created_at "
        "FROM thread_closure_edges WHERE thread_id = ANY($1::uuid[])",
        ids,
    )
    edges_by_thread: dict[uuid.UUID, list[dict[str, Any]]] = {}
    for r in edge_rows:
        edges_by_thread.setdefault(r["thread_id"], []).append(
            {"type": r["edge_type"], "strength": r["strength"], "closer_id": r["closer_id"],
             "source_id": r["source_id"], "created_at": r["created_at"]}
        )

    status_rows = await pool.fetch(
        "SELECT DISTINCT ON (object_id) object_id, value #>> '{}' AS status "
        "FROM current_assertions WHERE name = 'status' AND object_id = ANY($1::uuid[]) "
        "ORDER BY object_id, confidence DESC, observed_at DESC",
        ids,
    )
    status_by_thread = {r["object_id"]: r["status"] for r in status_rows}

    _STRENGTH_RANK = {"strong": 2, "weak": 1}
    out = []
    for tid in ids:
        edges = edges_by_thread.get(tid, [])
        strength = max(
            (e["strength"] for e in edges), key=lambda s: _STRENGTH_RANK[s], default=None)
        closed = bool(edges)
        prop_status = status_by_thread.get(tid)
        out.append({
            "thread_id": tid,
            "closed_by_topology": closed,
            "strength": strength,
            "closure_edges": edges,
            "property_status": prop_status,
            "topology_property_disagreement": closed and prop_status == "open",
        })
    return out
