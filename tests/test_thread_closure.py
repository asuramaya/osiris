"""Migration 0043's `thread_closure_edges` view + its read function (Phase 2, Thoth DM
2508, decision cb38d922) — closure derived from topology, never from the multi-source
`status` property alone."""
from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

from src.actions.core import Actions
from src.orchestrator.capture import open_thread, record_decision, resolve_thread
from src.orchestrator.thread_closure import enumerate_threads, thread_closure_status

NOW = datetime(2026, 8, 1, tzinfo=UTC)


async def _repo(actions: Actions, name: str) -> None:
    proj = await actions.create_or_find_object("SoftwareProject", f"repo:{name}", "session")
    await actions.assert_property(proj, "name", name, "session", NOW, 0.9)


async def test_untouched_thread_has_no_closure_edge(actions: Actions) -> None:
    await _repo(actions, "tc1")
    tid = await open_thread(actions, "an untouched question", repo="tc1", source="agent:me")
    rows = await thread_closure_status(actions.pool, thread_ids=[tid])
    assert len(rows) == 1
    assert rows[0]["closed_by_topology"] is False
    assert rows[0]["strength"] is None
    assert rows[0]["closure_edges"] == []
    assert rows[0]["property_status"] == "open"
    assert rows[0]["topology_property_disagreement"] is False


async def test_resolve_thread_with_artifact_mints_a_strong_resolved_by_edge(
    actions: Actions,
) -> None:
    await _repo(actions, "tc2")
    tid = await open_thread(actions, "closed by an artifact", repo="tc2", source="agent:me")
    decision_id = await record_decision(actions, "an unrelated ruling", repo="tc2")
    await resolve_thread(actions, str(tid), artifact=str(decision_id)[:8])

    rows = await thread_closure_status(actions.pool, thread_ids=[tid])
    assert len(rows) == 1
    row = rows[0]
    assert row["closed_by_topology"] is True
    assert row["strength"] == "strong"
    assert len(row["closure_edges"]) == 1
    assert row["closure_edges"][0]["type"] == "resolved_by"
    assert row["closure_edges"][0]["closer_id"] == decision_id
    assert row["property_status"] == "resolved"
    assert row["topology_property_disagreement"] is False


async def test_record_decision_bears_on_never_counts_as_a_closure_edge(
    actions: Actions,
) -> None:
    """The mint_bears_on conflation fix (0055, Thoth DM 6230/6234, decision 36cbec2f): a
    bears_on citation mints the identical `answers` link type resolves= uses, but never
    writes `status` — so it must NOT satisfy the view's same-source status='resolved'
    corroboration check. Measured live: 9 of closure_health's 10 repo=osiris disagree rows
    were exactly this specimen, including one Thoth minted on her own coordinator thread."""
    await _repo(actions, "tc3b")
    tid = await open_thread(actions, "cited but not closed", repo="tc3b", source="agent:me")
    await record_decision(
        actions, "a passing finding that merely speaks to it", repo="tc3b", bears_on=[tid])

    rows = await thread_closure_status(actions.pool, thread_ids=[tid])
    row = rows[0]
    assert row["closed_by_topology"] is False   # no corroborated closure edge
    assert row["closure_edges"] == []
    assert row["property_status"] == "open"      # untouched — bears_on never writes status
    assert row["topology_property_disagreement"] is False  # no false signal left to flag


async def test_bears_on_and_resolves_on_the_same_thread_still_reports_the_real_edge(
    actions: Actions,
) -> None:
    """A thread can be cited in passing AND later genuinely closed — the bears_on edge
    stays uncorroborated and invisible to the view, the resolves= edge still counts.
    Distinct sources on purpose: the corroboration check is scoped to (object, source_id),
    which is exactly the real specimens measured tonight — a coordinator's own bears_on
    citation and a different mind's later resolves= call are never the same source."""
    await _repo(actions, "tc3c")
    tid = await open_thread(actions, "cited then actually closed", repo="tc3c",
                            source="agent:me")
    await record_decision(actions, "a passing mention", repo="tc3c", bears_on=[tid],
                          source="agent:citer")
    decision_id = await record_decision(
        actions, "the ruling that actually settles it", repo="tc3c", resolves=str(tid),
        source="agent:closer")

    rows = await thread_closure_status(actions.pool, thread_ids=[tid])
    row = rows[0]
    assert row["closed_by_topology"] is True
    assert row["strength"] == "strong"
    assert len(row["closure_edges"]) == 1  # the bears_on edge stays out, not double-counted
    assert row["closure_edges"][0]["closer_id"] == decision_id
    assert row["property_status"] == "resolved"


async def test_record_decision_resolves_mints_a_strong_answers_edge(actions: Actions) -> None:
    await _repo(actions, "tc3")
    tid = await open_thread(actions, "closed by a ruling", repo="tc3", source="agent:me")
    decision_id = await record_decision(
        actions, "the ruling that answers it", repo="tc3", resolves=str(tid))

    rows = await thread_closure_status(actions.pool, thread_ids=[tid])
    row = rows[0]
    assert row["closed_by_topology"] is True
    assert row["strength"] == "strong"
    assert row["closure_edges"][0]["type"] == "answers"
    assert row["closure_edges"][0]["closer_id"] == decision_id
    assert row["property_status"] == "resolved"


async def test_resolve_thread_without_artifact_now_mints_a_weak_closed_by_edge(
    actions: Actions,
) -> None:
    """Khnum's Phase 1a fix (commit 23c5991): resolve_thread() with no artifact at all no
    longer leaves the thread edgeless — it mints `closed_by` (weak) to the resolving agent
    instead. This is the forward-looking half of cb38d922's fix; the residual gap is only
    threads closed BEFORE that commit (see the next test)."""
    await _repo(actions, "tc4")
    tid = await open_thread(actions, "closed with no artifact", repo="tc4", source="agent:me")
    await resolve_thread(actions, str(tid), because="just done, nothing to cite",
                         source="agent:closer")

    rows = await thread_closure_status(actions.pool, thread_ids=[tid])
    row = rows[0]
    assert row["property_status"] == "resolved"
    assert row["closed_by_topology"] is True
    assert row["strength"] == "weak"
    assert len(row["closure_edges"]) == 1
    assert row["closure_edges"][0]["type"] == "closed_by"


async def test_pre_cutover_closure_with_no_edge_at_all_still_reads_false(
    actions: Actions,
) -> None:
    """The residual gap this migration does NOT heal: a thread closed before Khnum's
    Phase 1a landed has status='resolved' but never went through resolve_thread()'s new
    unconditional-edge path, so no edge of any kind exists. Simulated here by writing the
    status assertion directly (bypassing resolve_thread entirely) rather than via the verb,
    since the verb itself no longer produces an edgeless closure. closed_by_topology must
    stay False, and False here must NOT be read as "confirmed open" (it's the untraced-
    historical-closure case, not a live dispute)."""
    await _repo(actions, "tc4b")
    tid = await open_thread(actions, "closed before the fix existed", repo="tc4b",
                            source="agent:me")
    # confidence must match (or beat) open_thread's own 0.9 write — the winning-status read
    # orders by confidence DESC first, observed_at DESC only as the tie-break.
    later = datetime.now(UTC) + timedelta(seconds=1)
    await actions.assert_property(tid, "status", "resolved", "session-miner", later, 0.9,
                                  evidence_class="derived")

    rows = await thread_closure_status(actions.pool, thread_ids=[tid])
    row = rows[0]
    assert row["property_status"] == "resolved"
    assert row["closed_by_topology"] is False
    assert row["closure_edges"] == []
    assert row["topology_property_disagreement"] is False  # the expected gap, not a dispute


async def test_closure_edge_with_a_later_open_reassert_is_flagged_not_resolved(
    actions: Actions,
) -> None:
    """The transition-period case Thoth named directly: a strong closure edge exists AND
    a different source's freshest write says 'open'. The read function must not pick a
    winner — it flags the disagreement, same law _fn_lint's contradiction check follows."""
    await _repo(actions, "tc5")
    tid = await open_thread(actions, "closed then re-touched open", repo="tc5",
                            source="agent:me")
    decision_id = await record_decision(actions, "settles it", repo="tc5", resolves=str(tid))
    # a SEPARATE source reopens (e.g. a miner or another agent) without touching the edge —
    # record_decision stamps its own status='resolved' write at real wall-clock time
    # (datetime.now(UTC) internally), so this reassert must be strictly later to win the
    # winning-status tie-break (confidence is equal; observed_at DESC decides).
    later = datetime.now(UTC) + timedelta(seconds=1)
    await actions.assert_property(tid, "status", "open", "agent:other", later, 0.9)

    rows = await thread_closure_status(actions.pool, thread_ids=[tid])
    row = rows[0]
    assert row["closed_by_topology"] is True  # the edge is real and stays real
    assert row["closure_edges"][0]["closer_id"] == decision_id
    assert row["property_status"] == "open"  # the freshest winning read
    assert row["topology_property_disagreement"] is True  # flagged, not silently resolved


async def test_a_healed_edge_no_longer_counts(actions: Actions) -> None:
    """valid_until is the kernel's universal heal marker (never DELETE) — a closure edge
    that's been invalidated must drop out of the view exactly like every other reader
    already filters healed links."""
    await _repo(actions, "tc6")
    tid = await open_thread(actions, "closed then healed", repo="tc6", source="agent:me")
    decision_id = await record_decision(actions, "an unrelated ruling", repo="tc6")
    await resolve_thread(actions, str(tid), artifact=str(decision_id)[:8])
    await actions.invalidate_link(tid, decision_id, "resolved_by", "agent:me", NOW)

    rows = await thread_closure_status(actions.pool, thread_ids=[tid])
    row = rows[0]
    assert row["closed_by_topology"] is False
    assert row["closure_edges"] == []


async def test_repo_scoping_matches_open_thread_wall_shape(actions: Actions) -> None:
    await _repo(actions, "tc7a")
    await _repo(actions, "tc7b")
    tid_a = await open_thread(actions, "in repo a", repo="tc7a", source="agent:me")
    await open_thread(actions, "in repo b", repo="tc7b", source="agent:me")
    proj_a = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='repo:tc7a'")

    rows = await thread_closure_status(actions.pool, repo=proj_a)
    assert [r["thread_id"] for r in rows] == [tid_a]


async def test_empty_scope_returns_empty_list(actions: Actions) -> None:
    import uuid
    rows = await thread_closure_status(actions.pool, thread_ids=[uuid.uuid4()])
    assert rows == []


async def test_two_strong_edges_still_report_strong(actions: Actions) -> None:
    """Both an `answers` and a `resolved_by` edge can land on the same thread (a decision
    resolved it, and a session separately named an artifact) — strength stays 'strong',
    never downgraded by the presence of a second witness."""
    await _repo(actions, "tc8")
    tid = await open_thread(actions, "double-closed", repo="tc8", source="agent:me")
    decision_id = await record_decision(actions, "settles it", repo="tc8", resolves=str(tid))
    commit_decision = await record_decision(actions, "a second unrelated ruling", repo="tc8")
    await resolve_thread(actions, str(tid), artifact=str(commit_decision)[:8])

    rows = await thread_closure_status(actions.pool, thread_ids=[tid])
    row = rows[0]
    assert row["closed_by_topology"] is True
    assert row["strength"] == "strong"
    assert len(row["closure_edges"]) == 2
    types = {e["type"] for e in row["closure_edges"]}
    assert types == {"answers", "resolved_by"}
    closers = {e["closer_id"] for e in row["closure_edges"]}
    assert closers == {decision_id, commit_decision}


async def test_enumerate_threads_single_page_no_projection(actions: Actions) -> None:
    await _repo(actions, "en1")
    ids = {await open_thread(actions, f"en1 thread {i}", repo="en1", source="agent:me")
           for i in range(3)}

    out = await enumerate_threads(actions.pool, limit=100)
    got = {r["thread_id"] for r in out["rows"] if r["thread_id"] in ids}
    assert got == ids
    assert out["next_after"] is None
    assert "_projected" not in out


async def test_enumerate_threads_pagination_covers_every_row_exactly_once(
    actions: Actions,
) -> None:
    """The property Thoth cares about most: no reshuffling, no duplicates, no drops —
    paging by cursor over o.id must reconstruct the exact scope, one row each."""
    await _repo(actions, "en2")
    ids = {await open_thread(actions, f"en2 thread {i}", repo="en2", source="agent:me")
           for i in range(5)}
    proj = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='repo:en2'")

    seen: list[uuid.UUID] = []
    after = None
    pages = 0
    while True:
        out = await enumerate_threads(actions.pool, repo=proj, limit=2, after=after)
        pages += 1
        assert out["returned"] <= 2
        seen.extend(r["thread_id"] for r in out["rows"])
        if out["next_after"] is None:
            assert "_projected" not in out
            break
        assert "_projected" in out
        assert out["_projected"]["dropped"]["threads"]["of"] == 5
        after = uuid.UUID(out["next_after"])

    assert pages == 3  # 2 + 2 + 1
    assert set(seen) == ids
    assert len(seen) == len(ids)  # no duplicates across pages
    assert seen == sorted(seen, key=str)  # ascending by id, matches ORDER BY o.id


async def test_enumerate_threads_row_shape_and_has_in_repo(actions: Actions) -> None:
    await _repo(actions, "en3")
    tid_a = await open_thread(actions, "en3 closed thread", repo="en3", source="agent:me")
    await resolve_thread(actions, str(tid_a), because="done", source="agent:closer")
    tid_b = await open_thread(actions, "en3 repo-less thread", source="agent:me")

    out = await enumerate_threads(actions.pool, limit=5000)
    by_id = {r["thread_id"]: r for r in out["rows"]}
    row_a, row_b = by_id[tid_a], by_id[tid_b]

    assert row_a["summary"] == "en3 closed thread"
    assert row_a["property_status"] == "resolved"
    assert row_a["closed_by_topology"] is True
    assert row_a["strength"] == "weak"
    assert row_a["has_in_repo"] is True

    assert row_b["property_status"] == "open"
    assert row_b["closed_by_topology"] is False
    assert row_b["has_in_repo"] is False


async def test_enumerate_threads_repo_scoping(actions: Actions) -> None:
    await _repo(actions, "en4a")
    await _repo(actions, "en4b")
    tid_a = await open_thread(actions, "en4a thread", repo="en4a", source="agent:me")
    await open_thread(actions, "en4b thread", repo="en4b", source="agent:me")
    proj_a = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='repo:en4a'")

    out = await enumerate_threads(actions.pool, repo=proj_a, limit=100)
    assert {r["thread_id"] for r in out["rows"]} == {tid_a}
