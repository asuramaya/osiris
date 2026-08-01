"""Migration 0043's `thread_closure_edges` view + its read function (Phase 2, Thoth DM
2508, decision cb38d922) — closure derived from topology, never from the multi-source
`status` property alone."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

from src.actions.core import Actions
from src.orchestrator.capture import open_thread, record_decision, resolve_thread
from src.orchestrator.thread_closure import thread_closure_status

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
