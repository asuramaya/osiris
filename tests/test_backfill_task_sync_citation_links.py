"""WAVE 2 LANE A (thread 5f47e23d, decision a55b1014): task_sync's own "TASK/THREAD
DISAGREEMENT" / "THREAD SIDE ORPHAN" obligation Threads cite the disputed Thread in their
own summary prose but were minted before `open_thread` grew its door-side prose-citation
mint (task #189) — link the citation, via Lane 0's `derive_or_abstain`, never a guess.
"""
from __future__ import annotations

import uuid
from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator.capture import open_thread
from src.orchestrator.task_sync import backfill_task_sync_citation_links


async def _disagreement_thread(actions: Actions, disputed_short_id: str) -> uuid.UUID:
    """The real `tier2_mints` disagreement summary shape, cited BEFORE the target Thread
    exists (below) — the exact ordering that leaves this Thread zero-live-link today: the
    live door-side prose-citation mint (task #189) runs once, at THIS Thread's own birth,
    finds nothing to resolve yet, and never retries. That is what this lane's own backfill
    exists to repair, so every test constructs the real orphan condition rather than one
    the live path would have already healed."""
    summary = (
        f"TASK/THREAD DISAGREEMENT: Thread {disputed_short_id} carries "
        f"property_status='open', disputed by 1 citing task(s): task 7 (store ?) "
        f"says status='completed'. task_sync never resolves this automatically "
        f"(Tier 3) — needs a human/agent look."
    )
    return await open_thread(actions, summary, kind="obligation", source="test")


async def _orphan_thread(actions: Actions, orphan_short_id: str) -> uuid.UUID:
    summary = (
        f"THREAD SIDE ORPHAN: Thread {orphan_short_id} carries kind=task but no harness "
        f"task cites it (task_sync dry run). Stale, or the harness lost track of it — "
        f"task_sync never auto-closes an orphan, needs a human/agent look."
    )
    return await open_thread(actions, summary, kind="obligation", source="test")


async def _thread_with_short_id(actions: Actions, short_id: str, summary: str) -> uuid.UUID:
    """Inserts a Thread whose id starts with the exact 8-hex prefix a test wants
    `_find_thread`'s short-id leg to match later — direct SQL, not `open_thread`, so
    minting it never itself triggers a door-side citation mint back onto the obligation
    Thread that already cited this prefix (the race this whole lane exists to close)."""
    oid = uuid.UUID(f"{short_id}-0000-0000-0000-000000000000")
    await actions.pool.execute(
        "INSERT INTO objects (id, type, canonical, status) VALUES ($1, 'Thread', $2, "
        "'active')", oid, f"thread:{short_id}0000")
    observed = datetime.now(UTC)
    await actions.assert_property(oid, "summary", summary, "test", observed, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(oid, "status", "open", "test", observed, 0.9,
                                  evidence_class="self_declared")
    return oid


async def test_backfill_dry_run_writes_nothing(actions: Actions) -> None:
    obligation = await _disagreement_thread(actions, "b76a1fef")
    disputed = await _thread_with_short_id(actions, "b76a1fef", "the disputed thing")
    out = await backfill_task_sync_citation_links(actions, actor="test", dry_run=True)
    assert out["to_mint"] == 1
    assert out["plan"][0]["to"] == str(disputed)
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1", obligation)
    assert n == 0


async def test_backfill_mints_cites_when_the_disagreement_thread_exists(
    actions: Actions,
) -> None:
    obligation = await _disagreement_thread(actions, "b76a1fef")
    disputed = await _thread_with_short_id(actions, "b76a1fef", "the disputed thing")
    out = await backfill_task_sync_citation_links(
        actions, actor="test", dry_run=False, because="test authorization")
    assert out["to_mint"] == 1
    linked = await actions.pool.fetchval(
        "SELECT to_id FROM links WHERE from_id=$1 AND type='cites'", obligation)
    assert linked == disputed


async def test_backfill_mints_cites_for_a_thread_side_orphan_too(actions: Actions) -> None:
    obligation = await _orphan_thread(actions, "cafeface")
    orphaned = await _thread_with_short_id(actions, "cafeface", "an ordinary task thread")
    out = await backfill_task_sync_citation_links(
        actions, actor="test", dry_run=False, because="test authorization")
    assert out["to_mint"] == 1
    linked = await actions.pool.fetchval(
        "SELECT to_id FROM links WHERE from_id=$1 AND type='cites'", obligation)
    assert linked == orphaned


async def test_backfill_is_idempotent(actions: Actions) -> None:
    await _disagreement_thread(actions, "b76a1fef")
    await _thread_with_short_id(actions, "b76a1fef", "the disputed thing")
    first = await backfill_task_sync_citation_links(
        actions, actor="test", dry_run=False, because="test authorization")
    second = await backfill_task_sync_citation_links(
        actions, actor="test", dry_run=False, because="test authorization")
    assert first["to_mint"] == 1
    n = await actions.pool.fetchval("SELECT count(*) FROM links WHERE type='cites'")
    assert n == 1
    assert second["scanned"] == 0  # the link now exists, so the thread is no longer an orphan


async def test_backfill_abstains_when_the_cited_thread_does_not_exist(
    actions: Actions,
) -> None:
    obligation = await _disagreement_thread(actions, "ffffffff")
    out = await backfill_task_sync_citation_links(
        actions, actor="test", dry_run=False, because="test authorization")
    assert out["to_abstain"] == 1
    n = await actions.pool.fetchval("SELECT count(*) FROM links WHERE from_id=$1", obligation)
    assert n == 0
    reason = await actions.pool.fetchval(
        "SELECT a.value FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='derivation_abstained_cites'", obligation)
    assert reason is not None and reason["candidate_count"] == 0
    assert "resolves to no existing Thread" in reason["reason"]


async def test_backfill_abstains_on_an_ambiguous_short_id_never_guessing(
    actions: Actions,
) -> None:
    """Two Threads sharing the same 8-char id prefix — `_find_thread`'s own RefAmbiguous —
    must abstain, candidate ids kept, rather than pick one. Both targets are minted BEFORE
    the citing obligation Thread here (unlike the clean-mint tests above) precisely so the
    live door-side mint ALSO sees the ambiguity and skips it the same way — an ambiguous
    citation is refused at every stage, never just this backfill's own."""
    aaa1 = await _thread_with_short_id(actions, "aaaaaaaa", "target one")
    aaa2_id = uuid.UUID("aaaaaaaa-0000-0000-0000-000000000002")
    await actions.pool.execute(
        "INSERT INTO objects (id, type, canonical, status) VALUES ($1, 'Thread', "
        "'thread:aaaaaaaa0002', 'active')", aaa2_id)
    observed = datetime.now(UTC)
    await actions.assert_property(aaa2_id, "summary", "target two", "test", observed, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(aaa2_id, "status", "open", "test", observed, 0.9,
                                  evidence_class="self_declared")
    obligation = await _disagreement_thread(actions, "aaaaaaaa")
    out = await backfill_task_sync_citation_links(
        actions, actor="test", dry_run=False, because="test authorization")
    assert out["to_abstain"] == 1
    n = await actions.pool.fetchval("SELECT count(*) FROM links WHERE from_id=$1", obligation)
    assert n == 0
    reason = await actions.pool.fetchval(
        "SELECT a.value FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='derivation_abstained_cites'", obligation)
    assert reason is not None
    assert reason["candidate_count"] == 2
    assert set(reason["candidates"]) == {str(aaa1), str(aaa2_id)}


async def test_backfill_abstains_when_the_summary_has_no_citation(actions: Actions) -> None:
    obligation = await open_thread(
        actions, "TASK/THREAD DISAGREEMENT: no thread named here at all — malformed",
        kind="obligation", source="test")
    out = await backfill_task_sync_citation_links(
        actions, actor="test", dry_run=False, because="test authorization")
    assert out["to_abstain"] == 1
    reason = await actions.pool.fetchval(
        "SELECT a.value FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='derivation_abstained_cites'", obligation)
    assert reason is not None
    assert reason["reason"] == (
        "no Thread short-id citation found in this Thread's own summary")


async def test_backfill_never_touches_a_thread_that_already_has_a_link(
    actions: Actions,
) -> None:
    """Scoped to zero-live-link orphans only — a disagreement thread someone already
    linked (by hand or a prior pass) is out of this repair's population entirely."""
    obligation = await _disagreement_thread(actions, "b76a1fef")
    disputed = await _thread_with_short_id(actions, "b76a1fef", "the disputed thing")
    await actions.create_link(obligation, disputed, "cites", "test", datetime.now(UTC),
                              0.6, evidence_class="direct_observation")
    out = await backfill_task_sync_citation_links(
        actions, actor="test", dry_run=False, because="test authorization")
    assert out["scanned"] == 0


async def test_backfill_ignores_threads_outside_the_task_sync_shape(actions: Actions) -> None:
    """A zero-live-link Thread that merely mentions a (nonexistent, so the live door-side
    mint leaves it unlinked) short id in unrelated prose is not this lane's population at
    all — only task_sync's own two fixed summary prefixes qualify."""
    await open_thread(actions, "unrelated note about thread ffffffff", kind="task",
                      source="test")
    out = await backfill_task_sync_citation_links(actions, actor="test", dry_run=True)
    assert out["scanned"] == 0


async def test_backfill_requires_a_because_to_execute(actions: Actions) -> None:
    await _disagreement_thread(actions, "b76a1fef")
    await _thread_with_short_id(actions, "b76a1fef", "the disputed thing")
    out = await backfill_task_sync_citation_links(actions, actor="test", dry_run=False)
    assert "error" in out
    n = await actions.pool.fetchval("SELECT count(*) FROM links WHERE type='cites'")
    assert n == 0
