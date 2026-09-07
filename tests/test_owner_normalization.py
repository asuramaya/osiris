"""OWNER NORMALIZATION (thread 6d8f87a3, decision 0d863363 item 2) -- classify_thread_
owner's three rewritable shapes (project name, dead agent, empty-with-repo), the
already-durable shapes it must leave untouched, and the plan/apply split's own folding
of unresolvable rows into one operator thread per project.
"""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator.charter import set_charter
from src.orchestrator.owner_normalization import (
    MIGRATION_SOURCE,
    apply_owner_normalization,
    classify_thread_owner,
    plan_owner_normalization,
)
from src.orchestrator.seats import ensure_seat

NOW = datetime.now(UTC)
_SRC = "test-source"


async def _repo(actions: Actions, name: str) -> None:
    await actions.create_or_find_object("SoftwareProject", f"repo:{name}", "test")


async def _seat(actions: Actions, handle: str, *, house: str = "test") -> str:
    out = await ensure_seat(actions, house=house, handle=handle, source="test")
    return str(out["seat_id"])


async def _obligation(
    actions: Actions, canonical: str, *, owner: str | None = None, repo: str | None = None,
) -> str:
    t = await actions.create_or_find_object("Thread", canonical, _SRC)
    await actions.assert_property(t, "summary", canonical, _SRC, NOW, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(t, "status", "open", _SRC, NOW, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(t, "kind", "obligation", _SRC, NOW, 0.9,
                                  evidence_class="self_declared")
    if owner is not None:
        await actions.assert_property(t, "owner", owner, _SRC, NOW, 0.9,
                                      evidence_class="self_declared")
    if repo is not None:
        await actions.assert_property(t, "repo", repo, _SRC, NOW, 0.9,
                                      evidence_class="self_declared")
    return canonical


# ═══ classify_thread_owner: the three rewritable shapes ════════════════════════════════

async def test_project_name_owner_resolves_to_its_single_charter_seat(
    actions: Actions,
) -> None:
    await _repo(actions, "onown-projA")
    seat_id = await _seat(actions, "OnownSeatA")
    await set_charter(actions, seat_id, ["onown-projA"], actor="test")

    out = await classify_thread_owner(actions.pool, "onown-projA", None)
    assert out == {"class": "project_name", "new_owner": seat_id, "project": "onown-projA",
                   "reason": None}


async def test_dead_agent_owner_resolves_to_its_lineage_head(actions: Actions) -> None:
    ancestor = await actions.create_or_find_object("Agent", "agent:onown-lineage-i", "test")
    await actions.create_or_find_object("Agent", "agent:onown-lineage-ii", "test")
    await actions.assert_property(ancestor, "succeeded_by", "agent:onown-lineage-ii", _SRC,
                                  NOW, 0.9, evidence_class="self_declared")

    out = await classify_thread_owner(actions.pool, "agent:onown-lineage-i", None)
    assert out == {"class": "dead_agent", "new_owner": "agent:onown-lineage-ii",
                   "project": None, "reason": None}


async def test_empty_owner_with_a_repo_resolves_like_a_project_name(
    actions: Actions,
) -> None:
    await _repo(actions, "onown-projB")
    seat_id = await _seat(actions, "OnownSeatB")
    await set_charter(actions, seat_id, ["onown-projB"], actor="test")

    out = await classify_thread_owner(actions.pool, None, "onown-projB")
    assert out == {"class": "empty", "new_owner": seat_id, "project": "onown-projB",
                   "reason": None}
    out2 = await classify_thread_owner(actions.pool, "  ", "onown-projB")  # blank, not None
    assert out2["new_owner"] == seat_id


async def test_empty_owner_with_no_repo_at_all_has_nothing_to_resolve_from(
    actions: Actions,
) -> None:
    out = await classify_thread_owner(actions.pool, None, None)
    assert out["class"] == "empty"
    assert out["new_owner"] is None
    assert "nothing to resolve" in out["reason"]


# ═══ already-durable shapes: never touched ══════════════════════════════════════════════

async def test_operator_owner_is_left_alone(actions: Actions) -> None:
    out = await classify_thread_owner(actions.pool, "operator", None)
    assert out["class"] == "ok" and out["new_owner"] is None


async def test_seat_canonical_owner_is_left_alone(actions: Actions) -> None:
    out = await classify_thread_owner(actions.pool, "seat:onown0001", None)
    assert out["class"] == "ok" and out["new_owner"] is None


async def test_a_live_agent_owner_with_no_successor_is_left_alone(
    actions: Actions,
) -> None:
    await actions.create_or_find_object("Agent", "agent:onown-live", "test")
    out = await classify_thread_owner(actions.pool, "agent:onown-live", None)
    assert out["class"] == "ok" and out["new_owner"] is None


async def test_a_bare_seat_handle_owner_is_left_alone_even_when_a_same_spelled_project_exists(
    actions: Actions,
) -> None:
    """Rung-0-before-project-name (obligation_hygiene.py's own precedent, msg 7425): an
    owner string is a seat before it is a repo. This migration must never rewrite a bare
    seat-handle owner into that same seat's own canonical, or worse, chase a same-spelled
    project instead."""
    await _repo(actions, "OnownSeatC")  # a project sharing the seat's own spelling
    await _seat(actions, "OnownSeatC")

    out = await classify_thread_owner(actions.pool, "onownseatc", None)  # case-folded
    assert out["class"] == "ok" and out["new_owner"] is None


# ═══ unresolvable: never guessed ═════════════════════════════════════════════════════════

async def test_a_project_name_with_no_charter_at_all_has_no_coordinator(
    actions: Actions,
) -> None:
    await _repo(actions, "onown-orphan")  # real project, nobody charters it

    out = await classify_thread_owner(actions.pool, "onown-orphan", None)
    assert out["class"] == "project_name"
    assert out["new_owner"] is None
    assert out["project"] == "onown-orphan"
    assert "no seat's charter or pin names" in out["reason"]


# ═══ plan_owner_normalization / apply_owner_normalization: end to end ═══════════════════

async def test_plan_separates_resolved_from_no_coordinator_and_leaves_ok_rows_out(
    actions: Actions,
) -> None:
    await _repo(actions, "onown-plan1")
    seat_id = await _seat(actions, "OnownPlanSeat")
    await set_charter(actions, seat_id, ["onown-plan1"], actor="test")
    resolved_thread = await _obligation(actions, "onown-thread-resolved",
                                        owner="onown-plan1")
    await _repo(actions, "onown-orphan2")
    orphan_thread = await _obligation(actions, "onown-thread-orphan", owner="onown-orphan2")
    ok_thread = await _obligation(actions, "onown-thread-ok", owner="operator")

    out = await plan_owner_normalization(actions.pool)
    resolved_ids = {e["thread"] for e in out["resolved"]}
    assert resolved_thread in resolved_ids
    assert ok_thread not in resolved_ids
    orphan_entries = out["no_coordinator"]["onown-orphan2"]
    assert {e["thread"] for e in orphan_entries} == {orphan_thread}


async def test_apply_writes_a_compensating_owner_assertion_keeping_the_prior_in_history(
    actions: Actions,
) -> None:
    await _repo(actions, "onown-apply1")
    seat_id = await _seat(actions, "OnownApplySeat")
    await set_charter(actions, seat_id, ["onown-apply1"], actor="test")
    t = await _obligation(actions, "onown-thread-apply1", owner="onown-apply1")

    await apply_owner_normalization(actions)

    rows = await actions.pool.fetch(
        "SELECT value #>> '{}' AS value, source_id FROM current_assertions "
        "WHERE object_id=(SELECT id FROM objects WHERE canonical=$1) AND name='owner'", t)
    by_source = {r["source_id"]: r["value"] for r in rows}
    assert by_source[_SRC] == "onown-apply1"          # the prior owner, still current
    assert by_source[MIGRATION_SOURCE] == seat_id      # the new, durable owner


async def test_apply_is_idempotent_in_result_across_two_runs(actions: Actions) -> None:
    await _repo(actions, "onown-apply2")
    seat_id = await _seat(actions, "OnownApplySeat2")
    await set_charter(actions, seat_id, ["onown-apply2"], actor="test")
    t = await _obligation(actions, "onown-thread-apply2", owner="onown-apply2")

    await apply_owner_normalization(actions)
    await apply_owner_normalization(actions)

    rows = await actions.pool.fetch(
        "SELECT value #>> '{}' AS value FROM current_assertions "
        "WHERE object_id=(SELECT id FROM objects WHERE canonical=$1) AND name='owner' "
        "AND source_id=$2", t, MIGRATION_SOURCE)
    assert {r["value"] for r in rows} == {seat_id}    # never drifted to a different value


async def test_apply_folds_every_orphaned_obligation_of_one_project_into_a_single_thread(
    actions: Actions,
) -> None:
    await _repo(actions, "onown-fold1")
    t1 = await _obligation(actions, "onown-fold-thread-1", owner="onown-fold1")
    t2 = await _obligation(actions, "onown-fold-thread-2", owner="onown-fold1")

    before = await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Thread' AND canonical LIKE "
        "'thread:%' AND status='active'")
    out = await apply_owner_normalization(actions)
    after = await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Thread' AND canonical LIKE "
        "'thread:%' AND status='active'")

    fold_entries = [s for s in out["surfaced"] if s["project"] == "onown-fold1"]
    assert len(fold_entries) == 1
    assert fold_entries[0]["obligation_count"] == 2
    assert after - before == 1                        # one new Thread, not two
    assert t1 and t2                                   # both obligations were seen


async def test_apply_does_not_mint_a_second_fold_thread_on_a_second_run(
    actions: Actions,
) -> None:
    await _repo(actions, "onown-fold2")
    await _obligation(actions, "onown-fold2-thread", owner="onown-fold2")

    out1 = await apply_owner_normalization(actions)
    out2 = await apply_owner_normalization(actions)

    id1 = next(s["thread"] for s in out1["surfaced"] if s["project"] == "onown-fold2")
    id2 = next(s["thread"] for s in out2["surfaced"] if s["project"] == "onown-fold2")
    assert id1 == id2                                  # open_thread's own dedup-on-summary
