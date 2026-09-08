"""Project hygiene sweep (thread 14fae7d3, wave 6 dispatch msg 8063): SoftwareProject
junk retired the same way migration_0060's EXPIRY law retires an unclaimed derived Thread
-- see src/orchestrator/project_hygiene.py's own docstring for the two doors (ongoing rule,
one-shot legacy backlog) and the shared guard (no thread/decision/governing seat).
"""
from __future__ import annotations

import uuid
from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator.charter import set_charter
from src.orchestrator.project_hygiene import (
    LEGACY_JUNK_PROJECT_CANONICALS,
    apply_project_hygiene_sweep,
)
from src.orchestrator.seats import ensure_seat

NOW = datetime.now(UTC)


async def _project(actions: Actions, name: str, *, actor: str) -> str:
    canon = f"repo:{name}"
    await actions.create_or_find_object("SoftwareProject", canon, actor)
    return canon


async def _status(actions: Actions, canonical: str) -> str:
    return str(await actions.pool.fetchval(
        "SELECT status FROM objects WHERE canonical=$1", canonical))


# ═══ door (1): the ongoing mechanical rule ═══════════════════════════════════════════════

async def test_a_test_minted_stub_with_no_claims_is_retired(actions: Actions) -> None:
    """The literal test-fixture convention this thread mandates: a fixture that mints a
    throwaway SoftwareProject with actor='test' and leaves no thread/decision/governing
    seat behind is swept -- the acceptance test the dispatch itself asked for."""
    canon = await _project(actions, "hygiene-test-stub", actor="test")

    report = await apply_project_hygiene_sweep(actions)

    assert canon in report["retired"]
    assert await _status(actions, canon) == "retired"


async def test_a_disk_census_stub_whose_path_is_gone_is_retired(actions: Actions) -> None:
    canon = await _project(actions, "hygiene-census-gone", actor="disk-census")
    tid = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical=$1", canon)
    await actions.assert_property(tid, "on_disk_path", "/nonexistent/hygiene-census-gone",
                                  "disk-census", NOW, 0.9, evidence_class="direct_observation")

    report = await apply_project_hygiene_sweep(actions)

    assert canon in report["retired"]


async def test_a_disk_census_stub_whose_path_still_exists_is_left_alone(
    actions: Actions,
) -> None:
    canon = await _project(actions, "hygiene-census-present", actor="disk-census")
    tid = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical=$1", canon)
    await actions.assert_property(tid, "on_disk_path", "/",  # always present
                                  "disk-census", NOW, 0.9, evidence_class="direct_observation")

    report = await apply_project_hygiene_sweep(actions)

    assert canon not in report["retired"]
    assert await _status(actions, canon) == "active"


async def test_a_non_test_non_census_stub_never_matches_the_ongoing_rule(
    actions: Actions,
) -> None:
    canon = await _project(actions, "hygiene-real-looking", actor="agent:some-real-agent")

    report = await apply_project_hygiene_sweep(actions)

    assert canon not in report["retired"]


# ═══ shared guard: no thread / decision / governing seat ═════════════════════════════════

async def test_guard_an_open_thread_pointing_in_blocks_retirement(actions: Actions) -> None:
    canon = await _project(actions, "hygiene-guard-thread", actor="test")
    pid = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical=$1", canon)
    tid = await actions.create_or_find_object("Thread", "hygiene-guard-thread-t", "test")
    await actions.assert_property(tid, "status", "open", "test", NOW, 0.9,
                                  evidence_class="self_declared")
    await actions.create_link(tid, pid, "in_repo", "test", NOW, 0.9,
                              evidence_class="self_declared")

    report = await apply_project_hygiene_sweep(actions)

    assert canon not in report["retired"]
    assert any(g["project"] == canon for g in report["guarded"])


async def test_guard_a_decision_pointing_in_blocks_retirement(actions: Actions) -> None:
    canon = await _project(actions, "hygiene-guard-decision", actor="test")
    pid = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical=$1", canon)
    did = await actions.create_or_find_object("Decision", "decision:hygiene-guard", "test")
    await actions.create_link(did, pid, "in_repo", "test", NOW, 0.9,
                              evidence_class="self_declared")

    report = await apply_project_hygiene_sweep(actions)

    assert canon not in report["retired"]
    assert any(g["project"] == canon for g in report["guarded"])


async def test_guard_a_governing_seat_blocks_retirement(actions: Actions) -> None:
    canon = await _project(actions, "hygiene-guard-governs", actor="test")
    name = canon.removeprefix("repo:")
    seat_id = await ensure_seat(actions, house="test", handle="HygieneGuardSeat",
                                source="test")
    await set_charter(actions, str(seat_id["seat_id"]), [name], actor="operator")

    report = await apply_project_hygiene_sweep(actions)

    assert canon not in report["retired"]
    assert any(g["project"] == canon for g in report["guarded"])


# ═══ door (2): the one-shot legacy backlog ═══════════════════════════════════════════════

async def test_a_legacy_named_canonical_is_retired_even_with_a_non_test_actor(
    actions: Actions,
) -> None:
    name = next(iter(LEGACY_JUNK_PROJECT_CANONICALS)).removeprefix("repo:")
    canon = await _project(actions, name, actor=f"agent:{uuid.uuid4()}")

    report = await apply_project_hygiene_sweep(actions)

    assert canon in report["retired"]


async def test_a_legacy_named_canonical_still_respects_the_shared_guard(
    actions: Actions,
) -> None:
    name = "khnum-launch-acceptance-4-analog"
    canon = f"repo:{name}"
    # simulate the real khnum-launch-acceptance-4 exception: a legacy name WOULD match if
    # listed, but here we prove the guard, not the list -- attach a decision to a
    # test-actor project and confirm the guard alone is enough to exclude it.
    await _project(actions, name, actor="test")
    pid = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical=$1", canon)
    did = await actions.create_or_find_object("Decision", "decision:khnum-analog", "test")
    await actions.create_link(did, pid, "in_repo", "test", NOW, 0.9,
                              evidence_class="self_declared")

    report = await apply_project_hygiene_sweep(actions)

    assert canon not in report["retired"]


# ═══ idempotence ══════════════════════════════════════════════════════════════════════════

async def test_a_second_sweep_retires_nothing_new(actions: Actions) -> None:
    await _project(actions, "hygiene-idempotent", actor="test")

    first = await apply_project_hygiene_sweep(actions)
    second = await apply_project_hygiene_sweep(actions)

    assert len(first["retired"]) == 1
    assert second["retired"] == []


# ═══ the dispatch's own acceptance test: no fixture leaves an active project behind ═══════

async def test_no_test_actor_fixture_leaves_an_active_project_behind(actions: Actions) -> None:
    """The dispatch's own explicit ask: a test fixture that mints a SoftwareProject under
    the source='test' convention and never tears it down by hand is still swept clean by
    this sweep -- the mechanical backstop test fixtures may now rely on instead of a
    hand-written teardown in every single one."""
    for name in ("fixture-leftover-a", "fixture-leftover-b", "fixture-leftover-c"):
        await _project(actions, name, actor="test")

    await apply_project_hygiene_sweep(actions)

    left = await actions.pool.fetchval(
        "SELECT count(*) FROM objects o WHERE o.type='SoftwareProject' AND o.status='active' "
        "AND EXISTS (SELECT 1 FROM object_events oe WHERE oe.object_id=o.id "
        "AND oe.event_type='create' AND oe.actor='test')")
    assert left == 0
