"""One-time harness-board -> graph migration (decision d83804c8, ruling 50c3ed90, msg 4408).
plan_migration/aggregate_line are pure — no DB, no writes — tested directly. apply_migration
touches the graph and is exercised only by its own callers when actually authorized; not
covered here (this module's whole point is that it is NOT called yet)."""
from __future__ import annotations

from src.orchestrator.capture import ARCS
from src.orchestrator.roadmap_migration import (
    ARC_BY_TASK_ID,
    aggregate_line,
    plan_migration,
)

STORE = "store-a"


def _task(id_: str, subject: str = "x", description: str = "", status: str = "pending",
          blocks: list[str] | None = None, blocked_by: list[str] | None = None) -> dict:
    return {"id": id_, "subject": subject, "description": description, "status": status,
            "blocks": blocks or [], "blockedBy": blocked_by or []}


# ── plan_migration: completed -> Decision, live -> Thread ──────────────────────────────

def test_completed_row_becomes_a_decision_plan_entry() -> None:
    plan = plan_migration([_task("1", "Fix the thing", "long finding", status="completed")],
                          store=STORE)
    assert plan["counts"]["decisions"] == 1
    assert plan["counts"]["threads"] == 0
    entry = plan["decisions"][0]
    assert entry["task_id"] == "1"
    assert entry["summary"] == "Fix the thing"
    assert entry["rationale"] == "long finding"


def test_pending_row_becomes_a_thread_plan_entry() -> None:
    plan = plan_migration([_task("2", "Do the thing", "details", status="pending")],
                          store=STORE)
    assert plan["counts"]["threads"] == 1
    assert plan["counts"]["decisions"] == 0
    entry = plan["threads"][0]
    assert entry["task_id"] == "2"
    assert "Do the thing" in entry["summary"]
    assert "details" in entry["summary"]
    assert entry["harness_status"] == "pending"


def test_in_progress_row_also_becomes_a_thread_plan_entry() -> None:
    plan = plan_migration([_task("3", status="in_progress")], store=STORE)
    assert plan["counts"]["threads"] == 1
    assert plan["threads"][0]["harness_status"] == "in_progress"


def test_every_row_lands_in_exactly_one_plan() -> None:
    tasks = [_task("1", status="completed"), _task("2", status="pending"),
              _task("3", status="in_progress")]
    plan = plan_migration(tasks, store=STORE)
    assert plan["counts"]["total"] == 3
    assert plan["counts"]["decisions"] + plan["counts"]["threads"] == 3


# ── legacy_task_ref: the queryable address, always {store, id} ─────────────────────────

def test_every_plan_entry_carries_a_complete_legacy_task_ref() -> None:
    tasks = [_task("1", status="completed"), _task("2", status="pending")]
    plan = plan_migration(tasks, store="my-store")
    for entry in plan["decisions"] + plan["threads"]:
        assert entry["legacy_task_ref"] == {"store": "my-store", "id": entry["task_id"]}


def test_legacy_task_ref_uses_the_caller_supplied_store_not_a_guess() -> None:
    plan = plan_migration([_task("9", status="pending")], store="store-b")
    assert plan["threads"][0]["legacy_task_ref"]["store"] == "store-b"


# ── legacy_task_dependencies: the blocks/blockedBy DAG, preserved not invented ─────────

def test_dependency_free_row_carries_no_dependencies_payload() -> None:
    plan = plan_migration([_task("1", status="pending")], store=STORE)
    assert plan["threads"][0]["legacy_task_dependencies"] is None


def test_row_with_blocks_carries_the_dependencies_payload() -> None:
    plan = plan_migration([_task("1", status="pending", blocks=["2"])], store=STORE)
    assert plan["threads"][0]["legacy_task_dependencies"] == {"blocks": ["2"], "blockedBy": []}


def test_row_with_blocked_by_carries_the_dependencies_payload() -> None:
    plan = plan_migration([_task("2", status="completed", blocked_by=["1"])], store=STORE)
    assert plan["decisions"][0]["legacy_task_dependencies"] == {"blocks": [], "blockedBy": ["1"]}


# ── arc: closed taxonomy, honest-empty over confident-wrong ────────────────────────────

def test_arc_map_only_ever_names_values_in_the_closed_taxonomy() -> None:
    assert set(ARC_BY_TASK_ID.values()) <= set(ARCS)


def test_a_row_with_a_mapped_arc_gets_it_in_the_plan() -> None:
    mapped_id = next(iter(ARC_BY_TASK_ID))
    plan = plan_migration([_task(mapped_id, status="pending")], store=STORE)
    assert plan["threads"][0]["arc"] == ARC_BY_TASK_ID[mapped_id]


def test_an_unmapped_row_gets_no_arc_rather_than_a_guess() -> None:
    plan = plan_migration([_task("not-in-the-map-999", status="pending")], store=STORE)
    assert plan["threads"][0]["arc"] is None


def test_unsorted_count_matches_the_unmapped_threads() -> None:
    tasks = [_task("30", status="pending"), _task("not-mapped", status="pending")]
    plan = plan_migration(tasks, store=STORE)
    assert plan["counts"]["unsorted"] == 1


def test_completed_rows_never_consult_the_arc_map() -> None:
    # #30 is arc-mapped, but as a completed row it becomes a Decision, which has no arc
    # field at all — ARCS is a Thread-only taxonomy (decision d83804c8).
    plan = plan_migration([_task("30", status="completed")], store=STORE)
    assert plan["counts"]["threads"] == 0
    assert "arc" not in plan["decisions"][0]


# ── aggregate_line: one line, never per-row noise (Khnum's batch rule) ─────────────────

def test_aggregate_line_reports_total_and_unsorted_percentage() -> None:
    tasks = [_task("1", status="completed"), _task("2", status="pending"),
              _task("3", status="pending")]
    plan = plan_migration(tasks, store=STORE)
    line = aggregate_line(plan["counts"])
    assert "migrated 3" in line
    assert "1 decisions" in line
    assert "2 threads" in line
    assert "unsorted" in line


def test_aggregate_line_handles_zero_threads_without_dividing_by_zero() -> None:
    plan = plan_migration([_task("1", status="completed")], store=STORE)
    line = aggregate_line(plan["counts"])
    assert "0 unsorted (0% of threads)" in line
