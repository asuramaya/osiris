"""ONE-TIME FLEET SEED (thread 4dcc1849, decision f9e47d3c, Thoth DM 7763 item (a)):
attribute every currently-active seat's memory dir to its true current holder before
the archive rule ships, so the first mount() after deploy doesn't flood every live
session with memory_migration_needed."""
from __future__ import annotations

from pathlib import Path

from src.actions.core import Actions
from src.orchestrator.lineage_memory import (
    apply_lineage_memory_seed,
    claude_memory_dir,
    plan_lineage_memory_seed,
)
from src.orchestrator.mounts import save_mount
from src.orchestrator.seats import bind_holder, ensure_seat


async def test_plan_seeds_only_directories_that_exist_with_no_sentinel(
    actions: Actions, tmp_path: Path,
) -> None:
    cwd = str(tmp_path / "o")
    Path(cwd).mkdir()
    claude_memory_dir(cwd, home=tmp_path).mkdir(parents=True)  # exists, no sentinel yet

    seat = await ensure_seat(actions, house="osiris", handle="Rseed1", source="test",
                             anchor_cwd=cwd)
    await actions.create_or_find_object("Agent", "agent:seedlive-vii", "test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:seedlive-vii")
    await save_mount(actions.pool, job_dir="/jobs/seedlive", agent_id="agent:seedlive-vii",
                     project="osiris", cwd=cwd, model="claude-sonnet-5", session_key=None)

    plan = await plan_lineage_memory_seed(actions.pool, home=tmp_path)

    assert len(plan) == 1
    assert plan[0]["cwd"] == cwd
    assert plan[0]["lineage_root"] == "agent:seedlive"
    assert plan[0]["handle"] == "Rseed1"


async def test_plan_skips_a_directory_that_already_has_a_sentinel(
    actions: Actions, tmp_path: Path,
) -> None:
    cwd = str(tmp_path / "o")
    mem_dir = claude_memory_dir(cwd, home=tmp_path)
    mem_dir.mkdir(parents=True)
    (mem_dir / ".osiris-lineage").write_text("agent:already", encoding="utf-8")

    seat = await ensure_seat(actions, house="osiris", handle="Rseed2", source="test",
                             anchor_cwd=cwd)
    await actions.create_or_find_object("Agent", "agent:seedlive2-vii", "test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:seedlive2-vii")
    await save_mount(actions.pool, job_dir="/jobs/seedlive2", agent_id="agent:seedlive2-vii",
                     project="osiris", cwd=cwd, model="claude-sonnet-5", session_key=None)

    plan = await plan_lineage_memory_seed(actions.pool, home=tmp_path)
    assert plan == []


async def test_plan_skips_vacant_and_cold_seats(actions: Actions, tmp_path: Path) -> None:
    """No live holder to attribute a fresh sentinel to — a cold/vacant seat's memory
    (if any) is left for whoever mounts there next to see honestly as migration_needed,
    same as any other unattributed content."""
    cwd = str(tmp_path / "o")
    claude_memory_dir(cwd, home=tmp_path).mkdir(parents=True)

    vacant = await ensure_seat(actions, house="osiris", handle="Rseedvacant", source="test",
                               anchor_cwd=str(tmp_path / "vacant-o"))
    cold = await ensure_seat(actions, house="osiris", handle="Rseedcold", source="test",
                             anchor_cwd=cwd)
    await actions.create_or_find_object("Agent", "agent:coldholder", "test")
    await bind_holder(actions, seat_id=cold["seat_id"], agent_id="agent:coldholder")
    # no save_mount for either — vacant has no holder at all, cold has one but no live mount

    plan = await plan_lineage_memory_seed(actions.pool, home=tmp_path)
    assert plan == []
    _ = vacant


async def test_plan_dedupes_a_seat_whose_anchor_and_tree_cwd_are_the_same(
    actions: Actions, tmp_path: Path,
) -> None:
    """anchor_cwd/tree_cwd/live_cwd frequently coincide (no worktree in use) — the same
    physical directory must appear once in the plan, not once per field it happens to
    also be named in."""
    cwd = str(tmp_path / "o")
    claude_memory_dir(cwd, home=tmp_path).mkdir(parents=True)

    seat = await ensure_seat(actions, house="osiris", handle="Rseeddup", source="test",
                             anchor_cwd=cwd)
    await actions.create_or_find_object("Agent", "agent:dupholder-vii", "test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:dupholder-vii")
    await save_mount(actions.pool, job_dir="/jobs/dupholder", agent_id="agent:dupholder-vii",
                     project="osiris", cwd=cwd, model="claude-sonnet-5", session_key=None)

    plan = await plan_lineage_memory_seed(actions.pool, home=tmp_path)
    assert len(plan) == 1


async def test_apply_writes_the_sentinel_for_each_planned_entry(tmp_path: Path) -> None:
    cwd = str(tmp_path / "o")
    claude_memory_dir(cwd, home=tmp_path).mkdir(parents=True)
    plan = [{"seat": "seat:x", "handle": "X", "cwd": cwd,
            "memory_dir": str(claude_memory_dir(cwd, home=tmp_path)),
            "lineage_root": "agent:seededroot"}]

    apply_lineage_memory_seed(plan, home=tmp_path)

    sentinel = claude_memory_dir(cwd, home=tmp_path) / ".osiris-lineage"
    assert sentinel.read_text(encoding="utf-8") == "agent:seededroot"
