"""Ghost house-stamp retirement (thread a732e331 clause 3, wave 7 dispatch msg 8079): the
write-time sibling of derive_house's own read-time ghost clause (Khnum's bb1cdc2) -- see
src/orchestrator/house_hygiene.py's own docstring.
"""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.actions.core import Actions
from src.orchestrator.house_hygiene import apply_ghost_house_sweep

NOW = datetime.now(UTC)


async def _link_managed_by(actions: Actions, worker: Any, manager: Any) -> None:
    await actions.create_link(worker, manager, "managed_by", "test", NOW, 0.9,
                              evidence_class="self_declared")


async def _current_house(actions: Actions, seat_canonical: str) -> str | None:
    return await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='house' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", seat_canonical)


async def test_a_managed_ghost_stamp_is_retired(actions: Actions) -> None:
    manager = await actions.create_or_find_object("Seat", "seat:hh1mgr00", "test")
    await actions.assert_property(manager, "house", "monsterhouse", "test", NOW, 0.9)
    worker = await actions.create_or_find_object("Seat", "seat:hh1wrk00", "test")
    project = await actions.create_or_find_object("SoftwareProject", "repo:hh1wrk00", "test")
    await actions.create_link(worker, project, "governs", "test", NOW, 0.9)
    await actions.assert_property(worker, "house", "Hh1wrk00", "operator", NOW, 0.9)
    await _link_managed_by(actions, worker, manager)

    report = await apply_ghost_house_sweep(actions)

    assert "seat:hh1wrk00" in report["retired"]
    assert await _current_house(actions, "seat:hh1wrk00") == ""


async def test_a_heads_own_matching_stamp_survives(actions: Actions) -> None:
    """DELIBERATELY BLIND ON A HEAD -- a head's house equal to its own flagship project
    (Thoth's own 'osiris' governing 'osiris') is the ordinary, legitimate case."""
    head = await actions.create_or_find_object("Seat", "seat:hh2head0", "test")
    project = await actions.create_or_find_object("SoftwareProject", "repo:hh2head0", "test")
    await actions.create_link(head, project, "governs", "test", NOW, 0.9)
    await actions.assert_property(head, "house", "hh2head0", "test", NOW, 0.9)

    report = await apply_ghost_house_sweep(actions)

    assert "seat:hh2head0" not in report["retired"]
    assert await _current_house(actions, "seat:hh2head0") == "hh2head0"


async def test_a_managed_seats_real_differing_house_survives(actions: Actions) -> None:
    manager = await actions.create_or_find_object("Seat", "seat:hh3mgr00", "test")
    await actions.assert_property(manager, "house", "monsterhouse", "test", NOW, 0.9)
    worker = await actions.create_or_find_object("Seat", "seat:hh3wrk00", "test")
    project = await actions.create_or_find_object("SoftwareProject", "repo:hh3wrk00", "test")
    await actions.create_link(worker, project, "governs", "test", NOW, 0.9)
    await actions.assert_property(worker, "house", "hector-vector", "operator", NOW, 0.9)
    await _link_managed_by(actions, worker, manager)

    report = await apply_ghost_house_sweep(actions)

    assert "seat:hh3wrk00" not in report["retired"]
    assert await _current_house(actions, "seat:hh3wrk00") == "hector-vector"


async def test_an_unmanaged_seat_with_no_house_stamp_is_never_scanned_in(
    actions: Actions,
) -> None:
    await actions.create_or_find_object("Seat", "seat:hh4head0", "test")

    report = await apply_ghost_house_sweep(actions)

    assert report["retired"] == []


async def test_a_second_sweep_retires_nothing_new(actions: Actions) -> None:
    manager = await actions.create_or_find_object("Seat", "seat:hh5mgr00", "test")
    await actions.assert_property(manager, "house", "monsterhouse", "test", NOW, 0.9)
    worker = await actions.create_or_find_object("Seat", "seat:hh5wrk00", "test")
    project = await actions.create_or_find_object("SoftwareProject", "repo:hh5wrk00", "test")
    await actions.create_link(worker, project, "governs", "test", NOW, 0.9)
    await actions.assert_property(worker, "house", "hh5wrk00", "operator", NOW, 0.9)
    await _link_managed_by(actions, worker, manager)

    first = await apply_ghost_house_sweep(actions)
    second = await apply_ghost_house_sweep(actions)

    assert first["retired"] == ["seat:hh5wrk00"]
    assert second["retired"] == []
