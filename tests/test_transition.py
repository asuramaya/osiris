"""THE SELF-SERVICE TRANSITION VERB (Thoth dispatch 6901, the Jesus/Chad specimen) —
proves the compose-not-orchestrate contract: invalidate_works_in + correct_own_pin_value
+ set_charter, atomic-or-refused, never rebind_seat (THE ANCHOR INVARIANT, ruling
23771416 — that call is exactly what broke Jesus's and Chad's own anchors).
"""
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from src.actions.core import Actions
from src.orchestrator.charter import charter_of
from src.orchestrator.transition import transition_seat_project

NOW = datetime.now(UTC)


async def _agent(actions: Actions, canonical: str) -> None:
    await actions.create_or_find_object("Agent", canonical, canonical)


async def _repo(actions: Actions, name: str) -> str:
    await actions.create_or_find_object("SoftwareProject", f"repo:{name}", "test")
    return f"repo:{name}"


async def _seated(actions: Actions, agent_id: str, handle: str) -> str:
    from src.orchestrator.seats import bind_holder, ensure_seat

    out = await ensure_seat(actions, house="osiris", handle=handle, source="test")
    seat_id = str(out["seat_id"])
    await bind_holder(actions, seat_id=seat_id, agent_id=agent_id)
    return seat_id


async def _works_in(actions: Actions, agent_id: str, project_canonical: str) -> None:
    a_id = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1 AND type='Agent'", agent_id)
    p_id = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1 AND type='SoftwareProject'",
        project_canonical)
    await actions.create_link(a_id, p_id, "works_in", agent_id, NOW, 0.9,
                              evidence_class="self_declared")


def _write_pin(office: Path, project: str) -> None:
    office.mkdir(parents=True, exist_ok=True)
    (office / ".osiris").write_text(f'project = "{project}"\n')


async def test_transition_seat_project_dry_run_plans_all_three_steps(
    actions: Actions, tmp_path: Path,
) -> None:
    agent = "agent:jesus"
    await _agent(actions, agent)
    await _seated(actions, agent, "Jesus")
    fab = await _repo(actions, "Jesus")
    real = await _repo(actions, "Godel")
    await _works_in(actions, agent, fab)
    await _works_in(actions, agent, real)
    office_root = tmp_path / "seats"
    _write_pin(office_root / "jesus", "Jesus")

    out = await transition_seat_project(
        actions.pool, agent, dry_run=True, office_root=office_root)

    assert out["fabricated_project"] == fab
    assert out["real_project"] == real
    assert out["plan"]["invalidate_works_in"] == fab
    assert out["plan"]["correct_pin_value"] == {"key": "project", "value": "Godel"}
    assert out["plan"]["set_charter"] == ["Godel"]
    # nothing written
    assert (office_root / "jesus" / ".osiris").read_text() == 'project = "Jesus"\n'
    still = await actions.pool.fetch(
        "SELECT 1 FROM links l JOIN objects a ON a.id=l.from_id "
        "WHERE a.canonical=$1 AND l.type='works_in' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", agent)
    assert len(still) == 2


async def test_transition_seat_project_refuses_without_a_second_works_in_edge(
    actions: Actions, tmp_path: Path,
) -> None:
    agent = "agent:solo"
    await _agent(actions, agent)
    await _seated(actions, agent, "Solo")
    fab = await _repo(actions, "Solo")
    await _works_in(actions, agent, fab)

    out = await transition_seat_project(actions.pool, agent, dry_run=True)
    assert "error" in out
    assert "mount at the real repo's cwd first" in out["error"]


async def test_transition_seat_project_refuses_ambiguous_extra_edges(
    actions: Actions, tmp_path: Path,
) -> None:
    agent = "agent:multi"
    await _agent(actions, agent)
    await _seated(actions, agent, "Multi")
    fab = await _repo(actions, "Multi")
    real_a = await _repo(actions, "AlphaRepo")
    real_b = await _repo(actions, "BetaRepo")
    await _works_in(actions, agent, fab)
    await _works_in(actions, agent, real_a)
    await _works_in(actions, agent, real_b)

    out = await transition_seat_project(actions.pool, agent, dry_run=True)
    assert "error" in out
    assert "ambiguous" in out["error"]

    disambiguated = await transition_seat_project(
        actions.pool, agent, real_project="AlphaRepo", dry_run=True)
    assert disambiguated["real_project"] == real_a


async def test_transition_seat_project_refuses_without_a_held_seat(
    actions: Actions, tmp_path: Path,
) -> None:
    agent = "agent:unseated"
    await _agent(actions, agent)
    out = await transition_seat_project(actions.pool, agent, dry_run=True)
    assert "error" in out
    assert "holds no seat" in out["error"]


async def test_transition_seat_project_execute_requires_because(
    actions: Actions, tmp_path: Path,
) -> None:
    agent = "agent:noreason"
    await _agent(actions, agent)
    await _seated(actions, agent, "Noreason")
    fab = await _repo(actions, "Noreason")
    real = await _repo(actions, "RealRepo")
    await _works_in(actions, agent, fab)
    await _works_in(actions, agent, real)

    out = await transition_seat_project(actions.pool, agent, dry_run=False)
    assert "error" in out
    assert "because is required" in out["error"]


async def test_transition_seat_project_executes_all_three_steps(
    actions: Actions, tmp_path: Path,
) -> None:
    agent = "agent:chad"
    await _agent(actions, agent)
    seat_id = await _seated(actions, agent, "Chad")
    fab = await _repo(actions, "Chad")
    real = await _repo(actions, "cdking")
    await _works_in(actions, agent, fab)
    await _works_in(actions, agent, real)
    office_root = tmp_path / "seats"
    _write_pin(office_root / "chad", "Chad")

    out = await transition_seat_project(
        actions.pool, agent, because="fabricated project, real repo already worked in",
        dry_run=False, office_root=office_root)

    assert "error" not in out
    assert out["steps"]["invalidate_works_in"]["was_working_in"] == fab
    assert out["steps"]["correct_pin_value"]["new_value"] == "cdking"
    assert out["steps"]["set_charter"]["charter"] == ["cdking"]
    assert (office_root / "chad" / ".osiris").read_text() == 'project = "cdking"\n'
    assert await charter_of(actions.pool, seat_id) == ["cdking"]
    still = await actions.pool.fetch(
        "SELECT p.canonical FROM links l JOIN objects a ON a.id=l.from_id "
        "JOIN objects p ON p.id=l.to_id "
        "WHERE a.canonical=$1 AND l.type='works_in' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", agent)
    assert [r["canonical"] for r in still] == [real]


async def test_transition_seat_project_skips_already_correct_steps(
    actions: Actions, tmp_path: Path,
) -> None:
    agent = "agent:tidy"
    await _agent(actions, agent)
    seat_id = await _seated(actions, agent, "Tidy")
    fab = await _repo(actions, "Tidy")
    real = await _repo(actions, "AlreadyRight")
    await _works_in(actions, agent, fab)
    await _works_in(actions, agent, real)
    office_root = tmp_path / "seats"
    _write_pin(office_root / "tidy", "AlreadyRight")
    from src.orchestrator.charter import set_charter
    await set_charter(actions, seat_id, ["AlreadyRight"], actor=agent)

    out = await transition_seat_project(
        actions.pool, agent, dry_run=True, office_root=office_root,
        repos=["AlreadyRight"])
    assert out["plan"]["correct_pin_value"] is None
    assert out["plan"]["set_charter"] is None
    assert out["plan"]["invalidate_works_in"] == fab

    executed = await transition_seat_project(
        actions.pool, agent, because="drop the stale duplicate only", dry_run=False,
        office_root=office_root, repos=["AlreadyRight"])
    assert "correct_pin_value" not in executed["steps"]
    assert "set_charter" not in executed["steps"]
    assert executed["steps"]["invalidate_works_in"]["was_working_in"] == fab
