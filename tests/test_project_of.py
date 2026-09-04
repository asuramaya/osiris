"""project_of (decision 68fba2e4, thread 19d6bdcb7fa9 — the operator's own house/project
ruling): the resolving reader every DISPLAY/PROPAGATION caller of the old, misleadingly
named house_of should use instead — pin (if cwd given) -> charter (exactly one repo) ->
lineage_works_in (merge-normalized) -> None. Never house, never a raw stale copy. house_of
itself stays, narrowed to its two remaining raw-read callers (correct_agent_house's
before/after snapshot, claim_name's generation-counting comparison) — see its own
docstring.
"""
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from src.actions.core import Actions
from src.orchestrator import mounts
from src.orchestrator.agents import project_of
from src.orchestrator.seats import bind_holder, ensure_seat


async def _seated(actions: Actions, *, handle: str, house: str, anchor_cwd: str) -> str:
    agent = f"agent:{handle.lower()}"
    seat = await ensure_seat(actions, house=house, handle=handle, anchor_cwd=anchor_cwd,
                             source="test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id=agent)
    await actions.create_or_find_object("Agent", agent, agent)
    await mounts.save_mount(actions.pool, job_dir=f"/home/test/.claude/jobs/{handle.lower()}",
                            agent_id=agent, project=house, cwd=anchor_cwd, model=None,
                            session_key=handle.lower())
    return agent


async def test_project_of_pin_wins_outright_when_cwd_given(
    actions: Actions, tmp_path: Path,
) -> None:
    anchor = tmp_path / "pinwins"
    anchor.mkdir()
    (anchor / ".osiris").write_text('project = "realproject"\n')
    agent = await _seated(actions, handle="Pinwins", house="Pinwins", anchor_cwd=str(anchor))

    assert await project_of(actions.pool, agent, cwd=str(anchor)) == "realproject"


async def test_project_of_falls_back_to_a_single_chartered_repo_no_cwd(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.charter import set_charter

    anchor = tmp_path / "charteredpo"
    anchor.mkdir()  # no .osiris — no pin to win
    agent = await _seated(actions, handle="Charteredpo", house="Charteredpo",
                          anchor_cwd=str(anchor))
    proj = await actions.create_or_find_object("SoftwareProject", "repo:cdking2", "test")
    await actions.assert_property(proj, "name", "cdking2", "test", datetime.now(UTC), 0.9)
    seat_id = await actions.pool.fetchval(
        "SELECT o.canonical FROM objects o WHERE o.type='Seat' AND o.status='active' "
        "AND EXISTS (SELECT 1 FROM current_assertions a WHERE a.object_id=o.id "
        "AND a.name='handle' AND a.value #>> '{}' = 'Charteredpo')")
    await set_charter(actions, seat_id, ["cdking2"], actor="test")

    assert await project_of(actions.pool, agent) == "cdking2"


async def test_project_of_falls_back_to_lineage_works_in_no_charter(
    actions: Actions, tmp_path: Path,
) -> None:
    anchor = tmp_path / "workedinpo"
    anchor.mkdir()
    agent = await _seated(actions, handle="Workedinpo", house="Workedinpo",
                          anchor_cwd=str(anchor))
    proj = await actions.create_or_find_object("SoftwareProject", "repo:realproject2", "test")
    await actions.assert_property(proj, "name", "realproject2", "test", datetime.now(UTC), 0.9)
    agent_oid = await actions.create_or_find_object("Agent", agent, agent)
    await actions.create_link(agent_oid, proj, "works_in", agent, datetime.now(UTC), 0.9)

    assert await project_of(actions.pool, agent) == "realproject2"


async def test_project_of_stays_none_with_no_signal_never_house(
    actions: Actions, tmp_path: Path,
) -> None:
    anchor = tmp_path / "nosignalpo"
    anchor.mkdir()
    agent = await _seated(actions, handle="Nosignalpo", house="Nosignalpo",
                          anchor_cwd=str(anchor))

    assert await project_of(actions.pool, agent) is None


async def test_project_of_ignores_a_disagreeing_lineage(actions: Actions, tmp_path: Path) -> None:
    """lineage_works_in's own ABSTAIN law (two distinct projects across the lineage) must
    survive unchanged through project_of — a genuine disagreement is never broken by
    picking one, and never falls through to house either."""
    anchor = tmp_path / "disagreepo"
    anchor.mkdir()
    agent = await _seated(actions, handle="Disagreepo", house="Disagreepo",
                          anchor_cwd=str(anchor))
    proj_a = await actions.create_or_find_object("SoftwareProject", "repo:projecta", "test")
    proj_b = await actions.create_or_find_object("SoftwareProject", "repo:projectb", "test")
    await actions.assert_property(proj_a, "name", "projecta", "test", datetime.now(UTC), 0.9)
    await actions.assert_property(proj_b, "name", "projectb", "test", datetime.now(UTC), 0.9)
    agent_oid = await actions.create_or_find_object("Agent", agent, agent)
    await actions.create_link(agent_oid, proj_a, "works_in", agent, datetime.now(UTC), 0.9)
    await actions.create_link(agent_oid, proj_b, "works_in", agent, datetime.now(UTC), 0.9)

    assert await project_of(actions.pool, agent) is None
