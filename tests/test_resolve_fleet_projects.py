"""agents.resolve_fleet_projects — the async, DB-backed half of ruling f6b758fc's
requirement 1: resolve each fleet() node's REAL graph project (an active SoftwareProject
matching its own raw label, else a Worktree's parent / project_of's own cwd-pin fallback,
else None — unfiled), batched by distinct raw label and distinct cwd, never per-row.

fleetview.py stays pure and untested here — its own test_fleetview.py proves the render
consumes `resolved_project` correctly. This file proves the RESOLUTION itself, against a
real Postgres (hermetic, testcontainers) the same way test_project_of.py proves
`project_of` — reusing that file's own repo/worktree/seat-fixture shapes.
"""
from __future__ import annotations

import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest
from src.actions.core import Actions
from src.orchestrator import mounts
from src.orchestrator.agents import resolve_fleet_projects
from src.orchestrator.seats import bind_holder, ensure_seat


def _real_repo_with_worktree(tmp_path: Path, repo_name: str, wt_name: str) -> Path:
    """Same shape as test_project_of.py's own helper: a real git repo, no `.osiris`
    anywhere, plus one real worktree of it."""
    repo = tmp_path / repo_name
    repo.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.email", "t@t.com"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "t"], cwd=repo, check=True)
    (repo / "f.txt").write_text("x")
    subprocess.run(["git", "add", "."], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-q", "-m", "x"], cwd=repo, check=True)
    wt = tmp_path / wt_name
    subprocess.run(["git", "worktree", "add", "-q", "-b", wt_name, str(wt)],
                   cwd=repo, check=True)
    return wt


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


async def test_an_active_softwareproject_label_wins_first(
    actions: Actions, tmp_path: Path,
) -> None:
    """Rung 1: the node's own raw `project` label already names an ACTIVE SoftwareProject —
    resolved outright, no cwd/project_of round trip needed at all."""
    proj = await actions.create_or_find_object("SoftwareProject", "repo:activelabel", "test")
    await actions.assert_property(proj, "name", "activelabel", "test", datetime.now(UTC), 0.9)
    nodes = {"agent:x": {"project": "activelabel", "cwd": None}}

    await resolve_fleet_projects(actions.pool, nodes)

    assert nodes["agent:x"]["resolved_project"] == "activelabel"


async def test_a_worktree_session_resolves_through_its_registered_parent(
    actions: Actions, tmp_path: Path,
) -> None:
    """Rung 2 (a Worktree's parent, via project_of's own worktree-parent-path rung): the
    raw label ('junkbasename') names NOTHING active, but the session's cwd is a real git
    worktree of a registered parent project — resolves to the PARENT's name, never the
    worktree's own basename."""
    wt = _real_repo_with_worktree(tmp_path, "wtparent-rfp", "wtparent-rfp-branch")
    (tmp_path / "elsewhere").mkdir()
    agent = await _seated(actions, handle="Wtparentrfp", house="Wtparentrfp",
                          anchor_cwd=str(tmp_path / "elsewhere"))
    proj = await actions.create_or_find_object("SoftwareProject", "repo:wtparent-rfp", "test")
    await actions.assert_property(proj, "name", "wtparent-rfp", "test", datetime.now(UTC), 0.9)
    await actions.assert_property(proj, "on_disk_path", str(tmp_path / "wtparent-rfp"), "test",
                                  datetime.now(UTC), 0.9)
    nodes = {agent: {"project": "junkbasename", "cwd": str(wt)}}

    await resolve_fleet_projects(actions.pool, nodes)

    assert nodes[agent]["resolved_project"] == "wtparent-rfp"


async def test_a_question_mark_node_resolves_through_the_pin_before_unfiled(
    actions: Actions, tmp_path: Path,
) -> None:
    """The '?' bucket (no raw label AT ALL) must resolve through the SAME cwd-pin rung as
    a labelled session, never special-cased straight into unfiled."""
    anchor = tmp_path / "qmarkpin"
    anchor.mkdir()
    (anchor / ".osiris").write_text('project = "pinnedbyq"\n')
    agent = await _seated(actions, handle="Qmarkpin", house="Qmarkpin",
                          anchor_cwd=str(anchor))
    nodes = {agent: {"project": None, "cwd": str(anchor)}}

    await resolve_fleet_projects(actions.pool, nodes)

    assert nodes[agent]["resolved_project"] == "pinnedbyq"


async def test_junk_labels_with_no_resolvable_cwd_resolve_to_none(
    actions: Actions, tmp_path: Path,
) -> None:
    """A raw label naming no active project, AND a cwd with no pin/worktree-parent/charter/
    lineage signal at all, resolves to None — honestly unfiled, never a guess."""
    anchor = tmp_path / "nosignalrfp"
    anchor.mkdir()
    agent = await _seated(actions, handle="Nosignalrfp", house="Nosignalrfp",
                          anchor_cwd=str(anchor))
    nodes = {agent: {"project": "totally-fake-label", "cwd": str(anchor)}}

    await resolve_fleet_projects(actions.pool, nodes)

    assert nodes[agent]["resolved_project"] is None


async def test_a_node_with_no_label_and_no_cwd_resolves_to_none(actions: Actions) -> None:
    """No raw label, no cwd at all — nothing to resolve through, honestly None rather than
    a crash on a missing key."""
    nodes = {"agent:bare": {"project": None, "cwd": None}}

    await resolve_fleet_projects(actions.pool, nodes)

    assert nodes["agent:bare"]["resolved_project"] is None


async def test_resolution_is_batched_by_distinct_label_not_per_row(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Performance discipline (same law as the merged_into label-normalization pass and the
    ghost_gap probes beside it in fleet()): three nodes sharing ONE active label must cost
    exactly one `_resolve_repo` lookup, not three."""
    from src.orchestrator import capture as capture_mod

    proj = await actions.create_or_find_object("SoftwareProject", "repo:sharedlabel", "test")
    await actions.assert_property(proj, "name", "sharedlabel", "test", datetime.now(UTC), 0.9)

    calls: list[str] = []
    real = capture_mod._resolve_repo

    async def _counting(pool: object, name: str) -> object:
        calls.append(name)
        return await real(pool, name)

    monkeypatch.setattr(capture_mod, "_resolve_repo", _counting)  # type: ignore[attr-defined]
    nodes = {
        "agent:a": {"project": "sharedlabel", "cwd": None},
        "agent:b": {"project": "sharedlabel", "cwd": None},
        "agent:c": {"project": "sharedlabel", "cwd": None},
    }

    await resolve_fleet_projects(actions.pool, nodes)

    assert calls == ["sharedlabel"]
    assert all(n["resolved_project"] == "sharedlabel" for n in nodes.values())
