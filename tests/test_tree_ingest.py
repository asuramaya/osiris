"""Self-healing tree ingest (thread 5126) — ingest_project's dry-run/write receipt shapes,
the third-party sibling's required `because`, and the alarm heartbeat's cold-by-default,
one-per-24h-per-tree mailing."""
from __future__ import annotations

import subprocess
from datetime import UTC, datetime
from pathlib import Path

from src.actions.core import Actions
from src.config.settings import Settings
from src.orchestrator.monitor import get_cursor
from src.orchestrator.tree_ingest import (
    ingest_project,
    ingest_project_third_party,
    uningested_trees_alarm_tick,
)


def _git_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "toygarden"
    repo.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.email", "t@example.com"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "T"], cwd=repo, check=True)
    (repo / "a.txt").write_text("one")
    subprocess.run(["git", "add", "a.txt"], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-q", "-m", "first"], cwd=repo, check=True)
    return repo


async def _project_with_path(actions: Actions, canonical: str, path: str) -> None:
    proj = await actions.create_or_find_object("SoftwareProject", canonical, "git")
    await actions.assert_property(proj, "on_disk_path", path, "disk-census",
                                  datetime.now(UTC), 0.9)


async def test_ingest_project_refuses_without_on_disk_path(actions: Actions) -> None:
    await actions.create_or_find_object("SoftwareProject", "repo:nowhere", "session")
    out = await ingest_project(actions, project="nowhere", actor="test")
    assert "error" in out
    assert "on_disk_path" in out["error"]


async def test_ingest_project_dry_run_writes_nothing_and_names_what_would_land(
        actions: Actions, tmp_path: Path) -> None:
    repo = _git_repo(tmp_path)
    await _project_with_path(actions, "repo:toygarden", str(repo))
    out = await ingest_project(actions, project="toygarden", dry_run=True, actor="test")
    assert out["dry_run"] is True
    assert out["commits_on_disk"] == 1
    assert out["commits_already_graphed"] == 0
    assert out["commits_would_ingest"] == 1
    assert out["sample"] == ["first"]
    graphed = await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Commit'")
    assert graphed == 0


async def test_ingest_project_landed_then_closure_runs_over_it(
        actions: Actions, tmp_path: Path) -> None:
    repo = _git_repo(tmp_path)
    await _project_with_path(actions, "repo:toygarden", str(repo))
    out = await ingest_project(actions, project="toygarden", dry_run=False, actor="test")
    assert out["dry_run"] is False
    assert out["ingest"]["commits"] == 1
    assert out["closure"]["repo"] == "toygarden"
    graphed = await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Commit' AND status='active'")
    assert graphed == 1
    # idempotent: re-running dry_run now sees the commit already graphed, nothing new
    preview = await ingest_project(actions, project="toygarden", dry_run=True, actor="test")
    assert preview["commits_already_graphed"] == 1
    assert preview["commits_would_ingest"] == 0


async def test_ingest_project_third_party_refuses_an_empty_reason(actions: Actions) -> None:
    out = await ingest_project_third_party(
        actions, project="anything", because="  ", actor="coordinator")
    assert "error" in out
    assert "reason" in out["error"]


async def test_ingest_project_third_party_names_any_project_not_just_the_callers(
        actions: Actions, tmp_path: Path) -> None:
    repo = _git_repo(tmp_path)
    await _project_with_path(actions, "repo:toygarden", str(repo))
    out = await ingest_project_third_party(
        actions, project="toygarden", because="chronohorn wave dry-run receipt",
        actor="coordinator")
    assert out["because"] == "chronohorn wave dry-run receipt"
    assert out["dry_run"] is True
    assert out["commits_would_ingest"] == 1


async def test_alarm_tick_is_dark_by_default_and_sends_nothing(actions: Actions) -> None:
    out = await uningested_trees_alarm_tick(actions, settings=Settings())
    assert out == {"enabled": False, "alarmed": []}


async def test_alarm_tick_mails_the_owning_seat_when_enabled(actions: Actions) -> None:
    proj = await actions.create_or_find_object("SoftwareProject", "repo:blindtree", "git")
    await actions.assert_property(proj, "on_disk_path", "/home/x/code/blindtree",
                                  "disk-census", datetime.now(UTC), 0.9)
    seat = await actions.create_or_find_object("Seat", "seat:owner", "session")
    await actions.create_link(seat, proj, "governs", "session", datetime.now(UTC), 0.9)
    out = await uningested_trees_alarm_tick(
        actions, settings=Settings(osiris_tree_ingest_alarm_enabled=True))
    assert out["enabled"] is True
    assert out["alarmed"] == [{"tree": "blindtree", "seat": "seat:owner"}]
    cursor = await get_cursor(actions.pool, "tree-ingest-alarm:blindtree")
    assert cursor is not None


async def test_alarm_tick_reports_a_tree_with_no_governing_seat_without_mailing(
        actions: Actions) -> None:
    proj = await actions.create_or_find_object("SoftwareProject", "repo:orphantree", "git")
    await actions.assert_property(proj, "on_disk_path", "/home/x/code/orphantree",
                                  "disk-census", datetime.now(UTC), 0.9)
    out = await uningested_trees_alarm_tick(
        actions, settings=Settings(osiris_tree_ingest_alarm_enabled=True))
    assert out["alarmed"] == []
    assert out["unowned"] == ["orphantree"]


async def test_alarm_tick_never_re_alarms_the_same_tree_within_24h(actions: Actions) -> None:
    proj = await actions.create_or_find_object("SoftwareProject", "repo:cooling", "git")
    await actions.assert_property(proj, "on_disk_path", "/home/x/code/cooling",
                                  "disk-census", datetime.now(UTC), 0.9)
    seat = await actions.create_or_find_object("Seat", "seat:owner2", "session")
    await actions.create_link(seat, proj, "governs", "session", datetime.now(UTC), 0.9)
    settings = Settings(osiris_tree_ingest_alarm_enabled=True)
    first = await uningested_trees_alarm_tick(actions, settings=settings)
    assert first["alarmed"] == [{"tree": "cooling", "seat": "seat:owner2"}]
    second = await uningested_trees_alarm_tick(actions, settings=settings)
    assert second["alarmed"] == []
    assert second["cooling"] == ["cooling"]
    sent = await actions.pool.fetchval(
        "SELECT count(*) FROM fleet_messages WHERE body LIKE '%tree-ingest-alarm%'")
    assert sent == 1


async def test_alarm_tick_ignores_a_tree_with_no_on_disk_path_at_all(actions: Actions) -> None:
    await actions.create_or_find_object("SoftwareProject", "repo:noplace", "git")
    out = await uningested_trees_alarm_tick(
        actions, settings=Settings(osiris_tree_ingest_alarm_enabled=True))
    assert out["alarmed"] == []
    assert out["unowned"] == []
