"""MIGRATION 0062 (thread 922d920c): the worktree fold -- a pre-fix phantom SoftwareProject
that was really a git worktree (`.git` a FILE, not a directory) merges into a new Worktree
object linked `worktree_of` its real parent, live-checked against disk rather than a
hardcoded list of names.
"""
from __future__ import annotations

import subprocess
from datetime import UTC, datetime
from pathlib import Path

from src.actions.core import Actions
from src.orchestrator.migration_0062 import apply_migration_0062, plan_migration_0062


def _real_repo_with_worktree(tmp_path: Path, repo_name: str, wt_name: str) -> tuple[Path, Path]:
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
    return repo, wt


def _real_git_repo(path: Path) -> None:
    path.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=path, check=True)


async def _phantom_project(actions: Actions, name: str, on_disk_path: str) -> str:
    """A pre-fix census row: a worktree misfiled as its own SoftwareProject."""
    obj = await actions.create_or_find_object("SoftwareProject", f"repo:{name}", "disk-census")
    now = datetime.now(UTC)
    await actions.assert_property(obj, "name", name, "disk-census", now, 0.9,
                                  evidence_class="direct_observation")
    await actions.assert_property(obj, "on_disk_path", on_disk_path, "disk-census", now, 0.9,
                                  evidence_class="direct_observation")
    return str(obj)


async def test_migration_0062_folds_a_phantom_worktree_project_into_a_new_worktree(
    actions: Actions, tmp_path: Path,
) -> None:
    repo, wt = _real_repo_with_worktree(tmp_path, "m62parent", "m62parent-wt")
    phantom = await _phantom_project(actions, "m62parent-wt", str(wt))

    out = await apply_migration_0062(actions)

    assert out["folded"] == ["repo:m62parent-wt"]
    phantom_row = await actions.pool.fetchrow(
        "SELECT status, merged_into FROM objects WHERE id=$1", phantom)
    assert phantom_row["status"] == "merged"
    tree_row = await actions.pool.fetchrow(
        "SELECT o.id, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='on_disk_path' LIMIT 1) AS path, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='branch' LIMIT 1) AS branch "
        "FROM objects o WHERE o.type='Worktree' AND o.canonical='worktree:m62parent-wt'")
    assert tree_row is not None
    assert str(phantom_row["merged_into"]) == str(tree_row["id"])
    assert tree_row["path"] == str(wt)
    assert tree_row["branch"] == "m62parent-wt"

    parent_link = await actions.pool.fetchval(
        "SELECT p.canonical FROM links l JOIN objects p ON p.id=l.to_id "
        "WHERE l.from_id=$1 AND l.type='worktree_of'", tree_row["id"])
    assert parent_link == "repo:m62parent"
    parent_path = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a "
        "JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical='repo:m62parent' AND a.name='on_disk_path' LIMIT 1")
    assert parent_path == str(repo)


async def test_migration_0062_is_idempotent_on_a_second_run(
    actions: Actions, tmp_path: Path,
) -> None:
    _repo, wt = _real_repo_with_worktree(tmp_path, "m62idempotent", "m62idempotent-wt")
    await _phantom_project(actions, "m62idempotent-wt", str(wt))

    first = await apply_migration_0062(actions)
    second = await apply_migration_0062(actions)

    assert first["folded"] == ["repo:m62idempotent-wt"]
    assert second["folded"] == []  # already merged -- nothing left to fold
    link_count = await actions.pool.fetchval(
        "SELECT count(*) FROM links l JOIN objects t ON t.id=l.from_id "
        "WHERE t.canonical='worktree:m62idempotent-wt' AND l.type='worktree_of'")
    assert link_count == 1


async def test_migration_0062_leaves_a_real_repo_root_untouched(
    actions: Actions, tmp_path: Path,
) -> None:
    """A genuine repo (not a worktree of anything) must never be folded — `--git-common-
    dir`/`--git-dir` agree, so `worktree_parent_path` returns None and the plan skips it."""
    repo = tmp_path / "m62realrepo"
    _real_git_repo(repo)
    await _phantom_project(actions, "m62realrepo", str(repo))

    plan = await plan_migration_0062(actions.pool)

    assert plan["to_fold"] == []


async def test_migration_0062_leaves_a_project_whose_path_is_gone_untouched(
    actions: Actions, tmp_path: Path,
) -> None:
    """on_disk_path pointing at nothing on disk anymore (staleness the census already
    tolerates) — `git -C <gone>` fails, `worktree_parent_path` returns None, never a fold."""
    await _phantom_project(actions, "m62gonepath", str(tmp_path / "does-not-exist"))

    plan = await plan_migration_0062(actions.pool)

    assert plan["to_fold"] == []
