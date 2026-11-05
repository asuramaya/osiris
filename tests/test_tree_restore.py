"""A seat's worktree that the clean-up removed is put back by launch and resume.

`scripts/osiris_cleanup.py` removes a worktree (and its branch) once it is merged into
origin/main and idle, and nothing tells the seat bound to it. These tests use real git
repositories: the directory shapes are what decide the outcome."""

from __future__ import annotations

import io
import subprocess
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any

import pytest
from src import launch_voice
from src.actions.core import Actions
from src.orchestrator.tree_restore import restore_tree


def _git(repo: Path, *args: str) -> str:
    p = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True,
                       check=True, timeout=60)
    return p.stdout.strip()


@pytest.fixture
def repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    r = tmp_path / "repo"
    r.mkdir()
    subprocess.run(["git", "init", "-q", "-b", "main", str(r)], check=True, timeout=60)
    _git(r, "config", "user.email", "t@example.com")
    _git(r, "config", "user.name", "t")
    (r / "a.txt").write_text("a")
    _git(r, "add", "a.txt")
    _git(r, "commit", "-q", "-m", "first")
    (r / ".claude" / "worktrees").mkdir(parents=True)
    # the "deployed" snapshot is this repo's own HEAD
    monkeypatch.setenv("OSIRIS_DEPLOY_SNAPSHOT_DIR", str(r))
    return r


def test_a_present_tree_is_left_alone(repo: Path) -> None:
    tree = repo / ".claude" / "worktrees" / "here"
    _git(repo, "worktree", "add", "-q", "-b", "worktree-here", str(tree))
    out = restore_tree(str(tree))
    assert out.ok and not out.restored and out.line == ""


def test_a_removed_tree_comes_back_on_a_fresh_branch_at_the_deployed_sha(repo: Path) -> None:
    tree = repo / ".claude" / "worktrees" / "gone"
    out = restore_tree(str(tree))
    assert out.ok and out.restored
    assert (tree / ".git").exists() and (tree / "a.txt").read_text() == "a"
    assert _git(tree, "branch", "--show-current") == "worktree-gone"
    assert _git(tree, "rev-parse", "HEAD") == _git(repo, "rev-parse", "HEAD")
    assert "new branch worktree-gone" in out.line


def test_the_seats_own_branch_is_reused_when_it_survives(repo: Path) -> None:
    tree = repo / ".claude" / "worktrees" / "mine"
    _git(repo, "worktree", "add", "-q", "-b", "worktree-mine", str(tree))
    (tree / "b.txt").write_text("b")
    _git(tree, "add", "b.txt")
    _git(tree, "commit", "-q", "-m", "work")
    work = _git(tree, "rev-parse", "HEAD")
    _git(repo, "worktree", "remove", "--force", str(tree))  # what the clean-up did

    out = restore_tree(str(tree))
    assert out.ok and out.restored and "its own branch worktree-mine" in out.line
    assert _git(tree, "rev-parse", "HEAD") == work and (tree / "b.txt").exists()


def test_a_stale_registration_does_not_block_the_restore(repo: Path) -> None:
    tree = repo / ".claude" / "worktrees" / "stale"
    _git(repo, "worktree", "add", "-q", "-b", "worktree-stale", str(tree))
    for child in sorted(tree.rglob("*"), reverse=True):  # deleted by hand: still registered
        child.unlink() if child.is_file() or child.is_symlink() else child.rmdir()
    tree.rmdir()
    out = restore_tree(str(tree))
    assert out.ok and out.restored and (tree / ".git").exists()


def test_a_branch_checked_out_elsewhere_is_refused_and_nothing_is_made(repo: Path) -> None:
    other = repo / ".claude" / "worktrees" / "other"
    _git(repo, "worktree", "add", "-q", "-b", "worktree-taken", str(other))
    tree = repo / ".claude" / "worktrees" / "taken"
    out = restore_tree(str(tree))
    assert not out.ok and not out.restored
    assert "worktree-taken is checked out at" in out.line and str(other) in out.line
    assert not tree.exists()


def test_a_path_outside_the_repos_worktrees_is_refused(repo: Path, tmp_path: Path) -> None:
    for where in (tmp_path / "elsewhere" / "gone", repo / "sub" / "gone"):
        out = restore_tree(str(where))
        assert not out.ok and not out.restored
        assert "not under a repository's .claude/worktrees" in out.line
        assert not where.exists()


def test_the_empty_shell_left_where_a_tree_was_is_cleared_and_restored(repo: Path) -> None:
    tree = repo / ".claude" / "worktrees" / "shell"
    (tree / ".claude").mkdir(parents=True)  # the specimen: only an empty .claude is left
    out = restore_tree(str(tree))
    assert out.ok and out.restored and (tree / ".git").exists()


def test_a_directory_holding_real_files_is_never_touched(repo: Path) -> None:
    tree = repo / ".claude" / "worktrees" / "notes"
    tree.mkdir()
    (tree / "keep.txt").write_text("mine")
    out = restore_tree(str(tree))
    assert out.ok and not out.restored
    assert (tree / "keep.txt").read_text() == "mine" and not (tree / ".git").exists()


def test_the_voice_says_it_in_one_line_with_a_short_home_path() -> None:
    home = str(Path.home())
    line = f"{home}/code/r/.claude/worktrees/x was gone; recreated it on a new branch " \
           "worktree-x at abcd1234"
    said = launch_voice.tree_restored("launch", line)
    assert said == ("osiris launch: ~/code/r/.claude/worktrees/x was gone; recreated it on "
                    "a new branch worktree-x at abcd1234.")
    assert "\n" not in said


async def test_launch_puts_back_a_worktree_the_cleanup_removed(
    actions: Actions, repo: Path, tmp_path: Path,
) -> None:
    """The seat's tree was removed (merged and idle): launch recreates it, says so on the
    receipt, and spawns into it, where it used to refuse and ask for a hand-made directory."""
    from src.orchestrator import trigger as trigger_module
    from src.orchestrator.seats import bind_seat_tree

    from tests.test_trigger import _fake_agents_json, _fake_spawn, _managed_pair, _office

    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:tw04", manager_agent="agent:tm04",
        worker_handle="Treegone", house="osiris")
    office = tmp_path / "office3"
    office.mkdir()
    tree = repo / ".claude" / "worktrees" / "treegone"
    await _office(actions, worker_seat, str(office))
    await bind_seat_tree(actions, seat_id=worker_seat, tree_cwd=str(tree), actor="operator",
                         because="test: the tree was removed after binding")
    spawned: list[dict[str, Any]] = []
    d = await trigger_module.launch_seat(
        actions, caller="agent:tm04", target=worker_seat,
        spawn=_fake_spawn(spawned), agents_json=_fake_agents_json([[]]))
    assert d["status"] == "launched"
    assert (tree / ".git").exists()
    assert "recreated it on a new branch worktree-treegone" in d["tree_restored"]
    assert spawned


async def test_cmd_launch_says_in_one_line_that_it_put_the_seats_tree_back(
    actions: Actions, repo: Path, tmp_path: Path,
) -> None:
    from src.cli import cmd_launch
    from src.orchestrator.seats import bind_seat_tree, ensure_seat

    office = tmp_path / "office-back"
    office.mkdir()
    tree = repo / ".claude" / "worktrees" / "cliback"
    seat = await ensure_seat(actions, house="osiris", handle="cliback",
                             anchor_cwd=str(office), source="test")
    await bind_seat_tree(actions, seat_id=seat["seat_id"], tree_cwd=str(tree),
                         actor="operator", because="test: tree removed after binding")

    spawn_calls: list[str] = []
    polls = 0

    async def _spawn(repo_dir: str, *, name: str, model: str | None, prompt: str) -> str:
        spawn_calls.append(repo_dir)
        return "abcd1234"

    async def _agents_json(*, cwd: str | None = None, **k: Any) -> list[dict[str, Any]]:
        nonlocal polls
        polls += 1
        return [] if polls < 2 else [{"id": "abcd1234", "name": "[OS] cliback", "cwd": cwd,
                                      "sessionId": "sess-back"}]

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_launch("cliback", model=None, pool=actions.pool, spawn=_spawn,
                               agents_json=_agents_json)
    assert out == 0
    lines = buf.getvalue().strip().splitlines()
    assert len(lines) <= 3
    assert any("recreated it on a new branch worktree-cliback" in ln for ln in lines)
    assert (tree / ".git").exists() and spawn_calls[0] == str(tree)
