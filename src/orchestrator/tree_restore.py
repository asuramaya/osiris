"""Put back a seat's missing worktree.

A seat's `tree_cwd` is often a git worktree under `<repo>/.claude/worktrees/<name>`. The
daily clean-up (scripts/osiris_cleanup.py) removes a worktree whose branch is merged into
origin/main and idle for a while, together with the branch, and nothing tells the seat bound
to it. The next `launch` or `resume` then met a missing directory and stopped with an
instruction for the operator to make one by hand.

`restore_tree` makes it instead, for exactly one shape: a missing path whose parent is
`<repo>/.claude/worktrees` of a real repository. It reuses the seat's own branch
(`worktree-<name>`, then `<name>`) when that branch exists and is not checked out
elsewhere, and otherwise starts `worktree-<name>` at the deployed sha. It refuses a path
outside a repository's `.claude/worktrees`, and a branch that is checked out at another
path. It only ever creates; it removes nothing but empty directories left where the tree
used to be.

Everything here is blocking subprocess work: callers in async code run it via
`asyncio.to_thread`.
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

_GIT_TIMEOUT = 60


@dataclass(frozen=True)
class TreeRestore:
    """`restored` is True when this call put the tree back; `ok` is whether the path now
    holds a usable tree (True also when it was already there). `line` is the one line to
    show: what was done, or why it was refused."""

    ok: bool
    restored: bool
    line: str


def _git(repo: Path, *args: str) -> tuple[int, str]:
    try:
        p = subprocess.run(
            ["git", "-C", str(repo), *args], capture_output=True, text=True,
            timeout=_GIT_TIMEOUT, stdin=subprocess.DEVNULL, check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return 1, str(exc)
    return p.returncode, (p.stdout + p.stderr).strip()


def _worktree_repo(path: Path) -> Path | None:
    """The repository whose `.claude/worktrees` directly holds `path`, else None."""
    if path.parent.name != "worktrees" or path.parent.parent.name != ".claude":
        return None
    repo = path.parent.parent.parent
    return repo if (repo / ".git").is_dir() else None


def _clear_empty_shell(path: Path) -> bool:
    """Remove `path` when it is only empty directories (what is left where a tree was taken
    away and something later made `.claude` in it). Never touches a file: returns False when
    anything real is in there."""
    if not path.is_dir():
        return True
    for _top, _dirs, files in os.walk(path):
        if files:
            return False
    for emptied, _dirs, _files in os.walk(path, topdown=False):
        try:
            os.rmdir(emptied)
        except OSError:
            return False
    return True


def _checked_out_at(repo: Path, branch: str) -> str | None:
    code, out = _git(repo, "worktree", "list", "--porcelain")
    if code != 0:
        return None
    current = ""
    for line in out.splitlines():
        if line.startswith("worktree "):
            current = line[len("worktree "):]
        elif line == f"branch refs/heads/{branch}":
            return current
    return None


def _deployed_sha(repo: Path) -> str | None:
    from src.orchestrator.deploy_status import _snapshot_dir

    for source, ref in ((_snapshot_dir(), "HEAD"), (repo, "origin/main")):
        code, out = _git(source, "rev-parse", "--verify", "--quiet", ref)
        if code == 0 and out:
            if _git(repo, "cat-file", "-e", f"{out}^{{commit}}")[0] == 0:
                return out
    return None


def restore_tree(tree_cwd: str) -> TreeRestore:
    path = Path(tree_cwd)
    if (path / ".git").exists():
        return TreeRestore(True, False, "")
    repo = _worktree_repo(path)
    if path.is_dir() and (repo is None or not _clear_empty_shell(path)):
        # an existing directory that is not a worktree slot, or one holding real files:
        # not ours to touch
        return TreeRestore(True, False, "")
    if repo is None:
        return TreeRestore(
            False, False,
            f"{tree_cwd} is missing, and it is not under a repository's .claude/worktrees, "
            "so osiris will not make it")
    _git(repo, "worktree", "prune")
    name = path.name
    for branch in (f"worktree-{name}", name):
        if _git(repo, "rev-parse", "--verify", "--quiet", f"refs/heads/{branch}")[0] != 0:
            continue
        elsewhere = _checked_out_at(repo, branch)
        if elsewhere:
            return TreeRestore(
                False, False,
                f"{tree_cwd} is missing, and its branch {branch} is checked out at "
                f"{elsewhere}; free that branch or bind the seat to that tree")
        code, out = _git(repo, "worktree", "add", str(path), branch)
        if code != 0:
            return TreeRestore(False, False, f"could not recreate {tree_cwd}: "
                               f"{out.splitlines()[-1] if out else 'git worktree add failed'}")
        return TreeRestore(
            True, True, f"{tree_cwd} was gone; recreated it on its own branch {branch}")
    sha = _deployed_sha(repo)
    if sha is None:
        return TreeRestore(False, False, f"{tree_cwd} is missing and no deployed sha is known "
                                         "to start a branch from")
    branch = f"worktree-{name}"
    code, out = _git(repo, "worktree", "add", "-b", branch, str(path), sha)
    if code != 0:
        return TreeRestore(False, False, f"could not recreate {tree_cwd}: "
                           f"{out.splitlines()[-1] if out else 'git worktree add failed'}")
    return TreeRestore(
        True, True, f"{tree_cwd} was gone; recreated it on a new branch {branch} at {sha[:8]}")
