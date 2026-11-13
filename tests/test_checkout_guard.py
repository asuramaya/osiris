"""The shared-checkout guard (.githooks/reference-transaction) and the gate's HEAD assertion.

Real git in a scratch repo, because the hook's whole job is what git does around it: it must
REFUSE a move of HEAD off main in the shared checkout and put the files back, and must leave
commits, merges, `reset --keep`, `git worktree add` and everything inside a worktree alone."""
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
HOOK = ROOT / ".githooks" / "reference-transaction"
INSTALLER = ROOT / "scripts" / "install_checkout_guard_hook.sh"
ASSERT = ROOT / "scripts" / "assert_on_main.sh"

pytestmark = pytest.mark.skipif(shutil.which("git") is None, reason="needs git")


def _env(**extra: str) -> dict[str, str]:
    # a commit hook runs this suite with GIT_DIR and friends set: never inherit them
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    env.update(GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@t", GIT_COMMITTER_NAME="t",
               GIT_COMMITTER_EMAIL="t@t", **extra)
    return env


def git(cwd: Path, *args: str, check: bool = True, **env: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["git", *args], timeout=60, cwd=cwd, env=_env(**env),
                          capture_output=True, text=True, check=check)


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    r = tmp_path / "shared"
    r.mkdir()
    git(r, "init", "-q", "-b", "main")
    (r / "f.txt").write_text("A\n")
    git(r, "add", "f.txt")
    git(r, "commit", "-qm", "A")
    git(r, "branch", "other")
    (r / "f.txt").write_text("B\n")
    git(r, "commit", "-qam", "B")
    (r / "h.txt").write_text("C\n")
    git(r, "add", "h.txt")
    git(r, "commit", "-qm", "C")
    hook = r / ".git" / "hooks" / "reference-transaction"
    shutil.copy(HOOK, hook)
    hook.chmod(0o755)
    return r


def branch(r: Path) -> str:
    return git(r, "branch", "--show-current").stdout.strip()


def test_a_detach_in_the_shared_checkout_is_refused_and_the_files_come_back(repo: Path) -> None:
    out = git(repo, "checkout", "--detach", "HEAD~2", check=False)
    assert out.returncode != 0
    assert "REFUSED" in out.stderr and "git worktree add" in out.stderr
    assert branch(repo) == "main"
    assert (repo / "f.txt").read_text() == "B\n" and (repo / "h.txt").exists()
    assert git(repo, "status", "--short").stdout == ""  # index and files back at HEAD


def test_switching_to_another_branch_is_refused_too(repo: Path) -> None:
    out = git(repo, "switch", "other", check=False)
    assert out.returncode != 0 and "REFUSED" in out.stderr
    assert branch(repo) == "main"
    assert (repo / "f.txt").read_text() == "B\n"
    assert git(repo, "status", "--short").stdout == ""


def test_local_edits_survive_a_refused_move(repo: Path) -> None:
    (repo / "g.txt").write_text("new\n")
    git(repo, "add", "g.txt")
    out = git(repo, "checkout", "--detach", "HEAD~2", check=False)
    assert out.returncode != 0
    assert (repo / "g.txt").read_text() == "new\n"
    assert git(repo, "status", "--short").stdout.split() == ["A", "g.txt"]


def test_what_the_gate_and_normal_work_do_on_main_still_works(repo: Path) -> None:
    git(repo, "commit", "-q", "--allow-empty", "-m", "D")
    git(repo, "reset", "-q", "--keep", "HEAD~1")
    git(repo, "merge", "-q", "--ff-only", "main")
    git(repo, "checkout", "-q", "main")  # already there: a no-op, not a move
    (repo / "f.txt").write_text("edit\n")
    git(repo, "checkout", "--", "f.txt")
    assert branch(repo) == "main" and (repo / "f.txt").read_text() == "B\n"


def test_worktrees_are_free_and_building_one_is_not_refused(repo: Path, tmp_path: Path) -> None:
    wt1, wt2 = tmp_path / "wt1", tmp_path / "wt2"
    git(repo, "worktree", "add", "-q", "-b", "feature", str(wt1), "other")
    git(repo, "worktree", "add", "-q", "--detach", str(wt2), "HEAD~1")
    assert branch(wt1) == "feature" and branch(wt2) == ""
    git(wt1, "switch", "-q", "-c", "more")
    git(wt1, "commit", "-q", "--allow-empty", "-m", "x")
    git(wt2, "checkout", "-q", "--detach", "other")
    assert branch(repo) == "main"


def test_the_operator_override_lets_a_move_through(repo: Path) -> None:
    git(repo, "checkout", "-q", "--detach", "HEAD~1", OSIRIS_ALLOW_SHARED_HEAD_MOVE="1")
    assert branch(repo) == ""
    git(repo, "checkout", "-q", "main")  # back onto main is never refused
    assert branch(repo) == "main"


def test_the_installer_copies_the_hook_and_is_idempotent(repo: Path) -> None:
    (repo / ".githooks").mkdir()
    shutil.copy(HOOK, repo / ".githooks" / "reference-transaction")
    target = repo / ".git" / "hooks" / "reference-transaction"
    target.unlink()
    first = subprocess.run(["sh", str(INSTALLER)], timeout=60, cwd=repo, env=_env(),
                           capture_output=True, text=True)
    assert first.returncode == 0 and "installed" in first.stdout and target.exists()
    second = subprocess.run(["sh", str(INSTALLER)], timeout=60, cwd=repo, env=_env(),
                            capture_output=True, text=True)
    assert "already installed and current" in second.stdout
    assert os.access(target, os.X_OK)


def test_assert_on_main_passes_on_a_clean_main_and_names_each_way_to_fail(repo: Path) -> None:
    def check_main(*args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(["sh", str(ASSERT), *args], timeout=60, cwd=repo, env=_env(),
                              capture_output=True, text=True)

    head = git(repo, "rev-parse", "HEAD").stdout.strip()
    assert check_main("start", head).returncode == 0
    (repo / "f.txt").write_text("edited under the run\n")
    tainted = check_main("end", head)
    assert tainted.returncode == 1 and "differ from HEAD" in tainted.stderr
    git(repo, "checkout", "--", "f.txt")
    assert check_main("end", "0" * 40).returncode == 1  # HEAD is not the commit the run started on
    git(repo, "checkout", "-q", "--detach", "HEAD~1", OSIRIS_ALLOW_SHARED_HEAD_MOVE="1")
    off = check_main("start")
    assert off.returncode == 1 and "detached" in off.stderr
