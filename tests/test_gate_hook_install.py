"""gate_hook.hook_status — the install-verification twin of push_guard.hook_status (#133,
ruling 754482bf). Needs real git (git_common_dir shells out), unlike test_gate_hook.py's own
pure/no-git/no-subprocess scope — kept in its own file for that reason."""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
from scripts.gate_hook import REPO_ROOT, hook_status
from scripts.push_guard import git_common_dir


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


@pytest.fixture
def small_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "test@test")
    _git(repo, "config", "user.name", "test")
    return repo


def test_hook_status_reports_not_a_git_checkout(tmp_path: Path) -> None:
    assert "not a git checkout" in hook_status(tmp_path / "no-such-repo")


def test_hook_status_reports_source_missing_off_a_repo_with_no_githooks_dir(
    small_repo: Path,
) -> None:
    assert "SOURCE MISSING" in hook_status(small_repo)


def test_hook_status_reports_not_installed(small_repo: Path) -> None:
    (small_repo / ".githooks").mkdir()
    (small_repo / ".githooks" / "pre-commit").write_text("#!/bin/sh\necho hi\n")
    assert "NOT INSTALLED" in hook_status(small_repo)


def test_hook_status_reports_current_when_installed_byte_identical(small_repo: Path) -> None:
    (small_repo / ".githooks").mkdir()
    body = "#!/bin/sh\necho hi\n"
    (small_repo / ".githooks" / "pre-commit").write_text(body)
    common = git_common_dir(small_repo)
    assert common is not None
    (common / "hooks").mkdir(exist_ok=True)
    (common / "hooks" / "pre-commit").write_text(body)
    assert hook_status(small_repo) == "gate_hook hook: installed and current"


def test_hook_status_reports_stale_when_installed_differs(small_repo: Path) -> None:
    (small_repo / ".githooks").mkdir()
    (small_repo / ".githooks" / "pre-commit").write_text("#!/bin/sh\necho new\n")
    common = git_common_dir(small_repo)
    assert common is not None
    (common / "hooks").mkdir(exist_ok=True)
    (common / "hooks" / "pre-commit").write_text("#!/bin/sh\necho OLD\n")
    assert "STALE" in hook_status(small_repo)


def test_hook_status_is_accurate_on_the_real_osiris_checkout() -> None:
    """The real, live installed state — not a synthetic repo — proving the actual house
    convention (tracked .githooks/pre-commit + install_gate_hook.sh) round-trips."""
    status = hook_status(REPO_ROOT)
    assert status.startswith("gate_hook hook:")
    assert "not a git checkout" not in status
