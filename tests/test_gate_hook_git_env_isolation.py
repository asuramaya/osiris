"""The gate's pytest subprocess must not inherit git's per-hook GIT_* variables.

THE INCIDENT (2026-08-27, obligations 3da2dca9 / fdb04d23 / a35c042f): three workers
independently found the fleet's SHARED repository corrupted -- user.name/user.email
overwritten to test/test@test, worktree HEADs repointed to a fabricated orphan branch
`stray-history`, a fixture's own worktree registered in the real worktree list, and
core.bare set true on the main checkout.

THE MECHANISM: git exports GIT_DIR and GIT_INDEX_FILE (absolute paths) into every hook it
runs FROM A LINKED WORKTREE. gate_hook runs as a pre-commit hook, in worktrees, on every
commit, and spawned pytest with the ambient environment. GIT_DIR overrides repository
discovery for the entire subprocess tree, and `git -C <dir>` chdirs WITHOUT rescoping the
git directory -- so every test fixture building its own throwaway repo was writing into the
real one. The fixtures were correct; the environment was not.
"""

import importlib.util
import subprocess
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "gate_hook", Path(__file__).resolve().parent.parent / "scripts" / "gate_hook.py")
assert _SPEC and _SPEC.loader
gate_hook = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(gate_hook)


def _init(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init", "-q", str(path)], check=True)
    return path


def _local_email(repo: Path) -> str:
    done = subprocess.run(
        ["git", "-C", str(repo), "config", "--local", "--get", "user.email"],
        capture_output=True, text=True, check=False)
    return done.stdout.strip()


def test_pytest_env_drops_every_git_variable() -> None:
    scrubbed = gate_hook._pytest_env(
        {"GIT_DIR": "/real/.git", "GIT_INDEX_FILE": "/real/.git/index",
         "GIT_WORK_TREE": "/real", "PATH": "/usr/bin", "HOME": "/home/x"},
        {"TMPDIR": "/var/tmp/osiris-scratch"})
    assert not [k for k in scrubbed if k.startswith("GIT_")]
    # non-git ambient state survives, and the caller's own additions win
    assert scrubbed["PATH"] == "/usr/bin"
    assert scrubbed["HOME"] == "/home/x"
    assert scrubbed["TMPDIR"] == "/var/tmp/osiris-scratch"


def test_git_vars_are_removed_not_blanked() -> None:
    """An empty GIT_DIR is not "unset" -- it is a git dir whose path is "", equally wrong."""
    scrubbed = gate_hook._pytest_env({"GIT_DIR": "/real/.git"}, {})
    assert "GIT_DIR" not in scrubbed


def test_scrubbed_env_cannot_write_through_to_the_real_repo(tmp_path: Path) -> None:
    """The incident end to end: a -C-scoped write against an isolated repo, under a
    poisoned GIT_DIR, lands in the REAL repo -- and does not, once scrubbed."""
    real, iso = _init(tmp_path / "real"), _init(tmp_path / "iso")
    subprocess.run(["git", "-C", str(real), "config", "user.email", "real@real"], check=True)
    poisoned = {"GIT_DIR": str(real / ".git"), "GIT_INDEX_FILE": str(real / ".git" / "index"),
                "PATH": "/usr/bin:/bin", "HOME": str(tmp_path)}

    # NEGATIVE CONTROL: without the scrub the leak reproduces, or this test proves nothing.
    subprocess.run(["git", "-C", str(iso), "config", "user.email", "leak@leak"],
                   env=poisoned, check=True)
    assert _local_email(real) == "leak@leak", "control did not reproduce the leak"
    assert _local_email(iso) == "", "the write reached the isolated repo after all"

    subprocess.run(["git", "-C", str(real), "config", "user.email", "real@real"], check=True)

    subprocess.run(["git", "-C", str(iso), "config", "user.email", "scrubbed@scrubbed"],
                   env=gate_hook._pytest_env(poisoned, {}), check=True)
    assert _local_email(real) == "real@real", "the real repo was still written through to"
    assert _local_email(iso) == "scrubbed@scrubbed", "the isolated repo missed its own write"
