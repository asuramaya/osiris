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
    scrubbed, note = gate_hook._pytest_env(
        {"GIT_DIR": "/real/.git", "GIT_INDEX_FILE": "/real/.git/index",
         "GIT_WORK_TREE": "/real", "PATH": "/usr/bin", "HOME": "/home/x"},
        {"TMPDIR": "/var/tmp/osiris-scratch"})
    assert not [k for k in scrubbed if k.startswith("GIT_")]
    # non-git ambient state survives, and the caller's own additions win
    assert scrubbed["PATH"] == "/usr/bin"
    assert scrubbed["HOME"] == "/home/x"
    assert scrubbed["TMPDIR"] == "/var/tmp/osiris-scratch"
    assert note is None  # a safe TMPDIR relocates nothing and says nothing


def test_git_vars_are_removed_not_blanked() -> None:
    """An empty GIT_DIR is not "unset" -- it is a git dir whose path is "", equally wrong."""
    scrubbed, _note = gate_hook._pytest_env({"GIT_DIR": "/real/.git"}, {})
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

    scrubbed_env, _note = gate_hook._pytest_env(poisoned, {})
    subprocess.run(["git", "-C", str(iso), "config", "user.email", "scrubbed@scrubbed"],
                   env=scrubbed_env, check=True)
    assert _local_email(real) == "real@real", "the real repo was still written through to"
    assert _local_email(iso) == "scrubbed@scrubbed", "the isolated repo missed its own write"


# --- obligation 13d3ddbf: the gate must never measure the box it runs on ------------------

def test_pytest_env_relocates_a_tmpdir_nested_under_a_jobs_tree() -> None:
    """THE INCIDENT: a gate run whose TMPDIR sits under a live jobs/sessions tree makes
    job-anchor tests read the RUNNER's own job id instead of a synthetic test one
    (measured live, test_spawned_wake_carries_a_durable_job_dir_anchor). Relocates rather
    than refuses, and says so via the returned note — never silent, never a hard failure
    over an environment variable the gate can fix itself."""
    scrubbed, note = gate_hook._pytest_env(
        {"PATH": "/usr/bin"},
        {"TMPDIR": "/home/user/.claude/jobs/abc123/tmp"})
    assert scrubbed["TMPDIR"] == gate_hook._SAFE_TMPDIR
    assert note is not None
    assert "13d3ddbf" in note and "jobs/abc123" in note


def test_pytest_env_relocates_a_tmpdir_nested_under_a_sessions_tree() -> None:
    scrubbed, note = gate_hook._pytest_env(
        {"PATH": "/usr/bin"}, {"TMPDIR": "/home/user/.dsh/sessions/workspace/run-1/tmp"})
    assert scrubbed["TMPDIR"] == gate_hook._SAFE_TMPDIR
    assert note is not None


def test_pytest_env_leaves_an_ordinary_tmpdir_alone() -> None:
    """THE NEGATIVE CONTROL: the safe default this call site already hardcodes (and any
    other genuinely throwaway location) must never be second-guessed or relocated."""
    scrubbed, note = gate_hook._pytest_env(
        {"PATH": "/usr/bin"}, {"TMPDIR": gate_hook._SAFE_TMPDIR})
    assert scrubbed["TMPDIR"] == gate_hook._SAFE_TMPDIR
    assert note is None


def test_pytest_env_is_quiet_with_no_tmpdir_at_all() -> None:
    scrubbed, note = gate_hook._pytest_env({"PATH": "/usr/bin"}, {})
    assert "TMPDIR" not in scrubbed
    assert note is None


def test_no_tests_collected_is_its_own_verdict_not_a_failure(tmp_path: Path) -> None:
    """pytest exit 5 (NO_TESTS_COLLECTED) must not read as a test failure.

    LIVE SPECIMEN (2026-08-27): a commit touching ONLY tests/conftest.py. conftest is a real
    file under tests/, so the gate selects it; it declares fixtures and defines no test
    functions; pytest exits 5; a bare `returncode == 0` refused a clean commit. Found by the
    gate refusing this file's own companion commit.

    This is run_gates' own documented law one layer down -- it handles "nothing SELECTED"
    honestly and did not handle "selected, but zero tests in it". Reporting "not run" as
    "failed" is the mirror of reporting it as "passed", and equally a gate that cannot tell
    the two apart.
    """
    assert gate_hook._PYTEST_EXIT_NO_TESTS_COLLECTED == 5

    # the behaviour that defines the constant, asserted against real pytest, not a mock
    fixtures_only = tmp_path / "conftest.py"
    fixtures_only.write_text(
        "import pytest\n\n\n@pytest.fixture\ndef thing() -> int:\n    return 1\n")
    done = subprocess.run(
        [str(gate_hook.VENV_BIN / "pytest"), str(fixtures_only), "-q", "-p", "no:cacheprovider"],
        capture_output=True, text=True, check=False, cwd=tmp_path)
    assert done.returncode == gate_hook._PYTEST_EXIT_NO_TESTS_COLLECTED, (
        f"pytest exit for a fixtures-only file changed: {done.returncode}\n{done.stdout}")

    # NEGATIVE CONTROL: a file with a real test must NOT take the NO-TESTS branch
    with_a_test = tmp_path / "test_real.py"
    with_a_test.write_text("def test_ok() -> None:\n    assert True\n")
    done2 = subprocess.run(
        [str(gate_hook.VENV_BIN / "pytest"), str(with_a_test), "-q", "-p", "no:cacheprovider"],
        capture_output=True, text=True, check=False, cwd=tmp_path)
    assert done2.returncode == 0
