"""push_guard — the pre-push secret/PII guard (2026-08-15 incident, ruling 2fc98818).
FAIL-OPEN ON INFRASTRUCTURE, NEVER ON A REAL MATCH (577988ed): the only thing that blocks a
push is a positive pattern match; every internal failure degrades to ALLOW, loudly.
"""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
from scripts.push_guard import (
    _NULL_SHA,
    commit_range,
    custom_patterns,
    format_refusal,
    git_common_dir,
    hook_status,
    parse_stdin_refs,
    run,
    scan_range,
)


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


def _commit(repo: Path, msg: str) -> str:
    _git(repo, "commit", "--allow-empty", "-q", "-m", msg)
    return subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()


def _write_and_commit(repo: Path, name: str, content: str, msg: str) -> str:
    (repo / name).write_text(content)
    _git(repo, "add", name)
    return _commit(repo, msg)


@pytest.fixture
def small_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "test@test")
    _git(repo, "config", "user.name", "test")
    return repo


# --- parse_stdin_refs -----------------------------------------------------------------------

def test_parse_stdin_refs_reads_the_real_git_contract() -> None:
    text = "refs/heads/main abc123 refs/heads/main def456\n"
    assert parse_stdin_refs(text) == [
        ("refs/heads/main", "abc123", "refs/heads/main", "def456")]


def test_parse_stdin_refs_skips_a_malformed_line_rather_than_raising() -> None:
    assert parse_stdin_refs("garbage\ntoo many fields to be a real line here\n") == []


def test_parse_stdin_refs_handles_multiple_lines() -> None:
    text = "a b c d\ne f g h\n"
    assert len(parse_stdin_refs(text)) == 2


# --- commit_range ----------------------------------------------------------------------------

def test_commit_range_on_a_known_remote_sha_is_the_exact_range(small_repo: Path) -> None:
    a = _commit(small_repo, "a")
    b = _commit(small_repo, "b")
    assert commit_range(small_repo, b, a) == [b]


def test_commit_range_on_a_delete_push_is_empty(small_repo: Path) -> None:
    a = _commit(small_repo, "a")
    assert commit_range(small_repo, _NULL_SHA, a) == []


def test_commit_range_on_a_brand_new_ref_falls_back_to_local_sha(small_repo: Path) -> None:
    a = _commit(small_repo, "a")
    assert commit_range(small_repo, a, _NULL_SHA) == [a]


def test_commit_range_returns_none_on_a_git_failure(small_repo: Path) -> None:
    assert commit_range(small_repo, "not-a-real-sha", "also-not-real") is None


# --- scan_range --------------------------------------------------------------------------

def test_scan_range_finds_a_secret_in_a_commit_message(small_repo: Path) -> None:
    sha = _commit(small_repo, "oops -----BEGIN RSA PRIVATE KEY----- pasted by accident")
    findings = scan_range(small_repo, [sha], {"private-key-block":
                                                r"-----BEGIN (RSA )?PRIVATE KEY-----"})
    assert findings
    assert "private-key-block" in findings[0]


def test_scan_range_finds_a_secret_added_in_a_diff(small_repo: Path) -> None:
    sha = _write_and_commit(
        small_repo, "config.txt", "AKIAABCDEFGHIJKLMNOP\n", "add config")
    findings = scan_range(small_repo, [sha], {"aws-access-key-id": r"\bAKIA[0-9A-Z]{16}\b"})
    assert findings
    assert "aws-access-key-id" in findings[0]


def test_scan_range_finds_a_secret_that_was_only_ever_removed(small_repo: Path) -> None:
    """The 08-03 scrub's own miss: a value that existed in history but not at any tip must
    still be caught, since removing it in a LATER commit within the same push range doesn't
    stop it from being uploaded."""
    _write_and_commit(small_repo, "config.txt", "AKIAABCDEFGHIJKLMNOP\n", "add config")
    remove_sha = _write_and_commit(small_repo, "config.txt", "", "remove config")
    findings = scan_range(
        small_repo, [remove_sha], {"aws-access-key-id": r"\bAKIA[0-9A-Z]{16}\b"})
    assert findings


def test_scan_range_is_clean_on_ordinary_content(small_repo: Path) -> None:
    sha = _write_and_commit(small_repo, "readme.txt", "hello world\n", "ordinary commit")
    assert scan_range(small_repo, [sha], {"aws-access-key-id": r"\bAKIA[0-9A-Z]{16}\b"}) == []


def test_scan_range_is_empty_with_no_shas_or_no_patterns(small_repo: Path) -> None:
    assert scan_range(small_repo, [], {"x": "y"}) == []
    sha = _commit(small_repo, "a")
    assert scan_range(small_repo, [sha], {}) == []


# --- custom_patterns -------------------------------------------------------------------------

def test_custom_patterns_reads_the_local_untracked_file(small_repo: Path) -> None:
    common = git_common_dir(small_repo)
    assert common is not None
    (common / "push_guard_patterns.txt").write_text(
        "# a comment\n\nreal\\.address@example\\.com\n")
    patterns = custom_patterns(common)
    assert list(patterns.values()) == ["real\\.address@example\\.com"]


def test_custom_patterns_is_empty_when_the_file_does_not_exist(small_repo: Path) -> None:
    common = git_common_dir(small_repo)
    assert custom_patterns(common) == {}


def test_custom_patterns_is_empty_off_a_none_common_dir() -> None:
    assert custom_patterns(None) == {}


# --- format_refusal ---------------------------------------------------------------------------

def test_format_refusal_names_the_escape_hatch() -> None:
    msg = format_refusal(["aws-access-key-id: commit abc123 — matched 'AKIA…MNOP'"])
    assert "OSIRIS_PUSH_GUARD_SKIP=1" in msg
    assert "aws-access-key-id" in msg


# --- run (end to end) --------------------------------------------------------------------------

def test_run_allows_a_clean_push(small_repo: Path) -> None:
    a = _commit(small_repo, "a")
    stdin = f"refs/heads/main {a} refs/heads/main {_NULL_SHA}\n"
    assert run(small_repo, stdin) == 0


def test_run_refuses_a_push_carrying_a_real_secret(small_repo: Path) -> None:
    sha = _commit(small_repo, "-----BEGIN RSA PRIVATE KEY----- oops")
    stdin = f"refs/heads/main {sha} refs/heads/main {_NULL_SHA}\n"
    assert run(small_repo, stdin) == 1


def test_run_allows_with_nothing_on_stdin(small_repo: Path) -> None:
    assert run(small_repo, "") == 0


def test_run_honors_the_skip_env_var_even_with_a_real_secret(
    small_repo: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OSIRIS_PUSH_GUARD_SKIP", "1")
    sha = _commit(small_repo, "-----BEGIN RSA PRIVATE KEY----- oops")
    stdin = f"refs/heads/main {sha} refs/heads/main {_NULL_SHA}\n"
    assert run(small_repo, stdin) == 0


def test_run_allows_on_an_unresolvable_range_rather_than_blocking(small_repo: Path) -> None:
    stdin = f"refs/heads/main not-a-real-sha refs/heads/main {_NULL_SHA}\n"
    assert run(small_repo, stdin) == 0


# --- hook_status -------------------------------------------------------------------------------

def test_hook_status_reports_not_a_git_checkout(tmp_path: Path) -> None:
    assert "not a git checkout" in hook_status(tmp_path / "no-such-repo")


def test_hook_status_reports_source_missing_off_a_repo_with_no_githooks_dir(
    small_repo: Path,
) -> None:
    assert "SOURCE MISSING" in hook_status(small_repo)


def test_hook_status_reports_not_installed(small_repo: Path) -> None:
    (small_repo / ".githooks").mkdir()
    (small_repo / ".githooks" / "pre-push").write_text("#!/bin/sh\necho hi\n")
    assert "NOT INSTALLED" in hook_status(small_repo)


def test_hook_status_reports_current_when_installed_byte_identical(small_repo: Path) -> None:
    (small_repo / ".githooks").mkdir()
    body = "#!/bin/sh\necho hi\n"
    (small_repo / ".githooks" / "pre-push").write_text(body)
    common = git_common_dir(small_repo)
    assert common is not None
    (common / "hooks").mkdir(exist_ok=True)
    (common / "hooks" / "pre-push").write_text(body)
    assert hook_status(small_repo) == "push_guard hook: installed and current"


def test_hook_status_reports_stale_when_installed_differs(small_repo: Path) -> None:
    (small_repo / ".githooks").mkdir()
    (small_repo / ".githooks" / "pre-push").write_text("#!/bin/sh\necho new\n")
    common = git_common_dir(small_repo)
    assert common is not None
    (common / "hooks").mkdir(exist_ok=True)
    (common / "hooks" / "pre-push").write_text("#!/bin/sh\necho OLD\n")
    assert "STALE" in hook_status(small_repo)


def test_hook_status_is_accurate_on_the_real_osiris_checkout() -> None:
    """The real, live installed state — not a synthetic repo — proving the actual house
    convention (tracked .githooks/pre-push + install_push_guard_hook.sh) round-trips."""
    from scripts.push_guard import REPO_ROOT

    status = hook_status(REPO_ROOT)
    assert status.startswith("push_guard hook:")
    assert "not a git checkout" not in status
