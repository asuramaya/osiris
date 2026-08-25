"""scripts/blob_content_sweep.py — task #30/#182's checked-in, re-runnable full
blob-content walk (Thoth DM 5574: the prior sweep, decision 55669bac, was run by hand and
never committed, so 29+ later commits could not be re-checked without rebuilding the
method from memory).

SELF-INCIDENT (Thoth DM 5583, commit 17e7fa6): this file's first version wrote the two
known-real leaked addresses in plain text, twice, to prove the detector caught them —
exactly the specimen #30 hunts for. Every test below that exercises the known-real-address
check now uses an entirely SYNTHETIC address and its own hash, injected via `scan_objects`'
`known_hashes` param — never the module's own `KNOWN_REAL_HASHES`, and never the two real
addresses as a string literal anywhere in this file. See
`test_this_files_own_source_and_tests_carry_no_literal_leak` below for the guard that makes
this class of mistake structurally hard to reintroduce."""
from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path

import pytest
from scripts.blob_content_sweep import (
    EMAIL_TOKEN_RE,
    KNOWN_REAL_HASHES,
    iter_all_objects,
    scan_objects,
    sweep,
)

_SYNTHETIC_ADDRESS = "definitely-not-a-real-address@gmail.com"
_SYNTHETIC_HASHES = frozenset({hashlib.sha256(_SYNTHETIC_ADDRESS.encode()).hexdigest()})


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


def _commit(repo: Path, msg: str) -> None:
    _git(repo, "commit", "--allow-empty", "-q", "-m", msg)


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    r = tmp_path / "repo"
    r.mkdir()
    _git(r, "init", "-q")
    _git(r, "config", "user.email", "test@test")
    _git(r, "config", "user.name", "test")
    return r


def test_clean_repo_finds_nothing(repo: Path) -> None:
    _commit(repo, "root")
    (repo / "a.txt").write_text("nothing interesting here\n")
    _git(repo, "add", "a.txt")
    _commit(repo, "add a")
    out = sweep(repo)
    assert out["findings"] == []
    assert out["commits_scanned"] == 2
    assert out["blobs_scanned"] == 1


def test_catches_a_hash_matched_address_in_blob_content(repo: Path) -> None:
    """Proves the HASH mechanism, not the real addresses: a synthetic address whose own
    hash is passed in via `known_hashes`, matching only because this test says so."""
    _commit(repo, "root")
    (repo / "config.py").write_text(f"OWNER_EMAIL = {_SYNTHETIC_ADDRESS!r}\n")
    _git(repo, "add", "config.py")
    _commit(repo, "leak in file content")
    objects = iter_all_objects(repo)
    out = scan_objects(objects, {}, known_hashes=_SYNTHETIC_HASHES)
    assert any(f.pattern == "known-real-address" for f in out)


def test_catches_a_hash_matched_address_in_a_commit_message_only(repo: Path) -> None:
    """The exact shape decision 9ba0bda134e0 found: file content clean, but the address
    survives as plain commit-message text — invisible to a file-content-only scan."""
    _commit(repo, "root")
    _commit(repo, f"dev:{_SYNTHETIC_ADDRESS} is a test fixture owner")
    objects = iter_all_objects(repo)
    out = scan_objects(objects, {}, known_hashes=_SYNTHETIC_HASHES)
    hits = [f for f in out if f.object_type == "commit"]
    assert any(f.pattern == "known-real-address" for f in hits)


def test_default_sweep_never_flags_the_synthetic_address(repo: Path) -> None:
    """Without an injected hash set, `sweep()` uses the module's own KNOWN_REAL_HASHES —
    a synthetic address never hashes to one of those, so the production path stays quiet
    on test data (same law test_ignores_a_known_synthetic_test_fixture_address checks for
    the regex side)."""
    _commit(repo, "root")
    (repo / "config.py").write_text(f"OWNER_EMAIL = {_SYNTHETIC_ADDRESS!r}\n")
    _git(repo, "add", "config.py")
    _commit(repo, "synthetic only")
    out = sweep(repo)
    assert not any(f.pattern == "known-real-address" for f in out["findings"])


def test_catches_a_leak_in_a_dead_end_commit_no_branch_reaches(repo: Path) -> None:
    """The exact gap a ref-walk (git log --all) cannot see: a commit orphaned by a reset,
    still sitting in the object database until gc — 55669bac's own 'unreachable objects'
    finding. --batch-all-objects walks the STORE, not the ref graph, so this is caught
    without a separate unreachable-object enumeration."""
    _commit(repo, "root")
    _commit(repo, "will be orphaned")
    _git(repo, "reset", "--hard", "HEAD~1")
    (repo / "b.txt").write_text("sk_live_" + "x" * 24 + "\n")
    _git(repo, "add", "b.txt")
    _commit(repo, "unrelated head")
    # the orphaned commit's own message never mentions a pattern; prove the WALK itself
    # reaches unreachable objects by checking object counts directly instead.
    objects = iter_all_objects(repo)
    shas = {o[0] for o in objects}
    dangling = subprocess.run(
        ["git", "-C", str(repo), "fsck", "--unreachable", "--no-reflogs"],
        check=True, capture_output=True, text=True).stdout
    assert dangling.strip()  # confirms the fixture actually produced an unreachable object
    dangling_sha = dangling.split()[2]
    assert dangling_sha in shas  # and the sweep's own object walk saw it


def test_ignores_a_known_synthetic_test_fixture_address(repo: Path) -> None:
    _commit(repo, "root")
    (repo / "fixtures.py").write_text("EMAIL = 'dakota.jm@gmail.com'\n")
    _git(repo, "add", "fixtures.py")
    _commit(repo, "synthetic fixture, already judged safe")
    out = sweep(repo)
    assert out["findings"] == []


def test_binary_content_never_raises(repo: Path) -> None:
    _commit(repo, "root")
    (repo / "bin.dat").write_bytes(bytes(range(256)) * 4)
    _git(repo, "add", "bin.dat")
    _commit(repo, "binary blob")
    out = sweep(repo)  # must not raise
    assert out["findings"] == []


def test_scan_objects_hash_check_is_pure_and_overridable(repo: Path) -> None:
    _commit(repo, "root")
    (repo / "x.txt").write_text(f"{_SYNTHETIC_ADDRESS}\n")
    _git(repo, "add", "x.txt")
    _commit(repo, "leak")
    objects = iter_all_objects(repo)
    out = scan_objects(objects, {}, known_hashes=_SYNTHETIC_HASHES)
    assert len(out) == 1
    assert out[0].pattern == "known-real-address"


def test_email_token_regex_is_broader_than_the_webmail_shape() -> None:
    """The hash check's own candidate extraction (EMAIL_TOKEN_RE) is deliberately not
    limited to known webmail domains — a hash comparison has no false-positive cost the
    way a printed literal would, so a real address on ANY domain is still caught."""
    assert EMAIL_TOKEN_RE.search("contact: someone@example-corp.io") is not None


_REPO_ROOT = Path(__file__).resolve().parent.parent
_SWEEP_SOURCE = _REPO_ROOT / "scripts" / "blob_content_sweep.py"
_SWEEP_TESTS = Path(__file__)


def test_this_files_own_source_and_tests_carry_no_literal_leak() -> None:
    """THE SELF-SCAN (Thoth DM 5583's more valuable half): a detector that cannot detect
    itself has a permanent blind spot at its most sensitive point. Scans this file's own
    on-disk text AND scripts/blob_content_sweep.py's own on-disk text for any email-shaped
    token whose hash lands in KNOWN_REAL_HASHES — the same rule every other object this
    sweep walks is held to. This is what makes the original mistake (the addresses spelled
    out as a literal regex/fixture) impossible to reintroduce silently: it would fail HERE,
    at every test run, not just at the next full live sweep."""
    for path in (_SWEEP_SOURCE, _SWEEP_TESTS):
        text = path.read_text()
        for token_match in EMAIL_TOKEN_RE.finditer(text):
            digest = hashlib.sha256(token_match.group(0).lower().encode()).hexdigest()
            assert digest not in KNOWN_REAL_HASHES, (
                f"{path} contains a literal match for a known-real address — "
                "the exact self-incident this test exists to catch")
