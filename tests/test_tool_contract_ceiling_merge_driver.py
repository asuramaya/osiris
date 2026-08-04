"""Reproduces THE ACTUAL COLLISION (dispatch 26686b77, Thoth msg 3658): two branches raise
a shared, hand-authored ratchet constant from the same base by different amounts; each
branch's own dry-run against the base reports clean (the other branch doesn't exist yet
from either one's perspective), and "take the larger of the two" silently under-counts
disjoint additive growth. This is not a unit test of `_reconcile`/`_const_value` in
isolation — it drives the REAL git plumbing (a temp repo, `.gitattributes`, a locally
registered `merge.*.driver`, real `git merge`) against a small, deterministic,
self-contained fixture — not the real 101-tool MCP surface (too slow, and too likely to
drift under unrelated future docstring edits to make a reliable regression test), but the
identical STRUCTURAL shape: a hand-authored ceiling constant, in its own file, colliding
with itself across two branches while the content it bounds changes on genuinely disjoint,
non-conflicting lines elsewhere.
"""
from __future__ import annotations

import os
import shlex
import subprocess
from pathlib import Path

_DRIVER_SCRIPT = str(
    Path(__file__).resolve().parent.parent
    / "scripts" / "reconcile_tool_contract_ceiling.py"
)

_SURFACE_BASE = '''def alpha() -> str:
    """placeholder."""
    return "a" * 10


def unrelated_one() -> None:
    pass


def unrelated_two() -> None:
    pass


def beta() -> str:
    """placeholder."""
    return "b" * 10
'''

_RATCHET_BASE = "# base ceiling, 20 chars total (10 + 10)\nCEILING = 20\n"


def _git(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "GIT_AUTHOR_NAME": "Test", "GIT_AUTHOR_EMAIL": "t@x.io",
           "GIT_COMMITTER_NAME": "Test", "GIT_COMMITTER_EMAIL": "t@x.io",
           "GIT_EDITOR": "true"}
    return subprocess.run(["git", "-C", str(repo), *args],
                          capture_output=True, text=True, env=env)


def _write(repo: Path, name: str, content: str) -> None:
    (repo / name).write_text(content)


def _commit(repo: Path, message: str) -> None:
    _git(repo, "add", "-A")
    r = _git(repo, "commit", "-q", "-m", message)
    assert r.returncode == 0, r.stderr


def _install_driver(repo: Path) -> None:
    """Same registration shape tests/conftest.py installs for the real ratchet file —
    worktree-agnostic (cd to toplevel, bare `python3`), just pointed at THIS test's fixture
    constant name instead of the real TOOL_CONTRACT_CEILING_CHARS."""
    inner = (
        'cd "$(git rev-parse --show-toplevel)" && '
        f'exec python3 {shlex.quote(_DRIVER_SCRIPT)} "$1" "$2" "$3" "$4" '
        '--constant-name CEILING'
    )
    driver_cmd = f"sh -c {shlex.quote(inner)} -- %O %A %B %P"
    r = _git(repo, "config", "--local", "merge.fixture_ceiling.driver", driver_cmd)
    assert r.returncode == 0, r.stderr


def _repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "base")
    _write(repo, "surface.py", _SURFACE_BASE)
    _write(repo, "fixture_ratchet.py", _RATCHET_BASE)
    _write(repo, ".gitattributes", "fixture_ratchet.py merge=fixture_ceiling\n")
    _commit(repo, "base")
    _install_driver(repo)
    return repo


def _branch_a(repo: Path) -> None:
    """+7 chars to alpha, honestly self-measured: 20 -> 27. Mirrors the real
    succession_chain-gains-session lane (+215, base 135_077 -> 135_292)."""
    _git(repo, "checkout", "-q", "-b", "branch-a", "base")
    _write(repo, "surface.py", _SURFACE_BASE.replace('return "a" * 10', 'return "a" * 17'))
    _write(repo, "fixture_ratchet.py",
           "# base ceiling, 20 chars total (10 + 10)\n"
           "# RAISED: alpha grew +7 (branch A's own honest measurement, base 20 -> 27)\n"
           "CEILING = 27\n")
    _commit(repo, "branch A: alpha +7")


def _branch_b(repo: Path) -> None:
    """+11 chars to beta, honestly self-measured: 20 -> 31. Mirrors the real
    cache-coherence lane (+112, base 135_077 -> 135_189)."""
    _git(repo, "checkout", "-q", "-b", "branch-b", "base")
    _write(repo, "surface.py", _SURFACE_BASE.replace('return "b" * 10', 'return "b" * 21'))
    _write(repo, "fixture_ratchet.py",
           "# base ceiling, 20 chars total (10 + 10)\n"
           "# RAISED: beta grew +11 (branch B's own honest measurement, base 20 -> 31)\n"
           "CEILING = 31\n")
    _commit(repo, "branch B: beta +11")


def _true_total(repo: Path, ref: str) -> int:
    """The independent oracle: actually import the fully-merged surface module (real,
    live measurement — reliable HERE because it runs after `git merge` has fully
    completed and the working tree is guaranteed consistent, unlike mid-driver)."""
    show = _git(repo, "show", f"{ref}:surface.py")
    assert show.returncode == 0, show.stderr
    ns: dict[str, object] = {}
    exec(show.stdout, ns)  # noqa: S102 — trusted, test-authored fixture content only
    alpha, beta = ns["alpha"], ns["beta"]
    return len(alpha()) + len(beta())  # type: ignore[operator]


def _ceiling(repo: Path, ref: str) -> str:
    show = _git(repo, "show", f"{ref}:fixture_ratchet.py")
    assert show.returncode == 0, show.stderr
    return show.stdout


def test_merge_order_a_then_b_resolves_to_the_true_combined_total(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    _branch_a(repo)
    _branch_b(repo)

    _git(repo, "checkout", "-q", "-b", "int-order1", "branch-a")
    merge = _git(repo, "merge", "--no-edit", "branch-b")
    assert merge.returncode == 0, (
        f"expected a clean auto-resolved merge (that's the whole point — no human "
        f"conflict prompt for this collision shape), got: {merge.stdout}\n{merge.stderr}")

    status = _git(repo, "status", "--porcelain")
    assert "fixture_ratchet.py" not in status.stdout  # nothing left uncommitted/conflicted

    ratchet = _ceiling(repo, "int-order1")
    assert "<<<<<<<" not in ratchet
    assert "CEILING = 38" in ratchet
    assert "alpha grew +7" in ratchet  # branch A's own narrative survived
    assert "beta grew +11" in ratchet  # branch B's own narrative survived — never dropped

    assert _true_total(repo, "int-order1") == 38  # the independent oracle agrees exactly


def test_merge_order_b_then_a_resolves_to_the_same_true_total(tmp_path: Path) -> None:
    """Order independence: whichever branch lands first, the SAME collision fires and the
    SAME correct total comes out — never a value that depends on merge order."""
    repo = _repo(tmp_path)
    _branch_a(repo)
    _branch_b(repo)

    _git(repo, "checkout", "-q", "-b", "int-order2", "branch-b")
    merge = _git(repo, "merge", "--no-edit", "branch-a")
    assert merge.returncode == 0, f"{merge.stdout}\n{merge.stderr}"

    ratchet = _ceiling(repo, "int-order2")
    assert "<<<<<<<" not in ratchet
    assert "CEILING = 38" in ratchet
    assert _true_total(repo, "int-order2") == 38


def test_naive_take_the_larger_would_have_undercounted(tmp_path: Path) -> None:
    """The falsification this whole driver exists to prevent, made explicit: the
    'intuitive' resolution (pick whichever branch asked for more) is 27 or 31 — both
    strictly less than the true, honestly-measured combined total of 38. Never a coincidence
    for disjoint additive growth: true total = base + both deltas > either delta alone
    whenever both deltas are positive."""
    repo = _repo(tmp_path)
    _branch_a(repo)
    _branch_b(repo)
    a_ceiling = int(_ceiling(repo, "branch-a").splitlines()[-1].split("=")[1])
    b_ceiling = int(_ceiling(repo, "branch-b").splitlines()[-1].split("=")[1])
    assert max(a_ceiling, b_ceiling) < 38


def test_driver_leaves_a_real_conflict_when_the_constant_is_missing_on_one_side(
    tmp_path: Path,
) -> None:
    """FAILS LOUD, NEVER GUESSES: not every conflict on this file is the disjoint-additive
    ceiling-bump shape this driver knows how to reconcile. Here branch C removes the
    constant entirely (a pretend refactor) while branch D bumps it — the constant is
    missing on one side, so this driver must decline rather than invent a number. git's own
    merge is left in place, a real conflict, same as without any driver at all."""
    repo = _repo(tmp_path)
    _git(repo, "checkout", "-q", "-b", "branch-c", "base")
    _write(repo, "fixture_ratchet.py", "# CEILING retired, replaced by something else\n")
    _commit(repo, "branch C: remove the constant entirely")

    _git(repo, "checkout", "-q", "-b", "branch-d", "base")
    _write(repo, "fixture_ratchet.py",
           "# base ceiling, 20 chars total (10 + 10)\n# bumped for unrelated reasons\n"
           "CEILING = 999\n")
    _commit(repo, "branch D: bump the ceiling")

    _git(repo, "checkout", "-q", "-b", "int-conflict", "branch-c")
    merge = _git(repo, "merge", "--no-edit", "branch-d")
    assert merge.returncode != 0  # a genuine conflict, correctly NOT auto-resolved to a guess
    status = _git(repo, "status", "--porcelain")
    assert "UU fixture_ratchet.py" in status.stdout or "AU fixture_ratchet.py" in \
        status.stdout or "UA fixture_ratchet.py" in status.stdout  # still unmerged
