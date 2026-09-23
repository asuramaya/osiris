"""Drives real git plumbing (a temp repo, .gitattributes, a locally registered
merge.*.driver, real `git merge`) against a small, deterministic fixture that mirrors
the actual product-voice/taxonomy-drift baseline files: a flat {bucket: int} JSON
object that several concurrent tips each regenerate in full, colliding on keys they
never actually touched in common. See scripts/reconcile_product_voice_baselines.py's
own docstring for the per-key-minimum rationale.
"""
from __future__ import annotations

import json
import os
import shlex
import subprocess
from pathlib import Path

_DRIVER_SCRIPT = str(
    Path(__file__).resolve().parent.parent
    / "scripts" / "reconcile_product_voice_baselines.py"
)

_BASE = {"alpha": 10, "beta": 10, "gamma": 10}


def _git(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "GIT_AUTHOR_NAME": "Test", "GIT_AUTHOR_EMAIL": "t@x.io",
           "GIT_COMMITTER_NAME": "Test", "GIT_COMMITTER_EMAIL": "t@x.io",
           "GIT_EDITOR": "true"}
    return subprocess.run(["git", "-C", str(repo), *args],
                          capture_output=True, text=True, env=env)


def _write_json(repo: Path, name: str, data: dict[str, int]) -> None:
    (repo / name).write_text(json.dumps(data, indent=2) + "\n")


def _commit(repo: Path, message: str) -> None:
    _git(repo, "add", "-A")
    r = _git(repo, "commit", "-q", "-m", message)
    assert r.returncode == 0, r.stderr


def _install_driver(repo: Path) -> None:
    inner = (
        'cd "$(git rev-parse --show-toplevel)" && '
        f'exec python3 {shlex.quote(_DRIVER_SCRIPT)} "$1" "$2" "$3" "$4"'
    )
    driver_cmd = f"sh -c {shlex.quote(inner)} -- %O %A %B %P"
    r = _git(repo, "config", "--local", "merge.baseline_min.driver", driver_cmd)
    assert r.returncode == 0, r.stderr


def _repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "base")
    _write_json(repo, "baseline.json", _BASE)
    (repo / ".gitattributes").write_text("baseline.json merge=baseline_min\n")
    _commit(repo, "base")
    _install_driver(repo)
    return repo


def _read_json(repo: Path, ref: str) -> dict[str, int]:
    show = _git(repo, "show", f"{ref}:baseline.json")
    assert show.returncode == 0, show.stderr
    result: dict[str, int] = json.loads(show.stdout)
    return result


def test_disjoint_lowers_on_different_keys_merge_clean_to_the_minimum_of_each(
    tmp_path: Path,
) -> None:
    """The real collision shape: branch A lowers alpha (a cleanup tip), branch B lowers
    beta (a different cleanup tip landing around the same time); neither touches the
    other's key, but naive git still conflicts on the whole JSON object's single
    top-level diff hunk."""
    repo = _repo(tmp_path)
    _git(repo, "checkout", "-q", "-b", "branch-a", "base")
    _write_json(repo, "baseline.json", {"alpha": 3, "beta": 10, "gamma": 10})
    _commit(repo, "branch A: lower alpha")

    _git(repo, "checkout", "-q", "-b", "branch-b", "base")
    _write_json(repo, "baseline.json", {"alpha": 10, "beta": 4, "gamma": 10})
    _commit(repo, "branch B: lower beta")

    _git(repo, "checkout", "-q", "-b", "integrated", "branch-a")
    merge = _git(repo, "merge", "--no-edit", "branch-b")
    assert merge.returncode == 0, (
        f"expected a clean auto-resolved merge, got: {merge.stdout}\n{merge.stderr}")

    status = _git(repo, "status", "--porcelain")
    assert "baseline.json" not in status.stdout

    result = _read_json(repo, "integrated")
    assert result == {"alpha": 3, "beta": 4, "gamma": 10}


def test_a_key_only_one_side_ever_touched_keeps_that_sides_value(
    tmp_path: Path,
) -> None:
    """A key neither side's own conflicting hunk disagrees on (only one side ever wrote
    it at all, e.g. a brand-new bucket a tip introduced) is kept as-is, never dropped and
    never compared against a value the other side never had an opinion on."""
    repo = _repo(tmp_path)
    _git(repo, "checkout", "-q", "-b", "branch-c", "base")
    _write_json(repo, "baseline.json", {"alpha": 5, "beta": 10, "gamma": 10, "delta": 7})
    _commit(repo, "branch C: lower alpha, add a new bucket")

    _git(repo, "checkout", "-q", "-b", "branch-d", "base")
    _write_json(repo, "baseline.json", {"alpha": 10, "beta": 2, "gamma": 10})
    _commit(repo, "branch D: lower beta")

    _git(repo, "checkout", "-q", "-b", "integrated2", "branch-c")
    merge = _git(repo, "merge", "--no-edit", "branch-d")
    assert merge.returncode == 0, f"{merge.stdout}\n{merge.stderr}"

    result = _read_json(repo, "integrated2")
    assert result == {"alpha": 5, "beta": 2, "gamma": 10, "delta": 7}


def test_merge_order_independence(tmp_path: Path) -> None:
    """Whichever branch lands first, the same per-key-minimum result comes out."""
    repo = _repo(tmp_path)
    _git(repo, "checkout", "-q", "-b", "branch-e", "base")
    _write_json(repo, "baseline.json", {"alpha": 6, "beta": 10, "gamma": 10})
    _commit(repo, "branch E: lower alpha")

    _git(repo, "checkout", "-q", "-b", "branch-f", "base")
    _write_json(repo, "baseline.json", {"alpha": 10, "beta": 9, "gamma": 1})
    _commit(repo, "branch F: lower beta and gamma")

    _git(repo, "checkout", "-q", "-b", "order1", "branch-e")
    m1 = _git(repo, "merge", "--no-edit", "branch-f")
    assert m1.returncode == 0, f"{m1.stdout}\n{m1.stderr}"

    _git(repo, "checkout", "-q", "-b", "order2", "branch-f")
    m2 = _git(repo, "merge", "--no-edit", "branch-e")
    assert m2.returncode == 0, f"{m2.stdout}\n{m2.stderr}"

    expected = {"alpha": 6, "beta": 9, "gamma": 1}
    assert _read_json(repo, "order1") == expected
    assert _read_json(repo, "order2") == expected


def test_a_malformed_side_declines_rather_than_guessing(tmp_path: Path) -> None:
    """FAILS LOUD, NEVER GUESSES: a side whose file isn't a flat {str: int} object (a
    genuinely different shape, not this driver's own collision) must leave a real
    conflict, never invent a resolution."""
    repo = _repo(tmp_path)
    _git(repo, "checkout", "-q", "-b", "branch-g", "base")
    (repo / "baseline.json").write_text('{"alpha": "not-a-number"}\n')
    _commit(repo, "branch G: baseline.json is no longer a flat int map")

    _git(repo, "checkout", "-q", "-b", "branch-h", "base")
    _write_json(repo, "baseline.json", {"alpha": 2, "beta": 10, "gamma": 10})
    _commit(repo, "branch H: lower alpha normally")

    _git(repo, "checkout", "-q", "-b", "integrated3", "branch-g")
    merge = _git(repo, "merge", "--no-edit", "branch-h")
    assert merge.returncode != 0  # a genuine conflict, correctly not auto-resolved
    status = _git(repo, "status", "--porcelain")
    assert "UU baseline.json" in status.stdout or "AU baseline.json" in status.stdout \
        or "UA baseline.json" in status.stdout
