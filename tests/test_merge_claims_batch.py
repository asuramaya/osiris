"""`_batch_claim_notes` is `_verify_graph_claim` for many claims with two git processes in all.
`fleet` ran the per-claim path for every "merge <branch> (<sha>)" mention in the graph and spent
3.5 to 4.8 s of each call on the process spawns. The contract is EQUALITY with the per-claim path
on every kind of claim, so the repository here has every kind."""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest
from src.orchestrator import deploy_guard as dg

_ENV = {"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t", "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@t", "HOME": "/nonexistent", "GIT_CONFIG_NOSYSTEM": "1"}


def _git(repo: Path, *args: str) -> str:
    out = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True,
                         env={**os.environ, **_ENV}, check=True, timeout=30)
    return out.stdout.strip()


def _commit(repo: Path, name: str) -> str:
    (repo / name).write_text(name)
    _git(repo, "add", name)
    _git(repo, "commit", "-qm", f"add {name}")
    return _git(repo, "rev-parse", "HEAD")


@pytest.fixture
def repo(tmp_path: Path) -> dict[str, str]:
    r = tmp_path / "repo"
    r.mkdir()
    _git(r, "init", "-q", "-b", "main")
    first = _commit(r, "a")
    _git(r, "checkout", "-qb", "merged-feat")
    merged_tip = _commit(r, "b")
    _git(r, "checkout", "-q", "main")
    _git(r, "merge", "-q", "--no-ff", "merged-feat", "-m", "merge merged-feat")
    _git(r, "checkout", "-qb", "side")
    side_tip = _commit(r, "c")
    _git(r, "checkout", "-qb", "feat/slash")
    slash_tip = _commit(r, "d")
    _git(r, "checkout", "-q", "main")
    _git(r, "tag", "twin")  # a tag on main named like a branch that sits elsewhere
    _git(r, "branch", "twin", side_tip)
    blob = _git(r, "hash-object", "-w", "a")
    return {"root": str(r), "main": _git(r, "rev-parse", "main"), "first": first,
            "merged_tip": merged_tip, "side_tip": side_tip, "slash_tip": slash_tip,
            "blob": blob}


async def test_the_batch_equals_the_per_claim_path_on_every_kind_of_claim(
    repo: dict[str, str],
) -> None:
    root, main = Path(repo["root"]), repo["main"]
    short = lambda sha: sha[:7]  # noqa: E731
    claims: list[tuple[str, str | None]] = [
        ("merged-feat", None), ("merged-feat", short(repo["merged_tip"])),
        ("side", None), ("side", short(repo["side_tip"])), ("side", short(repo["first"])),
        ("feat/slash", None), ("feat/slash", short(repo["slash_tip"])),
        ("gone-branch", None), ("gone-branch", "deadbee"),
        ("gone-branch", short(repo["side_tip"])), ("merged-feat", "deadbee"),
        ("twin", None), ("twin", short(repo["first"])),
        ("main", None), ("main", short(repo["main"])),
        ("side", short(repo["blob"])), ("merged-feat", short(repo["blob"])),
        ("merged-feat", repo["merged_tip"]),  # a full 40-hex sha
    ]

    batch = dg._batch_claim_notes(root, main, claims)

    assert batch is not None
    for branch, cited in claims:
        per_claim = await dg._verify_graph_claim(root, main, branch, cited)
        assert batch[(branch, cited)] == per_claim, (branch, cited)
    # the claims that matter are all there: proven-false claims are reported, true ones are not
    assert batch[("side", None)] == "names 'side' but it is NOT an ancestor of main"
    assert batch[("merged-feat", None)] is None
    assert "NOT an ancestor of main" in str(batch[("side", short(repo["side_tip"]))])


def test_an_option_shaped_name_is_left_to_the_per_claim_path(repo: dict[str, str]) -> None:
    root, main = Path(repo["root"]), repo["main"]
    assert dg._batch_claim_notes(root, main, [("-oops", None), ("side", None)]) is None


def test_a_git_failure_is_left_to_the_per_claim_path(tmp_path: Path) -> None:
    not_a_repo = tmp_path / "nothing"
    not_a_repo.mkdir()
    assert dg._batch_claim_notes(not_a_repo, "0" * 40, [("side", None)]) is None


def test_the_batch_uses_two_git_processes_however_many_claims(
    repo: dict[str, str], monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []
    real = subprocess.run

    def counting(cmd: list[str], *a: object, **k: object) -> object:
        calls.append(cmd)
        return real(cmd, *a, **k)  # type: ignore[call-overload]

    monkeypatch.setattr(dg.subprocess, "run", counting)
    claims = [(f"branch-{i}", f"{i:07x}") for i in range(200)] + [("side", None)]

    out = dg._batch_claim_notes(Path(repo["root"]), repo["main"], claims)

    assert out is not None and len(out) == len(claims)
    assert len(calls) == 2, calls
