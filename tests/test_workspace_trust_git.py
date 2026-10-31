"""The trust rule for a directory inside a git repository, as measured on the installed
harness: judged by the repository, not by ancestors above it. A fresh `git init` directory
under a trusted parent was refused ("Workspace not trusted") while a plain directory in the
same place was accepted; a linked worktree with no entry of its own is accepted through its
main repository."""
from __future__ import annotations

from pathlib import Path

from src.orchestrator.workspace_trust import is_trusted, untrusted_dirs


def _trust(*paths: Path, false: tuple[Path, ...] = ()) -> dict[str, dict[str, bool]]:
    out = {str(p): {"hasTrustDialogAccepted": True} for p in paths}
    out.update({str(p): {"hasTrustDialogAccepted": False} for p in false})
    return out


def _repo(path: Path) -> Path:
    (path / ".git").mkdir(parents=True)
    return path


def _worktree_of(main: Path, wt: Path, name: str = "w1") -> Path:
    gitdir = main / ".git" / "worktrees" / name
    gitdir.mkdir(parents=True)
    (gitdir / "commondir").write_text("../..\n")
    wt.mkdir(parents=True)
    (wt / ".git").write_text(f"gitdir: {gitdir}\n")
    return wt


def test_a_plain_directory_under_a_trusted_parent_is_trusted(tmp_path: Path) -> None:
    parent = tmp_path / "code"
    plain = parent / "plain" / "deeper"
    plain.mkdir(parents=True)
    assert is_trusted(str(plain), _trust(parent))


def test_a_git_repository_under_a_trusted_parent_is_not_trusted_by_the_parent(
    tmp_path: Path,
) -> None:
    """The handlingtheloop shape: a repository with no entry of its own."""
    parent = tmp_path / "code"
    repo = _repo(parent / "loop")
    assert not is_trusted(str(repo), _trust(parent))
    (repo / "src").mkdir()
    assert not is_trusted(str(repo / "src"), _trust(parent))
    assert untrusted_dirs([str(repo)], home=_home(tmp_path, _trust(parent))) == [str(repo)]


def test_a_repository_with_its_own_entry_and_its_subdirectories_are_trusted(
    tmp_path: Path,
) -> None:
    repo = _repo(tmp_path / "code" / "loop")
    sub = repo / "server" / "api"
    sub.mkdir(parents=True)
    assert is_trusted(str(repo), _trust(repo))
    assert is_trusted(str(sub), _trust(repo))


def test_a_repository_whose_own_entry_says_false_is_not_trusted_even_under_a_trusted_parent(
    tmp_path: Path,
) -> None:
    parent = tmp_path / "code"
    repo = _repo(parent / "stop")
    assert not is_trusted(str(repo), _trust(parent, false=(repo,)))


def test_a_linked_worktree_is_trusted_through_its_main_repository(tmp_path: Path) -> None:
    main = _repo(tmp_path / "code" / "osiris")
    wt = _worktree_of(main, main / ".claude" / "worktrees" / "feature")
    sub = wt / "src"
    sub.mkdir()
    assert is_trusted(str(wt), _trust(main))        # no entry of its own
    assert is_trusted(str(sub), _trust(main))
    assert not is_trusted(str(wt), {})              # main repo untrusted too
    assert is_trusted(str(wt), _trust(wt))          # or trusted on its own entry


def test_a_worktree_of_an_untrusted_main_is_not_rescued_by_a_trusted_parent(
    tmp_path: Path,
) -> None:
    parent = tmp_path / "code"
    main = _repo(parent / "other")
    wt = _worktree_of(main, tmp_path / "elsewhere" / "wt")
    assert not is_trusted(str(wt), _trust(parent))


def test_an_unreadable_worktree_pointer_counts_as_the_worktree_alone(tmp_path: Path) -> None:
    wt = tmp_path / "wt"
    wt.mkdir()
    (wt / ".git").write_text("not a pointer")
    assert not is_trusted(str(wt), _trust(tmp_path))
    assert is_trusted(str(wt), _trust(wt))


def _home(tmp_path: Path, projects: dict[str, dict[str, bool]]) -> Path:
    import json

    home = tmp_path / "home"
    home.mkdir(exist_ok=True)
    (home / ".claude.json").write_text(json.dumps({"projects": projects}))
    return home
