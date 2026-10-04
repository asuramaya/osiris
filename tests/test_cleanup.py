"""Clean-up by default (scripts/osiris_cleanup.py): every rule and every guard, against scratch
directories, throwaway git repositories and a fake Docker, never the real disk."""
from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

import pytest
from scripts import osiris_cleanup as c

DAY = 86400.0


def _age_one(path: Path, days: float) -> None:
    t = time.time() - days * DAY
    os.utime(path, (t, t), follow_symlinks=False)


def _age(path: Path, days: float) -> None:
    for p in [path, *path.rglob("*")]:
        _age_one(p, days)


def _scratch(tmp_path: Path) -> Path:
    root = tmp_path / "osiris-scratch"
    root.mkdir()
    return root


def _scratch_report(root: Path, *, apply: bool, cwds: list[Path] | None = None,
                    only_pt: bool = False, days: float = 2.0) -> c.Report:
    report = c.Report(apply=apply)
    c.plan_scratch([(root, only_pt)], now=time.time(), max_age_days=days, apply=apply,
                   cwds=cwds or [], report=report)
    return report


# --- scratch -------------------------------------------------------------------------------

def test_an_old_scratch_entry_is_removed_and_a_recent_one_stays(tmp_path: Path) -> None:
    root = _scratch(tmp_path)
    old, new = root / "pt-111", root / "pt-222"
    for d in (old, new):
        (d / "deep").mkdir(parents=True)
        (d / "deep" / "f.bin").write_bytes(b"x" * 1000)
    _age(old, 5)
    report = _scratch_report(root, apply=True)
    assert not old.exists() and new.exists()
    assert [a.target for a in report.actions] == [str(old)]
    assert report.actions[0].bytes == 1000 and report.actions[0].done


def test_a_dry_run_deletes_nothing_and_still_reports_the_bytes(tmp_path: Path) -> None:
    root = _scratch(tmp_path)
    old = root / "pt-1"
    old.mkdir()
    (old / "f").write_bytes(b"y" * 500)
    _age(old, 9)
    report = _scratch_report(root, apply=False)
    assert old.exists() and report.actions[0].bytes == 500 and not report.actions[0].done
    assert report.freed() == 500


def test_one_recent_file_deep_inside_keeps_the_whole_entry(tmp_path: Path) -> None:
    root = _scratch(tmp_path)
    d = root / "pt-5"
    (d / "a" / "b").mkdir(parents=True)
    (d / "a" / "b" / "old").write_bytes(b"1")
    (d / "a" / "b" / "fresh").write_bytes(b"2")
    for p in (d / "a" / "b" / "old", d, d / "a", d / "a" / "b"):
        _age_one(p, 10)
    # `fresh` keeps its current mtime
    assert _scratch_report(root, apply=True).actions == []
    assert d.exists()


def test_a_directory_with_a_running_process_inside_is_never_removed(tmp_path: Path) -> None:
    root = _scratch(tmp_path)
    d = root / "pt-7"
    (d / "sub").mkdir(parents=True)
    _age(d, 30)
    report = _scratch_report(root, apply=True, cwds=[d / "sub"])
    assert d.exists() and report.actions == []
    assert any("working inside" in s for s in report.skipped)


def test_only_pt_directories_are_candidates_in_a_shared_parent(tmp_path: Path) -> None:
    parent = tmp_path / "var-tmp"
    parent.mkdir()
    mine, theirs, a_file = parent / "pt-42", parent / "launchpadlib.cache", parent / "notes.txt"
    mine.mkdir()
    theirs.mkdir()
    a_file.write_text("keep")
    for p in (mine, theirs, a_file):
        _age(p, 30)
    _scratch_report(parent, apply=True, only_pt=True)
    assert not mine.exists() and theirs.exists() and a_file.exists()


def test_a_symlink_is_never_followed_and_its_target_survives(tmp_path: Path) -> None:
    root = _scratch(tmp_path)
    precious = tmp_path / "precious"
    precious.mkdir()
    (precious / "keep.txt").write_text("keep")
    link = root / "pt-9"
    link.symlink_to(precious)
    os.utime(link, (time.time() - 30 * DAY,) * 2, follow_symlinks=False)
    _scratch_report(root, apply=True)
    assert (precious / "keep.txt").read_text() == "keep"


def test_the_root_itself_and_anything_outside_it_is_refused(tmp_path: Path) -> None:
    root = _scratch(tmp_path)
    outside = tmp_path / "elsewhere"
    outside.mkdir()
    with pytest.raises(ValueError):
        c._safe_remove(root, root)
    with pytest.raises(ValueError):
        c._safe_remove(outside, root)
    assert root.exists() and outside.exists()


def test_a_busy_tree_costs_a_few_stats_not_a_full_walk(tmp_path: Path, monkeypatch) -> None:
    root = _scratch(tmp_path)
    d = root / "pt-3"
    d.mkdir()
    for i in range(50):
        (d / f"f{i}").write_bytes(b"z")
    seen = {"n": 0}
    real = os.scandir

    def counting(path):  # type: ignore[no-untyped-def]
        seen["n"] += 1
        return real(path)

    monkeypatch.setattr(os, "scandir", counting)
    assert c.has_recent_file(d, time.time() - DAY) is True
    assert seen["n"] == 0  # the directory's own fresh mtime answered it


# --- worktrees -----------------------------------------------------------------------------

def _git(cwd: Path, *args: str, env: dict[str, str] | None = None) -> str:
    full = {**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t",
            "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t", **(env or {})}
    out = subprocess.run(
        ["git", *args], cwd=cwd, capture_output=True, text=True, env=full, timeout=60)
    assert out.returncode == 0, out.stderr
    return out.stdout


def _old_commit(cwd: Path, name: str, days: float = 10) -> None:
    (cwd / f"{name}.txt").write_text(name)
    _git(cwd, "add", ".")
    when = f"{int(time.time() - days * DAY)} +0000"
    _git(cwd, "commit", "-q", "-m", name, env={"GIT_COMMITTER_DATE": when, "GIT_AUTHOR_DATE": when})


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    bare = tmp_path / "origin.git"
    _git(tmp_path, "init", "-q", "--bare", "-b", "main", str(bare))
    main = tmp_path / "main"
    _git(tmp_path, "clone", "-q", str(bare), str(main))
    _git(main, "checkout", "-q", "-b", "main")
    _old_commit(main, "base", 30)
    _git(main, "push", "-q", "origin", "main")
    (main / ".claude" / "worktrees").mkdir(parents=True)
    return main


def _worker(repo: Path, name: str, *, merged: bool, days: float = 10) -> Path:
    path = repo / ".claude" / "worktrees" / name
    _git(repo, "worktree", "add", "-q", "-b", name, str(path), "main")
    _old_commit(path, name, days)
    # the worktree's own HEAD file was written just now; a real idle worktree's is old
    _age_one(repo / ".git" / "worktrees" / name / "HEAD", days)
    if merged:
        _git(repo, "merge", "-q", "--no-ff", "-m", f"merge {name}", name)
        _git(repo, "push", "-q", "origin", "main")
    return path


def _wt_report(repo: Path, *, apply: bool, cwds: list[Path] | None = None) -> c.Report:
    report = c.Report(apply=apply)
    c.plan_worktrees(repo=repo, worktrees_dir=repo / ".claude" / "worktrees", now=time.time(),
                     max_idle_days=3, apply=apply, cwds=cwds or [], runner=c.run, report=report)
    return report


def _branches(repo: Path) -> list[str]:
    return [b.strip().lstrip("*+ ") for b in _git(repo, "branch").splitlines()]


def test_a_merged_clean_idle_worktree_and_its_branch_are_removed(repo: Path) -> None:
    wt = _worker(repo, "done-work", merged=True)
    report = _wt_report(repo, apply=True)
    assert not wt.exists() and "done-work" not in _branches(repo)
    assert [a.kind for a in report.actions] == ["worktree", "branch"]
    assert all(a.done for a in report.actions)
    assert repo.exists() and "main" in _branches(repo)  # the main checkout is never touched


def test_a_dry_run_removes_no_worktree_and_no_branch(repo: Path) -> None:
    wt = _worker(repo, "done-work", merged=True)
    report = _wt_report(repo, apply=False)
    assert wt.exists() and "done-work" in _branches(repo)
    assert [a.kind for a in report.actions] == ["worktree", "branch"]


def test_an_unmerged_worktree_is_left_alone(repo: Path) -> None:
    wt = _worker(repo, "unfinished", merged=False)
    report = _wt_report(repo, apply=True)
    assert wt.exists() and "unfinished" in _branches(repo)
    assert any("not merged" in s for s in report.skipped)


def test_a_worktree_with_real_uncommitted_work_is_left_alone(repo: Path) -> None:
    wt = _worker(repo, "wip", merged=True)
    (wt / "notes.txt").write_text("the only copy of something")
    (wt / "base.txt").write_text("edited")
    report = _wt_report(repo, apply=True)
    assert wt.exists() and (wt / "notes.txt").read_text() == "the only copy of something"
    assert any("uncommitted" in s for s in report.skipped)


def test_a_worktree_with_only_an_untracked_notes_file_is_left_alone(repo: Path) -> None:
    wt = _worker(repo, "notes-only", merged=True)
    (wt / "COMMIT_MSG.txt").write_text("draft")
    _wt_report(repo, apply=True)
    assert wt.exists()


def test_a_generated_virtualenv_is_not_work_and_goes_with_the_worktree(repo: Path) -> None:
    wt = _worker(repo, "with-venv", merged=True)
    (wt / ".venv" / "lib").mkdir(parents=True)
    (wt / ".venv" / "lib" / "big.so").write_bytes(b"0" * 100)
    (wt / ".mypy_cache").mkdir()
    report = _wt_report(repo, apply=True)
    assert not wt.exists() and all(a.done for a in report.actions)


def test_a_recently_active_worktree_is_left_alone(repo: Path) -> None:
    wt = _worker(repo, "fresh", merged=True, days=0.1)
    report = _wt_report(repo, apply=True)
    assert wt.exists() and any("active within" in s for s in report.skipped)


def test_a_worktree_with_a_process_inside_is_left_alone(repo: Path) -> None:
    wt = _worker(repo, "in-use", merged=True)
    report = _wt_report(repo, apply=True, cwds=[wt / "sub"])
    assert wt.exists() and any("working inside" in s for s in report.skipped)


def test_a_worktree_outside_the_workers_directory_is_never_considered(
    repo: Path, tmp_path: Path,
) -> None:
    other = tmp_path / "elsewhere-wt"
    _git(repo, "worktree", "add", "-q", "-b", "elsewhere", str(other), "main")
    _old_commit(other, "elsewhere", 20)
    _age_one(repo / ".git" / "worktrees" / "elsewhere-wt" / "HEAD", 20)
    _git(repo, "merge", "-q", "--no-ff", "-m", "merge elsewhere", "elsewhere")
    _git(repo, "push", "-q", "origin", "main")
    _wt_report(repo, apply=True)
    assert other.exists()


# --- docker --------------------------------------------------------------------------------

class _FakeDocker:
    def __init__(self, *, up: bool = True, dangling: int = 0, volumes: list[str] | None = None,
                 in_use: set[str] | None = None) -> None:
        self.up, self.dangling = up, dangling
        self.volumes, self.in_use = volumes or [], in_use or set()
        self.calls: list[list[str]] = []

    def __call__(self, cmd: list[str], cwd: Path | None) -> tuple[int, str]:
        self.calls.append(cmd)
        if cmd[:2] == ["docker", "version"]:
            return (0, "24.0") if self.up else (1, "cannot connect")
        if cmd[:3] == ["docker", "images", "-f"]:
            return 0, "\n".join(f"id{i}" for i in range(self.dangling))
        if cmd[:3] == ["docker", "image", "prune"]:
            return 0, "Total reclaimed space: 2.5GB"
        if cmd[:3] == ["docker", "builder", "prune"]:
            return 0, "Total reclaimed space: 1.5GB"
        if cmd[:3] == ["docker", "volume", "ls"]:
            return 0, "\n".join(self.volumes)
        if cmd[:2] == ["docker", "ps"]:
            vol = cmd[-1].split("=", 1)[1]
            return 0, "abc123" if vol in self.in_use else ""
        if cmd[:3] == ["docker", "volume", "rm"]:
            return 0, cmd[3]
        return 1, "unexpected"


def _docker_report(fake: _FakeDocker, *, apply: bool) -> c.Report:
    report = c.Report(apply=apply)
    c.plan_docker(apply=apply, build_cache_days=7, runner=fake, report=report)
    return report


ANON = "a" * 64
ANON2 = "b" * 64


def test_dangling_images_and_old_build_cache_are_pruned(tmp_path: Path) -> None:
    fake = _FakeDocker(dangling=3)
    report = _docker_report(fake, apply=True)
    kinds = {a.kind: a for a in report.actions}
    assert kinds["docker-image"].bytes == 2_500_000_000 and kinds["docker-image"].done
    assert kinds["docker-cache"].bytes == 1_500_000_000
    assert ["docker", "builder", "prune", "-f", "--filter", "until=168h"] in fake.calls


def test_only_anonymous_unused_volumes_are_removed(tmp_path: Path) -> None:
    fake = _FakeDocker(volumes=[ANON, ANON2, "osiris-pg-data", "osiris_pgdata"], in_use={ANON2})
    report = _docker_report(fake, apply=True)
    removed = [cmd[3] for cmd in fake.calls if cmd[:3] == ["docker", "volume", "rm"]]
    assert removed == [ANON]
    assert [a.target for a in report.actions if a.kind == "docker-volume"] == [ANON]


def test_the_dangerous_prunes_are_never_issued(tmp_path: Path) -> None:
    fake = _FakeDocker(dangling=1, volumes=[ANON, "osiris-pg-data"])
    _docker_report(fake, apply=True)
    for cmd in fake.calls:
        assert cmd[:3] != ["docker", "system", "prune"]
        assert cmd[:3] != ["docker", "volume", "prune"]
        assert not (cmd[:3] == ["docker", "volume", "rm"] and cmd[3] == "osiris-pg-data")
        assert "-a" not in cmd and "--all" not in cmd


def test_a_dry_run_issues_no_removal_command(tmp_path: Path) -> None:
    fake = _FakeDocker(dangling=2, volumes=[ANON])
    report = _docker_report(fake, apply=False)
    assert not any(cmd[:3] in (["docker", "image", "prune"], ["docker", "builder", "prune"],
                               ["docker", "volume", "rm"]) for cmd in fake.calls)
    assert {a.kind for a in report.actions} >= {"docker-image", "docker-volume"}


def test_docker_not_running_is_skipped_not_an_error(tmp_path: Path) -> None:
    report = _docker_report(_FakeDocker(up=False), apply=True)
    assert report.actions == [] and any("docker" in s for s in report.skipped)


# --- the whole thing -----------------------------------------------------------------------

def test_the_report_totals_and_renders(tmp_path: Path) -> None:
    root = _scratch(tmp_path)
    old = root / "pt-1"
    old.mkdir()
    (old / "f").write_bytes(b"x" * 2000)
    _age(old, 9)
    report = c.build_report(
        apply=False, scratch_root=root, test_tmp_parents=(), repo=tmp_path / "no-repo",
        skip_docker=True, cwds=[])
    text = c.render(report)
    assert "dry run, nothing deleted" in text and "would free: 2.0 kB" in text
    assert "scratch" in text


def test_main_prints_json_and_is_a_dry_run_by_default(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    assert c.main(["--json", "--skip-docker", "--scratch-days", "100000",
                   "--worktree-days", "100000"]) == 0
    out = capsys.readouterr().out
    assert '"apply": false' in out


# --- the units -----------------------------------------------------------------------------

def test_the_timer_and_service_ship_and_the_installer_knows_them() -> None:
    root = Path(__file__).resolve().parent.parent
    service = (root / "deploy" / "osiris-cleanup.service").read_text()
    timer = (root / "deploy" / "osiris-cleanup.timer").read_text()
    assert "scripts/osiris_cleanup.py --apply" in service
    assert "Nice=19" in service and "IOSchedulingClass=idle" in service
    assert "OnCalendar=" in timer and "Persistent=true" in timer
    installer = (root / "scripts" / "install_prune_timers.sh").read_text()
    assert "osiris-cleanup" in installer.split('UNITS="', 1)[1].split('"', 1)[0]
    assert "osiris-cleanup.timer" in installer.split("enable --now", 1)[1]
