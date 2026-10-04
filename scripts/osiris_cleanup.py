"""CLEAN-UP BY DEFAULT: the leftovers of working on this box (test and gate scratch, merged
worker worktrees and their branches, dangling Docker images, build cache and anonymous unused
volumes) used to pile up until the disk was full, because nothing ever removed them. This
removes them on a daily timer, by itself.

DRY-RUN IS THE DEFAULT: with no `--apply` it prints the plan and the bytes it would free and
deletes nothing. The timer runs it with `--apply`.

WHAT IT WILL NEVER TOUCH (each rule is enforced where the deletion happens, not by a list of
things it remembers to skip):
- Scratch: only entries whose newest file is older than the age limit AND where no running
  process has its working directory inside; only names this house creates (`pt-<pid>` test
  directories, and anything under /var/tmp/osiris-scratch); never a symlink target; never the
  scratch root itself; never a path that does not resolve inside its root.
- Worktrees: only worktrees under `.claude/worktrees/` (never the main checkout), whose branch
  is fully merged into origin/main, with a clean tree (no modified or untracked file), idle past
  the limit, with no process inside. The branch is deleted with `git branch -d`, which itself
  refuses an unmerged branch. A worktree git refuses to remove stays.
- Docker: only dangling images, build cache older than the limit, and ANONYMOUS volumes (a
  64-hex name) that no container, running or stopped, references. Never `docker system prune`,
  never `docker volume prune`, never a named volume: the database's own volume is named and
  belongs to a container, so it is outside every rule here by construction.

Everything the function layer does is injectable (clock, process table, git and docker
runners) so the whole plan is tested against a scratch directory, never a real disk."""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from pathlib import Path

SCRATCH_ROOT = Path("/var/tmp/osiris-scratch")
TEST_TMP_PARENTS = (Path("/var/tmp"), Path("/tmp"))
REPO = Path.home() / "code" / "osiris"
WORKTREES_DIR = REPO / ".claude" / "worktrees"
DAY = 86400.0
DEFAULT_SCRATCH_DAYS = 2.0
DEFAULT_WORKTREE_DAYS = 3.0
DEFAULT_BUILD_CACHE_DAYS = 7.0

# Untracked names a worktree grows by itself and that hold no work: the virtualenv uv makes
# for it and the tool caches. Any OTHER untracked or modified file counts as real work.
_GENERATED = frozenset({".venv", ".mypy_cache", ".pytest_cache", ".ruff_cache", "__pycache__"})
_PT_NAME = re.compile(r"^pt-\d+$")
_ANON_VOLUME = re.compile(r"^[0-9a-f]{64}$")


@dataclass
class Action:
    kind: str          # scratch | worktree | branch | docker-image | docker-cache | docker-volume
    target: str
    bytes: int = 0
    done: bool = False
    note: str = ""


@dataclass
class Report:
    apply: bool
    actions: list[Action] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)

    def freed(self) -> int:
        return sum(a.bytes for a in self.actions if a.done or not self.apply)


# --- scratch ------------------------------------------------------------------------------

def has_recent_file(path: Path, cutoff: float) -> bool:
    """True as soon as anything under `path` (or `path` itself) is newer than `cutoff`. Stops
    at the first hit, so a busy directory costs a handful of stats and only a fully old one is
    walked to the end. Symlinks are never followed."""
    try:
        if path.lstat().st_mtime >= cutoff:
            return True
    except OSError:
        return True  # cannot tell: treat as recent, never delete on a guess
    stack = [path]
    while stack:
        cur = stack.pop()
        try:
            with os.scandir(cur) as it:
                for e in it:
                    try:
                        st = e.stat(follow_symlinks=False)
                    except OSError:
                        continue
                    if st.st_mtime >= cutoff:
                        return True
                    if e.is_dir(follow_symlinks=False):
                        stack.append(Path(e.path))
        except OSError:
            return True
    return False


def tree_bytes(path: Path) -> int:
    """Total file bytes under `path`, symlinks never followed."""
    total = 0
    stack = [path]
    while stack:
        cur = stack.pop()
        try:
            with os.scandir(cur) as it:
                for e in it:
                    try:
                        st = e.stat(follow_symlinks=False)
                    except OSError:
                        continue
                    if e.is_dir(follow_symlinks=False):
                        stack.append(Path(e.path))
                    else:
                        total += st.st_size
        except OSError:
            continue
    return total


def live_working_dirs(proc: Path = Path("/proc")) -> list[Path]:
    """The working directory of every running process this user can see."""
    out: list[Path] = []
    try:
        pids = [p for p in proc.iterdir() if p.name.isdigit()]
    except OSError:
        return out
    for p in pids:
        try:
            out.append(Path(os.readlink(p / "cwd")))
        except OSError:
            continue
    return out


def _inside(path: Path, others: list[Path]) -> bool:
    return any(o == path or path in o.parents for o in others)


def _safe_remove(path: Path, root: Path) -> None:
    """Remove `path` only if it resolves inside `root`, is not `root` itself and is not a
    symlink (a link is unlinked, never followed)."""
    if path.is_symlink():
        path.unlink()
        return
    real = path.resolve()
    if real == root.resolve() or root.resolve() not in real.parents:
        raise ValueError(f"refusing to remove {path}: not inside {root}")
    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink()


def plan_scratch(
    roots: list[tuple[Path, bool]], *, now: float, max_age_days: float, apply: bool,
    cwds: list[Path], report: Report,
) -> None:
    """`roots`: (directory, only_pt_names). For the scratch root every entry is a candidate; for
    /var/tmp and /tmp only `pt-<pid>` test directories are (those parents hold other people's
    files too)."""
    limit = max_age_days * DAY
    for root, only_pt in roots:
        try:
            entries = sorted(root.iterdir())
        except OSError:
            continue
        for entry in entries:
            if only_pt and not _PT_NAME.match(entry.name):
                continue
            try:
                if entry.is_symlink():
                    continue
                if has_recent_file(entry, now - limit):
                    continue
                size = tree_bytes(entry) if entry.is_dir() else entry.lstat().st_size
            except OSError:
                continue
            if _inside(entry, cwds):
                report.skipped.append(f"{entry}: a running process is working inside it")
                continue
            act = Action("scratch", str(entry), size)
            if apply:
                try:
                    _safe_remove(entry, root)
                    act.done = True
                except (OSError, ValueError) as exc:
                    act.note = f"{type(exc).__name__}: {exc}"
            report.actions.append(act)


# --- worktrees ----------------------------------------------------------------------------

Runner = Callable[[list[str], Path | None], tuple[int, str]]


def run(cmd: list[str], cwd: Path | None = None, timeout: float = 600.0) -> tuple[int, str]:
    try:
        p = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=timeout)
    except (OSError, subprocess.SubprocessError) as exc:
        return 1, f"{type(exc).__name__}: {exc}"
    return p.returncode, (p.stdout + p.stderr)


def _last_line(out: str) -> str:
    lines = out.strip().splitlines()
    return lines[-1] if lines else ""


def _generated_untracked(status: str) -> list[str] | None:
    """The generated entries `git status --porcelain` lists as untracked, or None when
    anything else is listed (a modified file, a staged change, an untracked file that may be
    work). An empty list means a clean tree."""
    names: list[str] = []
    for line in status.splitlines():
        if not line.strip():
            continue
        if not line.startswith("?? "):
            return None
        name = line[3:].strip().strip('"').rstrip("/")
        if name not in _GENERATED:
            return None
        names.append(name)
    return names


def list_worktrees(repo: Path, runner: Runner) -> list[dict[str, str]]:
    code, out = runner(["git", "worktree", "list", "--porcelain"], repo)
    if code != 0:
        return []
    trees: list[dict[str, str]] = []
    cur: dict[str, str] = {}
    for line in out.splitlines() + [""]:
        if not line:
            if cur:
                trees.append(cur)
            cur = {}
            continue
        key, _, value = line.partition(" ")
        cur[key] = value
    return trees


def worktree_idle_since(path: Path, runner: Runner, repo: Path) -> float:
    """Newest sign of activity: the date of the worktree's last commit, or when its HEAD
    last moved (a checkout or rebase), whichever is later. NOT its index (`git status`, which
    this very script runs, refreshes it) and NOT its reflog (a repository-wide housekeeping
    pass rewrites every reflog at once, measured here: all of them on one minute). A worktree
    with uncommitted work never reaches this check (it is not clean)."""
    newest = 0.0
    code, out = runner(["git", "log", "-1", "--format=%ct", "HEAD"], path)
    if code == 0 and out.strip().isdigit():
        newest = float(out.strip())
    code, out = runner(["git", "rev-parse", "--git-dir"], path)
    if code == 0:
        gitdir = Path(out.strip())
        if not gitdir.is_absolute():
            gitdir = path / gitdir
        try:
            newest = max(newest, (gitdir / "HEAD").stat().st_mtime)
        except OSError:
            pass
    return newest


def plan_worktrees(
    *, repo: Path, worktrees_dir: Path, now: float, max_idle_days: float, apply: bool,
    cwds: list[Path], runner: Runner, report: Report,
) -> None:
    code, _ = runner(["git", "fetch", "-q", "origin"], repo)  # best effort: a stale view only
    del code                                                   # ever keeps more, never less
    protected = {"main", "master"}
    for tree in list_worktrees(repo, runner):
        path = Path(tree.get("worktree", ""))
        branch_ref = tree.get("branch", "")
        if not path.is_dir() or worktrees_dir.resolve() not in path.resolve().parents:
            continue
        if tree.get("locked") is not None or tree.get("prunable") is not None:
            continue
        branch = branch_ref.removeprefix("refs/heads/") if branch_ref else ""
        head = tree.get("HEAD", "")
        if branch in protected:
            continue
        merged_code, _ = runner(
            ["git", "merge-base", "--is-ancestor", branch_ref or head, "origin/main"], repo)
        if merged_code != 0:
            report.skipped.append(f"{path}: not merged into origin/main")
            continue
        code, status = runner(["git", "status", "--porcelain"], path)
        generated = _generated_untracked(status) if code == 0 else None
        if generated is None:
            report.skipped.append(f"{path}: has uncommitted or untracked files")
            continue
        if now - worktree_idle_since(path, runner, repo) < max_idle_days * DAY:
            report.skipped.append(f"{path}: active within {max_idle_days:g} days")
            continue
        if _inside(path, cwds):
            report.skipped.append(f"{path}: a running process is working inside it")
            continue
        size = tree_bytes(path)
        act = Action("worktree", str(path), size)
        if apply:
            for name in generated:
                _safe_remove(path / name, path)
            code, out = runner(["git", "worktree", "remove", str(path)], repo)
            act.done = code == 0
            act.note = "" if code == 0 else _last_line(out)
        report.actions.append(act)
        if branch and (not apply or act.done):
            bact = Action("branch", branch, 0)
            if apply:
                code, out = runner(["git", "branch", "-d", branch], repo)
                bact.done = code == 0
                bact.note = "" if code == 0 else _last_line(out)
            report.actions.append(bact)
    if apply:
        runner(["git", "worktree", "prune"], repo)


# --- docker -------------------------------------------------------------------------------

def _docker_bytes(out: str) -> int:
    m = re.search(r"Total reclaimed space:\s*([\d.]+)\s*([kMGT]?B)", out)
    if not m:
        return 0
    scale = {"B": 1, "kB": 1e3, "MB": 1e6, "GB": 1e9, "TB": 1e12}[m.group(2)]
    return int(float(m.group(1)) * scale)


def plan_docker(
    *, apply: bool, build_cache_days: float, runner: Runner, report: Report,
) -> None:
    code, _ = runner(["docker", "version", "--format", "{{.Server.Version}}"], None)
    if code != 0:
        report.skipped.append("docker: not reachable, skipped")
        return
    # dangling images (untagged, referenced by nothing)
    _, listing = runner(["docker", "images", "-f", "dangling=true", "-q"], None)
    n_images = len([x for x in listing.split() if x])
    if n_images:
        act = Action("docker-image", f"{n_images} dangling image(s)")
        if apply:
            code, out = runner(["docker", "image", "prune", "-f"], None)
            act.done, act.bytes = code == 0, _docker_bytes(out)
        report.actions.append(act)
    # build cache older than the limit
    hours = int(build_cache_days * 24)
    if apply:
        code, out = runner(
            ["docker", "builder", "prune", "-f", "--filter", f"until={hours}h"], None)
        reclaimed = _docker_bytes(out)
        if code == 0 and reclaimed:
            report.actions.append(Action(
                "docker-cache", f"build cache older than {build_cache_days:g} days",
                reclaimed, True))
    else:
        report.actions.append(Action(
            "docker-cache", f"build cache older than {build_cache_days:g} days (size known "
            "only when applied)"))
    # anonymous volumes nothing references
    _, vols = runner(["docker", "volume", "ls", "-q", "-f", "dangling=true"], None)
    for name in vols.split():
        if not _ANON_VOLUME.match(name):
            continue  # a NAMED volume is never touched, whoever it belongs to
        _, users = runner(["docker", "ps", "-aq", "--filter", f"volume={name}"], None)
        if users.strip():
            continue  # a container (even a stopped one) still references it
        act = Action("docker-volume", name)
        if apply:
            code, _out = runner(["docker", "volume", "rm", name], None)
            act.done = code == 0
        report.actions.append(act)


# --- the whole plan -----------------------------------------------------------------------

def build_report(
    *, apply: bool, scratch_days: float = DEFAULT_SCRATCH_DAYS,
    worktree_days: float = DEFAULT_WORKTREE_DAYS,
    build_cache_days: float = DEFAULT_BUILD_CACHE_DAYS, skip_docker: bool = False,
    now: float | None = None, scratch_root: Path = SCRATCH_ROOT,
    test_tmp_parents: tuple[Path, ...] = TEST_TMP_PARENTS, repo: Path = REPO,
    worktrees_dir: Path = WORKTREES_DIR, cwds: list[Path] | None = None,
    runner: Runner = run,
) -> Report:
    report = Report(apply=apply)
    now = time.time() if now is None else now
    cwds = live_working_dirs() if cwds is None else cwds
    roots = [(scratch_root, False)] + [(p, True) for p in test_tmp_parents]
    plan_scratch(roots, now=now, max_age_days=scratch_days, apply=apply, cwds=cwds,
                 report=report)
    if repo.is_dir():
        plan_worktrees(repo=repo, worktrees_dir=worktrees_dir, now=now,
                       max_idle_days=worktree_days, apply=apply, cwds=cwds,
                       runner=runner, report=report)
    if not skip_docker:
        plan_docker(apply=apply, build_cache_days=build_cache_days, runner=runner,
                    report=report)
    return report


def _human(n: int) -> str:
    size = float(n)
    for unit in ("B", "kB", "MB", "GB", "TB"):
        if size < 1000 or unit == "TB":
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1000
    return f"{n} B"


def render(report: Report) -> str:
    verb = "freed" if report.apply else "would free"
    lines = [f"clean-up ({'applied' if report.apply else 'dry run, nothing deleted'}):"]
    kinds: dict[str, tuple[int, int]] = {}
    for a in report.actions:
        n, b = kinds.get(a.kind, (0, 0))
        kinds[a.kind] = (n + 1, b + a.bytes)
    for kind, (n, b) in sorted(kinds.items()):
        lines.append(f"  {kind:14s} {n:5d} item(s)  {_human(b)}")
    failed = [a for a in report.actions if report.apply and not a.done]
    for a in failed:
        lines.append(f"  could not remove {a.kind} {a.target}: {a.note or 'unknown'}")
    lines.append(f"  {verb}: {_human(report.freed())}; left alone: {len(report.skipped)}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--apply", action="store_true",
                    help="delete (the default is a dry run that deletes nothing)")
    ap.add_argument("--json", action="store_true", help="print the plan as JSON")
    ap.add_argument("--scratch-days", type=float, default=DEFAULT_SCRATCH_DAYS)
    ap.add_argument("--worktree-days", type=float, default=DEFAULT_WORKTREE_DAYS)
    ap.add_argument("--build-cache-days", type=float, default=DEFAULT_BUILD_CACHE_DAYS)
    ap.add_argument("--skip-docker", action="store_true")
    args = ap.parse_args(argv)
    report = build_report(
        apply=args.apply, scratch_days=args.scratch_days, worktree_days=args.worktree_days,
        build_cache_days=args.build_cache_days, skip_docker=args.skip_docker)
    if args.json:
        print(json.dumps({"apply": report.apply, "freed_bytes": report.freed(),
                          "actions": [asdict(a) for a in report.actions],
                          "skipped": report.skipped}, indent=2))
    else:
        print(render(report))
    return 0


if __name__ == "__main__":
    sys.exit(main())
