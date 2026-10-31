"""Which seat workspaces the harness does not trust yet.

`claude --bg` refuses a directory the user has not trusted ("Workspace not trusted. Run
`claude` in <dir> once and accept the trust prompt, then retry.") and a seat launched
there never exists. The harness keeps that choice in `~/.claude.json` under
`projects[<path>].hasTrustDialogAccepted`.

The rule, measured on the installed harness with fresh directories under a trusted
`/home/asuramaya/code` (entries read before and after; the harness wrote none):

  * a directory that is NOT inside a git repository is trusted when it or ANY ancestor has
    `hasTrustDialogAccepted: true` (plain dir, dir with `.claude/settings.local.json`, dir
    with a `.osiris` pin: all accepted);
  * a directory inside a git repository is judged by the REPOSITORY, not its ancestors: a
    fresh `git init` directory under the same trusted parent was REFUSED, and so was the same
    plus `.claude/worktrees` and a pin. A repository is trusted by its own entry (or the
    directory's own); a linked worktree (`.git` is a file) is trusted through its main
    repository's entry, so a worktree with no entry of its own is accepted when the main
    repo is trusted, as is any subdirectory of a trusted worktree.

An earlier version of this module applied the first rule to every directory, so it called
`/home/asuramaya/code/handlingtheloop` (a git repository with no entry of its own) trusted
while the harness refused it.

This module only reads. Trust is the operator's to grant, by running `claude` once in the
directory and accepting the prompt; osiris never writes it.
"""
from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import asyncpg


def load_projects(home: Path | None = None) -> dict[str, Any] | None:
    """The `projects` table of `~/.claude.json`, or None when it cannot be read: an
    unreadable config is "unknown", never an accusation that every directory is untrusted."""
    try:
        data = json.loads(((home or Path.home()) / ".claude.json").read_text())
    except (OSError, ValueError):
        return None
    projects = data.get("projects") if isinstance(data, dict) else None
    return projects if isinstance(projects, dict) else None


def _git_roots(path: Path) -> list[Path] | None:
    """The git repository roots that govern `path`, or None when it is not inside one: the
    nearest directory holding `.git`, and, when that `.git` is a FILE (a linked worktree),
    the main repository's root as well. An unreadable worktree pointer counts as the
    worktree root alone."""
    for cand in (path, *path.parents):
        marker = cand / ".git"
        if marker.is_dir():
            return [cand]
        if marker.is_file():
            roots = [cand]
            try:
                text = marker.read_text()
                gitdir = Path(text.split("gitdir:", 1)[1].strip().splitlines()[0])
                common = (gitdir / (gitdir / "commondir").read_text().strip()).resolve()
                if common.name == ".git":
                    roots.append(common.parent)
            except (OSError, IndexError, ValueError):
                pass
            return roots
    return None


def is_trusted(path: str, projects: dict[str, Any]) -> bool:
    """Whether the harness accepts `path` (see the module docstring for the measured rule):
    inside a git repository, only the repository's own root (or the path itself) counts;
    outside one, the path or any ancestor."""

    def accepted(candidate: Path) -> bool:
        entry = projects.get(str(candidate))
        return isinstance(entry, dict) and entry.get("hasTrustDialogAccepted") is True

    here = Path(path)
    roots = _git_roots(here)
    if roots is not None:
        return accepted(here) or any(accepted(r) for r in roots)
    return any(accepted(c) for c in (here, *here.parents))


def untrusted_dirs(paths: Sequence[str | None], home: Path | None = None) -> list[str]:
    """The directories among `paths` that exist but are not trusted, each once, in order.
    Empty when the config cannot be read. A directory that does not exist is a different
    problem with its own message and is skipped here."""
    projects = load_projects(home)
    if projects is None:
        return []
    out: list[str] = []
    for p in paths:
        if p and p not in out and Path(p).is_dir() and not is_trusted(p, projects):
            out.append(p)
    return out


def hint(directory: str) -> str:
    return f"workspace not trusted: run claude once in {directory}"


async def governing_seat_dirs(pool: asyncpg.Pool, project_oid: Any) -> list[str]:
    """The office and code tree of every active seat that governs this project."""
    from src.orchestrator.seats import seat_facts

    rows = await pool.fetch(
        "SELECT s.canonical FROM links l JOIN objects s ON s.id=l.from_id "
        "WHERE l.to_id=$1 AND l.type='governs' AND s.type='Seat' AND s.status='active' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", project_oid)
    dirs: list[str] = []
    for r in rows:
        facts = await seat_facts(pool, r["canonical"])
        dirs += [str(d) for d in (facts.get("tree_cwd"), facts.get("anchor_cwd")) if d]
    return dirs
