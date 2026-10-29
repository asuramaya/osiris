"""Which seat workspaces the harness does not trust yet.

`claude --bg` refuses a directory the user has not trusted ("Workspace not trusted. Run
`claude` in <dir> once and accept the trust prompt, then retry.") and a seat launched
there never exists. The harness keeps that choice in `~/.claude.json` under
`projects[<path>].hasTrustDialogAccepted`. Measured against the installed harness: a
directory is trusted when it, or ANY ancestor directory, has `hasTrustDialogAccepted:
true`, even when its own entry says false or is missing (a fresh directory under a
trusted parent spawns fine; one under none of them is refused).

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


def is_trusted(path: str, projects: dict[str, Any]) -> bool:
    """True when `path` or any ancestor has `hasTrustDialogAccepted: true`."""
    here = Path(path)
    for candidate in (here, *here.parents):
        entry = projects.get(str(candidate))
        if isinstance(entry, dict) and entry.get("hasTrustDialogAccepted") is True:
            return True
    return False


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
