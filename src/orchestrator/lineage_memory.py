"""Lineage memory custody: pure filesystem logic, no database access.

THE PROBLEM: Claude Code's own harness-native auto-memory system keys its storage path
purely by session cwd (`~/.claude/projects/<slug(cwd)>/memory/`). A seat with no
worktree has a STABLE cwd (its own anchor_cwd) across every lineage that ever holds it,
so the harness's own memory directory is already seat-wide today, collapsing every
generation's personal memory into one shared store. The governing ruling requires that
memory be attributable to the lineage that wrote it, never silently inherited by
whichever agent holds the seat next.

REJECTED APPROACH: symlinking the harness's own memory/ directory at SessionStart-hook
time to a canonical per-lineage location. Whether that hook runs before or after the
harness's own memory-directory creation/load point is unknowable from this codebase
(the harness's internal timing is proprietary), so this is not something to build on a
guess, especially since the hook is shared, fleet-wide, boot-critical infrastructure.

CHOSEN INSTEAD: a `.osiris-lineage` sentinel file inside the harness-native memory/
directory, naming which lineage currently owns it. It is checked and, if needed,
archived from mount(), an ordinary MCP call that always runs first in any session,
fully within this system's own timing control, with no hook dependency. A collision (a
different lineage's sentinel already present) archives the whole directory sideways (a
plain rename, never a delete, per constitution #3: never delete, heal with compensating
events) and lets the harness create a fresh one naturally. A directory with no sentinel
at all (pre-existing content that predates this system) is never auto-archived: silently
moving real memory that nobody has reviewed is exactly the kind of silent, unreviewed
state change that constitution #6 forbids. Such a directory is flagged as
`migration_needed` instead, for a human to resolve by hand.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncpg

_SENTINEL_NAME = ".osiris-lineage"


def encode_claude_project_slug(cwd: str) -> str:
    """The forward direction of claude_jsonl.py's `decode_claude_project_name`: Claude
    Code's own project slug for a cwd is that path with every '/' and '.' replaced by
    '-'. Confirmed against three live examples (a plain repo, a worktree, and a seat
    office's dotted ~/.osiris path), not merely assumed; see
    tests/test_lineage_memory.py's own examples."""
    return cwd.replace("/", "-").replace(".", "-")


def claude_memory_dir(cwd: str, *, home: Path | None = None) -> Path:
    """Where Claude Code's own harness-native auto-memory system stores this cwd's
    MEMORY.md + topic files. `home` is injectable only for tests; real callers always
    use the actual home directory."""
    home = home if home is not None else Path.home()
    return home / ".claude" / "projects" / encode_claude_project_slug(cwd) / "memory"


@dataclass
class MemoryCustodyResult:
    """`action`: 'archived' (a different lineage's memory was just moved sideways;
    `path`/`prior_lineage` are set), 'migration_needed' (pre-existing content with no
    sentinel at all; `path` is set, nothing was touched), or 'noop' (nothing to do:
    either this lineage already owns the sentinel, the directory doesn't exist yet, or
    a filesystem error made this best-effort check give up)."""
    action: str
    path: str | None = None
    prior_lineage: str | None = None


def peek_lineage_memory_owner(cwd: str, *, home: Path | None = None) -> str | None:
    """Read-only: returns the sentinel's current named owner, or None (no sentinel, no
    directory, or a filesystem error, degrading exactly like
    `ensure_lineage_memory_custody`'s own fail-open behavior). Never touches anything; a
    caller uses this to decide whether to call that function at all, for example an
    unrecognized caller checking whose memory it is about to evict before it evicts
    it."""
    try:
        sentinel = claude_memory_dir(cwd, home=home) / _SENTINEL_NAME
        if not sentinel.is_file():
            return None
        return sentinel.read_text(encoding="utf-8").strip() or None
    except OSError:
        return None


def ensure_lineage_memory_custody(
    cwd: str, lineage_root: str, *, home: Path | None = None,
) -> MemoryCustodyResult:
    """Best-effort, filesystem-only: never raises (any OSError degrades to 'noop', since
    this plumbing must never be able to block a mount). Does NOT write the sentinel
    itself: call `stamp_lineage_sentinel` after, and only for 'archived'/'noop' results,
    never for 'migration_needed' (that directory's content is not this system's to
    touch until a human resolves it)."""
    try:
        mem_dir = claude_memory_dir(cwd, home=home)
        if not mem_dir.is_dir():
            return MemoryCustodyResult(action="noop")
        sentinel = mem_dir / _SENTINEL_NAME
        if not sentinel.is_file():
            return MemoryCustodyResult(action="migration_needed", path=str(mem_dir))
        owner = sentinel.read_text(encoding="utf-8").strip()
        if owner == lineage_root:
            return MemoryCustodyResult(action="noop")
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        archived = mem_dir.parent / (
            f"memory.archived-{owner.replace(':', '_')}-{stamp}")
        mem_dir.rename(archived)
        return MemoryCustodyResult(
            action="archived", path=str(archived), prior_lineage=owner)
    except OSError:
        return MemoryCustodyResult(action="noop")


def stamp_lineage_sentinel(cwd: str, lineage_root: str, *, home: Path | None = None) -> None:
    """Write/refresh the sentinel naming this lineage as the memory dir's current owner.
    Creates the directory if it doesn't exist yet (a fresh cwd, nothing to protect
    against). Best-effort: never raises."""
    try:
        mem_dir = claude_memory_dir(cwd, home=home)
        mem_dir.mkdir(parents=True, exist_ok=True)
        (mem_dir / _SENTINEL_NAME).write_text(lineage_root, encoding="utf-8")
    except OSError:
        pass


# --- ONE-TIME FLEET SEED -----------------------------------------------------------
#
# Without this, every currently-active lineage's own memory directory has no sentinel
# the instant this ships, so its very next mount() call would report
# `memory_migration_needed`. This is safe (nothing gets wrongly archived; archiving
# only fires on a named, different sentinel, and none exist anywhere yet), but it would
# cause a fleet-wide false-alarm flood the moment every live session reconnects.
# Seeding attributes today's real content to its true current holder now, once,
# deliberately: dry-run by default (`apply_lineage_memory_seed` is a separate, explicit
# second call), the same two-step shape every other backfill script in this repo
# already uses.

async def plan_lineage_memory_seed(
    pool: asyncpg.Pool, *, home: Path | None = None,
) -> list[dict[str, Any]]:
    """Checks every active seat's current holder against each of its distinct real cwd
    candidates (anchor_cwd/tree_cwd/live_cwd; a live holder's mount cwd can differ from
    both with nothing wrong, which is expected behavior documented by roster() itself)
    for a Claude-memory directory that exists but carries no sentinel yet. Read-only:
    plans, never writes. A vacant/cold seat is skipped, since there is nothing live to
    attribute a fresh sentinel to; its own memory (if any) surfaces as
    `migration_needed` honestly on whoever mounts there next, the same as any other
    pre-existing, unattributed content."""
    from src.orchestrator.agents import _generation
    from src.orchestrator.seats import roster

    data = await roster(pool)
    plan: list[dict[str, Any]] = []
    seen_dirs: set[str] = set()
    for row in data["seats"]:
        holder = row.get("holder")
        if row.get("occupancy") != "occupied" or not holder:
            continue
        lineage_root = _generation(holder)[0]
        candidates = {row.get("anchor_cwd"), row.get("tree_cwd"), row.get("live_cwd")}
        for cwd in filter(None, candidates):
            mem_dir = claude_memory_dir(cwd, home=home)
            key = str(mem_dir)
            if key in seen_dirs:
                continue
            seen_dirs.add(key)
            if mem_dir.is_dir() and not (mem_dir / _SENTINEL_NAME).is_file():
                plan.append({"seat": row["seat"], "handle": row["handle"], "cwd": cwd,
                            "memory_dir": key, "lineage_root": lineage_root})
    return plan


def apply_lineage_memory_seed(
    plan: list[dict[str, Any]], *, home: Path | None = None,
) -> None:
    """Write the sentinels a prior `plan_lineage_memory_seed` call planned. Re-derives
    nothing from the plan beyond cwd/lineage_root. Idempotent, safe to re-run (a
    directory already sentineled by the time this runs is a no-op via
    `stamp_lineage_sentinel` itself, which only ever writes/refreshes, never
    checks-then-skips)."""
    for item in plan:
        stamp_lineage_sentinel(item["cwd"], item["lineage_root"], home=home)
