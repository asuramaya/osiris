"""LINEAGE MEMORY CUSTODY (thread 4dcc1849, operator ruling 3b52e9d6 as confirmed
standing by dda9f248, design decision f9e47d3c) — pure filesystem logic, no DB.

THE PROBLEM: Claude Code's own harness-native auto-memory system keys its storage path
purely by session cwd (`~/.claude/projects/<slug(cwd)>/memory/`). A seat with no
worktree has a STABLE cwd (its own anchor_cwd) across every lineage that ever holds
it — so the harness's own memory directory is ALREADY seat-wide today, collapsing every
generation's personal memory into one shared store. The operator's ruling wants memory
"attributable" to the lineage that wrote it, never silently inherited by whichever mind
holds the seat next.

REJECTED: symlinking the harness's own memory/ path at SessionStart-hook time to a
canonical per-lineage location. Whether that hook runs before or after the harness's own
memory-directory creation/load point is unknowable from this codebase (proprietary
harness-internal timing) — not something to build on a guess, especially since the hook
is shared, fleet-wide, boot-critical infrastructure.

CHOSEN INSTEAD: a `.osiris-lineage` sentinel file inside the harness-native memory/ dir,
naming which lineage currently owns it. Checked and (if needed) archived from mount() —
an ordinary MCP call, always the first of any session, fully within osiris's own timing
control, no hook dependency. A collision (a DIFFERENT lineage's sentinel already there)
archives the whole directory SIDEWAYS (a plain rename, never a delete — constitution #3,
never DELETE, heal with compensating events) and lets the harness create a fresh one
naturally. A directory with NO sentinel at all (pre-existing content predating this
system) is never auto-archived — silently moving real memory nobody has ruled on is
exactly the "loop closes silently" failure the membrane law (constitution #6) forbids;
it is flagged as `migration_needed` instead, for a human to resolve by hand.
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
    '-' — confirmed against three live specimens (a plain repo, a worktree, and a seat
    office's dotted ~/.osiris path), not merely assumed; see
    tests/test_lineage_memory.py's own specimens."""
    return cwd.replace("/", "-").replace(".", "-")


def claude_memory_dir(cwd: str, *, home: Path | None = None) -> Path:
    """Where Claude Code's own harness-native auto-memory system stores this cwd's
    MEMORY.md + topic files. `home` is injectable only for tests — real callers always
    use the actual home directory."""
    home = home if home is not None else Path.home()
    return home / ".claude" / "projects" / encode_claude_project_slug(cwd) / "memory"


@dataclass
class MemoryCustodyResult:
    """`action`: 'archived' (a different lineage's memory was just moved sideways —
    `path`/`prior_lineage` are set), 'migration_needed' (pre-existing content with no
    sentinel at all — `path` is set, nothing was touched), or 'noop' (nothing to do:
    either this lineage already owns the sentinel, the directory doesn't exist yet, or
    a filesystem error made this best-effort check give up)."""
    action: str
    path: str | None = None
    prior_lineage: str | None = None


def ensure_lineage_memory_custody(
    cwd: str, lineage_root: str, *, home: Path | None = None,
) -> MemoryCustodyResult:
    """Best-effort, filesystem-only: never raises (any OSError degrades to 'noop', since
    this plumbing must never be able to block a mount). Does NOT write the sentinel
    itself — call `stamp_lineage_sentinel` after, and only for 'archived'/'noop' results,
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
    """Write/refresh the sentinel naming this lineage as the memory dir's current
    owner — creates the directory if it doesn't exist yet (a fresh cwd, nothing to
    protect against). Best-effort: never raises."""
    try:
        mem_dir = claude_memory_dir(cwd, home=home)
        mem_dir.mkdir(parents=True, exist_ok=True)
        (mem_dir / _SENTINEL_NAME).write_text(lineage_root, encoding="utf-8")
    except OSError:
        pass


# --- ONE-TIME FLEET SEED (Thoth's own condition on the design, DM 7763 item (a)) ---------
#
# Without this, every currently-active lineage's own memory dir has no sentinel yet the
# instant this ships, so its own very next mount() would report `memory_migration_needed`
# — safe (nothing gets wrongly archived; archiving only fires on a NAMED-DIFFERENT
# sentinel, and none exist anywhere yet), but a fleet-wide false-alarm flood the moment
# every live session reconnects. Seeding attributes today's real content to its true
# current holder NOW, once, deliberately — dry-run by default (`apply_lineage_memory_seed`
# is a separate, explicit second call), same two-step shape every other backfill script
# in this repo already uses.

async def plan_lineage_memory_seed(
    pool: asyncpg.Pool, *, home: Path | None = None,
) -> list[dict[str, Any]]:
    """Every active seat's CURRENT HOLDER, checked against each of its distinct real cwd
    candidates (anchor_cwd/tree_cwd/live_cwd — a live holder's mount cwd can differ from
    both with nothing wrong, roster()'s own law) for a Claude-memory dir that exists but
    carries no sentinel yet. Read-only: plans, never writes. A vacant/cold seat is
    skipped — nothing live to attribute a fresh sentinel to; its own memory (if any)
    surfaces as `migration_needed` honestly on whoever mounts there next, same as any
    other pre-existing, unattributed content."""
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
    nothing from the plan beyond cwd/lineage_root — idempotent, safe to re-run (a dir
    already sentineled by the time this runs is a no-op via `stamp_lineage_sentinel`
    itself, which only ever writes/refreshes, never checks-then-skips)."""
    for item in plan:
        stamp_lineage_sentinel(item["cwd"], item["lineage_root"], home=home)
