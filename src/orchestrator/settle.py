"""THE OFFLOAD RITUAL'S BOXES, promoted (ruling c5b184cd, the /settle primitive; queue item
4 / #49 piece 3 originally built these inside scripts/osiris_stophook.py). Two callers now
need the SAME check — the Stop hook (deciding whether to refuse a quiet stop) and the new
/settle MCP tool (surfacing what's still unwritten, then confirming after the agent's own
dump) — and "two copies drifting" is precisely the bug class this house keeps finding
(manager_of_seat's own promotion out of a hook-local copy is the direct precedent). Kept
deliberately thin: this module answers "what's checked and what's still missing", never
"should a stop be refused" — that policy (occupancy %, once-then-allow) stays the hook's own,
since it is about WHEN to enforce, not WHAT to check.

`conn_or_pool` accepts either an asyncpg.Pool (the MCP server) or a raw asyncpg.Connection
(the hook script, which cannot hold a pool across its ~1s budget) — both implement the same
fetch/fetchval surface, so one implementation genuinely serves both callers.
"""
from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any, Protocol


class _Fetchable(Protocol):
    async def fetchval(self, query: str, *args: Any) -> Any: ...


async def settle_boxes(
    conn_or_pool: _Fetchable, *, agent_id: str, mounted_at: datetime, cwd: str | None,
) -> dict[str, bool | None]:
    """The boxes this session's own life should have checked, best-effort per box — a box
    this query could not evaluate is None and never counts as missing (fail open per-box,
    the same law the whole ritual runs on).

    Decisions/threads are 'this session's' when their defining assertion's source_id is this
    EXACT agent_id (the identity that mounted this job_dir) at or after `mounted_at` — the
    session's own first-mount stamp, set once at INSERT, never touched by a re-attach."""
    boxes: dict[str, bool | None] = {}
    try:
        boxes["decisions recorded this session"] = bool(await conn_or_pool.fetchval(
            "SELECT 1 FROM assertions a JOIN objects o ON o.id = a.object_id "
            "WHERE o.type = 'Decision' AND a.name = 'summary' AND a.source_id = $1 "
            "AND a.observed_at >= $2 LIMIT 1", agent_id, mounted_at))
    except Exception:  # noqa: BLE001 — one box's failure never dooms the others
        boxes["decisions recorded this session"] = None
    try:
        boxes["threads trued this session (opened or resolved)"] = bool(await conn_or_pool.fetchval(
            "SELECT 1 FROM assertions a JOIN objects o ON o.id = a.object_id "
            "WHERE o.type = 'Thread' AND a.name IN ('summary', 'status') "
            "AND a.source_id = $1 AND a.observed_at >= $2 LIMIT 1", agent_id, mounted_at))
    except Exception:  # noqa: BLE001
        boxes["threads trued this session (opened or resolved)"] = None
    boxes["charter.md touched this session"] = charter_touched(cwd, mounted_at)
    # a live succession/handoff note — ONLY asked of a session whose own agent object was
    # itself born by a mint (minted_because stamped at birth, permanent on that exact
    # generation): a fresh heir owes its OWN heir at least one obligation left behind, not
    # just mail settled. No content-classifier — the primitive is 'opened an obligation',
    # not 'looks like a handoff'.
    try:
        minted = bool(await conn_or_pool.fetchval(
            "SELECT 1 FROM current_assertions a JOIN objects o ON o.id = a.object_id "
            "WHERE o.canonical = $1 AND a.name = 'minted_because' LIMIT 1", agent_id))
    except Exception:  # noqa: BLE001
        minted = False
    if minted:
        try:
            boxes["a live succession/handoff note (this lineage was minted)"] = bool(
                await conn_or_pool.fetchval(
                    "SELECT 1 FROM assertions a JOIN objects o ON o.id = a.object_id "
                    "WHERE o.type = 'Thread' AND a.name = 'kind' "
                    "AND a.value #>> '{}' = 'obligation' AND a.source_id = $1 "
                    "AND a.observed_at >= $2 LIMIT 1", agent_id, mounted_at))
        except Exception:  # noqa: BLE001
            boxes["a live succession/handoff note (this lineage was minted)"] = None
    return boxes


def charter_touched(cwd: str | None, mounted_at: datetime) -> bool | None:
    """None (can't evaluate, fails open) when this cwd has no charter.md at all — a session
    working in an ordinary repo, not an office, is never punished for a file that was never
    scaffolded here. Present: mtime at or after session start."""
    if not cwd:
        return None
    from pathlib import Path
    try:
        charter = Path(cwd) / "charter.md"
        if not charter.exists():
            return None
        return charter.stat().st_mtime >= mounted_at.timestamp()
    except OSError:
        return None


def missing_boxes(boxes: dict[str, bool | None]) -> list[str]:
    """The labels of any box explicitly False — never None (fog-of-war, could not be
    evaluated) or True (satisfied). Pure; the one line both the hook's verdict and
    /settle's own confirm step share instead of each re-deriving it."""
    return [label for label, ok in boxes.items() if ok is False]


async def uncommitted_git_work(cwd: str | None, *, timeout_s: float = 2.0) -> list[str] | None:
    """THE ONE BOX NOT IN THE GRAPH (operator, 2026-07-26, watching a live compaction: an
    agent asked "safe to compact?" had to run `git status` BY HAND before answering —
    mechanical, and a wasted expensive turn). /settle-only: unlike settle_boxes above, this
    is NOT shared with the Stop hook, which runs on a strict ~1s budget the hook's own
    docstring is explicit about — a subprocess against a large working tree has no place
    racing that clock for a question the hook never asks.

    None (fails open) when cwd is empty, not inside a git worktree, git itself errors, or
    the check outruns `timeout` — the common, INNOCENT case for a seat-office agent, whose
    cwd is the office, not the repo it governs (CLAUDE.md: 'work on it with absolute
    paths'). A [] means clean; a non-empty list is `git status --porcelain` lines verbatim
    (' M path', '?? path', ...) — the file, not just a boolean, since 'what' is the whole
    point of the box."""
    if not cwd:
        return None
    try:
        proc = await asyncio.create_subprocess_exec(
            "git", "-C", cwd, "status", "--porcelain",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        )
    except OSError:
        return None
    try:
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout_s)
    except TimeoutError:
        proc.kill()
        await proc.wait()
        return None
    if proc.returncode != 0:
        return None
    return [line for line in out.decode("utf-8", errors="replace").splitlines() if line]
