"""How much archived WAL is waiting to be pruned: the write-ahead log older than the newest
VERIFIED base backup's start segment. Nothing in the retention plan needs that WAL (a restore
starts from the base backup and replays forward), so a large amount of it means the weekly
prune did not run or could not, and the disk is paying for it. Read from the vault with plain
file reads, never the database. Cached for a few minutes: the vault holds tens of thousands of
segment files and readiness is a polled route."""
from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any

DEFAULT_LIMIT_GB = 50
_CACHE_SECONDS = 300.0
_cache: dict[str, tuple[float, dict[str, Any]]] = {}


def default_vault() -> Path:
    return Path(os.environ.get("OSIRIS_VAULT", str(Path.home() / "osiris-vault")))


def _measure(vault: Path) -> dict[str, Any]:
    from scripts.osiris_prune_ladder import (
        _backup_start_segment,
        _scan,
        _scan_wal,
        newest_verified_base,
        plan_prune_wal,
    )

    verified = newest_verified_base(_scan(vault / "basebackups"))
    if verified is None:
        return {"verified": None, "bytes": 0, "segments": 0, "base": None}
    start = _backup_start_segment(verified.path)
    if start is None:
        return {"verified": None, "bytes": 0, "segments": 0, "base": Path(verified.path).name,
                "unreadable": True}
    plan = plan_prune_wal(_scan_wal(vault / "wal_archive"), kept_start_segments=[start])
    removable = plan["remove"]
    return {"verified": True, "base": Path(verified.path).name,
            "bytes": sum(s.size_bytes for s in removable), "segments": len(removable)}


def wal_backlog(vault: Path | None = None, *, now: float | None = None) -> dict[str, Any]:
    """`{"verified": bool|None, "bytes": n, "segments": n, "base": name}`; `verified` is None
    when no base backup has been verified yet (nothing can be judged)."""
    root = vault or default_vault()
    clock = time.monotonic() if now is None else now
    hit = _cache.get(str(root))
    if hit is not None and clock - hit[0] < _CACHE_SECONDS:
        return hit[1]
    value = _measure(root)
    _cache[str(root)] = (clock, value)
    return value


def clear_cache() -> None:
    _cache.clear()
