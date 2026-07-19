"""The telemetry reader — neo's second instrument, ported onto the store (task #35).

Claude Code retains failed-to-send telemetry on disk under ~/.claude/telemetry/ as
1p_failed_events.*.json (JSONL: one ClaudeCodeInternalEvent per line — event name,
session id, device id, model, platform, CLI version, betas). Nothing reads it, nothing
prunes it; it just accumulates. The ancestor's forensics lens answered "what's retained
on your disk"; this is that lens in the house's grain.

THE MEASURE-DON'T-AMPLIFY LAW: only normalized columns land in the store — the raw
payload is deliberately never copied (duplicating retained telemetry into the graph
would double the very retention the lens exists to expose). source_ref (file:line)
points back to the authoritative row on disk, the same doctrine as harness_turns.

Fed on the OBSERVER's switch (OSIRIS_TRANSCRIPTS) beside the transcript backfill —
a free, deterministic sweep with the same spend gate: an unchanged file costs one stat
and one row lookup, never a read.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncpg


def _stat(path: Path) -> tuple[datetime | None, int]:
    """mtime + size in one bare stat (sync helper for the blocking-call lint)."""
    try:
        st = path.stat()
        return datetime.fromtimestamp(st.st_mtime, tz=UTC), st.st_size
    except OSError:
        return None, 0


def _ts(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _rows_of_file(path: Path) -> list[tuple[Any, ...]]:
    """Parse one retained-events file into normalized rows (sync: pure disk IO).

    Row shape matches harness_telemetry's columns, source_ref first."""
    rows: list[tuple[Any, ...]] = []
    try:
        text = path.read_text("utf-8", errors="ignore")
    except OSError:
        return rows
    for i, line in enumerate(text.splitlines()):
        if not line.strip():
            continue
        try:
            d = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(d, dict):
            continue
        ev = d.get("event_data") or {}
        if not isinstance(ev, dict):
            continue
        env = ev.get("env") or {}
        if not isinstance(env, dict):
            env = {}
        rows.append((
            f"{path.name}:{i}",
            _ts(ev.get("client_timestamp")),
            str(ev.get("event_name") or ""),
            str(ev.get("session_id") or "") or None,
            str(ev.get("parent_session_id") or "") or None,
            str(ev.get("device_id") or "") or None,
            str(env.get("version") or "") or None,
            str(ev.get("model") or "") or None,
            str(env.get("platform") or "") or None,
            str(env.get("arch") or "") or None,
        ))
    return rows


class TelemetryStore:
    """The retained-telemetry index: fed from ~/.claude/telemetry, read by /overhead."""

    def __init__(self, pool: asyncpg.Pool, root: Path | None = None) -> None:
        self.pool = pool
        self.root = root or (Path.home() / ".claude" / "telemetry")

    async def backfill(self) -> int:
        """Eat every retained-events file, spend-gated per file. Returns files read."""
        if not self.root.is_dir():
            return 0
        files_read = 0
        for path in sorted(self.root.glob("*.json")):
            if path.is_symlink() or not path.is_file():
                continue
            mtime, size = _stat(path)
            if mtime is None:
                continue
            prev = await self.pool.fetchrow(
                "SELECT last_ingested_at FROM harness_telemetry_files WHERE file=$1",
                path.name)
            if prev is not None and mtime <= prev["last_ingested_at"]:
                continue
            rows = _rows_of_file(path)
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    if rows:
                        await conn.executemany(
                            "INSERT INTO harness_telemetry "
                            "  (source_ref, recorded_at, event, session_id, "
                            "   parent_session_id, device_id, version, model, "
                            "   platform, arch) "
                            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) "
                            "ON CONFLICT (source_ref) DO NOTHING",
                            rows)
                    await conn.execute(
                        "INSERT INTO harness_telemetry_files "
                        "  (file, last_ingested_at, rows, bytes) "
                        "VALUES ($1, $2, $3, $4) "
                        "ON CONFLICT (file) DO UPDATE "
                        "  SET last_ingested_at=$2, rows=$3, bytes=$4",
                        path.name, mtime, len(rows), size)
            files_read += 1
        return files_read

    async def summary(self) -> dict[str, Any] | None:
        """The forensics glance for /overhead: what Claude Code has retained on disk.
        None when nothing has been eaten (absence, never a zero-row pretence)."""
        row = await self.pool.fetchrow(
            "SELECT count(*) AS events, "
            "       count(DISTINCT session_id) AS sessions, "
            "       count(DISTINCT device_id) FILTER "
            "         (WHERE device_id IS NOT NULL) AS devices, "
            "       count(DISTINCT event) AS event_kinds, "
            "       min(recorded_at) AS oldest, max(recorded_at) AS newest "
            "FROM harness_telemetry")
        if row is None or not row["events"]:
            return None
        files = await self.pool.fetchrow(
            "SELECT count(*) AS files, COALESCE(SUM(bytes), 0) AS bytes "
            "FROM harness_telemetry_files")
        top = await self.pool.fetch(
            "SELECT event, count(*) AS n FROM harness_telemetry "
            "GROUP BY event ORDER BY n DESC LIMIT 5")
        return {
            "events": int(row["events"]),
            "sessions": int(row["sessions"]),
            "devices": int(row["devices"]),
            "event_kinds": int(row["event_kinds"]),
            "oldest": row["oldest"], "newest": row["newest"],
            "files": int(files["files"]) if files else 0,
            "bytes": int(files["bytes"]) if files else 0,
            "top_events": [{"event": r["event"], "n": int(r["n"])} for r in top],
        }
