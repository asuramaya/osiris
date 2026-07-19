"""Crush transcript adapter — SQLite at <data_dir>/crush.db.

Crush (charmbracelet's CLI) stores sessions in SQLite, not JSONL. The per-project data
dir is resolved from ~/.local/share/crush/projects.json (maps cwd → data_dir), falling
back to <cwd>/.crush/crush.db. model + provider are first-class columns on the messages
table — richer than Claude's embedded-in-envelope field, and the reason the store exists.

Token accounting is session-level in Crush (sessions.prompt_tokens / completion_tokens /
cost), not per-turn. The adapter records what's available; the store's per-turn token
columns stay NULL for Crush. Slice 2 can add a session-aggregate rollup if cost telemetry
needs it.
"""
from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

from src.ingest.harness import SessionLocator, TurnRow

_PROJECTS_JSON = Path.home() / ".local" / "share" / "crush" / "projects.json"


def _project_entries() -> list[dict[str, object]]:
    """projects.json's entries, normalized. Crush has shipped BOTH shapes: a
    {cwd: {data_dir}} mapping and (current on this box, field-verified 2026-07-19) a
    LIST of {path, data_dir} records. Normalize either to [{path, data_dir}, ...] so
    the two consumers below never care again."""
    try:
        data = json.loads(_PROJECTS_JSON.read_text()) if _PROJECTS_JSON.is_file() else {}
    except (OSError, ValueError):
        return []
    projects = data.get("projects") if isinstance(data, dict) else None
    if isinstance(projects, dict):
        return [{"path": cwd, "data_dir": e.get("data_dir")}
                for cwd, e in projects.items() if isinstance(e, dict)]
    if isinstance(projects, list):
        return [e for e in projects if isinstance(e, dict)]
    return []


def _resolve_data_dir(cwd: str | None) -> str | None:
    """Find Crush's SQLite DB for a cwd: projects.json mapping, else <cwd>/.crush/."""
    if cwd is None:
        return None
    for entry in _project_entries():
        if entry.get("path") != cwd:
            continue
        dd = entry.get("data_dir")
        if isinstance(dd, str) and (Path(dd) / "crush.db").is_file():
            return dd
    local = Path(cwd) / ".crush" / "crush.db"
    return str(Path(cwd) / ".crush") if local.is_file() else None


def _to_dt(epoch_ms: int | None) -> datetime | None:
    if epoch_ms is None:
        return None
    try:
        from datetime import UTC
        return datetime.fromtimestamp(int(epoch_ms) / 1000, tz=UTC)
    except (OSError, ValueError, OverflowError):
        return None


class CrushSqliteAdapter:
    """Crush's <data_dir>/crush.db transcript."""

    name = "crush"

    def discover(
        self, *, cwd: str | None, job_dir: str | None, root: Path | None = None,
    ) -> SessionLocator | None:
        dd = _resolve_data_dir(cwd) or _resolve_data_dir(str(Path.cwd()))
        if dd is None:
            return None
        db = Path(dd) / "crush.db"
        if not db.is_file():
            return None
        try:
            conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        except sqlite3.Error:
            return None
        try:
            # anchor on job_dir basename if it parses as a session-id prefix
            jid = _job_basename(job_dir)
            sid: str | None = None
            if jid:
                row = conn.execute(
                    "SELECT id FROM sessions WHERE id LIKE ? || '%' "
                    "ORDER BY created_at DESC LIMIT 1", (jid,),
                ).fetchone()
                sid = row[0] if row else None
            anchored = sid is not None
            if sid is None:
                # the newest-session guess — a co-tenant's session may be hotter; the
                # locator confesses it so identity never grades this as an anchored read
                row = conn.execute(
                    "SELECT id FROM sessions ORDER BY updated_at DESC LIMIT 1"
                ).fetchone()
                sid = row[0] if row else None
            if sid is None:
                return None
            return SessionLocator(
                anchor_sid=sid[:8], session_id=sid, harness=self.name,
                source_path=str(db), cwd=cwd,
                project=Path(cwd).name if cwd else None,
                anchored=anchored,
            )
        except sqlite3.Error:
            return None
        finally:
            conn.close()

    def enumerate(self, *, root: Path | None = None) -> Iterator[SessionLocator]:
        """Every Crush session across every known project — the miner's backfill sweep.

        Walks projects.json for known cwds, plus ~/.osiris/seats/*/.crush/ (seat offices).
        Each crush.db holds many sessions; yield one locator per session."""
        # 1. projects.json-registered cwds (either shape — see _project_entries)
        seen_dbs: set[str] = set()
        for entry in _project_entries():
            cwd, dd = entry.get("path"), entry.get("data_dir")
            if not isinstance(dd, str):
                continue
            db = Path(dd) / "crush.db"
            if not db.is_file() or str(db) in seen_dbs:
                continue
            seen_dbs.add(str(db))
            cwd_s = cwd if isinstance(cwd, str) else None
            yield from _sessions_in_db(db, cwd_s, Path(cwd_s).name if cwd_s else None)
        # 2. seat offices (~/.osiris/seats/<seat>/.crush/crush.db) — the per-seat Crush data
        seats_root = Path.home() / ".osiris" / "seats"
        if seats_root.is_dir():
            for seat_dir in seats_root.iterdir():
                if not seat_dir.is_dir():
                    continue
                db = seat_dir / ".crush" / "crush.db"
                if not db.is_file() or str(db) in seen_dbs:
                    continue
                seen_dbs.add(str(db))
                yield from _sessions_in_db(db, str(seat_dir), seat_dir.name)

    def read_turns(
        self, locator: SessionLocator, *, since_idx: int = 0,
    ) -> Iterator[TurnRow]:
        try:
            conn = sqlite3.connect(f"file:{locator.source_path}?mode=ro", uri=True)
        except sqlite3.Error:
            return
        try:
            rows = conn.execute(
                "SELECT rowid, role, model, provider, is_summary_message, "
                "       created_at, finished_at, updated_at "
                "FROM messages WHERE session_id = ? AND rowid > ? "
                "ORDER BY rowid ASC",
                (locator.session_id, since_idx),
            ).fetchall()
        except sqlite3.Error:
            return
        finally:
            conn.close()
        for rid, role, model, provider, is_sum, created, finished, _updated in rows:
            model_s = model if isinstance(model, str) and model else None
            prov_s = provider if isinstance(provider, str) and provider else None
            dur_ms: int | None = None
            if finished is not None and created is not None:
                dur_ms = int(finished - created) if finished > created else None
            yield TurnRow(
                turn_idx=int(rid), role=str(role),
                model=model_s, provider=prov_s,
                duration_ms=dur_ms,
                recorded_at=_to_dt(created),
                is_summary=bool(is_sum),
                swap_deliberate=None,  # Crush has no /model-on-record signal yet
                source_ref=f"rowid:{rid}",
            )


def _job_basename(job_dir: str | None) -> str | None:
    if not job_dir:
        return None
    name = Path(job_dir).name
    return name if name else None


def _sessions_in_db(
    db: Path, cwd: str | None, project: str | None,
) -> Iterator[SessionLocator]:
    """Yield one SessionLocator per session row in a Crush crush.db."""
    try:
        conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    except sqlite3.Error:
        return
    try:
        rows = conn.execute("SELECT id FROM sessions ORDER BY created_at ASC").fetchall()
    except sqlite3.Error:
        return
    finally:
        conn.close()
    for (sid,) in rows:
        if not sid or len(sid) < 8:
            continue
        yield SessionLocator(
            anchor_sid=sid[:8], session_id=sid, harness="crush",
            source_path=str(db), cwd=cwd, project=project,
        )
