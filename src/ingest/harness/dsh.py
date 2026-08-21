"""DSH transcript adapter — zstd-compressed session JSONL under ~/.dsh/sessions/.

DeepSeek Harness stores each session as a directory named after its workspace slug
containing a zstd-compressed JSONL file. The JSONL contains event-structured lines
— a session header event, then user/assistant messages, tool calls, boundaries, etc.

Model info lives in the `request/header` event (route.config.model) and
`request/context` event (provider + model fields). Individual assistant messages
do not carry per-turn model info in DSH.

This adapter normalizes DSH transcripts into the HarnessAdapter protocol so the
identity, swap-detection, and mining layers work without DSH-specific code.
"""
from __future__ import annotations

import json
import re
import shutil
import subprocess
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.ingest.harness import SessionLocator, TurnRow

_DSH_HOME = Path.home() / ".dsh"
_DSH_SESSIONS = _DSH_HOME / "sessions"
_ZSTD_EXT = ".zstd"

# DSH session id pattern: session-<uuid>
_SESSION_ID_RE = re.compile(
    r"^(.+)-([0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})$"
)

# Events that carry model info
_MODEL_EVENT_TYPES = frozenset({"request/header", "request/context"})

# Turn-level events (not system/streaming noise)
_TURN_MSG_TYPES = frozenset({"user/message", "assistant/message"})

# System-reminder regex (neo's, task #34)
_REMINDER_RE = re.compile(
    r"<system-reminder>.*?</system-reminder>", re.IGNORECASE | re.DOTALL
)


def _cwd_to_slug(cwd: str) -> str:
    """Convert a cwd path to DSH's slug convention.
    /home/user/code/project -> --home-user-code-project"""
    parts = Path(cwd).parts
    if parts and parts[0] == "/":
        parts = parts[1:]
    return "--" + "-".join(parts)


def _session_id_from_dir(slug: str) -> str:
    """Extract session UUID from a DSH session directory name."""
    m = _SESSION_ID_RE.match(slug)
    return m.group(2) if m else slug


def _session_file_in(session_dir: Path) -> Path | None:
    """Find the zstd-compressed JSONL file inside a session directory."""
    if not session_dir.is_dir():
        return None
    for f in session_dir.iterdir():
        if f.is_file() and f.suffix == _ZSTD_EXT:
            return f
    return None


def _find_slug_for(cwd: str) -> str | None:
    """Find the DSH session slug matching a given cwd, or the most recent one."""
    if not _DSH_SESSIONS.is_dir():
        return None
    expected = _cwd_to_slug(cwd)
    slug_dir = _DSH_SESSIONS / expected
    if slug_dir.is_dir() and _session_file_in(slug_dir) is not None:
        return expected
    return None


def _decompress(path: Path) -> list[str] | None:
    """Decompress a zstd-compressed DSH session file. Returns lines or None."""
    if not path.is_file():
        return None
    zstd_path = shutil.which("zstd")
    if zstd_path is None:
        return None
    try:
        result = subprocess.run(
            [zstd_path, "-dc", str(path)],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            return None
        return [line for line in result.stdout.splitlines() if line.strip()]
    except (OSError, subprocess.TimeoutExpired):
        return None


def _normalize_model(raw: str) -> str:
    """Normalize DSH model names: 'deepseek/deepseek-v4-flash' -> 'deepseek-v4-flash'.
    Strips provider prefix if present."""
    if not raw or not isinstance(raw, str):
        return raw
    parts = raw.split("/")
    return parts[-1]


def _parse_timestamp(ts: Any) -> datetime | None:
    """Parse a DSH timestamp (epoch ms) into a UTC datetime."""
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(int(ts) / 1000, tz=UTC)
    except (OSError, ValueError, OverflowError):
        return None


class DshSessionAdapter:
    """DeepSeek Harness transcript adapter for ~/.dsh/sessions/."""

    name = "dsh"

    def discover(
        self, *, cwd: str | None, job_dir: str | None, root: Path | None = None,
    ) -> SessionLocator | None:
        if not _DSH_SESSIONS.is_dir():
            return None
        cwd_str = cwd or str(Path.cwd())
        slug = _find_slug_for(cwd_str)
        if slug is None:
            return None
        slug_dir = _DSH_SESSIONS / slug
        session_file = _session_file_in(slug_dir)
        if session_file is None:
            return None
        lines = _decompress(session_file)
        if not lines:
            return None
        session_id: str | None = None
        for line in lines[:5]:
            try:
                d = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            if d.get("type") == "session":
                session_id = d.get("id")
                break
        if not session_id:
            return None
        anchor_sid = _session_id_from_dir(session_id)[:8]
        return SessionLocator(
            anchor_sid=anchor_sid,
            session_id=session_id,
            harness=self.name,
            source_path=str(session_file),
            cwd=cwd_str,
            project=Path(cwd_str).name,
            anchored=False,
        )

    def discover_at(self, path: Path) -> SessionLocator | None:
        """Explicit-path discovery lane for transcript_store."""
        if not path.is_file() or path.suffix != _ZSTD_EXT:
            return None
        lines = _decompress(path)
        if not lines:
            return None
        session_id: str | None = None
        cwd: str | None = None
        for line in lines[:5]:
            try:
                d = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            if d.get("type") == "session":
                session_id = d.get("id") or d.get("data", {}).get("id")
                cwd = d.get("cwd") or d.get("data", {}).get("cwd")
                break
        if not session_id:
            return None
        return SessionLocator(
            anchor_sid=_session_id_from_dir(session_id)[:8],
            session_id=session_id,
            harness=self.name,
            source_path=str(path),
            cwd=cwd,
            project=Path(cwd).name if cwd else None,
            anchored=True,
        )

    def enumerate(self, *, root: Path | None = None) -> Iterator[SessionLocator]:
        """Yield every DSH session on disk."""
        sessions_dir = Path(root) if root else _DSH_SESSIONS
        if not sessions_dir.is_dir():
            return
        for slug_dir in sessions_dir.iterdir():
            if not slug_dir.is_dir():
                continue
            session_file = _session_file_in(slug_dir)
            if session_file is None:
                continue
            lines = _decompress(session_file)
            if not lines:
                continue
            session_id: str | None = None
            cwd: str | None = None
            for line in lines[:5]:
                try:
                    d = json.loads(line)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
                if d.get("type") == "session":
                    session_id = d.get("id")
                    cwd = d.get("cwd")
                    break
            if not session_id:
                continue
            yield SessionLocator(
                anchor_sid=_session_id_from_dir(session_id)[:8],
                session_id=session_id,
                harness=self.name,
                source_path=str(session_file),
                cwd=cwd,
                project=Path(cwd).name if cwd else None,
            )

    def read_turns(
        self, locator: SessionLocator, *, since_idx: int = 0,
    ) -> Iterator[TurnRow]:
        path = Path(locator.source_path)
        lines = _decompress(path)
        if not lines or since_idx >= len(lines):
            return

        # Build model timeline from request/header and request/context events
        model_timeline: list[tuple[int, str]] = []
        for i, line in enumerate(lines):
            try:
                d = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            t = d.get("type", "")
            if t == "request/header":
                config = d.get("data", {}).get("header", {}).get("config", {})
                model = _normalize_model(config.get("model", ""))
                if model:
                    model_timeline.append((i, model))
            elif t == "request/context":
                model = d.get("data", {}).get("model", "")
                if model:
                    model_timeline.append((i, _normalize_model(model)))

        # Walk the events and produce TurnRows for user/assistant messages
        # DSH doesn't number its turns in the event stream — we assign turn_idx
        # sequentially based on user/assistant message pairs.
        turn_idx = 0
        for i in range(since_idx, len(lines)):
            line = lines[i]
            try:
                d = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            t = d.get("type", "")
            if t not in _TURN_MSG_TYPES:
                continue

            data = d.get("data", {})
            msg = data.get("message", data) if isinstance(data, dict) else data
            role = data.get("role", "") or msg.get("role", "") if isinstance(msg, dict) else ""
            if role not in ("user", "assistant"):
                continue

            # Find the model in effect for this event
            event_model: str | None = None
            for mt_idx, mt_model in reversed(model_timeline):
                if mt_idx <= i:
                    event_model = mt_model
                    break

            # Count reminders (system-reminder blocks in user messages)
            reminders = 0
            content = None
            if isinstance(msg, dict):
                content = msg.get("content")
            reminders = _reminder_count(content) if role == "user" else 0

            # Parse timestamps
            ts = _parse_timestamp(data.get("time") or d.get("time") or d.get("timestamp"))
            finished_at = _parse_timestamp(data.get("finished_at"))
            duration: int | None = None
            if ts and finished_at:
                duration = int((finished_at - ts).total_seconds() * 1000)

            turn_idx += 1

            yield TurnRow(
                turn_idx=turn_idx,
                role=role,
                model=event_model,
                reminders=reminders if reminders else None,
                recorded_at=ts,
                duration_ms=duration,
                is_summary=False,
                source_ref=f"dsh:{locator.anchor_sid}:{i}",
            )


def _reminder_count(content: Any) -> int:
    """Count system-reminder blocks in a message content tree."""
    total = 0
    if isinstance(content, str):
        total += len(_REMINDER_RE.findall(content))
    elif isinstance(content, list):
        for item in content:
            if isinstance(item, dict):
                for key in ("text", "content"):
                    total += _reminder_count(item.get(key))
    return total
