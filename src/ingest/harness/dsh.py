"""DSH transcript adapter — zstd-compressed session JSONL under ~/.dsh/sessions/.

DeepSeek Harness stores sessions under a directory named after the workspace slug
(the cwd with `/` folded to `-`, prefixed `--`): `~/.dsh/sessions/<slug>/`. Inside
the slug dir each session owns its own nested dir named `session-<uuid>` holding
the zstd-compressed JSONL: `<slug>/session-<uuid>/session.jsonl.zstd` (verified
live, 2026-08-23 — the adapter originally assumed the zstd file sits directly in
the slug dir and so never discovered a single real session; the soul store carried
zero `harness='dsh'` rows). The JSONL contains event-structured lines — a session
header event (`type: "session"` carrying `id: "session-<uuid>"` and `cwd`), then
user/assistant messages, tool calls, boundaries, etc.

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
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.ingest.harness import SessionLocator, TurnRow

_DSH_HOME = Path.home() / ".dsh"
_DSH_SESSIONS = _DSH_HOME / "sessions"
_ZSTD_EXT = ".zstd"


def _dsh_sessions() -> Path:
    """The sessions root, resolved at CALL time — a test's tmp HOME (or a relocated
    home) must not be frozen at import. The module constants stay for callers that
    want the import-time default; discovery paths use this."""
    return Path.home() / ".dsh" / "sessions"

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


# Chars DSH's projectKey keeps verbatim (everything else ~XXXX-escapes); mirrors
# packages/session/session-persistence-jsonl/src/format.ts in the harness.
_SAFE = re.compile(r"^[A-Za-z0-9._-]$")


def _cwd_to_slug(cwd: str) -> str:
    r"""Convert a cwd path to DSH's projectKey slug convention, exactly as the
    harness computes it (format.ts projectKey): separators (/ \ :) fold to one
    `-`, safe chars kept, unsafe code units `~XXXX`-escaped, leading dashes
    stripped, wrapped in `--…--` and bounded to 251 slug chars.
    /home/user/code/project -> --home-user-code-project--"""
    readable: list[str] = []
    separator_run = False
    for ch in cwd:
        if ch in "/\\:":
            if not separator_run:
                readable.append("-")
            separator_run = True
        elif ch != "~" and _SAFE.match(ch):
            readable.append(ch)
            separator_run = False
        else:
            readable.append(f"~{ord(ch):04X}")
            separator_run = False
    slug = "".join(readable).lstrip("-") or "root"
    return f"--{slug[:251]}--"


def _session_id_from_dir(slug: str) -> str:
    """Extract session UUID from a DSH session directory name."""
    m = _SESSION_ID_RE.match(slug)
    return m.group(2) if m else slug


# The nested per-session dir name — TWO grammars, both real and live-verified
# (2026-08-23, same finding src/ingest/sessions.py and src/orchestrator/handshake.py's
# own _UUID_RE/_sid8 helpers already account for): depth-0 interactive sessions carry a
# `session-` prefix, spawned subagent sessions are a bare uuid. A regex that only
# matched the prefixed form silently dropped every subagent session from `enumerate`/
# `_session_dirs_in`/`_session_dir_from_job_dir` — found live via
# test_enumerate_finds_every_session_under_a_slug_with_more_than_one's own two-session
# fixture (one of each shape), not assumed.
_SESSION_DIR_RE = re.compile(r"^(?:session-)?[0-9a-f-]{36}$")


def _session_file_in(session_dir: Path) -> Path | None:
    """Find the zstd-compressed JSONL inside one DSH session directory.

    Handles BOTH layouts: the real nested one (`<dir>/session-<uuid>/session.jsonl.zstd`)
    and a flat `<dir>/session.jsonl.zstd` (the shape this module originally assumed —
    kept so a layout change never silently blinds the adapter again)."""
    if not session_dir.is_dir():
        return None
    for f in sorted(session_dir.iterdir()):
        if f.is_file() and f.suffix == _ZSTD_EXT:
            return f
    for f in sorted(session_dir.iterdir()):
        if f.is_dir() and _SESSION_DIR_RE.match(f.name):
            zst = _session_file_in(f)
            if zst is not None:
                return zst
    return None


def _session_dirs_in(slug_dir: Path) -> list[tuple[Path, Path]]:
    """Every (session_dir, zstd_file) pair under a slug dir, newest first.

    A slug dir holds ONE session per nested `session-<uuid>` dir — a workspace can
    accumulate many sessions over days, so cwd-based (unanchored) discovery must pick
    the hottest rather than whichever sorted first."""
    if not slug_dir.is_dir():
        return []
    out: list[tuple[Path, Path]] = []
    for f in sorted(slug_dir.iterdir()):
        if f.is_dir() and _SESSION_DIR_RE.match(f.name):
            zst = _session_file_in(f)
            if zst is not None:
                out.append((f, zst))
    # newest mtime first — same hottest-wins rule as locate_transcript_by_cwd
    out.sort(key=lambda pair: pair[1].stat().st_mtime, reverse=True)
    return out


def _session_dir_from_job_dir(job_dir: str | Path | None) -> Path | None:
    """The DSH session dir a job_dir names, if it is one — `.../.dsh/sessions/
    <slug>/session-<uuid>` (or the zstd file inside it, or the file path alone).
    None for anything else (a Claude jobs dir, a wake stub, a foreign path)."""
    if not job_dir:
        return None
    p = Path(job_dir).expanduser()
    if p.is_file() and p.suffix == _ZSTD_EXT:
        p = p.parent
    if _SESSION_DIR_RE.match(p.name) and p.is_dir() and _session_file_in(p) is not None:
        return p
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
        # THE ANCHOR LANE (the mount() tool's DSH door): a job_dir naming a real DSH
        # session dir is this session's OWN record — anchored, exactly like a Claude
        # jobs/<sid8> dir. Without it, cwd discovery is a hottest-guess across every
        # session ever run in that workspace (anchored=False, the same grade the cwd
        # lane has always carried for Claude).
        anchored_dir = _session_dir_from_job_dir(job_dir)
        if anchored_dir is not None:
            locator = self._locator_for(anchored_dir, cwd=cwd)
            if locator is not None:
                return locator
        sessions_root = Path(root) if root else _dsh_sessions()
        if not sessions_root.is_dir():
            return None
        cwd_str = cwd or str(Path.cwd())
        slug = _cwd_to_slug(cwd_str)
        slug_dir = sessions_root / slug
        # newest session in this workspace (the nested layout holds many)
        pairs = _session_dirs_in(slug_dir)
        if not pairs:
            # flat layout fallback (one zstd directly under the slug dir)
            flat = _session_file_in(slug_dir)
            if flat is not None:
                locator = self._locator_for(slug_dir, cwd=cwd, source=flat)
                if locator is not None:
                    return replace(locator, anchored=False)
            return None
        session_dir, session_file = pairs[0]
        locator = self._locator_for(session_dir, cwd=cwd, source=session_file)
        if locator is None:
            return None
        return replace(locator, anchored=False)

    def _locator_for(
        self, session_dir: Path, *, cwd: str | None, source: Path | None = None,
    ) -> SessionLocator | None:
        """Read the session header out of one session dir's zstd and build the
        ANCHORED locator for it (the caller downgrades anchored when guessing)."""
        session_file = source or _session_file_in(session_dir)
        if session_file is None:
            return None
        lines = _decompress(session_file)
        if not lines:
            return None
        session_id: str | None = None
        header_cwd: str | None = None
        for line in lines[:5]:
            try:
                d = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            if d.get("type") == "session":
                session_id = d.get("id")
                header_cwd = d.get("cwd")
                break
        if not session_id:
            return None
        anchor_sid = _session_id_from_dir(session_id)[:8]
        return SessionLocator(
            anchor_sid=anchor_sid,
            session_id=session_id,
            harness=self.name,
            source_path=str(session_file),
            cwd=cwd or header_cwd,
            project=Path(cwd or header_cwd or "").name or None,
            anchored=True,
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
        """Yield every DSH session on disk (nested and flat layouts alike) —
        BEST-EFFORT COMPLETE, not guaranteed: a nesting shape this box has never seen
        would silently drop that slug's sessions, same class of gap that made earlier
        walks under-count in the first place. See HarnessAdapter's own docstring for
        what "complete" means across adapters."""
        sessions_dir = Path(root) if root else _DSH_SESSIONS
        if not sessions_dir.is_dir():
            return
        for slug_dir in sorted(sessions_dir.iterdir()):
            if not slug_dir.is_dir():
                continue
            for session_dir, session_file in _session_dirs_in(slug_dir):
                locator = self._locator_for(session_dir, cwd=None, source=session_file)
                if locator is not None:
                    yield locator
            # flat layout fallback: one zstd directly under the slug dir
            for f in sorted(slug_dir.iterdir()):
                if f.is_file() and f.suffix == _ZSTD_EXT:
                    locator = self._locator_for(slug_dir, cwd=None, source=f)
                    if locator is not None:
                        yield locator

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
