"""Rebuild a stored session as a NEW session in another harness's native format.

osiris keeps every line of every session it has ingested, from any harness and any account.
This reads one from the store and writes its turns as a brand-new session (a new session id,
a new database row or file) that the target harness can open and continue. The original is
never touched, and nothing here claims anyone's ownership: no account or organization stamp
is written, the old account's or the current one's. The new session opens with a marker
turn that says what it is, "rebuilt by osiris from session <id>, <date range>", and names
whatever was not carried as a turn.

Faithful means: turns, roles, tool calls and tool results are carried where the target has a
slot for them; reasoning is carried where there is a slot; anything else (attachments, hook
lines, bookkeeping records, images) is FOLDED, never dropped silently: it is counted by kind
in the marker turn and in the returned report.

The neutral model below is deliberately small (text, reasoning, tool call, tool result) so
each target writer is a plain mapping. Targets: crush (SQLite). dsh and claude are separate
writers over the same turns."""
from __future__ import annotations

import contextlib
import json
import os
import shutil
import sqlite3
import tempfile
import uuid
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


@dataclass
class Part:
    kind: str                     # text | reasoning | tool_call | tool_result
    text: str = ""
    call_id: str = ""
    name: str = ""
    input: str = ""               # tool_call: the JSON-encoded input
    is_error: bool = False        # tool_result: the tool reported a failure


@dataclass
class Turn:
    role: str                     # user | assistant | tool
    parts: list[Part] = field(default_factory=list)
    at: datetime | None = None
    model: str | None = None
    summary: bool = False         # a compaction summary the old session wrote about itself
    src_line: int = 0
    msg_id: str = ""              # assistant lines that share one message id are one turn


@dataclass
class Rebuilt:
    turns: list[Turn]
    folded: Counter[str]          # what was not carried as a turn, by kind
    first_at: datetime | None
    last_at: datetime | None
    source_lines: int
    stored_total: int | None = None   # lines the store says the session has, when known

    @property
    def truncated(self) -> bool:
        """The store's chain broke before the end: fewer lines read than the session holds."""
        return self.stored_total is not None and self.source_lines < self.stored_total


def _ts(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _result_text(body: Any, folded: Counter[str]) -> str:
    if isinstance(body, str):
        return body
    out: list[str] = []
    for b in body if isinstance(body, list) else []:
        if isinstance(b, dict) and b.get("type") == "text":
            out.append(str(b.get("text", "")))
        elif isinstance(b, dict):
            folded[f"{b.get('type', 'block')} inside a tool result"] += 1
            out.append(f"[{b.get('type', 'block')} not carried]")
    return "\n".join(out)


def turns_from_claude_lines(lines: Iterable[tuple[int, bytes]]) -> Rebuilt:
    """The neutral turns of a stored Claude Code session, plus what was folded."""
    turns: list[Turn] = []
    folded: Counter[str] = Counter()
    call_names: dict[str, str] = {}      # a tool result names its call by id only
    first: datetime | None = None
    last: datetime | None = None
    n = 0
    for idx, raw in lines:
        n = idx + 1
        try:
            d = json.loads(raw)
        except ValueError:
            folded["unreadable line"] += 1
            continue
        if not isinstance(d, dict):
            continue
        at = _ts(d.get("timestamp"))
        if at is not None:
            first = first or at
            last = at
        kind = d.get("type")
        msg = d.get("message")
        content = msg.get("content") if isinstance(msg, dict) else None
        if kind == "user" and not d.get("isMeta"):
            if isinstance(content, str):
                if content.strip():
                    turns.append(Turn("user", [Part("text", content)], at, None,
                                      bool(d.get("isCompactSummary")), idx))
            elif isinstance(content, list):
                results = [b for b in content
                           if isinstance(b, dict) and b.get("type") == "tool_result"]
                if results:
                    turns.append(Turn("tool", [
                        Part("tool_result", _result_text(b.get("content"), folded),
                             call_id=str(b.get("tool_use_id") or ""),
                             name=call_names.get(str(b.get("tool_use_id") or ""), ""),
                             is_error=bool(b.get("is_error")))
                        for b in results], at, None, False, idx))
                texts = [str(b.get("text", "")) for b in content
                         if isinstance(b, dict) and b.get("type") == "text"]
                if any(t.strip() for t in texts):
                    turns.append(Turn("user", [Part("text", "\n".join(texts))], at, None,
                                      bool(d.get("isCompactSummary")), idx))
                for b in content:
                    if isinstance(b, dict) and b.get("type") not in ("tool_result", "text"):
                        folded[f"{b.get('type', 'block')} in a user turn"] += 1
        elif kind == "assistant" and isinstance(msg, dict):
            parts: list[Part] = []
            for b in content if isinstance(content, list) else (
                    [{"type": "text", "text": content}] if isinstance(content, str) else []):
                if not isinstance(b, dict):
                    continue
                t = b.get("type")
                if t == "text":
                    parts.append(Part("text", str(b.get("text", ""))))
                elif t == "thinking":
                    parts.append(Part("reasoning", str(b.get("thinking", ""))))
                elif t == "tool_use":
                    call_names[str(b.get("id") or "")] = str(b.get("name") or "")
                    parts.append(Part("tool_call", call_id=str(b.get("id") or ""),
                                      name=str(b.get("name") or ""),
                                      input=json.dumps(b.get("input") or {})))
                else:
                    folded[f"{t or 'block'} in an assistant turn"] += 1
            if not parts:
                continue
            mid = str(msg.get("id") or "")
            if turns and turns[-1].role == "assistant" and mid and turns[-1].msg_id == mid:
                turns[-1].parts.extend(parts)
            else:
                turns.append(Turn("assistant", parts, at, str(msg.get("model") or "") or None,
                                  False, idx, mid))
        else:
            folded[str(kind or "untyped") + (" (meta)" if d.get("isMeta") else "")] += 1
    return Rebuilt(turns, folded, first, last, n)


def marker_text(anchor_sid: str, rebuilt: Rebuilt) -> str:
    span = (f"{rebuilt.first_at:%Y-%m-%d} to {rebuilt.last_at:%Y-%m-%d}"
            if rebuilt.first_at and rebuilt.last_at else "dates unknown")
    fold = ", ".join(f"{k} {v}" for k, v in rebuilt.folded.most_common()) or "nothing"
    cut = (f" TRUNCATED: the stored chain broke at line {rebuilt.source_lines} of "
           f"{rebuilt.stored_total}; everything after that line is missing from this copy."
           if rebuilt.truncated else "")
    return (f"[osiris import] This conversation was rebuilt by osiris from stored session "
            f"{anchor_sid} ({span}; {rebuilt.source_lines} stored lines, "
            f"{len(rebuilt.turns)} turns). It is a copy, not the original: no owner or "
            "account is recorded on it." + cut + " Not carried as turns (folded): "
            + fold + ".")


# --- crush ---------------------------------------------------------------------------------

_CRUSH_SCHEMA = (
    """CREATE TABLE IF NOT EXISTS goose_db_version (
        id INTEGER PRIMARY KEY AUTOINCREMENT, version_id INTEGER NOT NULL,
        is_applied INTEGER NOT NULL, tstamp TIMESTAMP DEFAULT (datetime('now')))""",
    """CREATE TABLE IF NOT EXISTS sessions (
        id TEXT PRIMARY KEY, parent_session_id TEXT, title TEXT NOT NULL,
        message_count INTEGER NOT NULL DEFAULT 0 CHECK (message_count >= 0),
        prompt_tokens INTEGER NOT NULL DEFAULT 0 CHECK (prompt_tokens >= 0),
        completion_tokens INTEGER NOT NULL DEFAULT 0 CHECK (completion_tokens >= 0),
        cost REAL NOT NULL DEFAULT 0.0 CHECK (cost >= 0.0),
        updated_at INTEGER NOT NULL, created_at INTEGER NOT NULL,
        summary_message_id TEXT, todos TEXT)""",
    """CREATE TABLE IF NOT EXISTS files (
        id TEXT PRIMARY KEY, session_id TEXT NOT NULL, path TEXT NOT NULL,
        content TEXT NOT NULL, version INTEGER NOT NULL DEFAULT 0,
        created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
        FOREIGN KEY (session_id) REFERENCES sessions (id) ON DELETE CASCADE,
        UNIQUE(path, session_id, version))""",
    """CREATE TABLE IF NOT EXISTS messages (
        id TEXT PRIMARY KEY, session_id TEXT NOT NULL, role TEXT NOT NULL,
        parts TEXT NOT NULL default '[]', model TEXT, created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL, finished_at INTEGER, provider TEXT,
        is_summary_message INTEGER DEFAULT 0 NOT NULL,
        FOREIGN KEY (session_id) REFERENCES sessions (id) ON DELETE CASCADE)""",
    """CREATE TABLE IF NOT EXISTS read_files (
        session_id TEXT NOT NULL CHECK (session_id != ''), path TEXT NOT NULL CHECK (path != ''),
        read_at INTEGER NOT NULL,
        FOREIGN KEY (session_id) REFERENCES sessions (id) ON DELETE CASCADE,
        PRIMARY KEY (path, session_id))""",
    "CREATE INDEX IF NOT EXISTS idx_messages_session_id ON messages (session_id)",
    "CREATE INDEX IF NOT EXISTS idx_sessions_created_at ON sessions (created_at)",
    "CREATE INDEX IF NOT EXISTS idx_messages_created_at ON messages (created_at)",
    "CREATE INDEX IF NOT EXISTS idx_files_session_id ON files (session_id)",
    "CREATE INDEX IF NOT EXISTS idx_files_path ON files (path)",
    "CREATE INDEX IF NOT EXISTS idx_read_files_session_id ON read_files (session_id)",
    """CREATE TRIGGER IF NOT EXISTS update_sessions_updated_at AFTER UPDATE ON sessions
        BEGIN UPDATE sessions SET updated_at = strftime('%s', 'now') WHERE id = new.id; END""",
    """CREATE TRIGGER IF NOT EXISTS update_messages_updated_at AFTER UPDATE ON messages
        BEGIN UPDATE messages SET updated_at = strftime('%s', 'now') WHERE id = new.id; END""",
    """CREATE TRIGGER IF NOT EXISTS update_session_message_count_on_insert
        AFTER INSERT ON messages
        BEGIN UPDATE sessions SET message_count = message_count + 1
        WHERE id = new.session_id; END""",
    """CREATE TRIGGER IF NOT EXISTS update_session_message_count_on_delete
        AFTER DELETE ON messages
        BEGIN UPDATE sessions SET message_count = message_count - 1
        WHERE id = old.session_id; END""",
)
_GOOSE_VERSIONS = (0, 20250424200609, 20250515105448, 20250624000000, 20250627000000,
                   20250810000000, 20250812000000, 20260127000000)


def _with_writer_folds(rebuilt: Rebuilt, to: str) -> Rebuilt:
    """`rebuilt` with what THIS target cannot carry added to its folded counts, so the opening
    note and the receipt say it. Claude: a thinking block needs the provider's signature, which
    the store never had for a rebuilt copy, so reasoning is left out rather than written with
    an empty one. DSH: no error flag on a tool result and no compaction-summary marker."""
    fold: Counter[str] = Counter(rebuilt.folded)
    parts = [p_ for t in rebuilt.turns for p_ in t.parts]
    if to == "claude":
        n = sum(1 for p_ in parts if p_.kind == "reasoning")
        if n:
            fold["reasoning (a thinking block needs a signature this copy cannot have)"] += n
    if to == "dsh":
        n = sum(1 for p_ in parts if p_.kind == "tool_result" and p_.is_error)
        if n:
            fold["tool-error flags (DSH has no slot for them)"] += n
        n = sum(1 for t in rebuilt.turns if t.summary)
        if n:
            fold["compaction summaries carried as marked text"] += n
    return Rebuilt(rebuilt.turns, fold, rebuilt.first_at, rebuilt.last_at,
                   rebuilt.source_lines, rebuilt.stored_total)


def _crush_parts(turn: Turn, at: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for p in turn.parts:
        if p.kind == "text":
            out.append({"type": "text", "data": {"text": p.text}})
        elif p.kind == "reasoning":
            out.append({"type": "reasoning", "data": {
                "thinking": p.text, "signature": "", "thought_signature": "", "tool_id": "",
                "responses_data": None, "started_at": at, "finished_at": at}})
        elif p.kind == "tool_call":
            out.append({"type": "tool_call", "data": {
                "id": p.call_id, "name": p.name, "input": p.input,
                "provider_executed": False, "finished": True}})
        elif p.kind == "tool_result":
            out.append({"type": "tool_result", "data": {
                "tool_call_id": p.call_id, "name": p.name, "content": p.text,
                "is_error": p.is_error}})
    if turn.role == "assistant":
        calls = any(p.kind == "tool_call" for p in turn.parts)
        out.append({"type": "finish", "data": {
            "reason": "tool_use" if calls else "stop", "time": 0}})
    return out


def write_crush(
    rebuilt: Rebuilt, *, anchor_sid: str, workspace: Path, title: str | None = None,
) -> dict[str, Any]:
    """A NEW Crush session in `<workspace>/.crush/crush.db` (created with Crush's own schema
    when absent, added to when present), opening with the marker turn. Returns the new
    session id, the database path and the folded counts."""
    data_dir = workspace / ".crush"
    db = data_dir / "crush.db"
    _refuse_symlinks(data_dir, db)
    _refuse_session_store(workspace, data_dir, db)
    _private_mkdir(data_dir)
    fresh = not db.exists()
    conn = sqlite3.connect(db)
    try:
        if fresh:
            conn.execute("PRAGMA journal_mode=WAL")
            for ddl in _CRUSH_SCHEMA:
                conn.execute(ddl)
            conn.executemany("INSERT INTO goose_db_version (version_id, is_applied) VALUES (?, 1)",
                             [(v,) for v in _GOOSE_VERSIONS])
        sid = str(uuid.uuid4())
        started = rebuilt.first_at or datetime.now(UTC)
        t0 = int(started.timestamp())
        conn.execute(
            "INSERT INTO sessions (id, title, message_count, updated_at, created_at) "
            "VALUES (?, ?, 0, ?, ?)",
            (sid, title or f"Rebuilt from {anchor_sid}", t0, t0))
        marker = Turn("user", [Part("text", marker_text(anchor_sid, rebuilt))], started)
        for t in [marker, *rebuilt.turns]:
            at = int((t.at or started).timestamp())
            model = t.model if t.role == "assistant" else ""
            conn.execute(
                "INSERT INTO messages (id, session_id, role, parts, model, created_at, "
                "updated_at, finished_at, provider, is_summary_message) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (str(uuid.uuid4()), sid, t.role, json.dumps(_crush_parts(t, at)), model or "",
                 at, at, at if t.role == "assistant" else None, None, int(t.summary)))
        conn.commit()
    finally:
        conn.close()
    for f in db.parent.glob(db.name + "*"):   # crush.db and its -wal / -shm
        f.chmod(0o600)
    return {"to": "crush", "session_id": sid, "path": str(db), "turns": len(rebuilt.turns) + 1,
            "folded": dict(rebuilt.folded)}


# --- claude --------------------------------------------------------------------------------

_CLAUDE_VERSION = "2.1.322"   # the version whose transcript shape this follows (observed)


def dsh_sessions_dir() -> str:
    """Where DSH keeps its sessions (`~/.dsh/sessions`)."""
    return str(Path.home() / ".dsh" / "sessions")


def real_claude_projects_dir() -> Path:
    return Path.home() / ".claude" / "projects"


def _refuse_session_store(*paths: Path | str) -> None:
    """Refuse any path that lands in Claude Code's own session store (`~/.claude/projects`),
    whatever the target format and however it is spelled: each path is resolved first, so a
    symlink into the store, a sub-directory of it, and the store itself are all caught."""
    store = real_claude_projects_dir().resolve()
    for path in paths:
        resolved = Path(path).resolve()
        if resolved == store or resolved.is_relative_to(store):
            raise ValueError(
                "writing into Claude Code's own session store "
                f"({real_claude_projects_dir()}) is held until the operator allows it; "
                f"{path} resolves into it. Point it at a scratch directory")


def _private_mkdir(path: Path) -> None:
    """Create `path` and any missing parent with mode 0700. A rebuilt session is decrypted
    conversation text, so no directory made for it is readable by anyone else. Directories
    that already exist keep the mode they have."""
    missing = [d for d in (path, *path.parents) if not d.exists()]
    for d in reversed(missing):
        d.mkdir(mode=0o700, exist_ok=True)
        d.chmod(0o700)   # mkdir's mode is masked by the umask; this sets it exactly


def _private_write(path: Path, text: str) -> None:
    """Write `text` to a new file readable by its owner only (0600)."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as fh:
        fh.write(text)
    path.chmod(0o600)


def _refuse_symlinks(*paths: Path) -> None:
    """A path the writer is about to create or open must not be a symlink: a link is the one
    way a safe-looking `--into` can be made to land somewhere else."""
    for path in paths:
        if path.is_symlink():
            raise ValueError(f"refusing to write through the symlink {path}")


def _slug(cwd: str) -> str:
    """Claude Code's project directory name for a working directory: every character that is
    not a letter or digit becomes a dash."""
    return "".join(c if c.isalnum() else "-" for c in cwd)


def write_claude(
    rebuilt: Rebuilt, *, anchor_sid: str, projects_dir: Path, cwd: str,
    version: str = _CLAUDE_VERSION,
) -> dict[str, Any]:
    """A NEW Claude Code session file, `<projects_dir>/<slug of cwd>/<new id>.jsonl`.

    Written the way Claude Code writes one, with a consistent `parentUuid` chain and one line
    per content block, and with NO ownership recorded: no `session-owner` line and no owner
    or organization stamp of any account. The first line is the import note. This is a copy
    made by osiris, claiming nobody's ownership; whether Claude Code will load it is the
    harness's own decision, and nothing here tries to influence that."""
    rebuilt = _with_writer_folds(rebuilt, "claude")
    sid = str(uuid.uuid4())
    started = rebuilt.first_at or datetime.now(UTC)
    out: list[dict[str, Any]] = []
    prev: str | None = None

    def emit(kind: str, message: dict[str, Any], at: datetime | None, **extra: Any) -> None:
        nonlocal prev
        uid = str(uuid.uuid4())
        out.append({"parentUuid": prev, "isSidechain": False, "type": kind, "message": message,
                    "uuid": uid, "timestamp": (at or started).isoformat().replace("+00:00", "Z"),
                    "userType": "external", "entrypoint": "cli", "cwd": cwd, "sessionId": sid,
                    "version": version, "gitBranch": "HEAD", **extra})
        prev = uid

    marker = Turn("user", [Part("text", marker_text(anchor_sid, rebuilt))], started)
    for t in [marker, *rebuilt.turns]:
        if t.role == "user":
            emit("user", {"role": "user", "content": "\n".join(
                p_.text for p_ in t.parts if p_.kind == "text")}, t.at,
                **({"isCompactSummary": True} if t.summary else {}))
        elif t.role == "tool":
            emit("user", {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": p_.call_id, "content": p_.text,
                 **({"is_error": True} if p_.is_error else {})}
                for p_ in t.parts if p_.kind == "tool_result"]}, t.at)
        else:
            mid = "msg_osiris_" + uuid.uuid4().hex[:24]
            for p_ in t.parts:
                if p_.kind == "text":
                    block: dict[str, Any] = {"type": "text", "text": p_.text}
                elif p_.kind == "reasoning":
                    continue      # counted in the fold: it would need a signature
                else:
                    try:
                        tool_input = json.loads(p_.input) if p_.input else {}
                    except ValueError:
                        tool_input = {"input": p_.input}
                    block = {"type": "tool_use", "id": p_.call_id, "name": p_.name,
                             "input": tool_input}
                emit("assistant", {"model": t.model or "unknown", "id": mid, "type": "message",
                                   "role": "assistant", "content": [block],
                                   "stop_reason": None, "stop_sequence": None}, t.at)
    target = projects_dir / _slug(cwd) / f"{sid}.jsonl"
    _refuse_symlinks(target.parent, target)
    _refuse_session_store(projects_dir, target.parent, target)
    _private_mkdir(target.parent)
    _private_write(target, "".join(json.dumps(d) + "\n" for d in out))
    return {"to": "claude", "session_id": sid, "path": str(target), "turns": len(rebuilt.turns) + 1,
            "lines": len(out), "folded": dict(rebuilt.folded)}


# --- dsh -----------------------------------------------------------------------------------

def write_dsh(
    rebuilt: Rebuilt, *, anchor_sid: str, sessions_dir: Path, cwd: str,
) -> dict[str, Any]:
    """A NEW DSH session: `<sessions_dir>/<slug of cwd>/session-<new uuid>/session.jsonl.zstd`,
    the zstd-compressed event stream DSH keeps, with a session header, a request header naming
    the model, and user/assistant/tool-result events for every turn, opening with the import
    note. DSH has no slot for a model's reasoning outside streaming chunks, so reasoning is
    carried as clearly marked text and counted in the fold report. No owner or account is
    recorded. The event shapes follow a real DSH session; whether DSH itself loads the result
    is its own decision and was not tested against a running DSH."""
    import subprocess

    from src.ingest.harness.dsh import _cwd_to_slug

    counted = _with_writer_folds(rebuilt, "dsh")
    fold = counted.folded
    reasoning = sum(1 for t in rebuilt.turns for p_ in t.parts if p_.kind == "reasoning")
    if reasoning:
        fold["reasoning carried as marked text"] += reasoning
    sid = f"session-{uuid.uuid4()}"
    started = rebuilt.first_at or datetime.now(UTC)
    t_ms = int(started.timestamp() * 1000)
    seq = 0
    events: list[dict[str, Any]] = [{
        "type": "session", "version": 0, "id": sid, "createdAt": t_ms, "cwd": cwd,
        "delegationDepth": 0, "agentPreset": "standard"}]

    def emit(kind: str, at: datetime | None, data: dict[str, Any]) -> None:
        nonlocal seq
        seq += 1
        events.append({"type": kind, "seq": seq,
                       "time": int((at or started).timestamp() * 1000), "data": data})

    model = next((t.model for t in rebuilt.turns if t.role == "assistant" and t.model), None)
    emit("request/header", started, {"header": {"config": {
        "provider": "osiris-import", "model": model or "unknown"}}})
    emit("user/message", started, {
        "content": [{"type": "text", "text": marker_text(anchor_sid, counted)}],
        "source": {"kind": "osiris-import"}, "role": "user", "id": str(uuid.uuid4())})
    turn, step = 0, 0
    for t in rebuilt.turns:
        if t.role == "user":
            turn, step = turn + 1, 0
            body = "\n".join(p_.text for p_ in t.parts if p_.kind == "text")
            emit("user/message", t.at, {
                "content": [{"type": "text", "text": (
                    "[compaction summary of the earlier conversation] " + body
                    if t.summary else body)}],
                "source": {"kind": "user"}, "role": "user", "id": str(uuid.uuid4())})
        elif t.role == "assistant":
            step += 1
            content: list[dict[str, Any]] = []
            for p_ in t.parts:
                if p_.kind == "text":
                    content.append({"type": "text", "text": p_.text})
                elif p_.kind == "reasoning":
                    content.append({"type": "text", "text": "[reasoning] " + p_.text})
                elif p_.kind == "tool_call":
                    content.append({"type": "tool-call", "id": p_.call_id, "name": p_.name,
                                    "arguments": p_.input})
            emit("assistant/message", t.at, {
                "turn": max(turn, 1), "step": step,
                "message": {"role": "assistant", "content": content}})
        else:
            for p_ in t.parts:
                emit("tool/result", t.at, {"turn": max(turn, 1), "step": max(step, 1), "message": {
                    "source": {"kind": "tool", "callId": p_.call_id},
                    "content": [{"type": "tool-result", "toolCallId": p_.call_id,
                                 "content": [{"type": "text", "text": p_.text}]}]}})
    target_dir = sessions_dir / _cwd_to_slug(cwd) / sid
    _refuse_symlinks(target_dir.parent, target_dir)
    _refuse_session_store(sessions_dir, target_dir.parent, target_dir)
    if target_dir.exists():
        raise ValueError(f"refusing to overwrite {target_dir}")
    # Build the session in a private scratch directory beside its final place and rename it
    # into position only when it is whole: a failed compress (zstd missing, a full disk) leaves
    # nothing behind in a real DSH home, never a half-made session DSH might list.
    parent = target_dir.parent
    made_parent = not parent.exists()
    _private_mkdir(parent)
    work = Path(tempfile.mkdtemp(prefix=".partial-", dir=parent))
    try:
        plain = work / "session.jsonl"
        _private_write(plain, "".join(json.dumps(e) + "\n" for e in events))
        subprocess.run(["zstd", "-q", "--rm", "-f", str(plain), "-o", str(plain) + ".zstd"],
                       check=True, timeout=120)
        Path(str(plain) + ".zstd").chmod(0o600)
        work.rename(target_dir)
    except BaseException:
        shutil.rmtree(work, ignore_errors=True)
        if made_parent:
            with contextlib.suppress(OSError):
                parent.rmdir()
        raise
    return {"to": "dsh", "session_id": sid, "path": str(target_dir / "session.jsonl.zstd"),
            "turns": len(rebuilt.turns) + 1, "events": len(events), "folded": dict(fold)}


def _check_target(to: str, into: Path | str, cwd: str | None) -> None:
    """Refuse what cannot or must not be written: an unknown target, a claude target with no
    workspace, and Claude Code's own session store."""
    if to not in ("crush", "claude", "dsh"):
        raise ValueError(f"rebuild target {to!r} is not available yet (crush, claude, dsh)")
    if to in ("claude", "dsh"):
        if cwd is None:
            raise ValueError(f"--to {to} needs --cwd, the workspace the session belongs to")
    _refuse_session_store(into)


async def rebuild_session(
    pool: Any, anchor_sid: str, *, to: str, into: Path | str, title: str | None = None,
    cwd: str | None = None,
) -> dict[str, Any]:
    """Read stored session `anchor_sid` (any account) and write it as a NEW session for `to`
    ('crush', 'claude' or 'dsh'). `into` is the workspace directory (crush) or a Claude projects
    directory (claude, with `cwd` naming the workspace the session belongs to). Writes only
    there; the store and the original session are untouched.

    Claude Code's own session store (`~/.claude/projects`) is refused as a target: writing
    into it waits on the operator's say-so, so the claude writer is pointed at a scratch
    directory until then."""
    from src.ingest.soul_store import SoulStore

    _check_target(to, into, cwd)
    rebuilt = turns_from_claude_lines([
        pair async for pair in SoulStore(pool).stream_lines(anchor_sid)])
    if not rebuilt.turns:
        raise ValueError(f"the store holds no turns for session {anchor_sid!r}")
    # `stream_lines` ends quietly at the last verified line when the stored chain breaks, so
    # compare what it gave with what the store says the session holds, and say so if short
    total = await pool.fetchval(
        "SELECT last_line_idx FROM soul_sessions WHERE harness='claude-code' AND anchor_sid=$1",
        anchor_sid)
    rebuilt.stored_total = int(total) if total is not None else None
    if to == "claude":
        assert cwd is not None
        receipt = write_claude(rebuilt, anchor_sid=anchor_sid, projects_dir=Path(into), cwd=cwd)
    elif to == "dsh":
        assert cwd is not None
        receipt = write_dsh(rebuilt, anchor_sid=anchor_sid, sessions_dir=Path(into), cwd=cwd)
    else:
        receipt = write_crush(rebuilt, anchor_sid=anchor_sid, workspace=Path(into), title=title)
    receipt["truncated"] = (f"stored chain broke at line {rebuilt.source_lines} of "
                            f"{rebuilt.stored_total}" if rebuilt.truncated else None)
    return receipt
