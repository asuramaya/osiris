"""Process-lifecycle adapter for coding-agent harnesses: spawn, resume, list, one-shot
reply, and stop. This is distinct from src/ingest/harness/, which normalizes a harness's
own transcript into TurnRows as a read-only concern; this module owns process lifecycle
instead. The two stay deliberately separate rather than being folded into one Protocol.

Six capabilities are supported, and none should be added without a design pass: spawn
(start a fresh persistent background session), resume (continue an existing persistent
session), reply (the one-shot headless turn used by automated wake/triage handling, with
a cost result attached; this is not the same as spawn/resume's `--bg` persistence),
list_sessions, stop, and materialize (write a harness's native transcript file back to
disk from the stored copy alone). Every adapter declares its own capabilities()
statically; whenever the active adapter lacks a capability a caller needs, it refuses
with a named reason dict, `{"error": ...}`, the same idiom other refusal paths in this
codebase already use, rather than raising an exception.

Materialize is the one capability that touches the graph database, unlike the other five
(which are pure subprocess/file IO): it takes an explicit `pool: asyncpg.Pool` parameter,
the same dependency-injection style trigger.py's own resume-materialization functions
already use for their own callers (never a fetched global singleton, which would tie
this process-lifecycle module to one specific daemon's pool lifecycle). ClaudeAdapter's
materialize is a thin wrap around `SoulStore.rematerialize_to_disk`, the same function the
MCP `rematerialize` tool and the `osiris rematerialize` CLI command already call directly;
this adapter method exists so a caller going through the harness-abstraction layer
(rather than assuming Claude) can ask whether a given harness can materialize a session,
the same way it already asks about spawn/resume/stop.

The dsh and crush adapters refuse materialize by name: both have a read-only harness
adapter (src/ingest/harness/) that can discover their own session files, but neither has
a verbatim store to materialize from. SoulStore.backfill() is scoped to claude-code only
(Crush is SQLite-backed with no line-oriented raw concept, which is out of scope here on
purpose), so there is nothing in soul_lines for either harness yet. Refusing honestly now,
until a verbatim-ingest path exists for either harness, is the graceful degradation this
design calls for rather than a stub that pretends to work.

The Claude adapter is a thin wrap, not a rewrite: every method calls one of trigger.py's
own five existing functions (_spawn_claude_bg, _spawn_claude, _claude_agents_json,
_real_kill_pid, _clear_stale_stopped_record) verbatim, with zero behavior change for the
harness already running in production.

The dsh adapter is the clearest example of graceful degradation: it supports
list_sessions only, built on DshSessionAdapter.enumerate(). There is no dsh
process-control surface anywhere in this codebase today (confirmed live), so
spawn/resume/reply/stop refuse by name rather than pretend to support them.

The crush adapter: spawn (via `crush run <prompt>`, fire-and-forget, the same discipline
as `_spawn_claude`) and list_sessions (via `crush session list --json`) are real;
resume/reply/stop refuse by name. Crush's CLI does support continuing a session
(`--session`/`--continue`), but this work was scoped to exactly these two capabilities,
matching the dsh adapter's narrower shape rather than building the rest speculatively.

A live-measured quirk, not documented anywhere (found by running the real binary):
`crush session list --json --cwd <dir>` silently returns `[]` regardless of what is on
disk. The `--cwd` flag is a no-op for this subcommand (confirmed: a project with a real,
non-empty crush.db returned `[]` via `--cwd`, then the correct row via the same command
run with the subprocess's own OS-level cwd set to that directory instead). So
`list_sessions` sets the subprocess's own `cwd=`, exactly like `_claude_agents_json`
already does for a different reason (repo-scoped listing), never the `--cwd` flag, which
this adapter never passes to `session list` at all. Cursor stays a stub:
capabilities() = frozenset(), nothing built.

Selection: `resolve_process_adapter()` reads Settings.osiris_harness_adapter ('auto' by
default; pydantic-settings already reads the OSIRIS_HARNESS_ADAPTER env var into that
field by name). 'auto' picks the first available() adapter in [claude, dsh, crush,
cursor] order; naming one explicitly forces it even when unavailable, so every
capability then refuses by name instead of silently falling through to a different
harness than the one asked for. available() is cached per adapter instance so the reply
lane does not shell out to `which` on every wake."""
from __future__ import annotations

import asyncio
import json
import shutil
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    import asyncpg

CAPABILITIES = frozenset(
    {"spawn", "resume", "reply", "list_sessions", "stop", "materialize"})


def _refuse(adapter_name: str, capability: str, declared: frozenset[str]) -> dict[str, Any]:
    return {"error": f"adapter {adapter_name!r} does not support {capability!r} — "
                     f"declared: {sorted(declared) or 'none'}"}


class ProcessAdapter(Protocol):
    """One harness's process lifecycle: spawn, resume, reply, list_sessions, stop, plus the
    capability declaration every caller checks before calling any of the five. Every method
    below is async (real subprocess or daemon IO); `capabilities()`/`available()` are sync
    (a static declaration and a cheap on-box presence check, respectively)."""

    name: str

    def capabilities(self) -> frozenset[str]: ...

    def available(self) -> bool: ...

    async def spawn(
        self, *, repo: str, prompt: str | None = None, job_dir: str | None = None,
        model: str | None = None, allowed_tools: str | None = None, name: str | None = None,
    ) -> dict[str, Any]: ...

    async def resume(
        self, *, repo: str, session_id: str, prompt: str | None = None,
        model: str | None = None, allowed_tools: str | None = None,
    ) -> dict[str, Any]: ...

    async def reply(
        self, *, repo: str, prompt: str, job_dir: str | None = None,
        resume_session: str | None = None, model: str | None = None,
        allowed_tools: str | None = None, spawn_parent: str | None = None,
    ) -> dict[str, Any]: ...

    async def list_sessions(
        self, *, cwd: str | None = None, include_completed: bool = False,
    ) -> list[dict[str, Any]]: ...

    async def stop(
        self, *, session_id: str | None = None, pid: int | None = None,
    ) -> dict[str, Any]: ...

    async def materialize(
        self, *, pool: asyncpg.Pool, anchor_sid: str, dest: str | None = None,
        force: bool = False,
    ) -> dict[str, Any]: ...


def claude_pty_argv(model: str | None) -> list[str]:
    """The single argv builder for a `claude` PTY session. trigger.py's own PTY-broker
    fallback lane and cli.py's `_cmd_launch_pty` each built an identical
    `["claude", *(["--model", m] if m else [])]` independently before this existed. Neither
    lane execs this argv directly (both hand it to osiris-manager's pty_spawn over the claim
    socket), so it stays a bare builder rather than a ProcessAdapter method, but it is now
    the only place that decides what a `claude` PTY argv looks like."""
    return ["claude", *(["--model", model] if model else [])]


class ClaudeAdapter:
    """Wraps today's existing behavior: every method below calls trigger.py's own existing
    function verbatim. No new subprocess-invocation logic lives here."""

    name = "claude"
    _available: bool | None = None

    def capabilities(self) -> frozenset[str]:
        return CAPABILITIES

    def available(self) -> bool:
        # Cached per instance: the reply lane fires on every wake, and shelling out to
        # `which` that often is real, avoidable syscall cost for a fact that cannot
        # change within one process's life (the installed binary does not move mid-run).
        if self._available is None:
            from src.config.settings import get_settings
            self._available = shutil.which(get_settings().osiris_claude_binary) is not None
        return self._available

    async def spawn(
        self, *, repo: str, prompt: str | None = None, job_dir: str | None = None,
        model: str | None = None, allowed_tools: str | None = None, name: str | None = None,
    ) -> dict[str, Any]:
        from src.orchestrator.trigger import _spawn_claude_bg

        await _spawn_claude_bg(repo, name=name, model=model, prompt=prompt,
                               allowed_tools=allowed_tools)
        return {"spawned": True, "repo": repo, "name": name}

    async def resume(
        self, *, repo: str, session_id: str, prompt: str | None = None,
        model: str | None = None, allowed_tools: str | None = None,
    ) -> dict[str, Any]:
        from src.orchestrator.trigger import _clear_stale_stopped_record, _spawn_claude_bg

        # Pre-emptive stale-record clear (mirrors resume_seat/_cmd_resume_harness
        # exactly): a leftover harness "stopped" record turns `--bg --resume` into a
        # silent copy rather than a genuine continuation.
        await _clear_stale_stopped_record(session_id)
        await _spawn_claude_bg(repo, model=model, prompt=prompt, allowed_tools=allowed_tools,
                               resume_session=session_id)
        return {"resumed": True, "repo": repo, "session_id": session_id}

    async def reply(
        self, *, repo: str, prompt: str, job_dir: str | None = None,
        resume_session: str | None = None, model: str | None = None,
        allowed_tools: str | None = None, spawn_parent: str | None = None,
    ) -> dict[str, Any]:
        from src.orchestrator.trigger import _spawn_claude

        await _spawn_claude(repo, prompt, job_dir=job_dir, resume_session=resume_session,
                            model=model, allowed_tools=allowed_tools,
                            spawn_parent=spawn_parent)
        return {"replied": True, "repo": repo, "job_dir": job_dir,
                "resume_session": resume_session}

    async def list_sessions(
        self, *, cwd: str | None = None, include_completed: bool = False,
    ) -> list[dict[str, Any]]:
        from src.orchestrator.trigger import _claude_agents_json

        return await _claude_agents_json(cwd=cwd, include_completed=include_completed)

    async def stop(
        self, *, session_id: str | None = None, pid: int | None = None,
    ) -> dict[str, Any]:
        from src.orchestrator.trigger import _clear_stale_stopped_record, _real_kill_pid

        if session_id is None and pid is None:
            return {"error": "adapter 'claude' stop needs session_id or pid"}
        if pid is not None:
            await _real_kill_pid(pid, session_id)
            return {"stopped": True, "session_id": session_id, "pid": pid}
        # session_id only, no pid on hand: use the harness-aware stop path only.
        # Never fall through to _real_kill_pid's own SIGTERM branch with a
        # fabricated pid.
        assert session_id is not None  # guaranteed by the guard above (pid is None here)
        try:
            proc = await asyncio.create_subprocess_exec(
                "claude", "stop", session_id,
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
            ok = await proc.wait() == 0
        except OSError as exc:
            return {"error": f"adapter 'claude' stop failed to exec: {exc} — and no pid "
                             "was given for a SIGTERM fallback"}
        if ok:
            await _clear_stale_stopped_record(session_id)
            return {"stopped": True, "session_id": session_id}
        return {"error": f"claude stop {session_id!r} exited non-zero and no pid was given "
                         "for a SIGTERM fallback"}

    async def materialize(
        self, *, pool: asyncpg.Pool, anchor_sid: str, dest: str | None = None,
        force: bool = False,
    ) -> dict[str, Any]:
        from src.ingest.soul_store import SoulStore

        return await SoulStore(pool).rematerialize_to_disk(
            anchor_sid, dest=dest, force=force)


_DSH_CAPABILITIES = frozenset({"list_sessions"})
_DSH_CAPABILITIES_WITH_PROFILE = frozenset({"list_sessions", "spawn", "resume"})


class DshAdapter:
    """list_sessions is always real, built on DshSessionAdapter.enumerate(). spawn/resume
    are the clearest example of graceful degradation in this module, but are no longer
    an unconditional refusal: dsh's own launcher forwards everything after its own flags
    verbatim to a user-configured profile app. There is no universal dsh spawn command
    the way `claude --bg` or `crush run` are, only whatever profile an operator has
    actually set up for headless task execution. `osiris_dsh_profile` empty (the
    default) keeps refusing both by name, honestly, exactly as before; set it and
    spawn/resume become real, using `osiris_dsh_resume_flag` verbatim, never a guessed
    or hardcoded profile name or resume flag."""

    name = "dsh"

    def capabilities(self) -> frozenset[str]:
        # Dynamic, unlike every sibling adapter's static declaration, because whether
        # spawn/resume are real here depends on operator configuration, not on code
        # that exists unconditionally. Declaring them always would be exactly the kind
        # of false capability claim this module's design is meant to avoid.
        from src.config.settings import get_settings

        if get_settings().osiris_dsh_profile:
            return _DSH_CAPABILITIES_WITH_PROFILE
        return _DSH_CAPABILITIES

    def available(self) -> bool:
        from src.ingest.harness.dsh import _dsh_sessions
        return _dsh_sessions().is_dir()

    async def spawn(
        self, *, repo: str, prompt: str | None = None, job_dir: str | None = None,
        model: str | None = None, allowed_tools: str | None = None, name: str | None = None,
    ) -> dict[str, Any]:
        from src.config.settings import get_settings

        profile = get_settings().osiris_dsh_profile
        if not profile:
            return _refuse(self.name, "spawn", self.capabilities())
        if not prompt:
            return {"error": "adapter 'dsh' spawn needs a prompt"}
        cmd = ["dsh", "--profile", profile, prompt]
        # Fire-and-forget, same discipline as CrushAdapter's own spawn: an arq timeout
        # that once awaited a live billing subprocess wedged the worker. This confirms
        # only that the command was issued, never that it completed.
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd, cwd=repo, stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL)
        except OSError as exc:
            return {"error": f"adapter 'dsh' spawn failed to exec: {exc}"}
        return {"spawned": True, "repo": repo, "pid": proc.pid, "profile": profile}

    async def resume(
        self, *, repo: str, session_id: str, prompt: str | None = None,
        model: str | None = None, allowed_tools: str | None = None,
    ) -> dict[str, Any]:
        from src.config.settings import get_settings

        profile = get_settings().osiris_dsh_profile
        if not profile:
            return _refuse(self.name, "resume", self.capabilities())
        resume_flag = get_settings().osiris_dsh_resume_flag
        cmd = ["dsh", "--profile", profile, resume_flag, session_id]
        if prompt:
            cmd.append(prompt)
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd, cwd=repo, stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL)
        except OSError as exc:
            return {"error": f"adapter 'dsh' resume failed to exec: {exc}"}
        return {"resumed": True, "repo": repo, "session_id": session_id,
                "pid": proc.pid, "profile": profile}

    async def reply(self, **_kwargs: Any) -> dict[str, Any]:
        return _refuse(self.name, "reply", self.capabilities())

    async def list_sessions(
        self, *, cwd: str | None = None, include_completed: bool = False,
    ) -> list[dict[str, Any]]:
        from src.ingest.harness.dsh import DshSessionAdapter

        return [
            {"id": loc.session_id, "anchor_sid": loc.anchor_sid, "cwd": loc.cwd,
             "project": loc.project, "harness": "dsh", "anchored": loc.anchored}
            for loc in DshSessionAdapter().enumerate()
        ]

    async def stop(self, **_kwargs: Any) -> dict[str, Any]:
        return _refuse(self.name, "stop", self.capabilities())

    async def materialize(self, **_kwargs: Any) -> dict[str, Any]:
        return _refuse(self.name, "materialize", self.capabilities())


class _StubAdapter:
    """Crush and Cursor: declared, nothing built. capabilities() is the empty set, so
    every caller refuses by name until a real implementation lands, rather than
    silently no-op'ing."""

    name = "stub"

    def capabilities(self) -> frozenset[str]:
        return frozenset()

    def available(self) -> bool:
        return False

    async def spawn(self, **_kwargs: Any) -> dict[str, Any]:
        return _refuse(self.name, "spawn", self.capabilities())

    async def resume(self, **_kwargs: Any) -> dict[str, Any]:
        return _refuse(self.name, "resume", self.capabilities())

    async def reply(self, **_kwargs: Any) -> dict[str, Any]:
        return _refuse(self.name, "reply", self.capabilities())

    async def list_sessions(
        self, *, cwd: str | None = None, include_completed: bool = False,
    ) -> list[dict[str, Any]]:
        return []

    async def stop(self, **_kwargs: Any) -> dict[str, Any]:
        return _refuse(self.name, "stop", self.capabilities())

    async def materialize(self, **_kwargs: Any) -> dict[str, Any]:
        return _refuse(self.name, "materialize", self.capabilities())


_CRUSH_CAPABILITIES = frozenset({"spawn", "list_sessions", "materialize"})


class CrushAdapter:
    """Real spawn and list_sessions on crush's own CLI; resume/reply/stop refuse by
    name, matching DshAdapter's own narrower-than-full-parity shape."""

    name = "crush"
    _available: bool | None = None

    def capabilities(self) -> frozenset[str]:
        return _CRUSH_CAPABILITIES

    def available(self) -> bool:
        if self._available is None:
            from src.config.settings import get_settings
            self._available = shutil.which(get_settings().osiris_crush_binary) is not None
        return self._available

    async def spawn(
        self, *, repo: str, prompt: str | None = None, job_dir: str | None = None,
        model: str | None = None, allowed_tools: str | None = None, name: str | None = None,
    ) -> dict[str, Any]:
        if not prompt:
            return {"error": "adapter 'crush' spawn needs a prompt"}
        cmd = ["crush", "run"]
        if model:
            cmd += ["--model", model]
        cmd.append(prompt)
        # Fire-and-forget, same discipline as trigger.py's own _spawn_claude: an arq
        # timeout that once awaited a live billing subprocess wedged the worker. This
        # confirms only that the command was issued, never that it completed.
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd, cwd=repo, stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL)
        except OSError as exc:
            return {"error": f"adapter 'crush' spawn failed to exec: {exc}"}
        return {"spawned": True, "repo": repo, "pid": proc.pid}

    async def resume(self, **_kwargs: Any) -> dict[str, Any]:
        return _refuse(self.name, "resume", self.capabilities())

    async def reply(self, **_kwargs: Any) -> dict[str, Any]:
        return _refuse(self.name, "reply", self.capabilities())

    async def list_sessions(
        self, *, cwd: str | None = None, include_completed: bool = False,
    ) -> list[dict[str, Any]]:
        if cwd is None:
            return []
        # cwd= on the subprocess, never a --cwd flag (see this module's own docstring:
        # the flag is a live-confirmed no-op for `session list`, silently returning []
        # instead of the real on-disk rows).
        try:
            proc = await asyncio.create_subprocess_exec(
                "crush", "session", "list", "--json", cwd=cwd,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
            out, _stderr = await proc.communicate()
        except OSError:
            return []
        try:
            rows = json.loads(out.decode() or "[]")
        except (json.JSONDecodeError, UnicodeDecodeError):
            return []
        return rows if isinstance(rows, list) else []

    async def stop(self, **_kwargs: Any) -> dict[str, Any]:
        return _refuse(self.name, "stop", self.capabilities())

    async def materialize(
        self, *, pool: asyncpg.Pool, anchor_sid: str, dest: str | None = None,
        force: bool = False,
    ) -> dict[str, Any]:
        """Crush's own materialize implementation, filling the gap that was left refusing
        until a verbatim-ingest reader existed for crush sessions (ingest_crush_session
        is that reader). Unlike ClaudeAdapter's byte-exact file rewrite, there is no
        single crush.db a session "belongs" to reconstruct: one db holds many sessions,
        and SQLite files carry no meaningful byte-exact target anyway (page
        layout/vacuum state, not content). `dest` names the target crush.db to write
        the session's `sessions`/`messages` rows into, created fresh (real crush
        schema) if it doesn't exist yet, never a rewrite of whatever db the session
        originally lived in. Reads the canonical rows soul_store stored
        (`SoulStore._all_raw_lines`, harness='crush') and `json.loads`s each back into
        a row dict, the exact inverse of `_crush_line_bytes`'s `json.dumps`.

        `force=False` (default) refuses to overwrite a session id already present at
        `dest`; `force=True` deletes both its `messages` rows and its `sessions` row
        explicitly, never relying on `ON DELETE CASCADE` alone: SQLite does not
        enforce foreign keys on a connection unless `PRAGMA foreign_keys = ON` is set
        (confirmed live: the FK clause is schema documentation only without it), so a
        cascade-only delete would silently orphan the old messages and collide on
        their still-live `id` primary keys on the very next insert. A real `crush`
        install can open the written file and read the session back; this is a
        genuine restore, not an export in some other shape."""
        from src.ingest.soul_store import SoulStore

        if dest is None:
            return {"error": "adapter 'crush' materialize needs dest (a target crush.db "
                             "path)"}
        store = SoulStore(pool)
        lines = await store._all_raw_lines("crush", anchor_sid)
        if lines is None:
            return {"error": f"no soul_lines ingested for {anchor_sid!r} — nothing to "
                             "materialize"}
        rows = [json.loads(line) for line in lines]
        session_id = rows[0]["session_id"] if rows else None
        if not session_id:
            return {"error": f"{anchor_sid!r}'s stored rows carry no session_id — cannot "
                             "materialize"}

        def _write() -> dict[str, Any]:
            import sqlite3
            from pathlib import Path

            target = Path(dest)
            target.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(str(target))
            try:
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS sessions ("
                    " id TEXT PRIMARY KEY, parent_session_id TEXT, title TEXT NOT NULL,"
                    " message_count INTEGER NOT NULL DEFAULT 0,"
                    " prompt_tokens INTEGER NOT NULL DEFAULT 0,"
                    " completion_tokens INTEGER NOT NULL DEFAULT 0,"
                    " cost REAL NOT NULL DEFAULT 0.0,"
                    " updated_at INTEGER NOT NULL, created_at INTEGER NOT NULL,"
                    " summary_message_id TEXT, todos TEXT)")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS messages ("
                    " id TEXT PRIMARY KEY, session_id TEXT NOT NULL, role TEXT NOT NULL,"
                    " parts TEXT NOT NULL DEFAULT '[]', model TEXT,"
                    " created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,"
                    " finished_at INTEGER, provider TEXT,"
                    " is_summary_message INTEGER DEFAULT 0 NOT NULL,"
                    " FOREIGN KEY (session_id) REFERENCES sessions (id) "
                    "   ON DELETE CASCADE)")
                existing = conn.execute(
                    "SELECT 1 FROM sessions WHERE id=?", (session_id,)).fetchone()
                if existing is not None:
                    if not force:
                        conn.close()
                        return {"error": f"refused — session {session_id!r} already "
                                         f"exists at {dest} — pass force=True to "
                                         "overwrite"}
                    conn.execute(
                        "DELETE FROM messages WHERE session_id=?", (session_id,))
                    conn.execute("DELETE FROM sessions WHERE id=?", (session_id,))
                created_ats = [r["created_at"] for r in rows if r.get("created_at")]
                updated_ats = [r["updated_at"] for r in rows if r.get("updated_at")]
                conn.execute(
                    "INSERT INTO sessions (id, title, message_count, updated_at, "
                    " created_at) VALUES (?,?,?,?,?)",
                    (session_id, f"materialized from soul_lines ({anchor_sid})",
                     len(rows), max(updated_ats) if updated_ats else 0,
                     min(created_ats) if created_ats else 0))
                conn.executemany(
                    "INSERT INTO messages (id, session_id, role, parts, model, "
                    " provider, created_at, updated_at, finished_at, "
                    " is_summary_message) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    [(r["id"], r["session_id"], r["role"], r["parts"], r.get("model"),
                      r.get("provider"), r.get("created_at"), r.get("updated_at"),
                      r.get("finished_at"), int(r.get("is_summary_message") or 0))
                     for r in rows])
                conn.commit()
            finally:
                conn.close()
            return {"written": dest, "session_id": session_id, "messages": len(rows)}

        return await asyncio.to_thread(_write)


class CursorAdapter(_StubAdapter):
    name = "cursor"


_ADAPTER_CLASSES: dict[str, type[ProcessAdapter]] = {
    "claude": ClaudeAdapter, "dsh": DshAdapter, "crush": CrushAdapter,
    "cursor": CursorAdapter,
}
_AUTO_ORDER = ("claude", "dsh", "crush", "cursor")


def resolve_process_adapter(settings: Any = None) -> ProcessAdapter:
    """'auto' (default): the first available() adapter in [claude, dsh, crush, cursor]
    order. Naming one explicitly forces it even when unavailable; every capability then
    refuses by name (via each adapter's own capability check) instead of silently
    falling through to a harness nobody asked for. An unknown name gets the same
    treatment as 'auto' finding nothing: it falls back to ClaudeAdapter, this system's
    own long-standing default, rather than raising on a typo'd setting."""
    from src.config.settings import get_settings

    st = settings or get_settings()
    pin = (getattr(st, "osiris_harness_adapter", "auto") or "auto").strip().lower()
    if pin != "auto" and pin in _ADAPTER_CLASSES:
        return _ADAPTER_CLASSES[pin]()
    for candidate in _AUTO_ORDER:
        adapter = _ADAPTER_CLASSES[candidate]()
        if adapter.available():
            return adapter
    return ClaudeAdapter()
