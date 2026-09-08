"""THE HARNESS PROCESS ADAPTER (thread e7f173a6, operator ruling 2026-09-08: "it has to be
abstract, dsh, crush, cursor are all options that degrade gracefully until we build parity
on all features"). DISTINCT from src/ingest/harness/ (that Protocol normalizes a harness's
own TRANSCRIPT into TurnRows -- a read-only concern); this one is PROCESS LIFECYCLE: spawn a
session, resume one, list what's running, send a one-shot reply, stop one. Two different
doors, deliberately never folded into one Protocol under the same word.

FIVE CAPABILITIES, never a sixth without a design pass: spawn (mint a fresh persistent
background session), resume (continue an existing persistent one), reply (the daemon
wake-triage lane -- a one-shot headless turn with a cost receipt, NOT the same as spawn/
resume's `--bg` persistence), list_sessions, stop. Every adapter declares its own
capabilities() statically; every door needing one the active adapter lacks refuses with a
NAMED reason dict -- `{"error": ...}`, the same idiom retire_project/retire_assertion/every
other refusal door in this codebase already uses -- never an exception.

THE CLAUDE ADAPTER IS A THIN WRAP, NOT A REWRITE: it calls trigger.py's own five existing
functions (_spawn_claude_bg, _spawn_claude, _claude_agents_json, _real_kill_pid,
_clear_stale_stopped_record) verbatim -- zero behavior change for the harness this box
already runs on. THE DSH ADAPTER IS THE GRACEFUL-DEGRADE SPECIMEN THE RULING NAMES: it
supports list_sessions only, built on Khnum's DshSessionAdapter.enumerate() (1209db6) --
there is no DSH process-control surface anywhere in this codebase today, confirmed live, so
spawn/resume/reply/stop refuse by name rather than pretend. CRUSH AND CURSOR ARE STUBS:
capabilities() = frozenset() -- declared and refusing everything, nothing built.

SELECTION: `resolve_process_adapter()` reads Settings.osiris_harness_adapter ('auto' by
default; pydantic-settings already reads the OSIRIS_HARNESS_ADAPTER env var into that field
by name -- the operator's own "pin"). 'auto' picks the first available() adapter in
[claude, dsh, crush, cursor] order; naming one explicitly FORCES it even when unavailable,
so every door then refuses by name instead of silently falling through to a different
harness than the one asked for. available() is cached per adapter INSTANCE (Thoth's own
addition, msg 8147): the reply lane must not shell out to `which` on every wake."""
from __future__ import annotations

import asyncio
import shutil
from typing import Any, Protocol

CAPABILITIES = frozenset({"spawn", "resume", "reply", "list_sessions", "stop"})


def _refuse(adapter_name: str, capability: str, declared: frozenset[str]) -> dict[str, Any]:
    return {"error": f"adapter {adapter_name!r} does not support {capability!r} — "
                     f"declared: {sorted(declared) or 'none'}"}


class ProcessAdapter(Protocol):
    """One harness's process lifecycle -- spawn/resume/reply/list_sessions/stop, plus the
    capability declaration every door checks before calling any of the five. Every method
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


def claude_pty_argv(model: str | None) -> list[str]:
    """THE ONE ARGV BUILDER (Thoth's own addition, msg 8147: "no second spawn path
    survives") -- trigger.py's own PTY-broker fallback lane and cli.py's `_cmd_launch_pty`
    each built an identical `["claude", *(["--model", m] if m else [])]` independently
    before this. Neither lane execs this argv directly (both hand it to osiris-manager's
    pty_spawn over the claim socket), so it stays a bare builder rather than a ProcessAdapter
    method -- but it is now the ONLY place that decides what a `claude` PTY argv looks like."""
    return ["claude", *(["--model", model] if model else [])]


class ClaudeAdapter:
    """Today's behaviour, wrapped -- every method below calls trigger.py's own existing
    function verbatim. No new subprocess-invocation logic lives here."""

    name = "claude"
    _available: bool | None = None

    def capabilities(self) -> frozenset[str]:
        return CAPABILITIES

    def available(self) -> bool:
        # CACHED PER INSTANCE (Thoth's own addition, msg 8147): the reply lane fires on
        # every wake -- shelling out to `which` that often is real, avoidable syscall cost
        # for a fact that cannot change within one process's life (the binary a box has
        # installed does not move mid-run).
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

        # PRE-EMPTIVE STALE-RECORD CLEAR (mirrors resume_seat/_cmd_resume_harness exactly,
        # Thoth dispatch 7543 item 1): a leftover harness "stopped" record turns `--bg
        # --resume` into a silent copy rather than a genuine continuation.
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
        # session_id only, no pid on hand: the harness-aware door only -- NEVER fall
        # through to _real_kill_pid's own SIGTERM branch with a fabricated pid.
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


_DSH_CAPABILITIES = frozenset({"list_sessions"})


class DshAdapter:
    """THE GRACEFUL-DEGRADE SPECIMEN the operator's ruling names by example: list_sessions
    only, built on Khnum's DshSessionAdapter.enumerate() (1209db6) -- confirmed live, this
    codebase has no DSH process-control surface at all, so spawn/resume/reply/stop refuse
    by name rather than pretend a capability that doesn't exist."""

    name = "dsh"

    def capabilities(self) -> frozenset[str]:
        return _DSH_CAPABILITIES

    def available(self) -> bool:
        from src.ingest.harness.dsh import _dsh_sessions
        return _dsh_sessions().is_dir()

    async def spawn(self, **_kwargs: Any) -> dict[str, Any]:
        return _refuse(self.name, "spawn", self.capabilities())

    async def resume(self, **_kwargs: Any) -> dict[str, Any]:
        return _refuse(self.name, "resume", self.capabilities())

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


class _StubAdapter:
    """Crush and Cursor (thread e7f173a6): declared, nothing built. capabilities() = the
    empty set -- every door refuses by name until a real build lands, matching the
    operator's own "degrade gracefully" framing rather than silently no-op'ing."""

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


class CrushAdapter(_StubAdapter):
    name = "crush"


class CursorAdapter(_StubAdapter):
    name = "cursor"


_ADAPTER_CLASSES: dict[str, type[ProcessAdapter]] = {
    "claude": ClaudeAdapter, "dsh": DshAdapter, "crush": CrushAdapter,
    "cursor": CursorAdapter,
}
_AUTO_ORDER = ("claude", "dsh", "crush", "cursor")


def resolve_process_adapter(settings: Any = None) -> ProcessAdapter:
    """'auto' (default): the first available() adapter in [claude, dsh, crush, cursor]
    order. Naming one explicitly FORCES it even when unavailable -- every door then
    refuses by name (via each adapter's own capability check) instead of silently falling
    through to a harness nobody asked for. Unknown name: same treatment as 'auto' finding
    nothing -- falls to ClaudeAdapter, this box's own long-standing default, rather than
    raising on a typo'd setting."""
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
