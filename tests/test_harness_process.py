"""The harness process adapter (thread e7f173a6): spawn/resume/reply/list_sessions/stop
across claude/dsh/crush/cursor, capability refusal by name, and selection by pin or auto.
"""
from __future__ import annotations

from typing import Any

import pytest
from src.config.settings import Settings
from src.orchestrator.harness_process import (
    ClaudeAdapter,
    CrushAdapter,
    CursorAdapter,
    DshAdapter,
    claude_pty_argv,
    resolve_process_adapter,
)

# ═══ ClaudeAdapter: a thin wrap, zero behavior change ═════════════════════════════════════

async def test_claude_spawn_calls_spawn_claude_bg(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []

    async def _fake_spawn_bg(repo: str, **kwargs: Any) -> None:
        calls.append({"repo": repo, **kwargs})

    monkeypatch.setattr("src.orchestrator.trigger._spawn_claude_bg", _fake_spawn_bg)

    out = await ClaudeAdapter().spawn(repo="/tmp/r", prompt="hi", model="sonnet")

    assert out["spawned"] is True
    assert calls == [{"repo": "/tmp/r", "name": None, "model": "sonnet", "prompt": "hi",
                      "allowed_tools": None}]


async def test_claude_resume_clears_stale_record_then_spawns_bg(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    order: list[str] = []

    async def _fake_clear(job_dir_key: str) -> bool:
        order.append(f"clear:{job_dir_key}")
        return True

    async def _fake_spawn_bg(repo: str, **kwargs: Any) -> None:
        order.append(f"spawn:{kwargs.get('resume_session')}")

    monkeypatch.setattr("src.orchestrator.trigger._clear_stale_stopped_record", _fake_clear)
    monkeypatch.setattr("src.orchestrator.trigger._spawn_claude_bg", _fake_spawn_bg)

    out = await ClaudeAdapter().resume(repo="/tmp/r", session_id="sess123")

    assert out == {"resumed": True, "repo": "/tmp/r", "session_id": "sess123"}
    assert order == ["clear:sess123", "spawn:sess123"]  # clear BEFORE spawn, always


async def test_claude_reply_calls_spawn_claude(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []

    async def _fake_spawn(repo: str, prompt: str, **kwargs: Any) -> None:
        calls.append({"repo": repo, "prompt": prompt, **kwargs})

    monkeypatch.setattr("src.orchestrator.trigger._spawn_claude", _fake_spawn)

    out = await ClaudeAdapter().reply(repo="/tmp/r", prompt="wake up")

    assert out["replied"] is True
    assert calls[0]["repo"] == "/tmp/r"
    assert calls[0]["prompt"] == "wake up"


async def test_claude_list_sessions_calls_claude_agents_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_agents(**kwargs: Any) -> list[dict[str, Any]]:
        return [{"id": "s1", **kwargs}]

    monkeypatch.setattr("src.orchestrator.trigger._claude_agents_json", _fake_agents)

    out = await ClaudeAdapter().list_sessions(cwd="/tmp/r")

    assert out == [{"id": "s1", "cwd": "/tmp/r", "include_completed": False}]


async def test_claude_stop_with_pid_calls_real_kill_pid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[int, str | None]] = []

    async def _fake_kill(pid: int, job_dir_key: str | None) -> None:
        calls.append((pid, job_dir_key))

    monkeypatch.setattr("src.orchestrator.trigger._real_kill_pid", _fake_kill)

    out = await ClaudeAdapter().stop(session_id="sess1", pid=4242)

    assert out == {"stopped": True, "session_id": "sess1", "pid": 4242}
    assert calls == [(4242, "sess1")]


async def test_claude_stop_with_no_session_and_no_pid_refuses() -> None:
    out = await ClaudeAdapter().stop()
    assert "error" in out


async def test_claude_stop_with_session_only_and_a_missing_binary_reports_an_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No pid on hand and the binary itself is missing: nothing to fall back to (never a
    fabricated pid), so this reports an error rather than raising."""
    async def _fake_exec(*argv: str, **kw: Any) -> Any:
        raise FileNotFoundError(2, "No such file or directory", "claude")

    monkeypatch.setattr("src.orchestrator.harness_process.asyncio.create_subprocess_exec",
                        _fake_exec)

    out = await ClaudeAdapter().stop(session_id="sess1")

    assert "error" in out


async def test_claude_capabilities_includes_materialize() -> None:
    assert "materialize" in ClaudeAdapter().capabilities()


async def test_claude_materialize_calls_rematerialize_to_disk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    class _FakeSoulStore:
        def __init__(self, pool: Any) -> None:
            calls.append({"pool": pool})

        async def rematerialize_to_disk(
            self, anchor_sid: str, *, dest: Any = None, force: bool = False,
        ) -> dict[str, Any]:
            calls.append({"anchor_sid": anchor_sid, "dest": dest, "force": force})
            return {"written": dest or "/default/path", "lines": 3, "sha256": "abc"}

    monkeypatch.setattr("src.ingest.soul_store.SoulStore", _FakeSoulStore)

    out = await ClaudeAdapter().materialize(
        pool="fake-pool", anchor_sid="deadbeef", dest="/tmp/out.jsonl")

    assert out == {"written": "/tmp/out.jsonl", "lines": 3, "sha256": "abc"}
    assert calls == [
        {"pool": "fake-pool"},
        {"anchor_sid": "deadbeef", "dest": "/tmp/out.jsonl", "force": False},
    ]


async def test_claude_available_is_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []

    def _fake_which(name: str) -> str | None:
        calls.append(name)
        return "/usr/bin/claude"

    monkeypatch.setattr("src.orchestrator.harness_process.shutil.which", _fake_which)
    adapter = ClaudeAdapter()

    assert adapter.available() is True
    assert adapter.available() is True
    assert len(calls) == 1  # cached after the first call


# ═══ DshAdapter: the graceful-degrade specimen ═══════════════════════════════════════════

async def test_dsh_capabilities_is_list_sessions_only() -> None:
    assert DshAdapter().capabilities() == frozenset({"list_sessions"})
    assert "materialize" not in DshAdapter().capabilities()


async def test_dsh_spawn_resume_reply_stop_materialize_all_refuse_by_name() -> None:
    dsh = DshAdapter()
    coros = (dsh.spawn(), dsh.resume(), dsh.reply(), dsh.stop(),
             dsh.materialize(pool=None, anchor_sid="x"))
    for coro in coros:
        out = await coro
        assert out["error"].startswith("adapter 'dsh' does not support")


async def test_dsh_list_sessions_wraps_enumerate(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.ingest.harness import SessionLocator

    def _fake_enumerate(self: Any, *, root: Any = None) -> Any:
        yield SessionLocator(anchor_sid="abcd1234", session_id="session-x", harness="dsh",
                             source_path="/tmp/x", cwd="/tmp/proj", project="proj")

    monkeypatch.setattr("src.ingest.harness.dsh.DshSessionAdapter.enumerate", _fake_enumerate)

    out = await DshAdapter().list_sessions()

    assert out == [{"id": "session-x", "anchor_sid": "abcd1234", "cwd": "/tmp/proj",
                    "project": "proj", "harness": "dsh", "anchored": True}]


# ═══ CrushAdapter: real spawn + list_sessions, everything else refuses ════════════════════

async def test_crush_capabilities_is_spawn_and_list_sessions_only() -> None:
    assert CrushAdapter().capabilities() == frozenset({"spawn", "list_sessions"})
    assert "materialize" not in CrushAdapter().capabilities()


async def test_crush_resume_reply_stop_materialize_all_refuse_by_name() -> None:
    crush = CrushAdapter()
    coros = (crush.resume(), crush.reply(), crush.stop(),
             crush.materialize(pool=None, anchor_sid="x"))
    for coro in coros:
        out = await coro
        assert out["error"].startswith("adapter 'crush' does not support")


async def test_crush_spawn_needs_a_prompt() -> None:
    out = await CrushAdapter().spawn(repo="/tmp/r")
    assert "error" in out


async def test_crush_spawn_calls_crush_run_with_the_prompt_as_a_trailing_positional(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    class _FakeProc:
        pid = 4242

    async def _fake_exec(*argv: str, **kwargs: Any) -> _FakeProc:
        calls.append({"argv": list(argv), **kwargs})
        return _FakeProc()

    monkeypatch.setattr("src.orchestrator.harness_process.asyncio.create_subprocess_exec",
                        _fake_exec)

    out = await CrushAdapter().spawn(repo="/tmp/r", prompt="do the thing", model="glm-4.6")

    assert out == {"spawned": True, "repo": "/tmp/r", "pid": 4242}
    assert calls == [{"argv": ["crush", "run", "--model", "glm-4.6", "do the thing"],
                      "cwd": "/tmp/r", "stdout": -3, "stderr": -3}]


async def test_crush_spawn_tolerates_a_missing_binary(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fake_exec(*argv: str, **kwargs: Any) -> Any:
        raise FileNotFoundError(2, "No such file or directory", "crush")

    monkeypatch.setattr("src.orchestrator.harness_process.asyncio.create_subprocess_exec",
                        _fake_exec)

    out = await CrushAdapter().spawn(repo="/tmp/r", prompt="do the thing")

    assert "error" in out


async def test_crush_list_sessions_needs_a_cwd() -> None:
    assert await CrushAdapter().list_sessions() == []


async def test_crush_list_sessions_sets_the_subprocess_cwd_never_a_cwd_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """LIVE-MEASURED (this module's own docstring): `--cwd` is a no-op for `crush session
    list` -- it silently returns [] regardless of what's on disk. The subprocess's own
    cwd= is the only door that actually works, and this asserts the flag is never passed."""
    calls: list[dict[str, Any]] = []

    class _FakeProc:
        async def communicate(self) -> tuple[bytes, bytes]:
            return (b'[{"id":"abc123","title":"a session"}]', b"")

    async def _fake_exec(*argv: str, **kwargs: Any) -> _FakeProc:
        calls.append({"argv": list(argv), **kwargs})
        return _FakeProc()

    monkeypatch.setattr("src.orchestrator.harness_process.asyncio.create_subprocess_exec",
                        _fake_exec)

    out = await CrushAdapter().list_sessions(cwd="/tmp/r")

    assert out == [{"id": "abc123", "title": "a session"}]
    call = calls[0]
    assert call["argv"] == ["crush", "session", "list", "--json"]  # no --cwd flag, ever
    assert call["cwd"] == "/tmp/r"


async def test_crush_list_sessions_tolerates_a_missing_binary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_exec(*argv: str, **kwargs: Any) -> Any:
        raise FileNotFoundError(2, "No such file or directory", "crush")

    monkeypatch.setattr("src.orchestrator.harness_process.asyncio.create_subprocess_exec",
                        _fake_exec)

    assert await CrushAdapter().list_sessions(cwd="/tmp/r") == []


async def test_crush_list_sessions_tolerates_malformed_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeProc:
        async def communicate(self) -> tuple[bytes, bytes]:
            return (b"not json", b"")

    async def _fake_exec(*argv: str, **kwargs: Any) -> _FakeProc:
        return _FakeProc()

    monkeypatch.setattr("src.orchestrator.harness_process.asyncio.create_subprocess_exec",
                        _fake_exec)

    assert await CrushAdapter().list_sessions(cwd="/tmp/r") == []


# ═══ Cursor stub: declared, nothing built ══════════════════════════════════════════════════

async def test_cursor_stub_declares_no_capabilities_and_refuses_everything() -> None:
    adapter = CursorAdapter()
    assert adapter.capabilities() == frozenset()
    assert adapter.available() is False
    coros = (adapter.spawn(), adapter.resume(), adapter.reply(), adapter.stop(),
             adapter.materialize(pool=None, anchor_sid="x"))
    for coro in coros:
        out = await coro
        assert "error" in out
    assert await adapter.list_sessions() == []


# ═══ selection: pin (env-mapped setting) or auto ══════════════════════════════════════════

def test_resolve_auto_picks_claude_when_available(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ClaudeAdapter, "available", lambda self: True)
    st = Settings(osiris_harness_adapter="auto")

    adapter = resolve_process_adapter(st)

    assert adapter.name == "claude"


def test_resolve_auto_skips_unavailable_claude_for_dsh(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ClaudeAdapter, "available", lambda self: False)
    monkeypatch.setattr(DshAdapter, "available", lambda self: True)
    st = Settings(osiris_harness_adapter="auto")

    adapter = resolve_process_adapter(st)

    assert adapter.name == "dsh"


def test_resolve_pin_forces_the_named_adapter_even_when_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(DshAdapter, "available", lambda self: False)
    st = Settings(osiris_harness_adapter="dsh")

    adapter = resolve_process_adapter(st)

    assert adapter.name == "dsh"  # forced, not silently swapped for claude


def test_resolve_unknown_name_falls_back_to_the_auto_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unknown pin name is never a caller error this door raises on -- it degrades to
    the SAME 'auto' order every other unnamed selection uses (never a guess at what the
    typo meant). Every real candidate forced unavailable here so the test is deterministic
    regardless of whatever this box's own ~/.dsh/sessions, PATH, or installed crush binary
    actually holds."""
    monkeypatch.setattr(ClaudeAdapter, "available", lambda self: False)
    monkeypatch.setattr(DshAdapter, "available", lambda self: False)
    monkeypatch.setattr(CrushAdapter, "available", lambda self: False)
    st = Settings(osiris_harness_adapter="not-a-real-harness")

    adapter = resolve_process_adapter(st)

    assert adapter.name == "claude"  # the final, unconditional fallback


def test_resolve_env_var_maps_onto_the_setting_by_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OSIRIS_HARNESS_ADAPTER", "cursor")

    st = Settings()

    assert st.osiris_harness_adapter == "cursor"


# ═══ claude_pty_argv: one builder, two call sites ═════════════════════════════════════════

def test_claude_pty_argv_with_no_model() -> None:
    assert claude_pty_argv(None) == ["claude"]


def test_claude_pty_argv_with_a_model() -> None:
    assert claude_pty_argv("sonnet-5") == ["claude", "--model", "sonnet-5"]
