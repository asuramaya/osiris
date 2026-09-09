"""The harness process adapter (thread e7f173a6): spawn/resume/reply/list_sessions/stop
across claude/dsh/crush/cursor, capability refusal by name, and selection by pin or auto.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pytest
from src.actions.core import Actions
from src.config.settings import Settings
from src.orchestrator.harness_process import (
    ClaudeAdapter,
    CrushAdapter,
    CursorAdapter,
    DshAdapter,
    claude_pty_argv,
    resolve_process_adapter,
)


def _make_crush_db(path: Path, *, session_id: str, n: int) -> Path:
    """A minimal, real crush.db (the actual schema, verified live against a real
    install) — `n` messages for one session, so ingest_crush_session has something
    genuine to read."""
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            "CREATE TABLE sessions (id TEXT PRIMARY KEY, parent_session_id TEXT, "
            " title TEXT NOT NULL, message_count INTEGER NOT NULL DEFAULT 0, "
            " prompt_tokens INTEGER NOT NULL DEFAULT 0, "
            " completion_tokens INTEGER NOT NULL DEFAULT 0, "
            " cost REAL NOT NULL DEFAULT 0.0, updated_at INTEGER NOT NULL, "
            " created_at INTEGER NOT NULL, summary_message_id TEXT, todos TEXT)")
        conn.execute(
            "CREATE TABLE messages (id TEXT PRIMARY KEY, session_id TEXT NOT NULL, "
            " role TEXT NOT NULL, parts TEXT NOT NULL DEFAULT '[]', model TEXT, "
            " created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, "
            " finished_at INTEGER, provider TEXT, "
            " is_summary_message INTEGER DEFAULT 0 NOT NULL)")
        conn.execute(
            "INSERT INTO sessions (id, title, message_count, updated_at, created_at) "
            "VALUES (?, 'test session', ?, 1700000000, 1700000000)", (session_id, n))
        for i in range(n):
            conn.execute(
                "INSERT INTO messages (id, session_id, role, parts, model, "
                " created_at, updated_at) VALUES (?,?,?,?,?,?,?)",
                (f"msg-{i}", session_id, "user" if i % 2 == 0 else "assistant",
                 f'[{{"type":"text","data":{{"text":"line {i}"}}}}]', "test-model",
                 1700000000 + i, 1700000000 + i))
        conn.commit()
    finally:
        conn.close()
    return path

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

async def test_dsh_capabilities_is_list_sessions_only_with_no_profile_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.config.settings.get_settings", lambda: Settings(osiris_dsh_profile=""))
    assert DshAdapter().capabilities() == frozenset({"list_sessions"})
    assert "spawn" not in DshAdapter().capabilities()
    assert "materialize" not in DshAdapter().capabilities()


async def test_dsh_capabilities_gains_spawn_and_resume_when_a_profile_is_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.config.settings.get_settings",
        lambda: Settings(osiris_dsh_profile="headless"))
    assert DshAdapter().capabilities() == frozenset(
        {"list_sessions", "spawn", "resume"})


async def test_dsh_reply_stop_materialize_always_refuse_by_name() -> None:
    dsh = DshAdapter()
    coros = (dsh.reply(), dsh.stop(), dsh.materialize(pool=None, anchor_sid="x"))
    for coro in coros:
        out = await coro
        assert out["error"].startswith("adapter 'dsh' does not support")


async def test_dsh_spawn_refuses_by_name_with_no_profile_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.config.settings.get_settings", lambda: Settings(osiris_dsh_profile=""))
    out = await DshAdapter().spawn(repo="/tmp/r", prompt="hi")
    assert out["error"].startswith("adapter 'dsh' does not support")


async def test_dsh_resume_refuses_by_name_with_no_profile_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.config.settings.get_settings", lambda: Settings(osiris_dsh_profile=""))
    out = await DshAdapter().resume(repo="/tmp/r", session_id="s1")
    assert out["error"].startswith("adapter 'dsh' does not support")


async def test_dsh_spawn_calls_dsh_with_the_configured_profile_and_prompt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.config.settings.get_settings",
        lambda: Settings(osiris_dsh_profile="headless"))
    captured: dict[str, Any] = {}

    class _FakeProc:
        pid = 4242

    async def _fake_exec(*argv: str, **kwargs: Any) -> _FakeProc:
        captured["argv"] = argv
        captured["kwargs"] = kwargs
        return _FakeProc()

    monkeypatch.setattr(
        "src.orchestrator.harness_process.asyncio.create_subprocess_exec", _fake_exec)

    out = await DshAdapter().spawn(repo="/tmp/r", prompt="do the thing")

    assert out == {"spawned": True, "repo": "/tmp/r", "pid": 4242, "profile": "headless"}
    assert captured["argv"] == ("dsh", "--profile", "headless", "do the thing")
    assert captured["kwargs"]["cwd"] == "/tmp/r"


async def test_dsh_spawn_needs_a_prompt_even_with_a_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.config.settings.get_settings",
        lambda: Settings(osiris_dsh_profile="headless"))
    out = await DshAdapter().spawn(repo="/tmp/r")
    assert "error" in out


async def test_dsh_resume_forwards_the_configured_resume_flag_verbatim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.config.settings.get_settings",
        lambda: Settings(osiris_dsh_profile="tui", osiris_dsh_resume_flag="--continue"))
    captured: dict[str, Any] = {}

    class _FakeProc:
        pid = 99

    async def _fake_exec(*argv: str, **kwargs: Any) -> _FakeProc:
        captured["argv"] = argv
        return _FakeProc()

    monkeypatch.setattr(
        "src.orchestrator.harness_process.asyncio.create_subprocess_exec", _fake_exec)

    out = await DshAdapter().resume(repo="/tmp/r", session_id="sess-9", prompt="go on")

    assert out == {"resumed": True, "repo": "/tmp/r", "session_id": "sess-9",
                   "pid": 99, "profile": "tui"}
    assert captured["argv"] == ("dsh", "--profile", "tui", "--continue", "sess-9", "go on")


async def test_dsh_spawn_tolerates_a_missing_binary(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "src.config.settings.get_settings",
        lambda: Settings(osiris_dsh_profile="headless"))

    async def _boom(*argv: str, **kwargs: Any) -> Any:
        raise OSError("no such file")

    monkeypatch.setattr(
        "src.orchestrator.harness_process.asyncio.create_subprocess_exec", _boom)

    out = await DshAdapter().spawn(repo="/tmp/r", prompt="hi")
    assert "error" in out


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

async def test_crush_capabilities_is_spawn_list_sessions_and_materialize(
) -> None:
    assert CrushAdapter().capabilities() == frozenset(
        {"spawn", "list_sessions", "materialize"})


async def test_crush_resume_reply_stop_all_refuse_by_name() -> None:
    crush = CrushAdapter()
    for coro in (crush.resume(), crush.reply(), crush.stop()):
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


# ═══ CrushAdapter.materialize (wave 13 item 2): writes soul_lines back into a target
# crush.db — a genuine restore (real sessions/messages schema), never a rewrite of the
# session's original db. ═══════════════════════════════════════════════════════════════════

async def test_crush_materialize_needs_a_dest(actions: Actions) -> None:
    out = await CrushAdapter().materialize(pool=actions.pool, anchor_sid="x")
    assert "error" in out


async def test_crush_materialize_errors_when_nothing_ingested(actions: Actions) -> None:
    out = await CrushAdapter().materialize(
        pool=actions.pool, anchor_sid="never-ingested", dest="/tmp/whatever.db")
    assert "error" in out


async def test_crush_materialize_writes_a_real_readable_crush_db(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.ingest.soul_store import SoulStore

    source_db = _make_crush_db(tmp_path / "source.db", session_id="sess-mat-1", n=3)
    store = SoulStore(actions.pool)
    n = await store.ingest_crush_session(str(source_db), "sess-mat-1", "sessmat1")
    assert n == 3

    dest = tmp_path / "restored" / "crush.db"
    out = await CrushAdapter().materialize(
        pool=actions.pool, anchor_sid="sessmat1", dest=str(dest))
    assert out == {"written": str(dest), "session_id": "sess-mat-1", "messages": 3}

    conn = sqlite3.connect(str(dest))
    try:
        msg_count = conn.execute("SELECT count(*) FROM messages").fetchone()[0]
        session_row = conn.execute(
            "SELECT id, message_count FROM sessions").fetchone()
    finally:
        conn.close()
    assert msg_count == 3
    assert session_row == ("sess-mat-1", 3)


async def test_crush_materialize_refuses_an_existing_session_without_force(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.ingest.soul_store import SoulStore

    source_db = _make_crush_db(tmp_path / "source.db", session_id="sess-mat-2", n=2)
    store = SoulStore(actions.pool)
    await store.ingest_crush_session(str(source_db), "sess-mat-2", "sessmat2")

    dest = tmp_path / "crush.db"
    first = await CrushAdapter().materialize(
        pool=actions.pool, anchor_sid="sessmat2", dest=str(dest))
    assert "error" not in first

    second = await CrushAdapter().materialize(
        pool=actions.pool, anchor_sid="sessmat2", dest=str(dest))
    assert "error" in second
    assert "already exists" in second["error"]

    forced = await CrushAdapter().materialize(
        pool=actions.pool, anchor_sid="sessmat2", dest=str(dest), force=True)
    assert "error" not in forced


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
