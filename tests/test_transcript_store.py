"""Harness-agnostic transcript store (ruling be741d3e).

The store is the SEAM: adapters normalize per-harness formats (Claude JSONL, Crush SQLite)
into TurnRows; the store indexes them; identity resolution reads the model off the store
and a non-Claude mind mounts RESOLVED. These tests prove the round trip against real
Postgres (testcontainers) and real SQLite (a tmp crush.db), with synthetic transcripts.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest
import pytest_asyncio
from src.actions.core import Actions
from src.ingest.harness import SessionLocator, TurnRow
from src.ingest.harness.claude_jsonl import ClaudeJsonlAdapter
from src.ingest.harness.crush_sqlite import CrushSqliteAdapter
from src.ingest.transcript_store import TranscriptStore, _reading_from_turns
from src.orchestrator.agents import resolve_identity
from src.orchestrator.context_lens import (
    _usage_from_store,
    detail_from_usage,
    glance_from_usage,
)

# --- pure helpers -------------------------------------------------------

def test_reading_from_turns_picks_latest_model() -> None:
    turns = [
        TurnRow(turn_idx=0, role="user"),
        TurnRow(turn_idx=1, role="assistant", model="claude-fable-5"),
        TurnRow(turn_idx=2, role="assistant", model="claude-opus-4-8"),
    ]
    r = _reading_from_turns(turns, "claude-code", "deadbeef")
    assert r.current == "claude-opus-4-8"
    assert r.history == ("claude-fable-5", "claude-opus-4-8")
    assert r.method == "claude-code"


def test_reading_from_turns_skips_synthetic_and_summary() -> None:
    turns = [
        TurnRow(turn_idx=0, role="assistant", model="<synthetic>"),
        TurnRow(turn_idx=1, role="assistant", model="glm-5.2", is_summary=True),
        TurnRow(turn_idx=2, role="assistant", model="glm-5.2"),
    ]
    r = _reading_from_turns(turns, "crush", "cafef00d")
    assert r.current == "glm-5.2"
    assert r.history == ("glm-5.2",)


def test_reading_from_turns_empty_when_no_assistant_models() -> None:
    r = _reading_from_turns([], "crush", "cafef00d")
    assert r.current is None
    assert r.history == ()


# --- Claude JSONL adapter ----------------------------------------------

def _write_claude_transcript(
    path: Path, sid: str, turns: list[tuple[str, str | None]],
) -> Path:
    """Write a synthetic Claude-Code JSONL transcript. turns = [(role, model|None), ...]."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    for i, (role, model) in enumerate(turns):
        ts = datetime(2026, 7, 18, 12, i, tzinfo=UTC).isoformat()
        entry: dict = {"type": role, "timestamp": ts}
        if role == "assistant" and model:
            entry["message"] = {
                "model": model,
                "usage": {"input_tokens": 100, "output_tokens": 50},
            }
        else:
            entry["message"] = {"content": "hi"}
        lines.append(json.dumps(entry))
    path.write_text("\n".join(lines) + "\n")
    return path


def test_claude_adapter_discovers_and_reads(tmp_path: Path) -> None:
    sid = "aabbccdd"
    projects = tmp_path / "projects"
    proj_dir = projects / "-home-x-code-widget"
    transcript = _write_claude_transcript(
        proj_dir / f"{sid}-session-uuid.jsonl", sid,
        [("user", None), ("assistant", "claude-fable-5"), ("assistant", "claude-opus-4-8")],
    )
    adapter = ClaudeJsonlAdapter()
    loc = adapter.discover(
        cwd="/home/x/code/widget", job_dir=f"/home/x/.claude/jobs/{sid}", root=projects)
    assert loc is not None
    assert loc.anchor_sid == sid
    assert loc.source_path == str(transcript)
    turns = list(adapter.read_turns(loc))
    models = [t.model for t in turns if t.role == "assistant"]
    assert models == ["claude-fable-5", "claude-opus-4-8"]


def test_claude_adapter_returns_none_when_no_anchor(tmp_path: Path) -> None:
    adapter = ClaudeJsonlAdapter()
    assert adapter.discover(cwd="/nowhere", job_dir="/x/jobs/deadbeef", root=tmp_path) is None


# --- Crush SQLite adapter ---------------------------------------------

def _write_crush_db(
    db_path: Path, session_id: str, turns: list[tuple[str, str, str | None]],
) -> None:
    """Write a synthetic Crush crush.db. turns = [(role, provider, model|None), ...]."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS sessions (
            id TEXT PRIMARY KEY, parent_session_id TEXT, title TEXT,
            message_count INTEGER, prompt_tokens INTEGER, completion_tokens INTEGER,
            cost REAL, updated_at INTEGER, created_at INTEGER, summary_message_id TEXT, todos TEXT
        );
        CREATE TABLE IF NOT EXISTS messages (
            id TEXT, session_id TEXT, role TEXT, parts TEXT, model TEXT,
            created_at INTEGER, updated_at INTEGER, finished_at INTEGER,
            provider TEXT, is_summary_message INTEGER
        );
    """)
    conn.execute(
        "INSERT INTO sessions (id, created_at, updated_at) VALUES (?, 1000, 2000)",
        (session_id,),
    )
    for i, (role, provider, model) in enumerate(turns):
        conn.execute(
            "INSERT INTO messages (id, session_id, role, model, provider, created_at, "
            "                      finished_at, is_summary_message) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 0)",
            (f"msg-{i}", session_id, role, model, provider, 1000 + i * 100, 1000 + i * 100 + 50),
        )
    conn.commit()
    conn.close()


def test_crush_adapter_discovers_and_reads(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sid = "cafef00d-aaaa-bbbb-cccc-dddddddddddd"
    data_dir = tmp_path / "crush-data"
    _write_crush_db(data_dir / "crush.db", sid, [
        ("user", "", None),
        ("assistant", "zai", "glm-5.2"),
        ("assistant", "zai", "glm-5.2"),
    ])
    # point the adapter at our tmp data dir via a fake projects.json
    import src.ingest.harness.crush_sqlite as mod
    monkeypatch.setattr(mod, "_PROJECTS_JSON", tmp_path / "projects.json")
    (tmp_path / "projects.json").write_text(json.dumps({
        "projects": {str(tmp_path): {"data_dir": str(data_dir)}}}))
    monkeypatch.setattr(Path, "cwd", classmethod(lambda cls: tmp_path))

    adapter = CrushSqliteAdapter()
    loc = adapter.discover(cwd=str(tmp_path), job_dir=None)
    assert loc is not None
    assert loc.anchor_sid == sid[:8]
    assert loc.harness == "crush"
    turns = list(adapter.read_turns(loc))
    assistant_models = [t.model for t in turns if t.role == "assistant"]
    assert assistant_models == ["glm-5.2", "glm-5.2"]
    assert all(t.provider == "zai" for t in turns if t.role == "assistant")


def test_crush_adapter_returns_none_when_no_db(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import src.ingest.harness.crush_sqlite as mod
    monkeypatch.setattr(mod, "_PROJECTS_JSON", tmp_path / "nonexistent.json")
    monkeypatch.setattr(Path, "cwd", classmethod(lambda cls: tmp_path))
    adapter = CrushSqliteAdapter()
    assert adapter.discover(cwd=str(tmp_path / "nowhere"), job_dir=None) is None


# --- store round-trip (real Postgres) ----------------------------------

@pytest_asyncio.fixture
async def store(actions: Actions) -> TranscriptStore:
    return TranscriptStore(actions.pool)


async def test_store_ingests_and_reads_back(store: TranscriptStore, tmp_path: Path) -> None:
    """A Crush session eaten into the store reads back with the right model + history."""
    sid = "deadbeef-aaaa-bbbb-cccc-dddddddddddd"
    db_path = tmp_path / "crush.db"
    _write_crush_db(db_path, sid, [
        ("user", "", None),
        ("assistant", "zai", "glm-5.2"),
    ])
    # inject a fake adapter pointing at our tmp DB
    class _FakeCrush(CrushSqliteAdapter):
        def discover(self, *, cwd, job_dir, root=None):
            return SessionLocator(
                anchor_sid=sid[:8], session_id=sid, harness="crush",
                source_path=str(db_path), cwd=str(tmp_path), project="atlas")
    reading = await store.discover_and_ingest(
        cwd=str(tmp_path), job_dir=None, adapters=[_FakeCrush()])
    assert reading is not None
    assert reading.current == "glm-5.2"
    assert reading.history == ("glm-5.2",)
    assert reading.method == "crush"
    # read back from the store
    back = await store.model_of_session("crush", sid[:8])
    assert back is not None
    assert back.current == "glm-5.2"


async def test_store_ingest_is_idempotent(store: TranscriptStore, tmp_path: Path) -> None:
    """Re-ingesting the same session does not duplicate turns."""
    sid = "feedface-aaaa-bbbb-cccc-dddddddddddd"
    db_path = tmp_path / "crush.db"
    _write_crush_db(db_path, sid, [("assistant", "zai", "glm-5.2")])
    class _FakeCrush(CrushSqliteAdapter):
        def discover(self, *, cwd, job_dir, root=None):
            return SessionLocator(
                anchor_sid=sid[:8], session_id=sid, harness="crush",
                source_path=str(db_path), cwd=str(tmp_path), project="x")
    await store.discover_and_ingest(cwd=str(tmp_path), job_dir=None, adapters=[_FakeCrush()])
    await store.discover_and_ingest(cwd=str(tmp_path), job_dir=None, adapters=[_FakeCrush()])
    count = await store.pool.fetchval(
        "SELECT count(*) FROM harness_turns WHERE harness='crush' AND anchor_sid=$1", sid[:8])
    assert count == 1


# --- resolve_identity wired to the store -------------------------------

def test_resolve_identity_prefers_store_reading_over_legacy() -> None:
    """A store reading (e.g. from Crush) resolves identity even with no JSONL transcript."""
    from src.ingest.harness import ModelReading
    reading = ModelReading(
        current="glm-5.2", history=("glm-5.2",), deliberate=False,
        observed_at=None, method="crush", anchor_sid="cafef00d")
    ident = resolve_identity(
        cwd="/home/asuramaya/.osiris/seats/atlas", job_dir=None,
        store_reading=reading, root=Path("/nonexistent"),
    )
    assert ident.model == "glm-5.2"
    assert ident.model_method == "crush"
    assert ident.model_history == ("glm-5.2",)
    assert ident.resolved is True  # the store reading carried an anchor


def test_resolve_identity_falls_back_when_no_store_reading(tmp_path: Path) -> None:
    """No store reading → legacy JSONL path still works (Slice 2 hasn't migrated it yet)."""
    ident = resolve_identity(
        cwd="/home/x/code/widget", job_dir="/home/x/.claude/jobs/aabbccdd",
        model="claude-opus-4-8", root=tmp_path, store_reading=None,
    )
    assert ident.model == "claude-opus-4-8"
    assert ident.model_method == "self_report"


# --- adapter enumerate() — the backfill sweep --------------------------

def test_claude_enumerate_yields_all_transcripts(tmp_path: Path) -> None:
    """enumerate() walks ~/.claude/projects/*/*.jsonl — one locator per session."""
    projects = tmp_path / "projects"
    _write_claude_transcript(
        projects / "-home-x-code-widget" / "aaaa1111-session.jsonl",
        "aaaa1111", [("assistant", "claude-fable-5")])
    _write_claude_transcript(
        projects / "-home-x-code-widget" / "bbbb2222-session.jsonl",
        "bbbb2222", [("assistant", "claude-opus-4-8")])
    _write_claude_transcript(
        projects / "-home-y-code-other" / "cccc3333-session.jsonl",
        "cccc3333", [("assistant", "claude-haiku-4-5-20251001")])
    adapter = ClaudeJsonlAdapter()
    locs = list(adapter.enumerate(root=projects))
    anchors = sorted(loc.anchor_sid for loc in locs)
    assert anchors == ["aaaa1111", "bbbb2222", "cccc3333"]
    # project comes from the parent dir's basename
    projects_found = sorted(loc.project for loc in locs if loc.project)
    assert "widget" in projects_found
    assert "other" in projects_found


def test_claude_enumerate_skips_empty_root(tmp_path: Path) -> None:
    adapter = ClaudeJsonlAdapter()
    assert list(adapter.enumerate(root=tmp_path / "nonexistent")) == []


def test_crush_enumerate_yields_all_sessions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """enumerate() walks every session row in every known crush.db."""
    # two fake projects, each with a crush.db holding multiple sessions
    dd1 = tmp_path / "p1" / ".crush"
    dd2 = tmp_path / "p2" / ".crush"
    _write_crush_db(dd1 / "crush.db", "aaaa1111-aaaa-bbbb-cccc-dddddddddddd",
                    [("assistant", "zai", "glm-5.2")])
    _write_crush_db(dd1 / "crush.db", "bbbb2222-aaaa-bbbb-cccc-dddddddddddd",
                    [("assistant", "zai", "glm-5.2")])
    _write_crush_db(dd2 / "crush.db", "cccc3333-aaaa-bbbb-cccc-dddddddddddd",
                    [("assistant", "zai", "glm-5.2")])
    import src.ingest.harness.crush_sqlite as mod
    monkeypatch.setattr(mod, "_PROJECTS_JSON", tmp_path / "projects.json")
    (tmp_path / "projects.json").write_text(json.dumps({"projects": {
        str(tmp_path / "p1"): {"data_dir": str(dd1)},
        str(tmp_path / "p2"): {"data_dir": str(dd2)},
    }}))
    # hide seat offices for this test (no $HOME interference)
    monkeypatch.setattr(Path, "home", lambda: tmp_path / "fakehome")
    adapter = CrushSqliteAdapter()
    locs = list(adapter.enumerate())
    anchors = sorted(loc.anchor_sid for loc in locs)
    assert anchors == ["aaaa1111", "bbbb2222", "cccc3333"]


def test_crush_reads_the_list_shaped_projects_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Crush's CURRENT projects.json is a LIST of {path, data_dir} records (field-verified
    2026-07-19 — the dict lane was silently dead on the live box); both shapes must read."""
    dd = tmp_path / "p9" / ".crush"
    _write_crush_db(dd / "crush.db", "dddd9999-aaaa-bbbb-cccc-dddddddddddd",
                    [("assistant", "zai", "glm-5.2")])
    import src.ingest.harness.crush_sqlite as mod
    monkeypatch.setattr(mod, "_PROJECTS_JSON", tmp_path / "projects.json")
    (tmp_path / "projects.json").write_text(json.dumps({"projects": [
        {"path": str(tmp_path / "p9"), "data_dir": str(dd),
         "last_accessed": "2026-07-19T00:00:00Z"},
    ]}))
    monkeypatch.setattr(Path, "home", lambda: tmp_path / "fakehome")
    locs = list(CrushSqliteAdapter().enumerate())
    assert [x.anchor_sid for x in locs] == ["dddd9999"]
    loc = CrushSqliteAdapter().discover(cwd=str(tmp_path / "p9"), job_dir=None)
    assert loc is not None and loc.anchor_sid == "dddd9999"


# --- store backfill + usage reads -------------------------------------

async def test_backfill_ingests_via_enumerate(store: TranscriptStore, tmp_path: Path) -> None:
    """backfill() walks each adapter's enumerate() and ingests everything."""
    projects = tmp_path / "projects"
    _write_claude_transcript(
        projects / "-home-x-code-widget" / "dddd4444-session.jsonl",
        "dddd4444",
        [("user", None), ("assistant", "claude-fable-5"),
         ("assistant", "claude-opus-4-8")])
    class _FakeAdapter(ClaudeJsonlAdapter):
        def enumerate(self, *, root=None):  # type: ignore[override]
            yield SessionLocator(
                anchor_sid="dddd4444", session_id="dddd4444-session",
                harness="claude-code", source_path=str(
                    projects / "-home-x-code-widget" / "dddd4444-session.jsonl"),
                cwd=None, project="widget")
    counts = await store.backfill(adapters=[_FakeAdapter()])
    assert counts == {"claude-code": 1}
    r = await store.model_of_session("claude-code", "dddd4444")
    assert r is not None
    assert r.current == "claude-opus-4-8"
    assert r.history == ("claude-fable-5", "claude-opus-4-8")


async def test_backfill_is_idempotent(store: TranscriptStore, tmp_path: Path) -> None:
    """Running backfill twice doesn't duplicate rows."""
    projects = tmp_path / "projects"
    _write_claude_transcript(
        projects / "-home-x-code-widget" / "eeee5555-session.jsonl",
        "eeee5555", [("assistant", "claude-fable-5")])
    class _FakeAdapter(ClaudeJsonlAdapter):
        def enumerate(self, *, root=None):  # type: ignore[override]
            yield SessionLocator(
                anchor_sid="eeee5555", session_id="eeee5555-session",
                harness="claude-code",
                source_path=str(projects / "-home-x-code-widget" / "eeee5555-session.jsonl"),
                cwd=None, project="widget")
    await store.backfill(adapters=[_FakeAdapter()])
    await store.backfill(adapters=[_FakeAdapter()])
    n = await store.pool.fetchval(
        "SELECT count(*) FROM harness_turns WHERE harness='claude-code' AND anchor_sid='eeee5555'")
    assert n == 1


async def test_last_usage_of_session_returns_latest_turn(
    store: TranscriptStore, tmp_path: Path,
) -> None:
    """last_usage_of_session() returns the most recent assistant turn's tokens."""
    sid = "ffff6666-aaaa-bbbb-cccc-dddddddddddd"
    db = tmp_path / "crush.db"
    _write_crush_db(db, sid, [
        ("user", "", None),
        ("assistant", "zai", "glm-5.2"),  # Crush has no per-turn tokens
    ])
    class _FakeCrush(CrushSqliteAdapter):
        def discover(self, *, cwd, job_dir, root=None):
            return SessionLocator(
                anchor_sid=sid[:8], session_id=sid, harness="crush",
                source_path=str(db), cwd=str(tmp_path), project="x")
    await store.discover_and_ingest(cwd=str(tmp_path), job_dir=None, adapters=[_FakeCrush()])
    # Crush has no per-turn tokens — last_usage returns None
    assert await store.last_usage_of_session("crush", sid[:8]) is None


async def test_last_usage_of_session_with_tokens(store: TranscriptStore, tmp_path: Path) -> None:
    """Claude transcripts carry usage — last_usage_of_session surfaces it."""
    sid = "abcd1234"
    projects = tmp_path / "projects"
    _write_claude_transcript(
        projects / "-home-x-code-widget" / f"{sid}-session.jsonl",
        sid, [("assistant", "claude-fable-5")])
    class _FakeClaude(ClaudeJsonlAdapter):
        def discover(self, *, cwd, job_dir, root=None):
            return SessionLocator(
                anchor_sid=sid, session_id=f"{sid}-session", harness="claude-code",
                source_path=str(projects / "-home-x-code-widget" / f"{sid}-session.jsonl"),
                cwd=None, project="widget")
    await store.discover_and_ingest(cwd=None, job_dir=None, adapters=[_FakeClaude()])
    u = await store.last_usage_of_session("claude-code", sid)
    assert u is not None
    # _write_claude_transcript uses input_tokens=100, output_tokens=50
    assert u["input"] == 100


async def test_usage_of_session_aggregates(store: TranscriptStore, tmp_path: Path) -> None:
    """usage_of_session() sums tokens across all assistant turns."""
    sid = "bcde2345"
    projects = tmp_path / "projects"
    # 3 assistant turns, each 100 input + 50 output → 300/150 aggregate
    _write_claude_transcript(
        projects / "-home-x-code-widget" / f"{sid}-session.jsonl",
        sid, [("assistant", "claude-fable-5"), ("assistant", "claude-fable-5"),
              ("assistant", "claude-fable-5")])
    class _FakeClaude(ClaudeJsonlAdapter):
        def discover(self, *, cwd, job_dir, root=None):
            return SessionLocator(
                anchor_sid=sid, session_id=f"{sid}-session", harness="claude-code",
                source_path=str(projects / "-home-x-code-widget" / f"{sid}-session.jsonl"),
                cwd=None, project="widget")
    await store.discover_and_ingest(cwd=None, job_dir=None, adapters=[_FakeClaude()])
    u = await store.usage_of_session("claude-code", sid)
    assert u is not None
    assert u["input"] == 300
    assert u["output"] == 150


# --- context_lens store-sourced rendering ------------------------------

def test_usage_from_store_adapts_row_to_usage_dict() -> None:
    row = {"input": 1000, "output": 200, "cache_read": 5000, "cache_creation": 500}
    u = _usage_from_store(row)
    assert u == {"input": 1000, "cache_read": 5000, "cache_creation": 500, "output_last_turn": 200}


def test_usage_from_store_returns_none_without_input() -> None:
    assert _usage_from_store({"input": None, "output": 10}) is None


def test_glance_from_usage_computes_pct() -> None:
    u = {"input": 50000, "cache_read": 0, "cache_creation": 0, "output_last_turn": 0}
    g = glance_from_usage(u, raw_model=None)
    # 50000 / 200000 (default window) = 25%
    assert g["pct"] == 25
    assert g["assumed"] is True  # no [1m] marker, no env override


def test_detail_from_usage_builds_full_dict() -> None:
    u = {"input": 170000, "cache_read": 0, "cache_creation": 0, "output_last_turn": 0}
    d = detail_from_usage(u, raw_model=None, window_hint=200_000)
    assert d["pct"] == 85  # 170000/200000
    assert d["window_assumed"] is False  # window_hint → not assumed
    assert d["window"] == 200_000
    assert d["remaining"] == 30_000
    # 85% >= ALARM_PCT(80) and not assumed → warning fires
    assert "warning" in d


def test_detail_from_usage_no_warning_when_assumed() -> None:
    """The alarm fires only on a KNOWN window — Anubis VII's false eulogy protection."""
    u = {"input": 170000, "cache_read": 0, "cache_creation": 0, "output_last_turn": 0}
    d = detail_from_usage(u, raw_model=None)  # no window_hint → assumed
    assert d["window_assumed"] is True
    assert "warning" not in d
    assert "note" in d  # the assumed caveat


# --- the spend gate (Thoth XLV's hardening) ----------------------------

class _CountingAdapter(ClaudeJsonlAdapter):
    """A fixed-locator adapter that counts source reads — the spend gate's witness."""

    def __init__(self, locator: SessionLocator) -> None:
        self._loc = locator
        self.reads = 0

    def discover(self, *, cwd=None, job_dir=None, root=None):  # type: ignore[override]
        return self._loc

    def enumerate(self, *, root=None):  # type: ignore[override]
        yield self._loc

    def read_turns(self, locator, *, since_idx: int = 0):  # type: ignore[override]
        self.reads += 1
        yield from super().read_turns(locator, since_idx=since_idx)


def _counting_setup(tmp_path: Path) -> _CountingAdapter:
    p = _write_claude_transcript(
        tmp_path / "projects" / "-home-x-w" / "eeee5555-session.jsonl", "eeee5555",
        [("user", None), ("assistant", "claude-fable-5")])
    import os
    os.utime(p, times=(1_000_000_000, 1_000_000_000))  # a fixed past mtime
    return _CountingAdapter(SessionLocator(
        anchor_sid="eeee5555", session_id="eeee5555-session", harness="claude-code",
        source_path=str(p), cwd=None, project="w"))


async def test_an_unchanged_source_is_never_reread(
    store: TranscriptStore, tmp_path: Path,
) -> None:
    """THE SPEND GATE: the second discover_and_ingest costs a stat + a row lookup, zero
    file reads — and still returns the FULL reading (from the store, never the delta)."""
    adapter = _counting_setup(tmp_path)
    r1 = await store.discover_and_ingest(cwd=None, job_dir=None, adapters=[adapter])
    assert r1 is not None and r1.current == "claude-fable-5"
    assert adapter.reads == 1
    r2 = await store.discover_and_ingest(cwd=None, job_dir=None, adapters=[adapter])
    assert r2 is not None and r2.current == "claude-fable-5"
    assert adapter.reads == 1  # the gate held: no second read


async def test_a_changed_source_is_read_from_the_delta_with_full_history(
    store: TranscriptStore, tmp_path: Path,
) -> None:
    """An appended source re-reads (from since_idx), and the reading keeps the WHOLE model
    history — the delta alone would amnesia the swap record."""
    import os

    adapter = _counting_setup(tmp_path)
    await store.discover_and_ingest(cwd=None, job_dir=None, adapters=[adapter])
    p = Path(adapter._loc.source_path)  # noqa: SLF001 — the test owns the fixture
    _write_claude_transcript(
        p, "eeee5555",
        [("user", None), ("assistant", "claude-fable-5"), ("assistant", "glm-5.2")])
    os.utime(p, times=(2_000_000_000, 2_000_000_000))  # newer than the last ingest stamp
    r = await store.discover_and_ingest(cwd=None, job_dir=None, adapters=[adapter])
    assert adapter.reads == 2
    assert r is not None
    assert r.current == "glm-5.2"
    assert r.history == ("claude-fable-5", "glm-5.2")
    n = await store.pool.fetchval(
        "SELECT count(*) FROM harness_turns WHERE anchor_sid='eeee5555'")
    assert n == 3  # no duplicate turns from the re-read


async def test_backfill_skips_the_unchanged_and_eats_the_changed(
    store: TranscriptStore, tmp_path: Path,
) -> None:
    """A steady-state sweep over a quiet fleet does no file IO at all (thread 51000597:
    the miner-walked-everything-forever shape must never come back)."""
    adapter = _counting_setup(tmp_path)
    c1 = await store.backfill(adapters=[adapter])
    assert c1 == {"claude-code": 1}
    assert adapter.reads == 1
    c2 = await store.backfill(adapters=[adapter])
    assert c2 == {"claude-code": 0}
    assert adapter.reads == 1  # unchanged source: stat only, no read


# --- the overhead lens (neo's eye, task #34) ---------------------------

def test_reminders_counted_in_nested_content() -> None:
    """The modern harness nests reminder text inside tool_result content lists — the
    counter walks the whole tree (the ancestor's top-level walk undercounted here)."""
    from src.ingest.harness.claude_jsonl import _reminders_of_line
    line = {
        "type": "user",
        "message": {"role": "user", "content": [
            {"type": "text", "text": "<system-reminder>a</system-reminder>"},
            {"type": "tool_result", "content": [
                {"type": "text",
                 "text": "x <system-reminder>b</system-reminder> y"}]},
        ]},
    }
    assert _reminders_of_line(line) == 2
    assert _reminders_of_line({"type": "user", "message": {"content": "plain"}}) == 0


def test_read_turns_carries_overhead_facts(tmp_path: Path) -> None:
    """Reminders ride live user turns only (a compact summary QUOTES the past — counting
    its reminders again after every compaction would inflate the churn number), and the
    compact-summary line itself is flagged is_compaction."""
    path = tmp_path / "p" / "cafe1234-s.jsonl"
    path.parent.mkdir(parents=True)
    lines = [
        {"type": "user", "message": {
            "role": "user",
            "content": "<system-reminder>hi</system-reminder> question"}},
        {"type": "assistant", "message": {
            "model": "claude-fable-5",
            "usage": {"input_tokens": 10, "output_tokens": 5}}},
        {"type": "user", "isCompactSummary": True, "message": {
            "role": "user",
            "content": "<system-reminder>quoted</system-reminder> summary"}},
    ]
    path.write_text("\n".join(json.dumps(x) for x in lines) + "\n")
    loc = SessionLocator(
        anchor_sid="cafe1234", session_id="cafe1234-s", harness="claude-code",
        source_path=str(path), cwd=None, project=None)
    turns = list(ClaudeJsonlAdapter().read_turns(loc))
    assert turns[0].reminders == 1
    assert turns[1].reminders is None       # assistant turns carry no reminders
    assert turns[2].reminders is None       # the summary's quotes are not re-counted
    assert turns[2].is_compaction is True
    assert turns[2].is_summary is True
    assert turns[0].is_compaction is False


def _write_channel_fixture(projects: Path) -> None:
    """A primary with one sidechain channel beside it and one workflow fan-out below it,
    in the modern on-disk layout (<project>/<stem>.jsonl +
    <stem>/subagents/agent-*.jsonl + .meta.json +
    <stem>/subagents/workflows/wf_*/agent-*.jsonl)."""
    _write_claude_transcript(
        projects / "-home-x-code-widget" / "beef0001-s.jsonl",
        "beef0001", [("user", None), ("assistant", "claude-fable-5")])
    sa = projects / "-home-x-code-widget" / "beef0001-s" / "subagents"
    sa.mkdir(parents=True)
    (sa / "agent-aaaa11112222333.jsonl").write_text(json.dumps(
        {"type": "assistant", "isSidechain": True,
         "message": {"model": "claude-fable-5",
                     "usage": {"input_tokens": 7, "output_tokens": 3}}}) + "\n")
    (sa / "agent-aaaa11112222333.meta.json").write_text(
        json.dumps({"agentType": "Explore"}))
    wf = sa / "workflows" / "wf_test-1"
    wf.mkdir(parents=True)
    (wf / "agent-bbbb44445555666.jsonl").write_text(json.dumps(
        {"type": "assistant", "isSidechain": True,
         "message": {"model": "claude-fable-5",
                     "usage": {"input_tokens": 20, "output_tokens": 10}}}) + "\n")
    (wf / "agent-bbbb44445555666.meta.json").write_text(
        json.dumps({"agentType": "general-purpose"}))


def test_enumerate_yields_hidden_channels(tmp_path: Path) -> None:
    """enumerate() walks the subagents dir beside each primary: channel='sidechain',
    parent_sid ties it home, agent_type comes off the meta.json sidecar."""
    projects = tmp_path / "projects"
    _write_channel_fixture(projects)
    locs = list(ClaudeJsonlAdapter().enumerate(root=projects))
    prim = [loc for loc in locs if loc.channel == "primary"]
    sides = [loc for loc in locs if loc.channel == "sidechain"]
    wfs = [loc for loc in locs if loc.channel == "workflow"]
    assert len(prim) == 1 and len(sides) == 1 and len(wfs) == 1
    c = sides[0]
    assert c.parent_sid == "beef0001"
    assert c.agent_type == "Explore"
    assert c.anchor_sid == "aaaa11112222333"
    w = wfs[0]
    assert w.parent_sid == "beef0001"
    assert w.agent_type == "general-purpose"
    assert w.anchor_sid == "bbbb44445555666"


async def test_overhead_of_session_splits_channels(
    store: TranscriptStore, tmp_path: Path,
) -> None:
    """The lens's core claim: visible = the primary window, hidden = the channels
    beside it, multiplier honest from reported tokens."""
    projects = tmp_path / "projects"
    _write_channel_fixture(projects)

    class _Rooted(ClaudeJsonlAdapter):
        def enumerate(self, *, root=None):  # type: ignore[override]
            yield from super().enumerate(root=projects)

    counts = await store.backfill(adapters=[_Rooted()])
    assert counts == {"claude-code": 3}  # the primary AND both channels
    oh = await store.overhead_of_session("claude-code", "beef0001")
    assert oh is not None
    assert oh["visible"]["total"] == 150      # 100 in + 50 out
    assert oh["hidden"]["total"] == 40        # sidechain 10 + workflow 30
    assert oh["total_tokens"] == 190
    assert oh["sidechains"] == 1
    assert oh["workflows"] == 1
    assert oh["basis"] == "tokens"
    assert oh["multiplier"] == 1.3
    assert oh["hidden_pct"] == 21.1
    # detail is sorted by tokens desc — the workflow fan-out leads
    assert oh["detail"][0]["agent_type"] == "general-purpose"
    assert oh["detail"][0]["tokens"] == 30
    assert oh["detail"][1]["tokens"] == 10
    # bytes rode in from the ingest stat — the fallback basis is real, not fabricated
    assert oh["visible"]["bytes"] > 0 and oh["hidden"]["bytes"] > 0


async def test_overhead_fleet_rolls_up_by_root(
    store: TranscriptStore, tmp_path: Path,
) -> None:
    """The chrome's reading: channels fold into their root session; totals speak."""
    projects = tmp_path / "projects"
    _write_channel_fixture(projects)

    class _Rooted(ClaudeJsonlAdapter):
        def enumerate(self, *, root=None):  # type: ignore[override]
            yield from super().enumerate(root=projects)

    await store.backfill(adapters=[_Rooted()])
    fleet = await store.overhead_fleet(top=5)
    t = fleet["totals"]
    assert t["sessions"] == 1                 # the channels folded into their root
    assert t["channel_files"] == 2
    assert t["total_tokens"] == 190
    assert t["hidden_tokens"] == 40
    top = fleet["top"][0]
    assert top["anchor_sid"] == "beef0001"
    assert top["project"] == "widget"
    assert top["channel_files"] == 2
    assert top["multiplier"] == 1.3


async def test_rederive_resets_and_reeats_with_new_facts(
    store: TranscriptStore, tmp_path: Path,
) -> None:
    """rederive() forgets a session's derived rows and the next sweep re-eats them with
    the current extraction — the post-schema-growth heal. A session whose source has
    vanished keeps its rows (they are the only record left)."""
    projects = tmp_path / "projects"
    _write_channel_fixture(projects)
    gone = _write_claude_transcript(
        projects / "-home-x-code-widget" / "dead0002-s.jsonl",
        "dead0002", [("assistant", "claude-fable-5")])

    class _Rooted(ClaudeJsonlAdapter):
        def enumerate(self, *, root=None):  # type: ignore[override]
            yield from super().enumerate(root=projects)

    await store.backfill(adapters=[_Rooted()])
    # simulate the pre-lens era: blank the overhead facts the first eat recorded
    await store.pool.execute(
        "UPDATE harness_turns SET reminders=NULL, is_compaction=false")
    gone.unlink()  # this session's source vanishes — its rows must survive the reset
    n = await store.rederive("claude-code")
    assert n == 1  # beef0001 reset; dead0002 guarded
    assert await store.pool.fetchval(
        "SELECT count(*) FROM harness_turns WHERE anchor_sid='dead0002'") == 1
    assert await store.pool.fetchval(
        "SELECT count(*) FROM harness_turns WHERE anchor_sid='beef0001'") == 0
    counts = await store.backfill(adapters=[_Rooted()])
    assert counts["claude-code"] >= 1  # the reset primary was re-eaten
    oh = await store.overhead_of_session("claude-code", "beef0001")
    assert oh is not None
    assert oh["visible"]["total"] == 150  # the re-eat restored the full accounting
