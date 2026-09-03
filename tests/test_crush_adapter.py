"""CrushSqliteAdapter's timestamp bug — the FIRST test file for this adapter (its own
docstring confessed "no test of its own" until now; that missing coverage is exactly
why the bug survived, Thoth dispatch 6528/6535). `crush_sqlite.py`'s `_to_dt()` divided
`created_at`/`finished_at` by 1000 assuming epoch-MILLISECONDS; a real crush.db's own
columns are epoch-SECONDS (verified live: sessions.created_at=1785360810 is 2026-07-29,
a sane date — read back as 1970-01-21 under the old, wrong assumption). The same unit
mistake compounded in `read_turns`'s own duration_ms computation, which subtracted two
raw epoch-seconds values and used the result AS milliseconds, undercounting every real
duration 1000x.
"""
from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from src.ingest.harness.crush_sqlite import CrushSqliteAdapter, _to_dt

# The real schema's own column shapes, minimal — matches what a live crush.db carries
# (sessions.id/created_at/updated_at, messages.session_id/role/model/provider/
# is_summary_message/created_at/finished_at), verified against a real database before
# writing this fixture.
_SCHEMA = """
CREATE TABLE sessions (
    id TEXT PRIMARY KEY, created_at INTEGER, updated_at INTEGER
);
CREATE TABLE messages (
    rowid INTEGER PRIMARY KEY, session_id TEXT, role TEXT, model TEXT, provider TEXT,
    is_summary_message INTEGER, created_at INTEGER, finished_at INTEGER, updated_at INTEGER
);
"""


def _make_db(path: Path, *, session_id: str, messages: list[dict[str, object]]) -> None:
    conn = sqlite3.connect(str(path))
    try:
        conn.executescript(_SCHEMA)
        conn.execute("INSERT INTO sessions (id, created_at, updated_at) VALUES (?, ?, ?)",
                     (session_id, 1785360810, 1785360923))
        for m in messages:
            conn.execute(
                "INSERT INTO messages (session_id, role, model, provider, "
                "is_summary_message, created_at, finished_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (session_id, m["role"], m.get("model"), m.get("provider"), 0,
                 m["created_at"], m.get("finished_at"), m["created_at"]))
        conn.commit()
    finally:
        conn.close()


def test_to_dt_reads_the_real_epoch_seconds_column_not_milliseconds() -> None:
    # 1785360810 is the exact value read live off a real crush.db — 2026-07-29, sane.
    # Under the old (epoch-ms) assumption this landed on 1970-01-21 instead.
    assert _to_dt(1785360810) == datetime(2026, 7, 29, 21, 33, 30, tzinfo=UTC)


def test_to_dt_none_stays_none() -> None:
    assert _to_dt(None) is None


def test_read_turns_recorded_at_is_a_sane_recent_date_not_1970(tmp_path: Path) -> None:
    db = tmp_path / "crush.db"
    _make_db(db, session_id="sess-1", messages=[
        {"role": "user", "created_at": 1785360810},
        {"role": "assistant", "model": "qwen3.7-plus", "provider": "alibaba-singapore",
         "created_at": 1785360810, "finished_at": 1785360812},
    ])
    # discover() needs projects.json wiring this fixture doesn't set up — read_turns is
    # the function under test, so build the locator directly, same as the adapter itself
    # would once discover() resolved it.
    from src.ingest.harness import SessionLocator
    locator = SessionLocator(anchor_sid="sess-1", session_id="sess-1", harness="crush",
                             source_path=str(db), cwd=None, project=None)

    turns = list(CrushSqliteAdapter().read_turns(locator))
    assert len(turns) == 2
    for t in turns:
        assert t.recorded_at is not None
        assert t.recorded_at.year == 2026, (
            f"turn recorded_at={t.recorded_at} — the 1970 regression this fix guards")


def test_read_turns_duration_ms_is_milliseconds_not_seconds(tmp_path: Path) -> None:
    db = tmp_path / "crush.db"
    _make_db(db, session_id="sess-2", messages=[
        {"role": "user", "created_at": 1785360810},
        # 2 real seconds elapsed (finished - created = 2) — duration_ms must read 2000,
        # never the bare 2 the pre-fix code returned (a seconds count mislabeled as ms).
        {"role": "assistant", "created_at": 1785360810, "finished_at": 1785360812},
    ])
    from src.ingest.harness import SessionLocator
    locator = SessionLocator(anchor_sid="sess-2", session_id="sess-2", harness="crush",
                             source_path=str(db), cwd=None, project=None)

    turns = list(CrushSqliteAdapter().read_turns(locator))
    assistant_turn = next(t for t in turns if t.role == "assistant")
    assert assistant_turn.duration_ms == 2000


def test_read_turns_no_finished_at_leaves_duration_none(tmp_path: Path) -> None:
    db = tmp_path / "crush.db"
    _make_db(db, session_id="sess-3", messages=[{"role": "user", "created_at": 1785360810}])
    from src.ingest.harness import SessionLocator
    locator = SessionLocator(anchor_sid="sess-3", session_id="sess-3", harness="crush",
                             source_path=str(db), cwd=None, project=None)

    turns = list(CrushSqliteAdapter().read_turns(locator))
    assert turns[0].duration_ms is None
