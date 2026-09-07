"""Migration 0058 (thread b0c5ddff, decision 0d863363 item 1): every existing OPEN
kind='obligation' Thread without a stale_after gains one, idempotently, mechanically --
never a coordinator's hand pass. Exercises the exact SQL the migration ships (loaded from
the migration file itself, never hand-copied) directly against the test pool, since
alembic migrations run once at session start (conftest) against an otherwise-empty test
graph and so never touch a synthetic old obligation a test creates afterward.
"""
from __future__ import annotations

import importlib.util
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from src.actions.core import Actions

_MIGRATION_PATH = (
    Path(__file__).resolve().parent.parent
    / "alembic" / "versions" / "0058_stale_after_backfill.py"
)
_spec = importlib.util.spec_from_file_location("migration_0058", _MIGRATION_PATH)
assert _spec is not None and _spec.loader is not None
_migration_0058 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_migration_0058)
STALE_AFTER_BACKFILL_SQL = _migration_0058.STALE_AFTER_BACKFILL_SQL
STALE_AFTER_BACKFILL_SOURCE = _migration_0058.STALE_AFTER_BACKFILL_SOURCE


async def _obligation(actions: Actions, canonical: str, *, age_days: int, status: str = "open",
                      kind: str = "obligation", with_stale_after: str | None = None) -> uuid.UUID:
    """A Thread object shaped like an old obligation -- created_at pushed back directly
    (create_or_find_object always stamps now(), and this is exactly the "old row" shape
    the migration exists to reach)."""
    t = await actions.create_or_find_object("Thread", canonical, "test")
    now = datetime.now(UTC)
    await actions.pool.execute(
        "UPDATE objects SET created_at=$2 WHERE id=$1", t, now - timedelta(days=age_days))
    await actions.assert_property(t, "status", status, "test", now, 0.9)
    await actions.assert_property(t, "kind", kind, "test", now, 0.9)
    if with_stale_after:
        await actions.assert_property(t, "stale_after", with_stale_after, "test", now, 0.9)
    return t


async def _stale_after(actions: Actions, object_id: uuid.UUID) -> list[dict]:
    rows = await actions.pool.fetch(
        "SELECT value #>> '{}' AS value, source_id FROM current_assertions "
        "WHERE object_id=$1 AND name='stale_after'", object_id)
    return [dict(r) for r in rows]


async def test_backfill_gives_an_old_open_obligation_a_window(actions: Actions) -> None:
    oid = await _obligation(actions, "thread:backfill-old-open", age_days=40)
    await actions.pool.execute(STALE_AFTER_BACKFILL_SQL)

    rows = await _stale_after(actions, oid)
    assert len(rows) == 1
    assert rows[0]["source_id"] == STALE_AFTER_BACKFILL_SOURCE
    stamped = datetime.fromisoformat(rows[0]["value"])
    assert stamped > datetime.now(UTC)  # floored at now()+1day, never in the past


async def test_backfill_is_idempotent_on_a_second_run(actions: Actions) -> None:
    oid = await _obligation(actions, "thread:backfill-second-run", age_days=40)
    await actions.pool.execute(STALE_AFTER_BACKFILL_SQL)
    first = await _stale_after(actions, oid)
    await actions.pool.execute(STALE_AFTER_BACKFILL_SQL)
    second = await _stale_after(actions, oid)

    assert len(second) == 1  # no second row minted
    assert first[0]["value"] == second[0]["value"]  # untouched, not re-stamped


async def test_backfill_never_touches_a_thread_that_already_carries_a_window(
    actions: Actions,
) -> None:
    already = "2026-01-01T00:00:00+00:00"
    oid = await _obligation(actions, "thread:backfill-already-windowed", age_days=40,
                            with_stale_after=already)
    await actions.pool.execute(STALE_AFTER_BACKFILL_SQL)

    rows = await _stale_after(actions, oid)
    assert len(rows) == 1
    assert rows[0]["value"] == already
    assert rows[0]["source_id"] == "test"  # not overwritten by the migration's source


async def test_backfill_skips_a_non_obligation_thread(actions: Actions) -> None:
    oid = await _obligation(actions, "thread:backfill-question-kind", age_days=40,
                            kind="question")
    await actions.pool.execute(STALE_AFTER_BACKFILL_SQL)

    assert await _stale_after(actions, oid) == []


async def test_backfill_skips_a_resolved_thread(actions: Actions) -> None:
    oid = await _obligation(actions, "thread:backfill-resolved", age_days=40,
                            status="resolved")
    await actions.pool.execute(STALE_AFTER_BACKFILL_SQL)

    assert await _stale_after(actions, oid) == []
