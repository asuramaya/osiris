"""A graph position is a current value, not a fact with a history. The layout heartbeat keeps
one graph_layout row per object (upserted in place); the three property assertions it used to
write on every re-layout (graph_x, graph_y, graph_layout_v) are copied across by the 0073
upgrade and then retired from the assertions table."""
from __future__ import annotations

import importlib.util
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from src.actions.core import Actions
from src.orchestrator.graph_layout import GRAPH_LAYOUT_SOURCE, _bulk_assert_positions
from src.orchestrator.retention import retire_layout_history

ROOT = Path(__file__).resolve().parent.parent
NOW = datetime.now(UTC)
OLD = NOW - timedelta(days=30)


def _migration_module() -> Any:
    spec = importlib.util.spec_from_file_location(
        "m0073", ROOT / "alembic" / "versions" / "0073_graph_layout.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def _legacy(actions: Actions, oid: Any, name: str, value: Any, *, current: bool,
                  source: str = GRAPH_LAYOUT_SOURCE) -> int:
    return int(await actions.pool.fetchval(
        "INSERT INTO assertions (object_id, name, value, source_id, observed_at, confidence, "
        " is_current, created_at) VALUES ($1,$2,$3,$4,$5,0.9,$6,$5) RETURNING id",
        oid, name, value, source, OLD, current))


async def _legacy_position(actions: Actions, oid: Any, x: float, y: float, v: int = 9) -> list[int]:
    """Two layouts' worth of history: a superseded pair, then the current triple."""
    ids = [await _legacy(actions, oid, "graph_x", 1.0, current=False),
           await _legacy(actions, oid, "graph_y", 1.0, current=False)]
    ids += [await _legacy(actions, oid, "graph_x", x, current=True),
            await _legacy(actions, oid, "graph_y", y, current=True),
            await _legacy(actions, oid, "graph_layout_v", v, current=True)]
    return ids


async def _present(actions: Actions, ids: list[int]) -> set[int]:
    rows = await actions.pool.fetch("SELECT id FROM assertions WHERE id = ANY($1::bigint[])", ids)
    return {r["id"] for r in rows}


async def test_the_backfill_statement_copies_only_the_layout_sources_current_rows(
    actions: Actions,
) -> None:
    oid = await actions.create_or_find_object("Thread", "thread:gls-bf-1", "test")
    await _legacy_position(actions, oid, 12.5, -3.25, v=7)
    other = await actions.create_or_find_object("Thread", "thread:gls-bf-2", "test")
    await _legacy(actions, other, "graph_x", 5.0, current=True, source="someone-else")
    await _legacy(actions, other, "graph_y", 5.0, current=True, source="someone-else")

    class _Op:
        statements: list[str] = []

        @classmethod
        def execute(cls, sql: str) -> None:
            cls.statements.append(sql)

    module = _migration_module()
    module.op = _Op
    module.upgrade()
    backfill = _Op.statements[1]
    await actions.pool.execute(backfill)
    await actions.pool.execute(backfill)  # a retried upgrade changes nothing

    row = await actions.pool.fetchrow("SELECT x, y, layout_v FROM graph_layout WHERE object_id=$1",
                                      oid)
    assert row is not None and (row["x"], row["y"], row["layout_v"]) == (12.5, -3.25, 7)
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM graph_layout WHERE object_id=$1", other) == 0


async def test_a_position_is_one_row_updated_in_place(actions: Actions) -> None:
    oid = await actions.create_or_find_object("Thread", "thread:gls-w-1", "test")
    await _bulk_assert_positions(actions, {oid: (1.234, 5.678)}, NOW)
    await _bulk_assert_positions(actions, {oid: (9.0, -9.0)}, NOW)
    rows = await actions.pool.fetch("SELECT x, y FROM graph_layout WHERE object_id=$1", oid)
    assert [(r["x"], r["y"]) for r in rows] == [(9.0, -9.0)]
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM assertions WHERE object_id=$1 AND name IN "
        "('graph_x','graph_y','graph_layout_v')", oid) == 0
    # the stream still hears about the move
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM outbox WHERE object_id=$1 AND event_type='layout_moved'", oid) == 2


async def test_retirement_removes_the_history_of_objects_already_copied_across(
    actions: Actions,
) -> None:
    copied = await actions.create_or_find_object("Thread", "thread:gls-r-1", "test")
    uncopied = await actions.create_or_find_object("Thread", "thread:gls-r-2", "test")
    gone = await _legacy_position(actions, copied, 3.0, 4.0)
    kept = await _legacy_position(actions, uncopied, 7.0, 8.0)
    await actions.pool.execute(
        "INSERT INTO graph_layout (object_id, x, y, layout_v) VALUES ($1, 3, 4, 9)", copied)
    foreign = await _legacy(actions, copied, "graph_x", 1.0, current=True, source="someone-else")
    other_name = await _legacy(actions, copied, "label", "x", current=True)

    out = await retire_layout_history(actions.pool, execute=True)

    assert out["finished"] is True and out["deleted"] >= len(gone)
    assert await _present(actions, gone) == set()
    # an object whose position is not in the new table keeps its assertions until it is
    assert await _present(actions, kept) == set(kept)
    # only the layout heartbeat's own rows of the three names go
    assert await _present(actions, [foreign, other_name]) == {foreign, other_name}


async def test_retirement_dry_run_counts_and_deletes_nothing(actions: Actions) -> None:
    oid = await actions.create_or_find_object("Thread", "thread:gls-d-1", "test")
    ids = await _legacy_position(actions, oid, 3.0, 4.0)
    await actions.pool.execute(
        "INSERT INTO graph_layout (object_id, x, y, layout_v) VALUES ($1, 3, 4, 9)", oid)
    out = await retire_layout_history(actions.pool)
    assert out["executed"] is False and out["eligible"] >= len(ids)
    assert await _present(actions, ids) == set(ids)


async def test_retirement_resumes_after_a_time_budget(actions: Actions) -> None:
    oid = await actions.create_or_find_object("Thread", "thread:gls-b-1", "test")
    ids = await _legacy_position(actions, oid, 3.0, 4.0)
    await actions.pool.execute(
        "INSERT INTO graph_layout (object_id, x, y, layout_v) VALUES ($1, 3, 4, 9)", oid)
    first = await retire_layout_history(actions.pool, execute=True, window=1, max_seconds=0.0)
    assert first["finished"] is False
    second = await retire_layout_history(actions.pool, execute=True)
    assert second["finished"] is True
    assert await _present(actions, ids) == set()


async def test_the_daily_job_retires_layout_history_and_says_so(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import src.orchestrator.mailbox as mailbox
    from src.workers.arq_worker import storage_housekeeping_heartbeat

    oid = await actions.create_or_find_object("Thread", "thread:gls-h-1", "test")
    ids = await _legacy_position(actions, oid, 3.0, 4.0)
    await actions.pool.execute(
        "INSERT INTO graph_layout (object_id, x, y, layout_v) VALUES ($1, 3, 4, 9)", oid)
    captured: dict[str, Any] = {}

    async def _fake_send(pool: Any, **kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return {"sent": 1}

    monkeypatch.setattr(mailbox, "send_message", _fake_send)
    await storage_housekeeping_heartbeat({"cascade": SimpleNamespace(actions=actions)})
    assert await _present(actions, ids) == set()
    assert "graph layout rows" in captured["body"]
