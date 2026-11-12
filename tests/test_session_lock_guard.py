"""A session-level advisory lock must never be held on a shared-pool slot.

The holder keeps the slot for as long as it holds the lock and usually calls the pool again for
its own work, so a few holders use up every slot while each waits for one more. The lock goes on
a side connection (`src.db.session_lock.session_lock_connection`). This reads every src function
that takes a session lock, or calls the layout lock helper, and fails when the same function also
calls `.acquire(`. The one sanctioned exception is the seat spawn lock's documented fallback."""
from __future__ import annotations

import ast
import asyncio
import re
from pathlib import Path

import pytest
from src.actions.core import Actions
from src.db.session_lock import close_side_connection, session_lock_connection

SRC = Path(__file__).resolve().parent.parent / "src"
SESSION_LOCK = re.compile(r"pg_(try_)?advisory_lock\b")
LAYOUT_HELPER = "_try_acquire_layout_lock"
# (file under src, function): the pool slot is only the fallback when no side connection can be
# opened, and it is announced where it happens
ALLOWED_POOL_FALLBACK = {("orchestrator/trigger.py", "seat_spawn_lock")}


def _functions(tree: ast.AST) -> list[ast.AsyncFunctionDef | ast.FunctionDef]:
    return [n for n in ast.walk(tree) if isinstance(n, ast.AsyncFunctionDef | ast.FunctionDef)]


def _callee(call: ast.Call) -> str:
    return str(getattr(call.func, "id", getattr(call.func, "attr", "")))


def _takes_session_lock(fn: ast.AST) -> bool:
    for n in ast.walk(fn):
        if (isinstance(n, ast.Constant) and isinstance(n.value, str)
                and "SELECT" in n.value and SESSION_LOCK.search(n.value)):
            return True  # SQL text, not a docstring that names the function
        if isinstance(n, ast.Call) and _callee(n) == LAYOUT_HELPER:
            return True
    return False


def _takes_pool_slot(fn: ast.AST) -> bool:
    return any(isinstance(n, ast.Call) and getattr(n.func, "attr", "") == "acquire"
               for n in ast.walk(fn))


def _holders() -> list[tuple[str, str, bool]]:
    found = []
    for path in sorted(SRC.rglob("*.py")):
        tree = ast.parse(path.read_text())
        rel = path.relative_to(SRC).as_posix()
        for fn in _functions(tree):
            if _takes_session_lock(fn):
                found.append((rel, fn.name, _takes_pool_slot(fn)))
    return found


def test_the_scan_sees_the_known_session_lock_holders() -> None:
    names = {(f, n) for f, n, _ in _holders()}
    for known in [("orchestrator/trigger.py", "seat_spawn_lock"),
                  ("orchestrator/daily_catchup.py", "exclusive"),
                  ("orchestrator/graph_layout.py", "run_layout_migrate"),
                  ("workers/arq_worker.py", "graph_layout_heartbeat"),
                  ("orchestrator/graph_layout.py", "_try_acquire_layout_lock")]:
        assert known in names, f"the scan no longer sees {known}"
    assert len(names) >= 6


def test_no_session_lock_is_taken_on_a_pool_slot() -> None:
    bad = [(f, n) for f, n, pooled in _holders()
           if pooled and (f, n) not in ALLOWED_POOL_FALLBACK]
    assert not bad, (
        f"{bad}: a session-level advisory lock held on a pool.acquire() slot pins the slot "
        "while the holder keeps calling the pool; use src.db.session_lock.session_lock_connection")


async def test_the_daily_lock_does_not_hold_a_pool_slot(actions: Actions) -> None:
    from src.orchestrator import daily_catchup as dc

    pool = actions.pool
    size = pool.get_max_size()
    async with dc.exclusive(pool, "guard_job") as got:
        assert got is True
        held = [await asyncio.wait_for(pool.acquire(), timeout=5) for _ in range(size)]
        for conn in held:
            await pool.release(conn)


async def test_a_side_lock_dies_with_its_connection(actions: Actions) -> None:
    pool = actions.pool
    async with session_lock_connection(pool) as conn:
        assert await conn.fetchval("SELECT pg_try_advisory_lock(424242)") is True
        async with pool.acquire() as other:
            assert await other.fetchval("SELECT pg_try_advisory_lock(424242)") is False
    async with pool.acquire() as other:
        got = await other.fetchval("SELECT pg_try_advisory_lock(424242)")
        assert got is True  # closing the side connection dropped the lock
        await other.execute("SELECT pg_advisory_unlock(424242)")


async def test_closing_a_side_connection_passes_a_cancel_on(monkeypatch) -> None:
    class Stuck:
        terminated = False

        async def close(self) -> None:
            raise asyncio.CancelledError

        def terminate(self) -> None:
            self.terminated = True

    side = Stuck()
    with pytest.raises(asyncio.CancelledError):
        await close_side_connection(side)  # type: ignore[arg-type]
    assert side.terminated


async def test_without_a_side_connection_it_falls_back_to_a_pool_slot(
        actions: Actions, monkeypatch, caplog) -> None:
    import src.db.session_lock as sl

    async def none(_pool):
        return None

    monkeypatch.setattr(sl, "open_side_connection", none)
    with caplog.at_level("WARNING"):
        async with sl.session_lock_connection(actions.pool) as conn:
            assert await conn.fetchval("SELECT 1") == 1
    assert "pool slot" in caplog.text
