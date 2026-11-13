"""0081: the per-message wake lookup is an index seek, not a scan of agent_wakes.

`trigger._dms_with_unread` asks for the latest wake of each unread message; without an index on
`message_id` that read the whole table once per message and made the worker's `trigger_mail`
cron take a constant ~4.8 s a tick. The index must exist, serve that lookup with no sort, and be
created and dropped CONCURRENTLY (no transaction)."""
from __future__ import annotations

import importlib.util
from pathlib import Path

from src.actions.core import Actions

ROOT = Path(__file__).resolve().parent.parent
_INDEX = "agent_wakes_message_time"
_LOOKUP = "SELECT mode FROM agent_wakes WHERE message_id=$1 ORDER BY woke_at DESC LIMIT 1"


def _run(which: str, *, leftover: bool = False) -> list[str]:
    """The statements the migration runs, each noted with whether it is in an autocommit block."""
    spec = importlib.util.spec_from_file_location(
        "m0081", ROOT / "alembic" / "versions" / "0081_agent_wakes_message_index.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    seen: list[str] = []
    state = {"auto": False}

    class _Block:
        def __enter__(self) -> None:
            state["auto"] = True

        def __exit__(self, *exc: object) -> None:
            state["auto"] = False

    class _Result:
        @staticmethod
        def scalar() -> int | None:
            return 1 if leftover else None

    class _Bind:
        @staticmethod
        def execute(stmt: object, params: object = None) -> _Result:
            seen.append("read: " + str(stmt))
            return _Result()

    class _Op:
        @staticmethod
        def get_bind() -> _Bind:
            return _Bind()

        @staticmethod
        def execute(sql: str) -> None:
            seen.append(("auto: " if state["auto"] else "plain: ") + sql)

        @staticmethod
        def get_context() -> _Op:
            return _Op()

        @staticmethod
        def autocommit_block() -> _Block:
            return _Block()

    module.op = _Op
    getattr(module, which)()
    return seen


def test_the_index_is_built_and_dropped_outside_a_transaction() -> None:
    up, down = _run("upgrade"), _run("downgrade")

    writes = [s for s in up if not s.startswith("read: ")]
    assert len(writes) == 1
    assert writes[0].startswith("auto: CREATE INDEX CONCURRENTLY IF NOT EXISTS")
    assert _INDEX in writes[0] and "(message_id, woke_at DESC)" in writes[0]
    assert len(down) == 1 and down[0].startswith("auto: DROP INDEX CONCURRENTLY IF EXISTS")


def test_a_leftover_invalid_index_is_dropped_and_rebuilt_not_kept() -> None:
    """A failed concurrent build leaves an INVALID index; IF NOT EXISTS would keep it and the
    planner would never use it."""
    writes = [s for s in _run("upgrade", leftover=True) if not s.startswith("read: ")]

    assert [w.split(" IF ")[0] for w in writes] == [
        "auto: DROP INDEX CONCURRENTLY", "auto: CREATE INDEX CONCURRENTLY"]


async def test_the_chain_reaches_0081_and_the_lookup_uses_the_index(actions: Actions) -> None:
    pool = actions.pool
    assert int(await pool.fetchval("SELECT version_num FROM alembic_version")) >= 81
    assert await pool.fetchval("SELECT 1 FROM pg_indexes WHERE indexname=$1", _INDEX) == 1
    for n in range(40):
        await pool.execute(
            "INSERT INTO agent_wakes (to_project, from_agent, message_id, mode) "
            "VALUES ('p', 'a', $1, 'dm-resume')", n)
    async with pool.acquire() as conn, conn.transaction():
        await conn.execute("SET LOCAL enable_seqscan = off")  # a tiny table would otherwise scan
        plan = "\n".join(
            r[0] for r in await conn.fetch("EXPLAIN " + _LOOKUP.replace("$1", "7")))
    assert _INDEX in plan
    assert "Sort" not in plan  # the latest wake is read straight off the index order


async def test_the_invalid_index_check_sees_a_broken_index_and_not_a_good_one(
    actions: Actions,
) -> None:
    """The migration's own query, run against a real index marked invalid the way a failed
    concurrent build leaves it."""
    from src.db.concurrent_index import INVALID_SQL

    sql = INVALID_SQL.replace(":name", "$1")  # sqlalchemy's bind style, asyncpg's
    pool = actions.pool

    assert await pool.fetchval(sql, _INDEX) is None  # the good index is not flagged
    await pool.execute(
        "UPDATE pg_index SET indisvalid = false WHERE indexrelid = $1::regclass", _INDEX)
    try:
        assert await pool.fetchval(sql, _INDEX) == 1
    finally:
        await pool.execute(
            "UPDATE pg_index SET indisvalid = true WHERE indexrelid = $1::regclass", _INDEX)
