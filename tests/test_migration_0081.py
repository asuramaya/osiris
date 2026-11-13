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


def _run(which: str) -> list[str]:
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

    class _Op:
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

    assert len(up) == 1 and up[0].startswith("auto: CREATE INDEX CONCURRENTLY IF NOT EXISTS")
    assert _INDEX in up[0] and "(message_id, woke_at DESC)" in up[0]
    assert len(down) == 1 and down[0].startswith("auto: DROP INDEX CONCURRENTLY IF EXISTS")


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
