"""0079: stored transcript resume points, and the unread-operator-brief lineage index.

The count of one lineage's unread operator briefs (`mailbox.desk_briefs_from`, asked on every
mount) must be an index seek, return exactly what the old `from_agent = base OR LIKE base-%`
predicate returned, and the migration must be creatable and droppable without a transaction
(CONCURRENTLY)."""
from __future__ import annotations

import importlib.util
from datetime import UTC, datetime
from pathlib import Path

from src.actions.core import Actions
from src.orchestrator import mailbox

ROOT = Path(__file__).resolve().parent.parent
_INDEX = "fleet_messages_operator_unread_lineage_idx"


def _statements(which: str) -> tuple[list[str], list[str]]:
    """(plain statements, statements inside an autocommit block) the migration would run."""
    spec = importlib.util.spec_from_file_location(
        "m0079", ROOT / "alembic" / "versions" / "0079_mount_offsets_and_brief_index.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    plain: list[str] = []
    auto: list[str] = []
    state = {"auto": False}

    class _Block:
        def __enter__(self) -> None:
            state["auto"] = True

        def __exit__(self, *exc: object) -> None:
            state["auto"] = False

    class _Op:
        @staticmethod
        def execute(sql: str) -> None:
            (auto if state["auto"] else plain).append(sql)

        @staticmethod
        def get_context() -> _Op:
            return _Op()

        @staticmethod
        def autocommit_block() -> _Block:
            return _Block()

    module.op = _Op
    getattr(module, which)()
    return plain, auto


async def test_the_chain_reaches_0079_with_the_columns_and_the_index(actions: Actions) -> None:
    assert int(await actions.pool.fetchval("SELECT version_num FROM alembic_version")) >= 79
    cols = {r["column_name"] for r in await actions.pool.fetch(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name='harness_sessions'")}
    assert {"last_byte_offset", "last_line_crc", "last_offset_idx"} <= cols
    assert await actions.pool.fetchval(
        "SELECT 1 FROM pg_indexes WHERE tablename='fleet_messages' AND indexname=$1", _INDEX)


def test_the_index_is_built_and_dropped_outside_a_transaction() -> None:
    plain, auto = _statements("upgrade")
    assert any("ADD COLUMN IF NOT EXISTS last_byte_offset" in s for s in plain)
    assert len(auto) == 1 and auto[0].startswith("CREATE INDEX CONCURRENTLY IF NOT EXISTS")
    assert "left(from_agent, 14)" in auto[0] and "to_project = 'operator'" in auto[0]
    plain_down, auto_down = _statements("downgrade")
    assert auto_down == [f"DROP INDEX CONCURRENTLY IF EXISTS {_INDEX}"]
    assert any("DROP COLUMN IF EXISTS last_byte_offset" in s for s in plain_down)


async def _brief(actions: Actions, sender: str, *, read: bool = False) -> None:
    await actions.pool.execute(
        "INSERT INTO fleet_messages (from_agent, from_project, to_project, body, desk_kind, "
        "read_at) VALUES ($1, 'osiris', 'operator', 'b', 'fyi', $2)",
        sender, datetime.now(UTC) if read else None)


async def test_the_lineage_count_matches_the_old_predicate_on_awkward_senders(
    actions: Actions,
) -> None:
    for sender in ("agent:abcd1234", "agent:abcd1234-g2", "agent:abcd1234-xv",
                   "agent:abcd12345",             # 9 hex: a different agent
                   "agent:abcd1234x", "agent:abcd123", "agent:ABCD1234", "agent:abcd1235-g2",
                   "operator", "cron:abcd1234"):
        await _brief(actions, sender)
    await _brief(actions, "agent:abcd1234-g3", read=True)   # settled: not counted
    got = await mailbox.desk_briefs_from(actions.pool, "agent:abcd1234-g9")
    old_sql = ("SELECT count(*) FROM fleet_messages m WHERE "
               + mailbox._DESK_BRIEF_ROW
               + " AND (m.from_agent = $2 OR m.from_agent LIKE $2 || '-%')").replace("$op", "$1")
    old = await actions.pool.fetchval(old_sql, "operator", "agent:abcd1234")
    assert got == old == 3        # the bare id and its two generations, nothing else
    assert await mailbox.desk_briefs_from(actions.pool, None) == 0


async def test_the_lineage_count_seeks_the_index(actions: Actions) -> None:
    # Enough rows, a fresh ANALYZE and one connection for the SET and the EXPLAIN: on a
    # near-empty table the planner may legitimately read the heap, which says nothing.
    await actions.pool.execute(
        "INSERT INTO fleet_messages (from_agent, from_project, to_project, body, desk_kind) "
        "SELECT 'agent:' || lpad(to_hex(g % 90), 8, '0') || '-g1', 'osiris', 'operator', 'b', "
        "'fyi' FROM generate_series(1, 1500) g")
    async with actions.pool.acquire() as conn:
        await conn.execute("ANALYZE fleet_messages")
        await conn.execute("SET enable_seqscan = off")
        q = ("EXPLAIN SELECT count(*) FROM fleet_messages m WHERE "
             + mailbox._DESK_BRIEF_ROW.replace("$op", "'operator'")
             + " AND left(m.from_agent, 14) = $1"
             + " AND (length(m.from_agent) = 14 OR substr(m.from_agent, 15, 1) = '-')")
        plan = "\n".join(r[0] for r in await conn.fetch(q, "agent:00000007"))
    assert _INDEX in plan, plan
