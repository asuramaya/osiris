"""0077: objects by canonical alone (a lookup that names an object without its type seeks
instead of walking the (type, canonical) index end to end), and a partial index on the
in-flight message recipients (the count every statusline render makes)."""
from __future__ import annotations

import importlib.util
from pathlib import Path

from src.actions.core import Actions

ROOT = Path(__file__).resolve().parent.parent


def _statements(which: str) -> list[str]:
    spec = importlib.util.spec_from_file_location(
        "m0077", ROOT / "alembic" / "versions" / "0077_statusline_indexes.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    captured: list[str] = []

    class _Op:
        @staticmethod
        def execute(sql: str) -> None:
            captured.append(sql)

        @staticmethod
        def get_context() -> _Op:
            return _Op()

        @staticmethod
        def autocommit_block() -> _Block:
            return _Block()

    class _Block:
        def __enter__(self) -> None:
            return None

        def __exit__(self, *exc: object) -> None:
            return None

    module.op = _Op
    getattr(module, which)()
    return captured


async def _has_index(actions: Actions) -> bool:
    return bool(await actions.pool.fetchval(
        "SELECT 1 FROM pg_indexes WHERE tablename='objects' AND indexname='objects_canonical_idx'"))


async def test_the_chain_reaches_0077_and_the_index_exists(actions: Actions) -> None:
    assert int(await actions.pool.fetchval("SELECT version_num FROM alembic_version")) >= 77
    assert await _has_index(actions)


async def test_a_canonical_only_lookup_uses_it(actions: Actions) -> None:
    await actions.pool.execute("SET enable_seqscan = off")
    plan = "\n".join(r[0] for r in await actions.pool.fetch(
        "EXPLAIN SELECT id FROM objects WHERE canonical = 'seat:0abe4f62'"))
    assert "objects_canonical_idx" in plan, plan


async def test_the_inflight_count_uses_the_partial_index(actions: Actions) -> None:
    assert await actions.pool.fetchval(
        "SELECT 1 FROM pg_indexes WHERE indexname='message_recipients_inflight_idx'")
    await actions.pool.execute("SET enable_seqscan = off")
    plan = "\n".join(r[0] for r in await actions.pool.fetch(
        "EXPLAIN SELECT count(*) FROM message_recipients r WHERE r.read_at IS NULL "
        "AND r.delivered_at IS NOT NULL AND r.delivered_at >= now() - interval '1 hour'"))
    assert "message_recipients_inflight_idx" in plan, plan


async def test_the_upgrade_is_idempotent_and_concurrent(actions: Actions) -> None:
    sql = _statements("upgrade")
    assert len(sql) == 2 and all("CONCURRENTLY IF NOT EXISTS" in q for q in sql)
    down = _statements("downgrade")
    assert len(down) == 2 and all("DROP INDEX CONCURRENTLY IF EXISTS" in q for q in down)
