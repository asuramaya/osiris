"""0078: a (type, canonical text_pattern_ops) index so a lineage's generations
(`canonical = $1 OR canonical LIKE $1 || '-%'`) are a range seek on a non-C collation, not a
sequential scan of objects on every ingest."""
from __future__ import annotations

import importlib.util
from datetime import UTC, datetime
from pathlib import Path

from src.actions.core import Actions
from src.orchestrator.agents import lineage_works_in

ROOT = Path(__file__).resolve().parent.parent
_INDEX = "objects_type_canonical_pattern_idx"


def _statements(which: str) -> list[str]:
    spec = importlib.util.spec_from_file_location(
        "m0078", ROOT / "alembic" / "versions" / "0078_agent_lineage_prefix_index.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    captured: list[str] = []

    class _Block:
        def __enter__(self) -> None:
            return None

        def __exit__(self, *exc: object) -> None:
            return None

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

    module.op = _Op
    getattr(module, which)()
    return captured


async def test_the_chain_reaches_0078_and_the_index_exists(actions: Actions) -> None:
    assert int(await actions.pool.fetchval("SELECT version_num FROM alembic_version")) >= 78
    assert await actions.pool.fetchval(
        "SELECT 1 FROM pg_indexes WHERE tablename='objects' AND indexname=$1", _INDEX)


async def test_the_lineage_lookup_seeks_the_index_instead_of_scanning_objects(
    actions: Actions,
) -> None:
    # Deterministic plan choice: on a near-empty table the planner may serve the type
    # predicate from objects_type_idx and filter the rest, which is cheap there and says
    # nothing about the index. Seed enough Agent rows that the type-only path is clearly
    # the costlier one, ANALYZE, and run the SET and the EXPLAIN on ONE connection (a
    # pool.execute SET may land on a different connection than the EXPLAIN).
    for i in range(400):
        await actions.create_or_find_object("Agent", f"agent:seed{i:04d}", "test")
    async with actions.pool.acquire() as conn:
        await conn.execute("ANALYZE objects")
        await conn.execute("SET enable_seqscan = off")
        plan = "\n".join(r[0] for r in await conn.fetch(
            "EXPLAIN SELECT a.id FROM objects a WHERE a.type='Agent' "
            "AND (a.canonical=$1 OR a.canonical LIKE $1 || '-%')", "agent:aad6603a"))
        await conn.execute("RESET enable_seqscan")
    assert _INDEX in plan, plan


async def test_the_lookup_still_answers_the_same_lineage(actions: Actions) -> None:
    project = await actions.create_or_find_object("SoftwareProject", "repo:lineageproj", "test")
    for canonical in ("agent:lin0a1b2", "agent:lin0a1b2-ii", "agent:lin0a1b2-g40"):
        agent = await actions.create_or_find_object("Agent", canonical, "test")
        await actions.create_link(agent, project, "works_in", "test",
                                  datetime.now(UTC), 0.9)
    other = await actions.create_or_find_object("Agent", "agent:lin0a1b2other-ii", "test")
    other_project = await actions.create_or_find_object("SoftwareProject", "repo:elseproj", "test")
    await actions.create_link(other, other_project, "works_in", "test",
                              datetime.now(UTC), 0.9)

    got = await lineage_works_in(actions.pool, "agent:lin0a1b2-ii")

    assert got["projects"] == ["lineageproj"]  # the lookalike prefix is a different lineage


async def test_the_upgrade_is_idempotent_and_concurrent(actions: Actions) -> None:
    up = _statements("upgrade")
    assert len(up) == 1 and "CONCURRENTLY IF NOT EXISTS" in up[0] and "text_pattern_ops" in up[0]
    down = _statements("downgrade")
    assert len(down) == 1 and "DROP INDEX CONCURRENTLY IF EXISTS" in down[0]
