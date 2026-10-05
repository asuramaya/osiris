"""0076 creates pgstattuple for the night compaction, and only drops it again on a downgrade
if this migration is what created it."""
from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any

from src.actions.core import Actions

ROOT = Path(__file__).resolve().parent.parent
MARKER = "migration:0076:created_pgstattuple"


def _statements(which: str) -> list[str]:
    spec = importlib.util.spec_from_file_location(
        "m0076", ROOT / "alembic" / "versions" / "0076_pgstattuple.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    captured: list[str] = []

    class _Op:
        @staticmethod
        def execute(sql: str) -> None:
            captured.append(sql)

    module.op = _Op
    getattr(module, which)()
    return captured


async def _installed(actions: Actions) -> bool:
    return bool(await actions.pool.fetchval(
        "SELECT 1 FROM pg_extension WHERE extname = 'pgstattuple'"))


async def _marked(actions: Actions) -> bool:
    return bool(await actions.pool.fetchval("SELECT 1 FROM watermarks WHERE key = $1", MARKER))


async def _run(actions: Actions, statements: list[str]) -> None:
    for sql in statements:
        await actions.pool.execute(sql)


async def test_the_chain_ends_at_0076_and_the_extension_is_there(actions: Actions) -> None:
    assert await actions.pool.fetchval(
        "SELECT version_num FROM alembic_version") == "0076"
    assert await _installed(actions)


async def test_the_upgrade_is_idempotent(actions: Actions) -> None:
    await _run(actions, _statements("upgrade"))
    await _run(actions, _statements("upgrade"))
    assert await _installed(actions)


async def test_a_downgrade_drops_the_extension_only_when_this_migration_created_it(
    actions: Actions,
) -> None:
    # the per-test reset clears watermarks, so record what the migration itself would have
    await actions.pool.execute(
        "INSERT INTO watermarks (key, cursor) VALUES ($1, '1') ON CONFLICT (key) DO NOTHING",
        MARKER)
    await _run(actions, _statements("downgrade"))
    assert not await _installed(actions) and not await _marked(actions)
    # back up again: created again, marked again
    await _run(actions, _statements("upgrade"))
    assert await _installed(actions) and await _marked(actions)


async def test_a_hand_installed_extension_survives_a_downgrade(actions: Actions) -> None:
    await actions.pool.execute("DELETE FROM watermarks WHERE key = $1", MARKER)  # not ours
    await _run(actions, _statements("downgrade"))
    assert await _installed(actions)
    await _run(actions, _statements("upgrade"))   # already there: nothing recorded, no error
    assert await _installed(actions) and not await _marked(actions)


async def test_the_compaction_measures_with_the_extension(actions: Actions) -> None:
    from src.orchestrator.db_compaction import reclaimable_bytes

    _: Any = await reclaimable_bytes(actions.pool, "audit_log")
    assert _[1] == "pgstattuple_approx"
