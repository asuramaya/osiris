"""`repack_cleanup`: find what a killed pg_repack left behind (its trigger on the target table
and the log and copy tables in the `repack` schema) and, only when told, drop exactly that.
The test database has no pg_repack, so a killed run is simulated with the same objects. No test
reaches a real container: `docker exec` is replaced by a fake."""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest
from src.actions.core import Actions
from src.orchestrator import repack_cleanup
from src.orchestrator.backfill import BACKFILL_TARGETS, run_backfill
from src.orchestrator.repack_cleanup import find_leftovers

TABLE = "repack_cleanup_scratch"


class Container:
    """A fake `docker exec`: (exit code, output) for `pidof pg_repack`."""

    def __init__(self, code: int = 1) -> None:
        self.code = code
        self.calls: list[list[str]] = []

    async def __call__(self, args: list[str], timeout_secs: float) -> tuple[int, str]:
        self.calls.append(args)
        return self.code, ""


@pytest.fixture(autouse=True)
def _never_a_real_container(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(repack_cleanup, "_docker_exec", Container(1))


@pytest.fixture
async def killed_run(actions: Actions) -> AsyncIterator[None]:
    """What a SIGKILLed repack of TABLE leaves: the trigger, the log table, the copy and the
    type, in a `repack` schema with pg_repack's own trigger function."""
    pool = actions.pool
    await pool.execute("DROP SCHEMA IF EXISTS repack CASCADE")
    await pool.execute("CREATE SCHEMA repack")
    await pool.execute(
        "CREATE FUNCTION repack.repack_trigger() RETURNS trigger LANGUAGE plpgsql AS "
        "$$ BEGIN RETURN NEW; END $$")
    await pool.execute(f"DROP TABLE IF EXISTS {TABLE}")
    await pool.execute(f"CREATE TABLE {TABLE} (id int primary key, v text)")
    await pool.execute(f"INSERT INTO {TABLE} SELECT g, 'x' FROM generate_series(1, 20) g")
    await pool.execute(
        f"CREATE TRIGGER repack_trigger AFTER INSERT OR UPDATE OR DELETE ON {TABLE} "
        "FOR EACH ROW EXECUTE FUNCTION repack.repack_trigger()")
    await pool.execute("CREATE TABLE repack.log_16384 (id bigserial primary key, pk text)")
    await pool.execute("CREATE TABLE repack.table_16384 (id int, v text)")
    await pool.execute("CREATE TYPE repack.pk_16384 AS (id int)")
    try:
        yield
    finally:
        await pool.execute("DROP SCHEMA IF EXISTS repack CASCADE")
        await pool.execute(f"DROP TABLE IF EXISTS {TABLE}")


async def test_a_clean_database_has_nothing_to_clean(actions: Actions) -> None:
    out = await repack_cleanup.repack_cleanup(actions.pool, actor="test")
    assert out["dry_run"] is True and out["leftover"] is False and out["steps"] == []
    assert out["running"] is False and out["extension_installed"] is False


async def test_the_dry_run_names_every_leftover_and_the_statements_and_drops_nothing(
    actions: Actions, killed_run: None,
) -> None:
    out = await repack_cleanup.repack_cleanup(actions.pool, actor="test")
    assert out["leftover"] is True and out["running"] is False
    assert [t["on_table"] for t in out["triggers"]] == [TABLE]
    assert {r["name"] for r in out["relations"]} == {"log_16384", "table_16384", "pk_16384"}
    assert out["steps"][0] == f"DROP TRIGGER repack_trigger ON {TABLE}"
    assert set(out["steps"][1:]) == {
        "DROP TABLE repack.log_16384", "DROP TABLE repack.table_16384",
        "DROP TYPE repack.pk_16384"}
    assert "DROP EXTENSION pg_repack CASCADE" in out["fallback"]
    assert len((await find_leftovers(actions.pool))["steps"]) == 4          # nothing dropped


async def test_an_apply_needs_a_because(actions: Actions, killed_run: None) -> None:
    out = await repack_cleanup.repack_cleanup(actions.pool, actor="test", dry_run=False)
    assert "error" in out
    assert (await find_leftovers(actions.pool))["leftover"] is True


async def test_the_apply_drops_exactly_the_leftovers_keeps_the_data_and_is_audited(
    actions: Actions, killed_run: None,
) -> None:
    out = await repack_cleanup.repack_cleanup(
        actions.pool, actor="test-actor", dry_run=False, because="a killed night")
    assert out["finished"] is True and out["failed"] == [] and out["remaining"] == []
    assert len(out["dropped"]) == 4
    assert (await find_leftovers(actions.pool))["leftover"] is False
    assert await actions.pool.fetchval(f"SELECT count(*) FROM {TABLE}") == 20
    assert out["audit"]["recorded"] is True
    summary = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a "
        "WHERE a.object_id = $1::uuid AND a.name='summary'", out["audit"]["decision"])
    assert "test-actor" in summary and "a killed night" in summary
    assert f"DROP TRIGGER repack_trigger ON {TABLE}" in summary
    again = await repack_cleanup.repack_cleanup(
        actions.pool, actor="test", dry_run=False, because="again")
    assert again["leftover"] is False and "audit" not in again          # idempotent


async def test_it_refuses_while_a_repack_process_runs_in_the_container(
    actions: Actions, killed_run: None,
) -> None:
    running = Container(0)                                   # `pidof pg_repack` finds one
    out = await repack_cleanup.repack_cleanup(
        actions.pool, actor="test", dry_run=False, because="x", exec_fn=running)
    assert out["running"] is True and "in progress" in out["error"]
    assert (await find_leftovers(actions.pool, running))["leftover"] is True   # nothing dropped


async def test_it_refuses_while_a_backend_is_working_on_a_repack_object(
    actions: Actions, killed_run: None,
) -> None:
    busy = asyncio.create_task(actions.pool.execute(
        "SELECT pg_sleep(4) /* INSERT INTO repack.table_16384 SELECT 1 */"))
    await asyncio.sleep(0.5)
    try:
        out = await repack_cleanup.repack_cleanup(
            actions.pool, actor="test", dry_run=False, because="x", exec_fn=Container(1))
        assert out["running"] is True and "in progress" in out["error"]
    finally:
        await asyncio.wait_for(busy, 20)
    assert (await find_leftovers(actions.pool, Container(1)))["running"] is False


async def test_a_container_that_cannot_be_asked_is_said_so_and_the_apply_still_runs(
    actions: Actions, killed_run: None,
) -> None:
    out = await repack_cleanup.repack_cleanup(
        actions.pool, actor="test", dry_run=False, because="x", exec_fn=Container(127))
    assert out["container_checked"] is False and out["finished"] is True


async def test_a_statement_that_cannot_run_is_reported_and_the_rest_still_runs(
    actions: Actions, killed_run: None, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(repack_cleanup, "LOCK_TIMEOUT", "100ms")
    holder = await actions.pool.acquire()
    tx = holder.transaction()
    await tx.start()
    await holder.execute(f"LOCK TABLE {TABLE} IN ACCESS EXCLUSIVE MODE")
    try:
        out = await repack_cleanup.repack_cleanup(
            actions.pool, actor="test", dry_run=False, because="x", exec_fn=Container(1))
    finally:
        await tx.rollback()
        await actions.pool.release(holder)
    assert out["finished"] is False
    assert [f["statement"] for f in out["failed"]] == [f"DROP TRIGGER repack_trigger ON {TABLE}"]
    assert "lock" in out["failed"][0]["error"].lower()
    assert len(out["dropped"]) == 3 and out["remaining"] == [
        f"DROP TRIGGER repack_trigger ON {TABLE}"]
    assert out["audit"]["recorded"] is True


async def test_it_is_a_backfill_target_and_dry_run_by_default(
    actions: Actions, killed_run: None,
) -> None:
    assert "repack_cleanup" in BACKFILL_TARGETS
    out: dict[str, Any] = await run_backfill(actions.pool, "repack_cleanup", actor="test")
    assert out["dry_run"] is True and out["leftover"] is True
    assert (await find_leftovers(actions.pool))["leftover"] is True
