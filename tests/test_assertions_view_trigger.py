"""The `assertions` view's INSTEAD OF UPDATE trigger changes only the columns the statement
changed. It used to write every column back from the statement's own snapshot, so a bump of
`observed_at` or a flip of `is_current` made through the view could undo what a concurrent
writer had just committed to the same row (a supersede's flag, a fold's or a collapse's
repointed `supersedes`). Each test holds the other writer's transaction open, starts the view
update (which then waits on the row lock), commits the writer, and checks the writer's change
survived. Real Postgres through the `actions` fixture."""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

from src.actions.core import Actions
from src.orchestrator import assertion_fold
from src.orchestrator.retirement import repair_stale_current_flags, retire_assertion

T0 = datetime(2026, 1, 1, tzinfo=UTC)


async def _insert(
    actions: Actions, obj: Any, name: str, value: str, *, at: datetime, supersedes: int | None,
    current: bool, created: datetime | None = None, table: str = "assertions_hot",
) -> int:
    return int(await actions.pool.fetchval(
        f"INSERT INTO {table} (id, object_id, name, value, source_id, observed_at, created_at, "
        "confidence, supersedes, is_current) VALUES (nextval('assertions_id_seq'), $1, $2, "
        "$3::jsonb, 'src:t', $4, $5, 0.9, $6, $7) RETURNING id",
        obj, name, value, at, created or at, supersedes, current))


async def _row(actions: Actions, row_id: int) -> Any:
    return await actions.pool.fetchrow(
        "SELECT id, is_current, supersedes, observed_at FROM assertions WHERE id=$1", row_id)


async def _let_it_block() -> None:
    await asyncio.sleep(0.5)      # the view update reaches the trigger and waits on the row lock


async def test_a_bump_racing_a_retirement_cannot_make_the_retired_row_current_again(
    actions: Actions, case_id: str,
) -> None:
    obj = await actions.create_or_find_object("Domain", "trig-retire.com", "analyst:t", case_id)
    old = await actions.assert_property(obj, "p", "A", "src:t", T0, 0.9)
    async with actions.atomic() as inner:
        done = await retire_assertion(
            inner, ref=str(obj), name="p", superseded_id=old, value="B",
            because="trigger test", actor="analyst:t")
        assert "error" not in done
        bump = asyncio.create_task(actions.pool.execute(
            "UPDATE assertions SET observed_at=$2 WHERE id=$1 AND is_current AND observed_at < $2",
            old, T0 + timedelta(days=1)))
        await _let_it_block()
        assert not bump.done()
    await asyncio.wait_for(bump, 20)
    after = await _row(actions, old)
    assert after["is_current"] is False
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM assertions WHERE object_id=$1 AND name='p' AND is_current",
        obj) == 1


async def test_a_bump_racing_a_fold_keeps_the_fold_repointed_supersedes(
    actions: Actions, case_id: str,
) -> None:
    obj = await actions.create_or_find_object("Domain", "trig-fold.com", "analyst:t", case_id)
    val = '"same"'
    r1 = await _insert(actions, obj, "p", val, at=T0, supersedes=None, current=False)
    r2 = await _insert(actions, obj, "p", val, at=T0 + timedelta(days=1), supersedes=r1,
                       current=False)
    r3 = await _insert(actions, obj, "p", val, at=T0 + timedelta(days=2), supersedes=r2,
                       current=True)
    cutoff = datetime.now(UTC)
    pool = actions.pool
    async with pool.acquire() as writer, writer.transaction():
        rows = await writer.fetch(assertion_fold._CANDIDATES_SQL, 0, r3, cutoff)
        assert [r["id"] for r in rows] == [r2]
        await assertion_fold._fold_window(writer, rows)       # r3 now points at r1, r2 is gone
        bump = asyncio.create_task(pool.execute(
            "UPDATE assertions SET observed_at=$2 WHERE id=$1 AND is_current AND observed_at < $2",
            r3, T0 + timedelta(days=9)))
        await _let_it_block()
        assert not bump.done()
    await asyncio.wait_for(bump, 20)
    after = await _row(actions, r3)
    assert after["supersedes"] == r1                          # not the stale pointer to r2
    assert after["observed_at"] == T0 + timedelta(days=9)
    assert await _row(actions, r2) is None


async def test_a_flag_repair_racing_a_repointing_writer_keeps_the_new_pointer(
    actions: Actions, case_id: str,
) -> None:
    """A row that is wrongly still current (superseded by a later row) is flipped by the flag
    repair through the view, while the collapse or the fold repoints the same row."""
    obj = await actions.create_or_find_object("Domain", "trig-flag.com", "analyst:t", case_id)
    first = await _insert(actions, obj, "p", '"x"', at=T0, supersedes=None, current=False)
    other = await _insert(actions, obj, "p", '"y"', at=T0 + timedelta(hours=1), supersedes=None,
                          current=False)
    stale = await _insert(actions, obj, "p", '"z"', at=T0 + timedelta(days=1), supersedes=first,
                          current=True)
    await _insert(actions, obj, "p", '"w"', at=T0 + timedelta(days=2), supersedes=stale,
                  current=True)
    pool = actions.pool
    async with pool.acquire() as writer, writer.transaction():
        await writer.execute("UPDATE assertions_hot SET supersedes=$2 WHERE id=$1", stale, other)
        flip = asyncio.create_task(repair_stale_current_flags(
            actions, dry_run=False, limit=10, actor="trigger-test"))
        await _let_it_block()
        assert not flip.done()
    out = await asyncio.wait_for(flip, 20)
    assert out["repaired"] >= 1
    after = await _row(actions, stale)
    assert after["is_current"] is False
    assert after["supersedes"] == other                       # the repoint survived the flip


async def test_the_view_update_still_changes_every_column_it_is_asked_to(
    actions: Actions, case_id: str,
) -> None:
    obj = await actions.create_or_find_object("Domain", "trig-all.com", "analyst:t", case_id)
    hot = await _insert(actions, obj, "p", '"a"', at=T0, supersedes=None, current=True)
    cold = await _insert(actions, obj, "p", '"b"', at=T0, supersedes=None, current=False,
                         table="assertions_cold")
    later = T0 + timedelta(days=3)
    pool = actions.pool
    await pool.execute(
        "UPDATE assertions SET observed_at=$2, confidence=0.5, value='\"c\"'::jsonb, "
        "evidence_class='observed', supersedes=$3, case_id=NULL, is_current=false "
        "WHERE id=$1", hot, later, cold)
    changed = await pool.fetchrow(
        "SELECT observed_at, confidence, value #>> '{}' AS v, evidence_class, supersedes, "
        "is_current FROM assertions_hot WHERE id=$1", hot)
    assert changed["observed_at"] == later and abs(changed["confidence"] - 0.5) < 1e-6
    assert changed["v"] == "c" and changed["evidence_class"] == "observed"
    assert changed["supersedes"] == cold and changed["is_current"] is False
    await pool.execute("UPDATE assertions SET observed_at=$2 WHERE id=$1", cold, later)
    assert (await _row(actions, cold))["observed_at"] == later
    assert await pool.fetchval("SELECT count(*) FROM assertions_hot WHERE id=$1", cold) == 0


async def test_the_migration_installs_the_changed_columns_trigger_and_downgrades_cleanly(
    actions: Actions,
) -> None:
    import importlib.util
    from pathlib import Path

    spec = importlib.util.spec_from_file_location(
        "m0080", Path(__file__).resolve().parent.parent / "alembic" / "versions"
        / "0080_assertions_view_update_changed_columns.py")
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    pool = actions.pool

    async def body() -> str:
        return str(await pool.fetchval(
            "SELECT prosrc FROM pg_proc WHERE proname='assertions_view_update'"))

    assert int(await pool.fetchval("SELECT version_num FROM alembic_version")) >= 80
    assert "IS DISTINCT FROM" in await body()
    try:
        await pool.execute(migration._function(migration._every_column))       # the downgrade
        old = await body()
        assert "IS DISTINCT FROM" not in old and "is_current=NEW.is_current" in old
    finally:
        await pool.execute(migration._function(migration._only_changed))       # the upgrade
    assert "IS DISTINCT FROM" in await body()
