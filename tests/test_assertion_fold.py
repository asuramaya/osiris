"""Folding no-op re-assertions: a superseded row that restates the row it replaced, on every
provenance field, is deleted and its time folded onto the survivor; nothing else moves."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from src.actions.core import Actions
from src.orchestrator.assertion_fold import apply_fold, compact_assertion_tables, plan_fold

LONG_AGO = datetime.now(UTC) - timedelta(days=30)


async def _row(actions: Actions, obj: Any, value: Any, *, supersedes: int | None,
               current: bool, age_days: int = 30, minutes: int = 0, source: str = "src:fold",
               confidence: float = 0.9, evidence_uri: str | None = None) -> int:
    """A legacy row written straight into the table with an old transaction timestamp."""
    when = datetime.now(UTC) - timedelta(days=age_days) + timedelta(minutes=minutes)
    return int(await actions.pool.fetchval(
        "INSERT INTO assertions (object_id, name, value, source_id, observed_at, confidence, "
        " supersedes, evidence_class, is_current, created_at, evidence_uri) "
        "VALUES ($1,'label',$2,$3,$4,$5,$6,'authoritative_api',$7,$4,$8) RETURNING id",
        obj, value, source, when, confidence, supersedes, current, evidence_uri))


async def _chain(actions: Actions, canonical: str) -> dict[str, Any]:
    """r1 = r2 = r3 (same value), then r4 (different), then r5 (current, same as r4)."""
    obj = await actions.create_or_find_object("Domain", canonical, "analyst:test")
    r1 = await _row(actions, obj, "A", supersedes=None, current=False, minutes=0)
    r2 = await _row(actions, obj, "A", supersedes=r1, current=False, minutes=1)
    r3 = await _row(actions, obj, "A", supersedes=r2, current=False, minutes=2)
    r4 = await _row(actions, obj, "B", supersedes=r3, current=False, minutes=3)
    r5 = await _row(actions, obj, "B", supersedes=r4, current=True, minutes=4)
    return {"obj": obj, "ids": [r1, r2, r3, r4, r5]}


async def _ids(actions: Actions, ids: list[int]) -> set[int]:
    rows = await actions.pool.fetch("SELECT id FROM assertions WHERE id = ANY($1::bigint[])", ids)
    return {r["id"] for r in rows}


async def _supersedes(actions: Actions, row_id: int) -> int | None:
    return await actions.pool.fetchval("SELECT supersedes FROM assertions WHERE id=$1", row_id)


async def test_a_run_of_identical_rows_collapses_to_the_first(actions: Actions) -> None:
    c = await _chain(actions, "fold-1.example")
    r1, r2, r3, r4, r5 = c["ids"]
    out = await apply_fold(actions.pool)
    assert out["executed"] is True and out["finished"] is True and out["folded"] >= 2
    # r2 and r3 restated r1 and are gone; the different value and the current row stay
    assert await _ids(actions, c["ids"]) == {r1, r4, r5}
    # the chain is still connected, with the survivor in the dead rows' place
    assert await _supersedes(actions, r1) is None
    assert await _supersedes(actions, r4) == r1
    assert await _supersedes(actions, r5) == r4


async def test_the_survivor_keeps_the_latest_time_the_fact_was_observed(
    actions: Actions,
) -> None:
    c = await _chain(actions, "fold-2.example")
    r1, _r2, r3 = c["ids"][:3]
    latest = await actions.pool.fetchval("SELECT observed_at FROM assertions WHERE id=$1", r3)
    await apply_fold(actions.pool)
    assert await actions.pool.fetchval(
        "SELECT observed_at FROM assertions WHERE id=$1", r1) == latest


async def test_the_current_row_is_never_deleted_and_stays_current(actions: Actions) -> None:
    c = await _chain(actions, "fold-3.example")
    await apply_fold(actions.pool)
    r5 = c["ids"][4]
    assert await actions.pool.fetchval(
        "SELECT is_current FROM current_assertions WHERE id=$1", r5) is True
    assert await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name='label'",
        c["obj"]) == "B"


async def test_a_run_ending_in_the_current_row_leaves_it_pointing_at_the_survivor(
    actions: Actions,
) -> None:
    obj = await actions.create_or_find_object("Domain", "fold-4.example", "analyst:test")
    r1 = await _row(actions, obj, "A", supersedes=None, current=False, minutes=0)
    r2 = await _row(actions, obj, "A", supersedes=r1, current=False, minutes=1)
    r3 = await _row(actions, obj, "A", supersedes=r2, current=True, minutes=2)
    await apply_fold(actions.pool)
    assert await _ids(actions, [r1, r2, r3]) == {r1, r3}
    assert await _supersedes(actions, r3) == r1


async def test_young_rows_are_left_alone(actions: Actions) -> None:
    obj = await actions.create_or_find_object("Domain", "fold-5.example", "analyst:test")
    r1 = await _row(actions, obj, "A", supersedes=None, current=False, age_days=1)
    r2 = await _row(actions, obj, "A", supersedes=r1, current=False, age_days=1, minutes=1)
    r3 = await _row(actions, obj, "A", supersedes=r2, current=True, age_days=1, minutes=2)
    await apply_fold(actions.pool)
    assert await _ids(actions, [r1, r2, r3]) == {r1, r2, r3}


async def test_rows_that_differ_on_any_provenance_field_are_not_folded(
    actions: Actions,
) -> None:
    obj = await actions.create_or_find_object("Domain", "fold-6.example", "analyst:test")
    r1 = await _row(actions, obj, "A", supersedes=None, current=False, minutes=0)
    r2 = await _row(actions, obj, "A", supersedes=r1, current=False, minutes=1,
                    evidence_uri="file:///evidence-two")           # different evidence
    r3 = await _row(actions, obj, "A", supersedes=r2, current=False, minutes=2,
                    evidence_uri="file:///evidence-two", confidence=0.5)  # different confidence
    r4 = await _row(actions, obj, "A", supersedes=r3, current=True, minutes=3,
                    evidence_uri="file:///evidence-two", confidence=0.5)
    await apply_fold(actions.pool)
    assert await _ids(actions, [r1, r2, r3, r4]) == {r1, r2, r3, r4}


async def test_a_second_run_finds_nothing_more(actions: Actions) -> None:
    await _chain(actions, "fold-7.example")
    await apply_fold(actions.pool)
    again = await apply_fold(actions.pool)
    assert again["folded"] == 0


async def test_small_windows_fold_the_same_rows_as_one_big_one(actions: Actions) -> None:
    c = await _chain(actions, "fold-8.example")
    r1, _r2, _r3, r4, r5 = c["ids"]
    out = await apply_fold(actions.pool, window=1)
    assert out["finished"] is True
    assert await _ids(actions, c["ids"]) == {r1, r4, r5}
    assert await _supersedes(actions, r4) == r1


async def test_a_time_budget_stops_between_windows_and_the_next_run_finishes(
    actions: Actions,
) -> None:
    c = await _chain(actions, "fold-9.example")
    first = await apply_fold(actions.pool, window=1, max_seconds=0.0)
    assert first["finished"] is False
    await apply_fold(actions.pool)
    r1, _r2, _r3, r4, r5 = c["ids"]
    assert await _ids(actions, c["ids"]) == {r1, r4, r5}


async def test_the_history_a_reader_walks_stays_correct(actions: Actions) -> None:
    """Walk r5 back through `supersedes`: the values read the same, minus the repeats."""
    c = await _chain(actions, "fold-10.example")
    r5 = c["ids"][4]

    async def _walk() -> list[str]:
        out: list[str] = []
        cursor: int | None = r5
        while cursor is not None:
            row = await actions.pool.fetchrow(
                "SELECT value #>> '{}' AS v, supersedes FROM assertions WHERE id=$1", cursor)
            assert row is not None
            out.append(row["v"])
            cursor = row["supersedes"]
        return out

    assert await _walk() == ["B", "B", "A", "A", "A"]
    await apply_fold(actions.pool)
    assert await _walk() == ["B", "B", "A"]


async def test_the_dry_run_counts_without_deleting(actions: Actions) -> None:
    c = await _chain(actions, "fold-11.example")
    out = await plan_fold(actions.pool)
    assert out["executed"] is False and out["eligible"] >= 2
    assert await _ids(actions, c["ids"]) == set(c["ids"])


async def test_cold_rows_fold_too(actions: Actions) -> None:
    """A run split across the tiers: the old rows already moved to the cold table."""
    from src.orchestrator.migration_0064 import apply_migration_0064

    c = await _chain(actions, "fold-12.example")
    r1, _r2, _r3, r4, r5 = c["ids"]
    await apply_migration_0064(actions.pool, cutoff=datetime.now(UTC) - timedelta(days=1))
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM assertions_cold WHERE id = ANY($1::bigint[])", c["ids"]) >= 3
    await apply_fold(actions.pool)
    assert await _ids(actions, c["ids"]) == {r1, r4, r5}
    assert await _supersedes(actions, r4) == r1


async def test_compacting_reports_both_tables(actions: Actions) -> None:
    out = await compact_assertion_tables(actions.pool)
    assert set(out) == {"assertions_hot", "assertions_cold"}
    assert all(v["after_bytes"] <= v["before_bytes"] + 65536 for v in out.values())


async def test_the_archiver_stops_between_batches_when_its_budget_is_spent(
    actions: Actions,
) -> None:
    from src.orchestrator.migration_0064 import apply_migration_0064

    c = await _chain(actions, "fold-13.example")
    cutoff = datetime.now(UTC) - timedelta(days=1)
    first = await apply_migration_0064(
        actions.pool, cutoff=cutoff, batch_size=1, max_seconds=0.0)
    assert first["rows_moved"] == 1 and first["count_preserved"] is True
    rest = await apply_migration_0064(actions.pool, cutoff=cutoff)
    assert rest["rows_moved"] >= 3
    assert await _ids(actions, c["ids"]) == set(c["ids"])


async def test_the_daily_job_folds_then_moves_old_history_and_says_so(
    actions: Actions, monkeypatch: Any,
) -> None:
    from types import SimpleNamespace

    import src.orchestrator.mailbox as mailbox
    from src.workers.arq_worker import storage_housekeeping_heartbeat

    c = await _chain(actions, "fold-14.example")
    r1, _r2, _r3, r4, r5 = c["ids"]
    captured: dict[str, Any] = {}

    async def _fake_send(pool: Any, **kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return {"sent": 1}

    monkeypatch.setattr(mailbox, "send_message", _fake_send)
    changed = await storage_housekeeping_heartbeat({"cascade": SimpleNamespace(actions=actions)})

    assert changed >= 2 + 2  # two folded, then the superseded survivors moved to cold
    assert await _ids(actions, c["ids"]) == {r1, r4, r5}
    assert "folded" in captured["body"] and "cold table" in captured["body"]
    # the superseded survivors left the hot table; the current row never does
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM assertions_hot WHERE id = ANY($1::bigint[])", [r1, r4]) == 0
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM assertions_hot WHERE id=$1 AND is_current", r5) == 1
