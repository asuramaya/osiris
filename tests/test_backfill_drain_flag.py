"""A bulk-delete backfill that has not finished holds a drain flag of its own, so the night
compaction does not rewrite the assertions tables in the middle of it. The nightly housekeeping
overwrites its own flag every pass, so this one is separate. Real Postgres through `actions`."""
from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from src.actions.core import Actions
from src.orchestrator import abstention_churn
from src.orchestrator.backfill import run_backfill
from src.orchestrator.db_compaction import (
    BACKFILL_DRAIN_KEY,
    drain_running,
    set_backfill_drain,
    set_drain,
)

_NAME = "derivation_abstained_in_repo"
_TARGET = "abstention_churn_collapse"


async def _seed_chain(actions: Actions, tag: str, rows: int) -> None:
    obj = await actions.create_or_find_object("Decision", tag, "test")
    base = datetime.now(UTC) - timedelta(days=30)
    prev: int | None = None
    for i in range(rows):
        prev = await actions.pool.fetchval(
            "INSERT INTO assertions_cold (id, object_id, name, value, source_id, observed_at, "
            "created_at, confidence, supersedes, is_current) VALUES "
            "(nextval('assertions_id_seq'), $1, $2, '{\"reason\": \"same\"}'::jsonb, 'test', "
            "$3, $3, 0.6, $4, false) RETURNING id",
            obj, _NAME, base + timedelta(minutes=i), prev)


async def _both(actions: Actions) -> tuple[bool, bool]:
    return (await drain_running(actions.pool, "assertions_hot"),
            await drain_running(actions.pool, "assertions_cold"))


def _stop_after_the_first_chunk(monkeypatch: pytest.MonkeyPatch) -> None:
    real = abstention_churn.collapse_abstention_churn

    async def hurried(pool: Any, **kwargs: Any) -> dict[str, Any]:
        return await real(pool, **{**kwargs, "chunk_rows": 1, "max_seconds": 0.0})

    monkeypatch.setattr(abstention_churn, "collapse_abstention_churn", hurried)


async def test_an_unfinished_apply_holds_the_flag_and_the_night_housekeeping_cannot_clear_it(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _seed_chain(actions, "drainflag-a", 6)
    assert await _both(actions) == (False, False)
    _stop_after_the_first_chunk(monkeypatch)
    out = await run_backfill(actions.pool, _TARGET, actor="operator", dry_run=False,
                             because="test authorization")
    assert out["finished"] is False
    assert await _both(actions) == (True, True)
    # the housekeeping pass writes its own flag every night, and says "no drain"
    await set_drain(actions.pool, ("assertions_hot", "assertions_cold"), False)
    assert await _both(actions) == (True, True)


async def test_a_finishing_apply_clears_the_flag(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _seed_chain(actions, "drainflag-b", 6)
    _stop_after_the_first_chunk(monkeypatch)
    await run_backfill(actions.pool, _TARGET, actor="operator", dry_run=False,
                       because="test authorization")
    assert await _both(actions) == (True, True)
    monkeypatch.undo()
    out = await run_backfill(actions.pool, _TARGET, actor="operator", dry_run=False,
                             because="test authorization")
    assert out["finished"] is True
    assert await _both(actions) == (False, False)


async def test_the_flag_is_set_before_the_first_delete(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[tuple[bool, bool]] = []
    real = abstention_churn.collapse_abstention_churn

    async def spy(pool: Any, **kwargs: Any) -> dict[str, Any]:
        seen.append(await _both(actions))
        return await real(pool, **kwargs)

    monkeypatch.setattr(abstention_churn, "collapse_abstention_churn", spy)
    await run_backfill(actions.pool, _TARGET, actor="operator", dry_run=False,
                       because="test authorization")
    assert seen == [(True, True)]


async def test_a_dry_run_a_refused_apply_and_other_targets_set_nothing(
    actions: Actions,
) -> None:
    await run_backfill(actions.pool, _TARGET, actor="operator")
    await run_backfill(actions.pool, _TARGET, actor="operator", dry_run=False)   # no because
    await run_backfill(actions.pool, "desk_triage", actor="operator")
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM watermarks WHERE key LIKE $1", BACKFILL_DRAIN_KEY + "%") == 0
    assert await _both(actions) == (False, False)


async def test_an_abandoned_flag_lapses_after_a_week(actions: Actions) -> None:
    await set_backfill_drain(actions.pool, _TARGET, ("assertions_hot", "assertions_cold"), True)
    assert await _both(actions) == (True, True)
    old = (datetime.now(UTC) - timedelta(days=8)).isoformat()
    await actions.pool.execute(
        "UPDATE watermarks SET cursor=$2 WHERE key=$1", BACKFILL_DRAIN_KEY + _TARGET,
        json.dumps({"running": True, "tables": ["assertions_hot", "assertions_cold"],
                    "at": old}))
    assert await _both(actions) == (False, False)


async def test_a_flag_only_holds_the_tables_it_names(actions: Actions) -> None:
    await set_backfill_drain(actions.pool, "some_target", ("assertions_cold",), True)
    assert await _both(actions) == (False, True)
