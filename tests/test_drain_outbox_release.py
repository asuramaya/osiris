"""drain_outbox claims a batch up front and publishes row by row, and a claimed row is never
claimed again. A drain that stops part-way must give the unpublished rest back, or those
events are lost for good (543 sat stranded in the live table after worker restarts)."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest
from src.actions.core import Actions
from src.orchestrator import cascade

_TAG = "drain_release_probe"


async def _rows(actions: Actions, n: int) -> list[int]:
    obj = await actions.create_or_find_object("Person", "drain-probe-person", "test")
    case = await actions.pool.fetchval(
        "INSERT INTO cases (name, owner) VALUES ('drain-probe','analyst:test') RETURNING id")
    ids = []
    for _ in range(n):
        ids.append(await actions.pool.fetchval(
            "INSERT INTO outbox (event_type, object_id, case_id, payload) "
            "VALUES ($1, $2, $3, '{}') RETURNING id", _TAG, obj, case))
    return ids


async def _state(actions: Actions, ids: list[int]) -> dict[int, tuple[bool, bool]]:
    rows = await actions.pool.fetch(
        "SELECT id, claimed_at IS NOT NULL AS claimed, published_at IS NOT NULL AS pub "
        "FROM outbox WHERE id = ANY($1::bigint[])", ids)
    return {r["id"]: (r["claimed"], r["pub"]) for r in rows}


async def _only_mine(actions: Actions, ids: list[int]) -> None:
    """Publish every other unpublished row so the drain's batch is exactly `ids`."""
    await actions.pool.execute(
        "UPDATE outbox SET published_at=now() "
        "WHERE published_at IS NULL AND NOT id = ANY($1::bigint[])", ids)


async def test_a_drain_cancelled_mid_batch_releases_the_rest_for_the_next_drain(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    ids = await _rows(actions, 5)
    await _only_mine(actions, ids)
    ctx: Any = SimpleNamespace(pool=actions.pool)
    seen: list[int] = []
    stuck = asyncio.Event()

    async def fire(_ctx: Any, _event: str, obj: Any, case: Any) -> None:
        seen.append(1)
        if len(seen) == 3:  # the third row never finishes: the worker is being shut down
            stuck.set()
            await asyncio.sleep(3600)

    monkeypatch.setattr(cascade, "fire_triggers", fire)
    task = asyncio.create_task(cascade.drain_outbox(ctx, limit=100))
    await asyncio.wait_for(stuck.wait(), 10)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    state = await _state(actions, ids)
    # the claim returns rows in no promised order, so judge by counts, not by id
    values = sorted(state.values())
    assert values == [(False, False)] * 3 + [(True, True)] * 2
    # finished rows stay claimed and published; the row in hand and those after are handed back

    seen.clear()

    async def quick(*_a: Any) -> None:
        seen.append(1)

    monkeypatch.setattr(cascade, "fire_triggers", quick)
    assert await cascade.drain_outbox(ctx, limit=100) == 3  # the next drain picks them up
    assert all(p for _c, p in (await _state(actions, ids)).values())


async def test_a_row_that_raises_stays_claimed_but_the_rows_behind_it_are_released(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    ids = await _rows(actions, 4)
    await _only_mine(actions, ids)
    ctx: Any = SimpleNamespace(pool=actions.pool)
    calls = 0

    async def fire(*_a: Any) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("this row always fails")

    monkeypatch.setattr(cascade, "fire_triggers", fire)
    with pytest.raises(RuntimeError):
        await cascade.drain_outbox(ctx, limit=100)
    values = sorted((await _state(actions, ids)).values())
    # one published, the failing row alone still claimed, the two behind it handed back
    assert values == [(False, False)] * 2 + [(True, False), (True, True)]
