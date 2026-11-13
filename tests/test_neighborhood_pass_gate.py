"""neighborhood_pass is a daily job with run_at_startup, so it ran at every worker restart: 150
to 215 s and one to three LLM summaries each time (74 metered calls, $1.24, on 2026-11-11 against
one a day undisturbed). It now keeps its own mark and a run inside the gap does nothing."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from src.actions.core import Actions
from src.orchestrator import neighborhoods
from src.orchestrator.monitor import get_cursor, set_cursor
from src.workers import arq_worker


class _Passes:
    def __init__(self) -> None:
        self.ran: list[str] = []
        self.fail = False


@pytest.fixture
def passes(monkeypatch: pytest.MonkeyPatch) -> _Passes:
    state = _Passes()

    async def consolidate(actions: Any) -> dict[str, int]:
        state.ran.append("consolidate")
        if state.fail:
            raise RuntimeError("a model hiccup")
        return {"threads_merged": 0}

    async def summarize(actions: Any, llm: Any) -> dict[str, int]:
        state.ran.append("summarize")
        return {"summarized": 1}

    monkeypatch.setattr(neighborhoods, "consolidate_pass", consolidate)
    monkeypatch.setattr(neighborhoods, "summarize_neighborhoods", summarize)
    from src.ingest import providers

    monkeypatch.setattr(providers, "llm_provider", lambda: object())
    return state


async def _run(actions: Actions) -> int:
    return await arq_worker.neighborhood_pass({"cascade": SimpleNamespace(actions=actions)})


async def test_a_second_run_inside_the_gap_does_nothing(
    actions: Actions, passes: _Passes,
) -> None:
    assert await _run(actions) == 1
    assert passes.ran == ["consolidate", "summarize"]
    assert await get_cursor(actions.pool, arq_worker._NEIGHBORHOOD_DONE_KEY)

    assert await _run(actions) == 0  # a restart an hour later
    assert passes.ran == ["consolidate", "summarize"]  # no LLM call, no second pass


async def test_it_runs_again_once_the_gap_has_passed_or_the_mark_is_unreadable(
    actions: Actions, passes: _Passes,
) -> None:
    old = datetime.now(UTC) - timedelta(seconds=arq_worker._NEIGHBORHOOD_MIN_GAP_SECS + 60)
    await set_cursor(actions.pool, arq_worker._NEIGHBORHOOD_DONE_KEY, old.isoformat())
    assert await _run(actions) == 1

    await set_cursor(actions.pool, arq_worker._NEIGHBORHOOD_DONE_KEY, "not a time")
    assert await _run(actions) == 1
    assert passes.ran.count("summarize") == 2


async def test_a_failed_pass_does_not_stamp_so_the_next_run_tries_again(
    actions: Actions, passes: _Passes,
) -> None:
    passes.fail = True
    assert await _run(actions) == 0
    assert await get_cursor(actions.pool, arq_worker._NEIGHBORHOOD_DONE_KEY) is None

    passes.fail = False
    assert await _run(actions) == 1
