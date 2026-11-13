"""The provenance sweep says which lane is slow, even when the 60 s budget cancels it."""
from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest
from src.orchestrator import capture


async def _quick(*a: Any, **k: Any) -> dict[str, Any]:
    return {"to_mint": 1, "to_abstain": 2}


@pytest.fixture
def lanes(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("resolve_agent_orphans", "backfill_lineage_repo_links",
                 "backfill_lineage_repo_links_at_write_time", "resolve_reference_orphans",
                 "resolve_practice_orphans", "resolve_superstition_orphans",
                 "resolve_seat_orphans", "resolve_project_orphans"):
        monkeypatch.setattr(capture, name, _quick)


async def test_the_sweep_logs_every_lanes_time_and_keeps_its_results(
    lanes: None, caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="osiris.capture")

    out = await capture.apply_provenance_sweep_heartbeat(object())  # type: ignore[arg-type]

    assert out["total_minted"] == 8 and out["total_abstained"] == 16
    line = next(r.getMessage() for r in caplog.records if "lane timings" in r.getMessage())
    for name in ("agent", "decision_thread", "reference", "practice", "superstition", "seat",
                 "project"):
        assert f"'{name}'" in line


async def test_a_slow_lane_gets_its_own_line(
    lanes: None, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(capture, "_SLOW_LANE_SECS", 0.0)
    caplog.set_level(logging.INFO, logger="osiris.capture")

    await capture.apply_provenance_sweep_heartbeat(object())  # type: ignore[arg-type]

    assert any("lane agent took" in r.getMessage() for r in caplog.records)


async def test_a_lane_that_is_cancelled_by_the_budget_says_where_it_stopped(
    lanes: None, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    async def stuck(*a: Any, **k: Any) -> dict[str, Any]:
        await asyncio.sleep(30)
        return {}

    monkeypatch.setattr(capture, "resolve_reference_orphans", stuck)
    caplog.set_level(logging.INFO, logger="osiris.capture")

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(
            capture.apply_provenance_sweep_heartbeat(object()), timeout=0.3)  # type: ignore[arg-type]

    msg = next(r.getMessage() for r in caplog.records if "was cancelled" in r.getMessage())
    assert "lane reference was cancelled" in msg and "agent" in msg  # the lanes done before it


async def test_one_lanes_failure_is_recorded_and_the_rest_still_run(
    lanes: None, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def broken(*a: Any, **k: Any) -> dict[str, Any]:
        raise RuntimeError("db hiccup")

    monkeypatch.setattr(capture, "resolve_practice_orphans", broken)

    out = await capture.apply_provenance_sweep_heartbeat(object())  # type: ignore[arg-type]

    assert "db hiccup" in out["lanes"]["practice_error"] and "seat" in out["lanes"]
