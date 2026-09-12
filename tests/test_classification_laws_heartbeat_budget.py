"""Thread 9150aec2 follow-up (Thoth mail 10204): the w218 liveness regression fixed the
specific defect (an unbounded per-agent transcript walk) but left classification_laws_
heartbeat's own seven sub-sweeps measured only in aggregate — a single slow sweep could
still hold arq_worker._boot_lock for its whole run with nobody able to see WHICH of the
seven was responsible short of re-reading source. These tests pin the two follow-up
guarantees directly: a slow sub-sweep is bounded by its own budget rather than the
cron's whole run, and every tick's seven timings are logged unconditionally.
"""
from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

import pytest
from src.actions.core import Actions
from src.workers import arq_worker


async def test_a_slow_subsweep_is_bounded_by_its_own_budget_not_the_whole_tick(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A sub-sweep that overruns its own budget must be abandoned for THIS tick and
    logged by name — never let to run to completion, and never allowed to sink the
    sweeps around it."""
    monkeypatch.setattr(arq_worker, "_SUBSWEEP_TIMEOUT_SECS", 0.05)

    async def _slow_ghost_sweep(actions: Actions) -> dict[str, object]:
        await asyncio.sleep(1.0)
        return {"retired": [], "refused": []}

    monkeypatch.setattr(
        "src.orchestrator.house_hygiene.apply_ghost_house_sweep", _slow_ghost_sweep)

    ctx = {"cascade": SimpleNamespace(actions=actions)}
    caplog.set_level("WARNING", logger="osiris.worker")

    t0 = time.monotonic()
    result = await arq_worker.classification_laws_heartbeat(ctx)
    elapsed = time.monotonic() - t0

    assert elapsed < 1.0, f"took {elapsed:.2f}s — the slow sweep's own sleep was not bounded"
    assert isinstance(result, int)
    assert any(
        "ghost_house" in r.getMessage() and "budget" in r.getMessage()
        for r in caplog.records), "no warning named the timed-out sub-sweep by name"


async def test_every_tick_logs_all_seven_subsweep_timings_unconditionally(
    actions: Actions, caplog: pytest.LogCaptureFixture,
) -> None:
    """A quiet tick (nothing to do in any of the seven sub-sweeps) must still leave a
    timing line naming every sub-sweep — a slow tick is diagnosed from the log alone,
    never re-guessed after the fact, whether or not anything actually happened."""
    ctx = {"cascade": SimpleNamespace(actions=actions)}
    caplog.set_level("INFO", logger="osiris.worker")

    await arq_worker.classification_laws_heartbeat(ctx)

    timing_lines = [r.getMessage() for r in caplog.records if "sub-sweep timings" in r.getMessage()]
    assert len(timing_lines) == 1
    for name in (
        "migration_0060", "project_hygiene", "ghost_house", "fleet_prune",
        "stacked_office_headers", "provenance_sweep", "boot_drift_nudge",
    ):
        assert name in timing_lines[0], f"{name!r} missing from the timings log line"
