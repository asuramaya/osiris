"""Per-phase wall time for a slow call: the clock itself, and mount() reporting through it."""
from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from src.actions.core import Actions
from src.orchestrator.phase_clock import PhaseClock


def test_each_mark_closes_the_phase_since_the_previous_one() -> None:
    clock = PhaseClock()
    time.sleep(0.02)
    clock.mark("slow")
    clock.mark("instant")
    phases = clock.phases_ms()
    assert list(phases) == ["slow", "instant"]
    assert phases["slow"] >= 15 and phases["instant"] < phases["slow"]
    assert abs(clock.total_ms() - sum(phases.values())) < 1.0


def test_a_phase_marked_twice_adds_up_and_the_line_lists_the_slowest_first() -> None:
    clock = PhaseClock()
    time.sleep(0.01)
    clock.mark("a")
    time.sleep(0.03)
    clock.mark("b")
    time.sleep(0.01)
    clock.mark("a")
    line = clock.line()
    assert line.startswith("total=")
    assert line.index(" b=") < line.index(" a=")
    assert clock.phases_ms()["a"] >= 15


async def test_a_verbose_mount_reports_its_phases_and_logs_them(
    actions: Actions, tmp_path: Path, monkeypatch: Any, caplog: Any,
) -> None:
    import logging

    from src import mcp_server

    monkeypatch.setattr(mcp_server, "_pool", actions.pool)
    office = tmp_path / "office"
    office.mkdir()
    with caplog.at_level(logging.INFO, logger="osiris.mcp"):
        out = await mcp_server.mount(cwd=str(office), job_dir=str(tmp_path / "jobs" / "abcd1234"),
                                     verbose=True)
    phases = out["phase_ms"]
    for name in ("identity_reading", "register_agent", "unread_counts", "save_mount"):
        assert name in phases and phases[name] >= 0
    assert any("mount phases: total=" in r.getMessage() for r in caplog.records)
    terse = await mcp_server.mount(cwd=str(office), job_dir=str(tmp_path / "jobs" / "abcd1234"))
    assert "phase_ms" not in terse
