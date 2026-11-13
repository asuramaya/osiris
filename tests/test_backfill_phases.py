"""backfill_transcripts says where a slow tick went (it took ~3.9 s a tick and nothing said which
of its six sweeps it was), and says nothing on a quick one."""
from __future__ import annotations

import logging
from types import SimpleNamespace
from typing import Any

import pytest
from src.workers import arq_worker


def _stub(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    from src.ingest.soul_store import SoulStore
    from src.ingest.telemetry import TelemetryStore
    from src.ingest.transcript_store import TranscriptStore
    from src.orchestrator import neighborhoods

    async def one(self: Any, **kw: Any) -> dict[str, int]:
        return {"x": 1}

    async def zero(self: Any, **kw: Any) -> int:
        return 0

    async def census(actions: Any, *, roots: list[str]) -> dict[str, Any]:
        return {"minted": [], "refused": []}

    monkeypatch.setattr(TranscriptStore, "backfill", one)
    monkeypatch.setattr(TelemetryStore, "backfill", zero)
    monkeypatch.setattr(SoulStore, "backfill", one)
    monkeypatch.setattr(SoulStore, "backfill_crush", one)
    monkeypatch.setattr(SoulStore, "backfill_dsh", one)
    monkeypatch.setattr(neighborhoods, "census_trees", census)
    monkeypatch.setattr(arq_worker, "get_settings", lambda: SimpleNamespace(
        osiris_transcripts=str(tmp_path), osiris_census_roots=""))


async def test_a_slow_tick_logs_each_phase_and_a_quick_one_stays_quiet(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any, caplog: pytest.LogCaptureFixture,
) -> None:
    _stub(monkeypatch, tmp_path)
    ctx = {"pool": object()}

    with caplog.at_level(logging.INFO, logger=arq_worker._log.name):
        monkeypatch.setattr(arq_worker, "_BACKFILL_LAP_FLOOR_SECS", 0.0)
        assert await arq_worker.backfill_transcripts(ctx) > 0
        loud = [r.getMessage() for r in caplog.records if "backfill phases" in r.getMessage()]
        caplog.clear()
        monkeypatch.setattr(arq_worker, "_BACKFILL_LAP_FLOOR_SECS", 3600.0)
        await arq_worker.backfill_transcripts(ctx)
        quiet = [r for r in caplog.records if "backfill phases" in r.getMessage()]

    assert len(loud) == 1
    for phase in ("transcripts", "telemetry", "soul", "crush", "dsh", "census"):
        assert f"{phase} " in loud[0]
    assert quiet == []
