"""The provenance sweep's own heartbeat sub-sweep (wave 15, mail 8840): every
cardinality-1-mint-or-abstain orphan lane the wave built, re-applied unattended on
classification_laws_heartbeat's own cadence -- "so a stranger's install self-heals."
"""
from __future__ import annotations

from datetime import UTC, datetime

import pytest
from src.actions.core import Actions
from src.orchestrator import capture
from src.orchestrator.capture import apply_provenance_sweep_heartbeat, kill_superstition


async def test_runs_every_lane_and_totals_a_real_mint(actions: Actions) -> None:
    killer = await capture.record_decision(actions, "a fix under repo:heartbeatproj",
                                           source="agent:hb1")
    await capture.link_repo(actions, killer, "heartbeatproj", datetime.now(UTC))
    await kill_superstition(actions, "A DEAD WORKAROUND FOR THE HEARTBEAT TEST",
                            killed_by=str(killer))

    out = await apply_provenance_sweep_heartbeat(actions)

    assert set(out["lanes"]) == {
        "agent", "decision_thread", "decision_thread_at_write_time",
        "reference", "practice", "superstition", "seat",
    }
    assert out["total_minted"] >= 1
    assert out["lanes"]["superstition"]["to_mint"] >= 1


async def test_a_single_lane_failure_never_sinks_the_others(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _boom(*args: object, **kwargs: object) -> None:
        raise RuntimeError("simulated DB hiccup")

    monkeypatch.setattr(capture, "resolve_agent_orphans", _boom)

    out = await apply_provenance_sweep_heartbeat(actions)

    assert "agent_error" in out["lanes"]
    assert "simulated DB hiccup" in out["lanes"]["agent_error"]
    # every OTHER lane still ran to completion
    for key in ("decision_thread", "decision_thread_at_write_time", "reference",
               "practice", "superstition", "seat"):
        assert key in out["lanes"]
        assert "to_mint" in out["lanes"][key]


async def test_is_idempotent_a_second_run_finds_nothing_left(actions: Actions) -> None:
    killer = await capture.record_decision(actions, "a fix under repo:idemproj",
                                           source="agent:hb2")
    await capture.link_repo(actions, killer, "idemproj", datetime.now(UTC))
    await kill_superstition(actions, "AN IDEMPOTENCY-CHECK WORKAROUND", killed_by=str(killer))

    first = await apply_provenance_sweep_heartbeat(actions)
    second = await apply_provenance_sweep_heartbeat(actions)

    assert first["lanes"]["superstition"]["to_mint"] >= 1
    assert second["lanes"]["superstition"]["scanned"] == 0
