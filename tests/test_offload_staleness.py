"""An enabled offload target with no successful offload for the stale limit is shown in
readiness and told to the desk once, with its reason."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from src.actions.core import Actions
from src.orchestrator import offload_runner
from src.orchestrator.backup_settings import write_backup_settings
from src.orchestrator.offload_staleness import (
    stale_offload_targets,
    stale_reason,
    stale_sentence,
)
from src.orchestrator.readiness import compute_readiness_steps

NOW = datetime(2026, 10, 12, 12, 0, tzinfo=UTC)


def _ago(days: float) -> str:
    return (NOW - timedelta(days=days)).isoformat()


NAS = {"name": "nas", "kind": "restic", "path_or_url": "sftp:nas:/r",
       "schedule": "*-*-* 03:00:00", "enabled": True}


# --- the pure decision ---------------------------------------------------------------------


def test_a_target_is_stale_from_its_last_success_at_the_limit_and_not_before() -> None:
    receipts = {"nas": {"last_successful_offload": _ago(6.9)}}
    assert stale_offload_targets([NAS], receipts, now=NOW, stale_days=7) == []
    receipts = {"nas": {"last_successful_offload": _ago(7.1)}}
    [entry] = stale_offload_targets([NAS], receipts, now=NOW, stale_days=7)
    assert entry["name"] == "nas" and entry["days"] == 7


def test_a_target_that_never_succeeded_is_judged_from_when_it_was_first_tracked() -> None:
    receipts = {"nas": {"tracked_since": _ago(9)}}
    [entry] = stale_offload_targets([NAS], receipts, now=NOW, stale_days=7)
    assert entry["days"] == 9
    assert entry["reason"] == "no offload has run yet"


def test_a_target_with_no_clock_yet_is_not_judged() -> None:
    assert stale_offload_targets([NAS], {}, now=NOW, stale_days=7) == []


def test_a_disabled_target_is_never_stale() -> None:
    disabled = dict(NAS, enabled=False)
    receipts = {"nas": {"last_successful_offload": _ago(100)}}
    assert stale_offload_targets([disabled], receipts, now=NOW, stale_days=7) == []


def test_the_limit_is_a_parameter() -> None:
    receipts = {"nas": {"last_successful_offload": _ago(3)}}
    assert stale_offload_targets([NAS], receipts, now=NOW, stale_days=2)
    assert not stale_offload_targets([NAS], receipts, now=NOW, stale_days=4)


def test_the_reason_for_a_tunnel_skip_says_since_when_and_what_to_do() -> None:
    reason = stale_reason({"last_skip_reason": "reachable only over tailscale0",
                           "skipped_since": _ago(8), "last_skip_at": _ago(0)})
    assert reason == "reachable only over tailscale0 since 2026-10-04: bring the laptop home"


def test_the_reason_for_an_undocked_drive_and_for_a_failed_attempt() -> None:
    assert stale_reason({"last_skip_reason": "not present (mountpoint absent)",
                         "skipped_since": _ago(8)}).endswith("dock the drive")
    assert stale_reason({"last_error": "boom"}) == "the last attempt failed: boom"


def test_the_sentence_names_target_days_limit_and_reason() -> None:
    sentence = stale_sentence({"name": "nas", "days": 9, "reason": "r"}, 7)
    assert sentence == "nas: no successful offload for 9 days (limit 7): r"


# --- readiness ---------------------------------------------------------------------------


def _steps(**kw: Any) -> dict[str, dict[str, Any]]:
    args: dict[str, Any] = dict(
        soul_key={"present": False}, restic_key={"present": False}, offload_targets=[],
        restore_drill_receipts={}, services_restarted=None)
    args.update(kw)
    return {s["key"]: s for s in compute_readiness_steps(**args)}


def test_readiness_shows_a_stale_target_with_its_reason() -> None:
    stale = [{"name": "nas", "days": 9, "clock": _ago(9),
              "reason": "reachable only over tailscale0 since 2026-10-03: bring the laptop home"}]
    step = _steps(offload_targets=[NAS], stale_offload=stale, stale_days=7)["offload_current"]
    assert step["status"] == "needs_attention"
    assert "bring the laptop home" in step["reason"]
    assert "9 days" in step["reason"]


def test_readiness_is_done_when_nothing_is_stale_and_waits_without_a_target() -> None:
    assert _steps(offload_targets=[NAS], stale_offload=[])["offload_current"]["status"] == "done"
    assert _steps()["offload_current"]["status"] == "missing"


# --- the tick: receipts and the desk, once -------------------------------------------------


@pytest.fixture(autouse=True)
def _isolated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(offload_runner._RECEIPTS_ENV, str(tmp_path / "offload_receipts.json"))
    monkeypatch.setenv("OSIRIS_RESTIC_PASSWORD", "a-test-password")


def _away(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.orchestrator import network_presence

    monkeypatch.setattr(network_presence, "lan_presence", lambda target: {
        "present": False, "reason": "reachable only over tailscale0"})


async def _one_target(actions: Actions) -> None:
    await write_backup_settings(actions.pool, actor="operator", because="x",
                                offload_targets=[NAS])


def _real_ago(days: float) -> str:
    return (datetime.now(UTC) - timedelta(days=days)).isoformat()


def _age_receipt(name: str, days: float) -> None:
    receipts = offload_runner._read_receipts()
    receipts[name]["tracked_since"] = _real_ago(days)
    receipts[name]["skipped_since"] = _real_ago(days)
    offload_runner._receipts_path().write_text(__import__("json").dumps(receipts))


async def _desk_count(actions: Actions) -> int:
    return int(await actions.pool.fetchval(
        "SELECT count(*) FROM fleet_messages WHERE from_agent = 'cron:offload_runner'"))


async def test_a_skipped_target_records_when_the_skipping_began_and_keeps_it(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _away(monkeypatch)
    await _one_target(actions)
    await offload_runner.run_offload_tick(actions.pool)
    first = offload_runner.offload_receipts()["nas"]["skipped_since"]
    await offload_runner.run_offload_tick(actions.pool)
    receipt = offload_runner.offload_receipts()["nas"]
    assert receipt["skipped_since"] == first  # the same stretch, not restarted each tick
    assert receipt["tracked_since"]


async def test_a_target_stale_for_days_is_told_to_the_desk_once_with_its_reason(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _away(monkeypatch)
    await _one_target(actions)
    await offload_runner.run_offload_tick(actions.pool)
    assert await _desk_count(actions) == 0  # not stale yet
    _age_receipt("nas", 9)

    out = await offload_runner.run_offload_tick(actions.pool)
    again = await offload_runner.run_offload_tick(actions.pool)

    assert len(out["stale_targets"]) == 1
    assert "reachable only over tailscale0" in out["stale_targets"][0]
    assert "stale_targets" not in again
    assert await _desk_count(actions) == 1


async def test_a_success_clears_the_notice_so_a_later_silence_is_told_again(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.orchestrator import network_presence

    _away(monkeypatch)
    await _one_target(actions)
    await offload_runner.run_offload_tick(actions.pool)
    _age_receipt("nas", 9)
    await offload_runner.run_offload_tick(actions.pool)
    assert await _desk_count(actions) == 1

    monkeypatch.setattr(network_presence, "lan_presence", lambda target: {"present": True})
    monkeypatch.setattr(offload_runner, "_run_restic_backup", lambda **kw: None)
    await offload_runner.run_offload_tick(actions.pool)
    receipt = offload_runner.offload_receipts()["nas"]
    assert receipt["last_successful_offload"] and not receipt["skipped_since"]
    assert not receipt["stale_notified_clock"]

    _away(monkeypatch)
    receipts = offload_runner._read_receipts()
    receipts["nas"]["last_successful_offload"] = _real_ago(9)
    offload_runner._receipts_path().write_text(__import__("json").dumps(receipts))
    await offload_runner.run_offload_tick(actions.pool)
    assert await _desk_count(actions) == 2


async def test_a_limit_of_zero_turns_the_notice_off(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.config.settings import Settings
    from src.orchestrator import settings_service

    _away(monkeypatch)
    await _one_target(actions)
    await offload_runner.run_offload_tick(actions.pool)
    _age_receipt("nas", 30)

    async def _off(pool: Any) -> Settings:
        return Settings(osiris_offload_stale_days=0)

    monkeypatch.setattr(settings_service, "settings_with_overlay", _off)
    out = await offload_runner.run_offload_tick(actions.pool)

    assert "stale_targets" not in out
    assert await _desk_count(actions) == 0


def test_the_stale_limit_is_a_registered_setting_defaulting_to_seven() -> None:
    from src.config.settings import Settings
    from src.config.settings_registry import spec_by_key

    spec = spec_by_key("backup.offload_stale_days")
    assert spec.default == 7 and spec.type == "int"
    assert Settings().osiris_offload_stale_days == 7


async def test_the_readiness_route_reports_a_stale_target(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import httpx
    from src.api.app import create_app
    from src.orchestrator.manifests import load_manifests

    _away(monkeypatch)
    await _one_target(actions)
    await offload_runner.run_offload_tick(actions.pool)
    _age_receipt("nas", 9)
    app = create_app(actions.pool)
    app.state.pool = actions.pool
    app.state.manifests = load_manifests(Path(__file__).parent.parent / "helpers")

    async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        body = (await client.get("/readiness")).json()

    step = next(s for s in body["steps"] if s["key"] == "offload_current")
    assert step["status"] == "needs_attention"
    assert "reachable only over tailscale0" in step["reason"]
