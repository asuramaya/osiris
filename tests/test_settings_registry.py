"""THE SETTINGS REGISTRY's own declarations (THE SETTINGS MENU, thread f4498ab304e4
piece 1) — src/config/settings_registry.py's SETTINGS tuple, the pure metadata half
service/door tests all read against."""
from __future__ import annotations

from src.config.settings import get_settings
from src.config.settings_registry import SETTINGS, spec_by_key


def test_registry_has_no_duplicate_keys() -> None:
    keys = [s.key for s in SETTINGS]
    assert len(keys) == len(set(keys))


def test_spec_by_key_finds_a_registered_knob_and_none_for_a_stranger() -> None:
    spec = spec_by_key("miner.abstention.enabled")
    assert spec is not None and spec.type == "bool" and spec.env_field == \
        "osiris_abstention_miner_enabled"
    assert spec_by_key("not.a.real.key") is None


def test_every_env_field_actually_exists_on_settings() -> None:
    """A SettingSpec naming a stale/misspelled env_field would silently no-op the
    overlay forever — caught here, once, for every registered knob at once."""
    settings = get_settings()
    for spec in SETTINGS:
        if spec.env_field is not None:
            assert hasattr(settings, spec.env_field), (
                f"{spec.key!r} names env_field {spec.env_field!r}, "
                "which is not a Settings attribute")


def test_every_default_matches_the_live_settings_default() -> None:
    """The registry's own `default` is a SEPARATE literal from settings.py's own field
    default — this test is the one place that keeps them from silently drifting apart."""
    settings = get_settings()
    for spec in SETTINGS:
        if spec.env_field is not None:
            assert getattr(settings, spec.env_field) == spec.default, (
                f"{spec.key!r}'s registered default {spec.default!r} disagrees with "
                f"settings.py's own {spec.env_field}={getattr(settings, spec.env_field)!r}")


def test_daemon_kill_switches_are_immediate_and_low_stakes_ones_skip_because() -> None:
    pit_watch = spec_by_key("daemon.pit_watch.enabled")
    assert pit_watch is not None
    assert pit_watch.effect == "immediate"
    assert pit_watch.requires_because is False and pit_watch.consequence == "low"

    retention = spec_by_key("daemon.retention_heartbeat.enabled")
    assert retention is not None
    assert retention.requires_because is True and retention.consequence == "high"


def test_miner_budgets_are_registered_next_tick() -> None:
    for key in ("miner.daily_budget_base", "miner.new_pair_starter_budget",
               "miner.zero_acceptance_window_days"):
        spec = spec_by_key(key)
        assert spec is not None and spec.effect == "next_tick" and spec.type == "int"


def test_wake_ladder_is_registered_next_tick_and_master_switches_are_high_stakes() -> None:
    for key in ("wake.trigger.rate_cap", "wake.hourly_budget", "wake.seat_hourly_cap",
               "wake.mail_lease_secs", "wake.owner_live_secs"):
        spec = spec_by_key(key)
        assert spec is not None and spec.effect == "next_tick"
        assert spec.requires_because is False and spec.consequence == "low"

    for key in ("wake.trigger.enabled", "wake.enabled"):
        spec = spec_by_key(key)
        assert spec is not None and spec.effect == "next_tick"
        assert spec.requires_because is True and spec.consequence == "high"


def test_diag_memory_enabled_is_registered_immediate() -> None:
    spec = spec_by_key("diag.memory_enabled")
    assert spec is not None
    assert spec.effect == "immediate" and spec.env_field == "osiris_memory_diag_enabled"


def test_diag_worker_boot_memtrace_is_registered_restart_osiris_worker() -> None:
    spec = spec_by_key("diag.worker_boot_memtrace.enabled")
    assert spec is not None
    assert spec.effect == "restart:osiris-worker"
    assert spec.env_field == "osiris_worker_boot_memtrace_enabled"
