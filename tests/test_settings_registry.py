"""THE SETTINGS REGISTRY's own declarations (THE SETTINGS MENU, piece 1),
src/config/settings_registry.py's SETTINGS tuple, the pure metadata half
service/door tests all read against."""
from __future__ import annotations

from src.config.settings import Settings, get_settings
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
    overlay forever, caught here, once, for every registered knob at once."""
    settings = get_settings()
    for spec in SETTINGS:
        if spec.env_field is not None:
            assert hasattr(settings, spec.env_field), (
                f"{spec.key!r} names env_field {spec.env_field!r}, "
                "which is not a Settings attribute")


def test_every_default_matches_the_live_settings_default() -> None:
    """The registry's own `default` is a SEPARATE literal from settings.py's own field
    default, this test is the one place that keeps them from silently drifting apart.

    Compares against `Settings.model_fields[...].default`, the MODEL's own declared
    default, never `get_settings()` (under the gate's real -n 4
    worker environment, OSIRIS_WAKE_HOURLY_BUDGET/etc. are genuinely set in that shell,
    so a live `Settings()` instance picks up the box's actual env rather than the bare
    field default, order/worker-dependent flakiness, passing alone but failing under
    the gate. `model_fields` reads the class declaration itself, no env involved."""
    for spec in SETTINGS:
        if spec.env_field is not None:
            field_default = Settings.model_fields[spec.env_field].default
            assert field_default == spec.default, (
                f"{spec.key!r}'s registered default {spec.default!r} disagrees with "
                f"settings.py's own {spec.env_field}={field_default!r}")


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


# --- daemon unit literals ---------------

def test_daemon_memory_max_specs_are_next_deploy_and_reject_a_bad_value() -> None:
    for key in ("daemon.osiris_mcp.memory_max", "daemon.osiris_worker.memory_max",
               "daemon.osiris_pulse.memory_max", "daemon.osiris_console.memory_max"):
        spec = spec_by_key(key)
        assert spec is not None and spec.effect == "next_deploy" and spec.type == "str"
        assert spec.validate is not None and spec.validate("not a memory value") is not None
        assert spec.validate("3G") is None
        assert spec.validate("infinity") is None


def test_pulse_memory_max_alone_defaults_to_empty_no_cap() -> None:
    """osiris-pulse ships with no MemoryMax= line at all today, the one spec among the
    four whose default isn't the shipped literal of a real cap."""
    spec = spec_by_key("daemon.osiris_pulse.memory_max")
    assert spec is not None and spec.default == ""


def test_the_other_three_memory_max_defaults_match_the_shipped_unit_files() -> None:
    mcp, worker, console = (spec_by_key(k) for k in (
        "daemon.osiris_mcp.memory_max", "daemon.osiris_worker.memory_max",
        "daemon.osiris_console.memory_max"))
    assert mcp is not None and mcp.default == "3G"
    assert worker is not None and worker.default == "3G"
    assert console is not None and console.default == "512M"


def test_pulse_watch_interval_is_a_positive_int_defaulting_to_the_shipped_value() -> None:
    spec = spec_by_key("daemon.osiris_pulse.watch_interval_secs")
    assert spec is not None and spec.type == "int" and spec.default == 600
    assert spec.validate is not None
    assert spec.validate(0) is not None and spec.validate(-5) is not None
    assert spec.validate(600) is None


def test_console_host_and_port_default_to_the_shipped_values() -> None:
    host = spec_by_key("daemon.osiris_console.host")
    port = spec_by_key("daemon.osiris_console.port")
    assert host is not None and host.default == "127.0.0.1" and host.type == "str"
    assert port is not None and port.default == 8011 and port.type == "int"
    assert port.validate is not None
    assert port.validate(0) is not None and port.validate(70000) is not None
    assert port.validate(8011) is None


def test_pg_autotune_schedule_is_registered_like_a_backup_timer_but_not_one() -> None:
    """Grouped under 'daemon' (a Postgres-maintenance timer, not a backup one), kept
    OUT of BACKUP_TIMER_UNITS, which stays exactly the 5 backup-lane units."""
    from src.config.settings_registry import BACKUP_TIMER_UNITS

    spec = spec_by_key("daemon.osiris_pg_autotune.schedule")
    assert spec is not None and spec.type == "schedule" and spec.effect == "next_deploy"
    assert spec.default is None
    assert "osiris-pg-autotune.timer" not in BACKUP_TIMER_UNITS


def test_the_four_pool_sizes_restart_the_exact_units_pool_health_names() -> None:
    """No drift, src/orchestrator/pool_health.py's own `_KNOWN_DAEMON_POOL_SETTINGS`
    names the exact unit each pool-size field restarts under (osiris_api_pool_size
    restarts osiris-console, not a unit named "osiris-api")."""
    from src.orchestrator.pool_health import _KNOWN_DAEMON_POOL_SETTINGS

    by_env_field = {s.env_field: s for s in SETTINGS if s.key.startswith("daemon.")
                    and s.key.endswith(".pool_size")}
    assert set(by_env_field) == set(_KNOWN_DAEMON_POOL_SETTINGS.values())
    for unit, env_field in _KNOWN_DAEMON_POOL_SETTINGS.items():
        spec = by_env_field[env_field]
        assert spec.effect == f"restart:{unit}"
        assert spec.type == "int"


def test_pool_size_specs_reject_zero_and_negative() -> None:
    spec = spec_by_key("daemon.osiris_mcp.pool_size")
    assert spec is not None and spec.validate is not None
    assert spec.validate(0) is not None and spec.validate(-1) is not None
    assert spec.validate(8) is None
