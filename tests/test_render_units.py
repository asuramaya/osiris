"""render_units generalizes render_backup_timers.py's own pattern past the 5
backup-lane timers. `render()` is pure (no DB, no network); `_configured_values()`
is the one async hop that reads every registered setting: proven separately,
against a real test DB, by monkeypatching `get_settings`.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from scripts.render_units import _configured_values, render
from src.actions.core import Actions


def _write_timer(deploy_dir: Path, unit: str, calendar: str) -> None:
    name = unit.removesuffix(".timer")
    (deploy_dir / f"{name}.timer").write_text(
        f"[Unit]\nDescription=fake {name}\n\n[Timer]\nOnCalendar={calendar}\n"
        "Persistent=true\n\n[Install]\nWantedBy=timers.target\n")
    (deploy_dir / f"{name}.service").write_text(f"[Unit]\nDescription={name}\n")


def _write_daemon_unit(user_dir: Path, name: str, *, memory_max: str | None,
                       extra_execstart: str = "") -> None:
    body = "[Unit]\nDescription=fake\n\n[Service]\nType=simple\n"
    body += f"ExecStart=/bin/true{extra_execstart}\n"
    if memory_max:
        body += f"MemoryMax={memory_max}\n"
    body += "\n[Install]\nWantedBy=default.target\n"
    (user_dir / f"{name}.service").write_text(body)


# --- the timer lane (reused verbatim from the retired render_backup_timers.py) -----

def test_render_substitutes_the_override_and_leaves_everything_else_untouched(
    tmp_path: Path,
) -> None:
    deploy = tmp_path / "deploy"
    deploy.mkdir()
    _write_timer(deploy, "osiris-backup.timer", "*-*-* 04,10,16,22:30:00")
    out = tmp_path / "out"
    n = render(deploy, out, {"backup.timer_schedule.osiris-backup.timer": "*-*-* 00,12:00:00"})
    assert n == 1
    rendered = (out / "osiris-backup.timer").read_text()
    assert "OnCalendar=*-*-* 00,12:00:00" in rendered
    assert "Persistent=true" in rendered  # everything else survives byte-for-byte
    assert "OnCalendar=*-*-* 04,10,16,22:30:00" not in rendered
    # the .service ships unchanged
    assert (out / "osiris-backup.service").read_text() == (
        deploy / "osiris-backup.service").read_text()


def test_render_with_no_override_is_a_byte_identical_copy(tmp_path: Path) -> None:
    deploy = tmp_path / "deploy"
    deploy.mkdir()
    _write_timer(deploy, "osiris-preflight.timer", "Mon *-*-* 05:00:00")
    out = tmp_path / "out"
    n = render(deploy, out, {})
    assert n == 0
    assert (out / "osiris-preflight.timer").read_text() == (
        deploy / "osiris-preflight.timer").read_text()


def test_render_ignores_an_override_for_a_unit_with_no_shipped_file(tmp_path: Path) -> None:
    deploy = tmp_path / "deploy"
    deploy.mkdir()
    out = tmp_path / "out"
    # no osiris-base-backup.timer exists in deploy/ at all, no crash, nothing written
    n = render(deploy, out, {"backup.timer_schedule.osiris-base-backup.timer": "Sun 02:00:00"})
    assert n == 0
    assert not (out / "osiris-base-backup.timer").exists()


def test_render_substitutes_the_pg_autotune_schedule_too(tmp_path: Path) -> None:
    deploy = tmp_path / "deploy"
    deploy.mkdir()
    _write_timer(deploy, "osiris-pg-autotune.timer", "*-*-* 03:00:00")
    out = tmp_path / "out"
    n = render(deploy, out, {"daemon.osiris_pg_autotune.schedule": "Sun *-*-* 02:00:00"})
    assert n == 1
    assert "OnCalendar=Sun *-*-* 02:00:00" in (out / "osiris-pg-autotune.timer").read_text()


# --- the daemon lane: MemoryMax=, --watch, --host/--port ----------------------------

def test_render_substitutes_memory_max_in_place(tmp_path: Path) -> None:
    deploy = tmp_path / "deploy"
    user_dir = deploy / "user"
    user_dir.mkdir(parents=True)
    _write_daemon_unit(user_dir, "osiris-mcp", memory_max="3G")
    out = tmp_path / "out"
    n = render(deploy, out, {"daemon.osiris_mcp.memory_max": "6G"})
    assert n == 1
    text = (out / "user" / "osiris-mcp.service").read_text()
    assert "MemoryMax=6G" in text and "MemoryMax=3G" not in text


def test_render_memory_max_default_is_byte_identical(tmp_path: Path) -> None:
    deploy = tmp_path / "deploy"
    user_dir = deploy / "user"
    user_dir.mkdir(parents=True)
    _write_daemon_unit(user_dir, "osiris-console", memory_max="512M")
    out = tmp_path / "out"
    n = render(deploy, out, {"daemon.osiris_console.memory_max": "512M"})
    assert n == 0
    assert (out / "user" / "osiris-console.service").read_text() == (
        user_dir / "osiris-console.service").read_text()


def test_render_memory_max_inserts_a_new_line_when_none_shipped(tmp_path: Path) -> None:
    deploy = tmp_path / "deploy"
    user_dir = deploy / "user"
    user_dir.mkdir(parents=True)
    _write_daemon_unit(user_dir, "osiris-pulse", memory_max=None)
    out = tmp_path / "out"
    n = render(deploy, out, {"daemon.osiris_pulse.memory_max": "2G"})
    assert n == 1
    text = (out / "user" / "osiris-pulse.service").read_text()
    assert "MemoryMax=2G" in text
    assert text.index("MemoryMax=2G") < text.index("[Install]")


def test_render_memory_max_empty_default_drops_no_line(tmp_path: Path) -> None:
    deploy = tmp_path / "deploy"
    user_dir = deploy / "user"
    user_dir.mkdir(parents=True)
    _write_daemon_unit(user_dir, "osiris-pulse", memory_max=None)
    out = tmp_path / "out"
    n = render(deploy, out, {"daemon.osiris_pulse.memory_max": ""})
    assert n == 0
    assert "MemoryMax=" not in (out / "user" / "osiris-pulse.service").read_text()


def test_render_memory_max_empty_value_removes_an_existing_cap(tmp_path: Path) -> None:
    deploy = tmp_path / "deploy"
    user_dir = deploy / "user"
    user_dir.mkdir(parents=True)
    _write_daemon_unit(user_dir, "osiris-mcp", memory_max="3G")
    out = tmp_path / "out"
    n = render(deploy, out, {"daemon.osiris_mcp.memory_max": ""})
    assert n == 1
    assert "MemoryMax=" not in (out / "user" / "osiris-mcp.service").read_text()


def test_render_substitutes_the_pulse_watch_interval(tmp_path: Path) -> None:
    deploy = tmp_path / "deploy"
    user_dir = deploy / "user"
    user_dir.mkdir(parents=True)
    _write_daemon_unit(user_dir, "osiris-pulse", memory_max=None,
                       extra_execstart=" -m src.orchestrator.pulse --watch 600")
    out = tmp_path / "out"
    n = render(deploy, out, {"daemon.osiris_pulse.watch_interval_secs": 300})
    assert n == 1
    assert "--watch 300" in (out / "user" / "osiris-pulse.service").read_text()


def test_render_watch_interval_default_is_byte_identical(tmp_path: Path) -> None:
    deploy = tmp_path / "deploy"
    user_dir = deploy / "user"
    user_dir.mkdir(parents=True)
    _write_daemon_unit(user_dir, "osiris-pulse", memory_max=None,
                       extra_execstart=" -m src.orchestrator.pulse --watch 600")
    out = tmp_path / "out"
    n = render(deploy, out, {"daemon.osiris_pulse.watch_interval_secs": 600})
    assert n == 0


def test_render_substitutes_console_host_and_port_independently(tmp_path: Path) -> None:
    deploy = tmp_path / "deploy"
    user_dir = deploy / "user"
    user_dir.mkdir(parents=True)
    _write_daemon_unit(user_dir, "osiris-console", memory_max="512M",
                       extra_execstart=" --factory src.api.app:create_app "
                                       "--host 127.0.0.1 --port 8011")
    out = tmp_path / "out"
    n = render(deploy, out, {"daemon.osiris_console.host": "0.0.0.0",
                             "daemon.osiris_console.port": 9000})
    assert n == 1
    text = (out / "user" / "osiris-console.service").read_text()
    assert "--host 0.0.0.0" in text and "--port 9000" in text


def test_render_console_defaults_are_byte_identical(tmp_path: Path) -> None:
    deploy = tmp_path / "deploy"
    user_dir = deploy / "user"
    user_dir.mkdir(parents=True)
    _write_daemon_unit(user_dir, "osiris-console", memory_max="512M",
                       extra_execstart=" --factory src.api.app:create_app "
                                       "--host 127.0.0.1 --port 8011")
    out = tmp_path / "out"
    n = render(deploy, out, {"daemon.osiris_console.memory_max": "512M",
                             "daemon.osiris_console.host": "127.0.0.1",
                             "daemon.osiris_console.port": 8011})
    assert n == 0
    assert (out / "user" / "osiris-console.service").read_text() == (
        user_dir / "osiris-console.service").read_text()


def test_render_substitutes_console_graceful_shutdown_independently(tmp_path: Path) -> None:
    """THE CONSOLE GRACEFUL SHUTDOWN: a deploy-reliability follow-up."""
    deploy = tmp_path / "deploy"
    user_dir = deploy / "user"
    user_dir.mkdir(parents=True)
    _write_daemon_unit(user_dir, "osiris-console", memory_max="512M",
                       extra_execstart=" --factory src.api.app:create_app "
                                       "--host 127.0.0.1 --port 8011 "
                                       "--timeout-graceful-shutdown 10")
    out = tmp_path / "out"
    n = render(deploy, out, {"daemon.osiris_console.graceful_shutdown_secs": 20})
    assert n == 1
    text = (out / "user" / "osiris-console.service").read_text()
    assert "--timeout-graceful-shutdown 20" in text


def test_render_console_graceful_shutdown_default_is_byte_identical(tmp_path: Path) -> None:
    deploy = tmp_path / "deploy"
    user_dir = deploy / "user"
    user_dir.mkdir(parents=True)
    _write_daemon_unit(user_dir, "osiris-console", memory_max="512M",
                       extra_execstart=" --factory src.api.app:create_app "
                                       "--host 127.0.0.1 --port 8011 "
                                       "--timeout-graceful-shutdown 10")
    out = tmp_path / "out"
    n = render(deploy, out, {"daemon.osiris_console.memory_max": "512M",
                             "daemon.osiris_console.host": "127.0.0.1",
                             "daemon.osiris_console.port": 8011,
                             "daemon.osiris_console.graceful_shutdown_secs": 10})
    assert n == 0
    assert (out / "user" / "osiris-console.service").read_text() == (
        user_dir / "osiris-console.service").read_text()


def test_shipped_osiris_console_unit_declares_a_graceful_shutdown_timeout() -> None:
    """Reads the REAL shipped file (not a tmp fixture), same discipline
    test_shipped_osiris_mcp_unit_declares_a_transcripts_root already holds: a future
    edit that drops the flag without meaning to fails here, loudly."""
    repo_root = Path(__file__).resolve().parent.parent
    text = (repo_root / "deploy" / "user" / "osiris-console.service").read_text()
    assert "--timeout-graceful-shutdown " in text


def test_render_skips_the_daemon_lane_when_no_user_dir_exists(tmp_path: Path) -> None:
    deploy = tmp_path / "deploy"
    deploy.mkdir()
    out = tmp_path / "out"
    n = render(deploy, out, {"daemon.osiris_mcp.memory_max": "6G"})
    assert n == 0
    assert not (out / "user").exists()


# --- the reboot-survival guard -------------------------------------------------------

def test_looks_like_a_real_unit_requires_description_and_execstart() -> None:
    from scripts.render_units import _looks_like_a_real_unit

    assert _looks_like_a_real_unit(
        "[Unit]\nDescription=fake\n\n[Service]\nExecStart=/bin/true\n")
    assert not _looks_like_a_real_unit("[Unit]\n\n[Service]\nExecStart=/bin/true\n")
    assert not _looks_like_a_real_unit("[Unit]\nDescription=fake\n\n[Service]\n")
    assert not _looks_like_a_real_unit("[Unit]\nDescription=\n\n[Service]\nExecStart=\n")


def test_render_falls_back_to_the_shipped_file_when_a_substitution_breaks_the_unit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A substitution function that corrupts ExecStart= (a bad regex match, an
    unexpected value shape: the exact failure mode the guard exists for) must never
    reach the installed file; the shipped original is used instead, logged to stderr."""
    import scripts.render_units as ru

    deploy = tmp_path / "deploy"
    user_dir = deploy / "user"
    user_dir.mkdir(parents=True)
    _write_daemon_unit(user_dir, "osiris-console", memory_max="512M")
    original = (user_dir / "osiris-console.service").read_text()

    def _corrupt(text: str, value: object) -> str:
        return "\n".join(ln for ln in text.splitlines() if not ln.startswith("ExecStart="))

    monkeypatch.setitem(ru._DAEMON_SERVICE_SUBS, "osiris-console.service",
                        [("daemon.osiris_console.memory_max", _corrupt)])
    out = tmp_path / "out"
    n = render(deploy, out, {"daemon.osiris_console.memory_max": "1G"})
    assert n == 0  # the fallback is byte-identical to the shipped file, not a real render
    assert (out / "user" / "osiris-console.service").read_text() == original


# --- the transcripts root (ingest.transcripts_root) ----------------------------------

def test_render_substitutes_transcripts_root_in_place(tmp_path: Path) -> None:
    deploy = tmp_path / "deploy"
    user_dir = deploy / "user"
    user_dir.mkdir(parents=True)
    (user_dir / "osiris-mcp.service").write_text(
        "[Unit]\nDescription=fake\n\n[Service]\nType=simple\n"
        "Environment=OSIRIS_TRANSCRIPTS=%h/.claude/projects\n"
        "ExecStart=/bin/true\n\n[Install]\nWantedBy=default.target\n")
    out = tmp_path / "out"
    n = render(deploy, out, {"ingest.transcripts_root": "/mnt/archive/transcripts"})
    assert n == 1
    text = (out / "user" / "osiris-mcp.service").read_text()
    assert "Environment=OSIRIS_TRANSCRIPTS=/mnt/archive/transcripts" in text
    assert "%h/.claude/projects" not in text


def test_render_transcripts_root_default_is_byte_identical(tmp_path: Path) -> None:
    deploy = tmp_path / "deploy"
    user_dir = deploy / "user"
    user_dir.mkdir(parents=True)
    (user_dir / "osiris-mcp.service").write_text(
        "[Unit]\nDescription=fake\n\n[Service]\nType=simple\n"
        "Environment=OSIRIS_TRANSCRIPTS=%h/.claude/projects\n"
        "ExecStart=/bin/true\n\n[Install]\nWantedBy=default.target\n")
    out = tmp_path / "out"
    n = render(deploy, out, {})  # no override, spec default is None
    assert n == 0
    assert (out / "user" / "osiris-mcp.service").read_text() == (
        user_dir / "osiris-mcp.service").read_text()


def test_render_transcripts_root_inserts_a_new_line_when_none_shipped(tmp_path: Path) -> None:
    deploy = tmp_path / "deploy"
    user_dir = deploy / "user"
    user_dir.mkdir(parents=True)
    (user_dir / "osiris-mcp.service").write_text(
        "[Unit]\nDescription=fake\n\n[Service]\nType=simple\n"
        "ExecStart=/bin/true\n\n[Install]\nWantedBy=default.target\n")
    out = tmp_path / "out"
    n = render(deploy, out, {"ingest.transcripts_root": "/mnt/archive/transcripts"})
    assert n == 1
    text = (out / "user" / "osiris-mcp.service").read_text()
    assert "Environment=OSIRIS_TRANSCRIPTS=/mnt/archive/transcripts" in text
    assert text.index("Environment=OSIRIS_TRANSCRIPTS=") < text.index("[Install]")


def test_shipped_osiris_mcp_unit_declares_a_transcripts_root() -> None:
    """The actual gap this whole lane traces back to: osiris-mcp's own
    unit never set OSIRIS_TRANSCRIPTS at all, so `settings.osiris_transcripts` defaulted
    to "" inside the MCP process. Reads the REAL shipped file (not a tmp fixture):
    a future edit that drops the line without meaning to fails here, loudly."""
    repo_root = Path(__file__).resolve().parent.parent
    text = (repo_root / "deploy" / "user" / "osiris-mcp.service").read_text()
    assert "Environment=OSIRIS_TRANSCRIPTS=" in text


# --- the one async hop --------------------------------------------------------------

async def test_configured_values_reads_the_real_settings_table(
    actions: Actions, monkeypatch: pytest.MonkeyPatch, pg_dsn: str,
) -> None:
    from src.orchestrator.settings_service import write_setting

    await write_setting(actions.pool, "daemon.osiris_mcp.memory_max", "5G",
                        actor="operator", because="testing render_units")

    class _FakeSettings:
        database_url = pg_dsn

    # `get_settings` is imported INSIDE `_configured_values` (a local import), so the
    # monkeypatch target is `src.config.settings` itself.
    from src.config import settings as settings_mod

    monkeypatch.setattr(settings_mod, "get_settings", lambda: _FakeSettings())

    values = await _configured_values()
    assert values.get("daemon.osiris_mcp.memory_max") == "5G"
    # every OTHER registered key is present too, at its own default
    assert values.get("daemon.osiris_worker.memory_max") == "3G"
