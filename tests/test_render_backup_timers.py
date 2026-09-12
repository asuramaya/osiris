"""render_backup_timers — "deploy's own timer-install step regenerates the units from
[backup_settings]" (Wave 21, thread f04cce36 piece 3). `render()` is pure (no DB, no
network); `_configured_schedules()` is the one async hop that reads the settings —
proven separately, against a real test DB, by monkeypatching `get_settings`.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from scripts.render_backup_timers import _configured_schedules, render
from src.actions.core import Actions


def _write_unit(deploy_dir: Path, unit: str, calendar: str) -> None:
    name = unit.removesuffix(".timer")
    (deploy_dir / f"{name}.timer").write_text(
        f"[Unit]\nDescription=fake {name}\n\n[Timer]\nOnCalendar={calendar}\n"
        "Persistent=true\n\n[Install]\nWantedBy=timers.target\n")
    (deploy_dir / f"{name}.service").write_text(f"[Unit]\nDescription={name}\n")


def test_render_substitutes_the_override_and_leaves_everything_else_untouched(
    tmp_path: Path,
) -> None:
    deploy = tmp_path / "deploy"
    deploy.mkdir()
    _write_unit(deploy, "osiris-backup.timer", "*-*-* 04,10,16,22:30:00")
    out = tmp_path / "out"
    n = render(deploy, out, {"osiris-backup.timer": "*-*-* 00,12:00:00"})
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
    _write_unit(deploy, "osiris-preflight.timer", "Mon *-*-* 05:00:00")
    out = tmp_path / "out"
    n = render(deploy, out, {})
    assert n == 0
    assert (out / "osiris-preflight.timer").read_text() == (
        deploy / "osiris-preflight.timer").read_text()


def test_render_ignores_an_override_for_a_unit_with_no_shipped_file(tmp_path: Path) -> None:
    deploy = tmp_path / "deploy"
    deploy.mkdir()
    out = tmp_path / "out"
    # no osiris-base-backup.timer exists in deploy/ at all — no crash, nothing written
    n = render(deploy, out, {"osiris-base-backup.timer": "Sun 02:00:00"})
    assert n == 0
    assert not (out / "osiris-base-backup.timer").exists()


async def test_configured_schedules_reads_the_real_settings_table(
    actions: Actions, monkeypatch: pytest.MonkeyPatch, pg_dsn: str,
) -> None:
    from src.orchestrator.backup_settings import write_backup_settings

    await write_backup_settings(
        actions.pool, actor="operator", because="testing render",
        timer_schedules={"osiris-base-backup.timer": "Sat 02:00:00"})

    class _FakeSettings:
        database_url = pg_dsn

    # `get_settings` is imported INSIDE `_configured_schedules` (a local import), so the
    # monkeypatch target is `src.config.settings` itself — the module the fresh import
    # resolves against on every call, not `render_backup_timers`'s own namespace (it
    # never binds the name at module level at all).
    from src.config import settings as settings_mod

    monkeypatch.setattr(settings_mod, "get_settings", lambda: _FakeSettings())

    overrides = await _configured_schedules()
    assert overrides == {"osiris-base-backup.timer": "Sat 02:00:00"}
