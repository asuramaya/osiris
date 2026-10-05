"""Every timer shipped in deploy/ is installed by deploy, or is named here as manual on purpose.
A timer that ships but is never installed does nothing, silently: the 30 day outbox reaper sat in
deploy/ for weeks while the table it was meant to prune kept growing."""
from __future__ import annotations

import re
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEPLOY = ROOT / "deploy"
INSTALLER = (ROOT / "scripts" / "install_prune_timers.sh").read_text()

# timers that are deliberately NOT installed by scripts/install_prune_timers.sh
INTENTIONALLY_MANUAL: frozenset[str] = frozenset()


def _installed_units() -> set[str]:
    match = re.search(r'^UNITS="([^"]+)"', INSTALLER, re.MULTILINE)
    assert match, "the installer's UNITS list was not found"
    return set(match.group(1).split())


def _enabled_timers() -> set[str]:
    block = INSTALLER.split("systemctl --user enable --now", 1)[1].split("\nfi", 1)[0]
    return {name.removesuffix(".timer") for name in re.findall(r"osiris-[a-z0-9-]+\.timer", block)}


def _shipped_timers() -> set[str]:
    return {p.stem for p in DEPLOY.glob("osiris-*.timer")}


def test_the_floor_is_not_empty() -> None:
    assert len(_shipped_timers()) >= 8 and len(_installed_units()) >= 8


def test_every_shipped_timer_is_installed_or_named_as_manual() -> None:
    missing = _shipped_timers() - _installed_units() - INTENTIONALLY_MANUAL
    assert not missing, f"shipped but never installed by deploy: {sorted(missing)}"


def test_every_installed_unit_has_both_files_and_is_enabled() -> None:
    installed = _installed_units()
    for name in installed:
        assert (DEPLOY / f"{name}.timer").is_file(), name
        assert (DEPLOY / f"{name}.service").is_file(), name
    assert installed == _enabled_timers(), "UNITS and the enable --now list disagree"


def test_a_manual_exemption_names_a_timer_that_still_ships() -> None:
    assert INTENTIONALLY_MANUAL <= _shipped_timers()


# --- the weekly restore drill's own schedule --------------------------------------------


def _oncalendar(unit: str) -> tuple[str, int, int]:
    text = (DEPLOY / f"{unit}.timer").read_text()
    found = re.search(r"^OnCalendar=(\w+) (?:\*-\*-\* )?(\d\d):(\d\d)", text, re.M)
    assert found, f"{unit}: no weekly OnCalendar"
    return found.group(1), int(found.group(2)), int(found.group(3))


def test_the_restore_drill_runs_after_the_sunday_base_backup_and_ends_before_preflight() -> None:
    day, hh, mm = _oncalendar("osiris-pitr-drill")
    base_day, base_hh, base_mm = _oncalendar("osiris-base-backup")
    assert day == base_day == "Sun"
    assert (hh, mm) > (base_hh, base_mm)
    service = (DEPLOY / "osiris-pitr-drill.service").read_text()
    timeout = int(re.search(r"^TimeoutStartSec=(\d+)h", service, re.M).group(1))
    started = timedelta(hours=hh, minutes=mm)
    preflight_day, p_hh, p_mm = _oncalendar("osiris-preflight")
    assert preflight_day == "Mon"
    monday_preflight = timedelta(days=1, hours=p_hh, minutes=p_mm)
    assert started + timedelta(hours=timeout) < monday_preflight


def test_the_restore_drill_unit_is_capped_low_priority_and_runs_the_drill_script() -> None:
    service = (DEPLOY / "osiris-pitr-drill.service").read_text()
    assert re.search(r"^MemoryMax=10G$", service, re.M)
    assert re.search(r"^Nice=\d+$", service, re.M)
    assert "IOSchedulingClass=idle" in service
    assert "scripts/osiris_pitr_drill.py" in service
    assert (ROOT / "scripts" / "osiris_pitr_drill.py").is_file()


def test_the_wal_pull_runs_on_its_own_frequent_timer_apart_from_the_dump() -> None:
    """A full dump of a large database is slow and saturates the disk; the pull of completed
    WAL segments is small and must run often. They are separate units: a daily dump, and a
    pull every 15 minutes."""
    backup_timer = (DEPLOY / "osiris-backup.timer").read_text()
    pull_timer = (DEPLOY / "osiris-wal-pull.timer").read_text()
    assert "OnCalendar=*-*-* 04:30:00" in backup_timer
    assert "OnCalendar=*:0/15" in pull_timer
    assert "osiris_wal_pull.sh" in (DEPLOY / "osiris-wal-pull.service").read_text()
    assert "osiris-wal-pull" in _installed_units() and "osiris-wal-pull" in _enabled_timers()
    dump_script = (ROOT / "scripts" / "osiris_backup.sh").read_text()
    assert "docker exec osiris-pg cat" not in dump_script   # the pull is not in the dump script
    assert "pg_dump" in dump_script and "osiris_disk_guard.py" in dump_script
    pull_script = (ROOT / "scripts" / "osiris_wal_pull.sh").read_text()
    assert "wal_archive" in pull_script and "docker exec osiris-pg pg_dump" not in pull_script
