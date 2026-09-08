"""The PITR drill (vault lane, ruling 39384a87/c53a5fc0, item 3's own last piece) — the
config-text builder is pure and tested directly; the real docker orchestration was proven
by an actual run against real production data (base backup osiris-basebackup-20260908-164247
.tar.gz, marker decision:1f5c985a63a4), reported to Thoth, same discipline
osiris_archive_wal.sh's own WAL-writing half was verified with."""
from __future__ import annotations

from pathlib import Path

from scripts.osiris_pitr_drill import CONTAINER, DRILL_NAME, postgresql_auto_conf_pitr, run_drill

# --- thread 9fac4e0d part 4: never the live cluster ---------------------------------

def test_run_drill_refuses_when_drill_name_equals_the_source_container() -> None:
    """The exact live incident this obligation was named for: a drill target that
    collides with the container being drilled generates real WAL against the thing
    it's supposed to leave untouched — this must refuse outright, before touching
    docker at all."""
    fail = run_drill(
        Path("/nonexistent.tar.gz"), None, "message:1",
        drill_name=CONTAINER)
    assert fail is not None
    assert "REFUSING" in fail
    assert CONTAINER in fail


def test_run_drill_default_drill_name_never_equals_the_default_container() -> None:
    """The DEFAULT shape (no caller override) must already be safe — a regression
    guard on the two module constants themselves, not just the runtime check."""
    assert DRILL_NAME != CONTAINER


def test_no_target_time_sets_no_recovery_target() -> None:
    """The default drill mode: replay everything the archive can produce, promote at
    the end — no recovery_target_time/recovery_target_action lines at all."""
    conf = postgresql_auto_conf_pitr("cp /wal/%f %p", None)
    assert "restore_command = 'cp /wal/%f %p'" in conf
    assert "recovery_target_time" not in conf
    assert "recovery_target_action" not in conf


def test_an_explicit_target_time_is_set_with_a_promote_action() -> None:
    conf = postgresql_auto_conf_pitr("cp /wal/%f %p", "2026-09-08T21:50:00+00:00")
    assert "recovery_target_time = '2026-09-08T21:50:00+00:00'" in conf
    assert "recovery_target_action = 'promote'" in conf


def test_a_custom_target_action_is_honored() -> None:
    conf = postgresql_auto_conf_pitr("cp /wal/%f %p", "2026-09-08T21:50:00+00:00",
                                     target_action="pause")
    assert "recovery_target_action = 'pause'" in conf
