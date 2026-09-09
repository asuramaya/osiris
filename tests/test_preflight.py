"""Preflight — the audit that can't be forgotten. The judgments are pure; so are these."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest
from scripts.osiris_preflight import _run_check, evaluate


def _green() -> dict:
    return {
        "units": {"osiris-mcp": {"enabled": "enabled", "active": "active"}},
        "timers": {"osiris-backup.timer": {"enabled": "enabled", "active": "active"}},
        "containers": {"osiris-pg": {"status": "running", "restart": "unless-stopped",
                                     "vols": ["osiris-pg-data"]}},
        "ports": [], "backup_age_h": 3.0, "vault_age_d": 1.0, "unpushed": 180,
    }


def test_green_matrix_yields_no_failures() -> None:
    assert evaluate(_green()) == []


def test_each_rigging_is_named() -> None:
    m = _green()
    m["units"]["osiris-mcp"]["enabled"] = "disabled"          # class 2: silent absence
    m["containers"]["osiris-pg"]["restart"] = "no"            # class 1: dead after reboot
    m["containers"]["osiris-pg"]["vols"] = ["a" * 64]         # class 1: anonymous volume
    m["ports"] = ["5432"]                                     # class 4: the shadow trap
    m["backup_age_h"] = 72.0                                  # class 5: stale backups
    m["vault_age_d"] = 30.0
    fails = "\n".join(evaluate(m))
    assert "NOT start at boot" in fails
    assert "NO restart policy" in fails
    assert "ANONYMOUS volume" in fails
    assert "shadow-DB trap" in fails
    assert "backup is 72h old" in fails
    assert "vault untouched" in fails


def test_missing_everything_is_loud() -> None:
    m = _green()
    m["containers"]["osiris-pg"] = None
    m["backup_age_h"] = None
    m["vault_age_d"] = None
    fails = "\n".join(evaluate(m))
    assert "not running" in fails and "NO backups exist" in fails and "vault is empty" in fails


# --- failure-class 7: the miner tick (down a day behind a green heartbeat) --------------

def _healthy_miner() -> dict:
    return {"last_ok_age_min": 4.0, "recent_errors": 0, "recent": 6}


def test_healthy_miner_is_green() -> None:
    m = _green()
    m["miner"] = _healthy_miner()
    assert evaluate(m) == []


def test_absent_miner_telemetry_is_quiet() -> None:
    """None = the instrument is young or the DB is down — the latter fails elsewhere."""
    m = _green()
    m["miner"] = None
    assert evaluate(m) == []


def test_a_SILENT_adversary_is_not_a_BROKEN_one() -> None:
    """THE LAW CHANGED WHEN THE CRAWL DIED (ceae1604), and this test guarded the old one.

    "Three missed ticks = sensing is down" was TRUE of a cron that walked every transcript every
    ten minutes: silence meant the memory had stopped forming, and this check was right to fail on
    it (the miner once died for ten hours behind a green heartbeat). But the miner is SUMMONED now,
    at a session's death rite. A quiet hour means NOBODY'S SESSION ENDED — not that anything is
    broken. Demanding a tick from a job that no longer ticks would fail this preflight FOREVER, on
    purpose, about nothing.

    ABSENCE OF ACTIVITY IS NOT EVIDENCE OF FAILURE; IT IS ONLY EVIDENCE OF ABSENCE. It is the same
    distinction the wall now draws between "untouched" and "resolved", and the same one the
    liveness fix drew between "quiet" and "dead". Osiris keeps relearning it.
    """
    m = _green()
    m["miner"] = {**_healthy_miner(), "last_ok_age_min": 240.0}   # four hours of quiet...
    assert evaluate(m) == [], "silence from a summoned producer is not an outage"
    m["miner"] = {**_healthy_miner(), "last_ok_age_min": None}    # ...or no run at all
    assert evaluate(m) == []


def test_failing_open_miner_is_named() -> None:
    m = _green()
    m["miner"] = {**_healthy_miner(), "recent_errors": 3}
    assert "errored 3 of the last 6 runs" in "\n".join(evaluate(m))


# --- the deploy-ordering guard's weekly backstop (thread e6f5556f) --------------------------

def test_no_schema_drift_key_is_quiet() -> None:
    """collect_schema_drift() returning None (matched, or DB unreachable) is silence, same as
    every other None-shaped field in this matrix."""
    assert evaluate(_green()) == []


def test_a_real_schema_drift_is_named_loudly() -> None:
    m = _green()
    m["schema_drift"] = "code expects migration head '0036', DB is at '0034'"
    fails = "\n".join(evaluate(m))
    assert "SCHEMA DRIFT" in fails and "0034" in fails and "alembic upgrade head" in fails


# --- obligation a867ae37: the ENOSPC incident's own early-warning -----------------------

def test_no_tmp_inode_pct_key_is_quiet() -> None:
    """Absent (statvfs failed, or the field was never collected) is silence, same as every
    other None-shaped field in this matrix — a genuine 'can't answer' is never a false alarm."""
    assert evaluate(_green()) == []


def test_low_tmp_inode_use_is_quiet() -> None:
    m = _green()
    m["tmp_inode_pct"] = 23.0
    assert evaluate(m) == []


def test_high_tmp_inode_use_is_named_loudly() -> None:
    """The exact shape of the real incident: /tmp's per-call harness output files fail
    ENOSPC once inode use hits 100%, silently to anything that isn't shelling out — this
    is the signal that must fire well before that, per Thoth's own ask (msg 7096)."""
    m = _green()
    m["tmp_inode_pct"] = 91.4
    fails = "\n".join(evaluate(m))
    assert "/tmp inode use at 91.4%" in fails and "a867ae37" in fails


def test_tmp_inode_use_right_at_the_alarm_threshold_fires() -> None:
    """Boundary case: >= the threshold, not only strictly over it — a reading sitting
    exactly on 80% is exactly the situation this alarm exists to catch, not a near miss."""
    m = _green()
    m["tmp_inode_pct"] = 80.0
    assert any("/tmp inode use" in f for f in evaluate(m))


# --- thread 3e96c10e: a dead check must ALARM, never quietly pass as green -------------------

async def _boom() -> None:
    raise ModuleNotFoundError("No module named 'src'")


async def _down() -> None:
    raise ConnectionRefusedError("connection refused")


def test_a_broken_check_is_named_not_swallowed() -> None:
    """The canonical failure: an import-time error inside a collector used to be caught by a
    bare `except Exception` and degrade to a quiet None — preflight then reported "all green"
    while the check never actually ran. Now it surfaces as its own named failure."""
    result, broken = _run_check("collect_miner", _boom())
    assert result is None
    assert broken is not None
    assert "collect_miner check is BROKEN" in broken
    assert "ModuleNotFoundError" in broken
    assert "did NOT actually run" in broken


def test_a_genuinely_unreachable_db_still_degrades_quietly() -> None:
    """The distinction this fix must preserve: the DB actually being down is already reported
    by the unit/container checks — a collector failing on THAT is not a second, redundant
    alarm, so it still degrades to a quiet None exactly as before."""
    result, broken = _run_check("collect_schema_drift", _down())
    assert result is None
    assert broken is None


def test_tmp_inode_pct_reads_a_real_filesystem() -> None:
    """Proven against the real root filesystem, not a mock — os.statvfs is a thin, portable
    stdlib call (no `df` subprocess), and this just confirms it returns a plausible
    percentage rather than silently degrading to None on an ordinary, present path."""
    from scripts.osiris_preflight import _tmp_inode_pct

    pct = _tmp_inode_pct("/")
    assert pct is not None
    assert 0.0 <= pct <= 100.0


def test_tmp_inode_pct_degrades_quietly_on_a_path_that_does_not_exist() -> None:
    from scripts.osiris_preflight import _tmp_inode_pct

    assert _tmp_inode_pct("/no/such/path/at/all") is None


# --- the vault lane item 5: the disk guard's read-side weekly early-warning ------------------

def test_no_disk_free_pct_key_is_quiet() -> None:
    """Absent (the vault doesn't exist yet, or the field was never collected) is silence,
    same as every other None-shaped field in this matrix."""
    assert evaluate(_green()) == []


def test_plenty_of_free_disk_is_quiet() -> None:
    m = _green()
    m["disk_free_pct"] = 40.0
    assert evaluate(m) == []


def test_low_free_disk_is_named_loudly() -> None:
    """The exact shape of the emergency that opened this whole lane: 92% full, ~3 days
    runway at 44GB/day, caught by a human noticing rather than by any check."""
    m = _green()
    m["disk_free_pct"] = 8.0
    fails = "\n".join(evaluate(m))
    assert "disk free at 8.0%" in fails and "osiris_prune_ladder.py" in fails


def test_free_disk_right_at_the_alarm_threshold_fires() -> None:
    """Boundary case: <= the threshold, not only strictly under it."""
    m = _green()
    m["disk_free_pct"] = 15.0
    assert any("disk free at" in f for f in evaluate(m))


def test_disk_free_pct_reads_a_real_filesystem(tmp_path: Path) -> None:
    from scripts.osiris_preflight import _disk_free_pct

    pct = _disk_free_pct(tmp_path)
    assert pct is not None
    assert 0.0 <= pct <= 100.0


def test_disk_free_pct_degrades_quietly_on_a_path_that_does_not_exist() -> None:
    from scripts.osiris_preflight import _disk_free_pct

    assert _disk_free_pct(Path("/no/such/path/at/all")) is None


# --- the vault lane item 3's own last piece: the PITR drill --------------------------------

def test_drill_pitr_is_quiet_when_no_base_backup_exists_yet(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An environment where item 3 hasn't produced a base backup yet (a fresh checkout,
    a test run, a fleet member that hasn't hit its first weekly timer) is not a
    failure of THIS check — a real failed restore against an EXISTING base backup is."""
    import scripts.osiris_preflight as preflight

    monkeypatch.setattr(preflight, "VAULT_DIR", tmp_path)
    assert preflight.drill_pitr() is None


# --- thread 9fac4e0d part 4: never the live cluster -----------------------------------

def test_drill_container_name_never_collides_with_a_live_fleet_container() -> None:
    """The regression guard on the module constants themselves — the exact live
    incident this obligation was named for was a drill-shaped restore that DID
    collide with the live cluster, just under a different database name inside the
    same container, which this guard cannot see; codifying the container-name half
    at least closes the door this script's own drill could walk through."""
    from scripts.osiris_preflight import _DRILL_CONTAINER_NAME, CONTAINERS

    assert _DRILL_CONTAINER_NAME not in CONTAINERS


def test_restore_cmd_uses_pg_restore_for_a_dot_dump_file() -> None:
    """THE REAL LIVE BUG (found running the drill against a real production dump,
    thread 9fac4e0d part 4): -Fc custom-format dumps (the vault lane item 1) piped
    into psql fail — psql expects SQL text, not pg_dump's own binary container
    format. Only pg_restore reads a .dump file."""
    from scripts.osiris_preflight import _restore_cmd

    cmd = _restore_cmd("drillbox", "/vault/osiris-20260908-163007.dump")
    assert "pg_restore" in cmd
    assert "psql" not in cmd
    assert "--no-owner" in cmd


def test_restore_cmd_uses_psql_for_a_legacy_dot_sql_file() -> None:
    """The pre-item-1 legacy extension collect()'s own glob still tolerates during
    the transition window — genuine SQL text, still needs psql, never pg_restore."""
    from scripts.osiris_preflight import _restore_cmd

    cmd = _restore_cmd("drillbox", "/vault/osiris-20260901-000000.sql")
    assert "psql" in cmd
    assert "pg_restore" not in cmd


# --- thread 78efd46d, the soul store's own coverage guarantee ---------------------------

def test_no_missing_sessions_key_is_quiet() -> None:
    assert evaluate(_green()) == []


def test_zero_missing_is_quiet() -> None:
    m = _green()
    m["soul_store_missing"] = 0
    assert evaluate(m) == []


def test_a_real_coverage_gap_is_named_loudly() -> None:
    m = _green()
    m["soul_store_missing"] = 3
    fails = "\n".join(evaluate(m))
    assert "SOUL STORE COVERAGE GAP: 3 session" in fails and "78efd46d" in fails


def test_find_missing_sessions_is_a_pure_set_difference() -> None:
    from scripts.osiris_preflight import find_missing_sessions

    disk = {"aaa", "bbb", "ccc"}
    stored = {"aaa", "bbb"}
    assert find_missing_sessions(disk, stored) == {"ccc"}


# --- thread 78efd46d item 2: the soul store's own round-trip proof --------------------

def test_no_round_trip_failures_key_is_quiet() -> None:
    assert evaluate(_green()) == []


def test_empty_round_trip_failures_list_is_quiet() -> None:
    m = _green()
    m["soul_round_trip_failures"] = []
    assert evaluate(m) == []


def test_a_real_round_trip_failure_is_named_loudly() -> None:
    m = _green()
    m["soul_round_trip_failures"] = [{"anchor_sid": "deadbeef01", "error": "chain broken"}]
    fails = "\n".join(evaluate(m))
    assert ("SOUL STORE ROUND-TRIP FAILURE: 1 sampled session" in fails
            and "deadbeef01" in fails and "78efd46d" in fails)


def test_format_round_trip_failure_is_pure() -> None:
    from scripts.osiris_preflight import _format_round_trip_failure

    assert _format_round_trip_failure(None) is None
    assert _format_round_trip_failure([]) is None
    msg = _format_round_trip_failure([{"anchor_sid": "abc123", "error": "mismatch"}])
    assert msg is not None and "abc123" in msg


def test_find_missing_sessions_is_empty_when_store_has_everything() -> None:
    from scripts.osiris_preflight import find_missing_sessions

    assert find_missing_sessions({"aaa", "bbb"}, {"aaa", "bbb", "ccc"}) == set()


async def _no_transcripts_root(tmp_path: Path) -> int | None:
    from scripts.osiris_preflight import collect_soul_store_coverage

    # check_crush=False: CrushSqliteAdapter.enumerate() takes no root at all (it always
    # walks the REAL ~/.local/share/crush/projects.json + seat offices, wave 13 item 2's
    # own documented limit) — a dev box with real crush sessions would otherwise make
    # this "nothing to check" test reach for a real DB connection.
    return await collect_soul_store_coverage(
        root=tmp_path / "does-not-exist", check_crush=False)


def test_collect_soul_store_coverage_is_quiet_with_no_transcripts_root(
    tmp_path: Path,
) -> None:
    """No transcripts root present in this environment (a fresh checkout, a test box) is
    not a failure of THIS check — nothing to walk, nothing to claim about."""
    import asyncio

    assert asyncio.run(_no_transcripts_root(tmp_path)) is None


def test_collect_soul_store_coverage_is_quiet_with_an_empty_transcripts_root(
    tmp_path: Path,
) -> None:
    import asyncio

    from scripts.osiris_preflight import collect_soul_store_coverage

    root = tmp_path / "projects"
    root.mkdir()
    assert asyncio.run(
        collect_soul_store_coverage(root=root, check_crush=False)) is None


def test_backfill_bare_invocation_never_raises_module_not_found(tmp_path: Path) -> None:
    """The exact repro from thread 3e96c10e: `.venv/bin/python scripts/backfill_thread_arc.py`
    from the repo root, PYTHONPATH deliberately unset — sys.path[0] is the script's own
    directory, never CWD, so its top-level `from src...` imports crashed immediately. `--help`
    exercises exactly that import path (argparse prints usage and exits before any DB touch,
    so this stays hermetic — no real Postgres needed, unlike osiris_preflight.py's collectors,
    which this file's `_run_check` tests above cover without a subprocess)."""
    repo_root = Path(__file__).resolve().parent.parent
    env = {"PATH": "/usr/bin:/bin", "HOME": str(tmp_path)}  # explicitly no PYTHONPATH
    out = subprocess.run(
        [sys.executable, "scripts/backfill_thread_arc.py", "--help"], cwd=repo_root, env=env,
        capture_output=True, text=True, timeout=30)
    assert out.returncode == 0
    assert "ModuleNotFoundError" not in out.stderr
    assert "usage:" in out.stdout
