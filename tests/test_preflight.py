"""Preflight — the audit that can't be forgotten. The judgments are pure; so are these."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

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
