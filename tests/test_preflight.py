"""Preflight — the audit that can't be forgotten. The judgments are pure; so are these."""
from __future__ import annotations

from scripts.osiris_preflight import evaluate


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
