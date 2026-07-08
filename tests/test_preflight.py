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
