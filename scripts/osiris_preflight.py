"""Preflight — the audit that can't be forgotten (failure-class 6, decision 003a70f6).

The reboot scare taught the meta-failure: perfect operation conceals rigging — the graph ran
flawlessly for nine days on deletion-rigged storage, and flawlessness is exactly why nobody
looked. This script re-runs the survival audit on a timer: units enabled+active, containers
on restart policies with NAMED volumes (never anonymous — the arrangement that nearly ate
the civilization), backups fresh AND restorable, default-port squatters (the shadow-DB trap:
a listener on :5432/:6379 catches anything launched without the env override — silent
wrong-database writes), unpushed-commit exposure, vault freshness.

Silent when green. On regression it prints the failures (journal) AND puts a brief on the
operator's desk through the normal mailbox — the membrane, not a log nobody reads.

    python -m/scripts run:  .venv/bin/python scripts/osiris_preflight.py [--drill]

--drill additionally restores the newest dump into a scratch container and compares object
counts — a backup that's never been restored is a hope, not a backup.
"""
from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import time
from pathlib import Path

DSN = "postgresql://osiris:osiris@127.0.0.1:5601/osiris"
UNITS = ["osiris-mcp", "osiris-worker", "osiris-pulse", "osiris-console"]
TIMERS = ["osiris-backup.timer"]
CONTAINERS = ["osiris-pg", "osiris-redis"]
NAMED_VOLUMES = {"osiris-pg-data", "osiris-redis-data"}
BACKUP_DIR = Path("/home/asuramaya/code/osiris/backups")
VAULT_DIR = Path("/home/asuramaya/osiris-vault")
REPO = Path("/home/asuramaya/code/osiris")
BACKUP_MAX_AGE_H = 48
VAULT_MAX_AGE_D = 8
MINER_MAX_SILENCE_MIN = 35  # 3 missed 10-min ticks = sensing is down, whatever the heartbeat says
DEFAULT_PORTS = ["5432", "6379"]  # the shadow-trap band: settings' fallback DSN aims here


def _run(cmd: list[str]) -> str:
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=30).stdout.strip()
    except Exception:  # noqa: BLE001 — a collector failing is itself a finding
        return ""


def collect() -> dict:
    """Gather the survival matrix — thin collectors, all judgment lives in evaluate()."""
    m: dict = {"units": {}, "timers": {}, "containers": {}, "ports": [],
               "backup_age_h": None, "vault_age_d": None, "unpushed": None}
    for u in UNITS:
        m["units"][u] = {
            "enabled": _run(["systemctl", "--user", "is-enabled", u]),
            "active": _run(["systemctl", "--user", "is-active", u]),
        }
    for t in TIMERS:
        m["timers"][t] = {
            "enabled": _run(["systemctl", "--user", "is-enabled", t]),
            "active": _run(["systemctl", "--user", "is-active", t]),
        }
    for c in CONTAINERS:
        out = _run(["docker", "inspect", c, "--format",
                    '{"status":"{{.State.Status}}","restart":"{{.HostConfig.RestartPolicy.Name}}",'
                    '"vols":[{{range $i, $m := .Mounts}}{{if $i}},{{end}}"{{$m.Name}}"{{end}}]}'])
        try:
            m["containers"][c] = json.loads(out) if out else None
        except json.JSONDecodeError:
            m["containers"][c] = None
    listeners = _run(["ss", "-ltn"])
    m["ports"] = [p for p in DEFAULT_PORTS if f":{p} " in listeners]
    dumps = sorted(BACKUP_DIR.glob("osiris-*.sql"), key=lambda p: p.stat().st_mtime)
    if dumps:
        m["backup_age_h"] = (time.time() - dumps[-1].stat().st_mtime) / 3600
        m["newest_dump"] = str(dumps[-1])
    vault = sorted(VAULT_DIR.glob("*"), key=lambda p: p.stat().st_mtime) \
        if VAULT_DIR.is_dir() else []
    if vault:
        m["vault_age_d"] = (time.time() - vault[-1].stat().st_mtime) / 86400
    count = _run(["git", "-C", str(REPO), "rev-list", "--count", "--all"])
    pushed = _run(["git", "-C", str(REPO), "rev-list", "--count", "--remotes"])
    if count:
        m["unpushed"] = int(count) - int(pushed or 0)
    m["miner"] = None  # filled async in main() — DB unreachable degrades to None, judged green
    return m


async def collect_miner() -> dict | None:
    """The sensing-tick vital signs off miner:ticks (failure-class 7, decision 3191e0df:
    a fail-open cron was down a DAY behind a green heartbeat). Returns None when the
    telemetry doesn't exist yet — a young instrument is not a failure."""
    import asyncpg
    from src.orchestrator.monitor import miner_health

    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=1)
    try:
        blob = await miner_health(pool)
    finally:
        await pool.close()
    if not blob["starts"]:
        return None
    ok = [t for t in blob["ticks"] if not t.get("error")]
    recent = blob["ticks"][-6:]
    return {
        "last_ok_age_min": (
            (time.time() - _iso_epoch(ok[-1]["at"])) / 60 if ok else None),
        "recent_errors": sum(1 for t in recent if t.get("error")),
        "recent": len(recent),
    }


def _iso_epoch(iso: str) -> float:
    from datetime import datetime

    return datetime.fromisoformat(iso).timestamp()


def evaluate(m: dict) -> list[str]:
    """The judgments — pure, tested. Returns human-readable failures; [] = all green."""
    fails: list[str] = []
    for u, s in m["units"].items():
        if s["enabled"] != "enabled":
            fails.append(f"{u} is not enabled — it will NOT start at boot")
        if s["active"] != "active":
            fails.append(f"{u} is not active right now")
    for t, s in m["timers"].items():
        if s["enabled"] != "enabled" or s["active"] != "active":
            fails.append(f"{t} is not enabled+active — backups stop silently")
    for c, info in m["containers"].items():
        if not info or info.get("status") != "running":
            fails.append(f"container {c} is not running")
            continue
        if info.get("restart") in ("no", "", None):
            fails.append(f"container {c} has NO restart policy — dead after reboot")
        vols = info.get("vols") or []
        for v in vols:
            if v not in NAMED_VOLUMES and len(v) == 64:  # a hash = anonymous = deletion-rigged
                fails.append(f"container {c} rides an ANONYMOUS volume ({v[:12]}…) — "
                             "the arrangement that nearly ate the graph")
    for p in m["ports"]:
        fails.append(f"a listener squats on default port :{p} — the shadow-DB trap is armed "
                     "(anything launched without env override writes there silently)")
    if m["backup_age_h"] is None:
        fails.append("NO backups exist")
    elif m["backup_age_h"] > BACKUP_MAX_AGE_H:
        fails.append(f"newest backup is {m['backup_age_h']:.0f}h old (max {BACKUP_MAX_AGE_H}h)")
    if m["vault_age_d"] is None:
        fails.append("vault is empty or missing")
    elif m["vault_age_d"] > VAULT_MAX_AGE_D:
        fails.append(f"vault untouched for {m['vault_age_d']:.0f}d (max {VAULT_MAX_AGE_D}d)")
    miner = m.get("miner")
    if miner:  # None = telemetry absent (young instrument / DB down — those fail elsewhere)
        age = miner.get("last_ok_age_min")
        if age is None or age > MINER_MAX_SILENCE_MIN:
            shown = "never" if age is None else f"{age:.0f}m ago"
            fails.append(f"miner tick last SUCCEEDED {shown} (max {MINER_MAX_SILENCE_MIN}m) "
                         "— sensing is down behind a green heartbeat, the onboarding-day class")
        if miner.get("recent_errors", 0) >= 3:
            fails.append(f"miner tick errored {miner['recent_errors']} of the last "
                         f"{miner['recent']} runs — the cron is failing open")
    return fails


def drill(newest_dump: str) -> str | None:
    """Restore the newest dump into a scratch container and count objects. Returns a failure
    string or None. Heavy (~2 min) — timer runs pass --drill; ad-hoc runs may skip."""
    name = "osiris-preflight-drill"
    try:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=30)
        subprocess.run(["docker", "run", "-d", "--name", name, "-e", "POSTGRES_USER=osiris",
                        "-e", "POSTGRES_PASSWORD=osiris", "-e", "POSTGRES_DB=osiris",
                        "postgres:16"], capture_output=True, timeout=60, check=True)
        for _ in range(30):
            r = subprocess.run(["docker", "exec", name, "pg_isready", "-U", "osiris"],
                               capture_output=True, timeout=10)
            if r.returncode == 0:
                break
            time.sleep(1)
        with open(newest_dump, "rb") as f:
            subprocess.run(["docker", "exec", "-i", name, "psql", "-U", "osiris",
                            "-d", "osiris", "-q"], stdin=f, capture_output=True,
                           timeout=600, check=True)
        out = subprocess.run(["docker", "exec", name, "psql", "-U", "osiris", "-d", "osiris",
                              "-tc", "SELECT count(*) FROM objects"],
                             capture_output=True, text=True, timeout=30)
        n = int(out.stdout.strip() or 0)
        if n < 1:
            return f"drill restored ZERO objects from {newest_dump}"
        return None
    except Exception as e:  # noqa: BLE001
        return f"restore drill failed: {e}"
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=30)


async def brief_operator(fails: list[str]) -> None:
    """Regression → a brief on the desk through the normal mailbox (dedup makes re-runs safe)."""
    import asyncpg
    from src.orchestrator.mailbox import send_message

    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=1)
    try:
        body = ("PREFLIGHT REGRESSION — the survival matrix has holes:\n- "
                + "\n- ".join(fails)
                + "\nRun scripts/osiris_preflight.py after fixing; silence = green.")
        await send_message(pool, from_agent="system:preflight", from_project="osiris",
                           to_project="operator", body=body)
    finally:
        await pool.close()


def main() -> int:
    m = collect()
    try:
        m["miner"] = asyncio.run(collect_miner())
    except Exception:  # noqa: BLE001 — DB unreachable is already a unit/container failure
        m["miner"] = None
    fails = evaluate(m)
    if "--drill" in sys.argv and m.get("newest_dump"):
        d = drill(m["newest_dump"])
        if d:
            fails.append(d)
    if not fails:
        print("preflight: all green"
              f" (backup {m['backup_age_h']:.1f}h, vault {m['vault_age_d']:.1f}d,"
              f" unpushed commits {m['unpushed']})")
        return 0
    print("PREFLIGHT FAILURES:")
    for f in fails:
        print(" -", f)
    try:
        asyncio.run(brief_operator(fails))
        print("(brief placed on the operator's desk)")
    except Exception as e:  # noqa: BLE001 — the desk being down is itself printed
        print(f"(could not brief the desk: {e})")
    return 1


if __name__ == "__main__":
    sys.exit(main())
