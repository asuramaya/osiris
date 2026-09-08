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
import os
import subprocess
import sys
import time
from collections.abc import Coroutine
from pathlib import Path
from typing import Any

import asyncpg

# `from src...` (deferred, below) needs the repo root importable regardless of PYTHONPATH —
# osiris_fleet_glance.py's own precedent (thread 3e96c10e: this script's deferred `from
# src...` imports failed ModuleNotFoundError on the exact bare invocation its own docstring
# documents, since sys.path[0] is the script's own directory, never CWD).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

DSN = "postgresql://osiris:osiris@127.0.0.1:5601/osiris"
UNITS = ["osiris-mcp", "osiris-worker", "osiris-pulse", "osiris-console"]
TIMERS = ["osiris-backup.timer"]
CONTAINERS = ["osiris-pg", "osiris-redis"]
NAMED_VOLUMES = {"osiris-pg-data", "osiris-redis-data"}
# Portable: derive the repo from THIS file, never a hardcoded home — a path baked to one
# machine is a script that only works for the person who wrote it.
REPO = Path(os.environ.get("OSIRIS_REPO") or Path(__file__).resolve().parent.parent)
BACKUP_DIR = REPO / "backups"
VAULT_DIR = Path(os.environ.get("OSIRIS_VAULT") or Path.home() / "osiris-vault")
BACKUP_MAX_AGE_H = 48
VAULT_MAX_AGE_D = 8
DEFAULT_PORTS = ["5432", "6379"]  # the shadow-trap band: settings' fallback DSN aims here
# THE ENOSPC INCIDENT'S OWN EARLY-WARNING (obligation a867ae37): /tmp (tmpfs, 1,048,576
# inodes) hit 99.98% inode use at ~07:00Z 2026-09-04 — every Bash tool call in every live
# seat failed with ENOSPC on the harness's per-call output-capture file, while MCP/Read
# kept working (silent to anything that doesn't shell out). Root-caused: tests/conftest.py's
# own PID-keyed `basetemp = /tmp/pt-<pid>` (Sekhmet's fix for pytest's AF_UNIX socket-path-
# length bug, msg 2261) opts pytest OUT of ITS OWN default retention cleanup — pytest only
# ever prunes stale runs under its own auto-generated `pytest-of-<user>/pytest-<N>/`
# numbering; a caller-supplied --basetemp is never revisited by anyone else's run, so every
# `pt-<pid>` tree from every gate/test invocation, fleet-wide, accumulates FOREVER. Measured
# live during this obligation's own investigation: 92 leftover `pt-<pid>` dirs spanning ~4.7
# hours of ordinary same-day fleet activity already accounted for 233,279 of 235,301 files
# under /tmp (99.1%) — at real overnight peak concurrency this is squarely why the real
# incident reached ~1M. This alarm is deliberately NOT also the fix (that's a separate,
# larger change — pruning stale `pt-*` trees safely needs to distinguish a live run's own
# directory from an orphaned one, not just an age cutoff) — it only guarantees the NEXT
# climb is seen before every shell on the box dies silently, per Thoth's own ask.
TMP_INODE_ALARM_PCT = 80.0
# THE DISK GUARD'S OWN READ-SIDE HALF (the vault lane, item 5): osiris_disk_guard.py stops a
# single WRITE from landing on a full disk; this is the standing weekly early-warning that
# catches the climb long before any single write is refused — the same emergency that opened
# the whole lane (92% full, ~3 days runway at 44GB/day) was caught by a human noticing, not by
# a check. 15% free (85% used) gives real runway at that measured burn rate before the guard
# above starts refusing writes.
DISK_FREE_ALARM_PCT = 15.0


def _run(cmd: list[str]) -> str:
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=30).stdout.strip()
    except Exception:  # noqa: BLE001 — a collector failing is itself a finding
        return ""


def _tmp_inode_pct(path: str = "/tmp") -> float | None:
    """Percent of `path`'s filesystem inodes in use — `os.statvfs`, no `df` subprocess
    needed. None when the filesystem doesn't report inode counts at all (`f_files == 0`,
    e.g. some overlay/network filesystems) — a genuine "can't answer", never a false 0%."""
    try:
        st = os.statvfs(path)
    except OSError:
        return None
    if st.f_files == 0:
        return None
    return 100.0 * (st.f_files - st.f_ffree) / st.f_files


def _disk_free_pct(path: Path) -> float | None:
    """Percent of `path`'s filesystem free, by bytes (`shutil.disk_usage`) — None when
    `path` doesn't exist yet (a young vault is not a failure; `evaluate` already alarms
    separately on a missing/empty vault)."""
    import shutil

    if not path.exists():
        return None
    du = shutil.disk_usage(path)
    return 100.0 * du.free / du.total


def collect() -> dict:
    """Gather the survival matrix — thin collectors, all judgment lives in evaluate()."""
    m: dict = {"units": {}, "timers": {}, "containers": {}, "ports": [],
               "backup_age_h": None, "vault_age_d": None, "unpushed": None,
               "tmp_inode_pct": _tmp_inode_pct(),
               "disk_free_pct": _disk_free_pct(VAULT_DIR)}
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
    # `.dump` (osiris_backup.sh's own -Fc switch, the vault lane, ruling 39384a87 item 1)
    # is the CURRENT extension; `.sql` still matches so the retention window's last plain-
    # text dumps (kept ~7 days after the switch) don't cause a false "backup_age_h: None"
    # gap the moment this deploys.
    dumps = sorted([*BACKUP_DIR.glob("osiris-*.sql"), *BACKUP_DIR.glob("osiris-*.dump")],
                   key=lambda p: p.stat().st_mtime)
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

    pool = await asyncpg.create_pool(
        DSN, min_size=1, max_size=1,
        server_settings={"application_name": "osiris-script:preflight-miner"})
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


async def collect_schema_drift() -> str | None:
    """Belt-and-suspenders on top of the boot-time deploy guard (thread e6f5556f): the boot
    check only ever fires ONCE, at a service's own start — a process that booted clean and
    then drifted later (a migration landed on the DB, or got reverted, while the service kept
    running) would never re-check itself. This weekly pass reuses the SAME comparison
    (deploy_guard.check_schema_drift), not a duplicate of the logic, so the two never disagree
    on what counts as drift."""
    import asyncpg
    from src.orchestrator.deploy_guard import check_schema_drift

    pool = await asyncpg.create_pool(
        DSN, min_size=1, max_size=1,
        server_settings={"application_name": "osiris-script:preflight-schema-drift"})
    try:
        return await check_schema_drift(pool)
    finally:
        await pool.close()


def find_missing_sessions(disk_anchors: set[str], stored_anchors: set[str]) -> set[str]:
    """Pure set difference — on-disk sessions the store has no soul_sessions row for at
    all. Tested directly; the collector below is the thin disk+DB shell around it."""
    return disk_anchors - stored_anchors


async def collect_soul_store_coverage(root: Path | None = None) -> int | None:
    """THE SOUL STORE'S OWN COVERAGE GUARANTEE (thread 78efd46d, "let osiris eat every
    agent history": "a session on disk and not in the store is a preflight failure,
    never a quiet gap"). Every Claude Code transcript this box can see — the SAME
    ClaudeJsonlAdapter.enumerate() walk the backfill cron already trusts, so this check
    and that sweep can never disagree on what counts as a session — must have a
    soul_sessions row. Returns the count missing, or None when there's nothing to walk
    (no transcripts root configured/present here — not a failure, just nothing to check
    in this environment)."""
    from src.ingest.harness.claude_jsonl import ClaudeJsonlAdapter

    base = root or Path(os.environ.get("OSIRIS_TRANSCRIPTS")
                        or Path.home() / ".claude" / "projects")
    if not base.is_dir():
        return None
    disk = {loc.anchor_sid for loc in ClaudeJsonlAdapter().enumerate(root=base)}
    if not disk:
        return None
    pool = await asyncpg.create_pool(
        DSN, min_size=1, max_size=1,
        server_settings={"application_name": "osiris-script:preflight-soul-coverage"})
    try:
        rows = await pool.fetch(
            "SELECT anchor_sid FROM soul_sessions WHERE harness='claude-code'")
    finally:
        await pool.close()
    stored = {r["anchor_sid"] for r in rows}
    return len(find_missing_sessions(disk, stored))


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
    tmp_pct = m.get("tmp_inode_pct")
    if tmp_pct is not None and tmp_pct >= TMP_INODE_ALARM_PCT:
        fails.append(f"/tmp inode use at {tmp_pct:.1f}% (alarm at {TMP_INODE_ALARM_PCT:.0f}%) "
                     "— the fleet-wide ENOSPC incident's own early-warning (obligation "
                     "a867ae37): every Bash tool call across every live seat fails once "
                     "this reaches 100%, silently, until it does")
    disk_pct = m.get("disk_free_pct")
    if disk_pct is not None and disk_pct <= DISK_FREE_ALARM_PCT:
        fails.append(f"disk free at {disk_pct:.1f}% on the vault's filesystem (alarm at "
                     f"{DISK_FREE_ALARM_PCT:.0f}%) — the vault lane's own disk guard "
                     "(item 5): run scripts/osiris_prune_ladder.py for a dry-run of what "
                     "could be pruned, then act on the operator's word")
    missing = m.get("soul_store_missing")
    if missing:
        fails.append(f"SOUL STORE COVERAGE GAP: {missing} session(s) on disk have no "
                     "soul_sessions row (thread 78efd46d) — the store is not yet the "
                     "durable record it claims to be for these; investigate why "
                     "backfill_transcripts's cron hasn't caught them "
                     "(SoulStore(pool).ingest_path directly, per-session, is the fastest "
                     "way to see the real error instead of the sweep's own swallowed one)")
    # THE MINER IS SUMMONED, NOT SCHEDULED (ceae1604). It used to walk every transcript every ten
    # minutes, so a silent tick meant sensing was DOWN and this check was right to fail on it. The
    # crawl is gone: the adversary now runs ONCE, at a session's death rite, so a quiet hour means
    # nobody's session ended — not that anything is broken. Demanding a tick every 35 minutes from
    # a job that no longer ticks would fail this preflight FOREVER, on purpose, about nothing.
    #
    # We still fail on ERRORS, which are always real. We simply no longer mistake SILENCE for death
    # — the same distinction the wall now draws between "untouched" and "resolved", and the same
    # one the liveness fix drew between "quiet" and "dead". Absence of activity is not evidence of
    # failure; it is only evidence of absence.
    miner = m.get("miner")
    if miner and miner.get("recent_errors", 0) >= 3:
        fails.append(f"adversary errored {miner['recent_errors']} of the last "
                     f"{miner['recent']} runs — the death-rite sweep is failing")
    # THE DEPLOY-ORDERING GUARD'S WEEKLY BACKSTOP (thread e6f5556f): the boot-time check only
    # ever fires once, at start — this catches drift that happens AFTER a clean boot.
    drift = m.get("schema_drift")
    if drift:
        fails.append(f"SCHEMA DRIFT: {drift} — run `alembic upgrade head` against the real DB")
    return fails


def drill(newest_dump: str) -> str | None:
    """Restore the newest dump into a scratch container and count objects. Returns a failure
    string or None. Heavy (~2 min) — timer runs pass --drill; ad-hoc runs may skip."""
    name = "osiris-preflight-drill"
    try:
        # -v (not just -f): postgres:16 declares an anonymous VOLUME for its data dir —
        # `docker rm -f` alone drops the container but leaves that volume orphaned, links=0,
        # forever (Cupid's field report, network, msg 4938: 11GB across 4 weekly Mondays,
        # tracking the growing dump size, ~180GB/year on the operator's laptop unfixed).
        subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True, timeout=30)
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
        subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True, timeout=30)


def drill_pitr() -> str | None:
    """The vault lane's own last piece of item 3: a base backup plus WAL archiving is a
    hope, not a backup, until a real restore has proven a point in time — this reuses
    osiris_pitr_drill.py's own run_drill against the newest base backup in the vault
    and an auto-picked live marker (pick_and_ensure_marker — no operator-authored
    marker needed for an unattended weekly run). Quiet (None) when no base backup
    exists yet in this environment — item 3 not activated here is not a failure of
    this check; a real failed restore against an EXISTING base backup is."""
    from scripts.osiris_prune_ladder import _scan

    backups = _scan(VAULT_DIR / "basebackups")
    if not backups:
        return None
    from scripts.osiris_pitr_drill import pick_and_ensure_marker, run_drill

    newest = max(backups, key=lambda f: f.when)
    marker = pick_and_ensure_marker()
    if marker is None:
        return None
    return run_drill(Path(newest.path), None, marker)


async def brief_operator(fails: list[str]) -> None:
    """Regression → a brief on the desk through the normal mailbox (dedup makes re-runs safe)."""
    import asyncpg
    from src.orchestrator.mailbox import send_message

    pool = await asyncpg.create_pool(
        DSN, min_size=1, max_size=1,
        server_settings={"application_name": "osiris-script:preflight-brief"})
    try:
        body = ("PREFLIGHT REGRESSION — the survival matrix has holes:\n- "
                + "\n- ".join(fails)
                + "\nRun scripts/osiris_preflight.py after fixing; silence = green.")
        await send_message(pool, from_agent="system:preflight", from_project="osiris",
                           to_project="operator", body=body)
    finally:
        await pool.close()


# A check genuinely can't reach the DB — already reported as a unit/container failure, so
# degrading quietly to None here is correct, not a cover-up. Anything OUTSIDE this set (an
# import-time ModuleNotFoundError, a renamed function, any other programming defect) is the
# CHECK ITSELF broken, not the database — that must alarm, never degrade to a quiet None
# (thread 3e96c10e: this exact class of bug silently passed both weekly checks for a while).
_DB_UNREACHABLE = (OSError, TimeoutError, asyncpg.PostgresError)


def _run_check(name: str, coro: Coroutine[Any, Any, Any]) -> tuple[Any, str | None]:
    """(result, broken_msg). `broken_msg` is None on success OR a genuine DB-unreachable
    degrade; set only when the check itself failed to run at all."""
    try:
        return asyncio.run(coro), None
    except _DB_UNREACHABLE:
        return None, None
    except Exception as e:  # noqa: BLE001 — the alarm IS the handling; nothing swallowed
        return None, (f"{name} check is BROKEN ({type(e).__name__}: {e}) — it did NOT "
                      "actually run this pass")


def main() -> int:
    m = collect()
    m["miner"], miner_broken = _run_check("collect_miner", collect_miner())
    m["schema_drift"], drift_broken = _run_check("collect_schema_drift", collect_schema_drift())
    m["soul_store_missing"], soul_broken = _run_check(
        "collect_soul_store_coverage", collect_soul_store_coverage())
    fails = evaluate(m)
    fails.extend(b for b in (miner_broken, drift_broken, soul_broken) if b)
    if "--drill" in sys.argv and m.get("newest_dump"):
        d = drill(m["newest_dump"])
        if d:
            fails.append(d)
    if "--drill" in sys.argv:
        p = drill_pitr()
        if p:
            fails.append(p)
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
