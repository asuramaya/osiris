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
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from src.ingest.soul_store import RoundTripReport

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


def _format_round_trip_failure(failures: list[dict[str, str]] | None) -> str | None:
    """Pure message formatting for the soul store's own round-trip proof (thread
    78efd46d item 2) — shared by `evaluate()` (so a test can construct `m` directly,
    same convention as every other drill-shaped field here) and `main()`'s own
    --drill-gated call to `collect_soul_round_trip_sample` (which, like `drill_pitr`,
    runs AFTER `evaluate(m)`'s single pass, so `m` never actually carries this key at
    evaluate-time in the real flow — this function is what makes both call sites agree
    on the exact wording without a copy-pasted f-string). A skipped-live session is
    NEVER a failure — see `main()`'s own separate, unconditional "skipped live: N"
    print, per Thoth's ruling that the count must be reported, never folded silently
    into a clean pass, but also never treated as a defect worth an operator alarm."""
    if not failures:
        return None
    anchors = ", ".join(f["anchor_sid"] for f in failures[:5])
    return (f"SOUL STORE ROUND-TRIP FAILURE: {len(failures)} sampled session(s) do not "
            f"reconstruct byte-identical to disk (thread 78efd46d item 2) — {anchors} "
            "— a backup that's never been restored is a hope, not a backup")


async def collect_soul_store_coverage(
    root: Path | None = None, *, check_crush: bool = True,
) -> int | None:
    """THE SOUL STORE'S OWN COVERAGE GUARANTEE (thread 78efd46d, "let osiris eat every
    agent history": "a session on disk and not in the store is a preflight failure,
    never a quiet gap"). Every Claude Code transcript this box can see — the SAME
    ClaudeJsonlAdapter.enumerate() walk the backfill cron already trusts, so this check
    and that sweep can never disagree on what counts as a session — must have a
    soul_sessions row. Returns the count missing, or None when there's nothing to walk
    (no transcripts root configured/present here — not a failure, just nothing to check
    in this environment).

    EXTENDED TO CRUSH (wave 13 item 2, "crush sessions become canonical"): the SAME
    `CrushSqliteAdapter.enumerate()` walk `backfill_crush` already trusts, checked
    against `harness='crush'` soul_sessions rows — one combined missing-count so this
    function's contract (an int, or None when nothing to check) never has to change for
    every caller/test that already reads it that way. `root` only ever scopes the
    claude-code half — crush's own discovery walks the REAL `projects.json` + seat
    offices UNCONDITIONALLY (`CrushSqliteAdapter.enumerate` takes no root at all), so
    unlike the claude-code half this can never be sandboxed by a caller-supplied path.

    `check_crush=False` skips the crush half entirely (only a caller that genuinely
    wants to isolate the claude-code-only behavior — e.g. a test asserting "nothing to
    check" against an empty/fake root — should ever pass this; every real preflight
    run wants both, the default)."""
    from src.ingest.harness.claude_jsonl import ClaudeJsonlAdapter

    base = root or Path(os.environ.get("OSIRIS_TRANSCRIPTS")
                        or Path.home() / ".claude" / "projects")
    claude_disk = {loc.anchor_sid for loc in ClaudeJsonlAdapter().enumerate(root=base)} \
        if base.is_dir() else set()
    crush_disk: set[str] = set()
    if check_crush:
        from src.ingest.harness.crush_sqlite import CrushSqliteAdapter
        crush_disk = {loc.anchor_sid for loc in CrushSqliteAdapter().enumerate()}
    if not claude_disk and not crush_disk:
        return None
    pool = await asyncpg.create_pool(
        DSN, min_size=1, max_size=1,
        server_settings={"application_name": "osiris-script:preflight-soul-coverage"})
    try:
        claude_rows = await pool.fetch(
            "SELECT anchor_sid FROM soul_sessions WHERE harness='claude-code'")
        crush_rows = await pool.fetch(
            "SELECT anchor_sid FROM soul_sessions WHERE harness='crush'")
    finally:
        await pool.close()
    claude_stored = {r["anchor_sid"] for r in claude_rows}
    crush_stored = {r["anchor_sid"] for r in crush_rows}
    return (len(find_missing_sessions(claude_disk, claude_stored))
            + len(find_missing_sessions(crush_disk, crush_stored)))


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
    roundtrip_fail = _format_round_trip_failure(m.get("soul_round_trip_failures"))
    if roundtrip_fail:
        fails.append(roundtrip_fail)
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
    # THE MCP SERVER'S OWN LIVENESS ACROSS TIME (thread 007bfd6b, msg 9123 item 4): either
    # symptom alone is real damage — a NEW restart since last run, or a kill/OOM line in
    # the journal in that same window (systemd sometimes restarts silently faster than a
    # kill line lands, so both are checked rather than treating one as a subset of other).
    liveness = m.get("mcp_liveness")
    if liveness:
        delta = liveness["nrestarts_delta"]
        n_kills = len(liveness["kill_events"])
        if delta or n_kills:
            fails.append(
                f"MCP LIVENESS: {_format_mcp_liveness_line(liveness)} — "
                "osiris-mcp restarted or was killed since the last preflight run; "
                "journalctl --user -u osiris-mcp for the reason")
    return fails


_DRILL_CONTAINER_NAME = "osiris-preflight-drill"


def _restore_cmd(name: str, dump_path: str) -> list[str]:
    """-Fc CUSTOM FORMAT NEEDS pg_restore, NEVER psql (thread 9fac4e0d part 4's own
    live find: piping a .dump file into psql failed — pg_dump's own -Fc switch, the
    vault lane item 1, is a BINARY container format, not SQL text; only pg_restore
    reads it). `.sql` (the pre-item-1 legacy extension, collect()'s glob still
    tolerates it during the transition) is genuine SQL text and still needs psql —
    branch on the extension actually on disk rather than assuming one format forever."""
    if dump_path.endswith(".dump"):
        return ["docker", "exec", "-i", name, "pg_restore", "-U", "osiris",
                "-d", "osiris", "--no-owner"]
    return ["docker", "exec", "-i", name, "psql", "-U", "osiris", "-d", "osiris", "-q"]


def drill(newest_dump: str) -> str | None:
    """Restore the newest dump into a scratch container and count objects. Returns a failure
    string or None. Heavy (~2 min) — timer runs pass --drill; ad-hoc runs may skip.

    NEVER THE LIVE CLUSTER (thread 9fac4e0d part 4, codified after the exact live
    incident that named this obligation: a manual pg_basebackup restore into a
    DIFFERENTLY-NAMED database on the SAME live cluster still generated real WAL
    against production — a drill's own point is to generate zero WAL against the
    thing being drilled). `_DRILL_CONTAINER_NAME` is a hardcoded module constant, not
    a caller-supplied parameter, precisely so this can never drift toward the live
    container's own name by accident — the assertion below is the machine-checked
    version of that same guarantee, not just a naming convention trusted by eye."""
    name = _DRILL_CONTAINER_NAME
    assert name not in CONTAINERS, (  # noqa: S101 — a real safety assertion, not a debug aid
        f"the preflight drill's own scratch container name {name!r} must never "
        f"collide with a live-fleet container name {CONTAINERS!r}")
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
            subprocess.run(_restore_cmd(name, newest_dump), stdin=f,
                           capture_output=True, timeout=600, check=True)
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


async def collect_soul_round_trip_sample() -> RoundTripReport:
    """THE SOUL STORE'S OWN ROUND-TRIP PROOF (thread 78efd46d item 2): "a backup that's
    never been restored is a hope, not a backup" — the same law this file's own plain-
    dump `drill()` already holds, extended to the soul store. Reuses
    SoulStore.verify_round_trip_sample (a random sample, not a full sweep — see its own
    docstring for why a sample is the right cadence here) so this collector and the
    store's own acceptance test can never disagree on what "verified" means. Read-only
    (soul_lines/soul_sessions carry no jsonb columns) — a bare pool is correct here,
    unlike the mailbox writes thread 8542ee89 found broken."""
    from src.ingest.soul_store import SoulStore

    pool = await asyncpg.create_pool(
        DSN, min_size=1, max_size=1,
        server_settings={"application_name": "osiris-script:preflight-soul-roundtrip"})
    try:
        return await SoulStore(pool).verify_round_trip_sample()
    finally:
        await pool.close()


# THE BACKLOG BAND, piece 3 (thread 8608, operator nudge via Thoth): the weekly delta line
# needs a durable prior-week number to compare against — a bare `watermarks` cursor, the
# same generic key/value store digest.py's own OPERATOR_WATERMARK already uses.
_BACKLOG_WEEKLY_CURSOR_KEY = "preflight:backlog_weekly_fleet_total"


def _backlog_delta(fleet_total: int, prior: str | None) -> int | None:
    """Pure: `None` on the very first run (no prior cursor to compare against), never
    coerced to 0 — a real "no change" and "nothing to compare yet" are different facts."""
    return fleet_total - int(prior) if prior is not None else None


def _format_backlog_weekly_line(m: dict[str, Any]) -> str:
    """Pure message formatting for the backlog band's weekly line — shared by `main()`'s
    own unconditional print and `brief_backlog_weekly`'s desk post, same convention
    `_format_round_trip_failure` already set for this file's own DB-touching collectors."""
    delta = m["delta"]
    delta_text = ("first run, no prior week to compare" if delta is None else
                  f"{'+' if delta >= 0 else ''}{delta} since last week")
    seats = ", ".join(f"{s['seat']}:{s['open']}" for s in m["top_seats"]) or "none"
    return (f"BACKLOG BAND — {m['fleet_total']} open obligation(s) fleet-wide "
            f"({delta_text}). Top seats: {seats}.")


async def collect_obligation_backlog_weekly() -> dict[str, Any]:
    """THE BACKLOG BAND, piece 3: fleet total open obligations, the top five carrying
    seats, and the delta since the last time this ran — a bare snapshot tells nobody
    whether the crunch is working, the delta does. Reuses
    `compositions._fn_obligation_backlog` (piece 1) rather than a fourth hand-rolled
    query. Read-mostly: the only write is advancing this collector's own cursor, same
    posture `fleet_digest`'s watermark advance already holds itself to."""
    from src.db.pool import create_pool
    from src.orchestrator.compositions import _fn_obligation_backlog
    from src.orchestrator.monitor import get_cursor, set_cursor

    pool = await create_pool(
        DSN, min_size=1, max_size=1,
        application_name="osiris-script:preflight-backlog-weekly")
    try:
        result = await _fn_obligation_backlog(pool, None, {})
        fleet_total = int(result["fleet_total"])
        prior = await get_cursor(pool, _BACKLOG_WEEKLY_CURSOR_KEY)
        delta = _backlog_delta(fleet_total, prior)
        await set_cursor(pool, _BACKLOG_WEEKLY_CURSOR_KEY, str(fleet_total))
        return {"fleet_total": fleet_total, "top_seats": result["by_seat"][:5], "delta": delta}
    finally:
        await pool.close()


async def brief_backlog_weekly(m: dict[str, Any]) -> None:
    """Post the backlog band's weekly line to the operator's desk — informational
    (`desk_kind='fyi'`), never a regression alarm: this runs every week regardless of
    whether the number moved, same cadence `collect_soul_round_trip_sample` already
    runs at (--drill-gated, which the systemd timer passes only on its weekly pass)."""
    from src.db.pool import create_pool
    from src.orchestrator.mailbox import send_message

    pool = await create_pool(
        DSN, min_size=1, max_size=1,
        application_name="osiris-script:preflight-backlog-brief")
    try:
        await send_message(pool, from_agent="system:preflight", from_project="osiris",
                           to_project="operator", body=_format_backlog_weekly_line(m),
                           desk_kind="fyi", grade="fyi")
    finally:
        await pool.close()


# THE ORPHAN LAWS, item 3 (operator's word, wave 15, Thoth DM 8841): the same weekly-
# cursor shape the backlog band above already established, one lever pulled a second
# time rather than a fourth hand-rolled watermark.
_ORPHAN_WEEKLY_CURSOR_KEY = "preflight:orphan_weekly_total"


def _orphan_delta(total: int, prior: str | None) -> int | None:
    """Pure, same law as `_backlog_delta`: `None` on the very first run, never coerced
    to 0 — "nothing to compare yet" and "no change" are different facts."""
    return total - int(prior) if prior is not None else None


def _format_orphan_weekly_line(m: dict[str, Any]) -> str:
    """Pure message formatting for the orphan band's weekly line — shared by `main()`'s
    own unconditional print and `brief_orphan_weekly`'s desk post, same convention the
    backlog band's own `_format_backlog_weekly_line` already set."""
    delta = m["delta"]
    delta_text = ("first run, no prior week to compare" if delta is None else
                  f"{'+' if delta >= 0 else ''}{delta} since last week")
    top_types = ", ".join(f"{t}:{c['count']}" for t, c in
                          sorted(m["by_type"].items(), key=lambda kv: -kv[1]["count"])[:5]
                          ) or "none"
    return (f"ORPHAN BAND — {m['total']} disconnected object(s) fleet-wide "
            f"({m['abstained_total']} already abstained, {delta_text}). "
            f"Top types: {top_types}.")


async def collect_orphan_weekly() -> dict[str, Any]:
    """THE ORPHAN BAND, item 3: fleet total disconnected-object count, the by-type
    breakdown, and the delta since the last time this ran — reuses
    `compositions.orphan_census` (item 1) rather than a second hand-rolled query, the
    same "one derivation, not two" law `collect_obligation_backlog_weekly` already
    holds itself to. Read-mostly: the only write is advancing this collector's own
    cursor."""
    from src.db.pool import create_pool
    from src.orchestrator.compositions import orphan_census
    from src.orchestrator.monitor import get_cursor, set_cursor

    pool = await create_pool(
        DSN, min_size=1, max_size=1,
        application_name="osiris-script:preflight-orphan-weekly")
    try:
        result = await orphan_census(pool)
        total = int(result["total"])
        prior = await get_cursor(pool, _ORPHAN_WEEKLY_CURSOR_KEY)
        delta = _orphan_delta(total, prior)
        await set_cursor(pool, _ORPHAN_WEEKLY_CURSOR_KEY, str(total))
        return {"total": total, "by_type": result["by_type"],
               "abstained_total": int(result["abstained_total"]), "delta": delta}
    finally:
        await pool.close()


async def brief_orphan_weekly(m: dict[str, Any]) -> None:
    """Post the orphan band's weekly line to the operator's desk — informational
    (`desk_kind='fyi'`), never a regression alarm, same cadence the backlog band's own
    `brief_backlog_weekly` already runs at."""
    from src.db.pool import create_pool
    from src.orchestrator.mailbox import send_message

    pool = await create_pool(
        DSN, min_size=1, max_size=1,
        application_name="osiris-script:preflight-orphan-brief")
    try:
        await send_message(pool, from_agent="system:preflight", from_project="osiris",
                           to_project="operator", body=_format_orphan_weekly_line(m),
                           desk_kind="fyi", grade="fyi")
    finally:
        await pool.close()


# THE TRACEABILITY BAND, beside the orphan band (Graph-Engineering, operator decision
# f47d14a7, thread 7f547426, item 3/3): the SAME weekly-cursor shape as the orphan band
# just above, reusing `compositions.traceability_census` rather than a second hand-rolled
# query.
_TRACEABILITY_WEEKLY_CURSOR_KEY = "preflight:traceability_weekly_total"


def _traceability_delta(total: int, prior: str | None) -> int | None:
    """Pure, same law as `_orphan_delta`/`_backlog_delta`: `None` on the very first run,
    never coerced to 0 — "nothing to compare yet" and "no change" are different facts."""
    return total - int(prior) if prior is not None else None


def _format_traceability_weekly_line(m: dict[str, Any]) -> str:
    """Pure message formatting for the traceability band's weekly line — shared by
    `main()`'s own unconditional print and `brief_traceability_weekly`'s desk post, same
    convention `_format_orphan_weekly_line` already set."""
    delta = m["delta"]
    delta_text = ("first run, no prior week to compare" if delta is None else
                  f"{'+' if delta >= 0 else ''}{delta} since last week")
    top_types = ", ".join(f"{t}:{c['count']}" for t, c in
                          sorted(m["by_type"].items(), key=lambda kv: -kv[1]["count"])[:5]
                          ) or "none"
    return (f"TRACEABILITY BAND — {m['total']} output(s) fleet-wide missing at least one "
            f"of run/plan/source/evaluator after confession ({delta_text}). "
            f"Top types: {top_types}.")


async def collect_traceability_weekly() -> dict[str, Any]:
    """THE TRACEABILITY BAND: fleet total untraceable-output count, the by-type
    breakdown, and the delta since the last time this ran — reuses
    `compositions.traceability_census` (item 3) rather than a second hand-rolled query,
    the same "one derivation, not two" law `collect_orphan_weekly` already holds itself
    to. Read-mostly: the only write is advancing this collector's own cursor."""
    from src.db.pool import create_pool
    from src.orchestrator.compositions import traceability_census
    from src.orchestrator.monitor import get_cursor, set_cursor

    pool = await create_pool(
        DSN, min_size=1, max_size=1,
        application_name="osiris-script:preflight-traceability-weekly")
    try:
        result = await traceability_census(pool)
        total = int(result["total"])
        prior = await get_cursor(pool, _TRACEABILITY_WEEKLY_CURSOR_KEY)
        delta = _traceability_delta(total, prior)
        await set_cursor(pool, _TRACEABILITY_WEEKLY_CURSOR_KEY, str(total))
        return {"total": total, "by_type": result["by_type"], "delta": delta}
    finally:
        await pool.close()


async def brief_traceability_weekly(m: dict[str, Any]) -> None:
    """Post the traceability band's weekly line to the operator's desk — informational
    (`desk_kind='fyi'`), never a regression alarm, same cadence `brief_orphan_weekly`
    already runs at."""
    from src.db.pool import create_pool
    from src.orchestrator.mailbox import send_message

    pool = await create_pool(
        DSN, min_size=1, max_size=1,
        application_name="osiris-script:preflight-traceability-brief")
    try:
        await send_message(pool, from_agent="system:preflight", from_project="osiris",
                           to_project="operator", body=_format_traceability_weekly_line(m),
                           desk_kind="fyi", grade="fyi")
    finally:
        await pool.close()


# THE WEEKLY ABSTENTION DIGEST, beside the orphan band (Thoth mail 8960 item 2, msg
# 9071): the SAME weekly-cursor shape the backlog and orphan bands above already
# established, a third lever pulled the same way rather than a fourth hand-rolled
# watermark. Reads `adoption_meter._hatch_counts` — the `unlinked_because`/
# `unlinked_because_kind` hatch every declare-or-refuse door writes (record_decision,
# open_thread, ingest_reference/ingest_log/ingest_reference_doc, record_practice, and
# now the provenance sweep's own Agent/SoftwareProject/Seat lanes) — a DIFFERENT
# population from the orphan band's own zero-live-link census just above: a hatch
# confession is a DECLARED gap (the door refused to mint silently and the caller said
# why), while an orphan is discovered post-hoc with no declaration at all. Distinct
# numbers, worth a distinct line, not folded into the orphan band's own total.
_ABSTENTION_WEEKLY_CURSOR_KEY = "preflight:abstention_weekly_total"


def _abstention_delta(total: int, prior: str | None) -> int | None:
    """Pure, same law as `_orphan_delta`/`_backlog_delta`: `None` on the very first run,
    never coerced to 0 — "nothing to compare yet" and "no change" are different facts."""
    return total - int(prior) if prior is not None else None


def _format_abstention_weekly_line(m: dict[str, Any]) -> str:
    """Pure message formatting for the abstention digest's weekly line — shared by
    `main()`'s own unconditional print and `brief_abstention_weekly`'s desk post, same
    convention the orphan/backlog bands' own formatters already set."""
    delta = m["delta"]
    delta_text = ("first run, no prior week to compare" if delta is None else
                  f"{'+' if delta >= 0 else ''}{delta} since last week")
    split = m["split"]
    split_text = (f"extension={split['extension_link_pending']} "
                  f"standalone={split['standalone_other']}" if split is not None else
                  "unsplit — reason constant not on this build")
    return (f"ABSTENTION DIGEST — {m['total']} declared hatch confession(s) fleet-wide, "
            f"all-time cumulative ({delta_text}). {split_text}.")


async def collect_abstention_weekly() -> dict[str, Any]:
    """THE WEEKLY ABSTENTION DIGEST: fleet total hatch confessions, the extension/
    standalone split, and the delta since the last time this ran — reuses
    `adoption_meter._hatch_counts` rather than a second hand-rolled query, the same
    "one derivation, not two" law `collect_orphan_weekly`/`collect_obligation_backlog_
    weekly` already hold themselves to. Read-mostly: the only write is advancing this
    collector's own cursor.

    THE DELTA IS STILL A CUMULATIVE-TOTAL DIFFERENCE, NOT A WEEKLY RATE (same caveat
    `_hatch_counts`'s own docstring names for its raw total): `unlinked_because` is
    asserted once per object and never retracted, so a positive delta here means "this
    many MORE confessions exist than a week ago," not "this many were written this
    week" — the two agree only if nothing was ever resolved out from under the count in
    between, which this digest does not attempt to detect."""
    from src.db.pool import create_pool
    from src.orchestrator.adoption_meter import _hatch_counts
    from src.orchestrator.monitor import get_cursor, set_cursor

    pool = await create_pool(
        DSN, min_size=1, max_size=1,
        application_name="osiris-script:preflight-abstention-weekly")
    try:
        result = await _hatch_counts(pool)
        total = int(result["total"])
        prior = await get_cursor(pool, _ABSTENTION_WEEKLY_CURSOR_KEY)
        delta = _abstention_delta(total, prior)
        await set_cursor(pool, _ABSTENTION_WEEKLY_CURSOR_KEY, str(total))
        return {"total": total, "split": result["split"], "delta": delta}
    finally:
        await pool.close()


async def brief_abstention_weekly(m: dict[str, Any]) -> None:
    """Post the abstention digest's weekly line to the operator's desk — informational
    (`desk_kind='fyi'`), never a regression alarm, same cadence the orphan/backlog
    bands' own briefs already run at."""
    from src.db.pool import create_pool
    from src.orchestrator.mailbox import send_message

    pool = await create_pool(
        DSN, min_size=1, max_size=1,
        application_name="osiris-script:preflight-abstention-brief")
    try:
        await send_message(pool, from_agent="system:preflight", from_project="osiris",
                           to_project="operator", body=_format_abstention_weekly_line(m),
                           desk_kind="fyi", grade="fyi")
    finally:
        await pool.close()


# THE MCP SERVER'S OWN LIVENESS ACROSS TIME (thread 007bfd6b, Thoth dispatch msg 9123
# item 4): "smoke passes in the gaps between OOM kills, and NRestarts read 5 for hours
# with no surface reading it — a presence check mistaken for a health check." A RAW
# NRestarts read is a point-in-time counter systemd only ever resets on its own terms
# (a daemon-reload or `systemctl reset-failed`), never on a schedule this script
# controls — so this tracks the DELTA since the LAST preflight run (a per-run cursor,
# unlike the weekly bands above), the same shape the earlier dated plan named
# ("NRestarts-delta-over-time rather than a point read"). The journal's own killed/OOM
# lines are read the same way, since-last-run, never the unit's whole lifetime (a
# restart from three weeks ago must not alarm forever). Runs on EVERY invocation, not
# --drill-gated — an alarm-driving check, not a weekly digest; the weekly line under
# --drill reuses this SAME per-run read rather than a second cumulative-total tracker,
# since NRestarts is already its own natural delta.
_MCP_LIVENESS_NRESTARTS_CURSOR_KEY = "preflight:mcp_liveness_nrestarts"
_MCP_LIVENESS_JOURNAL_CURSOR_KEY = "preflight:mcp_liveness_journal_since"
_MCP_LIVENESS_UNIT = "osiris-mcp"
_MCP_KILL_PATTERN = "killed|Out of memory|oom-kill|segfault"


def _mcp_nrestarts() -> int | None:
    """Current NRestarts off systemd's own `show -p` — None when the unit or the
    property can't be read (a genuine can't-answer, never a false 0)."""
    out = _run(["systemctl", "--user", "show", _MCP_LIVENESS_UNIT, "-p", "NRestarts"])
    if not out.startswith("NRestarts="):
        return None
    try:
        return int(out.split("=", 1)[1])
    except ValueError:
        return None


def _mcp_journal_kill_events(since_utc: str | None) -> list[str]:
    """Journal lines since `since_utc` (None = the unit's whole retained history, the
    first-ever run) naming a kill/OOM/segfault — `--utc` on both the read and the
    stored cursor so the comparison never drifts across a host timezone change.

    journalctl inserts its OWN "-- Boot <id> --" boundary markers between boots in the
    output regardless of the `-g` grep filter (confirmed live, 10 lines back for 3 real
    kill lines) — these are journalctl's own formatting, never a real log line, and are
    filtered out here so a boot boundary alone can never be counted as a kill event."""
    cmd = ["journalctl", "--user", "-u", _MCP_LIVENESS_UNIT, "--utc",
           "-g", _MCP_KILL_PATTERN]
    if since_utc:
        cmd += ["--since", since_utc]
    out = _run(cmd)
    return [ln for ln in out.splitlines() if ln.strip() and not ln.startswith("-- ")]


def _format_mcp_liveness_line(m: dict[str, Any]) -> str:
    """Pure message formatting — shared by `main()`'s own unconditional weekly print and
    `evaluate()`'s alarm text, same convention the weekly bands above already set."""
    delta = m["nrestarts_delta"]
    delta_text = ("first run, no prior read to compare" if delta is None
                  else f"+{delta} since last run" if delta > 0 else "no change")
    n_kills = len(m["kill_events"])
    return (f"MCP LIVENESS — NRestarts {delta_text}, {n_kills} kill/OOM journal "
            f"line(s) since the last run")


async def collect_mcp_liveness() -> dict[str, Any]:
    """NRestarts-delta and journal kill/OOM events since the last preflight run —
    see the section comment above for why this is a per-run cursor, not a weekly one."""
    from datetime import UTC, datetime

    from src.db.pool import create_pool
    from src.orchestrator.monitor import get_cursor, set_cursor

    pool = await create_pool(
        DSN, min_size=1, max_size=1,
        application_name="osiris-script:preflight-mcp-liveness")
    try:
        nrestarts = _mcp_nrestarts()
        prior_nrestarts = await get_cursor(pool, _MCP_LIVENESS_NRESTARTS_CURSOR_KEY)
        delta = (nrestarts - int(prior_nrestarts)) if (
            nrestarts is not None and prior_nrestarts is not None) else None
        if nrestarts is not None:
            await set_cursor(pool, _MCP_LIVENESS_NRESTARTS_CURSOR_KEY, str(nrestarts))

        since = await get_cursor(pool, _MCP_LIVENESS_JOURNAL_CURSOR_KEY)
        kill_events = _mcp_journal_kill_events(since)
        now_utc = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
        await set_cursor(pool, _MCP_LIVENESS_JOURNAL_CURSOR_KEY, now_utc)
        return {"nrestarts": nrestarts, "nrestarts_delta": delta, "kill_events": kill_events}
    finally:
        await pool.close()


async def brief_mcp_liveness_weekly(m: dict[str, Any]) -> None:
    """Post the MCP liveness line to the operator's desk — informational (`desk_kind=
    'fyi'`), same weekly cadence the backlog/orphan/abstention bands already run at, even
    when clean: a standing 'still watching, nothing found' is worth more here than
    silence, since this check's whole point is that nobody was reading NRestarts before."""
    from src.db.pool import create_pool
    from src.orchestrator.mailbox import send_message

    pool = await create_pool(
        DSN, min_size=1, max_size=1, application_name="osiris-script:preflight-mcp-liveness-brief")
    try:
        await send_message(pool, from_agent="system:preflight", from_project="osiris",
                           to_project="operator", body=_format_mcp_liveness_line(m),
                           desk_kind="fyi", grade="fyi")
    finally:
        await pool.close()


async def brief_operator(fails: list[str]) -> None:
    """Regression → a brief on the desk through the normal mailbox (dedup makes re-runs safe).

    Uses `src.db.pool.create_pool`, NOT bare `asyncpg.create_pool` (thread 8542ee89): the
    former registers the jsonb codec (`json.dumps`/`json.loads`) every graph write through
    `Actions.assert_property` depends on — without it, `ensure_type`'s own `kind="object"`
    property assertion (the FIRST jsonb write `create_or_find_object` makes, upstream of
    the message's own summary/grade/status) hits Postgres as the raw unquoted text
    `object`, which fails as invalid JSON before ever reaching the actual message content.
    A bare pool worked for the plain relational INSERT into `fleet_messages` and only broke
    the graph-edge half — the exact "relational row already committed, graph edge write
    failed" split this house's own send_message already confesses rather than swallows."""
    from src.db.pool import create_pool
    from src.orchestrator.mailbox import send_message

    pool = await create_pool(
        DSN, min_size=1, max_size=1, application_name="osiris-script:preflight-brief")
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
    m["mcp_liveness"], mcp_liveness_broken = _run_check(
        "collect_mcp_liveness", collect_mcp_liveness())
    fails = evaluate(m)
    fails.extend(b for b in (miner_broken, drift_broken, soul_broken, mcp_liveness_broken) if b)
    if "--drill" in sys.argv and m.get("newest_dump"):
        d = drill(m["newest_dump"])
        if d:
            fails.append(d)
    if "--drill" in sys.argv:
        p = drill_pitr()
        if p:
            fails.append(p)
    if "--drill" in sys.argv:
        report, roundtrip_broken = _run_check(
            "collect_soul_round_trip_sample", collect_soul_round_trip_sample())
        if report is not None:
            # ALWAYS PRINTED, pass or fail — Thoth's own ruling off the 2026-09-08
            # full sweep: a skipped-live session must be reported as "skipped live:
            # N", never silently folded into a clean pass.
            print(f"soul store round-trip: {len(report.failures)} failure(s), "
                  f"skipped live: {report.skipped_live}")
            m["soul_round_trip_failures"] = report.failures
            f = _format_round_trip_failure(report.failures)
            if f:
                fails.append(f)
        if roundtrip_broken:
            fails.append(roundtrip_broken)
    if "--drill" in sys.argv:
        backlog_weekly, backlog_broken = _run_check(
            "collect_obligation_backlog_weekly", collect_obligation_backlog_weekly())
        if backlog_weekly is not None:
            print(_format_backlog_weekly_line(backlog_weekly))
            try:
                asyncio.run(brief_backlog_weekly(backlog_weekly))
            except Exception as e:  # noqa: BLE001 — the desk being down is itself printed
                print(f"(could not post the backlog band brief: {e})")
        if backlog_broken:
            fails.append(backlog_broken)
    if "--drill" in sys.argv:
        orphan_weekly, orphan_broken = _run_check(
            "collect_orphan_weekly", collect_orphan_weekly())
        if orphan_weekly is not None:
            print(_format_orphan_weekly_line(orphan_weekly))
            try:
                asyncio.run(brief_orphan_weekly(orphan_weekly))
            except Exception as e:  # noqa: BLE001 — the desk being down is itself printed
                print(f"(could not post the orphan band brief: {e})")
        if orphan_broken:
            fails.append(orphan_broken)
    if "--drill" in sys.argv:
        traceability_weekly, traceability_broken = _run_check(
            "collect_traceability_weekly", collect_traceability_weekly())
        if traceability_weekly is not None:
            print(_format_traceability_weekly_line(traceability_weekly))
            try:
                asyncio.run(brief_traceability_weekly(traceability_weekly))
            except Exception as e:  # noqa: BLE001 — the desk being down is itself printed
                print(f"(could not post the traceability band brief: {e})")
        if traceability_broken:
            fails.append(traceability_broken)
    if "--drill" in sys.argv:
        abstention_weekly, abstention_broken = _run_check(
            "collect_abstention_weekly", collect_abstention_weekly())
        if abstention_weekly is not None:
            print(_format_abstention_weekly_line(abstention_weekly))
            try:
                asyncio.run(brief_abstention_weekly(abstention_weekly))
            except Exception as e:  # noqa: BLE001 — the desk being down is itself printed
                print(f"(could not post the abstention digest brief: {e})")
        if abstention_broken:
            fails.append(abstention_broken)
    if "--drill" in sys.argv and m.get("mcp_liveness") is not None:
        # reuses THIS run's own already-collected read (m["mcp_liveness"], above) rather
        # than a second cumulative-total tracker — NRestarts-delta is already a per-run
        # count, unlike the lifetime-totals the backlog/orphan/abstention bands track.
        print(_format_mcp_liveness_line(m["mcp_liveness"]))
        try:
            asyncio.run(brief_mcp_liveness_weekly(m["mcp_liveness"]))
        except Exception as e:  # noqa: BLE001 — the desk being down is itself printed
            print(f"(could not post the MCP liveness brief: {e})")
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
