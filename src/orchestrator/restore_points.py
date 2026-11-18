"""What can be restored, and from when: the numbers behind the console's backup line.

`refresh` (run on the home network, after a thinning run and about weekly) reads the NAS
repository's snapshots and every file name they hold, works out the stretches the database can be
rewound to any moment (a base backup with an unbroken run of WAL after it), the holes between them,
and the days with no database dump, and saves that small result. `status` is what the console and
`osiris backup-status` show: that saved result plus the live facts (last backup, next expected,
the last thinning run). It never touches the NAS, so it answers at once from anywhere.

A window is approximate: it starts when its base backup finished and ends at the last WAL segment of
the unbroken run, using the times the files were written into the vault."""
from __future__ import annotations

import json
import os
import re
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

CACHE_ENV = "OSIRIS_RESTORE_POINTS_FILE"
_DEFAULT_CACHE = "~/.local/state/osiris/restore_points.json"
OFFLOAD_EVERY = timedelta(minutes=15)
STALE_AFTER = timedelta(days=8)
CHUNK = 20                       # snapshots per `restic find` call (bounds memory)
# a base finishing this long before a run's first segment still belongs to that run
BASE_SLACK = timedelta(hours=2)
_WAL = re.compile(r"^([0-9A-F]{8})([0-9A-F]{8})([0-9A-F]{8})(\.zst)?$")
# the files beside the segments that a rewind also needs: timeline history, base backup labels and
# the partial segment of a switched timeline
_WAL_AUX = re.compile(r"^([0-9A-F]{8}\.history|[0-9A-F]{24}\.[0-9A-F]{8}\.backup"
                      r"|[0-9A-F]{24}(\.zst)?\.partial)$")
_BASE = re.compile(r"^osiris-basebackup-\d{8}-\d{6}\.tar\.gz$")
_DUMP = re.compile(r"^osiris-\d{8}-\d{6}\.(dump|sql)$")


def cache_path() -> Path:
    return Path(os.environ.get(CACHE_ENV) or _DEFAULT_CACHE).expanduser()


def _iso(ts: datetime) -> str:
    return ts.astimezone(UTC).isoformat()


def _when(raw: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(raw))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def classify(path: str) -> tuple[str, str] | None:
    """(kind, name) for a vault file that matters to restoring: wal (and wal_aux: history, label and
    partial files), base, verified or dump."""
    name = path.rsplit("/", 1)[-1]
    if "/wal_archive/" in path and _WAL.match(name):
        return "wal", name
    if "/wal_archive/" in path and _WAL_AUX.match(name):
        return "wal_aux", name
    if "/basebackups/" in path and _BASE.match(name):
        return "base", name
    if "/basebackups/" in path and name.endswith(".tar.gz.verified"):
        return "verified", name  # keyed by its own name: it must not replace the base's entry
    if "/" not in path.split("osiris-vault/", 1)[-1] and _DUMP.match(name):
        return "dump", name
    return None


def compute(files: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """The restore map from `files`: {name: {"kind", "mtime"(ISO)}}. Pure."""
    wal: list[tuple[int, int, datetime, str]] = []
    bases: list[tuple[datetime, str, bool]] = []
    dumps: list[datetime] = []
    verified = {n[: -len(".verified")] for n, f in files.items() if f["kind"] == "verified"}
    for name, f in files.items():
        when = _when(f["mtime"])
        if when is None:
            continue
        if f["kind"] == "wal":
            m = _WAL.match(name)
            assert m
            wal.append((int(m[1], 16), int(m[2], 16) * 256 + int(m[3], 16), when, name))
        elif f["kind"] == "base":
            bases.append((when, name, name in verified))
        elif f["kind"] == "dump":
            dumps.append(when)
    wal.sort()
    runs: list[dict[str, Any]] = []
    for tl, pos, when, _name in wal:
        last = runs[-1] if runs else None
        if last and last["tl"] == tl and pos == last["pos"] + 1:
            last["pos"], last["end"], last["segments"] = pos, when, last["segments"] + 1
        else:
            runs.append({"tl": tl, "first": pos, "pos": pos, "start": when, "end": when,
                         "segments": 1})
    windows, gaps = [], []
    for i, run in enumerate(runs):
        ok = sorted(b for b in bases if run["start"] - BASE_SLACK <= b[0] <= run["end"])
        # a verified base is preferred; any base is a (marked) start
        chosen = next((b for b in ok if b[2]), ok[0] if ok else None)
        if chosen:
            windows.append({"from": _iso(chosen[0]), "to": _iso(run["end"]),
                            "base": chosen[1], "base_verified": chosen[2],
                            "segments": run["segments"]})
        if i + 1 < len(runs):
            nxt = runs[i + 1]
            missing = (nxt["first"] - run["pos"] - 1) if nxt["tl"] == run["tl"] else None
            gaps.append({"from": _iso(run["end"]), "to": _iso(nxt["start"]),
                         "missing_segments": missing})
    days = sorted({d.astimezone().date() for d in dumps})
    no_dump: list[str] = []
    if days:
        cur = days[0]
        have = set(days)
        while cur <= days[-1]:
            if cur not in have:
                no_dump.append(cur.isoformat())
            cur += timedelta(days=1)
    starts = [_when(w["from"]) for w in windows]
    oldest_candidates = [t for t in [*starts, *dumps] if t]
    return {"windows": windows, "gaps": gaps, "days_without_a_dump": no_dump,
            "dumps": len(dumps), "base_backups": len(bases), "wal_segments": len(wal),
            "oldest_restorable": _iso(min(oldest_candidates)) if oldest_candidates else None}


def collect(run: Callable[..., Any], snapshots: list[dict[str, Any]], *,
            chunk: int = CHUNK) -> dict[str, dict[str, Any]]:
    """Every restore-relevant file name in the given snapshots, read a few snapshots at a time."""
    files: dict[str, dict[str, Any]] = {}
    ids = [str(s["id"]) for s in snapshots]
    for i in range(0, len(ids), chunk):
        args = ["find", "--json", "--no-lock"]
        for sid in ids[i:i + chunk]:
            args += ["--snapshot", sid]
        proc = run([*args, "*"], timeout=1800)
        if proc.returncode != 0:
            tail = (proc.stderr or "").strip().splitlines()[-1:] or [""]
            raise RuntimeError(f"restic find failed: {tail[0][:300]}")
        for snap in json.loads(proc.stdout or "[]"):
            for m in snap.get("matches") or []:
                if m.get("type") != "file":
                    continue
                got = classify(str(m.get("path")))
                if got:
                    kind, name = got
                    files[name] = {"kind": kind, "mtime": str(m.get("mtime"))}
    return files


def refresh(target: dict[str, Any], run: Callable[..., Any], *,
            repo_bytes: Callable[[dict[str, Any]], int | None], now: datetime | None = None,
            ) -> dict[str, Any]:
    """Read the repository and save the restore map. Raises RuntimeError with a plain reason."""
    proc = run(["snapshots", "--json", "--no-lock"], timeout=600)
    if proc.returncode != 0:
        raise RuntimeError("restic snapshots failed")
    snaps = json.loads(proc.stdout or "[]")
    result = compute(collect(run, snaps))
    times = sorted(str(s.get("time")) for s in snaps)
    result.update({
        "as_of": _iso(now or datetime.now(UTC)), "snapshots": len(snaps),
        "oldest_snapshot": times[0] if times else None,
        "newest_snapshot": times[-1] if times else None, "repo_bytes": repo_bytes(target)})
    path = cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    try:
        tmp.write_text(json.dumps(result))
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)
    return result


def read_cache() -> dict[str, Any] | None:
    try:
        body = json.loads(cache_path().read_text())
    except (OSError, ValueError):
        return None
    return body if isinstance(body, dict) and "windows" in body else None


def status(*, offload: dict[str, Any], thinning: dict[str, Any], ceiling_gb: int = 0,
           cache: dict[str, Any] | None = None, now: datetime | None = None,
           stale_days: int = 7) -> dict[str, Any]:
    """The console line: live facts from the offload record and the thinning state, the rest from
    the last saved restore map (with its age, and whether it is old). Pure given its inputs."""
    now = now or datetime.now(UTC)
    last = offload.get("last_successful_offload")
    attempt = _when(offload.get("last_attempt_at"))
    last_at = _when(offload.get("last_backup_end") or last)
    due = (last_at + timedelta(days=stale_days)) if (last_at and stale_days) else None
    out: dict[str, Any] = {
        # overdue: no good backup for `stale_days` (the same limit readiness uses; 0 = never flag)
        "overdue": bool(due and now > due),
        "overdue_since": _iso(due) if due and now > due else None,
        "last_backup": offload.get("last_backup_end") or last,
        "last_backup_started": offload.get("last_backup_start") or last,
        "next_expected": _iso(attempt + OFFLOAD_EVERY) if attempt else None,
        "next_expected_note": "every 15 minutes, only while this machine is on the home network",
        "last_error": offload.get("last_error"),
        "thinning": {"last_run": thinning.get("last_run"),
                     "approved": bool(thinning.get("approved")),
                     "plan": ({k: thinning["last_plan"].get(k) for k in (
                         "id", "at", "freed_bytes", "total")} | {
                         "to_forget": len(thinning["last_plan"].get("remove") or [])}
                         if thinning.get("last_plan") else None)},
        "size_ceiling_gb": ceiling_gb or None}
    if cache is None:
        out.update({"known": False, "text": "The restore map has not been built yet; it is built "
                    "on the home network (osiris repo-thin --refresh-points)."})
        return out
    as_of = _when(cache.get("as_of"))
    repo = cache.get("repo_bytes")
    out.update({
        "known": True, "as_of": cache.get("as_of"),
        "stale": bool(as_of and now - as_of > STALE_AFTER),
        "oldest_restorable": cache.get("oldest_restorable"),
        "windows": cache.get("windows"), "gaps": cache.get("gaps"),
        "days_without_a_dump": cache.get("days_without_a_dump"),
        "snapshots": cache.get("snapshots"), "repo_bytes": repo,
        "over_ceiling": bool(ceiling_gb and repo and repo > ceiling_gb * 10**9)})
    return out


async def live_status(pool: Any, *, now: datetime | None = None) -> dict[str, Any]:
    """`status` with its inputs read from the box: the NAS target's offload record, the thinning
    state, the size limit setting and the saved restore map. Never touches the NAS."""
    from src.orchestrator import repo_thinning
    from src.orchestrator.backup_settings import get_backup_settings
    from src.orchestrator.nas_first_offload import pick_target
    from src.orchestrator.offload_runner import offload_receipts
    from src.orchestrator.settings_service import settings_with_overlay

    target, _why = pick_target((await get_backup_settings(pool)).get("offload_targets") or [], None)
    try:
        overlay = await settings_with_overlay(pool)
        ceiling = int(overlay.osiris_thin_size_ceiling_gb or 0)
        stale_days = int(overlay.osiris_offload_stale_days)
    except Exception:  # noqa: BLE001 - the limits are display only
        ceiling, stale_days = 0, 7
    return status(offload=(offload_receipts().get(str((target or {}).get("name"))) or {}),
                  thinning=repo_thinning.read_state(), ceiling_gb=ceiling,
                  cache=read_cache(), now=now, stale_days=stale_days)
