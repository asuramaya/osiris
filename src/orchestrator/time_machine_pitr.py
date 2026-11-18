"""TIME MACHINE: a point in time, from the base backup and the archived WAL in the local vault.

A window is a stretch the database can be rewound to any moment inside: a base backup with an
unbroken run of WAL after it (the same map `restore_points` builds for the NAS repository, here
computed over the files that are on this machine). Pick a time inside a window and this unpacks
the base backup into the throwaway's volume, adds the archive recovery settings (recovery
target time, promote when it is reached), and lets Postgres replay the WAL up to that time.

A time that falls in a gap, before the oldest window or after the newest, is refused with a
sentence that names the nearest restorable times. A window that exists only on the NAS (the
older ones, once the local copies are thinned) is named as such and refused: opening those means
pulling them over the home network, which is a separate step.

The throwaway never archives, never connects to the live database, and reads the WAL directory
read-only."""
from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.orchestrator import restore_points
from src.orchestrator import time_machine as tm
from src.orchestrator.instants import parse_time

# "t|<time>" while replaying, "f|<time>" once open. (A boolean joined to text reads "true" or
# "false", so the flag is spelled out here; the wait below accepts either spelling.)
RECOVERY_STATE_SQL = ("SELECT CASE WHEN pg_is_in_recovery() THEN 't' ELSE 'f' END || '|' || "
                      "COALESCE(pg_last_xact_replay_timestamp()::text, '')")
RECOVERY_WAIT_SECS = 3600
RECOVERY_POLL_SECS = 5
EXTRACT_TIMEOUT_SECS = 1800
# A recovery server refuses to start with fewer connections than the server the base backup was
# taken from allowed, so it is set well above it.
RECOVERY_MAX_CONNECTIONS = 150
HELPER_MEMORY = "2g"


def _vault(vault_dir: Path | None) -> Path:
    return vault_dir or tm.VAULT_DIR


def local_files(vault_dir: Path | None = None) -> dict[str, dict[str, Any]]:
    """The restore-relevant files on THIS machine, in the shape `restore_points.compute` reads."""
    vault = _vault(vault_dir)
    files: dict[str, dict[str, Any]] = {}
    for sub in ("basebackups", "wal_archive"):
        d = vault / sub
        if not d.is_dir():
            continue
        for p in d.iterdir():
            got = restore_points.classify(str(p))
            if got is None or not p.is_file():
                continue
            kind, name = got
            mtime = datetime.fromtimestamp(p.stat().st_mtime, UTC).isoformat()
            files[name] = {"kind": kind, "mtime": mtime}
    return files


def local_windows(vault_dir: Path | None = None) -> dict[str, Any]:
    """{'windows': [...], 'gaps': [...]} computed over what is on this machine right now."""
    return restore_points.compute(local_files(vault_dir))


def _fmt(raw: str) -> str:
    when = parse_time(raw)
    return when.strftime("%Y-%m-%d %H:%M UTC") if when else raw


NAS_BASE_ESTIMATE = 5 * 2**30     # what the disk check assumes for a base backup not yet pulled


def pitr_moments(vault_dir: Path | None = None, cache: dict[str, Any] | None = None, *,
                 presence: Callable[[dict[str, Any]], dict[str, Any]] | None = None
                 ) -> list[dict[str, Any]]:
    """Point-in-time moments for the console list: one per window. Local windows can always be
    opened; a window only the NAS still has can be opened while on the home network (else it is
    listed with the reason it cannot be)."""
    from src.orchestrator import time_machine_restic as restic

    vault = _vault(vault_dir)
    out: list[dict[str, Any]] = []
    local_bases = set()
    for w in local_windows(vault).get("windows") or []:
        base = vault / "basebackups" / w["base"]
        if not base.is_file():
            continue
        local_bases.add(w["base"])
        out.append({
            "id": "pitr-" + w["base"].removesuffix(".tar.gz"), "kind": "pitr",
            "label": f"any time {_fmt(w['from'])} to {_fmt(w['to'])}",
            "when": w["to"], "size_bytes": base.stat().st_size, "available": True,
            "pick_time": True, "window_from": w["from"], "window_to": w["to"],
            "base": w["base"]})
    nas_windows = [w for w in ((cache if cache is not None else restore_points.read_cache())
                               or {}).get("windows") or [] if w.get("base") not in local_bases]
    why = restic.nas_reason(presence) if nas_windows else None
    for w in nas_windows:
        item = {
            "id": "pitr-" + str(w["base"]).removesuffix(".tar.gz"), "kind": "pitr",
            "label": f"any time {_fmt(w['from'])} to {_fmt(w['to'])} (NAS)",
            "when": w["to"], "size_bytes": NAS_BASE_ESTIMATE, "available": why is None,
            "pick_time": True, "window_from": w["from"], "window_to": w["to"],
            "base": w["base"]}
        if why:
            item["reason"] = why
        out.append(item)
    out.sort(key=lambda m: str(m["when"]), reverse=True)
    return out


def _spans(windows: list[dict[str, Any]]) -> list[tuple[datetime, datetime]]:
    out: list[tuple[datetime, datetime]] = []
    for w in windows:
        a, b = parse_time(w.get("from")), parse_time(w.get("to"))
        if a and b:
            out.append((a, b))
    return out


def target_problem(at: datetime, local: dict[str, Any],
                   cache: dict[str, Any] | None) -> str | None:
    """None when `at` is inside a window (local, or one only the NAS holds); otherwise a
    sentence saying why not and what the nearest restorable times are."""
    lw = _spans(local.get("windows") or [])
    if any(a <= at <= b for a, b in lw):
        return None
    cw = _spans((cache or {}).get("windows") or [])
    if any(a <= at <= b for a, b in cw):
        return None
    spans = sorted({*lw, *cw})
    if not spans:
        return "There is no point-in-time history on this machine to open."
    before = [b for _a, b in spans if b < at]
    after = [a for a, _b in spans if a > at]
    prev, nxt = (max(before) if before else None), (min(after) if after else None)
    if prev and nxt:
        return (f"{_fmt(at.isoformat())} falls in a gap, between {_fmt(prev.isoformat())} and "
                f"{_fmt(nxt.isoformat())}, when no continuous history was kept. The nearest "
                f"restorable times are {_fmt(prev.isoformat())} and {_fmt(nxt.isoformat())}.")
    if prev:
        return (f"{_fmt(at.isoformat())} is after the newest history. The latest restorable "
                f"time is {_fmt(prev.isoformat())}.")
    assert nxt is not None
    return (f"{_fmt(at.isoformat())} is before the oldest history. The earliest restorable "
            f"time is {_fmt(nxt.isoformat())}.")


def find_pitr_moment(moment_id: str, at: str | None,
                     vault_dir: Path | None = None) -> tm.Moment:
    """The moment for a window id and a picked time, or a refusal that says why."""
    vault = _vault(vault_dir)
    base_name = moment_id.removeprefix("pitr-") + ".tar.gz"
    local = local_windows(vault)
    cache = restore_points.read_cache()
    match = next((w for w in local.get("windows") or [] if w["base"] == base_name), None)
    if not at:
        raise tm.TimeMachineError("Pick the time to open inside that window.")
    when = parse_time(at)
    if when is None:
        raise tm.TimeMachineError(f"{at!r} is not a time. Use a date and time such as "
                                  "2026-11-17T09:30:00Z.")
    problem = target_problem(when, local, cache)
    if problem:
        raise tm.TimeMachineError(problem)
    if match is None:
        nas = next((w for w in (cache or {}).get("windows") or []
                    if w.get("base") == base_name), None)
        if nas is None:
            raise tm.TimeMachineError(f"no such moment: {moment_id!r}")
        na, nb = parse_time(nas["from"]), parse_time(nas["to"])
        if not (na and nb and na <= when <= nb):
            raise tm.TimeMachineError(
                f"{_fmt(when.isoformat())} is outside that window ({_fmt(nas['from'])} to "
                f"{_fmt(nas['to'])}). Pick a time inside it.")
        return tm.Moment(
            id=moment_id, kind="pitr", label=f"point in time {_fmt(when.isoformat())} (NAS)",
            when=when.isoformat(), path="", size_bytes=NAS_BASE_ESTIMATE, at=when.isoformat(),
            window_from=nas["from"], window_to=nas["to"], repo_path=base_name)
    a, b = parse_time(match["from"]), parse_time(match["to"])
    if not (a and b and a <= when <= b):
        raise tm.TimeMachineError(
            f"{_fmt(when.isoformat())} is outside that window ({_fmt(match['from'])} to "
            f"{_fmt(match['to'])}). Pick a time inside it.")
    base = vault / "basebackups" / base_name
    if not base.is_file():
        raise tm.TimeMachineError(f"the base backup for this window is gone: {base_name}")
    return tm.Moment(
        id=moment_id, kind="pitr", label=f"point in time {_fmt(when.isoformat())}",
        when=when.isoformat(), path=str(base), size_bytes=base.stat().st_size,
        at=when.isoformat(), window_from=match["from"], window_to=match["to"])


def recovery_conf(wal_dir: Path, at: str) -> str:
    """The lines the unpacked data directory needs to replay WAL up to `at` and then open."""
    from scripts.osiris_pitr_drill import postgresql_auto_conf_pitr, restore_command_for

    return postgresql_auto_conf_pitr(restore_command_for((wal_dir,)), at)


def recovery_server_args() -> tuple[str, ...]:
    """The scratch server's settings: small memory, and above all never archiving."""
    return (*tm.SCRATCH_POSTGRES_ARGS, "-c", "archive_mode=off",
            "-c", f"max_connections={RECOVERY_MAX_CONNECTIONS}")


def materialize(moment: tm.Moment, rec: tm.Throwaway, phase: Callable[[str], None], *,
                sh: tm.Run, now: Callable[[], float],
                sleep: Callable[[float], None], wal_dir: Path | None = None,
                progress: Callable[[str], None] | None = None) -> None:
    """Unpack the base backup into the throwaway's volume, set up recovery to `moment.at`,
    start the server and wait for it to replay and open, then add the reader role."""
    if not moment.at:
        raise tm.TimeMachineError("a point-in-time moment needs the time to open")
    wal_dir = wal_dir or _vault(None) / "wal_archive"
    if not wal_dir.is_dir():
        raise tm.TimeMachineError("the WAL archive is not on this machine")
    image = tm.image_name(sh)
    tm._docker(sh, ["docker", "volume", "create", "--label", f"{tm.LABEL}=1", rec.volume], 60,
               "creating the scratch volume")
    phase("restoring")
    tm._docker(sh, [
        "docker", "run", "--rm", "--label", f"{tm.LABEL}=1", "--memory", HELPER_MEMORY,
        "-v", f"{rec.volume}:/data", "-v", f"{moment.path}:/tm/base.tar.gz:ro",
        "--entrypoint", "sh", image, "-c", "tar -xzf /tm/base.tar.gz -C /data"],
        EXTRACT_TIMEOUT_SECS, "unpacking the base backup")
    tm._docker(sh, [
        "docker", "run", "--rm", "--label", f"{tm.LABEL}=1", "--memory", HELPER_MEMORY,
        "-v", f"{rec.volume}:/data", "-e", f"TM_CONF={recovery_conf(wal_dir, moment.at)}",
        "--entrypoint", "sh", image, "-c",
        'touch /data/recovery.signal && printf "\\n%s\\n" "$TM_CONF" >> '
        "/data/postgresql.auto.conf && chown postgres:postgres /data/recovery.signal "
        "/data/postgresql.auto.conf"], 120, "preparing recovery")
    tm._docker(sh, [
        "docker", "run", "-d", "--name", rec.container, "--label", f"{tm.LABEL}=1",
        "--memory", tm.SCRATCH_MEMORY, "--memory-swap", tm.SCRATCH_MEMORY,
        "-p", f"127.0.0.1:{rec.port}:5432",
        "-v", f"{rec.volume}:/var/lib/postgresql/data", "-v", f"{wal_dir}:{wal_dir}:ro",
        image, *recovery_server_args()], 120, "starting the scratch database")
    wait_until_open(sh, rec.container, now=now, sleep=sleep, progress=progress,
                    target=moment.at)
    tm.finish_scratch(sh, rec)


def replay_text(replayed: str, target: str | None) -> str:
    """"replaying: reached Oct 30 11:02 of Oct 30 14:00" for the throwaway's status line."""
    got, want = parse_time(replayed), parse_time(target) if target else None
    if got is None:
        return ""
    text = f"replaying: reached {got:%b %d %H:%M}"
    return text + (f" of {want:%b %d %H:%M}" if want else "") + " UTC"


def _log_gist(text: str) -> str:
    """The lines of a server log that say what went wrong (FATAL, PANIC, ERROR), else its end."""
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    hits = [ln for ln in lines if any(w in ln for w in ("FATAL", "PANIC", "ERROR"))]
    return " | ".join((hits or lines)[-3:])[-500:]


def wait_until_open(sh: tm.Run, container: str, *, now: Callable[[], float],
                    sleep: Callable[[float], None],
                    progress: Callable[[str], None] | None = None,
                    target: str | None = None) -> None:
    """Block until archive recovery has reached the target and the server has opened.
    `pg_isready` is not that signal: a server replaying WAL already answers, long before it has
    reached the target time. Only `pg_is_in_recovery() = false` means it did."""
    deadline = now() + RECOVERY_WAIT_SECS
    last = ""
    while now() < deadline:
        r = sh(["docker", "exec", container, "psql", "-U", "osiris", "-d", "postgres", "-tAc",
                RECOVERY_STATE_SQL], 30)
        out = (r.stdout or "").strip()
        flag, _, replayed = out.partition("|")
        if r.returncode == 0 and flag in ("f", "false"):
            return
        if out:
            last = out
        if progress and replayed:
            progress(replay_text(replayed, target))
        state = sh(["docker", "inspect", "-f", "{{.State.Status}}", container], 10)
        status = (state.stdout or "").strip()
        if state.returncode != 0 or not status:
            raise tm.TimeMachineError(
                "the scratch database is gone (it was removed while replaying to that time)")
        if status != "running":
            logs = sh(["docker", "logs", "--tail", "60", container], 10)
            tail = _log_gist((logs.stdout or "") + (logs.stderr or ""))
            raise tm.TimeMachineError(
                "the scratch database stopped while replaying to that time"
                + (f" ({tail})" if tail else "")
                + ". The archive may not reach it.")
        sleep(RECOVERY_POLL_SECS)
    raise tm.TimeMachineError(
        f"replaying to that time had not finished after {RECOVERY_WAIT_SECS // 60} minutes"
        + (f" (last: {last})" if last else ""))


