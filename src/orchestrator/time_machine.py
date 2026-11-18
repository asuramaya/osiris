"""TIME MACHINE, stage 1: the throwaway database a past moment is browsed in.

Pick a moment (a vault dump), and this starts a separate Postgres from the same image the
live database runs, restores that dump into it, and hands back a read-only way in. Nothing
here ever touches the live database: the throwaway has its own container, its own named
volume, and a port on 127.0.0.1 that is never the live one.

SAFETY, each rule a refusal or a limit in code, not a convention:
  - one throwaway at a time (a second `start` is refused while one is recorded or running);
  - never the live container name, the live volume, or the live port;
  - a disk check before anything is created (free space >= DISK_FACTOR x the dump);
  - a hard time limit (TTL_SECS, extendable once) enforced by `reap`, which also removes any
    container or volume carrying our label that no record owns (a crashed start);
  - `start` tears everything down on any failure, so a failed attempt leaves nothing behind;
  - the reader role is read-only at the role level (default_transaction_read_only) with a
    statement timeout, so a browse query cannot write even by mistake.

Every docker call goes through an injectable runner, so the lifecycle is tested without a
daemon and the live run uses the same code path."""
from __future__ import annotations

import contextlib
import json
import os
import re
import secrets
import shutil
import socket
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field, fields
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

LIVE_CONTAINER = "osiris-pg"
LIVE_PORTS = frozenset({5601, 5432})
LABEL = "osiris-tm"
NAME_PREFIX = "osiris-tm-"
VOLUME_PREFIX = "osiris-tm-vol-"
FALLBACK_IMAGE = "osiris-pg:16-repack"
VAULT_DIR = Path.home() / "osiris-vault"

TTL_SECS = 2 * 3600
MAX_EXTENSIONS = 1
FAILED_KEEP_SECS = 15 * 60       # a failed start's message stays readable this long
DISK_FACTOR = 3.0
# a base backup is compressed; the data directory it unpacks to is not
PITR_DISK_FACTOR = 8.0
NAS_PITR_DISK_FACTOR = 8.0
SCRATCH_MEMORY = "6g"
RESTORE_TIMEOUT_SECS = 1800
READY_WAIT_SECS = 60
READER_ROLE = "tm_reader"
READER_STATEMENT_TIMEOUT = "30s"
# Scratch tuning: the throwaway must never inherit production sizing.
SCRATCH_POSTGRES_ARGS = (
    "-c", "shared_buffers=512MB", "-c", "maintenance_work_mem=256MB",
    "-c", "max_wal_size=4GB", "-c", "autovacuum=off", "-c", "fsync=off",
    "-c", "synchronous_commit=off", "-c", "full_page_writes=off",
)

STATE_TEXT = {
    "starting": "Starting a scratch database...",
    "pulling": "Fetching that backup from the NAS over the home network (several minutes)...",
    "restoring": "Restoring that moment (about two minutes)...",
    "ready": "Ready to browse. It is removed automatically when time runs out.",
    "failed": "That moment could not be opened.",
}

Run = Callable[[list[str], float], "subprocess.CompletedProcess[str]"]


class TimeMachineError(RuntimeError):
    """A refusal or a failure the caller can show the operator as is."""


def _default_run(cmd: list[str], timeout: float) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def state_path() -> Path:
    return Path(os.environ.get(
        "OSIRIS_TIME_MACHINE_STATE", "~/.local/state/osiris/time_machine.json")).expanduser()


@dataclass(frozen=True)
class Moment:
    """One point in the past the box can be restored to."""
    id: str            # stable: the dump's file stem; start takes this
    kind: str          # "dump" (a vault dump); PITR and restic moments share this shape
    label: str         # what the operator sees
    when: str          # ISO time of the dump
    path: str          # where the dump file (or, for a point in time, the base backup) is
    size_bytes: int
    at: str | None = None            # point in time only: the moment picked inside the window
    window_from: str | None = None   # point in time only: the window this moment lives in
    window_to: str | None = None
    snapshot: str | None = None      # NAS dump only: the restic snapshot that holds it
    repo_path: str | None = None     # NAS dump only: its path inside that snapshot


@dataclass
class Throwaway:
    id: str
    container: str
    volume: str
    port: int
    moment: dict[str, Any]
    started_at: float
    expires_at: float
    extensions: int = 0
    state: str = "starting"     # starting | restoring | ready | failed
    error: str | None = None
    scratch: str = ""               # a NAS copy's temporary files, removed with the throwaway
    progress: str = ""              # what a long step is doing right now, in words
    superuser_password: str = field(default="", repr=False)
    reader_password: str = field(default="", repr=False)

    def reader_dsn(self) -> str:
        return (f"postgresql://{READER_ROLE}:{self.reader_password}"
                f"@127.0.0.1:{self.port}/osiris")

    def public(self) -> dict[str, Any]:
        """What a console may show: never a password."""
        d = asdict(self)
        d.pop("superuser_password", None)
        d.pop("reader_password", None)
        d.pop("scratch", None)
        d["seconds_left"] = max(0, int(self.expires_at - time.time()))
        text = STATE_TEXT.get(self.state, self.state)
        d["state_text"] = f"{text} {self.progress}".strip() if self.progress else text
        return d


# --- moments -----------------------------------------------------------------------------

_DUMP_RE = re.compile(r"^osiris-(\d{8})-(\d{6})\.dump$")


def dump_time(name: str) -> datetime | None:
    """The time stamped in a dump's file name, or None when it is not one of ours."""
    m = _DUMP_RE.match(name)
    if not m:
        return None
    return datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M%S").astimezone(UTC)


def list_moments(vault_dir: Path | None = None) -> list[Moment]:
    """Vault dumps, newest first. A name that is not our dump pattern is ignored, and a
    zero-byte file is not a moment (a half-written dump must never be offered)."""
    vault = vault_dir or VAULT_DIR
    found: list[Moment] = []
    if not vault.is_dir():
        return found
    for p in vault.iterdir():
        m = _DUMP_RE.match(p.name)
        if not m or not p.is_file():
            continue
        size = p.stat().st_size
        if size <= 0:
            continue
        when = datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M%S").astimezone(UTC)
        found.append(Moment(p.stem, "dump", f"dump {when:%Y-%m-%d %H:%M}", when.isoformat(),
                            str(p), size))
    found.sort(key=lambda x: x.when, reverse=True)
    return found


def find_moment(moment_id: str, vault_dir: Path | None = None, *,
                at: str | None = None) -> Moment:
    """A moment by id. A point-in-time id needs `at` (the time picked inside its window)."""
    for m in list_moments(vault_dir):
        if m.id == moment_id:
            return m
    if moment_id.startswith("restic-"):
        from src.orchestrator import time_machine_restic as restic

        return restic.find_restic_moment(moment_id)
    if moment_id.startswith("pitr-"):
        from src.orchestrator import time_machine_pitr as pitr

        return pitr.find_pitr_moment(moment_id, at, vault_dir)
    raise TimeMachineError(f"no such moment: {moment_id!r}")


# --- the one record ----------------------------------------------------------------------

_RECORD_FIELDS = frozenset(f.name for f in fields(Throwaway))


def _progress_path(p: Path) -> Path:
    return p.with_suffix(".progress")


def read_record(path: Path | None = None) -> Throwaway | None:
    """The recorded throwaway, or None when there is none or it cannot be read. Keys this
    version does not know are ignored (a newer writer may add some), and what a long step is
    doing right now lives in a side file, so an older reader of the same record is never
    broken by it."""
    p = path or state_path()
    try:
        raw = json.loads(p.read_text())
        rec = Throwaway(**{k: v for k, v in raw.items() if k in _RECORD_FIELDS})
    except (OSError, ValueError, TypeError, AttributeError):
        return None
    with contextlib.suppress(OSError):
        rec.progress = _progress_path(p).read_text().strip()
    return rec


def record_unreadable(path: Path | None = None) -> bool:
    """A record file is there but cannot be read: not the same as "no throwaway"."""
    p = path or state_path()
    return p.exists() and read_record(p) is None


def _write_record(rec: Throwaway, path: Path | None = None) -> None:
    p = path or state_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    body = asdict(rec)
    body.pop("progress", None)             # in its own file: see read_record
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as fh:
        json.dump(body, fh)
    os.replace(tmp, p)
    if not rec.progress:
        _progress_path(p).unlink(missing_ok=True)


def _write_progress(rec: Throwaway, text: str, path: Path | None = None) -> None:
    p = path or state_path()
    rec.progress = text
    q = _progress_path(p)
    q.parent.mkdir(parents=True, exist_ok=True)
    q.write_text(text)


def _clear_record(path: Path | None = None) -> None:
    p = path or state_path()
    p.unlink(missing_ok=True)
    _progress_path(p).unlink(missing_ok=True)


# --- checks ------------------------------------------------------------------------------

def free_bytes(path: str | Path) -> int:
    """Free space where docker keeps its volumes. The docker directory itself is often
    unreadable to us, so walk up to the nearest directory we can stat: same filesystem."""
    p = Path(path)
    while True:
        try:
            return shutil.disk_usage(p).free
        except (PermissionError, FileNotFoundError):
            if p.parent == p:
                raise
            p = p.parent


def docker_root(sh: Run = _default_run) -> str:
    try:
        out = sh(["docker", "info", "--format", "{{.DockerRootDir}}"], 30)
        root = (out.stdout or "").strip()
    except (OSError, subprocess.SubprocessError):
        root = ""
    return root or "/var/lib/docker"


def disk_problem(size_bytes: int, free: int, *, factor: float | None = None) -> str | None:
    factor = DISK_FACTOR if factor is None else factor
    need = int(size_bytes * factor)
    if free < need:
        return (f"not enough disk for a throwaway restore: needs about {need // 2**30} GiB "
                f"free ({factor:g}x the backup), only {free // 2**30} GiB is free")
    return None


def pick_port(*, taken: frozenset[int] = LIVE_PORTS) -> int:
    """A free port on 127.0.0.1 chosen by the kernel, never one of the live ports."""
    for _ in range(50):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5)
            s.bind(("127.0.0.1", 0))
            port = int(s.getsockname()[1])
        if port not in taken:
            return port
    raise TimeMachineError("could not find a free local port")


def image_name(sh: Run = _default_run) -> str:
    """The image the live database runs, so the past moment is read by the binary the box
    actually uses; the repack image tag when the live container cannot be asked."""
    try:
        out = sh(["docker", "inspect", LIVE_CONTAINER, "--format", "{{.Config.Image}}"], 30)
        image = (out.stdout or "").strip() if out.returncode == 0 else ""
    except (OSError, subprocess.SubprocessError):
        image = ""
    return image or FALLBACK_IMAGE


def live_volumes(sh: Run = _default_run) -> set[str]:
    try:
        out = sh(["docker", "inspect", LIVE_CONTAINER, "--format",
                   "{{range .Mounts}}{{.Name}} {{end}}"], 30)
        return set((out.stdout or "").split()) if out.returncode == 0 else set()
    except (OSError, subprocess.SubprocessError):
        return set()


# --- the lifecycle -----------------------------------------------------------------------

def _docker(sh: Run, cmd: list[str], timeout: float, what: str) -> str:
    out = sh(cmd, timeout)
    if out.returncode != 0:
        raise TimeMachineError(f"{what} failed: {(out.stderr or out.stdout or '').strip()[:400]}")
    return out.stdout or ""


def teardown(*, sh: Run = _default_run, record_path: Path | None = None) -> list[str]:
    """Remove the recorded throwaway and anything else carrying our label. Safe to call
    twice and when nothing exists. Returns what it removed, for the audit line."""
    removed: list[str] = []
    rec = read_record(record_path)
    names: set[str] = set()
    vols: set[str] = set()
    if rec is not None:
        names.add(rec.container)
        vols.add(rec.volume)
    names |= set(_labelled(sh, "container"))
    vols |= set(_labelled(sh, "volume"))
    names = {n for n in names if n.startswith(NAME_PREFIX) and n != LIVE_CONTAINER}
    live = live_volumes(sh)
    vols = {v for v in vols if v.startswith(VOLUME_PREFIX) and v not in live}
    for n in sorted(names):
        sh(["docker", "rm", "-f", "-v", n], 60)
        removed.append(f"container {n}")
    for v in sorted(vols):
        sh(["docker", "volume", "rm", "-f", v], 60)
        removed.append(f"volume {v}")
    if rec is not None and rec.scratch:
        from src.orchestrator import time_machine_restic as restic

        restic.remove_scratch(rec.scratch)
        removed.append("scratch copy")
    _clear_record(record_path)
    return removed


def _labelled(sh: Run, kind: str) -> list[str]:
    if kind == "container":
        cmd = ["docker", "ps", "-a", "--filter", f"label={LABEL}=1", "--format", "{{.Names}}"]
    else:
        cmd = ["docker", "volume", "ls", "--filter", f"label={LABEL}=1", "--format",
               "{{.Name}}"]
    try:
        out = sh(cmd, 30)
    except (OSError, subprocess.SubprocessError):
        return []
    return (out.stdout or "").split() if out.returncode == 0 else []


def _materialize_dump(moment: Moment, rec: Throwaway, phase: Callable[[str], None], *,
                      sh: Run, now: Callable[[], float],
                      sleep: Callable[[float], None]) -> None:
    """A vault dump into a fresh scratch database: start it, pg_restore, add the reader."""
    image = image_name(sh)
    _docker(sh, ["docker", "volume", "create", "--label", f"{LABEL}=1", rec.volume], 60,
            "creating the scratch volume")
    _docker(sh, [
        "docker", "run", "-d", "--name", rec.container, "--label", f"{LABEL}=1",
        "--memory", SCRATCH_MEMORY, "--memory-swap", SCRATCH_MEMORY,
        "-p", f"127.0.0.1:{rec.port}:5432",
        "-v", f"{rec.volume}:/var/lib/postgresql/data",
        "-v", f"{moment.path}:/tm/restore.dump:ro",
        "-e", "POSTGRES_USER=osiris", "-e", f"POSTGRES_PASSWORD={rec.superuser_password}",
        "-e", "POSTGRES_DB=osiris", image, *SCRATCH_POSTGRES_ARGS], 120,
        "starting the scratch database")
    deadline = now() + READY_WAIT_SECS
    while True:
        ready = sh(["docker", "exec", rec.container, "pg_isready", "-h", "127.0.0.1",
                    "-U", "osiris"], 15)
        if ready.returncode == 0:
            break
        if now() > deadline:
            raise TimeMachineError("the scratch database did not become ready in time")
        sleep(1)

    phase("restoring")
    _docker(sh, ["docker", "exec", rec.container, "pg_restore", "-U", "osiris", "-d", "osiris",
                 "--no-owner", "-j", "4", "/tm/restore.dump"], RESTORE_TIMEOUT_SECS,
            "restoring the dump")
    finish_scratch(sh, rec)


def finish_scratch(sh: Run, rec: Throwaway) -> None:
    """Prove the restored copy holds objects, then add the read-only role."""
    count = _docker(sh, ["docker", "exec", rec.container, "psql", "-U", "osiris", "-d",
                         "osiris", "-tAc", "SELECT count(*) FROM objects"], 60,
                    "counting restored objects").strip()
    if not count.isdigit() or int(count) < 1:
        raise TimeMachineError("the restore produced no objects")
    _docker(sh, ["docker", "exec", rec.container, "psql", "-U", "osiris", "-d", "osiris",
                 "-v", "ON_ERROR_STOP=1", "-c", reader_role_sql(rec.reader_password)],
            60, "creating the read-only role")


def start(
    moment: Moment, *, sh: Run = _default_run, record_path: Path | None = None,
    now: Callable[[], float] = time.time, sleep: Callable[[float], None] = time.sleep,
    free: Callable[[str], int] = free_bytes,
    on_phase: Callable[[Throwaway], None] | None = None,
    pull: Callable[[Moment, Path], Path] | None = None,
    pull_pitr: Callable[..., tuple[Path, Path]] | None = None,
) -> Throwaway:
    """Bring a throwaway up on `moment` and return it ready. Blocking (a restore takes
    minutes): a caller runs it in a worker and polls `status`. Any failure tears down
    everything it made, then raises. `pull` fetches a NAS dump into a scratch directory
    (tests pass a fake)."""
    if moment.kind not in ("dump", "pitr", "restic"):
        raise TimeMachineError(f"moment kind {moment.kind!r} is not supported yet")
    if moment.kind != "restic" and not (moment.kind == "pitr" and not moment.path):
        source = Path(moment.path)
        if not source.is_file():
            what = "dump" if moment.kind == "dump" else "base backup"
            raise TimeMachineError(f"the {what} for this moment is gone: {source.name}")
    existing = read_record(record_path)
    if existing is not None and existing.state == "failed":
        _clear_record(record_path)          # a failed attempt only keeps its message
        existing = None
    if existing is not None:
        raise TimeMachineError(
            f"a throwaway is already {existing.state} ({existing.container}); "
            "tear it down first")
    factor = {"pitr": PITR_DISK_FACTOR, "restic": 4.0}.get(moment.kind, DISK_FACTOR)
    if moment.kind == "pitr" and not moment.path:
        factor = NAS_PITR_DISK_FACTOR      # the size is an estimate of the pulled base backup
    problem = disk_problem(moment.size_bytes, free(docker_root(sh)), factor=factor)
    if problem:
        raise TimeMachineError(problem)

    tid = secrets.token_hex(4)
    container, volume = f"{NAME_PREFIX}{tid}", f"{VOLUME_PREFIX}{tid}"
    if container == LIVE_CONTAINER or volume in live_volumes(sh):
        raise TimeMachineError("refusing: the throwaway would collide with the live database")
    t0 = now()
    rec = Throwaway(
        id=tid, container=container, volume=volume, port=pick_port(),
        moment=asdict(moment), started_at=t0, expires_at=t0 + TTL_SECS,
        superuser_password=secrets.token_urlsafe(18), reader_password=secrets.token_urlsafe(18))

    def phase(state: str) -> None:
        rec.state = state
        rec.progress = ""
        _write_record(rec, record_path)
        if on_phase:
            on_phase(rec)

    def progress(text: str) -> None:
        _write_progress(rec, text, record_path)
        if on_phase:
            on_phase(rec)

    try:
        phase("starting")
        if moment.kind == "pitr":
            from src.orchestrator import time_machine_pitr as pitr

            wal_dir: Path | None = None
            if not moment.path:                          # a window only the NAS still has
                from src.orchestrator import time_machine_restic as restic

                scratch = restic.scratch_root() / f"osiris-tm-{tid}"
                rec.scratch = str(scratch)
                phase("pulling")
                base, wal_dir = (pull_pitr or restic.pull_pitr)(moment, scratch, progress)
                moment = Moment(**{**asdict(moment), "path": str(base)})
            rec.progress = ""
            pitr.materialize(moment, rec, phase, sh=sh, now=now, sleep=sleep,
                             wal_dir=wal_dir, progress=progress)
            if rec.scratch:
                from src.orchestrator import time_machine_restic as restic

                restic.remove_scratch(rec.scratch)      # recovery is finished
                rec.scratch = ""
        elif moment.kind == "restic":
            from src.orchestrator import time_machine_restic as restic

            scratch = restic.scratch_root() / f"osiris-tm-{tid}"
            rec.scratch = str(scratch)
            phase("pulling")
            fetched = (pull or restic.pull)(moment, scratch)
            local = Moment(**{**asdict(moment), "path": str(fetched)})
            _materialize_dump(local, rec, phase, sh=sh, now=now, sleep=sleep)
            restic.remove_scratch(scratch)          # the dump is in the database now
            rec.scratch = ""
        else:
            _materialize_dump(moment, rec, phase, sh=sh, now=now, sleep=sleep)
        phase("ready")
        return rec
    except BaseException as exc:
        teardown(sh=sh, record_path=record_path)
        if isinstance(exc, TimeMachineError):
            raise
        if isinstance(exc, subprocess.TimeoutExpired):
            raise TimeMachineError(f"a step timed out after {exc.timeout:.0f}s") from exc
        raise


def reader_role_sql(password: str) -> str:
    """A role that can read and cannot write, whatever the query says."""
    if not re.fullmatch(r"[A-Za-z0-9_\-]+", password):
        raise TimeMachineError("unsafe reader password")
    return (
        f"CREATE ROLE {READER_ROLE} LOGIN PASSWORD '{password}'; "
        f"ALTER ROLE {READER_ROLE} SET default_transaction_read_only = on; "
        f"ALTER ROLE {READER_ROLE} SET statement_timeout = '{READER_STATEMENT_TIMEOUT}'; "
        f"GRANT USAGE ON SCHEMA public TO {READER_ROLE}; "
        f"GRANT SELECT ON ALL TABLES IN SCHEMA public TO {READER_ROLE};")


def status(*, record_path: Path | None = None) -> dict[str, Any] | None:
    rec = read_record(record_path)
    return rec.public() if rec else None


def extend(*, record_path: Path | None = None, now: Callable[[], float] = time.time) -> Throwaway:
    """Add one more TTL, once. A second extension is refused: a throwaway is not a
    long-lived copy of the database."""
    rec = read_record(record_path)
    if rec is None:
        raise TimeMachineError("no throwaway to extend")
    if rec.extensions >= MAX_EXTENSIONS:
        raise TimeMachineError("this throwaway was already extended once; start a new one")
    rec.extensions += 1
    rec.expires_at = max(rec.expires_at, now()) + TTL_SECS
    _write_record(rec, record_path)
    return rec


def reap(*, sh: Run = _default_run, record_path: Path | None = None,
         now: Callable[[], float] = time.time) -> list[str]:
    """Tear down an expired throwaway (a failed one after FAILED_KEEP_SECS), and sweep
    labelled leftovers that no record owns. Cheap and idempotent: the worker calls it on a
    timer. A live, unexpired throwaway is left alone, and so is its own container and
    volume."""
    rec = read_record(record_path)
    if rec is None and record_unreadable(record_path):
        # something is there that this version cannot read (a newer writer, a half-written
        # file): never take that for "no throwaway" and sweep a live one away
        return []
    keep = rec is not None and (
        now() < rec.started_at + FAILED_KEEP_SECS if rec.state == "failed"
        else now() < rec.expires_at)
    if rec is not None and keep:
        owned = {rec.container, rec.volume}
        stray = [n for n in _labelled(sh, "container") + _labelled(sh, "volume")
                 if n not in owned and (n.startswith(NAME_PREFIX) or n.startswith(VOLUME_PREFIX))]
        removed: list[str] = []
        for n in stray:
            if n.startswith(VOLUME_PREFIX):
                sh(["docker", "volume", "rm", "-f", n], 60)
                removed.append(f"volume {n}")
            else:
                sh(["docker", "rm", "-f", "-v", n], 60)
                removed.append(f"container {n}")
        return removed + _sweep_scratch(rec)
    return teardown(sh=sh, record_path=record_path) + _sweep_scratch(None)


def _sweep_scratch(rec: Throwaway | None, now: Callable[[], float] = time.time) -> list[str]:
    """Remove NAS scratch directories left by a start that died without a record (older than a
    throwaway can live), never the one a record owns."""
    from src.orchestrator import time_machine_restic as restic

    root = restic.scratch_root()
    out: list[str] = []
    try:
        entries = list(root.glob("osiris-tm-*"))
    except OSError:
        return out
    for d in entries:
        if (rec is not None and str(d) == rec.scratch) or not d.is_dir():
            continue
        try:
            age = now() - d.stat().st_mtime
        except OSError:
            continue
        if age > TTL_SECS + 3600:
            restic.remove_scratch(d)
            out.append(f"scratch {d.name}")
    return out


def record_failure(moment: Moment, message: str, *, record_path: Path | None = None,
                   now: Callable[[], float] = time.time) -> Throwaway:
    """Keep why a start failed where the console polls, as a record in state "failed" (no
    container behind it). The next start replaces it and the reaper clears it."""
    t0 = now()
    rec = Throwaway(
        id=secrets.token_hex(4), container="", volume="", port=0, moment=asdict(moment),
        started_at=t0, expires_at=t0 + TTL_SECS, state="failed", error=message)
    _write_record(rec, record_path)
    return rec


def run_detached(moment_id: str, *, vault_dir: Path | None = None, at: str | None = None) -> int:
    """What `osiris time-machine run <moment id>` does, in its own process so a server or
    console restart cannot kill a restore half way: bring the throwaway up, and on failure
    leave the reason in the record. 0 when it came up, 1 when it did not."""
    moment: Moment | None = None
    try:
        moment = find_moment(moment_id, vault_dir, at=at)
        start(moment)
        return 0
    except TimeMachineError as exc:
        if moment is not None and "already" not in str(exc):
            record_failure(moment, str(exc))
        print(f"time machine: {exc}")
        return 1
    except Exception as exc:  # noqa: BLE001 - the console shows a sentence, never a traceback
        if moment is not None:
            record_failure(moment, f"{type(exc).__name__}: {exc}")
        print(f"time machine: {type(exc).__name__}: {exc}")
        return 1


def spawn_run(moment_id: str, *, at: str | None = None,
              popen: Callable[..., Any] = subprocess.Popen,
              log_path: Path | None = None) -> int:
    """Start `run_detached` as a separate, session-detached process; returns its pid. Its
    output goes to a log file beside the record, never to a pipe nobody reads."""
    log = log_path or state_path().with_suffix(".log")
    log.parent.mkdir(parents=True, exist_ok=True)
    root = Path(__file__).resolve().parents[2]
    with open(log, "ab") as fh:
        proc = popen(
            [sys.executable, "-m", "src.cli", "time-machine", "run", moment_id,
             *(["--at", at] if at else [])],
            cwd=str(root), stdin=subprocess.DEVNULL, stdout=fh, stderr=fh,
            start_new_session=True)
    return int(proc.pid)
