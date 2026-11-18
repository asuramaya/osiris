"""TIME MACHINE: dump moments that live only in the NAS restic repository.

The vault on this machine keeps the recent dumps; older ones survive only inside restic
snapshots on the NAS. This lists those dumps (from a small saved map, so the moment list is
instant and works away from home) and, when one is picked, copies that ONE dump file back to a
scratch directory so the ordinary dump restore can open it.

Every use of the repository follows the rules the other NAS jobs follow:
  - only over the home network (`lan_presence` with a real interface, never a tunnel);
  - the offload tick's own lock is taken for the copy, or the pull is refused ("an offload is
    running"), so two restic jobs never run in the repository together;
  - reads only, with `--no-lock`: nothing in the repository changes, no lock is left behind;
  - the copy is checked against the size the listing recorded, and the scratch directory is
    removed afterwards.

`refresh` builds the saved map (run on the home network, `osiris time-machine refresh`)."""
from __future__ import annotations

import contextlib
import json
import os
import re
import shutil
import tarfile
import threading
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from src.orchestrator import restore_points
from src.orchestrator import time_machine as tm
from src.orchestrator.instants import parse_time

CACHE_ENV = "OSIRIS_TM_RESTIC_FILE"
_DEFAULT_CACHE = "~/.local/state/osiris/time_machine_restic.json"
CHUNK = 20
FIND_TIMEOUT_SECS = 1800
LIST_TIMEOUT_SECS = 300
PULL_MIN_BYTES_PER_SEC = 2 * 1024**2     # the slowest copy worth waiting for
PULL_BASE_SECS = 300.0
# scratch copy + the throwaway's own data: both must fit at once
RESTIC_DISK_FACTOR = 4.0
_AWAY = "not on the home network"


@dataclass
class Deps:
    """What the module needs from the outside, so the rules are testable without a NAS."""
    run: Callable[..., Any]                          # run(args, timeout=..., text=True)
    presence: Callable[[dict[str, Any]], dict[str, Any]]
    hold_lock: Callable[[], AbstractContextManager[bool]]
    now: Callable[[], datetime] = lambda: datetime.now(UTC)  # noqa: E731


def cache_path() -> Path:
    return Path(os.environ.get(CACHE_ENV) or _DEFAULT_CACHE).expanduser()


def read_cache() -> dict[str, Any] | None:
    try:
        body = json.loads(cache_path().read_text())
    except (OSError, ValueError):
        return None
    return body if isinstance(body, dict) and "dumps" in body else None


def _write_cache(body: dict[str, Any]) -> None:
    path = cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    try:
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as fh:
            json.dump(body, fh)
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def real_deps(target: dict[str, Any], password: bytes) -> Deps:
    from src.orchestrator import restic_locks
    from src.orchestrator.network_presence import lan_presence
    from src.orchestrator.offload_runner import hold_tick_lock

    env = {**os.environ, "RESTIC_REPOSITORY": str(target.get("path_or_url") or ""),
           "RESTIC_PASSWORD": password.decode()}

    def run(args: list[str], *, timeout: float, text: bool = True) -> Any:
        return restic_locks.run_restic(args, env, timeout=timeout, text=text)

    return Deps(run=run, presence=lan_presence, hold_lock=hold_tick_lock)


def _home(presence: dict[str, Any]) -> str | None:
    """None when on the home network, else the sentence that says why not."""
    if presence.get("present") and presence.get("dev"):
        return None
    why = str(presence.get("reason") or "").strip()
    return f"{_AWAY[0].upper()}{_AWAY[1:]}" + (f" ({why})" if why else "") + "."


def refresh(target: dict[str, Any], deps: Deps) -> dict[str, Any]:
    """Rebuild the saved map of dumps held in the repository (read-only on the NAS). Raises
    TimeMachineError with a sentence when away, or when restic fails."""
    away = _home(deps.presence(target))
    if away:
        raise tm.TimeMachineError(away + " Nothing was read.")
    proc = deps.run(["snapshots", "--json", "--no-lock"], timeout=LIST_TIMEOUT_SECS)
    if proc.returncode != 0:
        raise tm.TimeMachineError("Could not list the backup snapshots: "
                                  + _tail(proc.stderr))
    snaps = json.loads(proc.stdout or "[]")
    best: dict[str, dict[str, Any]] = {}      # dump name -> the newest snapshot that holds it
    ids = [(str(s["id"]), str(s.get("time") or "")) for s in snaps]
    for i in range(0, len(ids), CHUNK):
        part = ids[i:i + CHUNK]
        args = ["find", "--json", "--no-lock"]
        for sid, _t in part:
            args += ["--snapshot", sid]
        found = deps.run([*args, "osiris-*.dump"], timeout=FIND_TIMEOUT_SECS)
        if found.returncode != 0:
            raise tm.TimeMachineError("Could not search the snapshots for dumps: "
                                      + _tail(found.stderr))
        times = dict(part)
        for snap in json.loads(found.stdout or "[]"):
            sid = str(snap.get("snapshot") or "")
            for m in snap.get("matches") or []:
                if m.get("type") != "file":
                    continue
                got = restore_points.classify(str(m.get("path")))
                if not got or got[0] != "dump" or not got[1].endswith(".dump"):
                    continue
                name = got[1]
                entry = {"name": name, "snapshot": sid, "snapshot_time": times.get(sid, ""),
                         "path": str(m["path"]), "size": int(m.get("size") or 0)}
                old = best.get(name)
                if old is None or entry["snapshot_time"] > old["snapshot_time"]:
                    best[name] = entry
    body = {"as_of": deps.now().isoformat(), "repo": str(target.get("path_or_url") or ""),
            "target": {k: target.get(k) for k in ("name", "path_or_url", "allow_tunnel")},
            "snapshots": len(snaps),
            "dumps": sorted(best.values(), key=lambda e: e["name"], reverse=True)}
    _write_cache(body)
    return body


def refresh_map(target: dict[str, Any], run: Callable[..., Any], *,
                presence: Callable[[dict[str, Any]], dict[str, Any]] | None = None) -> str | None:
    """Rebuild the saved map of NAS dumps from a restic runner the caller already has (the
    weekly repo-thin run calls this right after its own restore-map refresh, so there is one
    NAS reader and one timer). Read only: every restic call here is a listing with
    `--no-lock`. It needs the home-network proof itself, so a caller that did not check still
    cannot read the NAS from away. Returns a failure sentence, or None when the map is fresh;
    never raises."""
    if presence is None:
        from src.orchestrator.network_presence import lan_presence

        presence = lan_presence

    @contextlib.contextmanager
    def no_lock() -> Iterator[bool]:
        yield True                      # a listing takes no lock, so there is none to hold

    try:
        refresh(target, Deps(run=run, presence=presence, hold_lock=no_lock))
    except tm.TimeMachineError as exc:
        return str(exc)
    except Exception as exc:  # noqa: BLE001 - the weekly run must not stop on a listing
        return f"{type(exc).__name__}: {exc}"
    return None


def _tail(text: str | None) -> str:
    lines = (text or "").strip().splitlines()[-1:] or [""]
    return lines[0][:300] or "no detail"


def restic_moments(vault_dir: Path | None = None, *,
                   presence: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
                   cache: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Dump moments held only on the NAS, for the console list. A dump the vault still has is
    not listed again. Each carries `available` (on the home network now) and, when it is not,
    the reason. Never touches the NAS: it reads the saved map and asks the network layer."""
    cache = cache if cache is not None else read_cache()
    if not cache:
        return []
    vault = vault_dir or tm.VAULT_DIR
    have = {m.id + ".dump" for m in tm.list_moments(vault)}
    reason: str | None
    look = presence
    if look is None:
        from src.orchestrator.network_presence import lan_presence

        look = lan_presence
    try:
        reason = _home(look(cache.get("target") or {}))
    except Exception:  # noqa: BLE001 - a failed look at the network means "not now"
        reason = "The home network could not be checked."
    out: list[dict[str, Any]] = []
    for e in cache.get("dumps") or []:
        if e["name"] in have:
            continue
        when = tm.dump_time(e["name"])
        if when is None:
            continue
        item = {"id": "restic-" + e["name"].removesuffix(".dump"), "kind": "restic",
                "label": f"dump {when:%Y-%m-%d %H:%M} (NAS)", "when": when.isoformat(),
                "size_bytes": int(e["size"]), "available": reason is None}
        if reason:
            item["reason"] = reason
        out.append(item)
    out.sort(key=lambda i: str(i["when"]), reverse=True)
    return out


def nas_reason(presence: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
               cache: dict[str, Any] | None = None) -> str | None:
    """None when something held only on the NAS can be opened right now; otherwise the sentence
    that says why not (away from home, or the saved map not built yet)."""
    cache = cache if cache is not None else read_cache()
    if not cache or not (cache.get("target") or {}).get("path_or_url"):
        return ("The list of NAS backups has not been built yet. Run `osiris time-machine "
                "refresh` at home first.")
    look = presence
    if look is None:
        from src.orchestrator.network_presence import lan_presence

        look = lan_presence
    try:
        return _home(look(cache.get("target") or {}))
    except Exception:  # noqa: BLE001 - a failed look at the network means "not now"
        return "The home network could not be checked."


def find_restic_moment(moment_id: str, cache: dict[str, Any] | None = None) -> tm.Moment:
    cache = cache if cache is not None else read_cache()
    name = moment_id.removeprefix("restic-") + ".dump"
    entry = next((e for e in (cache or {}).get("dumps") or [] if e["name"] == name), None)
    if entry is None:
        raise tm.TimeMachineError(f"no such moment: {moment_id!r}")
    when = tm.dump_time(name)
    return tm.Moment(
        id=moment_id, kind="restic", label=f"dump {when:%Y-%m-%d %H:%M} (NAS)" if when
        else name, when=when.isoformat() if when else "", path="", size_bytes=int(entry["size"]),
        snapshot=str(entry["snapshot"]), repo_path=str(entry["path"]))


def pull_budget_secs(size_bytes: int) -> float:
    return PULL_BASE_SECS + size_bytes / PULL_MIN_BYTES_PER_SEC


SCRATCH_ENV = "OSIRIS_TM_SCRATCH"


def scratch_root() -> Path:
    """Where copies from the NAS are staged: on disk, never the memory-backed /tmp (a base
    backup and its log are several GB)."""
    return Path(os.environ.get(SCRATCH_ENV) or "/var/tmp")


def pull(moment: tm.Moment, scratch: Path, deps: Deps | None = None,
         target: dict[str, Any] | None = None) -> Path:
    """Copy the one dump of `moment` out of the repository into `scratch` and return its path.
    Refuses away from home, and while an offload holds the lock. Read-only on the NAS."""
    if not moment.snapshot or not moment.repo_path:
        raise tm.TimeMachineError("that moment does not say where its dump is")
    if deps is None:
        from src.orchestrator.restic_credential import ResticPasswordMissing, get_restic_password

        cache = read_cache() or {}
        target = cache.get("target") or {}
        try:
            password = get_restic_password()
        except ResticPasswordMissing as exc:
            raise tm.TimeMachineError(f"The backup repository's password could not be read: "
                                      f"{exc}") from exc
        deps = real_deps(target, password)
    away = _home(deps.presence(target or {}))
    if away:
        raise tm.TimeMachineError(away + " That backup is only on the NAS, so it can only be "
                                  "opened at home.")
    scratch.mkdir(parents=True, exist_ok=True)
    with deps.hold_lock() as got:
        if not got:
            raise tm.TimeMachineError(
                "A backup to the NAS is running right now. Try again in a few minutes.")
        proc = deps.run(
            ["restore", moment.snapshot, "--no-lock", "--target", str(scratch),
             "--include", moment.repo_path], timeout=pull_budget_secs(moment.size_bytes))
    if proc.returncode != 0:
        raise tm.TimeMachineError("Copying that backup from the NAS failed: "
                                  + _tail(proc.stderr))
    got_files = [p for p in scratch.rglob("*.dump") if p.is_file()]
    if len(got_files) != 1:
        raise tm.TimeMachineError("The NAS copy did not contain exactly that dump.")
    f = got_files[0]
    if moment.size_bytes and f.stat().st_size != moment.size_bytes:
        raise tm.TimeMachineError(
            f"The copy from the NAS is {f.stat().st_size} bytes, but the backup recorded "
            f"{moment.size_bytes}; it was not used.")
    return f


def remove_scratch(path: str | Path) -> None:
    """Delete a scratch directory this module made (by name only), ignoring errors."""
    p = Path(path)
    if p.name.startswith("osiris-tm-") and p.is_dir():
        with contextlib.suppress(OSError):
            shutil.rmtree(p, ignore_errors=True)


async def refresh_command(*, pool: Any, say: Callable[[str], None] = print,
                          deps: Deps | None = None) -> int:
    """`osiris time-machine refresh`: rebuild the saved map of NAS dumps. Home network only."""
    import asyncio

    from src.orchestrator.backup_settings import get_backup_settings
    from src.orchestrator.nas_first_offload import pick_target
    from src.orchestrator.restic_credential import ResticPasswordMissing, get_restic_password

    settings = await get_backup_settings(pool)
    target, why = pick_target(settings.get("offload_targets") or [], None)
    if target is None:
        say(f"time-machine: {why}.")
        return 1
    if deps is None:
        try:
            deps = real_deps(target, get_restic_password())
        except ResticPasswordMissing as exc:
            say(f"time-machine: {exc}")
            return 1
    try:
        body = await asyncio.to_thread(refresh, target, deps)
    except tm.TimeMachineError as exc:
        say(f"time-machine: {exc}")
        return 1
    say(f"time-machine: {len(body['dumps'])} dump(s) found across {body['snapshots']} "
        "snapshot(s); the list of backups you can open is up to date.")
    return 0


# --- a point in time held only on the NAS --------------------------------------------------
#
# Opening a window the vault no longer has means pulling its base backup and the WAL from the
# start of that base to the picked time. The catch: the vault thins WAL as it ages, so each
# snapshot holds only a SLICE of the log (an older slice in older snapshots). The cover is
# therefore worked out LIVE, at open time, by listing the segments each snapshot holds (one
# `find` per snapshot, a second or two): never from a saved list, because the set of snapshots
# changes whenever the repository is thinned. A hole anywhere in the run refuses the open.

_SEG = re.compile(r"^([0-9A-F]{8})([0-9A-F]{8})([0-9A-F]{8})(\.zst)?$")
_LABEL_FILE = re.compile(r"\(file ([0-9A-F]{24})\)")
# The time a segment carries is when the vault COPIED it, not when it was archived, and the
# copies come in batches (every 15 minutes lately, much less often in the past): the segments of
# one copy run share a stamp to within seconds. So the first segment stamped after the picked
# time can sit hours before the one that actually holds it. The log is therefore taken through
# LOG_BATCHES_PAST further copy batches (a batch ends where the stamps jump by BATCH_GAP), and
# read from a snapshot made TOP_MARGIN after the time so those batches are in it. Postgres stops
# at the picked time; the extra segments are never replayed.
BATCH_GAP = timedelta(seconds=120)
LOG_BATCHES_PAST = 2
TOP_MARGIN = timedelta(hours=12)
BASE_CANDIDATES = 8          # snapshots tried when looking for one that holds the base backup
MAX_PROBES = 60              # snapshots listed at most while building the cover


@dataclass(frozen=True)
class SegRef:
    pos: int            # log * 256 + segment, within one timeline
    name: str
    path: str
    mtime: datetime
    size: int


def seg_key(name: str) -> tuple[int, int] | None:
    """(timeline, position) of a WAL segment file name, or None for anything else."""
    m = _SEG.match(name)
    if not m:
        return None
    return int(m[1], 16), int(m[2], 16) * 256 + int(m[3], 16)


def seg_name(tl: int, pos: int, suffix: str = ".zst") -> str:
    return f"{tl:08X}{pos // 256:08X}{pos % 256:08X}{suffix}"


def backup_label_start(base: Path) -> tuple[int, int]:
    """(timeline, position) of the first WAL segment a base backup needs, read from the
    `backup_label` it carries (the first member of the archive, so only its start is read)."""
    try:
        with tarfile.open(base, "r|gz") as tf:
            for member in tf:
                if member.name.lstrip("./") != "backup_label":
                    continue
                f = tf.extractfile(member)
                text = f.read().decode("utf-8", "replace") if f else ""
                m = _LABEL_FILE.search(text)
                key = seg_key(m[1]) if m else None
                if key:
                    return key
                break
    except (tarfile.TarError, OSError, EOFError):
        pass
    raise tm.TimeMachineError("The base backup's own label could not be read, so the WAL it "
                              "needs cannot be worked out.")


def segments_of(deps: Deps, snapshot: str, timeline: int) -> dict[int, SegRef]:
    """The WAL segments of ONE timeline that a snapshot holds, by position (one `find`, read
    only). Another timeline's segment with the same position is a different piece of history
    and must never fill a hole in this one."""
    proc = deps.run(["find", "--json", "--no-lock", "--snapshot", snapshot, "0000000*"],
                    timeout=LIST_TIMEOUT_SECS)
    if proc.returncode != 0:
        raise tm.TimeMachineError("Could not list the log in a snapshot: " + _tail(proc.stderr))
    out: dict[int, SegRef] = {}
    for snap in json.loads(proc.stdout or "[]"):
        for m in snap.get("matches") or []:
            path = str(m.get("path") or "")
            key = seg_key(path.rsplit("/", 1)[-1])
            when = parse_time(m.get("mtime"))
            if m.get("type") != "file" or "/wal_archive/" not in path or not key or not when:
                continue
            if key[0] != timeline:
                continue
            out[key[1]] = SegRef(key[1], path.rsplit("/", 1)[-1], path, when,
                                 int(m.get("size") or 0))
    return out


@dataclass
class WalCover:
    first_pos: int
    last_pos: int
    chosen: list[tuple[str, list[SegRef]]]     # (snapshot id, the segments to take from it)

    @property
    def segments(self) -> int:
        return sum(len(refs) for _sid, refs in self.chosen)

    @property
    def bytes(self) -> int:
        return sum(r.size for _sid, refs in self.chosen for r in refs)


def _last_needed(segs: dict[int, SegRef], first_stamped: int) -> int:
    """The last segment to take: the end of the copy batch `first_stamped` belongs to, plus
    LOG_BATCHES_PAST more batches (or as far as the snapshot goes)."""
    positions = sorted(p for p in segs if p >= first_stamped)
    batches_ended = 0
    last = positions[0]
    for prev, nxt in zip(positions, positions[1:], strict=False):
        if segs[nxt].mtime - segs[prev].mtime > BATCH_GAP:
            batches_ended += 1
            if batches_ended > LOG_BATCHES_PAST:
                break
        last = nxt
    return last


def plan_wal_cover(*, snapshots: list[tuple[datetime, str]], target: datetime, tl: int,
                   first_pos: int, segments_in: Callable[[str], dict[int, SegRef]],
                   max_probes: int = MAX_PROBES) -> WalCover:
    """Choose which snapshots to take WAL from so the run from `first_pos` to the segment that
    holds `target` is complete. `snapshots` are (time, id), oldest first. Raises TimeMachineError
    with a sentence when a segment is missing from every snapshot looked at."""
    if not snapshots:
        raise tm.TimeMachineError("The NAS holds no snapshots to read the log from.")
    ordered = sorted(snapshots)
    idx = next((i for i, (t, _s) in enumerate(ordered) if t >= target + TOP_MARGIN),
               len(ordered) - 1)
    probes = 0
    last_pos: int | None = None
    top: dict[int, SegRef] = {}
    top_id = ""
    for _t, sid in ordered[idx:idx + 5]:
        probes += 1
        segs = segments_in(sid)
        later = [p for p, r in segs.items() if r.mtime >= target and p >= first_pos]
        if later:
            last_pos = _last_needed(segs, min(later))
            top, top_id = segs, sid
            break
    if last_pos is None:
        raise tm.TimeMachineError("No log was found on the NAS at or after that time, so it "
                                  "cannot be replayed to.")
    missing = set(range(first_pos, last_pos + 1))
    chosen: list[tuple[str, list[SegRef]]] = []
    seen = {top_id: top}

    def take(sid: str, segs: dict[int, SegRef]) -> None:
        got = sorted(missing & segs.keys())
        if got:
            chosen.append((sid, [segs[p] for p in got]))
            missing.difference_update(got)

    take(top_id, top)
    while missing and probes < max_probes:
        above = max(missing) + 1
        ref = next((r for _s, refs in chosen for r in refs if r.pos == above), None)
        est = ref.mtime if ref else target
        later_c = [(t, s) for t, s in ordered if s not in seen and t >= est]
        earlier_c = [(t, s) for t, s in reversed(ordered) if s not in seen and t < est]
        pool = later_c + earlier_c
        if not pool:
            break
        _t, sid = pool[0]
        probes += 1
        seen[sid] = segments_in(sid)
        take(sid, seen[sid])
    if missing:
        lost = seg_name(tl, min(missing))
        raise tm.TimeMachineError(
            f"The NAS no longer holds a complete run of the log between that backup and that "
            f"time (segment {lost} is missing from every snapshot looked at), so that time "
            "cannot be opened. Pick a different time.")
    return WalCover(first_pos, last_pos, chosen)


def _dir_bytes(path: Path) -> int:
    """Bytes actually written under `path`: restic reserves a file's full size when it starts
    it, so the length would read "done" at once; the blocks in use grow as the data arrives."""
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            with contextlib.suppress(OSError):
                total += (Path(root) / name).stat().st_blocks * 512
    return total


@contextlib.contextmanager
def watch_size(path: Path, total: int, label: str, progress: Callable[[str], None],
               every: float = 5.0) -> Iterator[None]:
    """Report "<label> 2.1/3.8 GB" while a restic copy runs, by watching the target grow."""
    stop = threading.Event()

    def loop() -> None:
        while not stop.wait(every):
            done = _dir_bytes(path)
            with contextlib.suppress(Exception):
                progress(f"{label} {done / 1e9:.1f}/{total / 1e9:.1f} GB")

    t = threading.Thread(target=loop, daemon=True)
    t.start()
    try:
        yield
    finally:
        stop.set()
        t.join(timeout=10)


def _find_base(deps: Deps, base_name: str, window_from: datetime,
               snaps: list[tuple[datetime, str]]) -> tuple[str, str, int]:
    """(snapshot id, path, size) of a snapshot that holds the base backup: tried in time order
    from the window's start, a few at most, since the base outlives only some snapshots."""
    for _t, sid in [x for x in sorted(snaps) if x[0] >= window_from][:BASE_CANDIDATES]:
        proc = deps.run(["find", "--json", "--no-lock", "--snapshot", sid, base_name],
                        timeout=LIST_TIMEOUT_SECS)
        if proc.returncode != 0:
            continue
        for snap in json.loads(proc.stdout or "[]"):
            for m in snap.get("matches") or []:
                if m.get("type") == "file" and str(m.get("path", "")).endswith(base_name):
                    return sid, str(m["path"]), int(m.get("size") or 0)
    raise tm.TimeMachineError(
        "The NAS no longer holds the base backup for that window, so it cannot be opened.")


def _default_deps() -> tuple[Deps, dict[str, Any]]:
    """Deps for the saved NAS target and the repository password, or a refusal sentence."""
    from src.orchestrator.restic_credential import ResticPasswordMissing, get_restic_password

    target = (read_cache() or {}).get("target") or {}
    if not target.get("path_or_url"):
        raise tm.TimeMachineError("The list of NAS backups has not been built yet. Run "
                                  "`osiris time-machine refresh` at home first.")
    try:
        return real_deps(target, get_restic_password()), target
    except ResticPasswordMissing as exc:
        raise tm.TimeMachineError(f"The backup repository's password could not be read: "
                                  f"{exc}") from exc


def base_size(base_name: str, window_from: datetime, deps: Deps | None = None) -> int | None:
    """The size of a window's base backup on the NAS (a listing, read only), or None when no
    snapshot holds it. The monthly drill uses it to stay inside its budget."""
    if deps is None:
        deps, _target = _default_deps()
    proc = deps.run(["snapshots", "--json", "--no-lock"], timeout=LIST_TIMEOUT_SECS)
    if proc.returncode != 0:
        raise tm.TimeMachineError("Could not list the backup snapshots: " + _tail(proc.stderr))
    snaps = [(t, str(x["id"])) for x in json.loads(proc.stdout or "[]")
             if (t := parse_time(x.get("time")))]
    try:
        return _find_base(deps, base_name, window_from, snaps)[2]
    except tm.TimeMachineError:
        return None


def pull_pitr(moment: tm.Moment, scratch: Path, progress: Callable[[str], None],
              deps: Deps | None = None, target: dict[str, Any] | None = None,
              free: Callable[[str], int] = tm.free_bytes) -> tuple[Path, Path]:
    """Copy a window's base backup and the log it needs up to `moment.at` out of the NAS into
    `scratch`: returns (base backup file, directory of WAL segments). Home network only, the
    offload tick lock held for the whole pull, reads only (`--no-lock`)."""
    if not moment.repo_path or not moment.at or not moment.window_from:
        raise tm.TimeMachineError("that moment does not say which backup it comes from")
    if deps is None:
        from src.orchestrator.restic_credential import ResticPasswordMissing, get_restic_password

        target = (read_cache() or {}).get("target") or {}
        if not target.get("path_or_url"):
            raise tm.TimeMachineError("The list of NAS backups has not been built yet. Run "
                                      "`osiris time-machine refresh` at home first.")
        try:
            deps = real_deps(target, get_restic_password())
        except ResticPasswordMissing as exc:
            raise tm.TimeMachineError(f"The backup repository's password could not be read: "
                                      f"{exc}") from exc
    away = _home(deps.presence(target or {}))
    if away:
        raise tm.TimeMachineError(away + " That time is only on the NAS, so it can only be "
                                  "opened at home.")
    at, window_from = parse_time(moment.at), parse_time(moment.window_from)
    if at is None or window_from is None:
        raise tm.TimeMachineError("that moment has no readable time")
    scratch.mkdir(parents=True, exist_ok=True)
    # The tick lock is what keeps these `--no-lock` reads apart from a thinning run (which
    # takes the repository's exclusive lock and forgets snapshots): the snapshot list is read
    # INSIDE it, so a thin cannot land between the listing and the pull and leave us naming
    # snapshots that are gone.
    with deps.hold_lock() as got:
        if not got:
            raise tm.TimeMachineError(
                "A backup to the NAS is running right now. Try again in a few minutes.")
        proc = deps.run(["snapshots", "--json", "--no-lock"], timeout=LIST_TIMEOUT_SECS)
        if proc.returncode != 0:
            raise tm.TimeMachineError("Could not list the backup snapshots: "
                                      + _tail(proc.stderr))
        snaps = [(t, str(s["id"])) for s in json.loads(proc.stdout or "[]")
                 if (t := parse_time(s.get("time")))]
        base_snap, base_path, base_size = _find_base(deps, moment.repo_path, window_from, snaps)
        progress("working out which snapshots hold the log")
        base_dir = scratch / "base"
        with watch_size(base_dir, base_size, "pulling base", progress):
            r = deps.run(["restore", base_snap, "--no-lock", "--target", str(base_dir),
                          "--include", base_path], timeout=pull_budget_secs(base_size))
        if r.returncode != 0:
            raise tm.TimeMachineError("Copying the base backup from the NAS failed: "
                                      + _tail(r.stderr))
        files = [p for p in base_dir.rglob(moment.repo_path) if p.is_file()]
        if len(files) != 1 or (base_size and files[0].stat().st_size != base_size):
            raise tm.TimeMachineError("The base backup copied from the NAS is not the size "
                                      "the repository recorded; it was not used.")
        base_file = files[0]
        tl, first_pos = backup_label_start(base_file)
        cache: dict[str, dict[int, SegRef]] = {}

        def listing(sid: str) -> dict[int, SegRef]:
            if sid not in cache:
                cache[sid] = segments_of(deps, sid, tl)
            return cache[sid]

        cover = plan_wal_cover(snapshots=snaps, target=at, tl=tl, first_pos=first_pos,
                               segments_in=listing)
        need = base_size * tm.PITR_DISK_FACTOR + cover.bytes
        if free(str(scratch)) < need:
            raise tm.TimeMachineError(
                f"There is not enough disk to open that time: it needs about "
                f"{int(need) // 2**30} GiB free, only {free(str(scratch)) // 2**30} GiB is free.")
        wal_dir = scratch / "wal"
        wal_dir.mkdir()
        for n, (sid, refs) in enumerate(cover.chosen, start=1):
            raw = scratch / f"walraw-{n}"
            listfile = scratch / f"walraw-{n}.txt"
            listfile.write_text("\n".join(r.path for r in refs) + "\n")
            label = f"pulling log {n} of {len(cover.chosen)} snapshots"
            with watch_size(raw, sum(r.size for r in refs), label, progress):
                r = deps.run(["restore", sid, "--no-lock", "--target", str(raw),
                              "--include-file", str(listfile)],
                             timeout=pull_budget_secs(sum(x.size for x in refs)))
            if r.returncode != 0:
                raise tm.TimeMachineError("Copying the log from the NAS failed: "
                                          + _tail(r.stderr))
            for ref in refs:
                src = next(raw.rglob(ref.name), None)
                if src is None or (ref.size and src.stat().st_size != ref.size):
                    raise tm.TimeMachineError(
                        f"The log segment {ref.name} copied from the NAS is missing or the "
                        "wrong size; it was not used.")
                shutil.move(str(src), wal_dir / ref.name)
            shutil.rmtree(raw, ignore_errors=True)
            listfile.unlink(missing_ok=True)
    return base_file, wal_dir
