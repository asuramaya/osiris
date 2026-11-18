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
import tempfile
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.orchestrator import restore_points
from src.orchestrator import time_machine as tm

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


def scratch_root() -> Path:
    return Path(os.environ.get("TMPDIR") or tempfile.gettempdir())


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
            import shutil

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
