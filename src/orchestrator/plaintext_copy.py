"""The old plaintext copy on the NAS, handled from the console: the operator's choice, the encrypted
backup of it, the proof that the backup holds everything, and only then its removal.

WHAT IT IS. `osiris-vault/vault` and `osiris-vault/checkout-backups` on the NAS hold about 280 GB of
database dumps and a base backup in the clear. About 280 GB of dumps in earlier backups exist only
there, so deleting it loses history unless it is first backed up into the restic repository (which
is encrypted). Three choices, recorded as an operator-authorized decision:

  keep                 leave it; nothing runs.
  encrypt_then_remove  convert it into the repository's timeline (below), prove the snapshots
                       hold every file (path and size), and only then, on a second explicit
                       confirm, remove it.
  delete               remove it knowing the history that only it holds is lost; a typed
                       confirmation, and only after this choice was recorded.

CONVERTED, NOT JUST COPIED. The files are grouped by the day they were made (UTC) and each day
becomes its own snapshot dated that day (`restic backup --time`, the day's files only, from a
file list), under the hostname the real backups use and tagged `history-import` and
`import-day:<day>`: `restic snapshots` then shows the September dumps and the base backup on their
true dates, as if the backup had been running then. The tag is protected from any thinning
(src/orchestrator/restic_tags.py). A day already in the repository is skipped, so a stopped run
resumes at the first day not yet added.

HOW THE BACKUP RUNS. restic is installed on the NAS and the repository is a local directory there,
so the backup runs ON the NAS over ssh, reading local disk: no 280 GB crosses the network and
nothing is staged on this machine. The repository password goes to that process on ssh's stdin
(`--password-file /dev/stdin`), never in an argument, the environment, a log or a result. Like the
first offload it needs the home LAN (the route proof), holds the offload tick's lock, and is
resumable: restic skips what it already stored, so running it again after a drop carries on.

THE PROOF. After the backup, the new snapshot's file list is compared to the plaintext listing by
path and size; the result is written to the same coverage record the checklist reads
(`at_home.coverage_path`). Removal re-runs that comparison live and refuses unless nothing is only
in the copy. Removing never touches any other directory: the two names are fixed here, are never
taken from a request, and a symbolic link in their place is refused.

Every outside effect (ssh, the clock, the lock) is a function passed in, so the tests run the whole
job against fakes."""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
import subprocess
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.orchestrator.restic_tags import HISTORY_IMPORT_TAG as BACKUP_TAG

KEEP, ENCRYPT, DELETE = "keep", "encrypt_then_remove", "delete"
CHOICES = (KEEP, ENCRYPT, DELETE)
SOURCE_ROOT = "/mnt/Bunker/Vault/osiris-vault"
SOURCE_DIRS = ("vault", "checkout-backups")
TOTAL = 5
CONFIRM_REMOVE = "remove"
CONFIRM_DELETE = "delete the old plaintext copy"
_CHOICE_ENV = "OSIRIS_AT_HOME_PLAINTEXT_CHOICE_FILE"
_DEFAULT_CHOICE = "~/.local/state/osiris/plaintext_choice.json"
DAY_TAG_PREFIX = "import-day:"
LIST_FILE = "/tmp/osiris-history-import.list"
_PROGRESS_EVERY_SECONDS = 15.0
_CHECK_SUBSET = "1%"


# ---- the recorded choice ---------------------------------------------------------------------


def choice_path() -> Path:
    return Path(os.environ.get(_CHOICE_ENV) or _DEFAULT_CHOICE).expanduser()


def read_choice() -> dict[str, Any] | None:
    """The operator's recorded choice, or None when none is recorded (or it cannot be read: that
    must never read as permission)."""
    try:
        body = json.loads(choice_path().read_text())
    except (OSError, ValueError):
        return None
    return body if isinstance(body, dict) and body.get("choice") in CHOICES else None


def _write_json(path: Path, body: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    try:
        tmp.write_text(json.dumps(body))
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


async def record_choice(
    pool: Any, choice: object, *, now: datetime | None = None,
) -> dict[str, Any]:
    """Record the operator's choice as a decision (operator-authorized) and keep it where the
    removal route reads it. {"error": sentence} for a choice that is not one of the three."""
    from src.actions.core import Actions
    from src.orchestrator.capture import record_decision

    if choice not in CHOICES:
        return {"error": "the choice is keep, encrypt_then_remove or delete"}
    when = (now or datetime.now(UTC)).isoformat()
    meaning = {
        KEEP: "keep the old plaintext NAS copy as it is",
        ENCRYPT: "back the old plaintext NAS copy up into the restic repository, then remove it "
                 "once the backup is proved to hold every file",
        DELETE: "delete the old plaintext NAS copy, accepting that the history only it holds "
                "(dumps and a base backup from 2026-08-25 to 2026-09-21) is lost",
    }[str(choice)]
    decision = await record_decision(
        Actions(pool), f"The operator chose, in the console's At home panel: {meaning}.",
        kind="choice", operator_authorized=True,
        rationale="Recorded from the console's At home panel at the operator's own click. "
                  "Nothing is removed by this choice alone: removal needs a second confirm.",
        unlinked_because="an operator's choice made in the console, not about one repository",
        unlinked_because_kind="standalone")
    _write_json(choice_path(), {"choice": choice, "at": when, "decision": str(decision)})
    return {"choice": choice, "at": when, "decision": str(decision)}


# ---- the NAS side ----------------------------------------------------------------------------


def repo_path_on_box(url: str) -> str | None:
    """`sftp:truenas:/mnt/.../repo` -> `/mnt/.../repo`: the repository as the NAS itself sees it."""
    parts = url.split(":", 2)
    if len(parts) == 3 and parts[0] == "sftp" and parts[2].startswith("/"):
        return parts[2]
    return None


def ssh_host(url: str) -> str | None:
    from src.orchestrator.network_presence import sftp_host

    return sftp_host(url)


@dataclass
class Deps:
    """The outside world. The defaults are the real functions."""
    presence: Callable[[dict[str, Any]], dict[str, Any]]
    # (host, remote shell command, stdin bytes or None) -> lines of stdout as they arrive; the
    # return code is read from the `result` dict the call fills ({"rc": int, "err": str})
    stream: Callable[[str, str, bytes | None, dict[str, Any]], Iterator[str]]
    hold_lock: Callable[[], Any]
    write_coverage: Callable[[dict[str, Any]], None]
    read_choice: Callable[[], dict[str, Any] | None] = read_choice
    now: Callable[[], datetime] = field(default=lambda: datetime.now(UTC))


def _ssh_stream(
    host: str, command: str, stdin: bytes | None, result: dict[str, Any],
) -> Iterator[str]:
    """Run one command on the NAS and yield its stdout lines. The command is built here from fixed
    names; the only variable data (the password) goes on stdin."""
    proc = subprocess.Popen(
        ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "-o", "ServerAliveInterval=20",
         host, command],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=False)
    assert proc.stdin is not None and proc.stdout is not None and proc.stderr is not None
    try:
        with contextlib.suppress(BrokenPipeError):
            if stdin is not None:
                proc.stdin.write(stdin)
            proc.stdin.close()
        for raw in proc.stdout:
            yield raw.decode(errors="replace").rstrip("\n")
        err = proc.stderr.read().decode(errors="replace")
        result["rc"] = proc.wait()
        result["err"] = _tail(err)
    finally:
        if proc.poll() is None:
            proc.terminate()  # a closed stream (the page went away) must not leave it running


def _tail(text: str, n: int = 300) -> str:
    one = " ".join(text.split())
    return one[-n:]


def real_deps() -> Deps:
    from src.orchestrator import at_home
    from src.orchestrator.network_presence import lan_presence
    from src.orchestrator.offload_runner import hold_tick_lock

    return Deps(presence=lan_presence, stream=_ssh_stream, hold_lock=hold_tick_lock,
                write_coverage=lambda body: _write_json(at_home.coverage_path(), body))


def _restic(repo: str, *args: str) -> str:
    # `repo` is the path of a fixed directory the target names; it is quoted all the same.
    import shlex

    return " ".join(["restic", "-r", shlex.quote(repo), "--password-file", "/dev/stdin", *args])


def _in_root(command: str) -> str:
    import shlex

    return f"cd {shlex.quote(SOURCE_ROOT)} && {command}"


def _drain(
    deps: Deps, host: str, command: str, stdin: bytes | None = None,
) -> tuple[list[str], int, str]:
    result: dict[str, Any] = {}
    lines = list(deps.stream(host, command, stdin, result))
    return lines, int(result.get("rc", 1)), str(result.get("err", ""))


def plaintext_files(deps: Deps, host: str) -> dict[str, tuple[int, float]] | None:
    """Every regular file in the two directories as {relative path: (size, modified time)}, or None
    when the NAS could not say. A name with a tab or newline in it is not listed faithfully, so
    the listing refuses (None) rather than guess."""
    dirs = " ".join(SOURCE_DIRS)
    find = f"find {dirs} -type f -printf '%p\\t%s\\t%T@\\n'"
    lines, rc, _err = _drain(deps, host, _in_root(find))
    if rc != 0:
        return None
    out: dict[str, tuple[int, float]] = {}
    for line in lines:
        parts = line.split("\t")
        if len(parts) != 3 or not parts[1].isdigit():
            return None
        try:
            out[parts[0]] = (int(parts[1]), float(parts[2]))
        except ValueError:
            return None
    return out


def plaintext_listing(deps: Deps, host: str) -> dict[str, int] | None:
    """The same, as {relative path: size}."""
    files = plaintext_files(deps, host)
    return None if files is None else {p: sz for p, (sz, _t) in files.items()}


def snapshot_listing(deps: Deps, host: str, repo: str, password: bytes,
                     snapshots: str | list[str]) -> dict[str, int] | None:
    """The files the named snapshots hold under the two directories, as {relative path: size}
    (the union, when several are named)."""
    ids = [snapshots] if isinstance(snapshots, str) else list(snapshots)
    if not ids:
        return {}
    lines, rc, _err = _drain(deps, host, _restic(repo, "ls", "--json", "--no-lock", *ids),
                             password + b"\n")
    if rc != 0:
        return None
    prefix = SOURCE_ROOT + "/"
    out: dict[str, int] = {}
    for line in lines:
        try:
            node = json.loads(line)
        except ValueError:
            continue
        if isinstance(node, dict) and node.get("type") == "file":
            path = str(node.get("path") or "")
            if path.startswith(prefix):
                out[path[len(prefix):]] = int(node.get("size") or 0)
    return out


def compare(plain: dict[str, int], held: Iterable[tuple[str, int]]) -> dict[str, Any]:
    """Which files exist only in the plaintext copy: a path whose size the backup does not hold."""
    have = set(held)
    only = sorted(p for p, s in plain.items() if (p, s) not in have)
    only_bytes = sum(plain[p] for p in only)
    return {"only_files": len(only), "only_bytes": only_bytes, "only": only[:20],
            "files": len(plain), "bytes": sum(plain.values())}


def _fmt_gb(n: float) -> str:
    return f"{n / 1e9:.1f} GB"


async def _route_proof(deps: Deps, target: dict[str, Any], stop: Callable[[int, str, str], int],
                       say: Callable[[str], None]) -> bool | int:
    seen = await asyncio.to_thread(deps.presence, target)
    if not seen.get("present"):
        return stop(1, f"not on the home network ({seen.get('reason') or 'route unknown'}).",
                    "run this again when you are home on the LAN, not over Tailscale.")
    if not seen.get("dev"):
        return stop(1, "could not confirm which interface the route to the NAS leaves through; "
                       "not going without that proof.",
                    "check `ip route get` for the NAS address, then run this again.")
    say(f"[1/{TOTAL}] home network: the route to the NAS leaves through {seen['dev']} and the "
        "NAS answers there.")
    return True


async def encrypt(
    target: dict[str, Any], password: bytes, deps: Deps, *, say: Callable[[str], None],
) -> int:
    """Turn the plaintext copy into backdated snapshots in the repository, so the history shows in
    `restic snapshots` on the days it was made, and prove they hold every file. 0 when they do,
    1 at the first step that did not. It never removes anything."""
    url = str(target.get("path_or_url") or "")
    host, repo = ssh_host(url), repo_path_on_box(url)

    def stop(n: int, why: str, what_next: str) -> int:
        say(f"[{n}/{TOTAL}] STOPPED: {why}")
        say(f"        next: {what_next}")
        return 1

    chosen = deps.read_choice()
    if not chosen or chosen.get("choice") != ENCRYPT:
        return stop(1, "the choice to back this copy up first is not recorded.",
                    "choose 'back it up, then remove it' first.")
    if host is None or repo is None:
        return stop(1, "the NAS target is not an sftp address this can run against.",
                    "check the NAS target in the backup settings.")
    proved = await _route_proof(deps, target, stop, say)
    if proved is not True:
        return int(proved)
    with deps.hold_lock() as locked:
        if not locked:
            return stop(1, "an offload or restore test is running right now.",
                        "wait a few minutes (the timers take about 15 minutes) and run this again.")
        # 2. what there is to back up, by the day it was made
        files = await asyncio.to_thread(plaintext_files, deps, host)
        if files is None:
            return stop(2, "the NAS did not list the plaintext copy (or a file name could not be "
                           "read faithfully).",
                        "check the NAS is on and reachable, then run this again.")
        plain = {p: sz for p, (sz, _t) in files.items()}
        days = group_by_day(files)
        say(f"[2/{TOTAL}] the plaintext copy holds {len(plain)} files, "
            f"{_fmt_gb(sum(plain.values()))}, from {len(days)} different days.")
        # 3. one snapshot per day, dated that day, on the NAS
        say(f"[3/{TOTAL}] adding one snapshot per day, dated the day the files were made, on the "
            "NAS itself (it reads local disk, so nothing crosses the network; days already "
            "added are skipped, so a stop here can be run again).")
        snaps, fail = await asyncio.to_thread(_import_history, deps, host, repo, password,
                                              days, say)
        if fail or snaps is None:
            return stop(3, f"the import did not finish ({fail or 'no snapshot was reported'}).",
                        "run this again: it resumes at the first day not yet added.")
        say(f"[3/{TOTAL}] the history is in the repository: {len(snaps)} snapshots, "
            f"{min(days)} to {max(days)}.")
        # 4. the proof
        held = await asyncio.to_thread(snapshot_listing, deps, host, repo, password,
                                       sorted(snaps.values()))
        if held is None:
            return stop(4, "the repository would not list the imported snapshots.",
                        "run this again; if it repeats, read `restic ls` on the NAS by hand.")
        verdict = compare(plain, held.items())
        newest = snaps[max(snaps)]
        deps.write_coverage({
            "only_files": verdict["only_files"], "only_bytes": verdict["only_bytes"],
            "files_checked": verdict["files"], "bytes_checked": verdict["bytes"],
            "snapshot": newest, "snapshots": sorted(snaps.values()), "days": len(snaps),
            "tag": BACKUP_TAG, "checked_at": deps.now().isoformat(),
            "method": "plaintext listing on the NAS compared to the imported snapshots, "
                      "path and size"})
        if verdict["only_files"]:
            return stop(4, f"{verdict['only_files']} file(s) are in the plaintext copy and not in "
                           f"the snapshots (first: {verdict['only'][0]}).",
                        "do not remove anything; run this again and read the first name.")
        say(f"[4/{TOTAL}] proved: all {verdict['files']} files ({_fmt_gb(verdict['bytes'])}) are "
            f"in the {len(snaps)} snapshots, path and size. Nothing is only in the plaintext copy.")
        # 5. the repository still reads back
        say(f"[5/{TOTAL}] reading back a sample of the repository's data to check it is whole "
            "(a few minutes).")
        _lines, rc, err = await asyncio.to_thread(
            _drain, deps, host, _restic(repo, "check", f"--read-data-subset={_CHECK_SUBSET}"),
            password + b"\n")
        if rc != 0:
            return stop(5, f"the repository check failed ({err or 'no reason given'}).",
                        "the backup is stored but NOT proved readable; do not remove the "
                        "plaintext copy until this passes.")
        say(f"[5/{TOTAL}] the repository check passed.")
    say("done: the history is in the backup, on the days it was made, and it holds everything in "
        "the plaintext copy. Removing the copy is a separate step that waits for your "
        "confirmation.")
    return 0


def group_by_day(files: dict[str, tuple[int, float]]) -> dict[str, dict[str, Any]]:
    """The files by the UTC day they were last modified: {day: {"files": [paths], "at": the
    newest modified time that day as "YYYY-MM-DD HH:MM:SS"}}. The snapshot is dated its day's
    newest file, as if the backup had run just after it was written."""
    days: dict[str, dict[str, Any]] = {}
    for path, (_size, mtime) in sorted(files.items()):
        when = datetime.fromtimestamp(mtime, UTC)
        day = days.setdefault(when.strftime("%Y-%m-%d"), {"files": [], "t": 0.0})
        day["files"].append(path)
        day["t"] = max(float(day["t"]), mtime)
    return {d: {"files": v["files"],
                "at": datetime.fromtimestamp(v["t"], UTC).strftime("%Y-%m-%d %H:%M:%S")}
            for d, v in days.items()}


def _existing_imports(deps: Deps, host: str, repo: str,
                      password: bytes) -> tuple[dict[str, str], str] | None:
    """The days already imported ({day: snapshot id}, from the per-day tag) and the host name the
    real backups use, or None when the repository would not list."""
    lines, rc, _err = _drain(deps, host, _restic(repo, "snapshots", "--json", "--no-lock"),
                             password + b"\n")
    if rc != 0:
        return None
    try:
        rows = json.loads("".join(lines) or "[]")
    except ValueError:
        return None
    done: dict[str, str] = {}
    names: dict[str, int] = {}
    for row in rows if isinstance(rows, list) else []:
        tags = list(row.get("tags") or [])
        if BACKUP_TAG in tags:
            for t in tags:
                if t.startswith(DAY_TAG_PREFIX):
                    sid = str(row.get("short_id") or row.get("id", ""))[:8]
                    done[t[len(DAY_TAG_PREFIX):]] = sid
        elif row.get("hostname"):
            names[row["hostname"]] = names.get(row["hostname"], 0) + 1
    name = max(names, key=lambda n: names[n]) if names else "osiris"
    return done, name


def _import_history(
    deps: Deps, host: str, repo: str, password: bytes, days: dict[str, dict[str, Any]],
    say: Callable[[str], None],
) -> tuple[dict[str, str] | None, str | None]:
    """Add one snapshot per day (the day's files only, dated that day, tagged `history-import` and
    `import-day:<day>`, under the hostname the real backups use so it sits in their timeline).
    Returns ({day: snapshot id} for every day, None) or (None, why). A day already in the
    repository is skipped, which is what makes a stopped run resumable."""
    import shlex
    import time

    known = _existing_imports(deps, host, repo, password)
    if known is None:
        return None, "the repository would not list its snapshots"
    snaps, hostname = dict(known[0]), known[1]
    todo = [d for d in sorted(days) if d not in snaps]
    last = 0.0
    for n, day in enumerate(todo, 1):
        listing = "".join(f"{SOURCE_ROOT}/{p}\n" for p in days[day]["files"]).encode()
        wrote = _drain(deps, host, f"umask 077 && cat > {LIST_FILE}", listing)
        if wrote[1] != 0:
            return None, f"could not write the file list on the NAS ({wrote[2] or 'no reason'})"
        command = ("TZ=UTC " + _restic(
            repo, "backup", "--json", "--tag", BACKUP_TAG, "--tag", DAY_TAG_PREFIX + day,
            "--host", shlex.quote(hostname), "--time", shlex.quote(days[day]["at"]),
            "--files-from-verbatim", LIST_FILE))
        result: dict[str, Any] = {}
        snap: str | None = None
        for line in deps.stream(host, command, password + b"\n", result):
            try:
                msg = json.loads(line)
            except ValueError:
                continue
            if not isinstance(msg, dict):
                continue
            if msg.get("message_type") == "summary":
                snap = str(msg.get("snapshot_id") or "")[:8] or snap
            elif (msg.get("message_type") == "status"
                  and time.monotonic() - last >= _PROGRESS_EVERY_SECONDS):
                last = time.monotonic()
                pct = float(msg.get("percent_done") or 0) * 100
                say(f"        day {n} of {len(todo)} ({day}): {pct:.0f}% "
                    f"({_fmt_gb(float(msg.get('bytes_done') or 0))} of "
                    f"{_fmt_gb(float(msg.get('total_bytes') or 0))})")
        rc = int(result.get("rc", 1))
        if rc not in (0, 3):  # 3: some files unreadable; the snapshot exists and the proof counts
            _drain(deps, host, f"rm -f {LIST_FILE}")
            return None, f"{day}: " + str(result.get("err") or f"restic exited {rc}")
        if not snap:
            _drain(deps, host, f"rm -f {LIST_FILE}")
            return None, f"{day}: restic reported no snapshot"
        snaps[day] = snap
        say(f"        day {n} of {len(todo)} ({day}): added snapshot {snap}.")
    _drain(deps, host, f"rm -f {LIST_FILE}")
    return {d: snaps[d] for d in days}, None


# ---- removing --------------------------------------------------------------------------------


async def remove(
    target: dict[str, Any], password: bytes, deps: Deps, *, confirm: object,
    say: Callable[[str], None],
) -> int:
    """Remove the plaintext copy. Needs the recorded choice and its typed confirmation; under
    `encrypt_then_remove` it also re-proves, live, that nothing is only in the copy. 0 when the two
    directories are gone, 1 when anything stopped it (nothing is removed then)."""
    url = str(target.get("path_or_url") or "")
    host, repo = ssh_host(url), repo_path_on_box(url)

    def stop(why: str, what_next: str) -> int:
        say(f"STOPPED: {why}")
        say(f"        next: {what_next}")
        return 1

    chosen = deps.read_choice()
    mode = chosen.get("choice") if chosen else None
    if mode not in (ENCRYPT, DELETE):
        return stop("the choice to remove this copy is not recorded.",
                    "choose 'back it up, then remove it' or 'delete it' first.")
    phrase = CONFIRM_REMOVE if mode == ENCRYPT else CONFIRM_DELETE
    if not isinstance(confirm, str) or confirm.strip().lower() != phrase:
        return stop(f"removing needs its confirmation typed exactly: {phrase!r}.",
                    "type it to confirm, or leave the copy where it is.")
    if host is None or repo is None:
        return stop("the NAS target is not an sftp address this can run against.",
                    "check the NAS target in the backup settings.")
    seen = await asyncio.to_thread(deps.presence, target)
    if not seen.get("present") or not seen.get("dev"):
        return stop(f"not on the home network ({seen.get('reason') or 'route unproven'}).",
                    "run this when you are home on the LAN.")
    with deps.hold_lock() as locked:
        if not locked:
            return stop("an offload or restore test is running right now.",
                        "wait a few minutes and try again.")
        if mode == ENCRYPT:
            cover = _read_coverage_snapshots()
            if cover is None:
                return stop("no backup of this copy is recorded as proved.",
                            "run the backup first.")
            plain = await asyncio.to_thread(plaintext_listing, deps, host)
            held = await asyncio.to_thread(snapshot_listing, deps, host, repo, password, cover)
            if plain is None or held is None:
                return stop("the NAS or the repository would not list the files to compare.",
                            "check the NAS is reachable and try again; nothing was removed.")
            verdict = compare(plain, held.items())
            if verdict["only_files"]:
                return stop(f"{verdict['only_files']} file(s) are in the plaintext copy and not in "
                            f"the imported snapshots (first: {verdict['only'][0]}).",
                            "nothing was removed; run the backup again.")
            say(f"checked just now: all {verdict['files']} files are in the {len(cover)} "
                f"imported snapshots.")
        say("removing the two directories on the NAS (nothing else is touched).")
        dirs = " ".join(SOURCE_DIRS)
        tests = " && ".join(f"test -d {d} && test ! -L {d}" for d in SOURCE_DIRS)
        gone = " && ".join(f"test ! -e {d}" for d in SOURCE_DIRS)
        command = _in_root(f"{tests} && rm -rf -- {dirs} && {gone} && echo removed")
        lines, rc, err = await asyncio.to_thread(_drain, deps, host, command)
        if rc != 0 or "removed" not in lines:
            why = err or "a directory was missing or a link"
            return stop(f"the NAS did not remove them ({why}).",
                        "look at the NAS by hand; nothing else was touched.")
    say("done: the old plaintext copy is gone.")
    return 0


def _read_coverage_snapshots() -> list[str] | None:
    """The snapshots the last proof named, only when that proof found nothing left over."""
    from src.orchestrator import at_home

    cover = at_home.read_coverage()
    if cover is None or int(cover.get("only_files") or 0) != 0:
        return None
    snaps = cover.get("snapshots") or ([cover["snapshot"]] if cover.get("snapshot") else [])
    return [str(x) for x in snaps] or None


async def close_thread(pool: Any) -> None:
    """The removal is done: close the obligation that held it (the checklist reads it as done)."""
    from src.actions.core import Actions
    from src.orchestrator.at_home import PLAINTEXT_COPY_THREAD
    from src.orchestrator.capture import resolve_thread

    prefix = PLAINTEXT_COPY_THREAD.split(":", 1)[1]
    full = await pool.fetchval(
        "SELECT id::text FROM objects WHERE type='Thread' AND id::text LIKE $1 || '%' LIMIT 1",
        prefix)
    if full:
        await resolve_thread(Actions(pool), str(full),
                             because="the old plaintext NAS copy was removed from the console")
