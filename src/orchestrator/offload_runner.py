"""THE OPPORTUNISTIC OFFLOAD RUNNER (THE BACKUP TOPOLOGY / INTERMITTENT TARGETS): this box
is a laptop, the 8TB drive is present only when docked, the NAS only on Tailscale/LAN. This
runner checks each configured `offload_targets[]` entry's presence on every tick, syncs the
vault to every target that IS present via restic (never auto-mounts, never blocks on an
absent target, never fails the local backup ladder: every failure here is caught and
recorded, never raised past `run_offload_tick`'s own boundary), and records
last-successful-offload per target to a local receipt file. `compositions.py`'s own
`_fn_backup_status` (`offbox_section`) reads this back directly, the same "live filesystem
read, never a DB write" pattern every other section of that Function already holds, rather
than routing per-tick telemetry through `backup.offload_targets`'s own settings-authority
route (that route is `operator_or_ruling` + `requires_because`: correctly heavy for a HUMAN
reconfiguring a target, wrong for an unattended timer's own routine write).

CREDENTIALS: `restic_credential.get_restic_password()`, the same systemd-creds custody built
for the soul-store key, reused unchanged for the restic repository password. The password is
set directly as the `RESTIC_PASSWORD` subprocess env var, never a temp file, never a CLI
argument (visible via `ps`), matching `osiris_offbox_backup.sh`'s own long-standing rule.

PRESENCE: 'local' targets use `backup_validation.check_local_target_presence`
(findmnt against `expected_mountpoint`) exactly as the read-only settings route
already does. 'restic' targets (the NAS, over sftp) have no local mountpoint to
check: presence there means "an attempt to reach it, bounded by a real timeout,
either succeeds or doesn't"; a reachability failure is treated identically to an
absent mountpoint (skip, record, never raise), the same "an absent target is the
expected common case, never an error" law `check_local_target_presence`'s own
docstring states for 'local'.

AT HOME ONLY: a network target counts as present only while the route to it leaves through a
local interface (`network_presence.lan_presence`). Away from home the NAS still answers, but
over the VPN, and a full upload over a tunnel is what the operator's rule forbids; such a
target is skipped quietly with a receipt reason ("reachable only over tailscale0"), and a
target with `allow_tunnel: true` opts out of the check."""
from __future__ import annotations

import asyncio
import contextlib
import fcntl
import json
import os
import subprocess
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncpg

_RECEIPTS_ENV = "OSIRIS_OFFLOAD_RECEIPTS_FILE"
_DEFAULT_RECEIPTS_FILE = "~/.local/state/osiris/offload_receipts.json"
# a full vault sync; osiris_offbox_restore_drill.py's own restore step uses the
# same order-of-magnitude ceiling for the same reason (a real off-box transfer,
# not a near-instant local op)
_RESTIC_TIMEOUT_SECONDS = 1800


def _receipts_path() -> Path:
    env = os.environ.get(_RECEIPTS_ENV)
    if env:
        return Path(env).expanduser()
    return Path(_DEFAULT_RECEIPTS_FILE).expanduser()


def _read_receipts() -> dict[str, Any]:
    return _load_receipts()[0]


def _quarantine_path() -> Path:
    path = _receipts_path()
    return path.with_name(path.name + ".unreadable")


def _load_receipts() -> tuple[dict[str, Any], bool]:
    """The receipts and whether they are UNTRUSTWORTHY: a file that exists but does not parse
    (half-written, damaged), or one that did not parse earlier and was set aside. Receipts are what
    says a target has offloaded before, so an unreadable file must never read as 'never offloaded'
    (that is the one fact that stands between a bad day and `restic init` on a repository that
    exists)."""
    path = _receipts_path()
    try:
        text = path.read_text()
    except FileNotFoundError:
        return {}, _quarantine_path().exists()
    except OSError:
        return {}, True
    try:
        return dict(json.loads(text)), _quarantine_path().exists()
    except (ValueError, TypeError):
        return {}, True


def _set_aside_unreadable_receipts() -> None:
    """Keep a receipts file that does not parse (never overwrite it): the copy is both the
    evidence and the standing mark that this box can no longer prove what it offloaded."""
    path = _receipts_path()
    quarantine = _quarantine_path()
    if quarantine.exists():
        return
    with contextlib.suppress(OSError):
        os.replace(path, quarantine)


def _write_receipt(name: str, receipt: dict[str, Any]) -> None:
    """Merges `receipt`'s fields into the existing per-target record rather than
    replacing it wholesale: a failed tick must never CLOBBER a real prior
    `last_successful_offload` with None; it only ever adds `last_attempt_at`/
    `last_error` alongside whatever succeeded most recently."""
    path = _receipts_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    receipts, untrusted = _load_receipts()
    if untrusted and path.exists():
        _set_aside_unreadable_receipts()
    existing = receipts.get(name, {})
    if receipt.get("last_successful_offload"):
        # the moment this target FIRST held a good backup, kept for good (a receipt written
        # before this field existed starts from its own last success): the restore test only
        # counts as done when it passed after this
        existing.setdefault(
            "first_successful_offload",
            existing.get("last_successful_offload") or receipt["last_successful_offload"])
    existing.update(receipt)
    receipts[name] = existing
    # temp file then rename: a crash or a stopped run leaves the old file or the new one, never half
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    try:
        tmp.write_text(json.dumps(receipts, indent=2))
        os.replace(tmp, path)
    finally:
        with contextlib.suppress(OSError):
            tmp.unlink()


def _note_skip(name: str, reason: str, now: str) -> None:
    """A quiet skip: why, when, and when this run of skips began (kept while the same
    skipping continues, cleared by the next attempt)."""
    existing = _read_receipts().get(name) or {}
    began = existing.get("skipped_since") if existing.get("last_skip_reason") else None
    _write_receipt(name, {"last_skip_reason": reason, "last_skip_at": now,
                          "skipped_since": began or now})


def _ensure_tracked(name: str, now: str) -> None:
    """The first time a tick sees a target enabled, remember it: that is the clock a target
    that has never succeeded is judged from (offload_staleness)."""
    if not (_read_receipts().get(name) or {}).get("tracked_since"):
        _write_receipt(name, {"tracked_since": now})


def offload_receipts() -> dict[str, Any]:
    """Public read entry point for `compositions._fn_backup_status`'s own `offbox_section`,
    never writes, mirrors `_read_receipts` under a name that doesn't look
    private to an external caller."""
    return _read_receipts()


# What restic prints when there is positively no repository at the location. Matched narrowly and
# only together with the absence of any sign that the failure was something else: a repository
# that exists but could not be read (a slow link, a lock, a wrong password, a dropped session)
# must NEVER be initialised over.
_MISSING_REPO = ("repository does not exist", "is there a repository at the following location")
_MISSING_CONFIG = ("unable to open config file",)
_MISSING_WHY = ("no such file or directory", "does not exist", "file does not exist",
                "not exist")
_NOT_MISSING = (
    "timed out", "timeout", "i/o timeout", "connection", "no route", "network", "dial",
    "broken pipe", "ssh:", "permission denied", "authentication", "handshake",
    "wrong password", "no key found", "locked", "unable to create lock", "unexpected eof",
    "host key", "name resolution", "unreachable",
)


def repository_is_missing(output: str, returncode: int | None = None) -> bool:
    """True only when restic says the repository does not exist, and nothing in its output points
    at a transient or access failure instead. restic's exit code is the primary signal (10 missing;
    11 locked and 12 wrong password mean it EXISTS); the output text decides when there is no
    usable code (an older restic, or a code that was not kept)."""
    from src.orchestrator import restic_locks

    text = output.lower()
    if returncode in (restic_locks.EXIT_REPOSITORY_LOCKED, restic_locks.EXIT_WRONG_PASSWORD):
        return False
    if any(sign in text for sign in _NOT_MISSING):
        return False
    if returncode == restic_locks.EXIT_REPOSITORY_MISSING:
        return True
    if any(sign in text for sign in _MISSING_REPO):
        return True
    return (any(sign in text for sign in _MISSING_CONFIG)
            and any(sign in text for sign in _MISSING_WHY))


def _one_line(raw: bytes) -> str:
    from src.orchestrator.restic_locks import failure_summary

    return failure_summary(raw.decode(errors="replace"))


# A backup that is stopped is given this long to finish its current pack, write its index and
# release its lock before the SIGKILL: over a slow link the default few seconds are not enough, and
# a kill mid-upload is what leaves the exclusive-looking lock the next run then has to untangle.
_BACKUP_STOP_GRACE_SECONDS = 90
_RESTIC_INCOMPLETE_SNAPSHOT = 3  # restic backup: the snapshot was created, some files unreadable
_PROBE_TIMEOUT_SECONDS = 30  # restic snapshots and init: a slow link must not hang the tick


def _run_restic_backup(
    *, repository: str, password: bytes, source: Path, init_if_needed: bool = True,
    notes: list[str] | None = None, no_init_reason: str | None = None,
) -> str | None:
    """Returns a failure string, or None on success, same return convention as
    `osiris_offbox_restore_drill.run_drill`.

    `restic init` is reachable only when `restic snapshots` positively says the repository does
    not exist (`repository_is_missing`) AND the caller allows it (`init_if_needed`; the tick
    turns it off for a target that has ever offloaded successfully). A timeout, a lock, a network
    or an authentication failure records its own error and skips the target: initialising over
    a repository that merely could not be read is how a slow link once tried to re-create the
    NAS repository (restic refused, but only because the config file was there).

    restic exits 3 when it could not read some source files but still wrote the snapshot: that is
    a success, and the caller gets the reason through `notes` (a list the call appends to)."""
    from src.orchestrator import restic_locks

    env = {**os.environ, "RESTIC_REPOSITORY": repository, "RESTIC_PASSWORD": password.decode()}
    try:
        # a run killed mid-flight leaves a lock; clear stale ones first (a stale exclusive lock
        # would otherwise make the probe below fail), and say plainly who holds the repository
        # when an exclusive lock is live (shared locks do not stop a backup)
        outcome = restic_locks.clear_stale_locks(env, blocking_only_exclusive=True)
        if outcome.busy:
            return outcome.busy

        def snapshots() -> Any:
            return restic_locks.run_restic(
                ["snapshots"], env, timeout=_PROBE_TIMEOUT_SECONDS, text=False)

        probe = snapshots()
        if probe.returncode != 0:
            detail = (probe.stdout + b"\n" + probe.stderr).decode(errors="replace")
            if restic_locks.is_lock_failure(detail, probe.returncode):
                # The pre-check let a lock through (or could not read the listing): look again
                # now that we KNOW the repository is locked, clear what is stale, retry once.
                again = restic_locks.clear_stale_locks(
                    env, blocking_only_exclusive=True, after_lock_failure=True)
                if again.busy:
                    return again.busy
                if again.cleared or again.note:
                    probe = snapshots()
                    detail = (probe.stdout + b"\n" + probe.stderr).decode(errors="replace")
            if probe.returncode != 0 and restic_locks.is_lock_failure(detail, probe.returncode):
                held = restic_locks.holder_line(detail) or _one_line(probe.stderr)
                why = f" (lock listing: {outcome.unreadable})" if outcome.unreadable else ""
                return (f"{restic_locks.BUSY_PREFIX} (locked by another run, nothing was "
                        f"initialized): {held}{why}")
            if probe.returncode != 0 and not repository_is_missing(detail, probe.returncode):
                return f"restic snapshots failed, not initializing: {_one_line(probe.stderr)}"
        if probe.returncode != 0:
            if not init_if_needed:
                return no_init_reason or (
                    "the repository looks missing but this target has offloaded before; "
                    "not initializing over it (is the target reachable?)")
            init = restic_locks.run_restic(
                ["init"], env, timeout=_PROBE_TIMEOUT_SECONDS, text=False)
            if init.returncode != 0:
                return f"restic init failed: {init.stderr.decode(errors='replace').strip()}"
        backup = restic_locks.run_restic(
            ["backup", str(source), "--exclude=*.tmp"], env,
            timeout=_RESTIC_TIMEOUT_SECONDS, text=False, grace=_BACKUP_STOP_GRACE_SECONDS)
        if backup.returncode == _RESTIC_INCOMPLETE_SNAPSHOT:
            if notes is not None:
                notes.append("snapshot saved, but restic could not read some source files: "
                             + (_one_line(backup.stderr) or "see restic output"))
        elif backup.returncode != 0:
            return f"restic backup failed: {backup.stderr.decode(errors='replace').strip()}"
        return None
    except subprocess.TimeoutExpired as exc:
        step = exc.cmd[1] if isinstance(exc.cmd, list) and len(exc.cmd) > 1 else "restic"
        head = f"restic {step} timed out after {exc.timeout:.0f}s (a slow or unreachable link); "
        if step == "backup":
            # a stopped backup keeps the data it already stored: the next run carries on from there
            return head + "what it already copied is kept and the next run will resume"
        if step == "init":
            return head + "the repository may or may not exist now; the next run checks first"
        return head + "nothing was initialized"
    except OSError as exc:
        return f"{type(exc).__name__}: {exc}"


def _lock_path() -> Path:
    return _receipts_path().with_name("offload_tick.lock")


@contextlib.contextmanager
def hold_tick_lock() -> Iterator[bool]:
    """Try to take the tick's own file lock (non-blocking); yields whether it was taken and
    releases it on the way out. The scheduled restore test uses the same lock as the tick, so the
    two never run in the repository together."""
    lock_path = _lock_path()
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            yield False
            return
        try:
            yield True
        finally:
            with contextlib.suppress(OSError):
                fcntl.flock(handle, fcntl.LOCK_UN)


async def run_offload_tick(pool: asyncpg.Pool, *, vault: Path | None = None) -> dict[str, Any]:
    """One tick, never two at once: a second tick (the 15 minute timer firing while a tick
    started by hand, or a long first copy, is still running) would start a second restic
    backup into the same repository. The tick holds an exclusive lock on a file beside the
    receipts for its whole run; a tick that cannot take it returns at once with
    `skipped: "already running"` and does nothing. The lock belongs to the process, so a
    crashed tick never leaves it held."""
    lock_path = _lock_path()
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return {"skipped": "already running", "targets": []}
        try:
            out = await _run_offload_tick_locked(pool, vault=vault)
            told = await _surface_stale(pool)
            if told:
                out["stale_targets"] = told
            return out
        finally:
            with contextlib.suppress(OSError):
                fcntl.flock(handle, fcntl.LOCK_UN)


async def _surface_stale(pool: asyncpg.Pool) -> list[str]:
    """Tell the desk, once, about every enabled target that has gone the stale limit (the
    `backup.offload_stale_days` setting, default 7) without a successful offload, naming why.
    "Once" is per stretch of silence: the receipt remembers which clock it told about, and a
    successful offload clears it. A limit of 0 turns this off. Never fails the tick."""
    import logging

    from src.orchestrator.backup_settings import get_backup_settings
    from src.orchestrator.mailbox import send_message
    from src.orchestrator.offload_staleness import stale_offload_targets, stale_sentence
    from src.orchestrator.settings_service import settings_with_overlay

    told: list[str] = []
    try:
        limit = int((await settings_with_overlay(pool)).osiris_offload_stale_days)
        if limit <= 0:
            return told
        targets = (await get_backup_settings(pool)).get("offload_targets") or []
        receipts = _read_receipts()
        stale = stale_offload_targets(targets, receipts, now=datetime.now(UTC), stale_days=limit)
        for entry in stale:
            if (receipts.get(entry["name"]) or {}).get("stale_notified_clock") == entry["clock"]:
                continue
            sentence = stale_sentence(entry, limit)
            physical = "since" in entry["reason"] and "attempt failed" not in entry["reason"]
            await send_message(
                pool, from_agent="cron:offload_runner", from_project="osiris",
                to_project="operator", body=f"offload: {sentence}",
                desk_kind="hands" if physical else "fyi", dedup_window_secs=3600)
            _write_receipt(entry["name"], {"stale_notified_clock": entry["clock"]})
            told.append(sentence)
    except Exception:  # noqa: BLE001 - telling the desk must never fail the tick
        logging.getLogger(__name__).warning("could not surface stale offload targets",
                                            exc_info=True)
    return told


async def _run_offload_tick_locked(
    pool: asyncpg.Pool, *, vault: Path | None = None,
) -> dict[str, Any]:
    """ONE TICK, meant to be `osiris-offload.timer`'s own `ExecStart`, opportunistic
    (never blocks, never fails the caller): for every ENABLED `offload_targets[]`
    row that is PRESENT right now, runs one restic backup, records a receipt.
    Absent/disabled targets are reported, never treated as an error. A missing
    restic password (`ResticPasswordMissing`) degrades the WHOLE tick with one
    `error` key (nothing to back up onto without it) rather than a confusing
    per-target failure repeated N times.

    BEFORE any backup, the security-key recovery file is copied to the vault and, as a plain
    file, beside every present target (`recovery_copies`, reported under `recovery_copies`,
    never an error for the tick). The restore test is NOT run here: it re-reads data and
    would delay the next tick, so it has its own timer (`scheduled_drill.run_drill_pass`,
    `osiris offload-runner drill`)."""
    from src.orchestrator import recovery_copies, restic_locks
    from src.orchestrator.backup_settings import get_backup_settings
    from src.orchestrator.backup_validation import check_local_target_presence
    from src.orchestrator.network_presence import is_unreachable_error, lan_presence
    from src.orchestrator.restic_credential import ResticPasswordMissing, get_restic_password

    settings = await get_backup_settings(pool)
    targets = settings.get("offload_targets") or []
    source = vault or Path(os.environ.get("OSIRIS_VAULT", str(Path.home() / "osiris-vault")))
    now = datetime.now(UTC).isoformat()

    present: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    for target in targets:
        if not isinstance(target, dict):
            continue
        name = target.get("name", "<unnamed>")
        if not target.get("enabled"):
            results.append({"name": name, "skipped": "disabled"})
            continue
        if target.get("kind") == "local":
            mountpoint = target.get("expected_mountpoint") or ""
            presence = await asyncio.to_thread(check_local_target_presence, mountpoint)
            if not presence.get("present"):
                reason = "not present (mountpoint absent)"
                _note_skip(name, reason, now)
                results.append({"name": name, "skipped": reason})
                continue
        else:
            lan = await asyncio.to_thread(lan_presence, target)
            if not lan.get("present"):
                reason = str(lan.get("reason"))
                # a quiet skip, not a failure: it must never clobber a real last success,
                # and it adds no `last_error` for the readiness view to report
                _note_skip(name, reason, now)
                results.append({"name": name, "skipped": reason})
                continue
        present.append(target)
    for target in targets:
        if isinstance(target, dict) and target.get("enabled"):
            _ensure_tracked(str(target.get("name", "<unnamed>")), now)

    extra: dict[str, Any] = {}
    copies = await recovery_copies.sync_recovery_copies(present, source)
    if copies:
        extra["recovery_copies"] = copies

    try:
        password = get_restic_password()
    except ResticPasswordMissing as exc:
        return {"error": str(exc), "targets": [], **extra}

    for target in present:
        name = target.get("name", "<unnamed>")
        repository = target.get("path_or_url", "")
        # a target that has ever offloaded has a repository: never initialise over it
        receipts, untrusted = _load_receipts()
        ever_offloaded = untrusted or bool(
            (receipts.get(name) or {}).get("last_successful_offload"))
        why_not = None
        if untrusted and not (receipts.get(name) or {}).get("last_successful_offload"):
            why_not = (
                f"the repository looks missing, but the offload receipts could not be read "
                f"({_quarantine_path()} holds the damaged copy), so this box cannot tell whether "
                f"{name} has offloaded before; not initializing. If {name} is a new target, run "
                f"`restic init` on it by hand; once the receipts are sound again, remove that "
                "file to let first-time setup run on its own")
        notes: list[str] = []
        fail = await asyncio.to_thread(
            _run_restic_backup, repository=repository, password=password, source=source,
            init_if_needed=not ever_offloaded, notes=notes, no_init_reason=why_not)
        if fail is None:
            _write_receipt(name, {"last_successful_offload": now, "last_attempt_at": now,
                                  "last_error": None, "last_skip_reason": None,
                                  "skipped_since": None, "stale_notified_clock": None,
                                  "last_note": notes[0] if notes else None})
            results.append({"name": name, "ok": True, **({"note": notes[0]} if notes else {})})
        elif restic_locks.is_busy(fail):
            # Somebody else holds the repository right now (the restore test, a backup started by
            # hand, another host). Nothing failed: a quiet skip, no last_error, no clobbered last
            # success. A lock that never lets go still surfaces, through the stale-offload notice.
            _note_skip(name, str(fail), now)
            results.append({"name": name, "skipped": str(fail)})
        elif is_unreachable_error(fail):
            # The network had no way to the target (away from home, a tunnel down): the
            # backup could not run, which says nothing about the repository. A quiet skip,
            # the same as the presence check's own: it never clobbers a real last success
            # and adds no last_error for the readiness view to report.
            reason = "target not reachable from this network"
            _note_skip(name, reason, now)
            results.append({"name": name, "skipped": reason})
        else:
            # a 'restic' target's own unreachability surfaces here identically to a
            # genuine backup failure: this runner has no separate network-reachable
            # probe for restic targets (the standing rule in this domain is never a
            # network call outside the real operation itself),
            # so "tried and failed" is the only signal a sftp/NAS target ever gets.
            _write_receipt(name, {"last_attempt_at": now, "last_error": fail,
                                  "last_skip_reason": None, "skipped_since": None})
            results.append({"name": name, "ok": False, "error": fail})

    return {"as_of": now, "targets": results, **extra}
