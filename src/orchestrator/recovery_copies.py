"""OFF-BOX COPIES OF THE RECOVERY FILE: losing the machine must not lose the only copy of the
security-key recovery file (and the backup password wrapped inside it). Safe to store anywhere:
opening it needs the physical security key and its PIN.

WHY A PLAIN FILE NEXT TO THE BACKUPS, NOT INSIDE THEM: the restic repositories are encrypted
with the backup password, and the backup password is inside this file. A copy that lives only
inside a repository would need the very password it protects. So the file is copied as a
plain file BESIDE each target, and also into the vault (which every backup then carries, a
second layer, not a substitute).

WHERE: `<vault>/osiris-recovery/`, `<mountpoint>/osiris-recovery/` on every present local
target, and `<repository's parent>/osiris-recovery/` on sftp targets (over the real sftp
client in batch mode, bounded, never prompting). Other restic kinds (rest:, s3:, b2:, ...)
cannot hold a plain file and are reported as such, never counted.

WHEN: every offload tick, at enrollment, and at deploy, all through `sync_recovery_copies`.
Idempotent: a copy that already matches the current file is left alone, so a tick that finds
nothing changed does no work. Receipts (`recovery_copies.json`) record the fingerprint of
what each destination holds, and `copy_status` compares them to the current file so a
destination holding an older enrollment reads as stale, not as protected.

Never raises past its own boundary: every failure is recorded against its destination."""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import posixpath
import shutil
import subprocess
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncpg

_RECEIPTS_ENV = "OSIRIS_RECOVERY_COPIES_FILE"
_DEFAULT_RECEIPTS_FILE = "~/.local/state/osiris/recovery_copies.json"
COPY_DIR = "osiris-recovery"
COPY_NAME = "soul.key.recovery.json"
VAULT_KEY = "(vault)"
_SSH_CONFIG_ENV = "OSIRIS_SSH_CONFIG"
_SFTP_TIMEOUT_SECONDS = 60
_SFTP_RECHECK_SECONDS = 7 * 86400.0  # a remote copy is trusted from its receipt for a week


def _receipts_path() -> Path:
    env = os.environ.get(_RECEIPTS_ENV)
    return Path(env).expanduser() if env else Path(_DEFAULT_RECEIPTS_FILE).expanduser()


def read_copy_receipts() -> dict[str, Any]:
    try:
        return dict(json.loads(_receipts_path().read_text()))
    except (OSError, ValueError):
        return {}


def _write_receipt(dest: str, receipt: dict[str, Any]) -> None:
    """Merged, never replaced: a failed attempt keeps an earlier real `sha256`/`last_copied_at`
    (the old copy is still there) and only adds the attempt and its error."""
    path = _receipts_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    receipts = read_copy_receipts()
    existing = receipts.get(dest, {})
    existing.update(receipt)
    receipts[dest] = existing
    path.write_text(json.dumps(receipts, indent=2))


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def source_file(*, path: str | None = None) -> Path | None:
    """The live recovery file, or None when no recovery method is enrolled yet."""
    from src.ingest import soul_crypto

    src = soul_crypto.recovery_file_path(path=path)
    return src if src.exists() else None


def copy_status(*, path: str | None = None) -> dict[str, Any]:
    """`{"enrolled", "count", "destinations", "current", "vault"}`: how many OFF-BOX
    destinations hold the CURRENT file (a copy of an older enrollment does not count), and
    whether the vault does. Cheap: two small file reads, no network."""
    src = source_file(path=path)
    if src is None:
        return {"enrolled": False, "count": 0, "destinations": [], "current": False,
                "vault": False}
    current = _sha256(src)
    good = sorted(
        name for name, r in read_copy_receipts().items()
        if name != VAULT_KEY and r.get("sha256") == current and not r.get("last_error"))
    vault = read_copy_receipts().get(VAULT_KEY, {})
    return {"enrolled": True, "count": len(good), "destinations": good,
            "current": bool(good),
            "vault": vault.get("sha256") == current and not vault.get("last_error")}


def _copy_local(src: Path, directory: Path) -> str | None:
    """Plain file copy into `directory` (created), 0600 where the filesystem honours it.
    Skips when the destination already matches. Returns a failure string or None."""
    dest = directory / COPY_NAME
    try:
        if dest.exists() and _sha256(dest) == _sha256(src):
            return None
        directory.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(dest.name + ".tmp")
        shutil.copyfile(src, tmp)
        try:
            tmp.chmod(0o600)
        except OSError:
            pass  # a drive format with no permission bits (exFAT, NTFS): nothing to set
        tmp.replace(dest)
        return None
    except OSError as exc:
        return f"{type(exc).__name__}: {exc}"


def _parse_sftp(url: str) -> tuple[str, str] | None:
    """`sftp:[user@]host:/path` (restic's scp-style form) -> (host, path); anything else,
    including the `sftp://` form, is None (not attempted rather than guessed at)."""
    if not url.startswith("sftp:") or url.startswith("sftp://"):
        return None
    host, sep, path = url[len("sftp:"):].partition(":")
    return (host, path) if sep and host and path.startswith("/") else None


def _sftp_quote(path: str) -> str:
    """One path as an sftp batch-file word: double-quoted, with quote and backslash escaped."""
    return '"' + path.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _ssh_config_args() -> list[str]:
    """`OSIRIS_SSH_CONFIG`: a dedicated ssh config file for the backup targets (a host alias,
    a port, a key), passed as `-F`. Unset, the user's ordinary `~/.ssh/config` applies, which
    is what restic itself uses to reach the same sftp target."""
    cfg = os.environ.get(_SSH_CONFIG_ENV)
    return ["-F", cfg] if cfg else []


def _copy_sftp(src: Path, url: str) -> str | None:
    """Copies `src` to `<repository's parent>/osiris-recovery/` over the real `sftp` client in
    batch mode: the same SFTP subsystem restic itself talks to, so it works for an sftp-only
    account (a NAS user with no shell) where `ssh host mkdir` would not. The file is uploaded
    under a temporary name and renamed into place, so a reader never sees a half-written
    copy. BatchMode: never prompts, a missing key or an unknown host fails cleanly."""
    parsed = _parse_sftp(url)
    if parsed is None:
        return f"cannot copy a plain file to {url!r}: only sftp:host:/path targets are supported"
    host, repo_path = parsed
    directory = posixpath.join(posixpath.dirname(repo_path.rstrip("/")) or "/", COPY_DIR)
    final = posixpath.join(directory, COPY_NAME)
    temp = final + ".tmp"
    # a leading `-` makes sftp carry on when that one command fails (the directory already
    # exists, there is no older copy to remove)
    batch = "\n".join([
        f"-mkdir {_sftp_quote(directory)}",
        f"put {_sftp_quote(str(src))} {_sftp_quote(temp)}",
        f"-rm {_sftp_quote(final)}",
        f"rename {_sftp_quote(temp)} {_sftp_quote(final)}",
    ]) + "\n"
    try:
        run = subprocess.run(
            ["sftp", "-q", "-b", "-", *_ssh_config_args(), "-o", "BatchMode=yes",
             "-o", "ConnectTimeout=10", host],
            input=batch.encode(), capture_output=True, timeout=_SFTP_TIMEOUT_SECONDS,
            check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return f"{type(exc).__name__}: {exc}"
    if run.returncode != 0:
        detail = (run.stderr or run.stdout).decode(errors="replace").strip()
        return f"sftp failed (exit {run.returncode}): {detail}"
    return None


def copy_to_vault(vault: Path, *, path: str | None = None) -> dict[str, Any] | None:
    """The vault copy (every backup then carries it). None when nothing is enrolled."""
    src = source_file(path=path)
    if src is None:
        return None
    now = datetime.now(UTC).isoformat()
    if not vault.is_dir():
        fail: str | None = f"vault directory {vault} does not exist"
    else:
        fail = _copy_local(src, vault / COPY_DIR)
    return _record(VAULT_KEY, "vault", src, fail, now)


def _record(dest: str, kind: str, src: Path, fail: str | None, now: str) -> dict[str, Any]:
    if fail is None:
        _write_receipt(dest, {"kind": kind, "sha256": _sha256(src), "last_copied_at": now,
                              "last_attempt_at": now, "last_error": None})
        return {"dest": dest, "ok": True}
    _write_receipt(dest, {"kind": kind, "last_attempt_at": now, "last_error": fail})
    return {"dest": dest, "ok": False, "error": fail}


def copy_to_target(
    target: dict[str, Any], *, path: str | None = None,
    sftp_copy: Callable[[Path, str], str | None] = _copy_sftp,
) -> dict[str, Any] | None:
    """One PRESENT offload target (the caller has checked presence). None when nothing is
    enrolled or the destination already holds the current file."""
    src = source_file(path=path)
    if src is None:
        return None
    name = str(target.get("name", "<unnamed>"))
    kind = target.get("kind")
    now = datetime.now(UTC).isoformat()
    receipt = read_copy_receipts().get(name, {})
    if kind == "local":
        mountpoint = target.get("expected_mountpoint") or ""
        fail = (_copy_local(src, Path(mountpoint) / COPY_DIR) if mountpoint
                else "this target has no mountpoint to copy into")
        return _record(name, "local", src, fail, now)
    url = str(target.get("path_or_url") or "")
    if _parse_sftp(url) is None:
        return _record(
            name, "restic", src,
            "this kind of target cannot hold a plain file; a copy inside the repository would "
            "need the very password it protects", now)
    if receipt.get("sha256") == _sha256(src) and not receipt.get("last_error"):
        try:
            age = (datetime.now(UTC) - datetime.fromisoformat(
                receipt["last_copied_at"])).total_seconds()
        except (KeyError, ValueError):
            age = _SFTP_RECHECK_SECONDS
        if age < _SFTP_RECHECK_SECONDS:
            return None
    return _record(name, "restic", src, sftp_copy(src, url), now)


async def sync_recovery_copies(
    targets: list[dict[str, Any]], vault: Path, *, path: str | None = None,
) -> list[dict[str, Any]]:
    """Vault first (so the backups that follow carry it), then each PRESENT target given.
    Blocking file and ssh work runs off the event loop. Returns only what was attempted."""
    out: list[dict[str, Any]] = []
    first = await asyncio.to_thread(copy_to_vault, vault, path=path)
    if first is not None:
        out.append(first)
    for target in targets:
        result = await asyncio.to_thread(copy_to_target, target, path=path)
        if result is not None:
            out.append(result)
    return out


async def present_targets(pool: asyncpg.Pool) -> list[dict[str, Any]]:
    """Enabled offload targets that can take a copy right now: a local one whose mountpoint
    is mounted, and an sftp one unless its route leaves through a tunnel (away from home;
    see network_presence). Beyond that, reachability is only knowable by trying."""
    from src.orchestrator.backup_settings import get_backup_settings
    from src.orchestrator.backup_validation import check_local_target_presence
    from src.orchestrator.network_presence import lan_presence

    settings = await get_backup_settings(pool)
    out = []
    for t in settings.get("offload_targets") or []:
        if not isinstance(t, dict) or not t.get("enabled"):
            continue
        if t.get("kind") == "local":
            mountpoint = t.get("expected_mountpoint") or ""
            presence = await asyncio.to_thread(check_local_target_presence, mountpoint)
            if not presence.get("present"):
                continue
        elif not (await asyncio.to_thread(lan_presence, t)).get("present"):
            continue
        out.append(t)
    return out


async def sync_now(pool: asyncpg.Pool, *, path: str | None = None) -> list[dict[str, Any]]:
    """Copies to the vault and every present target immediately (enrollment and deploy
    call this so the file leaves the machine without waiting for the next tick)."""
    vault = Path(os.environ.get("OSIRIS_VAULT", str(Path.home() / "osiris-vault")))
    return await sync_recovery_copies(await present_targets(pool), vault, path=path)
