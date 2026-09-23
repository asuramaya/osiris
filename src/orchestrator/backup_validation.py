"""THE BACKUP TOPOLOGY VALIDATOR: one shared module the CLI, MCP, and REST entry points
all call for the two checks that only ever mean one thing wherever they're asked, never
reimplemented per surface, the same discipline `is_live_handoff`/`HANDOFF_LIVE_PREDICATE_SQL`
already established for the handoff predicate.

THREE CHECKS, each a coarse-but-sufficient proxy over the real filesystem/mount state
(same disclosed-tradeoff shape as test_unbounded_wait.py's own scanner):

  `validate_vault_path`: the operator's own always-local backup.vault_path. Checks
  absolute, exists, writable, free space, and whether it sits on the SAME block device
  as `/` (informational only, warn, never refuse). Also refuses a vault_path that
  resolves under a mountpoint NOT declared always-present in /etc/fstab or a systemd
  .mount unit's own `Where=`, the operator's "always-present-mount law": the vault must
  survive a reboot with no manual remount, so a path under an intermittent target (the
  8TB drive, the NAS) is refused outright, not warned.

  `check_local_target_presence`: an offload_targets[].kind='local' entry's own
  `expected_mountpoint`. Checks whether it is ACTUALLY a mountpoint right now
  (`findmnt`, not `os.path.ismount`, per the operator's own tool choice), writable,
  free space. Read-only status, never a refusal: an absent drive is the expected common
  case for an intermittent target, not an error.

  `validate_restic_url`: an offload_targets[].kind='restic' entry's own `path_or_url`.
  SHAPE ONLY, no network call, ever, per the operator's own repeated instruction.
  Checks against restic's own documented backend-prefix grammar
  (https://restic.readthedocs.io/en/stable/030_preparing_a_new_repo.html): a scheme
  prefix (s3:/b2:/sftp:/rest:/swift:/azure:/gs:/rclone:) or a bare local/relative path,
  which restic also accepts unprefixed.
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

_RESTIC_SCHEME_RE = re.compile(
    r"^(s3|b2|sftp|rest|rest:http|rest:https|swift|azure|gs|rclone):", re.IGNORECASE)


def _findmnt_mountpoint(path: str) -> bool:
    """True iff `path` is ITSELF a mountpoint right now: `findmnt --mountpoint <path>`
    exits 0 when it is, nonzero (and prints nothing useful) when it isn't or doesn't
    exist. Subprocess, not `os.path.ismount`, per the operator's own tool choice:
    `findmnt` reads the live kernel mount table the same way `mount`/
    `systemctl status` do, the same source of truth an operator would check by hand."""
    try:
        proc = subprocess.run(
            ["findmnt", "--mountpoint", path], capture_output=True, timeout=5, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return False
    return proc.returncode == 0


def _fstab_mountpoints() -> set[str]:
    """Column 2 of every non-comment, non-blank /etc/fstab line: the box's own declared
    always-mount set. Best-effort: an unreadable or missing fstab (a container, a
    from-scratch dev box) returns empty rather than raising, so the always-present-mount
    refusal below degrades to "no declared always-present mounts" rather than crashing
    the validator entirely."""
    try:
        text = Path("/etc/fstab").read_text()
    except OSError:
        return set()
    points: set[str] = set()
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        fields = stripped.split()
        if len(fields) >= 2:
            points.add(fields[1])
    return points


def _systemd_mount_mountpoints() -> set[str]:
    """Every `Where=` value across every `.mount` unit under /etc/systemd/system and
    /usr/lib/systemd/system: the systemd-managed half of "always present", alongside
    fstab (systemd itself generates a .mount unit per fstab entry at boot, but a
    hand-authored unit for something like a LUKS-unlocked or network mount has no fstab
    line at all, so both sources are checked, not just one)."""
    points: set[str] = set()
    for root in ("/etc/systemd/system", "/usr/lib/systemd/system"):
        try:
            unit_paths = list(Path(root).glob("*.mount"))
        except OSError:
            continue
        for unit_path in unit_paths:
            try:
                text = unit_path.read_text(errors="replace")
            except OSError:
                continue
            for line in text.splitlines():
                stripped = line.strip()
                if stripped.startswith("Where="):
                    points.add(stripped[len("Where="):].strip())
    return points


def _real_mountpoint_of(path: str) -> str | None:
    """The mountpoint the LIVE kernel mount table says `path` currently resolves onto:
    `findmnt --target <path>` (not `--mountpoint`, which only matches path being ITSELF
    a mount root): a vault_path two directories below a real mount still resolves to
    that mount's own root, correctly, off the actual running table rather than a string
    walk that could not tell "this subdirectory sits on the same filesystem as its
    parent mount" from "this subdirectory sits on nothing declared at all". None when
    `path` doesn't exist or findmnt itself is unavailable: the caller treats that the
    same as "not declared", refusing rather than guessing."""
    try:
        proc = subprocess.run(
            ["findmnt", "--target", path, "--noheadings", "--output", "TARGET"],
            capture_output=True, timeout=5, check=False, text=True)
    except (OSError, subprocess.TimeoutExpired):
        return None
    if proc.returncode != 0:
        return None
    return proc.stdout.strip() or None


def _is_always_present_mountpoint(path: str) -> bool:
    """`path` currently resolves (via the live mount table) onto a mountpoint declared
    to survive a reboot with no manual remount: an fstab line or a systemd `.mount`
    unit's own `Where=`. `/` itself always counts (root is always present by
    construction, whether or not it has its own explicit fstab/systemd-unit line)."""
    real = _real_mountpoint_of(path)
    if real is None:
        return False
    declared = _fstab_mountpoints() | _systemd_mount_mountpoints() | {"/"}
    return real in declared


def _device_of(path: Path) -> int:
    """`st_dev` isolated behind its own function purely so a test can monkeypatch THIS
    (never the real `os.stat`, which pytest-xdist's own worker machinery also calls
    internally: patching it globally crashes the worker, not just the test)."""
    return os.stat(path).st_dev


def validate_vault_path(path: str) -> dict[str, Any]:
    checks: dict[str, Any] = {
        "absolute": False, "exists": False, "writable": False,
        "free_bytes": None, "same_device_as_root": False,
        "always_present_mount": False,
    }
    warnings: list[str] = []
    errors: list[str] = []

    p = Path(path)
    checks["absolute"] = p.is_absolute()
    if not checks["absolute"]:
        errors.append(f"vault_path must be absolute, got {path!r}")
        return {"ok": False, "checks": checks, "warnings": warnings, "errors": errors}

    checks["exists"] = p.exists()
    if not checks["exists"]:
        errors.append(f"vault_path {path!r} does not exist")
    else:
        checks["writable"] = os.access(p, os.W_OK)
        if not checks["writable"]:
            errors.append(f"vault_path {path!r} is not writable")
        try:
            checks["free_bytes"] = shutil.disk_usage(p).free
        except OSError:
            pass
        try:
            checks["same_device_as_root"] = _device_of(p) == _device_of(Path("/"))
        except OSError:
            pass
        if checks["same_device_as_root"]:
            warnings.append(
                "vault_path is on the same block device as /, a disk failure takes out "
                "the OS and the backup vault together (informational only, never refused)")

    checks["always_present_mount"] = _is_always_present_mountpoint(str(p))
    if not checks["always_present_mount"]:
        errors.append(
            f"vault_path {path!r} does not sit under a mountpoint declared in /etc/fstab "
            "or a systemd .mount unit's Where=. The vault must survive a reboot "
            "unattended, so an intermittent target (the docked drive, the NAS) belongs "
            "in offload_targets, never as vault_path itself")

    return {"ok": not errors, "checks": checks, "warnings": warnings, "errors": errors}


def check_local_target_presence(expected_mountpoint: str) -> dict[str, Any]:
    """Read-only status for one offload_targets[].kind='local' entry, never a refusal;
    an absent drive is the expected common case for an intermittent target."""
    present = _findmnt_mountpoint(expected_mountpoint)
    writable: bool | None = None
    free_bytes: int | None = None
    if present:
        p = Path(expected_mountpoint)
        writable = os.access(p, os.W_OK)
        try:
            free_bytes = shutil.disk_usage(p).free
        except OSError:
            pass
    return {"present": present, "writable": writable, "free_bytes": free_bytes}


def validate_restic_url(url: str) -> dict[str, Any]:
    """SHAPE ONLY, no network call, ever, per the operator's own repeated instruction. A
    scheme-prefixed backend (s3:/b2:/sftp:/rest:/swift:/azure:/gs:/rclone:) or a bare
    local/relative path (restic's own documented default when no prefix is given) both
    pass; an empty string or one that merely LOOKS like a bare word with no path
    separator and no scheme is refused as almost certainly a typo, not a real target."""
    if not url or not isinstance(url, str):
        return {"url_shape_ok": False, "error": "path_or_url must be a non-empty string"}
    if _RESTIC_SCHEME_RE.match(url):
        return {"url_shape_ok": True, "error": None}
    if "/" in url or url in (".", ".."):
        return {"url_shape_ok": True, "error": None}  # a bare local/relative repo path
    return {"url_shape_ok": False,
            "error": f"{url!r} is neither a restic backend prefix "
                     "(s3:/b2:/sftp:/rest:/swift:/azure:/gs:/rclone:) nor a path"}
