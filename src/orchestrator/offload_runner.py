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
docstring states for 'local'."""
from __future__ import annotations

import asyncio
import json
import os
import subprocess
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
    path = _receipts_path()
    try:
        return dict(json.loads(path.read_text()))
    except (OSError, ValueError):
        return {}


def _write_receipt(name: str, receipt: dict[str, Any]) -> None:
    """Merges `receipt`'s fields into the existing per-target record rather than
    replacing it wholesale: a failed tick must never CLOBBER a real prior
    `last_successful_offload` with None; it only ever adds `last_attempt_at`/
    `last_error` alongside whatever succeeded most recently."""
    path = _receipts_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    receipts = _read_receipts()
    existing = receipts.get(name, {})
    existing.update(receipt)
    receipts[name] = existing
    path.write_text(json.dumps(receipts, indent=2))


def offload_receipts() -> dict[str, Any]:
    """Public read entry point for `compositions._fn_backup_status`'s own `offbox_section`,
    never writes, mirrors `_read_receipts` under a name that doesn't look
    private to an external caller."""
    return _read_receipts()


def _run_restic_backup(
    *, repository: str, password: bytes, source: Path, init_if_needed: bool = True,
) -> str | None:
    """Returns a failure string, or None on success, same return convention as
    `osiris_offbox_restore_drill.run_drill`. `restic init` runs once per
    repository, idempotently, the same guard `osiris_offbox_backup.sh` already
    uses (`restic snapshots` failing first is how an uninitialized repo is told
    apart from a genuinely unreachable one: an unreachable one fails `init` too,
    caught the same way as any other restic failure below, never a special case)."""
    env = {**os.environ, "RESTIC_REPOSITORY": repository, "RESTIC_PASSWORD": password.decode()}
    try:
        if init_if_needed:
            probe = subprocess.run(
                ["restic", "snapshots"], env=env, capture_output=True, timeout=30, check=False)
            if probe.returncode != 0:
                init = subprocess.run(
                    ["restic", "init"], env=env, capture_output=True, timeout=30, check=False)
                if init.returncode != 0:
                    return (f"restic init failed: "
                            f"{init.stderr.decode(errors='replace').strip()}")
        backup = subprocess.run(
            ["restic", "backup", str(source), "--exclude=*.tmp"],
            env=env, capture_output=True, timeout=_RESTIC_TIMEOUT_SECONDS, check=False)
        if backup.returncode != 0:
            return f"restic backup failed: {backup.stderr.decode(errors='replace').strip()}"
        return None
    except (OSError, subprocess.TimeoutExpired) as exc:
        return f"{type(exc).__name__}: {exc}"


async def run_offload_tick(pool: asyncpg.Pool, *, vault: Path | None = None) -> dict[str, Any]:
    """ONE TICK, meant to be `osiris-offload.timer`'s own `ExecStart`, opportunistic
    (never blocks, never fails the caller): for every ENABLED `offload_targets[]`
    row that is PRESENT right now, runs one restic backup, records a receipt.
    Absent/disabled targets are reported, never treated as an error. A missing
    restic password (`ResticPasswordMissing`) degrades the WHOLE tick with one
    `error` key (nothing to back up onto without it) rather than a confusing
    per-target failure repeated N times."""
    from src.orchestrator.backup_settings import get_backup_settings
    from src.orchestrator.backup_validation import check_local_target_presence
    from src.orchestrator.restic_credential import ResticPasswordMissing, get_restic_password

    try:
        password = get_restic_password()
    except ResticPasswordMissing as exc:
        return {"error": str(exc), "targets": []}

    settings = await get_backup_settings(pool)
    targets = settings.get("offload_targets") or []
    source = vault or Path(os.environ.get("OSIRIS_VAULT", str(Path.home() / "osiris-vault")))
    now = datetime.now(UTC).isoformat()

    results: list[dict[str, Any]] = []
    for target in targets:
        if not isinstance(target, dict):
            continue
        name = target.get("name", "<unnamed>")
        if not target.get("enabled"):
            results.append({"name": name, "skipped": "disabled"})
            continue
        kind = target.get("kind")
        if kind == "local":
            mountpoint = target.get("expected_mountpoint") or ""
            presence = await asyncio.to_thread(check_local_target_presence, mountpoint)
            if not presence.get("present"):
                results.append({"name": name, "skipped": "not present (mountpoint absent)"})
                continue
        repository = target.get("path_or_url", "")
        fail = await asyncio.to_thread(
            _run_restic_backup, repository=repository, password=password, source=source)
        if fail is None:
            _write_receipt(name, {"last_successful_offload": now, "last_attempt_at": now,
                                  "last_error": None})
            results.append({"name": name, "ok": True})
        else:
            # a 'restic' target's own unreachability surfaces here identically to a
            # genuine backup failure: this runner has no separate network-reachable
            # probe for restic targets (the standing rule in this domain is never a
            # network call outside the real operation itself),
            # so "tried and failed" is the only signal a sftp/NAS target ever gets.
            _write_receipt(name, {"last_attempt_at": now, "last_error": fail})
            results.append({"name": name, "ok": False, "error": fail})
    return {"as_of": now, "targets": results}
