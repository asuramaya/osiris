"""THE FIRST-RUN STEPPER: one ordered list of the things a fresh install walks through,
replacing the Settings pane's old Readiness checklist (a flat, unordered table of yes/no/
unknown facts with no sense of "what's next"). Operator ruling: no paragraphs in the
console, a guided step-by-step with flags when something is missing.

EVERY STEP CARRIES A `mode`: "auto" or "hands". Setup is automatic (operator ruling): the
key and the backup password are created by the deploy, existing data is encrypted by a
background worker job, the offload runs on its own timer, the recovery file is copied beside
the backups, the restore test runs after the first backup and then weekly. An automatic step
shows PROGRESS (`progress`, or a plain reason) and never has an action button. Only the steps
that physically need a person (touching the security key, naming a backup destination) carry
an `action`.

Deliberately a PURE function, no pool, no live reads of its own: every fact it needs
(soul-key status, restic-key status, the offload targets with their own live presence
already attached, the offload receipts, the restore-drill receipts, whether the
services have restarted since the key was minted) is fetched once by the route that
calls this, so the ordering/flag logic itself is unit-testable against synthetic
inputs with no database, no filesystem, no systemctl -- the same split every other
domain in this house already holds (backup_settings.py's write half vs.
compositions.py's own read half).
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

StepStatus = Literal["done", "missing", "needs_attention"]

STEP_ORDER = (
    "key_set_up", "services_restarted", "recovery_enrolled", "recovery_verified",
    "recovery_copy_off_box", "data_encrypted", "backup_password_set",
    "offload_target_present", "offload_run", "offload_current", "restore_test_passed",
    "key_tpm_sealed",
)


def _step(key: str, label: str, status: StepStatus, reason: str | None,
          action: dict[str, Any] | None, *, mode: Literal["auto", "hands"] = "auto",
          progress: dict[str, Any] | None = None,
          optional: bool = False) -> dict[str, Any]:
    return {"key": key, "label": label, "status": status, "reason": reason,
            "action": action, "current": False, "mode": mode, "progress": progress,
            "optional": optional}


def _eta_text(eta_seconds: int | None) -> str:
    if not eta_seconds:
        return ""
    if eta_seconds < 90:
        return ", almost done"
    if eta_seconds < 5400:
        return f", about {round(eta_seconds / 60)} min left"
    return f", about {round(eta_seconds / 3600)} h left"


def _data_encrypted_step(soul_key: dict[str, Any], key_present: bool) -> dict[str, Any]:
    label = "Existing data encrypted"
    if not key_present:
        return _step("data_encrypted", label, "missing", "Waiting for the key.", None)
    enc = soul_key.get("encryption")
    if enc is None:  # an older status shape: only a bare remaining count
        legacy_rows = soul_key.get("legacy_plaintext_rows")
        if not legacy_rows:
            return _step("data_encrypted", label, "done", None, None)
        return _step("data_encrypted", label, "missing",
                     f"{legacy_rows} item(s) still stored in plain text.", None)
    state = enc.get("state")
    if state == "complete":
        return _step("data_encrypted", label, "done", None, None)
    done, remaining = enc.get("rows_done") or 0, enc.get("rows_remaining")
    total = enc.get("rows_total")
    progress = {
        "done": done, "total": total, "eta_seconds": enc.get("eta_seconds"),
        "rate_per_sec": enc.get("rate_per_sec"),
        "estimate": (bool(enc.get("rows_estimated")) or state != "running"
                     or remaining is None),
    }
    if state == "error":
        return _step("data_encrypted", label, "needs_attention",
                     f"Encryption hit a problem and will retry: {enc.get('last_error')}",
                     None, progress=progress)
    if state == "running" and remaining is not None:
        return _step("data_encrypted", label, "missing",
                     f"Encrypting in the background: {remaining} item(s) left"
                     f"{_eta_text(enc.get('eta_seconds'))}.", None, progress=progress)
    return _step("data_encrypted", label, "missing",
                 "Encryption starts automatically in the background.", None,
                 progress=progress)


TPM_JOIN_COMMANDS = ["sudo usermod -aG tss $USER", "log out and back in"]


def _tpm_step(soul_key: dict[str, Any], key_present: bool) -> dict[str, Any]:
    """OPTIONAL, never a warning: sealing the key to the machine's TPM is a strength upgrade,
    so a box that skips it (or has no TPM at all) still reads as fully set up. Only the group
    join needs a person (it needs sudo); the re-seal itself runs by itself afterwards."""
    label = "Key sealed to the TPM (optional)"
    if not key_present:
        return _step("key_tpm_sealed", label, "missing", "Waiting for the key.", None,
                     mode="hands", optional=True)
    backend = soul_key.get("backend")
    tpm = soul_key.get("tpm") or {}
    if backend == "host+tpm2":
        return _step("key_tpm_sealed", label, "done", None, None, mode="hands", optional=True)
    if backend != "host-cred" or not tpm.get("device_present") or not tpm.get("creds_available"):
        return _step("key_tpm_sealed", label, "done", "Not available on this machine.", None,
                     mode="hands", optional=True)
    if tpm.get("usable"):
        return _step("key_tpm_sealed", label, "missing",
                     "Sealing to the TPM automatically.", None, mode="auto", optional=True)
    reason = ("Log out and back in to finish joining the tss group." if tpm.get("tss_member")
              else "Join the tss group to seal the key to the TPM.")
    return _step("key_tpm_sealed", label, "missing", reason,
                 {"kind": "tpm_setup", "commands": TPM_JOIN_COMMANDS}, mode="hands",
                 optional=True)


def _when(value: Any) -> datetime | None:
    try:
        return datetime.fromisoformat(value) if value else None
    except (TypeError, ValueError):
        return None


def _restore_test_step(
    enabled_targets: list[dict[str, Any]], restore_drill_receipts: dict[str, Any],
) -> dict[str, Any]:
    """DONE ONLY FOR A TARGET THAT IS CONFIGURED NOW: a restore test passed against that
    target's own repository AFTER its first successful offload. A receipt for some other
    repository, or one that predates the target ever holding a backup (a drill run by hand
    before any destination existed, a destination since removed), proves nothing about the
    backups this machine is actually making, and used to read as done."""
    label = "Restore test passed"
    failed: str | None = None
    for target in enabled_targets:
        receipt = restore_drill_receipts.get(str(target.get("path_or_url") or ""), {})
        first_offload = _when(target.get("first_successful_offload")
                              or target.get("last_successful_offload"))
        passed = _when(receipt.get("last_passed_at"))
        if first_offload is not None and passed is not None and passed >= first_offload:
            return _step("restore_test_passed", label, "done", None, None)
        if first_offload is not None and receipt.get("last_error"):
            failed = str(receipt["last_error"])
    if failed:
        return _step("restore_test_passed", label, "needs_attention",
                     f"The last restore test failed and will be retried: {failed}", None)
    if not enabled_targets:
        return _step("restore_test_passed", label, "missing",
                     "Runs automatically once a backup target is set up and has had a "
                     "first backup.", None)
    return _step("restore_test_passed", label, "missing",
                 "Runs automatically after the first backup, then weekly.", None)


def _offload_current_step(
    enabled_targets: list[dict[str, Any]], stale: list[dict[str, Any]], stale_days: int,
) -> dict[str, Any]:
    """Backups keep happening: no enabled target has gone `stale_days` without a successful
    offload. A skip while away from home is correct behaviour, but one that lasts this long
    means nothing is being copied off the box, so it is said here with its reason."""
    from src.orchestrator.offload_staleness import stale_sentence

    label = "Backups up to date"
    if not enabled_targets:
        return _step("offload_current", label, "missing", "Waiting for a backup target.", None)
    if stale:
        return _step("offload_current", label, "needs_attention",
                     " ".join(stale_sentence(e, stale_days) + "." for e in stale), None)
    return _step("offload_current", label, "done", None, None)


def compute_readiness_steps(
    *, soul_key: dict[str, Any], restic_key: dict[str, Any],
    offload_targets: list[dict[str, Any]], restore_drill_receipts: dict[str, Any],
    services_restarted: bool | None,
    stale_offload: list[dict[str, Any]] | None = None, stale_days: int = 7,
) -> list[dict[str, Any]]:
    """`stale_offload`: offload_staleness.stale_offload_targets's list for this box, computed by
    the caller from the receipts and the `backup.offload_stale_days` limit (`stale_days`).
    `offload_targets`: backup_settings.get_backup_settings's own list, each dict
    already carrying `presence` (live for 'local', None for 'restic', per
    backup_settings._target_presence's own documented reason) merged with
    offload_runner.offload_receipts()'s per-name last_successful_offload/
    last_attempt_at/last_error -- the caller's job, so this function never touches a
    receipts file or a mount point itself. `restore_drill_receipts`:
    soul_key.restore_drill_receipts()'s own dict, keyed by repo_url."""
    steps: list[dict[str, Any]] = []

    key_present = bool(soul_key.get("present"))
    steps.append(_step(
        "key_set_up", "Encryption key set up",
        "done" if key_present else "missing",
        None if key_present else "Created automatically when osiris is installed or updated.",
        None,
    ))

    if not key_present:
        steps.append(_step(
            "services_restarted", "Services restarted with the key", "missing",
            "Waiting for the key.", None))
    elif services_restarted is True:
        steps.append(_step(
            "services_restarted", "Services restarted with the key", "done", None, None))
    else:
        steps.append(_step(
            "services_restarted", "Services restarted with the key", "needs_attention",
            "Waiting for the services to restart with the key." if services_restarted is False
            else "Could not confirm the services restarted with the key.", None))

    recovery = soul_key.get("recovery") or {}
    recovery_count = len(soul_key.get("recovery_paths_enrolled") or [])
    if not key_present:
        steps.append(_step(
            "recovery_enrolled", "Recovery method enrolled", "missing",
            "Waiting for the key.", None, mode="hands"))
    elif recovery_count >= 1:
        steps.append(_step(
            "recovery_enrolled", "Recovery method enrolled", "done", None, None,
            mode="hands"))
    else:
        steps.append(_step(
            "recovery_enrolled", "Recovery method enrolled", "missing",
            "Touch your security key to enroll it.", {"kind": "enroll_recovery"},
            mode="hands"))

    verified = recovery.get("verified") or {}
    if recovery_count < 1:
        steps.append(_step(
            "recovery_verified", "Recovery checked", "missing",
            "Enroll the recovery method first.", None, mode="hands"))
    elif recovery.get("stale"):
        steps.append(_step(
            "recovery_verified", "Recovery checked", "needs_attention",
            "The key changed since recovery was enrolled. Enroll it again.",
            {"kind": "enroll_recovery"}, mode="hands"))
    elif verified.get("last_verified_at") and verified.get("ok") is not False:
        steps.append(_step("recovery_verified", "Recovery checked", "done", None, None,
                           mode="hands"))
    elif verified.get("last_error"):
        steps.append(_step(
            "recovery_verified", "Recovery checked", "needs_attention",
            f"The last check failed: {verified['last_error']}",
            {"kind": "verify_recovery"}, mode="hands"))
    else:
        steps.append(_step(
            "recovery_verified", "Recovery checked", "missing",
            "Touch your security key to confirm recovery works. Nothing is changed.",
            {"kind": "verify_recovery"}, mode="hands"))

    copies = recovery.get("off_box_copies") or {}
    if recovery_count < 1:
        steps.append(_step(
            "recovery_copy_off_box", "Recovery copy off-box", "missing",
            "Waiting for the recovery method.", None))
    elif copies.get("current"):
        steps.append(_step("recovery_copy_off_box", "Recovery copy off-box", "done", None, None))
    else:  # includes a copy of an older enrollment, which the copy status does not count
        steps.append(_step(
            "recovery_copy_off_box", "Recovery copy off-box", "missing",
            "Copied automatically once a backup target is present.", None))

    steps.append(_data_encrypted_step(soul_key, key_present))

    restic_present = bool(restic_key.get("present"))
    steps.append(_step(
        "backup_password_set", "Backup password set",
        "done" if restic_present else "missing",
        None if restic_present else "Created automatically when osiris is installed or updated.",
        None,
    ))

    enabled_targets = [t for t in offload_targets if t.get("enabled")]
    primary = enabled_targets[0] if enabled_targets else None
    if not enabled_targets:
        steps.append(_step(
            "offload_target_present", "First offload target configured and present",
            "missing", "Choose where backups go.", {"kind": "configure_offload"},
            mode="hands"))
    elif primary and primary.get("kind") == "local" and not (
            (primary.get("presence") or {}).get("present")):
        steps.append(_step(
            "offload_target_present", "First offload target configured and present",
            "needs_attention", f"'{primary.get('name')}' mount point not found.",
            {"kind": "configure_offload"}, mode="hands"))
    else:
        steps.append(_step(
            "offload_target_present", "First offload target configured and present",
            "done", None, None, mode="hands"))

    ever_succeeded = any(t.get("last_successful_offload") for t in offload_targets)
    if ever_succeeded:
        steps.append(_step("offload_run", "First offload run", "done", None, None))
    elif primary and primary.get("last_error"):
        steps.append(_step(
            "offload_run", "First offload run", "needs_attention",
            f"Last attempt failed: {primary['last_error']}", None))
    else:
        steps.append(_step(
            "offload_run", "First offload run", "missing",
            "Runs automatically once a backup target is present.", None))

    steps.append(_offload_current_step(enabled_targets, stale_offload or [], stale_days))

    steps.append(_restore_test_step(enabled_targets, restore_drill_receipts))

    steps.append(_tpm_step(soul_key, key_present))

    for s in steps:
        if s["status"] != "done" and not s["optional"]:
            s["current"] = True
            break
    return steps
