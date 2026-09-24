"""THE FIRST-RUN STEPPER: one ordered list of the eight things a fresh install walks
through, replacing the Settings pane's old Readiness checklist (a flat, unordered
table of yes/no/unknown facts with no sense of "what's next"). Operator ruling: no
paragraphs in the console, a guided step-by-step with flags when something is missing.

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

from typing import Any, Literal

StepStatus = Literal["done", "missing", "needs_attention"]

STEP_ORDER = (
    "key_set_up", "services_restarted", "recovery_enrolled", "data_encrypted",
    "backup_password_set", "offload_target_present", "offload_run", "restore_test_passed",
)


def _step(key: str, label: str, status: StepStatus, reason: str | None,
          action: dict[str, Any] | None) -> dict[str, Any]:
    return {"key": key, "label": label, "status": status, "reason": reason,
            "action": action, "current": False}


def compute_readiness_steps(
    *, soul_key: dict[str, Any], restic_key: dict[str, Any],
    offload_targets: list[dict[str, Any]], restore_drill_receipts: dict[str, Any],
    services_restarted: bool | None,
) -> list[dict[str, Any]]:
    """`offload_targets`: backup_settings.get_backup_settings's own list, each dict
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
        None if key_present else "Not set up yet.",
        None if key_present else {"kind": "init_key"},
    ))

    if not key_present:
        steps.append(_step(
            "services_restarted", "Services restarted with the key", "missing",
            "Set up the key first.", {"kind": "jump", "target": "key_set_up"}))
    elif services_restarted is True:
        steps.append(_step(
            "services_restarted", "Services restarted with the key", "done", None, None))
    elif services_restarted is False:
        steps.append(_step(
            "services_restarted", "Services restarted with the key", "needs_attention",
            "Restart osiris-mcp and osiris-worker to pick up the key.",
            {"kind": "restart_services"}))
    else:
        steps.append(_step(
            "services_restarted", "Services restarted with the key", "needs_attention",
            "Could not confirm; restart the services by hand if you haven't.",
            {"kind": "restart_services"}))

    recovery_count = len(soul_key.get("recovery_paths_enrolled") or [])
    if not key_present:
        steps.append(_step(
            "recovery_enrolled", "Recovery method enrolled", "missing",
            "Set up the key first.", {"kind": "jump", "target": "key_set_up"}))
    elif recovery_count >= 1:
        steps.append(_step("recovery_enrolled", "Recovery method enrolled", "done", None, None))
    else:
        steps.append(_step(
            "recovery_enrolled", "Recovery method enrolled", "missing",
            "No recovery method enrolled yet.", {"kind": "enroll_recovery"}))

    legacy_rows = soul_key.get("legacy_plaintext_rows")
    if not key_present:
        steps.append(_step(
            "data_encrypted", "Existing data encrypted", "missing",
            "Set up the key first.", {"kind": "jump", "target": "key_set_up"}))
    elif not legacy_rows:
        steps.append(_step("data_encrypted", "Existing data encrypted", "done", None, None))
    else:
        steps.append(_step(
            "data_encrypted", "Existing data encrypted", "needs_attention",
            f"{legacy_rows} item(s) still stored in plain text.",
            {"kind": "encrypt_existing"}))

    restic_present = bool(restic_key.get("present"))
    steps.append(_step(
        "backup_password_set", "Backup password set",
        "done" if restic_present else "missing",
        None if restic_present else "Not set up yet.",
        None if restic_present else {"kind": "init_restic"},
    ))

    enabled_targets = [t for t in offload_targets if t.get("enabled")]
    primary = enabled_targets[0] if enabled_targets else None
    if not enabled_targets:
        steps.append(_step(
            "offload_target_present", "First offload target configured and present",
            "missing", "No offload targets configured yet.", {"kind": "configure_offload"}))
    elif primary and primary.get("kind") == "local" and not (
            (primary.get("presence") or {}).get("present")):
        steps.append(_step(
            "offload_target_present", "First offload target configured and present",
            "needs_attention", f"'{primary.get('name')}' mount point not found.",
            {"kind": "configure_offload"}))
    else:
        steps.append(_step(
            "offload_target_present", "First offload target configured and present",
            "done", None, None))

    ever_succeeded = any(t.get("last_successful_offload") for t in offload_targets)
    if ever_succeeded:
        steps.append(_step("offload_run", "First offload run", "done", None, None))
    elif primary and primary.get("last_error"):
        steps.append(_step(
            "offload_run", "First offload run", "needs_attention",
            f"Last attempt failed: {primary['last_error']}", {"kind": "run_offload"}))
    else:
        steps.append(_step(
            "offload_run", "First offload run", "missing", "Never run yet.",
            {"kind": "run_offload"}))

    drill_passed = any(r.get("last_passed_at") for r in restore_drill_receipts.values())
    if drill_passed:
        steps.append(_step("restore_test_passed", "Restore test passed", "done", None, None))
    else:
        steps.append(_step(
            "restore_test_passed", "Restore test passed", "missing", "Never tested yet.",
            {"kind": "restore_drill"}))

    for s in steps:
        if s["status"] != "done":
            s["current"] = True
            break
    return steps
