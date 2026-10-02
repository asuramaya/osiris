"""AUTOMATIC KEY AND BACKUP SETUP: the one function install and first deploy call so that
nobody runs key-minting commands by hand. It does only what needs no human: mints the soul
key when none exists, mints (or adopts) the backup password, and, when a recovery
enrollment already exists, refreshes the backup password inside it. What genuinely needs a
person (touching the security key, naming a backup destination) stays out of here.

Pool-free and filesystem-only, like the primitives it composes. Idempotent: on a box that
is already set up it changes nothing and reports `minted=False`, so calling it on every
deploy is safe. The manual doors (`soul-key init`, `restic-key init`) stay for recovery
and debugging; they are not the setup path.
"""
from __future__ import annotations

import getpass
import os
from typing import Any

# Minting the soul key is what services need a restart for (they refuse to boot without
# one, and cache it once loaded); the backup password is read fresh by each offload run.
RESTART_UNITS = ["osiris-mcp.service", "osiris-worker.service"]


def ensure_key_setup(*, path: str | None = None, restic_path: str | None = None) -> dict[str, Any]:
    """Runs every automatic step, returning a report:

    - `key`: "present" or "minted" (or an `error` and `ok=False`, the only failure that
      should stop a deploy, since the services cannot boot without a key).
    - `backup_password`: "present", "minted", or "failed" (never fatal: offload simply
      has nothing to run with until it is retried on the next call).
    - `recovery_refreshed`: True when an existing recovery enrollment gained the backup
      password (no touch needed).
    - `minted`: whether the soul key was created by THIS call, which is what callers use
      to decide a service restart is needed.
    """
    from src.ingest import soul_crypto
    from src.orchestrator import restic_credential

    report: dict[str, Any] = {"ok": True, "minted": False}

    status = soul_crypto.soul_key_status(path=path)
    if status["present"]:
        report["key"] = "present"
        # status readers never decrypt the key, so the one decrypt that tells them whether
        # the recovery enrollment is current happens here, at deploy, and is remembered
        soul_crypto.ensure_key_fingerprint_cached(path=path)
    elif os.getuid() == 0:
        # Root has no natural owner for the key file (the units are systemd --user under
        # the operator's own login), so guessing one here would seal it to the wrong user.
        report.update(
            ok=False, key="failed",
            error="cannot mint the encryption key as root: run the deploy as the user "
                  f"the services run as (not {getpass.getuser()!r} via sudo)")
        return report
    else:
        out = soul_crypto.soul_key_init(path=path)
        if "error" in out:
            report.update(ok=False, key="failed", error=out["error"])
            return report
        report.update(key="minted", minted=True, backend=out.get("backend"),
                      tss_hint=out.get("tss_hint"))

    # An existing host-cred key moves onto the TPM the moment this session can use it
    # (same key, sealed a stronger way); a no-op on every other box and every other run.
    try:
        resealed = soul_crypto.soul_key_reseal_tpm(path=path)
    except Exception as exc:  # noqa: BLE001 - an upgrade attempt must never block setup
        report["tpm_note"] = f"{type(exc).__name__}: {exc}"
    else:
        report["tpm_resealed"] = bool(resealed.get("resealed"))
        if resealed.get("error"):
            report["tpm_note"] = resealed["error"]

    try:
        pw = restic_credential.restic_key_ensure(path=restic_path)
    except Exception as exc:  # noqa: BLE001 - a backup-password hiccup must never block the key
        report["backup_password"] = "failed"
        report["backup_password_error"] = f"{type(exc).__name__}: {exc}"
        return report
    if "error" in pw:
        report["backup_password"] = "failed"
        report["backup_password_error"] = pw["error"]
        return report
    report["backup_password"] = "minted" if pw.get("created") else "present"

    if soul_crypto.soul_key_recovery_facts(path=path)["enrolled"]:
        try:
            password = restic_credential.get_restic_password(path=restic_path)
            attached = soul_crypto.attach_restic_password_to_recovery(password, path=path)
        except Exception as exc:  # noqa: BLE001 - recovery refresh is best effort, reported
            report["recovery_note"] = f"{type(exc).__name__}: {exc}"
        else:
            if "error" in attached:
                report["recovery_note"] = attached["error"]
            else:
                report["recovery_refreshed"] = bool(attached["changed"])
    return report
