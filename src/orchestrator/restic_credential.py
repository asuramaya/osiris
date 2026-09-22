"""THE RESTIC REPOSITORY PASSWORD, THE SAME SHAPE AS THE SOUL-STORE KEY (KEY CUSTODY
REWRITTEN, ruling e0b98ff2: "Same shape for the restic repository password" — Thoth
mail 12836/12813, THE OFFLOAD RUNNER). Reuses `src.ingest.systemd_credential`'s own
subprocess boundary (the same module `soul_crypto.py` was refactored onto for this
exact reason) rather than a second hand-copy of the systemd-creds invocation.

DELIBERATELY SMALLER THAN THE SOUL-STORE DOOR: this password is a restic repository
passphrase, not a Fernet key — any bytes restic accepts as `RESTIC_PASSWORD` (no
fixed shape to validate), so there is no `MultiFernet`/legacy-key-rotation-window
concept here at all. THIS FIRST CUT SHIPS `init`/`status` ONLY — no `rotate`,
`enroll-recovery`, or `recover` yet (a deliberate scope cut, not an oversight: a
restic repository's own password rotation additionally needs a `restic key add`/
`restic key remove` pass against the live repository, not just a local file swap,
and a FIDO2 recovery wrap is "next" per the ruling's own wording, not "now" — Khnum
flagged this explicitly to Thoth rather than silently shipping partial rotate
support that looks complete).

RESTIC ITSELF NEVER SEES THE CUSTODY LAYER: `get_restic_password()` returns the raw
plaintext bytes (decrypted, if the backend is `host-cred`/`host+tpm2`) for the
caller (`src.orchestrator.offload_runner`) to set directly as the `RESTIC_PASSWORD`
subprocess env var — never a temp file, never a CLI argument (visible via `ps`),
matching `osiris_offbox_backup.sh`'s own long-standing "never a script argument"
law. Under `osiris-offload.service`'s own `ImportCredential=restic.password` (THE
FIRST KEY MUST COME FROM THE NORMAL CLI, Thoth mail 13065 — replacing an earlier
`LoadCredentialEncrypted=restic.password:<hard path>` draft, which failed the
unit's own start outright when the credential didn't exist yet), systemd has
ALREADY decrypted it into `$CREDENTIALS_DIRECTORY/restic.password` before the
process starts, when present — the exact same daemon fast-path `soul_crypto.
get_soul_key` uses, no subprocess at read time; when ABSENT, the unit still starts
(ImportCredential= tolerates a missing entry, confirmed live), and this module's
own `get_restic_password` raises `ResticPasswordMissing` for the caller to log
loudly rather than crash the whole runner."""
from __future__ import annotations

import json
import os
import secrets
from pathlib import Path
from typing import Any

from src.ingest import systemd_credential

_CRED_NAME = "restic.password"  # matches osiris-offload.service's own
                                # `ImportCredential=restic.password`


class ResticPasswordMissing(RuntimeError):
    """Raised by `get_restic_password` when nothing below resolves — never a silent
    auto-generate (the SAME law `soul_crypto.SoulKeyMissing` holds, for the same
    reason: a runner that quietly minted its own repository password would make
    every existing snapshot in that repository unreadable the moment it forgot
    that password again)."""


def _password_file_path(*, explicit: str | None = None) -> Path:
    """`explicit` always wins; else `OSIRIS_RESTIC_PASSWORD_FILE` if set; else
    `$XDG_CONFIG_HOME/osiris/restic.password` (or `~/.config/osiris/restic.
    password`) — this box only ever runs the systemd --user deploy shape (Thoth
    DM 9435, the same fact `soul_crypto.py`'s own resolution ladder was built
    against), so unlike `soul_crypto._key_file_path` there is no root/
    system-unit branch to carry here. XDG-aware (not a hard-coded `~/.config`)
    so the SAME `XDG_CONFIG_HOME` redirect a test uses to keep the credstore
    blob (`_credential_path`) off this developer's own real box also keeps
    this logical path's own `.meta.json` sidecar off it — caught live during
    this fix's own build (a stray sidecar was found sitting in this box's real
    `~/.config/osiris/`, cleaned up by hand)."""
    if explicit:
        return Path(explicit).expanduser()
    env = os.environ.get("OSIRIS_RESTIC_PASSWORD_FILE")
    if env:
        return Path(env).expanduser()
    xdg = os.environ.get("XDG_CONFIG_HOME")
    base = Path(xdg).expanduser() if xdg else Path.home() / ".config"
    return base / "osiris" / "restic.password"


def _credential_path(password_path: Path, *, explicit: bool = False) -> Path:
    """DEFAULT (`explicit=False`): the per-user encrypted credstore directory
    (`~/.config/credstore.encrypted/restic.password`) — the location `osiris-
    offload.service`'s own `ImportCredential=restic.password` searches by
    default (THE FIRST KEY MUST COME FROM THE NORMAL CLI, Thoth mail 13065 —
    same shape as `soul_crypto._credential_path`, see that function's own
    docstring for the full live-measured reasoning). EXPLICIT (`explicit=True`,
    a caller-given `--path`/`path=`): the old sibling-of-path shape
    (`<password_path>.cred`), a documented override. Strictly one or the
    other, never both checked — same single-ladder discipline `_password_file_
    path` itself already holds."""
    if explicit:
        return password_path.with_name(password_path.name + ".cred")
    return systemd_credential.user_credstore_encrypted_dir() / _CRED_NAME


def _meta_path(password_path: Path) -> Path:
    return password_path.with_name(password_path.name + ".meta.json")


def _resolve_backend(requested: str | None) -> str:
    """Identical ladder to `soul_crypto._resolve_backend` — see that function's own
    docstring for the live-measured reasoning (host+tpm2 once `tss`-joined, else
    host-cred, else the explicit `file` fallback)."""
    if requested is not None:
        return requested
    if not systemd_credential.systemd_creds_available():
        return "file"
    return "host+tpm2" if systemd_credential.is_tss_member() else "host-cred"


def _generate_password() -> bytes:
    """32 bytes of `secrets.token_urlsafe` — not a Fernet key (no fixed shape restic
    requires), just a long random passphrase. Never printed by this module; restic
    itself never sees anything BUT this value, via `RESTIC_PASSWORD`."""
    return secrets.token_urlsafe(32).encode()


def _write_password_for_backend(
    password: bytes, *, resolved: Path, backend: str, explicit_path: bool = False,
) -> tuple[Path, str]:
    if backend == "file":
        resolved.parent.mkdir(parents=True, exist_ok=True)
        resolved.write_bytes(password)
        resolved.chmod(0o600)
        return resolved, "file"
    with_key = "host+tpm2" if backend == "host+tpm2" else "host"
    blob = systemd_credential.encrypt_with_systemd_creds(
        password, name=_CRED_NAME, with_key=with_key)
    cred_path = _credential_path(resolved, explicit=explicit_path)
    cred_path.parent.mkdir(parents=True, exist_ok=True)
    cred_path.write_bytes(blob)
    cred_path.chmod(0o600)
    meta_path = _meta_path(resolved)
    meta_path.write_text(json.dumps({"backend": backend}))
    meta_path.chmod(0o600)
    return cred_path, backend


def restic_key_init(*, path: str | None = None, backend: str | None = None) -> dict[str, Any]:
    """Mints the repository password ONCE — refuses if one already exists at the
    target path under either shape, same idempotent-refusal law as
    `soul_crypto.soul_key_init`. No `owner=`/root branch (see `_password_file_path`):
    this box's only deploy shape is systemd --user, always the operator's own login
    user."""
    resolved = _password_file_path(explicit=path)
    if resolved.exists() or _credential_path(resolved, explicit=path is not None).exists():
        return {"error": f"a restic password already exists at {resolved} — restic-key "
                         "init never overwrites one in place"}
    resolved.parent.mkdir(parents=True, exist_ok=True)
    password = _generate_password()
    effective_backend = _resolve_backend(backend)
    written_path, effective_backend = _write_password_for_backend(
        password, resolved=resolved, backend=effective_backend, explicit_path=path is not None)
    tss_hint = None
    if (effective_backend == "host-cred" and not systemd_credential.is_tss_member()
            and systemd_credential.systemd_creds_available()):
        tss_hint = ("a stronger TPM2-bound credential is available once you join the "
                    "'tss' group: `usermod -aG tss <user>` (run by root; log out and "
                    "back in after, then re-run restic-key init after removing the "
                    "old credential to upgrade)")
    return {"path": str(written_path), "backend": effective_backend, "tss_hint": tss_hint}


def restic_key_status(*, path: str | None = None) -> dict[str, Any]:
    """Facts about the credential `_password_file_path` resolves to — NEVER the
    password bytes themselves. Same field shape as `soul_crypto.soul_key_status`
    minus the recovery-path fields (no FIDO2 recovery for this credential yet)."""
    import time

    resolved = _password_file_path(explicit=path)
    cred_path = _credential_path(resolved, explicit=path is not None)
    backend = "missing"
    present = False
    carrier: Path | None = None
    if cred_path.exists():
        present = True
        carrier = cred_path
        meta_path = _meta_path(resolved)
        try:
            backend = json.loads(meta_path.read_text()).get("backend", "host-cred")
        except (OSError, ValueError):
            backend = "host-cred"
    elif resolved.exists():
        present = True
        carrier = resolved
        backend = "file"
    created_age_seconds: float | None = None
    if carrier is not None:
        created_age_seconds = max(0.0, time.time() - carrier.stat().st_ctime)
    return {"present": present, "path": str(resolved), "backend": backend,
            "created_age_seconds": created_age_seconds}


def get_restic_password(*, path: str | None = None) -> bytes:
    """Resolution ladder (identical shape to `soul_crypto.get_soul_key`): an
    `OSIRIS_RESTIC_PASSWORD` env override always wins (tests, emergency operator
    override); else, under `osiris-offload.service`'s own
    `LoadCredentialEncrypted=restic.password:...`, systemd has already decrypted it
    into `$CREDENTIALS_DIRECTORY/restic.password` (the daemon fast path, no
    subprocess); else the `.cred` file at the resolved path, decrypted on demand;
    else a legacy plaintext file at the resolved path itself. Raises
    `ResticPasswordMissing` (naming the fix) when none of these resolve."""
    env = os.environ.get("OSIRIS_RESTIC_PASSWORD")
    if env:
        return env.encode()
    cred_dir = os.environ.get("CREDENTIALS_DIRECTORY")
    if cred_dir:
        daemon_path = Path(cred_dir) / _CRED_NAME
        if daemon_path.exists():
            return daemon_path.read_bytes()
    resolved = _password_file_path(explicit=path)
    cred_path = _credential_path(resolved, explicit=path is not None)
    if cred_path.exists():
        return systemd_credential.decrypt_with_systemd_creds(
            cred_path.read_bytes(), name=_CRED_NAME)
    if resolved.exists():
        return resolved.read_bytes()
    raise ResticPasswordMissing(
        f"no restic repository password found at {resolved} (and "
        "OSIRIS_RESTIC_PASSWORD is unset) — run `osiris restic-key init` once, by "
        "hand, in your own terminal")
