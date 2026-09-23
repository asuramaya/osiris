"""The shared systemd-creds primitives, extracted as part of the key-custody rewrite so
the restic offload password gets the same custody mechanics used for the soul-store key
in `soul_crypto.py`, rather than a hand-copied second implementation that could drift
from the first (a sibling that silently diverges from its twin is a well-known failure
mode this project's own standing practices warn against). `soul_crypto.py` itself still
owns its own `_credential_path`/`_meta_path`/`_resolve_backend`/`_write_key_for_backend`/
`soul_key_*`; those are shaped around one specific secret (a Fernet key wrapping the
soul store) and its own recovery model (FIDO2). Only the raw systemd-creds subprocess
boundary below is generic enough to share without forcing an unrelated caller through
soul-store-specific assumptions.

Measured live on this box (2026-09-22, all as the login user, no root): `systemd-creds
encrypt --user --with-key=host` round-trips without root; `--with-key=host+tpm2`
succeeds once the caller's own user has joined the `tss` group (owns `/dev/tpmrm0`);
`--with-key=tpm2` alone refuses in `--user` scope ("Selected key not available in
--uid= scoped mode, refusing"). `is_tss_member`/`systemd_creds_available` exist
specifically so a caller's own backend-selection logic can reproduce that same ladder
without re-deriving it.
"""
from __future__ import annotations

import getpass
import grp
import os
import shutil
import subprocess
from pathlib import Path

_TSS_GROUP = "tss"  # owns /dev/tpmrm0 on this box; membership gates --with-key=host+tpm2


def user_credstore_encrypted_dir() -> Path:
    """The requirement that the first key must come from the normal CLI or the
    console depends on this: the per-user service manager's own encrypted
    credential store directory, `$XDG_CONFIG_HOME/credstore.encrypted/`
    (confirmed live on this box via `systemd-path user-credential-store-
    encrypted`, matching systemd.exec(5)'s own documented per-user search path).
    A unit's `ImportCredential=<name>` searches this directory (among others)
    for a file literally named `<name>` and, unlike `LoadCredentialEncrypted=
    <name>:<hard path>`, which this replaces, treats a missing file there as
    not fatal to unit start (confirmed live: a throwaway --user oneshot unit
    with `ImportCredential=` against an absent credstore entry started and
    finished cleanly, `$CREDENTIALS_DIRECTORY/<name>` simply didn't exist).
    This is what makes minting the very first key possible without a
    workaround: the operator can deploy the code, start the units in a
    loudly-degraded state, then run `osiris soul-key init` (or the console's
    Init button) through the normal, already-deployed CLI, never a special
    pinned scratch worktree required just to mint the very first key before
    anything could start at all."""
    xdg = os.environ.get("XDG_CONFIG_HOME")
    base = Path(xdg).expanduser() if xdg else Path.home() / ".config"
    return base / "credstore.encrypted"


def is_tss_member() -> bool:
    """Whether the current process's own user is a member of the `tss` group.
    This gates whether `--with-key=host+tpm2` can ever succeed in `--user` scope.
    Checks supplementary membership only (`grp.getgrnam(...).gr_mem`), the
    realistic case (nobody's primary group is `tss`); a box with no `tss` group at
    all (no TPM tooling installed) reads as False, never an exception."""
    try:
        tss = grp.getgrnam(_TSS_GROUP)
    except KeyError:
        return False
    return getpass.getuser() in tss.gr_mem


def systemd_creds_available() -> bool:
    """Whether `systemd-creds` is on PATH at all: the one guard that decides
    whether a caller's own backend auto-selection ever proposes the credential
    backend instead of falling back to a plaintext fallback outright (a
    non-systemd host, or a systemd too old to carry the binary)."""
    return shutil.which("systemd-creds") is not None


def run_systemd_creds(args: list[str], *, input_bytes: bytes) -> bytes:
    """The one subprocess boundary every systemd-creds call in this module (and
    every module built on top of it) routes through: a bounded, nothing-fancy
    `subprocess.run` (deliberately sync; an async caller wraps this in
    `asyncio.to_thread` at their own boundary, never here) with stdin/stdout as
    pipes (`-`/`-` in the caller's own `args`) so a plaintext secret never touches
    a temp file. Raises `RuntimeError` naming the real stderr on any nonzero exit,
    never a silent empty-bytes return that could masquerade as an empty (still
    technically valid-looking) credential."""
    proc = subprocess.run(
        ["systemd-creds", *args], input=input_bytes, capture_output=True, timeout=30)
    if proc.returncode != 0:
        raise RuntimeError(
            f"systemd-creds {' '.join(args)} failed (exit {proc.returncode}): "
            f"{proc.stderr.decode(errors='replace').strip()}")
    return proc.stdout


def encrypt_with_systemd_creds(plaintext: bytes, *, name: str, with_key: str) -> bytes:
    """`name` is the `--name=` a matching `LoadCredentialEncrypted=<name>:<path>`
    unit directive must use verbatim: systemd binds the credential's decrypted
    identity to this name, so a mismatch between mint-time and load-time names
    fails at daemon start, not silently."""
    return run_systemd_creds(
        ["encrypt", "--user", f"--with-key={with_key}", f"--name={name}", "-", "-"],
        input_bytes=plaintext)


def decrypt_with_systemd_creds(blob: bytes, *, name: str) -> bytes:
    return run_systemd_creds(
        ["decrypt", "--user", f"--name={name}", "-", "-"], input_bytes=blob)
