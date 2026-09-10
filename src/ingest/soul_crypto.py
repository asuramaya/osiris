"""Soul-store encryption key management (Thoth mail 9134, operator ruling on thread
773d633a): the LIVE soul store (soul_lines/soul_lines_cold) is encrypted at rest by
osiris itself, one tier above host-disk trust. SAME key-management ladder
src.connectors.leases.get_lease_key already established for cookie_leases — env
override -> OS keyring -> a 0600 key file, never invented fresh. That module's own
leases stay on their own separate key (`OSIRIS_LEASE_KEY`); this module's soul key is
independent (`OSIRIS_SOUL_KEY`) — two different secrets protecting two different
tables, neither one a substitute for the other.

`/etc/osiris/soul.key` (overridable via `OSIRIS_SOUL_KEY_FILE`) is the production file
path, named via the SAME `EnvironmentFile=` both `deploy/osiris-worker.service` and
`deploy/osiris-mcp.service` already load for `DATABASE_URL` etc — one config file, no
new deploy step. The OS-keyring branch is real for a `--dev`/desktop install but will
almost always fail over to the file branch under systemd (no login session, no D-Bus
session bus) — see the design posted to thread 773d633a for the full reasoning.

MANDATORY OFFLINE RECOVERY SECRET: every time this module GENERATES a fresh key — first
boot, or a deliberate rotation via `rotate_soul_key` — it is printed to stdout ONCE, the
same key re-encoded for offline custody (a password manager, a printed copy), meant to
leave this box entirely. This module NEVER persists a second copy of it anywhere osiris
itself controls, and NEVER prints an already-existing key again on a later read. Losing
BOTH the live key file and that offline copy makes every row in soul_lines/soul_lines_
cold permanent, irrecoverable ciphertext — a real, disclosed cost, not a hypothetical.
"""
from __future__ import annotations

import os
from pathlib import Path

from cryptography.fernet import Fernet, MultiFernet

_KEYRING_SERVICE = "osiris"
_KEYRING_USER = "soul-encryption-key"


def _generate_and_disclose(where: str) -> bytes:
    """Mint a fresh key and print the MANDATORY offline recovery secret before this
    call's own caller persists it anywhere — a crash between this print and the
    persist step still leaves the operator holding the only copy that matters, never
    the reverse. ONE place both the keyring and file branches of `get_soul_key` route
    through, and `rotate_soul_key` too, so "print the recovery secret" can never be
    added to one branch and silently missed by another."""
    key = Fernet.generate_key()
    print(
        "osiris soul-store encryption key GENERATED — THIS IS THE ONLY TIME THIS KEY "
        f"PRINTS (persisting to {where}). Copy it now to OFFLINE custody — a password "
        "manager entry, a printed copy — kept OUTSIDE this box and OUTSIDE any backup "
        "target (a backed-up copy sitting next to the ciphertext it protects defeats "
        "the whole point). Losing BOTH this printed copy and the live file makes every "
        "stored transcript permanently, irrecoverably unreadable.\n"
        f"  {key.decode()}\n",
        flush=True)
    return key


def get_soul_key() -> bytes:
    """Resolve the PRIMARY Fernet key: explicit env override -> OS keyring -> key file.
    `OSIRIS_SOUL_KEY` overrides both (tests, emergency operator override). An EXISTING
    key is read silently, never re-disclosed — only a freshly GENERATED one ever prints
    (see `_generate_and_disclose`)."""
    env = os.environ.get("OSIRIS_SOUL_KEY")
    if env:
        return env.encode()
    try:
        import keyring  # local import: backend probing can be slow / fail on servers

        existing = keyring.get_password(_KEYRING_SERVICE, _KEYRING_USER)
        if existing is not None:
            return existing.encode()
        key = _generate_and_disclose("the OS keyring")
        keyring.set_password(_KEYRING_SERVICE, _KEYRING_USER, key.decode())
        return key
    except Exception:
        # headless / no Secret Service -> protected key file (systemd-creds-style)
        path = Path(
            os.environ.get("OSIRIS_SOUL_KEY_FILE", "/etc/osiris/soul.key")
        ).expanduser()
        if path.exists():
            return path.read_bytes()
        path.parent.mkdir(parents=True, exist_ok=True)
        key = _generate_and_disclose(str(path))
        path.write_bytes(key)
        path.chmod(0o600)
        return key


def get_soul_fernet() -> MultiFernet:
    """The encrypt/decrypt object every soul_store write/read site uses. `MultiFernet`
    over the PRIMARY key (`get_soul_key`, always first — new writes encrypt with this
    one only) plus any LEGACY keys named in `OSIRIS_SOUL_KEY_LEGACY` (comma-separated
    Fernet keys, still valid to DECRYPT, never used to encrypt a new write) — the
    rotation window this exists for: old ciphertext keeps reading while a migration
    re-encrypts it onto the new primary, so rotation runs as a background pass rather
    than stop-the-world. `MultiFernet.decrypt` tries each key in order and raises
    `cryptography.fernet.InvalidToken` only once NONE of them work."""
    keys = [Fernet(get_soul_key())]
    legacy = os.environ.get("OSIRIS_SOUL_KEY_LEGACY", "")
    keys.extend(Fernet(k.strip().encode()) for k in legacy.split(",") if k.strip())
    return MultiFernet(keys)
